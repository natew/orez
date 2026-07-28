# cf-do SQL backend — perf findings

Harness: `perf/scripts/bench-cf-do.ts` — drives the `ZeroDO` worker's
development `/exec`, `/batch`, and `/changes` HTTP tools and asserts
conformance.

Run the lean DO worker first (same as chat e2e, CHAT_E2E.md §5):

```bash
cd packages/orez-lite/src/cf-do && bunx wrangler dev --port 8799 --local --no-show-interactive-dev-session
# then, from repo root:
bun run perf/scripts/bench-cf-do.ts            # defaults: CONC=4 N=1000
CONC=8 N=2000 bun run perf/scripts/bench-cf-do.ts
```

JSON reports land in `perf/reports/` (gitignored).

## baseline (2026-06-08, wrangler dev --local, M-series laptop, CONC=4, N=1000)

| scenario                    |  ops/s |   mean |    p50 |     p95 |     p99 |
| --------------------------- | -----: | -----: | -----: | ------: | ------: |
| exec INSERT (tracked)       |  1,541 |   2.59 |   2.62 |    4.07 |    5.15 |
| exec SELECT (point)         |  1,756 |   2.28 |   2.05 |    3.89 |    7.36 |
| batch x20 INSERT (per-stmt) | 15,233 | 5.13\* | 4.95\* | 11.04\* | 11.56\* |

\* batch latency is per-batch (20 statements); the ops/s column is per-statement.

Conformance (all green): insert→select roundtrip, change capture, strictly
increasing watermark, batch atomicity (a bad statement rolls the whole batch
back), delete emits a DELETE change.

## the one finding that matters

**The DO SQL path is HTTP-round-trip-bound, not compute-bound.** A single `/exec`
is ~2.5 ms whether it's a read or a tracked write — that's the wrangler/DO request
hop, not SQLite work (DO SQLite is native, sub-ms). So **throughput scales with
how few HTTP calls you make: `/batch` is ~9× per statement** (1.5k → 15k stmt/s by
packing 20 statements into one request).

This quantifies CHAT_E2E.md §8's first listed lever. chat boot fires thousands of
individual `INSERT INTO reaction ... ON CONFLICT DO NOTHING` `/exec` calls, each
paying the full hop. The §4 amplification bugs (redundant metadata/probe HTTPs)
are already fixed — the remaining boot cost is the seed-insert round-trip count.

These HTTP routes are not on the production Orez Lite path. Production
application SQL uses Durable Object RPC, and the Rust host reads the native
change feed.

## cleanup done in this pass

`packages/orez-lite/src/cf-do/worker.ts` outer `export default` collapsed from a 35-line per-path
route table (that duplicated the DO's own `fetch` routing and could drift) to a
4-line forward-all to the singleton DO. Side benefit: CORS `OPTIONS` preflight now
reaches the DO's handler (was 404 at the outer worker). Validated: bench green,
`worker-schema` / `watermark` unit tests green, `OPTIONS /exec → 200`.

## should the data worker's TypeScript be Rust? (2026-07-27)

Investigated for the cf-do data worker: feed projection (`projectOrezFeedBody`),
the `/changes` pull decode, and the JS↔wasm boundary. **Answer: no.** Rust
measured 2.3-2.8x SLOWER on the projection path in all three runtimes, and the
path was never where the time went anyway.

Harness: `scratchpad/bench` (not checked in) — a faithful Rust port of
`projectFeedBody` built with the existing `crates/` wasm-pack pipeline, gated on
byte-exact agreement with the shipped TS before any timing was taken.

### projection: Rust loses, and the ceiling is structural

5000-change `/changes` body (2.7 MB), per call:

| runtime      | TS parse+project+stringify | Rust str→str (wasm) |        ratio |
| ------------ | -------------------------: | ------------------: | -----------: |
| workerd      |                   14.60 ms |            33.18 ms | 2.27x slower |
| node 25 (V8) |                   13.35 ms |            30.44 ms | 2.28x slower |
| bun (JSC)    |                    9.26 ms |            25.85 ms | 2.79x slower |

The drop-in shape (JS object in, JS object out, through `serde-wasm-bindgen`) is
**19x slower** than the TS projection it would replace: 78.81 ms vs 4.12 ms in
workerd. Anything that hands wasm a materialized JS object tree is a non-starter.

Two controls explain why, and they cap any future attempt:

- `roundtrip_json` — serde_json parse + serialize, **zero** projection work —
  costs 20.50 ms in workerd, already **1.40x more than the entire TS pipeline**.
  The JS engine's JSON codec is native C++; Rust's is wasm. No amount of
  optimizing the transform closes that.
- `echo_str` — copying the 2.7 MB payload into wasm memory and back out, doing
  nothing — costs 4.12 ms in workerd, exactly the cost of the whole TS
  projection. workerd's string boundary is ~4.5x more expensive than node's
  (0.92 ms), so the tax is worst in the runtime that ships.

Optimizing the Rust from clone-per-value to move-per-value (the idiomatic
version) bought 15% and changed nothing about the verdict.

The projection is also not worth attacking in any language. On a 5000-row pull
it is ~1.5 ms of compute; SQLite is ~1.7 ms. Neither is the bill.

### where a /changes pull actually goes

Differential measurement against a live DO, 5000 rows, each step adding exactly
one stage over the previous (`limit=5000`, wrangler dev --local):

| stage                                   |       p50 |        delta |
| --------------------------------------- | --------: | -----------: |
| request floor (`SELECT 1`)              |   0.78 ms |              |
| + SQLite scan of `_zero_changes`        |   2.51 ms |      1.73 ms |
| + full 2.8 MB payload out               |  22.17 ms |     19.66 ms |
| + per-row `JSON.parse` + `normalizeRow` | 110.03 ms | **87.86 ms** |

**SQLite is 1.6% of the pull.** 80% was the per-row decode — and most of that
turned out to be a defect, not decode.

### the defect: schemaForTable cached hits but not misses

`normalizeRow` asks `schemaForTable` for a schema on every change row.
`schemaForTable` cached a hit and never a miss, so a table absent from
`_zero_schema_tables` re-ran `CREATE TABLE IF NOT EXISTS`, a `SELECT`, and a
thrown-and-caught `.one()` error **per row, per pull, forever**.

This is not an edge case. The change feed always carries the mutation cursor
sources `_zsync_clients` and `<app>_0.clients|mutations`, which applications
must never add to their Zero schema (`projectFeedBody` rewrites them precisely
so applications don't have to), and `_zero_schema_tables` is populated only from
`schema.tables`. Those rows missed on every lookup by design.

Isolated with byte-identical payloads whose only difference is whether
`table_name` resolves:

|        | unregistered table | registered table |
| ------ | -----------------: | ---------------: |
| before |          114.49 ms |         40.01 ms |
| after  |           30.03 ms |         29.77 ms |

3.81x on the unregistered path at 5000 rows, 3.46x at 1000, registered path
unchanged. Fixed by caching `null` for a confirmed absence; safe because every
path that can change the answer already clears the whole map via
`invalidateSchemaCaches()`.

### if the pull needs to get faster again

Post-fix, a 5000-row pull is ~30 ms in the DO plus ~14.6 ms in the front worker,
and what remains is JSON codec and payload movement, not transform logic:

- The DO stores `row_data` as JSON text, parses it per row, normalizes it, then
  `Response.json` re-serializes it. Storing it in final shape would let the DO
  splice the stored strings straight into the response and skip both ends.
- `normalizeRow`'s serverName fallback is `Object.values(...).find(...)` per
  column, allocating an array per column per row. It is O(columns²) per row
  whenever physical names differ from schema keys, which is the drizzle-generated
  norm. A prebuilt serverName→type map per table removes it.

Neither needs Rust. Both are larger wins than the projection rewrite that was
being considered.
