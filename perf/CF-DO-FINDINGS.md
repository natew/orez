# cf-do SQL backend — perf findings

Harness: `perf/scripts/bench-cf-do.ts` — drives the `ZeroDO` worker's
`/exec`, `/batch`, `/changes` over HTTP (the surface `DoBackend` hammers during
chat e2e boot) and asserts conformance.

Run the lean DO worker first (same as chat e2e, CHAT_E2E.md §5):

```bash
cd src/cf-do && bunx wrangler dev --port 8799 --local --no-show-interactive-dev-session
# then, from repo root:
bun run perf/scripts/bench-cf-do.ts            # defaults: CONC=4 N=1000
CONC=8 N=2000 bun run perf/scripts/bench-cf-do.ts
```

JSON reports land in `perf/reports/` (gitignored).

## baseline (2026-06-08, wrangler dev --local, M-series laptop, CONC=4, N=1000)

| scenario                    |  ops/s | mean | p50  | p95   | p99   |
| --------------------------- | -----: | ---: | ---: | ----: | ----: |
| exec INSERT (tracked)       |  1,541 | 2.59 | 2.62 | 4.07  | 5.15  |
| exec SELECT (point)         |  1,756 | 2.28 | 2.05 | 3.89  | 7.36  |
| batch x20 INSERT (per-stmt) | 15,233 | 5.13*| 4.95*| 11.04*| 11.56*|

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

## next perf lever (NOT done here — flagged risky)

Have `DoBackend` (src/pg-proxy-do-backend.ts) coalesce a run of same-shape inserts
inside one transaction into a single `/batch` instead of N `/exec`. CHAT_E2E.md §8
flags this risky (prepared-statement / bind-param tracking). This harness is the
gate for it: it proves `/batch` is atomic + conformant and measures the win, so a
DoBackend batching change can be validated for speed *and* correctness here before
the full chat e2e run. Not attempted in this pass to keep the hot path stable
(chat e2e must-pass + shared-machine CPU budget).

## cleanup done in this pass

`src/cf-do/worker.ts` outer `export default` collapsed from a 35-line per-path
route table (that duplicated the DO's own `fetch` routing and could drift) to a
4-line forward-all to the singleton DO. Side benefit: CORS `OPTIONS` preflight now
reaches the DO's handler (was 404 at the outer worker). Validated: bench green,
`worker-schema` / `watermark` unit tests green, `OPTIONS /exec → 200`.
