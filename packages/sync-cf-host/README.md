# sync-cf-host M0 platform proof

This package is the executable platform-contract probe for the Rust sync
engine's Cloudflare host. It is intentionally a test Durable Object rather than
the M3 production host. `wasm-pack` compiles `crates/sync-wasm`, Wrangler bundles
the generated module, and the same 36-assertion script runs against local
workerd and the throwaway deployed Worker.

## Boundary proved

- Pull enters `ctx.storage.transactionSync()` and calls synchronous Rust wasm,
  which calls back into `SqlStorageSyncDb` for fully materialized queries.
- Push enters `ctx.storage.transaction(async)`. Rust preflight runs first, the
  TypeScript mutator performs SQL on both sides of `await scheduler.wait(1)`,
  and Rust finalization runs last.
- The SQL adapter exposes only synchronous `exec` and `query`, positional `?`
  bindings, and rejects transaction statements plus numbered `?N` parameters.
  Rust never emits `BEGIN`, `COMMIT`, `ROLLBACK`, or `SAVEPOINT`.
- Deferred external effects are examined only after the transaction promise
  resolves. Before an effect runs, the probe verifies that its mutation record
  is visible in durable SQL.

The real-shape mutators are read-then-write, multi-table (account, ledger, and
outbox), and application-error with a deferred notification. A separate JS
fault throws after an await *and after Rust finalization*, proving that account,
outbox, mutation log, and LMID writes all roll back. The Rust panic probe writes
both ledger and LMID before trapping across wasm; both roll back as well.

This behavior agrees with Cloudflare's documented SQLite transaction contract:
[`transactionSync` rolls back on a thrown exception and async `transaction`
includes direct SQLite storage operations](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/).

## Cookie, watermark, and i64 decision

Cookies, watermarks, and LMIDs use **canonical unsigned decimal strings** at
every JavaScript, JSON, and wasm boundary:

- grammar: `0` or `[1-9][0-9]*` (no sign, fraction, exponent, or leading zero)
- initial supported range: SQLite signed integer, `0..=9223372036854775807`
- Rust parses and compares the value as `i64`; JavaScript never compares the
  decimal strings lexicographically
- exact SQLite reads must select `CAST(counter AS TEXT)`
- an unsafe integer binding crosses JS as decimal text into an INTEGER-affinity
  column, allowing SQLite to parse it without an intermediate JS `Number`
- JSON emits the decimal string, never a `number` or `bigint`

This is required because SqlStorage explicitly documents that a retrieved
`int64` can lose precision in JavaScript. The value test stores and retrieves
`9007199254740993` (`2^53 + 1`) exactly through JS -> wasm -> SQLite -> wasm ->
JS. It also round-trips `-42`, `0.1 + 0.2` as
`0.30000000000000004`, Unicode text, a five-byte blob, null, nested JSON, and a
boolean. See [Cloudflare's SqlStorage numeric warning](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#exec).

The baseline TypeScript host currently types cookies as numbers. M1/M3 must
change that host/wire validation to accept the canonical string; converting a
persisted watermark to a JS number is not an allowed compatibility shortcut.

## Reproduction

Requirements are Bun 1.3.10, Rust 1.94, `wasm32-unknown-unknown`, and wasm-pack
0.14. The package pins Wrangler and workerd locally.

```sh
cd packages/sync-cf-host
bun install
bun run typecheck
bun run test
bun run bundle
bun run measure
```

`bun run test` builds wasm and then launches `wrangler dev --local`, which runs the Worker in local
workerd. To run the identical checks against a deployed probe:

```sh
bun run build:wasm
M0_BASE_URL=https://orez-rust-sync-m0-probe.lslcf.workers.dev bun test.mjs
```

The native SQLite proof is independent of the workspace and runs with:

```sh
cargo test --manifest-path probes/native-rusqlite/Cargo.toml
```

## Recorded results

Measured 2026-07-09 HST (2026-07-10 04:27 UTC) on local darwin-arm64 with
Wrangler 4.103.0 and workerd 1.20260617.1.

| Check | Result |
| --- | --- |
| Local workerd suite | 36 assertions passed |
| Deployed suite | 36 assertions passed |
| Native rusqlite | 3 tests passed |
| Wrangler upload | 197.30 KiB total; 83.41 KiB gzip |
| Wasm module | 167 KiB on disk |
| Deployed Worker startup | 1 ms reported by Wrangler |
| Local Wrangler spawn to ready | 629.339 ms (includes CLI/process startup) |
| Cold DO first pull, 30 new objects | p50 2.059 ms; p95 2.624 ms |
| Async read-then-write tx, 50 pushes | p50 3 ms; p95 3 ms DO-side elapsed |
| Rust/JS boundary portion | p50 0 ms; p95 1 ms (workerd timer resolution) |
| SQL adapter portion | p50 0 ms; p95 0 ms (workerd timer resolution) |
| workerd RSS baseline | 97.938 MiB |
| workerd RSS after 30 cold DOs | 115.703 MiB |
| workerd RSS after 50 additional pushes | 126.797 MiB; +28.859 MiB total |

The successful push uses five wasm-to-JS database crossings (two `exec`, three
`query`) in addition to the TypeScript mutator's direct SQL. The CPU figures are
local wall-time proxies from `performance.now()`, not billed production CPU.
The memory figure is the whole local workerd child RSS, because per-isolate
memory is not exposed; it is an initial regression baseline, not a per-namespace
allocation claim.

Cloudflare's conservative Free-plan bundle limit is 3 MiB gzip (10 MiB paid),
so 83.41 KiB is **97.3% below the 3 MiB limit**, comfortably beyond the M0 target
of 40% headroom. The platform memory limit is 128 MiB per isolate, but local
process RSS is not directly comparable to that isolate limit. Limits source:
[Cloudflare Workers limits](https://developers.cloudflare.com/workers/platform/limits/).

The deployed throwaway is `orez-rust-sync-m0-probe` on the `lslcf` account at
the URL above. The verified deployment version is
`135c1874-5efe-4e49-b340-4b33d6c14103`.

## Eviction model

Normal platform eviction is nondeterministic. Following `harness/cf/worker.ts`,
the probe deterministically discards its in-memory boot ID and side-effect list
after a 250 ms idle gap, then reconstructs behavior over unchanged Durable
Object SQL. The test requires a changed boot ID while LMID, balance, and the
mutation record remain intact. This is a deterministic re-instantiation model,
not a claim that workerd physically killed the object process at 250 ms.
