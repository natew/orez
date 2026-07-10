# `sync-cf-host`

This is the production-shaped Cloudflare Durable Object host for the Rust sync
engine. The package exposes a consumer integration surface (`createSyncWorker`,
`createSyncDurableObject`, `registerMutators`) and a harness deployment built
from `src/harness-worker.ts`. Consumers provide a JSON Zero schema, application
DDL/seed initializer, normalized-claims authenticator, mutator registry, and an
optional row-visibility hook.

The Worker authenticates at the consumer edge, forwards only normalized claims
to a per-namespace Durable Object, and never logs tokens, mutation arguments, or
row contents. Pull runs `engine_handle_pull` inside `transactionSync`; push runs
Rust `push_validate`/`preflight`/`finalize`/`record_app_error` steps around the
registered asynchronous TypeScript mutator inside `ctx.storage.transaction`.
Every SQL cursor is materialized before an await. Application effects are
collected and run only after their transaction commits; application failures use
the required second transaction to advance the LMID marker.

## Wake channel and eviction

`GET /<namespace>/wake?clientID=<id>` upgrades to a Durable Object hibernating
WebSocket. Socket attachments carry only the client ID. A committed push sends a
text `wake` frame to all connected clients except the pusher; a scheduler window
coalesces a burst into one frame per socket. `ping` receives `pong`. The message
contains no state and carries no correctness weight: clients pull after a wake
and retain their safety poll. `ctx.getWebSockets()` plus serialized attachments
means sockets remain discoverable after hibernation/re-instantiation.

`/admin/status` reports a boot ID, hibernation simulation count, connected wake
sockets, durable database size, engine watermark/floor, and aggregate counters.
After the configured idle gap (5 seconds in the harness deployment), the local
deterministic model resets in-memory state and changes boot ID while retaining
SQLite state and sockets. This models the harness/cf idle-teardown pattern; it
does not claim that a real platform eviction happens on a 5-second schedule.

## Counter and HTTP wire representation

Inside the wasm/JavaScript engine boundary, cookies, watermarks, and LMIDs are
canonical decimal strings (`0` or non-zero ASCII digits), parsed as signed
SQLite-range `i64`; exact SQLite reads use `CAST(... AS TEXT)`. The baseline HTTP
wire remains JSON numbers for Zero 1.7 byte compatibility, accepted only within
the JavaScript safe-integer range. M1 centralizes this conversion in
`sync-core::wire::counter_to_json` and accepts either representation inbound.
No engine code silently converts an unsafe persisted counter through a JS
`Number`.

## Local checks

```sh
cd packages/sync-cf-host
bun install
bun run typecheck             # builds probe-enabled wasm for all TS declarations
bun run test:platform         # M0 boundary regression: 36 local workerd assertions
bun run test:integration      # production host: pull/push/rollback/wake/eviction
bun run bundle                # Wrangler dry-run bundle report
bun run measure               # cold start/CPU/storage/memory baseline
cargo test -p sync-core       # engine/model/reference/soot suites (from repo root)
```

The production integration suite can target a deployed Worker:

```sh
M3_BASE_URL=https://orez-rust-sync.lslcf.workers.dev \
M3_ADMIN_KEY="$(tr -d '\n' < ~/.zharness-cf-admin-key)" \
bun integration-test.mjs
```

The platform regression remains a separate feature-enabled test path so probe
helpers are absent from the production wasm bundle:

```sh
bun run test:platform
```

## M3 measurements

Measured 2026-07-10 UTC on darwin-arm64, Bun 1.3.10, Rust 1.94.0,
wasm-pack 0.14.0, Wrangler 4.103.0, workerd 1.20260617.1. Local workerd was
started with the production `orez-rust-sync` config and an equivalent local
admin variable; deployed requests used the `lslcf` account and returned a
Cloudflare LAX edge (`cf-ray …-LAX`).

| Measurement | Result |
| --- | --- |
| Local production integration | 16 assertions passed |
| Deployed production integration | 16 assertions passed |
| Local M0 platform regression | 36 assertions passed |
| Rust core tests | 27 reference + 13 composition + 2 model tests passed |
| Bundle upload | 344.96 KiB total; 136.56 KiB gzip |
| Wasm module | approximately 273 KiB on disk |
| Wrangler reported startup | 1 ms |
| Local cold DO pull (30 namespaces) | p50 5.362 ms; p95 7.081 ms |
| Local push acknowledgement (50 mutations) | p50 1.507 ms; p95 2.971 ms |
| Seeded storage | 81,920 bytes |
| Storage after 50 pushes | 90,112 bytes (+8,192 bytes) |
| Local workerd RSS | 97.391 MiB baseline; 146.625 MiB after load (+49.234 MiB process RSS) |
| CF eviction lane | boot ID changed; 20 writes; 126 pulls; zero 409s; monotone cookies |
| CF 10-client/2-writer bench | ack p50/p95 178/243 ms; propagation p50/p95 381/524 ms |
| Equivalent TS DO bench | ack p50/p95 165/903 ms; propagation p50/p95 583/1,074 ms |

Wake fan-out is anchored with `waitUntil` after commit and is not on the push
response's critical path. The acknowledgement result is a local wall-time
proxy, not billed Cloudflare CPU. RSS is the whole local
workerd process, not an isolate allocation; Cloudflare's 128 MiB per-isolate
limit cannot be inferred from that process number. The bundle is 95.6% below
the 3 MiB gzip Free-plan limit (and 98.7% below the 10 MiB paid-plan limit).
See [Cloudflare Worker limits](https://developers.cloudflare.com/workers/platform/limits/).

## Deployment

`wrangler.toml` is checked in with Worker name `orez-rust-sync`, SQLite Durable
Object class `SyncDurableObject`, and account `6afff1f79e2fd12f1cfd1bfe1dfd08d1`.
The deployed test version used for this M3 pass was
`bdce2432-b6c0-4be0-b6b7-915586f25eba` at
`https://orez-rust-sync.lslcf.workers.dev`. The throwaway M0 Worker was already
absent when the cleanup command was run (Cloudflare returned error 10090), so no
old probe service remains to receive traffic.

The rust toolchain is pinned at the workspace root in
[rust-toolchain.toml](/Users/n8/.worktrees/orez-rust-sync/rust-toolchain.toml).
