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
Every SQL cursor is materialized before an await. Mutators may await only their
`MutatorSql` operations; timers, fetches, and other external work belong in
`context.defer`, which runs only after commit. Application failures use the
required second transaction to advance the LMID marker.

The root `orez/cf-do` executor and this host consume the same `post-commit`
module, so transaction retries discard effects from abandoned attempts in both
paths.

## Wake channel and eviction

`GET /<namespace>/wake?clientID=<id>&wakeToken=<capability>` upgrades to a
Durable Object hibernating WebSocket after `authorizeWake` accepts the
capability. Browser consumers should mint a short-lived, namespace-scoped token
at their authenticated edge because the native WebSocket constructor cannot set
an authorization header. Socket attachments carry only the client ID. A
committed push sends a text `wake` frame to all connected clients except the
pusher; a scheduler window
coalesces a burst into one frame per socket. `ping` receives `pong`. The message
contains no state and carries no correctness weight: clients pull after a wake
and retain their safety poll. `ctx.getWebSockets()` plus serialized attachments
means sockets remain discoverable after hibernation/re-instantiation.

### Consumer-minted wake capabilities

The consumer Worker owns both token minting and verification. Add an
authenticated edge route that signs the namespace and a short expiry, typically
30 to 60 seconds, with a secret that never reaches the browser. Return only the
signed token:

```ts
// consumer edge route, after normal session authentication
const expiresAt = Date.now() + 60_000
const token = await signWakeToken({ namespace, userID, expiresAt }, env.WAKE_SECRET)
return Response.json({ token, expiresAt })
```

Pass a mint callback to the canonical HTTP transport. It calls `getToken()` for
every socket attempt, including reconnects, so short-lived tokens are never
reused after the wake connection drops:

```ts
import { ensureHttpPullTransport } from 'orez/zero-http'

ensureHttpPullTransport({
  origin: syncOrigin,
  pullIntervalMs: 5_000,
  wake: {
    async getToken() {
      const response = await fetch(`/api/sync/${namespace}/wake-token`, {
        method: 'POST',
      })
      if (!response.ok) throw new Error('wake token mint failed')
      return (await response.json()).token
    },
  },
})
```

The transport appends that value as `wakeToken` because browser WebSockets
cannot set an authorization header. Verify its signature, expiry, and namespace
inside the consumer's `authorizeWake` callback. `sync-cf-host` deliberately does
not prescribe a token format or hold the signing key:

```ts
authorizeWake(request, env) {
  const url = new URL(request.url)
  return verifyWakeToken(url.searchParams.get('wakeToken'), {
    namespace: namespaceFrom(url),
    secret: env.WAKE_SECRET,
  })
}
```

Treat the URL token as a narrowly scoped capability and avoid logging it. If
minting or wake authorization is unavailable, the wake channel retries in the
background while HTTP pulls and the safety poll continue to provide
convergence.

`/admin/status` reports a boot ID, hibernation simulation count, connected wake
sockets, durable database size, engine watermark/floor, and aggregate counters.
After the configured idle gap (5 seconds in the harness deployment), the local
deterministic model resets in-memory state and changes boot ID while retaining
SQLite state and sockets. This models the harness/cf idle-teardown pattern; it
does not claim that a real platform eviction happens on a 5-second schedule.

## Authenticated operator controls

All `/admin/*` routes are rejected unless the consumer's `authorizeAdmin`
callback succeeds or the request presents the deployment's `ADMIN_KEY` through
`x-admin-key`. Harness and operator deployments expose `/admin/status`; it
includes the persisted writer-enabled state and wasm linear-memory byte length
in addition to the engine/counter fields above.

`POST /<namespace>/notify` is rejected unless `authorizeNotify` accepts a
service or operator capability. Both `authorizeWake` and `authorizeNotify` run
before `idFromName`, so rejected requests cannot instantiate namespace Durable
Objects.

### Consumer routing traps

If an outer application router has its own namespace gate, let only `/wake` and
`/notify` pass that outer gate. The sync worker's required `authorizeWake` and
`authorizeNotify` callbacks then enforce the real capability checks. Do not
bypass pull, push, or admin routes, and do not replace either callback with an
unconditional allow.

A delegated push must terminate at the application worker whose registry owns
the named mutator. Routing it to a sync host, control-plane worker, or another
application server with a different registry produces an authoritative error
such as `could not find mutator <name>`. The optimistic client write may appear
briefly, then disappear on reload and never reach peers. Route client pushes to
the sync host only when its `mutateBinding` and `mutateUrl` delegate to the
owning application's mutate endpoint. Preview and browser-local transports must
stay pointed at their in-process project server unless they provide the same
delegated route.

`POST /admin/writer` with `{ "enabled": false }` durably stops pushes for that
namespace. A stopped writer consumes and discards the request body, returns 503,
and performs no engine or application write. Canary rollback drills must stop
one writer and prove rejection before enabling another. The test-only runner is:

```sh
mise exec node@24.3.0 -- bun harness/src/rollback-drill.ts --confirm-test-only
```

The runner accepts only loopback or the `lslcf.workers.dev` test worker and does
not mutate a production route. These controls are mechanisms, not authorization
to perform a production cutover.

`POST /admin/resnapshot` is available only when the consumer configured an
upstream data service. It reads each modeled source table through bounded
keyset pages and commits them to a staged generation. Progress is durable, so a
restart resumes at the recorded table and cursor. The host then catches up
concurrent `/changes`, atomically swaps the generation into place, and bumps the
engine epoch. Every client performs one expected full resync after cutover.
Engine metadata, client last-mutation IDs, operator controls, and the
authoritative upstream database are preserved. The legacy single-response
`/snapshot` endpoint remains for small datasets and older harnesses.

## Delegated service addresses

`mutateBinding` selects the service binding that owns the application's mutate
endpoint and defaults to `upstream.binding`. Set `mutateOrigin` to an exact
absolute HTTP(S) origin when that worker's routing depends on the request
origin. `upstream.namespacePath` may return `/` for a root-mounted data feed;
internally that root is an empty path, while `null` alone means no upstream path
is configured.

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

| Measurement                                                          | Result                                                                |
| -------------------------------------------------------------------- | --------------------------------------------------------------------- |
| Local production integration                                         | 16 assertions passed                                                  |
| Deployed production integration                                      | 16 assertions passed                                                  |
| Local M0 platform regression                                         | 36 assertions passed                                                  |
| Rust core tests                                                      | 27 reference + 13 composition + 2 model tests passed                  |
| Bundle upload                                                        | 328.34 KiB total; 130.45 KiB gzip                                     |
| Wasm module                                                          | approximately 273 KiB on disk                                         |
| Wrangler reported startup                                            | 1 ms                                                                  |
| Local cold DO pull (30 namespaces)                                   | p50 5.362 ms; p95 7.081 ms                                            |
| Local push acknowledgement (50 mutations)                            | p50 1.507 ms; p95 2.971 ms                                            |
| Seeded storage                                                       | 81,920 bytes                                                          |
| Storage after 50 pushes                                              | 90,112 bytes (+8,192 bytes)                                           |
| Local workerd RSS                                                    | 97.391 MiB baseline; 146.625 MiB after load (+49.234 MiB process RSS) |
| CF eviction lane                                                     | boot ID changed; 20 writes; 126 pulls; zero 409s; monotone cookies    |
| CF wake-only storm (100 clients, 5 writers, 10 s safety poll)        | propagation p50/p95 809/810 ms                                        |
| CF clean-write propagation (10 clients, 20 writes, 10 s safety poll) | commit-to-seen p50/p95 136/406 ms; issue-to-seen p95 858 ms           |
| CF 10-client/2-writer bench                                          | ack p50/p95 178/243 ms; propagation p50/p95 381/524 ms                |
| Equivalent TS DO bench                                               | ack p50/p95 165/903 ms; propagation p50/p95 583/1,074 ms              |

Wake fan-out is anchored with `waitUntil` after commit and is not on the push
response's critical path. The acknowledgement result is a local wall-time
proxy, not billed Cloudflare CPU. RSS is the whole local
workerd process, not an isolate allocation; Cloudflare's 128 MiB per-isolate
limit cannot be inferred from that process number. The bundle is 95.8% below
the 3 MiB gzip Free-plan limit (and 98.7% below the 10 MiB paid-plan limit).
See [Cloudflare Worker limits](https://developers.cloudflare.com/workers/platform/limits/).

## Deployment

`wrangler.toml` is checked in with Worker name `orez-rust-sync`, SQLite Durable
Object class `SyncDurableObject`, and account `6afff1f79e2fd12f1cfd1bfe1dfd08d1`.
The deployed test version used for this M3 pass was
`871a13df-7c3b-4e1c-bc90-c3cbd00f2dea` at
`https://orez-rust-sync.lslcf.workers.dev`. The throwaway M0 Worker was already
absent when the cleanup command was run (Cloudflare returned error 10090), so no
old probe service remains to receive traffic.

The rust toolchain is pinned at the workspace root in
[`rust-toolchain.toml`](../../rust-toolchain.toml).
