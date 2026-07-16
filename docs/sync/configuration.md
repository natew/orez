# Configuration reference

Everything an app varies is a single `SyncHostConfig` object passed to
`createSyncWorker` and `createSyncDurableObject`. This page documents every
field, the environment variables the host and data worker read, and the Node
mount surface. Types are in `packages/sync-cf-host/src/types.ts`; validation and
defaults are in `packages/sync-cf-host/src/config.ts` and the DO constructor in
`packages/sync-cf-host/src/host.ts`.

The config is validated once at construction. An invalid combination throws
immediately rather than failing at request time.

## Required fields

| Field                           | Type                                          | Meaning                                                                                                                       |
| ------------------------------- | --------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `hostVersion`                   | `string`                                      | Version string emitted in every structured log line. Set it per deployment so logs are attributable.                          |
| `schema`                        | `ZeroSchemaConfig`                            | Tables, columns (with Zero types), and primary keys. Usually derived from your Zero `createSchema()` result.                  |
| `initialize(sql)`               | `(sql: SyncSql) => void`                      | Application DDL and optional seed. Runs inside the boot transaction, before sync-core initializes its own schema.             |
| `authenticate(request, env)`    | `=> NormalizedClaims \| null \| Promise<...>` | Edge authentication. Returns normalized claims (a stable `userID` plus anything else) or `null` to reject with 401.           |
| `namespace(request)`            | `(request) => string \| null`                 | Resolves the Durable Object partition key. Returning `null` makes the worker answer a plain health string instead of routing. |
| `authorizeWake(request, env)`   | `=> boolean \| Promise<boolean>`              | Authorizes the advisory wake WebSocket before a namespace object is selected. Required and fail-closed.                       |
| `authorizeNotify(request, env)` | `=> boolean \| Promise<boolean>`              | Authorizes upstream change notifications before a namespace object is selected. Required and fail-closed.                     |

`NormalizedClaims` must carry a non-empty `userID`; it owns client-group
ownership. Put the raw client token into claims (for example under a
`__zeroAuthToken` key) if `resolveQuery` needs to forward it to the app.

## Push mode: exactly one of two

A deployment picks one write model. The validator rejects supplying both or
neither.

### Local mutators

| Field      | Type              | Meaning                                                                                                                                                                                                                                                                        |
| ---------- | ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `mutators` | `MutatorRegistry` | Named mutators built with `registerMutators({...})`. Each runs inside the push transaction against a `MutatorSql` adapter. Await only `MutatorSql` calls there; schedule timers, fetches, and other external effects with `context.defer`. Forbidden together with `upstream`. |

### Delegated push

| Field                | Type     | Default              | Meaning                                                                                                                               |
| -------------------- | -------- | -------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `mutateUrl`          | `string` | required (this mode) | Absolute path on the upstream service, for example `/api/zero/push`. Client pushes are forwarded there verbatim. Requires `upstream`. |
| `mutateOrigin`       | `string` | internal binding URL | Exact absolute HTTP(S) origin used to construct delegated push requests. Set it when the target router depends on the request origin. |
| `mutateBinding`      | `string` | `upstream.binding`   | Env key of the service binding used for delegated pushes. Must be non-empty. Only valid with `mutateUrl`.                             |
| `delegatedPushRetry` | object   | see below            | Retry policy for the delegated push subrequest. Only valid with `mutateUrl`.                                                          |

`delegatedPushRetry` fields and defaults: `maxAttempts` 3 (including the first
request), `initialBackoffMs` 100, `maxBackoffMs` 1000, `timeoutMs` 5000 per
attempt. Retries fire only on transport failure, HTTP 429, and HTTP 5xx. Any
other 4xx returns immediately.

## Upstream ingest

Required for delegated push, forbidden with local mutators.

| Field                           | Type                              | Default  | Meaning                                                                                                                             |
| ------------------------------- | --------------------------------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `upstream.binding`              | `string`                          | required | Env key of the DATA service binding that owns the app write endpoint and the change feed.                                           |
| `upstream.namespacePath`        | `string \| (namespace) => string` | required | Path to this namespace on the bound service, for example `/data/<id>`. Must resolve to an absolute path. `/` is a valid root mount. |
| `upstream.changeLimit`          | `number`                          | 1000     | Feed page size. The cursor loop continues until the reported head is reached. Valid range 1 to 10000.                               |
| `upstream.intervalMs`           | `number`                          | 15000    | Durable Object alarm safety net between ingest passes. Minimum 1000.                                                                |
| `upstream.ingestBudgetRows`     | `number`                          | 150000   | Billable SQLite rows ingest may write per rolling window before the breaker trips.                                                  |
| `upstream.ingestBudgetWindowMs` | `number`                          | 300000   | Rolling ingest budget window (five minutes).                                                                                        |
| `upstream.ingestBackoffMs`      | `number`                          | 1000     | Initial breaker cooldown after a trip.                                                                                              |
| `upstream.ingestMaxBackoffMs`   | `number`                          | 60000    | Maximum breaker cooldown.                                                                                                           |

An internal empty path represents the valid `/` root mount. `null` alone means
the upstream path has not been configured or remembered yet.

The ingest budget knobs are the sync host's half of the write safeguards. The
data worker has its own independent budget, configured by environment variables
(below). See the trade-offs page for why both exist.

## Query awareness

Turn these on when clients send named queries and the app owns the
permission transform.

| Field                   | Type                                     | Default                 | Meaning                                                                                                                                                                         |
| ----------------------- | ---------------------------------------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `queryAware`            | `boolean \| (claims) => boolean`         | `Boolean(resolveQuery)` | Enables the desired-query pull path for this namespace.                                                                                                                         |
| `resolveQuery`          | `(name, args, claims, env) => JsonValue` | none                    | Resolves a named query plus args into a validated Zero AST before it reaches the engine. Commonly delegates to the app's real synced-queries endpoint over a service binding.   |
| `queryTransformVersion` | `number \| (claims) => number`           | 0                       | Server-owned invalidation epoch for permission or schema transforms. Must be a non-negative safe integer. Bump it to force recompilation of every client's transformed queries. |

## Visibility (local filtering)

An alternative to query awareness for simpler per-user row filtering, applied
inside the engine rather than delegated to the app.

| Field                              | Type                             | Default | Meaning                                                                                            |
| ---------------------------------- | -------------------------------- | ------- | -------------------------------------------------------------------------------------------------- |
| `visibility.rowLocal`              | `boolean \| (claims) => boolean` | none    | True only when every predicate depends on the selected row alone.                                  |
| `visibility.filter(table, claims)` | `=> {sql, params} \| undefined`  | none    | Returns a SQL `WHERE` fragment (without the keyword) selecting the user's visible rows of a table. |
| `visibilityEnabled`                | `boolean`                        | `false` | Enables visibility from the first request. Defaults off so harnesses start unfiltered.             |

A visibility filter can revoke rows without any row change, which a diff cannot
express, so any config with `visibility` always answers pulls with a full
snapshot.

## Retention, caps, and lifecycle

| Field                          | Type         | Default           | Meaning                                                                                                                       |
| ------------------------------ | ------------ | ----------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `retainChanges`                | `number`     | 4096              | Change-log rows kept below the high watermark. A client whose cookie falls below the pruned floor gets a snapshot.            |
| `caps.maxChangeRows`           | `number`     | 10000             | Upper bound on rows in one pull response. Must be a positive safe integer.                                                    |
| `caps.maxChangeBytes`          | `number`     | 2000000           | Upper bound on bytes in one pull response.                                                                                    |
| `idleTeardownMs`               | `number`     | 5000              | Simulated idle-teardown window. After this much inactivity the DO resets its in-memory boot state, mirroring a real eviction. |
| `wakeCoalesceMs`               | `number`     | 25                | Batching window for the wake fan-out. A storm of writes produces one pull wave instead of one per write.                      |
| `authorizeAdmin(request, env)` | `=> boolean` | `ADMIN_KEY` check | Authorizes `/admin/*` routes. The default requires `env.ADMIN_KEY` set and a matching `x-admin-key` header.                   |

`authorizeWake` and `authorizeNotify` have no permissive default. If an outer
application router applies its own namespace authorization, bypass only
`/wake` and `/notify` there so these inner callbacks can validate their
capabilities. Do not bypass pull, push, or admin routes.

## The environment (`SyncHostEnv`)

The DO namespace binding is required; the admin key is optional.

| Binding     | Type                     | Meaning                                                      |
| ----------- | ------------------------ | ------------------------------------------------------------ |
| `SYNC_DO`   | `DurableObjectNamespace` | The namespace DO class created by `createSyncDurableObject`. |
| `ADMIN_KEY` | `string` (optional)      | Shared secret for the default admin authorization.           |

Delegated deployments also declare service bindings named by
`upstream.binding` and `mutateBinding` (often `DATA` and `APP`). Those are
ordinary Cloudflare `[[services]]` bindings; the host reads them by name off
`env`.

## Data-worker environment variables (write safeguards)

These configure the write budget on the data worker (`src/cf-do/worker.ts`),
not the sync host. They are the source-side defense against a runaway writer.

| Variable                           | Default | Meaning                                                                                                                       |
| ---------------------------------- | ------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `OREZ_DO_WRITE_BUDGET_ROWS`        | 150000  | Billable SQLite rows the data worker may write per rolling window before the circuit trips and mutating endpoints return 429. |
| `OREZ_DO_WRITE_BUDGET_WINDOW_MS`   | 300000  | Rolling window (five minutes).                                                                                                |
| `OREZ_DO_WRITE_BUDGET_ADMIN_TOKEN` | none    | Token that authorizes a manual reopen via `POST /_orez/write-budget/reopen`.                                                  |
| `OREZ_DO_WRITE_BUDGET_DISABLED`    | unset   | Setting `1` or `true` is the only opt-out. It logs a loud `orez_do_write_budget_disabled` error at object construction.       |

Invalid or non-positive values fall back to the defaults. The budget is measured
in Cloudflare billable rows (index and tracking rows included), not application
rows. See the trade-offs page.

## Admin surface

Every route is under `/<namespace>/admin/` and gated by `authorizeAdmin` (or the
`ADMIN_KEY` header). Handlers live in `host.ts`, method `#admin`.

- `GET /admin/health`: liveness.
- `GET /admin/status`: boot id, hibernation count, database size, connected
  wake sockets, writer state, WASM and heap memory, engine state, request
  counters, and `ingestBreaker` status.
- `POST /admin/sql` `{query}`: read a SQL query against the DO storage.
- `POST /admin/invalidate`: bump the epoch so every client re-snapshots.
- `POST /admin/resnapshot`: rebuild derived application tables from bounded
  keyset pages, durably resume an interrupted generation, catch up concurrent
  changes, then atomically swap the staged tables into place. Finalization bumps
  the engine epoch, so every client performs one expected full resync. This
  preserves engine metadata, client mutation IDs, and upstream data.
- `POST /admin/writer` `{enabled}` (also `GET`): disable or enable the writer.
  A disabled writer answers pushes with 503.
- `POST /admin/retention` `{retainChanges}`: override retention at runtime.
- `POST /admin/visibility` `{enabled}` and `POST /admin/query-aware` `{enabled}`:
  flip those modes at runtime. Stored in the control table so an eviction does
  not silently revert them.
- `GET /admin/ingest-breaker` and `POST /admin/ingest-breaker`: read or clear
  the ingest circuit breaker.
- `GET /admin/upstream-write-budget`: proxy the data worker's write-budget
  status through the DATA binding.
- `POST /admin/fault` `{point, kind}` or `{clear:true}`, and
  `POST /admin/drop-next-push-response`, `POST /admin/restart`: fault injection
  and lifecycle controls used by the conformance harness.

## Node mount surface

`src/sync-server/sync-server.ts` is the TypeScript reference implementation of
the same protocol, usable directly as a Node or bun mount. It is the smaller
surface: local mutators and per-user visibility, without query awareness or
upstream ingest.

`createSyncServer(config)` returns `{handlePull, handlePush, watermark,
invalidate}`. Its config:

| Field                         | Type               | Default     | Meaning                                                                                                                                                  |
| ----------------------------- | ------------------ | ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `db`                          | `SyncDb`           | required    | A tiny SQLite handle with `exec`, `all`, and a synchronous `transaction`. bun:sqlite, better-sqlite3, and DO `ctx.storage.sql` all adapt in a few lines. |
| `tables`                      | `SyncTables`       | required    | Column types and primary keys. `tablesFromZeroSchema(schema)` derives this from a Zero schema.                                                           |
| `visible(table, userID)`      | `=> {sql, params}` | whole table | Per-user row visibility.                                                                                                                                 |
| `mutate(tx, name, args, ctx)` | `(...) => void`    | required    | Runs one custom mutation inside the push transaction. Throw `MutationAppError` for an app-level rejection (the last-mutation-id still advances).         |
| `retainChanges`               | `number`           | 4096        | Change-log retention.                                                                                                                                    |

`createSyncServerMount(config)` mounts the pull and push handlers behind one
database-id path segment. Its config is `pathPrefix` (must start with `/`, for
example `/p-` to produce `/p-<projectID>/pull`) and `server(databaseID)`, which
resolves the `SyncServer` for a database only after the caller has authorized
`route.databaseID`. It returns `{match(pathname), handle(route, body,
userID)}`. `match` does routing only; `handle` delegates without translating
bodies, responses, or errors.

## Native replica-file retention

`SyncNativeConfig.retention` controls deletion of per-namespace SQLite replica
files. `RetentionPolicy::default()` and `RetentionPolicy::disabled()` never
evict a namespace worker or delete a file. Deletion is an explicit ownership
claim through `RetentionPolicy::exclusive(...)`:

```rust
retention: RetentionPolicy::exclusive(
    Duration::from_secs(30 * 24 * 60 * 60),
    10 * 1024 * 1024 * 1024,
    Duration::from_secs(10 * 60),
    Duration::from_secs(10 * 60),
),
```

Enable this only for derived replica files owned exclusively by sync-native.
Shared or authoritative SQLite files must keep retention disabled because they
cannot be safely unlinked while another process or connection pool may hold
them open.

## Native host HTTP security

`SyncNativeHost::new(config, data_dir)` generates a fresh 256-bit admin token
for the process and allows no browser origins. An embedding supervisor that
needs the SQL/operator surface constructs `SyncNativeSecurity`, keeps the token
outside browser code and ordinary logs, explicitly adds each trusted HTTP(S)
origin, and calls `SyncNativeHost::new_with_security`.

Every native admin request, including `/admin/health`, supplies the process
token in `x-admin-key`. Admin requests with any `Origin` header are rejected
before route execution, even if their token is valid. Pull, push, and wake
requests with an `Origin` header must match an allowed origin exactly.
Originless native and server-side sync clients remain supported.

The standalone `sync-native` binary reads `SYNC_NATIVE_ADMIN_TOKEN` or accepts
`--admin-token <token>`, plus a repeatable `--allow-origin <origin>`. Prefer the
environment variable so the credential does not appear in the process command
line. Without either token input, the binary generates an unreported
process-local token, which intentionally leaves the admin surface unavailable
to external clients.

### Settling application-owned native pushes

An embedded application may own mutation execution while sharing the same
namespace SQLite file with `sync-native`. After its application transaction
commits, and before it returns the successful push response to the client, the
server must call `POST /<namespace>/admin/settle-push` with the process admin
token and this body:

```json
{
  "push": { "clientGroupID": "...", "pushVersion": 1, "mutations": [] },
  "response": { "pushResponse": { "mutations": [] } },
  "userID": "authenticated-user"
}
```

`push` is the original client request and `response` is the exact application
response. The route rejects missing, extra, reordered, or mismatched mutation
acknowledgements before changing engine state. It then advances each LMID
monotonically in a new namespace transaction. An `alreadyProcessed` recovery
response can catch an existing engine file up to the application store, while
repeating the same settlement is a no-op. Because the application commit runs
first on the serialized shared SQLite connection, its trigger rows precede the
LMID row. A pull cannot observe acknowledgement before effects, and a successful
application response implies the settlement is already pull-visible.
The coherence guarantee costs one serialized local admin request per
application-owned push.

This route is a machine-only part of the native admin surface. Browser requests
remain forbidden even when they carry the admin token.
