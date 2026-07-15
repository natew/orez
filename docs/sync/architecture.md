# Architecture

The orez rust sync server is a Zero sync server that runs on SQLite. It speaks
Zero's protocol-v51 `http-pull` dialect to stock `@rocicorp/zero` clients, and
it replaces zero-cache for deployments where you would rather not run Postgres
and a long-lived cache process. On Cloudflare it runs as one Durable Object per
namespace, holding both the sync engine and its SQLite storage inside the same
object.

The client half of Zero is unchanged. The ZQL query engine, the local store,
and optimistic mutations stay exactly as Rocicorp ships them. What orez replaces
is the server half: replicating upstream data, feeding row changes to clients,
running custom mutators, tracking per-client last-mutation-ids, and serving
per-query incremental sync.

## The pieces

The deterministic engine and its WASM wrapper feed three hosts: native,
Cloudflare, and a browser worker. A Node implementation remains beside them as
the executable reference and local mount surface.

### crates/sync-core

The deterministic sync engine, written in Rust. It is a port of the executable
TypeScript spec at `src/sync-server/sync-server.ts`, and the two are kept in
lockstep by differential tests (see the testing page). It has no async, no
network, and no filesystem access, which is what lets it compile to WASM and run
inside a Durable Object.

The engine owns a small set of internal SQLite tables and the protocol logic
over them. Its modules (`crates/sync-core/src/`):

- `pull.rs`: cursor-diff pulls, snapshot fallback, retention/floor, epoch
  invalidation (`handle_pull`, `watermark`, `prune`, `invalidate`).
- `push.rs`: v51 custom-mutator pushes as a set of step functions the host
  drives around its own (possibly async) mutator: `push_validate`, `preflight`,
  `finalize`, `record_app_error`, `assemble_push_response`. A synchronous
  `handle_push` composes those steps for the native host and tests.
- `query/`: the query-aware pull path: `ast.rs`, `compile.rs` (Zero AST to
  SQL), `membership.rs`, `qpull.rs`.
- `upstream.rs`: `apply_upstream` and `apply_upstream_snapshot`, which ingest
  rows from an upstream change feed and advance the engine's change log exactly
  as a push does.
- `schema.rs`: table specs and the change-tracking trigger DDL.
- `value.rs`, `wire.rs`, `db.rs`, `store.rs`, `error.rs`: value coercion to
  Zero column types, the counter wire format, the tiny SQLite trait the host
  implements, and errors.

Three boundary rules are pinned in `crates/sync-core/src/lib.rs` and must not be
relaxed:

1. The host owns transaction entry and exit. The engine never emits
   `BEGIN`/`COMMIT`/`SAVEPOINT` because Durable Object SQL rejects them. Every
   entry point documents the transaction the host must have open around it.
2. Positional `?` bindings only. DO `SqlStorage` has no `?N` numbered bindings.
3. Patch values come from live rows read inside the pull transaction, never
   from logged row images. SQLite's `json_object` rounds `REAL` to 15
   significant figures, so a float column round-tripped through the change log
   would corrupt. The log stores touched primary keys only; the diff re-reads
   the live row.

Counters (watermarks, cookies, last-mutation-ids) are `i64` end to end, read
with `CAST(x AS TEXT)` so a value never passes through a float.

### crates/sync-wasm

The `wasm-bindgen` wrapper around sync-core (`crates/sync-wasm/src/lib.rs`). It
exposes the engine as a flat set of `engine_*` functions the host calls across
the WASM boundary: `engine_init_schema`, `engine_handle_pull`,
`engine_handle_query_pull`, `engine_push_validate`, `engine_preflight`,
`engine_finalize`, `engine_assemble_push_response`, `engine_apply_upstream`,
`engine_apply_upstream_snapshot`, `engine_prune`, `engine_invalidate`,
`engine_compile_query`, `engine_state`, `engine_version`, `engine_memory_bytes`,
and a few more. It also declares `JsSyncDb`, the SQLite handle the host passes
in, so every SQL call goes back out to the host's storage.

`wasm-pack build crates/sync-wasm --target web` produces the JavaScript glue and
`.wasm`. The Cloudflare and browser host builds each package that output with
their host code.

### crates/sync-native

A standalone axum HTTP host for the same engine
(`crates/sync-native/src/main.rs` and friends: `engine.rs`, `namespace.rs`,
`wake.rs`, `seed.rs`, `fault.rs`, `obs.rs`). It serves the same pull/push
surface over native Rust with `rusqlite` storage instead of a Durable Object.
By default every process generates a 256-bit admin token. `/<namespace>/admin/*` and
`/admin/health` require it in `x-admin-key`, and browser-origin admin requests
are rejected even when the token is correct. Browser pull, push, and wake
traffic must match an origin explicitly allowed by the embedding process;
originless native/server clients remain supported.
It exists to keep one engine story across deployments. It is not on the
Cloudflare cutover path, so it trails the CF host on the newer modes (upstream
ingest parity is described as a follow-up in
`plans/rust-sync-upstream-ingest.md`).

### packages/sync-cf-host

The Cloudflare Durable Object host, published to npm as `orez-sync-cf-host`.
This is the layer real apps consume. It is TypeScript plus the vendored WASM
engine, and it exports two factories from `src/index.ts`:

- `createSyncWorker(config)`: the consumer-facing Worker router. It runs
  authentication at the edge, resolves the namespace, and forwards the request
  to the right Durable Object with normalized claims on a binding-private
  header.
- `createSyncDurableObject(config)`: the namespace Durable Object class. One
  instance per namespace (`env.SYNC_DO.idFromName(namespace)`), holding the
  engine and its SQLite storage. It handles `/pull`, `/push`, the `/wake`
  advisory socket, `/notify`, and an `/admin/*` surface.

Everything an app varies (schema, auth, namespace resolution, query transforms,
mutators or push delegation, upstream ingest) is passed in as one
`SyncHostConfig` object. That object is the entire public API; the configuration
page documents every field.

The `src/sync-server/sync-server.ts` file, despite living under `src/`, is not a
runtime dependency of the CF host. It is the TypeScript reference implementation
of the same protocol, and it is also a usable Node/bun mount (`createSyncServer`
and `createSyncServerMount`). The CF host runs the Rust port of it, compiled to
WASM.

### packages/sync-browser-host

The browser host is exported as `orez/sync-browser-host`. It loads sync-wasm
and Bedrock SQLite in one worker and serves the same authenticated `/pull` and
`/push` protocol through a `MessagePort`. The same port also exposes serialized
direct `query` and `exec` calls for preview tooling. All attached clients receive
an advisory `data-changed` event after a durable mutation or direct write.

One operation queue owns the database. A mutation runs its generated application
code and Rust last-mutation-id finalization inside one explicit SQLite
transaction. The host then snapshots Bedrock's complete in-memory VFS into one
versioned IndexedDB record and waits for that transaction before replying. If
the IndexedDB commit fails, the live database is closed and the host rejects all
later work. A new worker restores the previous complete snapshot before opening
SQLite. There is no in-memory fallback.

The host accepts a mutator registry and named-query resolver so an on-zero
adapter can preserve generated validators, permissions, transaction helpers,
and deferred effects without running on-zero's separate mutation bookkeeping.
The root Orez package includes both WASM binaries beside the subpath export, so
ordinary worker bundles do not need consumer-specific asset copies.

## The two request paths

At runtime a client does exactly two things against the host: pull and push.

**Pull** (`POST /<namespace>/pull`). The worker authenticates the request,
attaches normalized claims, and forwards to the DO. The DO opens one synchronous
SQLite transaction and calls the engine. The engine compares the client's cookie
to the change log's high watermark:

- cookie equals watermark: `{cookie, unchanged: true}`.
- cookie below the retained floor, or a per-user visibility filter is active, or
  the client is fresh: a full snapshot, `[{op:'clear'}, ...puts]`.
- cookie within the retained window: a diff, the put/del rows touched since the
  cookie, resolved against live table state.
- cookie ahead of the watermark: HTTP 409, and the client rebuilds its local
  store from scratch.

**Push** (`POST /<namespace>/push`). A v51 custom-mutator body. In local-mutator
mode the DO runs the app's mutator inside the push transaction, advances the
last-mutation-id, and appends a change-log marker so peers' pulls stop reporting
`unchanged`. In delegation mode the DO forwards the push to the app's real
endpoint and lets upstream ingest deliver the rows. Both modes are covered on
the delegation page.

## The change log

The cookie is the high watermark of a per-namespace change log
(`_zsync_changes` in the engine, `_zero_changes` in the data worker). SQLite
triggers installed per table append to the log on every write path, whether the
write came from a mutator, from admin SQL, or from upstream ingest. The log
stores which primary keys were touched, never row values. Retention is
size-bounded: once the log passes `retainChanges` rows, the oldest entries are
pruned and the floor rises. A client whose cookie has fallen below the floor
gets a snapshot on its next pull.

This is the whole recovery model. There is one snapshot path, used for fresh
clients, below-floor cookies, visibility-filtered configs, and epoch
invalidation. There is no CVR, no per-client server-side view state, and no
websocket poke stream. The only durable per-client state is the clients table
(`_zsync_clients`): a last-mutation-id and the client-group to user binding.

## How it fits the rest of orez

The broader orez project is a local Zero development stack (run Zero on PGlite or
embedded Postgres with no native dependencies, see the repo README). The rust
sync server is the piece that carries that same SQLite-native approach to
production on Cloudflare.

On Cloudflare the deployment has two Durable Object roles:

- The **data worker** (`src/cf-do/worker.ts`, the `ZeroSqlDO` class) is the
  write side. Apps write to it over the Postgres wire protocol through a
  DoBackend, and it stores rows in DO SQLite. It exposes a
  watermark-cursored change feed the sync host consumes:
  `GET/POST /<db>/changes {watermark, limit}` returns `{watermark, changes}` and
  answers HTTP 410 `watermarkTooOld` when the cursor precedes the retained
  floor; `GET /<db>/snapshot` returns every tracked table; `POST /<db>/notify`
  wakes ingest. Authoritative row capture is in `src/cf-do/cdc.ts`: generated
  SQLite triggers stage full before/after images in the same statement as the
  row write, including indirect writes from business triggers. Explicit
  transactions are grouped and promoted only at commit; `src/cf-do/row-undo.ts`
  uses the same images for rollback and crash recovery. This is logical CDC,
  not WAL/page copying. `src/do-sql-tracking.ts` meters billable writes, while
  `src/change-tracking.ts` is the separate Postgres-compatible replication
  entrypoint.
- The **sync host** (`packages/sync-cf-host`) is the read side. It ingests the
  data worker's feed into the engine's own change log, then serves pulls and
  pushes. In delegation mode it replaces the embedded zero-cache DO
  one-for-one, and the app's Postgres-wire writes to the data worker are
  unchanged.

The `src/zero-http/` transport is the client side of the `http-pull` dialect
(the on-zero fake-WebSocket transport that turns sync into stateless HTTP
polls). The sync host answers exactly what that transport expects.
