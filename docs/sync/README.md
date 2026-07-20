# The orez rust sync server

A Zero server that runs on SQLite. It speaks the Zero protocol to the real
`@rocicorp/zero` client and replaces zero-cache for deployments that would
rather not run Postgres and a long-lived cache process. On Cloudflare it runs
as one Durable Object per namespace, holding both the sync engine and its SQLite
storage in the same object.

The client half of Zero is unchanged. There is no Orez client API or client
fork. What this replaces is the server half:
replicating upstream data, feeding row changes to clients, running custom
mutators, tracking per-client last-mutation-ids, and serving per-query
incremental sync.

## Start here

- **[Architecture](./architecture.md)**: the Rust engine, its WASM wrapper,
  native, Cloudflare, and browser hosts, the two request paths, the change-log
  model, and how it fits the rest of orez.
- **[The delegation model](./delegation.md)**: the central design decision: an
  app either bundles mutators into the host or delegates writes to its own
  endpoint and lets the host replicate from a change feed. This is what makes it
  a drop-in for zero-cache.
- **[Configuration reference](./configuration.md)**: every `SyncHostConfig`
  field, the environment variables, and the Node mount surface.
- **[Testing](./testing.md)**: an honest account of what is tested, how, and
  what is not.
- **[Trade-offs and operational reality](./trade-offs.md)**: Durable Object
  storage versus an external database, single-writer semantics, the write
  amplification billing that caused real incidents, the circuit breakers, and the
  client reset on cutover.
- **[Consumers and integration guide](./consumers.md)**: how Chat (live) and
  Soot (mid-cutover) compose the host, and a distilled guide for a new app.

## The server shape in one screen

At the Orez Lite server boundary, sync work reduces to pull and push, with an
advisory wake channel that asks the client to pull promptly.

- **Pull** compares the client's cookie to the change log's high watermark and
  returns `unchanged`, a diff of touched rows, or a full snapshot. A cookie ahead
  of the watermark gets a 409 and the client rebuilds.
- **Push** is a v51 custom-mutator body. In local-mutator mode the host runs the
  mutator inside the push transaction. In delegation mode the host forwards the
  push to the app's real endpoint and lets upstream ingest deliver the rows.

The cookie is the high watermark of a per-namespace change log fed by SQLite
triggers on every write path. Retention is size-bounded; a client below the floor
gets a snapshot. There is one snapshot recovery path, no CVR, no per-client
server-side view state, and no websocket poke stream. The only durable per-client
state is a last-mutation-id and the client-group to user binding.

## Where the code lives

| Path                             | What it is                                                                 |
| -------------------------------- | -------------------------------------------------------------------------- |
| `crates/sync-core`               | The deterministic Rust sync engine.                                        |
| `crates/sync-wasm`               | The `wasm-bindgen` wrapper that compiles the engine for the DO host.       |
| `crates/sync-native`             | A standalone axum host for the same engine.                                |
| `packages/sync-cf-host`          | The Cloudflare Durable Object host, published as `orez-sync-cf-host`.      |
| `packages/sync-browser-host`     | The Bedrock and IndexedDB browser worker host, exported by Orez.           |
| `packages/sync-executor`         | Shared mutation transactions, CRUD, replay, and effect execution.          |
| `src/zero-http/mount.ts`         | The executor-backed TypeScript pull/push mount.                             |
| `src/cf-do/worker.ts`            | The data worker (`ZeroSqlDO`) that owns writes and serves the change feed. |
| `harness/`                       | The conformance and qualification test harness.                            |

The broader orez project is a local Zero development stack (run Zero on PGlite or
embedded Postgres with no native dependencies, see the repository README). The
rust sync server carries that same SQLite-native approach to production on
Cloudflare.
