# orez/worker: Embeddable orez for CF Workers

## Context

soot's DataDO has PGlite running in CF DO (proven). It uses on-zero for typed
mutations/queries and a FakeZeroCacheServer (zero-shim) for sync. The zero-shim
does full-table dumps — need real incremental sync via zero-cache.

Can't reimplement Zero's sync protocol. Instead: make the real zero-cache run
inside the DO via shim packages that swap its Node.js dependencies for
DO-compatible alternatives.

## Architecture

```
CF Durable Object
├── orez/worker
│   ├── PGlite ×1 (singleDb)
│   ├── ChangeTracker (SQL triggers, existing)
│   ├── ReplicationHandler (pgoutput encoding, existing)
│   │   └── InProcessWriter (replaces TCP socket)
│   └── QueryRouter (replaces TCP pg-proxy)
│
├── zero-cache (SINGLE_PROCESS=1, bundled with shims)
│   ├── postgres → orez/worker/shims/postgres (PGlite-backed)
│   ├── @rocicorp/zero-sqlite3 → orez/worker/shims/sqlite (DO SQLite)
│   └── Fastify → patched to not bind port, handlers called by DO fetch
│
├── on-zero (mutations/queries, unchanged)
└── DO fetch() routes to zero-cache handlers + on-zero
```

## Key findings from exploration

**zero-cache supports this:**

- `SINGLE_PROCESS=1` env var — runs all workers in-process via EventEmitter,
  no child_process.fork(). Entry: `runWorker(null, env)`
- `upstream.type: "custom"` — uses WebSocket to a change source endpoint
  instead of PostgreSQL logical replication. Already exists in zero-cache.

**orez's replication handler is transport-agnostic:**

- `ReplicationWriter` interface is just `{ write(data: Uint8Array): void, closed?: boolean }`
- Replace socket-based writer with callback-based writer = in-process streaming
- Change tracker + pgoutput encoder work without TCP

**What needs shimming (3 packages):**

| Package                  | Current                        | Shim                                                        | Complexity                                                                                                  |
| ------------------------ | ------------------------------ | ----------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `postgres`               | TCP to PostgreSQL              | Tagged template API backed by PGlite in-process             | Medium — need tagged templates, transactions, type config. NO replication (use custom upstream).            |
| `@rocicorp/zero-sqlite3` | Native Node.js SQLite bindings | DO's built-in SQLite (`this.ctx.storage.sql`)               | Medium — Database class, StatementRunner (run/get/all), transactions (begin/beginConcurrent/beginImmediate) |
| `fastify`                | HTTP server on port            | No-bind mode — extract route handlers, call from DO fetch() | Hard — WebSocket upgrade handling, route registration. May need targeted patching.                          |

## Phases

### Phase 1: orez/worker entry point (dev mode)

PGlite + change tracking, importable without Node.js. Useful immediately for
development and as foundation for everything else.

**New files:**

- `src/worker/index.ts` — `createOrezWorker(opts)` entry point
- `src/worker/types.ts` — interfaces

**API:**

```typescript
import { createOrezWorker } from 'orez/worker'

const orez = await createOrezWorker({
  pgliteOptions: { wasmModule, fsBundle, loadDataDir, startParams },
})

orez.query('SELECT ...') // direct PGlite access
orez.exec('CREATE TABLE ...')
orez.pool // pg-pool compat for on-zero
orez.dumpDataDir('gzip') // persistence
orez.close()
```

**Reuses:** `src/replication/change-tracker.ts` (already pure SQL),
`src/mutex.ts` (promise-based), `src/config.ts`

**Validation:** import in browser/vitest, create PGlite, install change
tracking, verify \_orez.\_zero_changes populates on mutations.

### Phase 2: Shim packages

Three independent, testable shim modules. Can develop in parallel.

#### 2a: postgres driver shim

**New file:** `src/worker/shims/postgres.ts`

Implements the `postgres` npm package API surface that zero-cache uses:

```typescript
// What zero-cache does:
const sql = postgres(connectionURI, { types: { bigint, json, ... } })
await sql`SELECT * FROM foo WHERE id = ${id}`
await sql.begin(async (tx) => { await tx`INSERT INTO ...` })
await sql.unsafe('RAW SQL')
```

Our shim wraps PGlite:

```typescript
export function createPostgresShim(pglite: PGlite) {
  function sql(strings: TemplateStringsArray, ...values: any[]) {
    // convert tagged template to parameterized query
    return pglite.query(buildQuery(strings, values))
  }
  sql.begin = async (fn) => pglite.transaction(fn)
  sql.unsafe = (text) => pglite.exec(text)
  // ... type handlers, connection options (mostly no-ops)
  return sql
}
```

Does NOT need replication support — zero-cache's custom upstream handles that.

**Validation:** run zero-cache's own query tests against our shim.

#### 2b: SQLite shim

**New file:** `src/worker/shims/sqlite.ts`

Wraps CF DO's built-in SQLite (`this.ctx.storage.sql`) in zero-sqlite3's API:

```typescript
export class Database {
  constructor(lc: LogContext, path: string) { /* use DO SQLite */ }
  prepare(sql: string): Statement { ... }
  exec(sql: string): void { ... }
  transaction(fn: () => T): T { ... }
}

export class StatementRunner {
  run(sql: string, ...args: unknown[]): RunResult { ... }
  get(sql: string, ...args: unknown[]): any { ... }
  all(sql: string, ...args: unknown[]): any[] { ... }
  begin(): RunResult { ... }
  beginConcurrent(): RunResult { ... }  // map to regular BEGIN
  beginImmediate(): RunResult { ... }   // map to BEGIN IMMEDIATE
  commit(): RunResult { ... }
  rollback(): RunResult { ... }
}
```

**Validation:** run StatementRunner tests against DO SQLite.

#### 2c: Fastify / HTTP adapter

**New file:** `src/worker/shims/http-service.ts`

This is the trickiest shim. Two approaches:

**Option A (patch):** Patch zero-cache's `HttpService.start()` to skip
`fastify.listen()`. Instead, expose `fastify.inject(request)` which Fastify
supports for testing — it processes a request without a TCP connection. DO
fetch() calls `fastify.inject()`.

**Option B (replace):** Monkey-patch the `HttpService` constructor to capture
route registrations. Build a simple router that DO's fetch() dispatches to.
WebSocket upgrades use DO's native WebSocket support.

Recommend **Option A** — Fastify's `inject()` is designed for exactly this.
Less code, less breakage risk.

**Validation:** register routes, call inject(), verify responses match.

### Phase 3: zero-cache integration

Wire the shims together. Run zero-cache's `runWorker()` in-process with
`SINGLE_PROCESS=1`.

**New file:** `src/worker/zero-cache-embed.ts`

```typescript
import { runWorker } from '@rocicorp/zero/out/zero-cache/src/server/main.js'

export async function startZeroCacheEmbed(opts: {
  pglite: PGlite
  doSqlite: SqlStorage // CF DO's built-in SQLite
}) {
  // register shims
  globalThis.__orez_pglite = opts.pglite
  globalThis.__orez_do_sqlite = opts.doSqlite

  // configure env for single-process + custom upstream
  const env = {
    ...process.env,
    SINGLE_PROCESS: '1',
    ZERO_UPSTREAM_DB: 'pglite://in-process', // shim intercepts this
    ZERO_CVR_DB: 'pglite://in-process',
    ZERO_CHANGE_DB: 'pglite://in-process',
    ZERO_REPLICA_FILE: ':do-sqlite:', // shim intercepts this
    ZERO_PORT: '0', // don't bind
  }

  await runWorker(null, env)
}
```

**Bundler config** (esbuild/wrangler resolve aliases):

```javascript
alias: {
  'postgres': './src/worker/shims/postgres.js',
  '@rocicorp/zero-sqlite3': './src/worker/shims/sqlite.js',
}
```

**Validation:** Zero client connects, receives pokes, mutations sync.

### Phase 4: In-process change streaming

Connect orez's change tracker → replication handler → zero-cache's change
source, all in-process. No TCP.

**Modified:** `src/replication/handler.ts` — add `InProcessWriter` that
implements `ReplicationWriter` with a callback instead of socket.write()

```typescript
// already exists:
interface ReplicationWriter {
  write(data: Uint8Array): void
  readonly closed?: boolean
}

// new:
class InProcessWriter implements ReplicationWriter {
  constructor(private onData: (data: Uint8Array) => void) {}
  write(data: Uint8Array) {
    this.onData(data)
  }
  get closed() {
    return false
  }
}
```

**Two options for zero-cache consumption:**

**Option A:** Use `upstream.type: "custom"` — zero-cache connects via WebSocket
to a change source endpoint. orez implements this endpoint in-process (no
actual WebSocket, just message passing). Change tracker → replication handler
→ pgoutput → custom change source protocol adapter → zero-cache.

**Option B:** Use `upstream.type: "pg"` with our postgres shim — the shim
intercepts the replication connection request and routes to orez's replication
handler directly. Zero-cache thinks it's talking to PostgreSQL.

Recommend **Option B** — less protocol translation, zero-cache's pg change
source is battle-tested, and we already have the pgoutput encoder.

**Validation:** insert row in PGlite → change tracker fires → replication
handler encodes → zero-cache receives → client gets poke.

### Phase 5: soot integration

Replace zero-shim with real zero-cache via orez/worker in DataDO.

**Modified:** `soot/src/worker/DataDO.ts`
**Deleted:** `soot/src/worker/zero-shim.ts`, `soot/src/worker/fake-websocket.ts`

```typescript
import { createOrezWorker } from 'orez/worker'

export class DataDO extends DurableObject {
  async fetch(request: Request) {
    await this.ensureInitialized()
    // zero-cache handles sync protocol via DO WebSocket
    // on-zero handles push/pull
    // orez handles PGlite + change tracking + replication
  }
}
```

**Validation:** deploy to CF, Zero client syncs with real incremental updates.

### Phase 6: WAL optimization (independent)

Rebuild PGlite WASM with `--wal-segsize=1`. PGDATA: 22.7MB → ~7.6MB.

## Critical files

**orez — existing (verify portability):**

- `src/replication/change-tracker.ts` — pure SQL, portable
- `src/replication/handler.ts` — ReplicationWriter interface, pgoutput encoding
- `src/replication/pgoutput-encoder.ts` — binary message encoding
- `src/mutex.ts` — promise-based, portable
- `src/config.ts` — portable

**orez — new:**

- `src/worker/index.ts` — createOrezWorker()
- `src/worker/types.ts` — interfaces
- `src/worker/shims/postgres.ts` — PGlite-backed postgres driver
- `src/worker/shims/sqlite.ts` — DO SQLite-backed zero-sqlite3
- `src/worker/shims/http-service.ts` — Fastify adapter
- `src/worker/zero-cache-embed.ts` — single-process zero-cache launcher

**soot — modified:**

- `src/worker/DataDO.ts` — use orez/worker

**soot — deleted:**

- `src/worker/zero-shim.ts` — replaced by real zero-cache
- `src/worker/fake-websocket.ts` — no longer needed

## Risks

1. **postgres driver shim fidelity** — zero-cache uses tagged templates with
   custom type serialization (bigint, json, timestamp). Must match exactly.
   Mitigation: run zero-cache's own tests against our shim.

2. **Fastify in no-bind mode** — using inject() for request handling is
   untested in production. WebSocket upgrades via inject() may not work.
   Mitigation: Option B (replace HttpService) as fallback.

3. **Memory pressure** — PGlite (~53MB) + zero-cache runtime (~10MB) + DO
   overhead. With WAL fix: ~38MB PGlite → plenty of headroom.

4. **zero-cache version coupling** — shims must match the postgres/sqlite API
   surface zero-cache uses. Mitigation: pin zero version, test on upgrade.

## Memory budget (CF DO, 128MB)

```
PGlite ×1 (singleDb)              ~53MB (→38MB with WAL fix)
zero-cache (single-process)        ~10MB
DO SQLite (CVR/CDB)                ~2MB (built-in, minimal overhead)
on-zero + shims                    ~3MB
JS runtime                         ~10MB
                                   ─────
                                   ~78MB → 50MB headroom
                                   (→63MB with WAL fix → 65MB headroom)
```

## Order of attack

Phase 1 → Phase 2a/2b/2c (parallel) → Phase 3 → Phase 4 → Phase 5

Phase 1 delivers "dev mode" immediately. Phases 2-4 deliver real sync.
Phase 6 (WAL) is independent, do anytime for memory savings.
