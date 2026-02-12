# orez

[Zero](https://zero.rocicorp.dev) is powerful, but running it locally means Postgres with native dependencies, native SQLite, and often just using Docker. orez is an experiment at making Postgres and Zero work with no setup or native dependencies at all — a drop-in [PGlite](https://pglite.dev) backend with logical replication, native-free WASM SQLite, and auto-configuration. CLI, programmatic API, and Vite plugin included.

```
bunx orez
```

**What orez handles automatically:**

- **Memory management** — auto-sizes Node heap based on system RAM, purges consumed WAL, batches restores with CHECKPOINTs to prevent WASM OOM
- **Fast replication** — adaptive polling (20ms when catching up, 500ms idle — ~500x faster after large restores), auto-tracks tables created at runtime
- **Auto-recovery** — resets on replication errors, finds available ports if configured ones are busy, cleans stale locks while preserving cache
- **PGlite compatibility** — rewrites unsupported queries, fakes wire protocol responses, filters unsupported column types, cleans session state between connections
- **Production restores** — `pg_dump`/`pg_restore` with COPY→INSERT conversion, skips unsupported extensions, handles oversized rows, recovers from non-fatal errors
- **Zero-cache workarounds** — fixes concurrent COPY bug, disables query planner (WASM infinite loop), respects publication filtering
- **Extensions** — pgvector and pg_trgm enabled by default

## CLI

```
bunx orez
```

```
--pg-port=6434            postgresql proxy port
--zero-port=5849          zero-cache port
--data-dir=.orez          data directory
--migrations=DIR          migrations directory (skipped if not set)
--seed=FILE               seed file path
--pg-user=user            postgresql user
--pg-password=password    postgresql password
--skip-zero-cache         run pglite + proxy only, skip zero-cache
--log-level=warn          error, warn, info, debug
--s3                      also start a local s3-compatible server
--s3-port=9200            s3 server port
--disable-wasm-sqlite     use native @rocicorp/zero-sqlite3 instead of wasm bedrock-sqlite
--on-db-ready=CMD         command to run after db+proxy are ready, before zero-cache starts
--on-healthy=CMD          command to run once all services are healthy
```

Ports auto-increment if already in use.

## Programmatic

```
bun install orez
```

```typescript
import { startZeroLite } from 'orez'

const { config, stop, db, instances } = await startZeroLite({
  pgPort: 6434,
  zeroPort: 5849,
  migrationsDir: 'src/database/migrations',
  seedFile: 'src/database/seed.sql',
})

// your app connects to zero-cache at localhost:5849
// database is at postgresql://user:password@localhost:6434/postgres

// db is the postgres PGlite instance (for direct queries)
// instances has all three: { postgres, cvr, cdb }

// when done
await stop()
```

All options are optional with sensible defaults. Ports auto-find if in use.

### Lifecycle hooks

| Hook        | CLI                 | Programmatic                   | When                                                            |
| ----------- | ------------------- | ------------------------------ | --------------------------------------------------------------- |
| on-db-ready | `--on-db-ready=CMD` | `onDbReady: 'CMD'`             | after db + proxy are ready, before zero-cache                   |
| before-zero | —                   | `beforeZero: async (db) => {}` | after on-db-ready, before zero-cache (receives PGlite instance) |
| on-healthy  | `--on-healthy=CMD`  | —                              | after all services are healthy                                  |

CLI hooks receive env vars: `DATABASE_URL`, `OREZ_PG_PORT`, `OREZ_ZERO_PORT`. Change tracking triggers are automatically re-installed after `onDbReady` and `beforeZero` run, so tables created by those hooks are tracked without extra setup.

## Vite plugin

```typescript
import { orezPlugin } from 'orez/vite'

export default {
  plugins: [
    orezPlugin({
      pgPort: 6434,
      zeroPort: 5849,
      migrationsDir: 'src/database/migrations',
    }),
  ],
}
```

Starts orez when vite dev server starts, stops on close.

## How it works

orez starts three things:

1. Three PGlite instances (full PostgreSQL 16 running in-process via WASM) — one for each database zero-cache expects (upstream, CVR, change)
2. A TCP proxy that speaks the PostgreSQL wire protocol, routing connections to the correct PGlite instance and handling logical replication
3. A zero-cache child process that connects to the proxy thinking it's a real Postgres server

### Multi-instance architecture

zero-cache expects three separate databases: `postgres` (app data), `zero_cvr` (client view records), and `zero_cdb` (change-streamer state). In real PostgreSQL these are independent databases with separate connection pools and transaction contexts.

orez creates a separate PGlite instance for each database, each with its own data directory and mutex. This is critical because PGlite is single-session — all proxy connections to the same instance share one session. Without isolation, transactions on the CVR database get corrupted by queries on the postgres database (zero-cache's view-syncer detects this as `ConcurrentModificationException` and crashes). Separate instances eliminate cross-database interference entirely.

The proxy routes connections based on the database name in the startup message:

| Connection database | PGlite instance | Data directory    |
| ------------------- | --------------- | ----------------- |
| `postgres`          | postgres        | `pgdata-postgres` |
| `zero_cvr`          | cvr             | `pgdata-cvr`      |
| `zero_cdb`          | cdb             | `pgdata-cdb`      |

Each instance has its own mutex for serializing queries. Extensions (pgvector, pg_trgm) and app migrations only run on the postgres instance.

### Replication

zero-cache needs logical replication to stay in sync with the upstream database. PGlite doesn't support logical replication natively, so orez fakes it. Every mutation is captured by triggers into a changes table, then encoded into the pgoutput binary protocol and streamed to zero-cache through the replication connection. zero-cache can't tell the difference.

Replication polling is adaptive — 20ms intervals when catching up to pending changes, 500ms when idle — with a batch size of 2000 changes per poll. After a large `pg_restore` (40K+ rows), this catches up in seconds instead of minutes. Consumed changes are purged every 10 poll cycles to prevent the `_zero_changes` table from growing unbounded and triggering WASM out-of-memory.

Tables created at runtime (e.g., zero-cache's shard schema tables like `chat_0.clients` and `chat_0.mutations`) are automatically detected via a DDL event trigger and enrolled in change tracking without a restart.

The replication handler also tracks shard schema tables so that `.server` promises on zero mutations resolve correctly.

### Zero native dependencies

The whole point of orez is that `bunx orez` works everywhere with no native compilation step. Postgres runs in-process as WASM via PGlite. zero-cache also needs SQLite, and `@rocicorp/zero-sqlite3` ships as a compiled C addon — so orez ships [bedrock-sqlite](https://www.npmjs.com/package/bedrock-sqlite), SQLite's [bedrock branch](https://sqlite.org/src/timeline?t=begin-concurrent) recompiled to WASM with BEGIN CONCURRENT and WAL2 support. At startup, orez patches `@rocicorp/zero-sqlite3` to load bedrock-sqlite instead of the native C addon. Both databases run as WASM — nothing to compile, nothing platform-specific. Just `bun install` and go.

### Auto heap sizing

The CLI detects system memory on startup and re-spawns the process with `--max-old-space-size` set to ~50% of available RAM (minimum 4GB). PGlite WASM needs substantial heap for large datasets and restores — this prevents cryptic V8 OOM crashes without requiring manual tuning.

## Environment variables

Your entire environment is forwarded to the zero-cache child process. This means any `ZERO_*` env vars you set are passed through automatically.

orez provides sensible defaults for a few variables:

| Variable                               | Default             | Overridable |
| -------------------------------------- | ------------------- | ----------- |
| `NODE_ENV`                             | `development`       | yes         |
| `ZERO_LOG_LEVEL`                       | from `--log-level`  | yes         |
| `ZERO_NUM_SYNC_WORKERS`                | `1`                 | yes         |
| `ZERO_ENABLE_QUERY_PLANNER`            | `false`             | yes         |
| `ZERO_INITIAL_SYNC_TABLE_COPY_WORKERS` | `999`               | yes         |
| `ZERO_AUTO_RESET`                      | `true`              | yes         |
| `ZERO_UPSTREAM_DB`                     | _(managed by orez)_ | no          |
| `ZERO_CVR_DB`                          | _(managed by orez)_ | no          |
| `ZERO_CHANGE_DB`                       | _(managed by orez)_ | no          |
| `ZERO_REPLICA_FILE`                    | _(managed by orez)_ | no          |
| `ZERO_PORT`                            | _(managed by orez)_ | no          |

The `--log-level` flag controls both zero-cache (`ZERO_LOG_LEVEL`) and PGlite's debug output. Default is `warn` to keep output quiet. Set to `info` or `debug` for troubleshooting.

`ZERO_INITIAL_SYNC_TABLE_COPY_WORKERS` is set high to work around a postgres.js bug where concurrent COPY TO STDOUT on reused connections hangs. This gives each table its own connection during initial sync. `ZERO_AUTO_RESET` lets zero-cache recover from replication errors (e.g. after `pg_restore`) by wiping and resyncing instead of crashing. `ZERO_ENABLE_QUERY_PLANNER` is disabled because it causes freezes with both WASM and native SQLite.

The layering is: orez defaults → your env → orez-managed connection vars. So setting `ZERO_LOG_LEVEL=debug` in your shell overrides the `--log-level` default, but you can't override the database connection strings (orez needs to point zero-cache at its own proxy).

Common vars you might want to set:

```bash
ZERO_MUTATE_URL=http://localhost:3000/api/zero/push
ZERO_QUERY_URL=http://localhost:3000/api/zero/pull
```

## What gets faked

The proxy intercepts several things to convince zero-cache it's talking to a real PostgreSQL server with logical replication enabled:

- `IDENTIFY_SYSTEM` returns a fake system ID and timeline
- `CREATE_REPLICATION_SLOT` persists slot info in a local table and returns a valid LSN
- `START_REPLICATION` enters streaming mode, encoding changes as pgoutput binary messages
- `version()` returns a standard PostgreSQL 16.4 version string (PGlite's Emscripten string breaks `pg_restore` and other tools)
- `current_setting('wal_level')` always returns `logical`
- `pg_replication_slots` queries are redirected to a local tracking table
- `SET TRANSACTION SNAPSHOT` is silently accepted (PGlite doesn't support imported snapshots)
- `ALTER ROLE ... REPLICATION` returns success
- `READ ONLY` is stripped from transaction starts (PGlite is single-session)
- `ISOLATION LEVEL` is stripped from all queries (meaningless with a single-session database)
- `SET TRANSACTION` / `SET SESSION` return synthetic success without hitting PGlite

The pgoutput encoder produces spec-compliant binary messages: Begin, Relation, Insert, Update, Delete, Commit, and Keepalive. Column values are encoded as text (typeOid 25) except booleans which use typeOid 16 with `t`/`f` encoding, matching PostgreSQL's native boolean wire format.

## Workarounds

A lot of things don't "just work" when you replace Postgres with PGlite and native SQLite with WASM. Here's what orez does to make it seamless.

### TCP proxy: raw wire protocol instead of pg-gateway

The proxy implements the PostgreSQL wire protocol from scratch using raw TCP sockets. pg-gateway uses `Duplex.toWeb()` which deadlocks under concurrent connections with large responses. Raw `net.Socket` with manual message framing avoids this entirely.

### Session state bleed between connections

PGlite is single-session — all proxy connections share one session. If `pg_restore` sets `search_path = ''`, every subsequent connection inherits that. On disconnect, orez resets `search_path`, `statement_timeout`, `lock_timeout`, and `idle_in_transaction_session_timeout`, and rolls back any open transaction. Without this, the next connection gets a corrupted session.

### Event loop starvation from mutex chains

The mutex uses `setImmediate`/`setTimeout` between releases instead of resolving the next waiter as a microtask. Without this, releasing the mutex triggers a chain of synchronous PGlite executions that blocks all socket I/O — connections stall because reads and writes can't be processed between queries.

### PGlite errors don't kill connections

When `execProtocolRaw` throws (PGlite internal error), the proxy sends a proper ErrorResponse + ReadyForQuery over the wire instead of destroying the socket. The client sees an error message and continues working.

### SQLite shim via ESM loader hooks

zero-cache imports `@rocicorp/zero-sqlite3` (a native C addon) via ESM `import`. orez uses Node's `module.register()` API with `--import` to intercept resolution — ESM `resolve` and `load` hooks redirect `@rocicorp/zero-sqlite3` to bedrock-sqlite WASM at runtime. The hook templates live in `src/shim/` and are written to tmpdir with the resolved bedrock-sqlite path substituted.

The shim also polyfills the better-sqlite3 API surface zero-cache expects: `unsafeMode()`, `defaultSafeIntegers()`, `serialize()`, `backup()`, and `scanStatus`/`scanStatusV2`/`scanStatusReset` on Statement prototypes (zero-cache's query planner calls these for scan statistics, which WASM doesn't support).

### Query planner disabled

`ZERO_ENABLE_QUERY_PLANNER` is set to `false` because it relies on SQLite scan statistics that trigger infinite loops in WASM sqlite (and have caused freezes with native sqlite too). The planner is an optimization, not required for correctness.

### postgres.js COPY hang workaround

`ZERO_INITIAL_SYNC_TABLE_COPY_WORKERS` is set to `999` to work around a postgres.js bug where concurrent `COPY TO STDOUT` on a reused connection causes `.readable()` to hang indefinitely. This gives each table its own connection during initial sync.

### Type OIDs in RELATION messages

Replication RELATION messages carry correct PostgreSQL type OIDs (not just text/25) so zero-cache selects the right value parsers. For example, `timestamp with time zone` gets OID 1184, which triggers `timestampToFpMillis` conversion. Without this, zero-cache misinterprets column types.

### Unsupported column exclusion

Columns with types zero-cache can't handle (`tsvector`, `tsquery`, `USER-DEFINED`) are filtered out of replication messages. Without exclusion, zero-cache crashes on the unknown types. The columns are removed from both new and old row data.

### Publication-aware change tracking

If `ZERO_APP_PUBLICATIONS` is set, only tables in that publication get change-tracking triggers. This prevents streaming changes for private tables (user sessions, accounts) that zero-cache doesn't know about. Stale triggers from previous installs (before the publication existed) are cleaned up automatically.

### Stale lock file cleanup on startup

Only the SQLite replica's lock files (`-wal`, `-shm`, `-wal2`) are deleted on startup — not the replica itself. The replica is a cache of PGlite data; keeping it lets zero-cache catch up via replication (nearly instant) instead of doing a full initial sync (COPY of all tables). If the replica is too stale, `ZERO_AUTO_RESET=true` makes zero-cache wipe and resync automatically. Lock files from a previous crash are cleaned to prevent startup failures.

### Data directory migration

Existing installs that used a single PGlite instance (`pgdata/`) are auto-migrated to the multi-instance layout (`pgdata-postgres/`) on first run. No manual intervention needed.

### Restore: dollar-quoting and statement boundaries

The restore parser tracks `$$` and `$tag$` blocks to correctly identify statement boundaries in function bodies. Without this, semicolons inside `CREATE FUNCTION` bodies are misinterpreted as statement terminators.

### Restore: broken trigger cleanup

After restore, orez drops triggers whose backing functions don't exist. This happens when a filtered `pg_dump` includes triggers on public-schema tables that reference functions from excluded schemas. The triggers survive TOC filtering because they're associated with public tables, but the functions they reference weren't included.

### Restore: wire protocol auto-detection

`pg_restore` tries connecting via wire protocol first (for restoring into a running orez instance). If the connection fails, it falls back to direct PGlite access. But if the connection succeeds and the restore itself fails, it does _not_ fall back — the error is real and should be reported, not masked by a retry.

### Callback-based message loop

The proxy uses callback-based `socket.on('data')` events instead of async iterators for the message loop. Async iterators have unreliable behavior across runtimes (Node.js vs Bun). The callback approach with manual pause/resume works everywhere.

## Tests

203 tests across 29 test files covering the full stack from binary encoding to TCP-level integration, including pg_restore end-to-end tests and bedrock-sqlite WASM engine tests:

```
bun run test                                # orez tests
cd sqlite-wasm && bunx vitest run           # bedrock-sqlite tests
```

The orez test suite includes a zero-cache compatibility layer that decodes pgoutput messages into the same typed format that zero-cache's PgoutputParser produces, validating end-to-end compatibility.

The bedrock-sqlite tests cover Database/Statement API, transactions, WAL/WAL2 modes, BEGIN CONCURRENT, FTS5, JSON functions, custom functions, aggregates, bigint handling, and file persistence.

## Limitations

This is a development tool. It is not suitable for production use.

- PGlite is single-session per instance. All queries to the same database are serialized through a mutex. Cross-database queries are independent (each database has its own PGlite instance and mutex). Fine for development but would bottleneck under real load.
- Triggers add overhead to every write. Again, fine for development.
- PGlite stores data on the local filesystem. No replication, no high availability. Use `orez pg_dump` / `orez pg_restore` for backups.

## Project structure

```
src/
  cli-entry.ts          thin wrapper for auto heap sizing
  cli.ts                cli with citty
  index.ts              main entry, orchestrates startup + sqlite wasm patching
  config.ts             configuration with defaults
  log.ts                colored log prefixes
  mutex.ts              simple mutex for serializing pglite access
  port.ts               auto port finding
  pg-proxy.ts           raw tcp proxy implementing postgresql wire protocol
  pglite-manager.ts     multi-instance pglite creation and migration runner
  s3-local.ts           local s3-compatible server (orez/s3)
  vite-plugin.ts        vite dev server plugin (orez/vite)
  replication/
    handler.ts          replication protocol state machine + adaptive polling
    pgoutput-encoder.ts binary pgoutput message encoder
    change-tracker.ts   trigger installation, DDL event triggers, change purging
  integration/
    integration.test.ts end-to-end zero-cache sync test
    restore.test.ts     pg_dump/restore integration test
sqlite-wasm/
  Makefile              emscripten build for bedrock-sqlite wasm binary
  bedrock-sqlite.d.ts   typescript declarations
  native/
    api.js              better-sqlite3 compatible database/statement API
    vfs.c               custom VFS with SHM support for WAL/WAL2
    vfs.js              javascript VFS bridge
  test/
    database.test.ts    wasm sqlite engine tests
```

## Backup & Restore

Dump and restore your local PGlite database using WASM-compiled `pg_dump` — no native Postgres install needed.

```
bunx orez pg_dump > backup.sql
bunx orez pg_dump --output backup.sql
bunx orez pg_restore backup.sql
bunx orez pg_restore backup.sql --clean
```

```
pg_dump options:
  --data-dir=.orez    data directory
  -o, --output        output file path (default: stdout)

pg_restore options:
  --data-dir=.orez    data directory
  --clean             drop and recreate public schema before restoring
```

`pg_restore` also supports connecting to a running orez instance via wire protocol — just pass `--pg-port`:

```
bunx orez pg_restore backup.sql --pg-port 6434
bunx orez pg_restore backup.sql --pg-port 6434 --pg-user user --pg-password password
bunx orez pg_restore backup.sql --direct   # force direct PGlite access, skip wire protocol
```

Restore streams the dump file line-by-line so it can handle large dumps without loading everything into memory. SQL is parsed using [pgsql-parser](https://www.npmjs.com/package/pgsql-parser) (the real PostgreSQL C parser compiled to WASM) for accurate statement classification and rewriting.

### What restore handles automatically

- **COPY FROM stdin → INSERT**: PGlite WASM doesn't support the COPY protocol, so COPY blocks are converted to batched multi-row INSERTs (50 rows per statement, flushed at 1MB)
- **Unsupported extensions**: `pg_stat_statements`, `pg_buffercache`, `pg_cron`, etc. — CREATE, DROP, and COMMENT ON EXTENSION statements are skipped
- **Idempotent DDL**: `CREATE SCHEMA` → `IF NOT EXISTS`, `CREATE FUNCTION/VIEW` → `OR REPLACE`
- **Oversized rows**: Rows larger than 16MB are skipped with a warning (PGlite WASM crashes around 24MB per value)
- **Missing table references**: DDL errors from filtered dumps (e.g. ALTER TABLE on excluded tables) log a warning and continue
- **Transaction batching**: Data statements are grouped 200 per transaction with CHECKPOINT every 3 batches to manage WASM memory
- **PostgreSQL 18+ artifacts**: `SET transaction_timeout` silently skipped
- **psql meta-commands**: `\restrict` and similar silently skipped

This means you can take a `pg_dump` from a production Postgres database and restore it directly into orez — incompatible statements are handled automatically.

When orez is not running, `pg_restore` opens PGlite directly. When orez is running, pass `--pg-port` to restore through the wire protocol. Standard Postgres tools (`pg_dump`, `pg_restore`, `psql`) also work against the running proxy since orez presents a standard PostgreSQL 16.4 version string over the wire.

## Extra: orez/s3

Since we use this stack often with a file uploading service like MinIO which also requires docker, I threw in a tiny s3-compatible endpoint too:

`bunx orez --s3` or standalone `bunx orez s3`.

```typescript
import { startS3Local } from 'orez/s3'

const server = await startS3Local({
  port: 9200,
  dataDir: '.orez',
})
```

Handles GET, PUT, DELETE, HEAD with CORS. Files stored on disk. No multipart, no ACLs, no versioning.

## License

MIT
