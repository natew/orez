# orez

[Zero](https://zero.rocicorp.dev) and [PGlite](https://pglite.dev) made to play well together, with no native dependencies. Helped by a custom WASM fork of SQLite's [bedrock branch](https://sqlite.org/src/timeline?t=begin-concurrent) called [bedrock-sqlite](https://www.npmjs.com/package/bedrock-sqlite), and [pgsql-parser](https://www.npmjs.com/package/pgsql-parser) (the real PostgreSQL C parser compiled to WASM) for SQL analysis. No Docker, no Postgres install, no `node-gyp`, no platform-specific binaries.

```
bunx orez
```

Starts PGlite (WASM Postgres), a TCP proxy, and zero-cache with WASM SQLite. Exports a CLI, programmatic API, and Vite plugin. Comes with PGlite extensions `pgvector` and `pg_trgm` enabled by default. Includes `pg_dump` and `pg_restore` subcommands that can restore production Postgres dumps directly into PGlite — handling COPY→INSERT conversion, unsupported extension filtering, idempotent DDL rewriting, and WASM memory management automatically.

<p align="center">
  <img src="logo.svg" alt="orez is hebrew for rice — zero, pglite, and sqlite-wasm hanging out" width="320" />
</p>

## Install

```
bun install orez
```

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

## Vite plugin

```typescript
import orez from 'orez/vite'

export default {
  plugins: [
    orez({
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

The replication handler also tracks shard schema tables (e.g., `chat_0.clients`, `chat_0.mutations`) that zero-cache creates for mutation state. These are monitored via change tracking triggers so that `.server` promises on zero mutations resolve correctly.

### Zero native dependencies

The whole point of orez is that `bunx orez` works everywhere with no native compilation step. Postgres runs in-process as WASM via PGlite. But zero-cache also needs SQLite, and `@rocicorp/zero-sqlite3` ships as a compiled C addon — which means `node-gyp`, build tools, and platform-specific binaries.

orez ships its own package, [bedrock-sqlite](https://www.npmjs.com/package/bedrock-sqlite) — SQLite's [bedrock branch](https://sqlite.org/src/timeline?t=begin-concurrent) recompiled to WASM with BEGIN CONCURRENT and WAL2 support. At startup, orez patches `@rocicorp/zero-sqlite3` to load bedrock-sqlite instead of the native C addon. Both databases run as WASM — nothing to compile, nothing platform-specific. Just `bun install` and go.

## Environment variables

Your entire environment is forwarded to the zero-cache child process. This means any `ZERO_*` env vars you set are passed through automatically.

orez provides sensible defaults for a few variables:

| Variable                | Default             | Overridable |
| ----------------------- | ------------------- | ----------- |
| `NODE_ENV`              | `development`       | yes         |
| `ZERO_LOG_LEVEL`        | from `--log-level`  | yes         |
| `ZERO_NUM_SYNC_WORKERS` | `1`                 | yes         |
| `ZERO_UPSTREAM_DB`      | _(managed by orez)_ | no          |
| `ZERO_CVR_DB`           | _(managed by orez)_ | no          |
| `ZERO_CHANGE_DB`        | _(managed by orez)_ | no          |
| `ZERO_REPLICA_FILE`     | _(managed by orez)_ | no          |
| `ZERO_PORT`             | _(managed by orez)_ | no          |

The `--log-level` flag controls both zero-cache (`ZERO_LOG_LEVEL`) and PGlite's debug output. Default is `warn` to keep output quiet. Set to `info` or `debug` for troubleshooting.

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

## Tests

145 tests — 108 orez tests across 8 test files covering the full stack from binary encoding to TCP-level integration, plus 37 bedrock-sqlite tests covering the WASM SQLite engine:

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
  index.ts              main entry, orchestrates startup + sqlite wasm patching
  cli.ts                cli with citty
  config.ts             configuration with defaults
  log.ts                colored log prefixes
  mutex.ts              simple mutex for serializing pglite access
  port.ts               auto port finding
  pg-proxy.ts           tcp proxy with per-instance routing and query rewriting
  pglite-manager.ts     multi-instance pglite creation and migration runner
  s3-local.ts           local s3-compatible server (orez/s3)
  vite-plugin.ts        vite dev server plugin (orez/vite)
  replication/
    handler.ts          replication protocol state machine
    pgoutput-encoder.ts binary pgoutput message encoder
    change-tracker.ts   trigger installation, shard schema tracking, and change reader
sqlite-wasm/
  Makefile              emscripten build for bedrock-sqlite wasm binary
  bedrock-sqlite.d.ts   typescript declarations
  native/
    api.js              better-sqlite3 compatible database/statement API
    vfs.c               custom VFS with SHM support for WAL/WAL2
    vfs.js              javascript VFS bridge
  test/
    database.test.ts    37 tests for the wasm sqlite engine
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
