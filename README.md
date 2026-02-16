# oreZ

[![npm version](https://img.shields.io/npm/v/orez.svg)](https://www.npmjs.com/package/orez)
[![license](https://img.shields.io/npm/l/orez.svg)](https://github.com/natew/orez/blob/main/LICENSE)

Run [Zero](https://zero.rocicorp.dev) locally with zero native dependencies. No Postgres install, no SQLite compilation, no Docker.

```
bunx orez
```

oreZ makes Zero work on [PGlite](https://pglite.dev) (Postgres in WASM) and [bedrock-sqlite](https://www.npmjs.com/package/bedrock-sqlite) (SQLite in WASM), bundled together so local development is as simple as `bun install && bunx orez`.

## Requirements

- **Bun** 1.0+ or **Node.js** 20+
- **Zero** 0.18+ (tested with 0.18.x)

## Limitations

This is a **development tool only**. Not suitable for production.

- **Single-session per database** — queries are serialized through a mutex. Fine for development, would bottleneck under load.
- **Trigger overhead** — every write fires change-tracking triggers.
- **Local filesystem** — no replication, no HA. Use `orez pg_dump` for backups.

## Features

```
bunx orez
```

**What oreZ handles automatically:**

- **Zero native deps** — both Postgres and SQLite run as WASM. Nothing to compile, nothing platform-specific.
- **Memory management** — auto-sizes Node heap (~50% RAM, min 4GB), purges consumed WAL, batches restores with CHECKPOINTs
- **Real-time replication** — changes sync instantly via `pg_notify` triggers, with adaptive polling fallback (20ms catching up, 500ms idle)
- **Auto-recovery** — finds available ports if configured ones are busy, provides reset/restart controls
- **PGlite compatibility** — rewrites unsupported queries, fakes wire protocol responses, filters unsupported column types
- **Admin dashboard** — live logs, HTTP request inspector, restart/reset controls, env viewer
- **Production restores** — `pg_dump`/`pg_restore` with COPY→INSERT conversion, auto-coordinates with zero-cache
- **Extensions** — pgvector, pg_trgm, pgcrypto, uuid-ossp, citext, hstore, ltree, fuzzystrmatch, btree_gin, btree_gist, cube, earthdistance enabled by default (lazy-loaded, no overhead unless used)

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
--force-wasm-sqlite       force wasm sqlite even if native is available
--disable-wasm-sqlite     force native sqlite (fail if not available)
--on-db-ready=CMD         command to run after db+proxy ready, before zero-cache
--on-healthy=CMD          command to run once all services healthy
--disable-admin           disable admin dashboard
--admin-port=6477         admin dashboard port (default: 6477)
```

Ports auto-increment if already in use.

## Admin Dashboard

Enabled by default at `http://localhost:6477`.

- **Logs** — live-streaming logs from zero-cache, filterable by source and level
- **HTTP** — request/response inspector for zero-cache traffic
- **Env** — environment variables passed to zero-cache
- **Actions** — restart zero-cache, reset (wipe replica + resync), full reset (wipe CVR/CDB too)

Logs are also written to separate files in your data directory: `zero.log`, `proxy.log`, `pglite.log`, etc.

```
bunx orez --disable-admin  # disable dashboard
```

## Programmatic API

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
  adminPort: 6477, // set to 0 to disable
})

// your app connects to zero-cache at localhost:5849
// database is at postgresql://user:password@localhost:6434/postgres

// db is the postgres PGlite instance (for direct queries)
// instances has all three: { postgres, cvr, cdb }

await stop()
```

All options are optional with sensible defaults.

### Lifecycle hooks

| Hook        | CLI                 | Programmatic                          | When                                |
| ----------- | ------------------- | ------------------------------------- | ----------------------------------- |
| on-db-ready | `--on-db-ready=CMD` | `onDbReady: 'CMD'` or `onDbReady: fn` | after db + proxy ready, before zero |
| on-healthy  | `--on-healthy=CMD`  | `onHealthy: 'CMD'` or `onHealthy: fn` | after all services ready            |

Shell commands receive env vars: `DATABASE_URL`, `OREZ_PG_PORT`, `OREZ_ZERO_PORT`. Change tracking triggers are re-installed after `onDbReady`.

## Vite Plugin

```typescript
import { orezPlugin } from 'orez/vite'

export default {
  plugins: [
    orezPlugin({
      pgPort: 6434,
      zeroPort: 5849,
      migrationsDir: 'src/database/migrations',
      onDbReady: () => console.log('db ready'),
      onHealthy: () => console.log('all services healthy'),
    }),
  ],
}
```

Starts oreZ when vite dev starts, stops on close. Supports all `startZeroLite` options plus `s3` and `s3Port`.

## Backup & Restore

Dump and restore your local database — no native Postgres install needed.

```bash
bunx orez pg_dump > backup.sql
bunx orez pg_dump --output backup.sql
bunx orez pg_restore backup.sql
bunx orez pg_restore backup.sql --clean  # drop public schema first
```

### Restoring into a running instance

When oreZ is running, restore through the wire protocol:

```bash
bunx orez pg_restore backup.sql --pg-port 6434
```

This automatically:

1. Stops zero-cache before restore (via admin API)
2. Clears replication state and shard schemas
3. Restores the dump
4. Adds all public tables to the publication
5. Restarts zero-cache

The `--direct` flag forces direct PGlite access, skipping wire protocol.

### What restore handles

- **COPY → INSERT** — PGlite doesn't support COPY protocol; converted to batched multi-row INSERTs
- **Unsupported extensions** — `pg_stat_statements`, `pg_buffercache`, `pg_cron` etc. silently skipped
- **Idempotent DDL** — `CREATE SCHEMA` → `IF NOT EXISTS`, `CREATE FUNCTION` → `OR REPLACE`
- **Oversized rows** — rows >16MB skipped with warning (WASM limit)
- **Transaction batching** — 200 statements per transaction, CHECKPOINT every 3 batches
- **Dollar-quoting** — correctly parses `$$` and `$tag$` in function bodies

Standard Postgres tools (`pg_dump`, `pg_restore`, `psql`) also work against the running proxy.

## Environment Variables

All `ZERO_*` env vars are forwarded to zero-cache. oreZ provides defaults:

| Variable                    | Default            | Overridable |
| --------------------------- | ------------------ | ----------- |
| `NODE_ENV`                  | `development`      | yes         |
| `ZERO_LOG_LEVEL`            | from `--log-level` | yes         |
| `ZERO_NUM_SYNC_WORKERS`     | `1`                | yes         |
| `ZERO_ENABLE_QUERY_PLANNER` | `false`            | yes         |
| `ZERO_UPSTREAM_DB`          | _(managed)_        | no          |
| `ZERO_CVR_DB`               | _(managed)_        | no          |
| `ZERO_CHANGE_DB`            | _(managed)_        | no          |
| `ZERO_REPLICA_FILE`         | _(managed)_        | no          |
| `ZERO_PORT`                 | _(managed)_        | no          |

Common vars you might set:

```bash
ZERO_MUTATE_URL=http://localhost:3000/api/zero/push
ZERO_QUERY_URL=http://localhost:3000/api/zero/pull
```

## Local S3

Since Zero apps often need file uploads and MinIO requires Docker:

```bash
bunx orez --s3           # with orez
bunx orez s3             # standalone
```

```typescript
import { startS3Local } from 'orez/s3'

const server = await startS3Local({ port: 9200, dataDir: '.orez' })
```

Handles GET, PUT, DELETE, HEAD with CORS. Files stored on disk. No multipart, no ACLs, no versioning.

---

# How It Works

## Architecture

oreZ runs three components:

1. **Three PGlite instances** — PostgreSQL 17 in WASM, one per database zero-cache expects (postgres, zero_cvr, zero_cdb)
2. **TCP proxy** — speaks PostgreSQL wire protocol, routes to correct PGlite, handles logical replication
3. **zero-cache** — child process connecting to proxy, thinks it's real Postgres

### Why three instances?

zero-cache expects three databases with independent transaction contexts. PGlite is single-session — all connections share one session. Without isolation, CVR transactions get corrupted by postgres queries (`ConcurrentModificationException`).

| Connection database | PGlite instance | Data directory    |
| ------------------- | --------------- | ----------------- |
| `postgres`          | postgres        | `pgdata-postgres` |
| `zero_cvr`          | cvr             | `pgdata-cvr`      |
| `zero_cdb`          | cdb             | `pgdata-cdb`      |

### Replication

PGlite doesn't support logical replication, so oreZ fakes it:

1. Triggers capture every mutation into `_orez._zero_changes`
2. Changes are encoded as pgoutput binary protocol
3. Streamed to zero-cache through the replication connection

Change notifications use `pg_notify` for real-time sync. Polling (20ms/500ms adaptive) is fallback only.

### SQLite WASM

zero-cache needs SQLite via `@rocicorp/zero-sqlite3` (native C addon). oreZ intercepts this at runtime using Node's ESM loader hooks, redirecting to [bedrock-sqlite](https://www.npmjs.com/package/bedrock-sqlite) — SQLite's bedrock branch compiled to WASM with BEGIN CONCURRENT and WAL2.

The shim also polyfills the better-sqlite3 API surface zero-cache expects.

### Native SQLite mode

For `--disable-wasm-sqlite`, bootstrap the native addon first:

```bash
bun run native:bootstrap
```

## Internal Schema

oreZ stores replication state in the `_orez` schema (survives `pg_restore --clean`):

- `_orez._zero_changes` — change log for replication
- `_orez._zero_replication_slots` — slot tracking
- `_orez._zero_watermark` — LSN sequence

## Wire Protocol Compatibility

The proxy intercepts and rewrites to make PGlite look like real Postgres:

| Query/Command                   | What oreZ does                                      |
| ------------------------------- | --------------------------------------------------- |
| `version()`                     | Returns `PostgreSQL 17.4 on x86_64-pc-linux-gnu...` |
| `current_setting('wal_level')`  | Returns `logical`                                   |
| `IDENTIFY_SYSTEM`               | Returns fake system ID and timeline                 |
| `CREATE_REPLICATION_SLOT`       | Persists to local table, returns valid LSN          |
| `START_REPLICATION`             | Streams changes as pgoutput binary                  |
| `pg_replication_slots`          | Redirects to local tracking table                   |
| `READ ONLY` / `ISOLATION LEVEL` | Stripped (single-session)                           |

## Workarounds

Things that don't "just work" when replacing Postgres with PGlite and native SQLite with WASM:

### Session state bleed

PGlite is single-session — if `pg_restore` sets `search_path = ''`, every subsequent connection inherits it. On disconnect, oreZ resets `search_path`, `statement_timeout`, `lock_timeout`, and rolls back open transactions.

### Query planner disabled

`ZERO_ENABLE_QUERY_PLANNER=false` because it relies on SQLite scan statistics that cause infinite loops in WASM.

### Unsupported column types

Columns with `tsvector`, `tsquery`, `USER-DEFINED` types are filtered from replication messages.

### Publication-aware tracking

If `ZERO_APP_PUBLICATIONS` is set, only tables in that publication get change-tracking triggers.

### Broken trigger cleanup

After restore, triggers whose backing functions don't exist are dropped (happens with filtered pg_dump).

## Tests

```bash
bun run test                                # orez tests
bun run test:integration:native             # native sqlite integration
cd sqlite-wasm && bunx vitest run           # bedrock-sqlite tests
```

## Project Structure

```
src/
  cli-entry.ts          auto heap sizing wrapper
  cli.ts                cli with citty
  index.ts              main entry, orchestrates startup
  config.ts             configuration with defaults
  log.ts                colored log prefixes, log files
  mutex.ts              serializing pglite access
  port.ts               auto port finding
  pg-proxy.ts           postgresql wire protocol proxy
  pglite-manager.ts     multi-instance pglite, migrations
  s3-local.ts           local s3 server (orez/s3)
  vite-plugin.ts        vite plugin (orez/vite)
  admin/
    server.ts           admin dashboard backend
    ui.ts               admin dashboard frontend
    log-store.ts        log aggregation
    http-proxy.ts       http request logging
  replication/
    handler.ts          replication state machine, adaptive polling
    pgoutput-encoder.ts binary pgoutput encoder
    change-tracker.ts   trigger installation, change purging
  integration/
    *.test.ts           end-to-end tests
sqlite-wasm/
  Makefile              emscripten build
  native/api.js         better-sqlite3 compatible API
  native/vfs.c          custom VFS with SHM for WAL2
```

## License

MIT
