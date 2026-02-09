# orez

[Zero](https://zero.rocicorp.dev) development backend powered by [PGlite](https://pglite.dev). Bundles PostgreSQL and zero-cache into a single process — no Docker, no Postgres install.

```
npx orez
```

Starts PGlite, the TCP proxy, and zero-cache. Ports auto-increment if already in use.

Exports a CLI, programmatic API, and Vite plugin.

## Install

```
npm install orez
```

`@rocicorp/zero` is included as a dependency and provides the zero-cache binary.

## CLI

```
npx orez
```

```
--pg-port          postgresql proxy port (default: 6434)
--zero-port        zero-cache port (default: 5849)
--data-dir         data directory (default: .zero-lite)
--migrations       migrations directory (default: src/database/migrations)
--seed             seed file path
--pg-user          postgresql user (default: user)
--pg-password      postgresql password (default: password)
--skip-zero-cache  run pglite + proxy only, skip zero-cache
--log-level        error, warn, info, debug (default: info)
--s3               also start a local s3-compatible server
--s3-port          s3 server port (default: 9200)
```

S3 can also run standalone:

```
npx orez s3
npx orez s3 --port 9200 --data-dir .orez
```

## Programmatic

```typescript
import { startZeroLite } from 'orez'

const { config, stop } = await startZeroLite({
  pgPort: 6434,
  zeroPort: 5849,
  migrationsDir: 'src/database/migrations',
  seedFile: 'src/database/seed.sql',
})

// your app connects to zero-cache at localhost:5849
// database is at postgresql://user:password@localhost:6434/postgres

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
      s3: true,
    }),
  ],
}
```

Starts orez when vite dev server starts, stops on close. Pass `s3: true` to also start a local s3 server.

## How it works

orez starts three things in one process:

1. A PGlite instance (full PostgreSQL 16 running in-process via WASM)
2. A TCP proxy that speaks the PostgreSQL wire protocol, including logical replication
3. A zero-cache child process that connects to the proxy thinking it's a real Postgres server

The trick is in the TCP proxy. zero-cache needs logical replication to stay in sync with the upstream database. PGlite doesn't support logical replication natively, so orez fakes it. Every mutation is captured by triggers into a changes table, then encoded into the pgoutput binary protocol and streamed to zero-cache through the replication connection. zero-cache can't tell the difference.

The proxy also handles multi-database routing. zero-cache expects three separate databases (upstream, CVR, change), but PGlite is a single database. orez maps database names to schemas, so `zero_cvr` becomes the `zero_cvr` schema and `zero_cdb` becomes `zero_cdb`.

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

The `--log-level` flag controls both zero-cache (`ZERO_LOG_LEVEL`) and PGlite's debug output. Setting it to `debug` enables verbose logging from both.

The layering is: orez defaults → your env → orez-managed connection vars. So setting `ZERO_LOG_LEVEL=debug` in your shell overrides the `--log-level` default, but you can't override the database connection strings (orez needs to point zero-cache at its own proxy).

Common vars you might want to set:

```bash
ZERO_MUTATE_URL=http://localhost:3000/api/zero/push
ZERO_QUERY_URL=http://localhost:3000/api/zero/pull
ZERO_LOG_LEVEL=debug
```

## What gets faked

The proxy intercepts several things to convince zero-cache it's talking to a real PostgreSQL server with logical replication enabled:

- `IDENTIFY_SYSTEM` returns a fake system ID and timeline
- `CREATE_REPLICATION_SLOT` persists slot info in a local table and returns a valid LSN
- `START_REPLICATION` enters streaming mode, encoding changes as pgoutput binary messages
- `current_setting('wal_level')` always returns `logical`
- `pg_replication_slots` queries are redirected to a local tracking table
- `SET TRANSACTION SNAPSHOT` is silently accepted (PGlite doesn't support imported snapshots)
- `ALTER ROLE ... REPLICATION` returns success
- `READ ONLY` is stripped from transaction starts to avoid PGlite serialization issues

The pgoutput encoder produces spec-compliant binary messages: Begin, Relation, Insert, Update, Delete, Commit, and Keepalive. All column values are encoded as text (typeOid 25), which zero-cache handles fine since it re-maps types downstream anyway.

## Extra: orez/s3

Local s3-compatible server for dev. Avoids needing Docker or MinIO.

```typescript
import { startS3Local } from 'orez/s3'

const server = await startS3Local({
  port: 9200,
  dataDir: '.orez',
})
```

Or via CLI: `npx orez --s3` or standalone `npx orez s3`.

Handles GET, PUT, DELETE, HEAD with CORS. Files stored on disk. No multipart, no ACLs, no versioning.

## Tests

82 tests across 6 test files covering the full stack from binary encoding to TCP-level integration:

```
bun test
```

The test suite includes a zero-cache compatibility layer that decodes pgoutput messages into the same typed format that zero-cache's PgoutputParser produces, validating end-to-end compatibility.

## Limitations

This is a development tool. It is not suitable for production use.

- PGlite runs single-threaded. All queries are serialized through a mutex. This is fine for development but would be a bottleneck under real load.
- Column types are all encoded as text in the replication stream. Zero-cache handles this, but other pgoutput consumers might not.
- Triggers add overhead to every write. Again, fine for development.
- PGlite stores data on the local filesystem. No replication, no backups, no high availability.

## Project structure

```
src/
  index.ts              main entry, orchestrates startup
  cli.ts                cli with citty
  config.ts             configuration with defaults
  log.ts                colored log prefixes
  port.ts               auto port finding
  pg-proxy.ts           tcp proxy with query rewriting
  pglite-manager.ts     pglite instance and migration runner
  s3-local.ts           standalone local s3 server (orez/s3)
  vite-plugin.ts        vite dev server plugin (orez/vite)
  replication/
    handler.ts          replication protocol state machine
    pgoutput-encoder.ts binary pgoutput message encoder
    change-tracker.ts   trigger installation and change reader
```

## License

MIT
