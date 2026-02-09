# zero-pglite

Drop-in replacement for the Docker-based development backend that Rocicorp's Zero requires. Instead of running PostgreSQL, zero-cache, and MinIO in Docker containers, zero-pglite bundles everything into a single process using PGlite (PostgreSQL compiled to WASM).

The goal is simple: `bun install && bun dev` with zero system dependencies.


## How it works

zero-pglite starts four things in one process:

1. A PGlite instance (full PostgreSQL 16 running in-process via WASM)
2. A TCP proxy that speaks the PostgreSQL wire protocol, including logical replication
3. A zero-cache child process that connects to the proxy thinking it's a real Postgres server
4. A minimal S3-compatible HTTP server for file uploads

The trick is in the TCP proxy. zero-cache needs logical replication to stay in sync with the upstream database. PGlite doesn't support logical replication natively, so zero-pglite fakes it. Every mutation is captured by triggers into a changes table, then encoded into the pgoutput binary protocol and streamed to zero-cache through the replication connection. zero-cache can't tell the difference.

The proxy also handles multi-database routing. zero-cache expects three separate databases (upstream, CVR, change), but PGlite is a single database. zero-pglite maps database names to schemas, so `zero_cvr` becomes the `zero_cvr` schema and `zero_cdb` becomes `zero_cdb`.


## Install

```
npm install zero-pglite
```

or with bun:

```
bun add zero-pglite
```

You also need `@rocicorp/zero` installed in your project for the zero-cache binary.


## Usage

```typescript
import { startZeroLite } from 'zero-pglite'

const { config, stop } = await startZeroLite({
  pgPort: 6434,
  zeroPort: 5849,
  s3Port: 10201,
  migrationsDir: 'src/database/migrations',
  seedFile: 'src/database/seed.sql',
})

// your app connects to zero-cache at localhost:5849
// database is at postgresql://user:password@localhost:6434/postgres

// when done
await stop()
```

All options are optional and have sensible defaults. See `src/config.ts` for the full list.


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


## Tests

80 unit tests across 5 test files covering the full stack from binary encoding to TCP-level integration:

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
- The S3 server is just a filesystem wrapper. No multipart uploads, no ACLs, no versioning.


## Project structure

```
src/
  index.ts              main entry, orchestrates startup
  config.ts             configuration with defaults
  pg-proxy.ts           tcp proxy with query rewriting
  pglite-manager.ts     pglite instance and migration runner
  s3-local.ts           minimal s3 http server
  replication/
    handler.ts          replication protocol state machine
    pgoutput-encoder.ts binary pgoutput message encoder
    change-tracker.ts   trigger installation and change reader
```


## License

MIT
