# orez/cf-do architecture - READ FIRST

## The fundamental constraint

**Cloudflare Durable Objects have a 128 MB memory budget per instance.**

PGlite + WASM Postgres + extension binaries pushes real deployments over that
budget. That is the dead end. zero-cache is still essential: it owns the sync
protocol, IVM/CVR machinery, replication handling, and client semantics.

The orez Cloudflare path is therefore:

- **yes** real `@rocicorp/zero` zero-cache
- **yes** Durable Object SQLite as the durable storage engine
- **yes** orez `DoBackend` serving Postgres-protocol semantics to zero-cache
- **no** PGlite
- **no** `pglite.wasm`, `pglite.data`, extension `.so` files, or WASM Postgres

## Production shape

```
┌────────────────────────────────────────────────────────────────┐
│ Worker                                                         │
│                                                                │
│ /sync/v* and /api/zero/* ─────► ZERO_CACHE_DO singleton        │
│ everything else ──────────────► ASSETS                         │
└────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────┐
│ ZERO_CACHE_DO Durable Object                                   │
│                                                                │
│ startZeroCacheEmbedCF() runs real zero-cache in-process        │
│   │                                                            │
│   ├─ zero-cache replica SQLite                                 │
│   │    @rocicorp/zero-sqlite3 -> orez worker SQLite shim       │
│   │    backed by ctx.storage.sql                               │
│   │                                                            │
│   ├─ zero-cache CVR + change-DB Postgres connections           │
│   │    postgres -> orez postgres browser shim                  │
│   │    DoBackend -> embed-LOCAL SQL backend (this DO's         │
│   │    ctx.storage.sql, zero cross-DO hops — CVR statements    │
│   │    are private view-syncer state; see                      │
│   │    src/worker/local-sql-backend.ts)                        │
│   │                                                            │
│   └─ zero-cache upstream Postgres connections                  │
│        postgres -> orez postgres browser shim                  │
│        DoBackend -> ZERO_SQL_DO /exec and /batch               │
└────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────┐
│ ZERO_SQL_DO Durable Object                                     │
│                                                                │
│ ZeroDO raw SQL endpoints                                       │
│   /exec, /batch, /commit-tx, /rollback-tx, /recover-txs,       │
│   /changes, /notify, /__orez/*                                 │
│   ctx.storage.sql                                              │
│   _zero_changes populated by transactional row triggers        │
└────────────────────────────────────────────────────────────────┘
```

All app traffic for a deployed project must use singleton DO IDs for both
`ZERO_CACHE_DO` and `ZERO_SQL_DO`; otherwise browser sessions will not share the
same zero-cache process and durable SQLite state.

## Important files

- `src/worker/zero-cache-embed-cf.ts` - starts real zero-cache inside a Durable
  Object and wires its storage/network dependencies to CF-safe shims.
- `src/worker/cf-patches.ts` - prepares an isolated zero-cache overlay whose
  worker graph and writer run in the Workers runtime without mutating
  `node_modules`.
- `src/pg-proxy-do-backend.ts` - implements the Postgres protocol/session,
  transaction, publication, and catalog boundary, then sends compiled SQLite
  to the DO SQL endpoints.
- `src/pg-sqlite-compiler/` - the first-class Postgres-to-SQLite compiler. SQL
  compatibility belongs here when it is generally useful; the proxy should not
  accumulate a second statement rewriter.
- `src/cf-do/worker.ts` - `ZeroDO`, the generic DO SQL backend. It also still
  contains a bespoke Zero sync protocol handler used for development and
  protocol experiments, but the production Soot deploy path uses real
  zero-cache through `startZeroCacheEmbedCF()`. Trusted subclasses can call its
  protected `runApplicationTransaction()` method to execute custom application
  work in the owning SQLite transaction. The method always passes through
  `atomically()`, exposes materialized row arrays instead of cursors, and runs
  deferred external effects after commit. It is not a public `fetch()` route.
- `src/do-sql-tracking.ts` - billable/logical write metering and the 150k
  rolling write circuit. `src/replication/*` streams committed
  `_zero_changes`; neither is the source of row capture.
- `src/cf-do/cdc.ts` - generated SQLite AFTER triggers for every published
  table. Triggers write full before/after row images to a staging table in the
  application statement; `ZeroDO` drains them into committed or transaction-
  pending changes before its storage transaction returns. This is logical row
  capture, not WAL/page copying, and includes writes made by business triggers.
  Capture is refused for any table with no stable row identity (see the undo
  invariants below), so CDC never records a change it could not roll back.
- `src/worker/local-sql-backend.ts` - serves the DoBackend HTTP protocol
  against the embed DO's own storage for the CVR/change DBs (no cross-DO hop).
- `src/cf-do/tx-journal.ts` - durable journal for DoBackend's emulated pg
  transactions: parsed DML uses transactional CDC before-images for rollback;
  unrecognized writes fall back to table snapshots. Journal markers and
  snapshots are recorded atomically, COMMIT is one storage transaction, and
  recovery at embed boot rolls back any
  transaction a dead DO generation left mid-flight (otherwise a deploy
  upgrade-kill mid-storer-write persists a partial cdc changeLog tx and wedges
  replication permanently).
- `src/cf-do/row-undo.ts` - restores captured before-images in reverse order
  for ROLLBACK and crash recovery. Internal CVR/CDB writes use the same path
  with `publish: false`, so they are undoable without entering the application
  change feed.

## What row-level undo actually guarantees

These are the properties rollback and crash recovery depend on. Each is a
verified guarantee, not an assumption.

- **The undo journal is lossless, and is not the Zero wire encoding.** Row
  images are stored twice on purpose. `row_data`/`old_data` hold the Zero wire
  image (plain JSON, blobs as postgres bytea text) that the changefeed consumer
  expects. `row_journal`/`old_journal` hold a typed image where every column is
  tagged with its SQLite storage class and an exact payload. The wire form alone
  cannot restore a row: a JSON number rounds any integer past 2^53, a blob is
  indistinguishable from a TEXT column holding the same `\xHEX` literal, and
  SQLite's JSON functions reject BLOB values outright. Integers keep their full
  signed 64-bit decimal, reals use 17 significant digits (which round-trips a
  double exactly, where a default text cast turns MAX_DOUBLE into `Inf`), and
  blobs keep their hex. SQLite infinities use PostgreSQL's JSON-safe
  `Infinity`/`-Infinity` text spellings instead of JavaScript numeric infinities,
  which `JSON.stringify` would silently replace with null.
- **Every undone row is identified by a stable key, never by its values.** A
  rowid table is matched on its captured rowid, which survives a primary-key
  update and tells two identical rows of a keyless table apart. A WITHOUT ROWID
  table has no rowid but always declares a primary key, so it matches on that.
  Matching on column values would delete or update every duplicate.
- **Undo never writes a generated column.** SQLite refuses to INSERT or UPDATE
  one, so a restore names only the writable columns reported by
  `PRAGMA table_xinfo`. The same applies to a snapshot restore, where `SELECT *`
  would otherwise select generated columns straight back into an INSERT.
- **A restore never fires the restored table's triggers, business triggers
  included.** Undo replays the inverse DML, so undoing an INSERT runs a DELETE.
  Letting that DELETE fire the table's own AFTER DELETE trigger would write side
  effects the original transaction never made, and if the side effect lands on a
  captured table, its CDC trigger stages a phantom change on top. Both row-undo
  and the snapshot restore detach every trigger on the tables they touch through
  `suspendTriggers`, then recreate them verbatim. Detaching only the generated
  CDC triggers leaves both holes open.
- **Every undo statement must move exactly one row, or the transaction fails.**
  A restore that quietly matches zero rows (the row is already gone) or several
  (a value-matched duplicate) has corrupted the table. It raises instead, which
  aborts the storage transaction and leaves the damage unwritten.
- **A table CDC cannot capture is never left claiming row-level rollback.**
  DoBackend marks a parsed write row-journaled before the DO has said whether it
  can capture the table. When CDC declines it, the worker takes the table
  snapshot the journal would otherwise have taken, in the same storage
  transaction as the write, so the transaction lands back on the snapshot path
  recovery already replays.
- **Statements with implicit side effects use snapshots, not trigger staging
  order.** SQLite can run a newly-created business trigger before an older CDC
  trigger, and foreign-key CASCADE/SET actions are not visible in
  `sqlite_master` at all. For a tracked write whose source has a business trigger
  or is referenced by a cascading/SET foreign key, the worker snapshots every
  user table once for that transaction. This covers unpublished and recursively
  triggered targets and makes rollback independent of CDC buffer order. Snapshot
  restore deletes every participating table before reinserting any snapshot rows,
  then inserts parents before children. The two-phase restore is required for
  cyclic cascading foreign keys, where an interleaved later delete could erase a
  row that had already been restored.
- **In-memory schema caches are re-derived from SQLite whenever a storage
  transaction aborts.** `ctx.storage.transaction()` rolls back the SQLite side
  of an aborted batch, including CDC triggers, its registration metadata, and
  any table a readiness flag just created. The caches that mirror them are plain
  fields and would otherwise keep asserting state SQLite no longer has: a table
  stays "registered and verified" with no trigger left on disk, and every later
  write to it goes silently uncaptured. Every storage transaction therefore runs
  through the `atomically` wrappers in `worker.ts` and `local-sql-backend.ts`,
  which reload the caches on abort.

## Crash-safety + flow control invariants

- A DoBackend pg transaction is committed if and only if its
  `_orez_tx_manifest` rows are gone. Anything else is rolled back by
  `recoverTxJournal` on the next embed boot (owner-scoped, so the app
  worker's live pg sessions are never touched).
- Application rows and their staging CDC rows are written by the same SQLite
  statement. A failed statement leaves neither behind. Published application
  changes drain into `_zero_pending_changes`; `/commit-tx` promotes the
  complete group to `_zero_changes` atomically with clearing the rollback
  journal. Rollback-only internal changes are deleted at commit. Rollback and
  crash recovery instead restore every captured before-image and discard the
  pending group. Only rows carrying a typed before-image are marked `undoable`;
  a pending row without one belongs to a table whose rollback a snapshot owns,
  and row-undo skips it rather than guessing at a lossy wire image.
- `_zero_changes` rows are purged only when the consumer CONFIRMS them
  (standby status updates / the resume LSN of a reconnect), mirroring how
  real postgres retains WAL until the slot's confirmed_flush_lsn passes it.
  A consumer killed between stream and store re-streams the transaction
  instead of silently losing it; the lsn→watermark batch mapping lives in
  `_orez._zero_streamed_batches`.

## What not to do

- Do not import `@electric-sql/pglite` into the Cloudflare DO deploy path.
- Do not bundle PGlite WASM/data/extensions into the deploy template.
- Do not replace zero-cache with the bespoke handler for production sync.
- Do not add a second fallback path that silently switches between PGlite,
  bespoke sync, and zero-cache. There should be one production path:
  zero-cache -> orez Postgres protocol -> DO SQLite.

If a future change needs generally useful Postgres SQL behavior, implement it
in `src/pg-sqlite-compiler/`. Keep protocol/session/catalog behavior in
`DoBackend` and storage behavior in the DO SQL backend. Do not put PGlite back
in the request path or grow a second SQL translator in the proxy.

## Running the chat e2e harness against this backend

See `src/cf-do/CHAT_E2E.md`. The DO path is exercised end-to-end by chat's
`--lite` mode harness. The repository wrapper patches only the cold lite
Postgres/Zero readiness waits to 120 seconds; it does not change Playwright
timeouts, retries, or production behavior. Keep the data worker's original
150k write circuit as the stronger amplification guard. A 2026-07-13 profile
found 1.08m billable rows from repeated full copies of a growing internal
`cdc_changeLog`; row-image journaling removed those copies and a clean global
setup completed at 125,402 rows. Re-profile requests and billable rows before
raising either budget.
