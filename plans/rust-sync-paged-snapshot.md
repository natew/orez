# Paged, resumable upstream snapshots for the rust sync host

Tracked by agentbus task `t-mrmjcw7h-1h660` (urgent). 2026-07-15.

## Problem

The snapshot path materializes whole datasets at every layer and applies them
in a single storage transaction:

- source (`src/cf-do/worker.ts` `handleSnapshot`): every row of every modeled
  table is read inside one `transactionSync` and serialized into one JSON
  response.
- destination (`packages/sync-cf-host/src/host.ts` ingest →
  `engine_apply_upstream_snapshot` in `crates/sync-core/src/upstream.rs`):
  the whole JSON is materialized again, deserialized into a wasm `BTreeMap`,
  then every modeled row is DELETEd and reinserted inside ONE
  `transactionSync`.

Above the 150k billable-row write breaker (or the DO memory / storage-op
limits, which can fail earlier), that single transaction deterministically
rolls back. No durable progress survives, so retry re-does the identical
oversized transaction and can never succeed. This is the same failure class
that livelocked the retired zero-cache embed's initial sync, reborn on the
go-forward stack. It blocks chat-scale datasets on the rust host.

## Design

Fuzzy paged snapshot + change-log catch-up + atomic staged cutover. The same
shape as classic WAL-based backup restore: pages are read without a
cross-request consistency guarantee, and the change feed closes the gap.

### 1. Source: keyset-paged snapshot reads

`GET /snapshot?table=<t>&cursor=<pk-token>&limit=<n>` returns one bounded page
ordered by primary key, plus the page's `nextCursor` (null when the table is
exhausted) and the CURRENT watermark. A bare `/snapshot` (no params) keeps the
legacy whole-dataset response for small datasets and existing tests, but the
host stops using it by default.

- each page is one bounded `SELECT ... WHERE pk > ? ORDER BY pk LIMIT n` —
  no cross-request snapshot is needed and source memory stays O(page).
- no consistency pinning: concurrent writes during paging are EXPECTED and
  reconciled by the catch-up phase.
- source read errors fail closed (HTTP 5xx), never an empty table
  (`t-mrmjcydx-1ht00` covers the same rule for the legacy path).

### 2. Destination: generation-staged apply with durable progress

The engine gains five entry points (wasm-exported like the existing ones):

- `engine_read_snapshot_progress(db) -> Progress | null` — STRICT read-only
  resume lookup. `null` means genuinely no active generation; an unreadable
  or corrupt progress row is an error, never `null` — defaulting a failed
  read to "no progress" would silently discard resumable work. The host
  calls this first on every ingest entry; an active generation resumes from
  its stored table+cursor without touching the source's page-1 watermark.

- `engine_begin_snapshot_generation(db, tables, startWatermark) -> generation`
  — creates `_zsync_stage_<g>_<table>` tables from the modeled schema and a
  durable progress row in `_zsync_snapshot_progress` (generation,
  startWatermark, table, cursor, state = 'paging'). `startWatermark` is a
  SOURCE-side fact the host captures from the FIRST page response — the
  engine cannot derive it (live `upstream_watermark` is exactly wrong after
  a 410-triggered restart). `begin` always creates a NEW generation, first
  marking any active one abandoned (lazy sweep); resume is
  `engine_read_snapshot_progress`'s job, never `begin`'s.
- `engine_apply_snapshot_page(db, tables, g, table, rows, nextCursor)` —
  validates rows through the same `upsert_row` path as today but targeted at
  the staging table, then durably records `nextCursor` with the same commit.
  The cursor is an OPAQUE source-owned token; the engine stores it verbatim
  and never interprets it. `nextCursor = null` advances progress to the next
  table (tables are paged one at a time in sorted-name order, which the
  progress row encodes). Called once per fetched page, each call inside ITS
  OWN `transactionSync` (bounded rows, far under the breaker).
- `engine_apply_snapshot_changes(db, tables, g, batch)` — catch-up: applies a
  changes batch through the same internal change/upsert/delete logic as
  `apply_upstream` but targeted at the STAGING tables, advancing a
  generation-local catch-up cursor in the progress row. It must not touch
  live tables or live `_zsync_meta.upstream_watermark`. A delete for a row
  the staging copy never saw is an idempotent no-op — replaying deletes is
  what makes the fuzzy page reads safe.
- `engine_finalize_snapshot_generation(db, tables, g, watermark)` — the
  cutover: per table `DROP` live + `ALTER TABLE ... RENAME` staging to live
  (metadata-only, cheap), set `upstream_watermark` to the catch-up DRAIN
  watermark (the last replayed change, not the page-time watermark), mark
  the generation complete, delete the progress row. One small transaction.
  RENAME emits no DML triggers, so the change log records nothing about the
  rebuild — finalize therefore also bumps the sync-core epoch
  (store/pull invalidation) INSIDE the same cutover transaction, so every
  pre-cutover client cookie is invalidated and the next pull returns a full
  snapshot of the new live generation. A client full-resync after a
  resnapshot is semantically required anyway (old cookies belong to a
  change-log lineage the rebuild replaced); synthesizing per-row diffs at
  cutover would recreate the giant-transaction problem this design removes.

Host orchestration (`#ingest` snapshot branch):

1. begin (or RESUME: if a progress row exists for an incomplete generation,
   continue from its stored table+cursor — this is what survives DO
   eviction/restart mid-rebuild).
2. loop: fetch page from source → apply page → each iteration commits
   independently. Page size default 2 000 rows, adaptive: a breaker trip or
   storage error on a page halves the page size and retries that page
   (floor 100; a page that still trips at the floor fails the generation with
   a diagnostic — bounded retries, no hot loop).
3. catch-up: replay `/changes?since=<startWatermark>` in bounded batches
   through the existing `apply_upstream` into the STAGING tables until the
   feed drains (delta below one page). `410 watermarkTooOld` during catch-up
   → abandon the generation and start a new one (source retention was
   outrun; with sane retention this converges because catch-up is much
   faster than paging).
4. finalize (cutover) + send a wake frame unconditionally (also resolves the
   empty-resnapshot wake gap, `t-mrmiwh6h-7da0`).

### 3. Cleanup and invariants

- abandoned/complete generations are swept lazily in bounded delete batches
  with a flush between batches (the f87cf06 lesson: cleanup itself must never
  be one giant implicit transaction).
- exactly one incomplete generation may exist; beginning a new one abandons
  the old (marks it, cleanup sweeps it).
- pulls keep serving the LIVE tables throughout; clients see the rebuild only
  as one atomic watermark jump at cutover.
- `_zsync_snapshot_progress` reads on the resume path are STRICT — a read
  failure aborts rather than defaulting to "no progress" (same rule as
  `t-mrmjd2vw-1jco0`).

### 4. What does not change

- `apply_upstream` (incremental changes path), push/pull, the wire protocol,
  and the client transport are untouched.
- small datasets behave identically apart from extra page-boundary commits.
- the legacy single-shot `/snapshot` response stays for compatibility and
  harness use; remove it only after all hosts run the paged flow.

## Validation plan

- rust: unit tests for begin/page/finalize incl. resume-from-cursor,
  adaptive page halving, catch-up overlap (a row updated mid-paging must end
  at its post-catch-up value), and cutover atomicity.
- host: ingest-harness lane that seeds > breaker-threshold rows on the source
  DO, forces a mid-rebuild DO restart, and proves convergence + one wake.
- regression: the existing whole-snapshot tests keep passing against the
  legacy endpoint.

## Sizing notes

150k billable rows per transaction is the hard ceiling; billable writes
amplify per index, so the default 2 000-row page keeps a wide margin even on
heavily indexed tables. Catch-up batches reuse the changes `limit` (SQL-side
after `t-mrmjcydx-1ht00`).
