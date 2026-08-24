# Trade-offs and operational reality

This page is the honest account of what you take on by running the sync engine
on Durable Object SQLite instead of zero-cache on Postgres. The engine is
simpler and cheaper to operate in most respects, and it has two costs that are
easy to underestimate: write amplification billing on Cloudflare, and a
client-side reset when you change the sync host origin.

## Durable Object storage instead of an external database

The engine and its storage live in the same Durable Object. There is no separate
database process to run, connect to, scale, or back up out of band. One
namespace is one object holding its own SQLite. This is why a deployment needs
no Postgres, no connection pool, and no long-lived cache process.

The cost is that a namespace inherits Durable Object limits. Memory is the
tight one: the DO memory budget makes carrying a full Postgres-in-WASM
untenable, which is exactly why the engine is a native SQLite protocol
implementation rather than PGlite in a DO. Storage per object is bounded, and
one object is a single isolate. A namespace that outgrows a single object's
limits is a data-modeling problem you solve by partitioning into more
namespaces, not by scaling the object up.

## Single-writer semantics

One Durable Object per namespace means one writer per namespace, and writes
serialize through that object. This buys correctness for free: every pull runs
in one synchronous SQLite transaction and sees one consistent snapshot, with no
repeatable-read gymnastics of the kind a Postgres-backed server needs. Push
mutations commit atomically, rows and last-mutation-id together.

The ceiling is the flip side: a single namespace's throughput is one object's
throughput. The measured chat staging run pushed 10,000 mutations across 16
concurrent writers at roughly 48 per second through one object, which is fine
for a chat server's write rate but is not a database you point a bulk import at.
Work that fans out (many servers, many projects) scales by having many
namespaces, each its own object.

## Write amplification on Cloudflare

This is the trade-off that caused real incidents, so it gets the most detail.

Cloudflare bills Durable Object SQLite by `rowsWritten` at the physical level.
Every logical `INSERT` or `UPDATE` also writes each index it touches, so one
application row write costs `1 + N_indexes` billable rows, plus the
change-tracking rows the sync model appends for every write. The billable number
is therefore much larger than the application row count, and it grows with your
index count.

The measured figures from the 2026-07-10 and 2026-07-11 soot incident analysis:

- **About 1.3k billable rows per push.** 100 control pushes wrote 133,819
  billable rows on soot's data tier. A single small mutation is not a single
  billable row; it is a mutation plus its indexes plus change tracking, times
  the tables it touches.
- **About 127.5k billable rows for one cascading account delete.** A single
  `limit=1` deletion that cascades across foreign keys finalized at 127,555
  `ZeroSqlDO` writes. One user action at the top of a foreign-key graph can be
  five orders of magnitude more billable rows than it looks like.

Two consequences follow. First, index count and foreign-key cascade depth are
now billing decisions, not just schema decisions. Second, the counter is subtle:
`SqlStorageCursor.rowsWritten` is the billing value and it can keep increasing
while a `RETURNING` cursor is iterated. Version 0.4.53 sampled it before the
cursor was consumed and undercounted; the fix in 0.4.54 meters the monotonic
cursor delta through `toArray()`, `one()`, `next()`, iteration, and `raw()`
iteration (`packages/sync-cf-host/src/write-safeguards.ts`,
`trackBillableCursorRows`).

### Six billable rows per captured change

Measured 2026-08-21 and pinned by `bills six rows per captured change` in
`packages/orez-lite/src/cf-do/worker-write-amplification.test.ts`. The test
measures the slope between a one-row and a ten-row synced `UPDATE`, so it
reports the marginal cost of one more captured row rather than a total that
fixed per-transaction overhead would muddy.

One captured row costs six billable rows, and only two of them are the point:

| stage                                                                      | rows |
| -------------------------------------------------------------------------- | ---- |
| application row, plus its capture trigger's insert into `_orez_cdc_buffer` | 2    |
| `DELETE FROM _orez_cdc_buffer` on drain                                    | 1    |
| `INSERT INTO _zero_pending_changes`                                        | 1    |
| `INSERT INTO _zero_changes` (the durable changefeed)                       | 1    |
| `DELETE FROM _zero_pending_changes` after promotion                        | 1    |

Plus three rows per transaction regardless of size: the `_orez_tx_schema` guard
in and out, and the `_zero_change_state` watermark bump. Add `N_indexes` to the
application row on top of all of this.

Four of the six are a row's round trip through two staging tables, each written
and then deleted. That is the shape to attack if this cost ever needs to come
down, and it is worth knowing it is four rows rather than the one the ledger
packing already handles: `_zsync_log_segments` writes one row per transaction,
so the ledger is not what scales here.

Two things that are NOT available, both verified against local workerd on
2026-08-21 rather than reasoned about:

- **The staging tables cannot move off durable storage.** `CREATE TEMP TABLE`,
  `ATTACH DATABASE ':memory:'`, and `PRAGMA temp_store` all fail with
  `SQLITE_AUTH`. A trigger body naming a temp table is accepted when the trigger
  is created and then fails when it fires, reporting the table missing under
  `main`, because unqualified names inside a trigger body resolve against the
  main database. There is no unbilled scratch surface inside a Durable Object.
- **Cloudflare exposes no write-ahead log.** Its own WAL is what backs
  point-in-time recovery, but the only handles on it are the opaque bookmark
  APIs (`getCurrentBookmark`, `getBookmarkForTime`,
  `onNextSessionRestoreBookmark`). There is no readable change stream, no
  `sqlite3_wal_hook`, no session extension, and no user-defined functions
  callable from a trigger body. Writing images to a table and reading them back
  is the only capture mechanism the platform allows, which is why the buffer
  exists at all.

Reads are the cheap side of this meter by three orders of magnitude: rows
written bill at $1.00/million against $0.001/million for rows read, both after
generous included tiers (50 million writes and 25 billion reads per month). Any
restructuring that converts a capture write into a capture read is close to
free. `INSERT`/`DELETE ... RETURNING` hand back the affected images at no extra
write cost, though `RETURNING` sees only the statement's own target table, so
it cannot observe rows a foreign-key cascade touched. That is the open question
against removing a staging hop, not a settled design.

At current volumes none of this costs anything. The 50-million-writes included
tier covers roughly 8 million captured changes per month before a single dollar
is billed, so this is a number to watch as a namespace grows rather than a
problem to fix today.

### The circuit breakers

Because a runaway writer bills real money and can wedge an object, the system has
defense in depth (`plans/orez-write-safeguards.md`). All three are independent,
so a failure in one does not disable the others.

1. **Data-worker write budget** (`packages/orez-lite/src/cf-do/worker.ts`). The source `ZeroSqlDO`
   meters billable rows in a rolling window. Past `OREZ_DO_WRITE_BUDGET_ROWS`
   (default 150,000 per five minutes) it becomes sticky and mutating endpoints
   return HTTP 429 `writeBudgetExceeded`. Reads stay open, the trip is persisted
   so an eviction cannot quietly reopen it, and a `POST /_orez/write-budget/reopen`
   with the admin token clears it. The 150k default sits below soot's external
   200k-per-five-minute alert.
2. **Sync-host ingest breaker** (`packages/sync-cf-host/src/host.ts`). One
   breaker catches two signatures: more than `ingestBudgetRows` billable rows in
   the window, and a non-advancing upstream cursor while pages keep arriving
   (`ingestCursorStalled`), which is the signature of a partial boot replaying
   forever. It returns a structured 429 and backs off with capped exponential
   cooldown.
3. **Delegated push bounds** (`packages/sync-cf-host/src/host.ts`). Delegated
   pushes retry only transport failures, 429, and 5xx, at most
   `delegatedPushRetry.maxAttempts` times (default 3), so a failing app endpoint
   cannot become an internal hot loop. An attempt that used its whole
   `timeoutMs` is terminal: retrying a hang repeats its full cost and pushes the
   host's answer past the deadline the waiting client is holding. See
   `docs/sync/configuration.md` for how to size `timeoutMs` against that
   deadline.

The budgets deliberately keep the hot-path counter in the worker layer and
persist only the sticky trip state. Metering every write into a table would add
billed writes and amplify the very incident it is meant to contain.

Sampled application SQL transactions emit one structured
`orez_sql_transaction_sample` with a physical-row breakdown (application table
and operation, private versus synced, indexes, CDC buffer, pending changes,
changefeed, fixed bookkeeping). The physical total is the same post-consume
`rowsWritten` meter as the circuit breaker. Attribution is logs only: it adds
zero SQLite rows and does not change `_zero_changes`. Cloudflare Workers logs
may drop events, so a partial capture is not an exact object-level total.

### Transaction rollback must not copy hot tables

The data worker emulates Postgres transactions over Durable Object requests.
For parsed `INSERT`, `UPDATE`, and `DELETE` statements it journals row
before-images transactionally through generated SQLite CDC triggers. Published
application changes become visible as one group at commit; private zero-cache
CVR/CDB changes use the same journal with `publish: false` and are retained only
long enough to support rollback and crash recovery. Full-table snapshots are a
fallback for writes the compiler cannot classify, not the normal DML path.

This distinction matters to billing. A 2026-07-13 Chat cold-start profile
recorded 1,191,374 billable rows, of which 1,077,552 came from 1,244 transaction
snapshots of a growing internal `cdc_changeLog`. The Chat seed itself was not
hundreds of thousands of rows. After routing internal parsed DML through the
row journal, a clean global setup completed at 125,402 rows under the existing
150k circuit. Keep that circuit as a regression guard; increasing it would hide
quadratic snapshot amplification rather than fix it.

## Cookie domain and cutovers

The cookie a client stores is the engine's change-log watermark, and a Zero
client persists its local store keyed to a server identity. When you move an app
to a different sync host origin, or start a fresh engine with a fresh watermark
domain, existing clients hold cookies from the old server that do not correspond
to the new engine's watermark. Those clients reset and re-snapshot on their next
pull.

This reset is cheap and correct. It is the same snapshot path used for a fresh
client or a below-floor cookie, and measured project snapshots are small. It is
also user-visible: a client that had local state throws it away and rebuilds from
a full snapshot once. Plan cutovers around it. Chat's staging already ran on the
rust cookie domain, so starting fresh there was free; the apex cutover inherits
the reset-on-cutover client story deliberately rather than trying to preserve
cookies across the domain change (`plans/rust-sync-upstream-ingest.md`).

## Ledger format 2 is a one-way migration

Orez 0.15 moved last-mutation-ids out of the packed ledger payload and into
`_zsync_clients`, and bumped the payload to format 2. The schema pass migrates a
format-1 namespace in place on the first boot after the upgrade: it copies the
active segment's `lmids` map into `_zsync_clients`, rewrites every retained
segment to format 2, and leaves the version boundary alone. The migration is
idempotent and runs inside the single `transactionSync` that wraps the whole
schema pass, so a crash part-way rolls all of it back and the next boot retries
from the original state.

**There is no rollback.** A 0.14.4 engine rejects a format-2 payload outright,
so pointing an older build at a migrated namespace fails every pull and every
push with `packed ledger format is unsupported` (HTTP 500). Roll forward to fix
a problem found after the upgrade; do not roll the host back. Plan the upgrade
as a one-way door per namespace.

What the migration is worth doing, from the operator's side, is that it also
repairs two states that permanently wedged namespaces on 0.14.4:

- **An oversized lmid map.** Once the map alone crossed the 768 KiB rotation
  threshold, the active segment could sit empty (`startVersion == endVersion +
1`) and still be over the threshold, so every write tried to rotate and its
  insert collided with the active row's own primary key
  (`UNIQUE constraint failed: _zsync_log_segments.startVersion`). Moving the map
  out of the payload takes the segment back under the threshold. Both rotate
  paths, the SQLite trigger and the engine, hit this and both recover.
- **An orphaned `captureMode = 1`.** The executor toggles that column through
  plain SQL mid-transaction, so a delegated push abandoned between the toggle and
  its commit used to leave it set forever. Every later push then failed with
  `packed ledger has uncommitted capture state`, and on the engine path the
  trigger bodies (gated on `captureMode = 0`) silently dropped the next write's
  change envelope while still writing its row, so the row went live and no client
  could pull it. The transaction journal restores the column on session disposal
  now, and the schema pass clears whatever a namespace was already stuck with.
  It leaves the column alone while `_orez_tx_manifest` holds rows, because those
  rows mean a transaction is still in flight and its own commit or rollback owns
  the toggle.

Pending trigger keys are not cleared by any of this. The engine drains them into
the next transaction, which is the correct handling; deleting them would discard
changes clients still need.

## Where this engine fits, and where it does not

It fits an app that wants Zero's client experience without operating Postgres and
zero-cache: per-namespace data that fits a Durable Object, a moderate per-object
write rate, and a schema whose index and cascade cost you have looked at. It is a
poor fit for a single namespace with a very high sustained write rate, a data set
too large for one object, or a workload dominated by wide cascading deletes,
unless you have budgeted the billable-row cost of those deletes up front.
