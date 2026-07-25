# Orez data-tier write amplification

## Result

The local workerd reproduction now uses exact `SqlStorageCursor.rowsWritten`
values after cursor consumption. On a fixture with one project, 40 tasks, 200
comments, primary keys on every table, and secondary indexes on both foreign
keys:

| Flow                                             | Before | After |     Reduction |
| ------------------------------------------------ | -----: | ----: | ------------: |
| One-row tracked push                             |    281 |    18 | 93.6% (15.6x) |
| Create project plus 12 tasks                     |    582 |   144 |  75.3% (4.0x) |
| Cascade-delete 1 project, 40 tasks, 200 comments | 32,594 | 1,477 | 95.5% (22.1x) |

The harness is `perf/write-amplification/harness.ts`. It runs the real `ZeroDO`
under local workerd, drives it through `DoBackend`, requests statement-level
metering, checks transaction rollback, and prints the complete breakdown.

## Root cause

Three costs compounded:

1. The PostgreSQL transaction adapter copied `_zero_changes`,
   `_zero_change_state`, and the watermark sequence into rollback snapshot
   tables on the first tracked write. Tracked rows already stay isolated in
   `_zero_pending_changes`, so these copies were unnecessary. A one-row push
   copied 241 retained change rows and paid 243 billable rows for that statement
   alone.
2. Watermark sequence discovery matched every table whose name contained
   `zero_watermark`, including the rollback snapshot. `CREATE TABLE AS` does not
   preserve the source primary-key constraint. For each committed tracked row,
   `INSERT OR IGNORE` therefore inserted another duplicate `dummy = 1` row into
   the snapshot, and the following `UPDATE ... WHERE dummy = 1` rewrote every
   duplicate accumulated so far. The cost grew as 1 + 2 + ... + N. In the
   241-row cascade, those snapshot updates alone wrote 29,402 rows.
3. Pending changes were promoted one at a time. Every row repeatedly updated
   `_zero_change_state` and the sequence table. The 241-row cascade issued 723
   state updates before the fix.

The secondary-index cost is visible in the same statement counters. Inserting
12 task rows wrote 36 billable rows: 12 table rows, 12 primary-key entries, and
12 `task_project_id` entries. No removable secondary indexes exist on the
tracking tables. `_zero_changes` and `_zero_pending_changes` use integer primary
keys, and the remaining write cost is the data needed for ordering and rollback.

## Change

- Tracked application writes no longer snapshot committed change-feed tables.
  Rollback still restores every application table and deletes the transaction's
  pending change rows atomically.
- Watermark discovery excludes all `_orez_tx_*` rollback tables.
- Watermark tables initialize once per object instance, and unchanged reads no
  longer rewrite `_zero_change_state`.
- Commit promotes a transaction's pending rows with one ordered
  `INSERT ... SELECT ... RETURNING` and advances watermark state once.
- An opt-in `x-orez-measure-writes: 1` diagnostic response exposes the exact
  per-statement counters used by the harness. Ordinary endpoint responses are
  unchanged.

## Production signature

The soot control namespace tripped at 301,642 billable rows for only 25 logical
rows while there was no wave load. That roughly 12,000x ratio matches the local
triangular signature: a modest tracked-row count can repeatedly grow and rewrite
the constraint-free watermark snapshot until it consumes the whole rolling
budget. The local cascade reproduced the same shape before the production alert,
with 29,402 of 32,594 rows coming from that one triangular update loop.

The previously observed anonymous-account delete cost about 127,500 rows. The
same measured 95.5% reduction projects that shape to about 5,800 rows. An initial
prune limit of 25 accounts would project to about 145,000 rows, below half of a
300,000-row window. Start at 25, confirm the production per-account distribution
from the exact meter, then raise it further if the measured tail permits.

## Operation-aware side-effect discovery (2026-07-24)

A second amplification with the same shape, found in soot's factory heartbeat.

`snapshotSideEffectWriteTables` copies every table SQLite can write implicitly
for a tracked statement, because those rows carry no before-image of their own.
It discovered them from the source _table_ alone: any foreign key with a
non-restricting `ON UPDATE` **or** `ON DELETE` action produced an edge, and any
trigger on a reachable table produced an edge regardless of its event.

`agentEvent` references `sootAgent` `ON DELETE CASCADE`. Soot renews
`sootAgent.runtimeClaimExpiresAt` every five seconds with a 20-second TTL, and
an UPDATE cannot fire an `ON DELETE` action — but every heartbeat still copied
the entire `agentEvent` table. The cost of one renewal grew 1:1 with retained
events until heartbeats alone consumed the 300,000-row budget. `agentEvent` also
cascades from `project` and `user`, so ordinary lifecycle updates to either paid
the same copy.

Reachability is now tracked per `(table, operation)`:

- `ON DELETE CASCADE` reaches the child as a DELETE, `ON DELETE SET NULL` /
  `SET DEFAULT` as an UPDATE, and only when the parent is deleted.
- `ON UPDATE CASCADE` / `SET NULL` / `SET DEFAULT` reaches the child as an
  UPDATE, and only when the parent is updated. Which columns the statement
  touched is not known at this layer, so any parent update follows the edge.
- An INSERT fires no referential action.
- An UPSERT resolves to an insert or an update and only SQLite knows which, so
  it seeds both.
- A trigger fires only for its own event, parsed from the `CREATE TRIGGER`
  header. Its body targets stay conservative: reading which operation the body
  performs would have to model `REPLACE` conflict resolution deleting rows, so
  every operation on the target is assumed. Only whether the trigger fires at
  all is decided precisely. An unreadable header keeps the existing all-table
  fallback.

Measured against soot's real application schema through the real application SQL
path, one claim renewal (soot `test/cloudflare-write-budget.test.ts`):

| retained `agentEvent` rows | before | after |
| -------------------------- | -----: | ----: |
| 0                          |     42 |    22 |
| 1,000                      |  1,042 |    22 |
| 5,000                      |      — |    22 |

The cost is now flat in retained events instead of growing 1:1.

## Validation

- The harness verifies three one-row updates in a storage transaction each
  report one row, proving counters are statement-local rather than cumulative.
- Its rollback phase updates a published row, rolls back, verifies the original
  value, verifies `_zero_pending_changes` is empty, and proves the committed
  change count, change state, and sequence row are byte-for-byte unchanged.
- Its project-create phase checks the 13 bulk-promoted rows retain insertion
  order and receive consecutive, unique watermarks in real workerd.
- Unit coverage asserts rollback snapshots are never treated as watermark
  sequences and that tracked transactions remain hidden until bulk commit or
  disappear on rollback.
- `src/pg-proxy-do-backend.test.ts` asserts application snapshots remain while
  committed change-feed tables are absent from the rollback journal.
