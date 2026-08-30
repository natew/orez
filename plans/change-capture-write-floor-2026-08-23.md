<!-- plan: status=active owner=m9426 reviewed=2026-08-24 -->

# Reduce Orez change-capture physical writes

date: 2026-08-23, revised 2026-08-24
consumer plan: `/Users/n8/soot/plans/contrast/cloudflare-spend-cut-2026-08-23.md`

## Objective and priority

Reduce the physical Durable Object rows written for a captured application
change without weakening transaction isolation, rollback, foreign-key cascade,
trigger, ordering, retry, or restart behavior. Land one capture path. The first
implementation hypothesis is removal of `_zero_pending_changes` insert/delete,
which would move the measured marginal slope from six toward four. A two-row
floor remains research until one universal transaction transport is proven.

Priority, stated honestly: there is no current dollar case. Contrast's total
`do.rowsWritten` is $0.130 per day, and the Contrast liveness fix no longer waits
on this plan; it removes the dominant capture traffic upstream by coarsening
its own cadence. What this plan is worth now is smaller and different: the
per-change physical slope matters at future scale, shorter write sessions
reduce write-grant queueing for every Orez consumer, and the journal-bound
work in stage 2 is a named prerequisite for Contrast's control-plane
decomposition trigger (the 2026-08-23 inspection found 164,517 retained
`_zero_changes` rows with no active writer, and sharding application tables
cannot fix an unbounded infrastructure journal). Stage 2's retention
verification does not depend on the stage 0 matrix and may run first.

This plan owns Orez implementation and evidence. The Contrast plan owns factory
liveness cadence. Application sharding is not acceptance for an Orez journal
that remains amplified or unbounded.

No production release, resnapshot, migration, or data mutation is authorized
by this plan. Publishing Orez always requires Nate's explicit permission.

## Measured baseline

For the existing one-row captured-write fixture, the current marginal source
cost is six physical rows:

1. application row plus its `_orez_cdc_buffer` trigger insert: two rows;
2. delete the drained CDC-buffer row: one row;
3. insert `_zero_pending_changes`: one row;
4. insert the final `_zero_changes` row: one row; and
5. delete the pending row: one row.

That six-row value is a measured marginal slope for the fixture, not a complete
transaction price. Each application transaction also pays fixed three-row
overhead, and every changed application index adds physical rows. Acceptance
therefore records both the marginal slope and total rows for each fixture.

The existing `perf/write-amplification/harness.ts` and workerd write meter are
the starting evidence. `SqlStorageCursor.rowsWritten` or SQLite
`total_changes()` must be read after cursor consumption. `changes()` is not a
billing counter because it excludes trigger-side rows.

## Constraints from the current transport

- `createApplicationSqlClient.transaction(async tx => ...)` opens an
  `ApplicationSqlSession`, then runs arbitrary asynchronous JavaScript in the
  application worker. Its later query/exec RPCs may branch on rows returned by
  earlier RPCs. That closure cannot be moved into one Durable Object call by
  serializing SQL text alone.
- `_orez_cdc_buffer` observes trigger and foreign-key side effects in SQLite.
  `RETURNING` cannot observe foreign-key cascade changes, so it cannot replace
  the buffer universally.
- The multi-RPC transaction needs a durable rollback/recovery journal until
  commit. Removing pending storage is correct only if the CDC buffer can own
  that role across failure and restart.
- There may be only one capture implementation. Do not add a direct fast path
  with the current CDC/pending path as a fallback.

## Stage 0: pin the physical-cost and correctness matrix

- [ ] Extend the real-workerd harness to report application rows, application
      index rows, CDC rows, pending rows, final changefeed rows, fixed transaction
      rows, and total physical rows separately.
- [ ] Prove the old path's measured marginal slope and fixed overhead before
      changing it. A new cost check must first fail for the intended extra rows.
- [ ] Cover direct insert, update, delete, and upsert with zero, one, and
      multiple application indexes.
- [ ] Cover foreign-key cascade, set-null/default effects, recursive cascades,
      trigger-side insert/update/delete, and multiple changes to one primary key.
- [ ] Cover several statements whose later branch depends on an earlier
      returned row, statement failure, explicit rollback, duplicate commit retry,
      and ordering of interleaved writes.
- [ ] Interrupt the Durable Object between statements, while preparing commit,
      and after final insertion but before the client observes the acknowledgement.
      Restart must either resume idempotently or roll back completely.

## Stage 1: test the six-to-four hypothesis

Use `_orez_cdc_buffer` as the single transaction-local capture and recovery
journal for the multi-RPC session. The proposed change removes
`_zero_pending_changes` insertion and deletion; commit drains the transaction's
ordered buffer into `_zero_changes` exactly once. Rollback removes the buffered
transaction and restores application state atomically.

- [ ] Define the buffer's transaction identity, ordering key, uniqueness, and
      recovery state explicitly. Do not rely on connection memory.
- [ ] Prove that concurrent application sessions cannot see or promote one
      another's buffered changes.
- [ ] Prove an acknowledgement loss retries the same commit identity without a
      duplicate changefeed row or a second application effect.
- [ ] Prove rollback and dead-session recovery leave no application mutation,
      pending visibility, or orphaned journal rows.
- [ ] Run the complete Stage 0 matrix and the existing Zero compatibility,
      browser sync, Rust ingest, snapshot, and recovery suites.
- [ ] Claim six-to-four only if the measured marginal slope is four across the
      matrix and total rows equal the documented fixed plus index costs.

## Stage 2: bound retained internal state

- [ ] Give the surviving CDC/recovery journal explicit row, byte, and age
      bounds across normal commit, rollback, lost acknowledgement, Durable Object
      restart, and abandoned session.
- [ ] Make caught-up and idle maintenance write zero rows.
- [x] Expose read-only attribution that separates application, capture,
      recovery, and changefeed physical rows without adding per-change telemetry
      writes. Sampled `orez_sql_transaction_sample` events now include that
      breakdown from post-consume `rowsWritten`. Emission is structured Workers
      logs only and writes zero SQLite rows.
- [ ] Verify `_zero_changes`, transaction manifests, and snapshot tables remain
      within their existing documented retention contracts.

## Stage 3: investigate the two-row floor (deprioritized research)

The theoretical marginal floor is the application row plus one final
changefeed row in the same SQLite storage transaction. Reaching it universally
requires a serializable transaction-program contract or a co-located
authoritative mutator boundary. With no cost pressure behind it, this stage
runs only after stages 0 through 2 are complete and only if a consumer names
a concrete need.

- [ ] Inventory every application transaction shape in Orez consumers,
      including data-dependent branching, loops, conditional writes, returned
      values, and non-SQL asynchronous work inside callbacks.
- [ ] Specify a typed declarative program capable of every supported shape, or
      reject the approach. It must preserve parameter typing, returned-row
      branching, rollback, cascades, triggers, ordering, idempotent retry, and
      restart behavior.
- [ ] Prototype against the hardest dynamic shapes first. A subset green path
      is not sufficient evidence.
- [ ] Migrate all transaction senders together and remove the old protocol, or
      do not land the new protocol. There is no permanent capability/fallback fork.
- [ ] Measure the resulting marginal and total slopes. Two is not an accepted
      result until the physical meter proves it.

## Acceptance

- The complete Stage 0 matrix preserves byte-for-byte application results and
  ordered changefeed results across commit, rollback, retry, and restart.
- The six-to-four stage removes both `_zero_pending_changes` physical effects
  from every supported captured transaction and introduces no replacement
  staging writes elsewhere.
- Idle and caught-up objects write zero rows.
- Physical totals are expressed as measured marginal rows plus fixed
  transaction and application-index costs; no logical statement count is
  presented as a Cloudflare bill.
- Rust ingestion receives each final change exactly once and does not ingest
  buffer or recovery state.
- Contrast may consume the Orez change only after this plan's relevant stage is
  complete, published with permission, pinned in Contrast, and validated against
  Contrast's transaction/cascade fixtures. Nothing on the Contrast side waits on any
  stage of this plan; the dependency runs the other way, with Contrast's
  decomposition trigger consuming stage 2's journal-bound proof.

## Validation receipts

- [x] Zero-write attribution receipt: workerd fixture matrix reconciles each
      transaction breakdown to physical `rowsWritten`; enabling
      `OREZ_SQL_TELEMETRY_SAMPLE_RATE=1` adds zero SQLite rows and leaves
      `_zero_changes` identical. Local only; no publish.
- [ ] Baseline physical-write receipt.
- [ ] Six-to-four falsification matrix receipt.
- [ ] Orez lint, format, typecheck, unit, workerd, and Rust compatibility
      receipts required by the changed packages.
- [ ] Published-version receipt and Contrast consumer pin, only after explicit
      publish approval.
