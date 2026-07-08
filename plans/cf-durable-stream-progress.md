# CF durable stream progress follow-up

## Context

Soot's July 2026 Cloudflare write-cost incident was triggered by a per-minute
demo-worker warm cron that repeatedly booted the embedded Zero/Orez data worker.
Removing the cron stopped the acute spend, but the durable upstream concern
remains: a data-worker boot or reconnect must not re-stream a retained
`_zero_changes` backlog after Zero has already durably consumed it.

Current Orez `0.4.44` already contains the first safety layer:

- `src/replication/change-tracker.ts` records streamed batch LSNs in
  `_orez._zero_streamed_batches`.
- `src/replication/handler.ts` confirms and purges streamed rows only when
  Zero proves durable progress through a standby status update or a non-zero
  `START_REPLICATION` resume LSN.
- Fresh `CREATE_REPLICATION_SLOT` purges rows covered by the initial-sync
  snapshot.
- The stale-unconfirmed reconnect loop now keeps streams open while Zero is
  alive and sending feedback, and keepalives request immediate feedback while a
  batch is unconfirmed.

That design intentionally mirrors Postgres WAL retention. It also means the
obvious patch, "persist `lastStreamedWatermark` and resume from it after boot",
is unsafe by itself.

## Safety invariant

`lastStreamedWatermark` means "rows were sent on the replication wire". It does
not mean "Zero stored them durably". Zero sends Orez a standby ack only after
the change streamer storer commits the change log transaction and advances its
durable `replicationState.lastWatermark`.

The non-zero reconnect leg has the same proof: Zero starts replication from a
non-zero resume LSN only after its change streamer reads the committed
`replicationState.lastWatermark`, so that LSN represents durable Zero-store
progress rather than an in-memory cursor.

Therefore, a persisted stream watermark must never be used as an unconditional
skip cursor. If a worker dies after Orez streams rows but before Zero's storer
commit, those rows must re-stream on the next boot. Skipping them would lose
replication history.

## Acceptance Contract

The durable fix is done only when runtime proof covers both halves:

1. **Acked/store-durable replay is bounded.**
   - Seed a large retained `_zero_changes` backlog.
   - Start the CF embedded Zero/Orez generation and let Zero durably store the
     stream.
   - Kill/restart the embed before or during Orez's ack flush.
   - The next generation must start with a non-zero `START_REPLICATION` LSN,
     call `confirmStreamedBatches`, purge the consumed rows, and avoid
     re-streaming the full backlog.

2. **Pre-store death still re-streams.**
   - Kill the embed after Orez writes rows to the replication socket but before
     Zero's storer commits the change-log transaction.
   - The next generation must re-stream those rows under fresh LSNs.
   - This case is expensive by necessity; it is correctness-preserving and
     should be bounded by the cost monitor, not hidden by a skip cursor.

3. **Fresh replica reset remains covered by snapshot semantics.**
   - If Zero rebuilds the replica, initial sync's snapshot covers pre-existing
     upstream rows.
   - Orez must purge `_zero_changes` through the slot snapshot watermark rather
     than retaining those pre-snapshot rows forever.

## Debug First

Before adding new state, reproduce the remaining write-amplification path with
runtime probes in the CF embed harness:

- Log the raw `START_REPLICATION` LSN for each generation.
- Log `clientStartLsn`, `currentLsn`, `lastStreamedWatermark`,
  `_zero_streamed_batches` count/span, and `_zero_changes` count/span.
- Log whether Zero's local change DB has advanced
  `replicationState.lastWatermark` before the generation stops.
- Separate three cases in the logs: fresh slot, non-zero reconnect, and
  `0/0` start without a fresh snapshot.

The fix depends on which case reproduces:

- If Zero already has a durable `lastWatermark` but sends `START_REPLICATION
  0/0`, fix the Zero/Orez embed startup path that lost the existing replica or
  change DB state.
- If Zero sends a non-zero LSN but Orez does not purge, fix the
  `confirmStreamedBatches` path or the DO SQL translation for
  `_zero_streamed_batches`.
- If the stream died before Zero stored the transaction, do not skip. Re-stream
  is required.

## Candidate Implementation

Only after the reproducer proves an ack-lost/store-durable gap should Orez add
new durable progress state.

A safe version would persist both sides of progress, not only the streamed
watermark:

- `last_streamed_lsn` and `last_streamed_watermark`: diagnostic/state for what
  Orez placed on the wire.
- `last_confirmed_lsn` and `last_confirmed_watermark`: the only values allowed
  to purge or skip retained `_zero_changes`.
- `stream_epoch` or `generation_id`: helps attribute repeated boot loops in
  logs and metrics.

Use rules:

- Persist streamed progress before sending a batch, as today with
  `_zero_streamed_batches`.
- Advance confirmed progress only from Zero's standby ack or from a non-zero
  reconnect LSN.
- On boot, idempotently purge retained `_zero_changes` rows whose watermark is
  `<= last_confirmed_watermark`; this closes the death window between receiving
  a durable ack and completing the purge.
- On boot, initialize module state from confirmed progress and the oldest
  pending row, never from `last_streamed_watermark` alone.
- Keep `_zero_streamed_batches` as the LSN-to-watermark bridge for ack recovery.

## Tests

Minimum test coverage for the upstream fix:

- `change-tracker` unit tests for any new durable progress table and monotonic
  update helpers.
- DO-backend tests that run the new durable-progress table DDL and monotonic
  update statements under `DoBackend`, not only through PGlite or the CF embed
  integration harness.
- `handler` tests for:
  - non-zero reconnect purges consumed rows after a simulated process reset,
  - `0/0` fresh start does not skip unconfirmed rows,
  - persisted streamed-only progress does not suppress re-stream.
- CF embed integration test for the ack-lost/store-durable restart case.
- Existing regression tests must keep passing:
  - `a replica reset does not orphan retained _zero_changes (2026-07 CF cost incident)`
  - `consumer feedback defers the stale-unconfirmed close; silence still closes`
  - `reconnect: confirmed batches purge from the resume lsn, unconfirmed rows re-stream`

## Non-goals

- Do not add a best-effort skip based solely on `lastStreamedWatermark`.
- Do not weaken Postgres-style "retain until confirmed" semantics.
- Do not add a fallback replication path or an environment toggle.
- Do not mask pre-store deaths as success; surface them through cost metrics.
