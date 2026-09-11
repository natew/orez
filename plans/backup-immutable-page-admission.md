<!-- plan: status=active owner=r22350 reviewed=2026-09-07 -->

# Read immutable backup pages through their snapshot lease

Approved by r22361 after Opus r22395 reviewed the assembled contract. Implementation is authorized; upstream publication remains unauthorized. The 50-client load and its blocking-session diagnosis retain priority.

## evidence and scope

- RAN the supplied natural 00:30 backup capture: two singleton exports each exhaust three background read sessions, then report `session_preempted`; their work phases last 1.184 to 5.369 seconds and their begin phases 91 to 108 ms. The competing writer IDs are absent. Receipt: `/tmp/launch-backup-natural-result-r22339.md`.
- TESTED namespace-backup.test.ts (21) and worker-transaction.test.ts (22), 43 passing tests; this receipt did not run the physical-snapshot suite: an arriving writer immediately preempts a background reader; an interrupted scan can discard its export. Receipt: `/tmp/launch-backup-admission-r22350.log`.
- READ `worker.ts`: snapshot creation first acquires writer admission and copies the tables in one synchronous SQLite transaction. Snapshot tables have generation-specific names, neutral columns and no source triggers. A second snapshot is rejected while the first lease owns its generation. Tests cover waiting for commit and rollback, failed cleanup admission, and an old generation's cleanup preserving a new generation.
- READ `namespace-backup.ts`: an 8 MiB scan chunk performs multiple page RPCs inside one background application read session. It reads only physical snapshot tables. A write can invalidate that whole chunk although it cannot change those rows. Three failed attempts abort multipart upload. Upload completion and latest-pointer publication already happen only after a complete scan.
- INFERRED repeated writer arrival explains the observed export preemptions. It does not identify the particular writers or establish the cause of the separate 50-client writer-queue stall.

## proposed contract

- Add `readPage(table, afterRowid, limit)` to the existing snapshot lease contract in `namespace-backup.ts`; implement it on `BackupSnapshotLease` in `worker.ts`. Keep snapshot creation and explicit awaited drop under existing writer admission.
- Bind the lease to its snapshot ID and the exact set of copied source table names. Dispatch page reads through an owner symbol, rather than a general SQL RPC. Validate the active generation, table membership, a nonnegative safe-integer cursor and an integer limit from 1 through 1000 before SQL execution. Derive and quote the physical snapshot name inside the owner. The caller cannot supply an arbitrary physical table or SQL statement.
- Execute one existing keyset page query synchronously and consume its cursor before returning. Page reads acquire no application SQL session. The application writer can remain logically active across RPCs while the lease reads the already committed snapshot; SQLite still serializes individual physical operations normally.
- Keep the parent DO stub alive for the lease lifetime, as the existing `lite-data-worker.ts` snapshot adapter does. Use a private `#owner` field on the lease. Retain the explicit `dropSnapshot` await and the lease disposal in `finally`. A drop invalidates its generation before waiting for writer admission; all later page calls on that lease fail, even if physical cleanup is still queued. Old cleanup cannot target a later generation.
- Page the export through `snapshot.lease.readPage`. Keep current row-page sizing, serialization chunks, multipart part sizes, bounded upload concurrency, hashing and restore format. A real page, transport or upload error aborts the export; no new retries or deadline changes.
- Remove the backup-only application read-session callback, preemption retries, read priorities and options, and preempted export result. Preserve general application SQL background preemption, which still protects unrelated application sessions. Migrate the public re-exports and every producer and consumer in the same change.
- In the Contrast adapter and scheduler, remove only the export-preemption handling and its retry path that this contract makes unreachable. Update subrequest estimates to count snapshot/drop, page RPCs and uploads, without four application-session RPCs per chunk. Keep spending, time, subrequest and cleanup limits unchanged. Coordinate these specific files with r22339 before implementation because that agent owns other backup fixes.

## cost and consistency

- No new schema or database migration; no additional physical writes. Snapshot copy and drop costs are unchanged. Each page still performs one bounded rowid keyset read against the copied table.
- Each scan removes four session RPCs per chunk and all preemption rereads. Retained memory adds a table-membership set bounded by the already captured schema. Existing row and multipart buffers remain bounded as before.
- A snapshot still represents one committed source state and its captured marker. Live writes after the copy affect the next export. The only owner that deletes physical copies is snapshot cleanup; a stale or disposed lease fails before it can access another generation.
- This does not fix or expand snapshot-copy admission time, customer schema migrations, inventory selection, R2 failure recovery or provider execution budgets. It makes no throughput claim without a runtime result.

## failure controls before landing

- Extend `worker-backup-marker.test.ts` against its real SQLite worker core: create a snapshot, keep a live application writer uncommitted, read several pages successfully, commit or roll back that writer, and verify the dump remains exactly the copied state. Assert page reads add no application reader or queued turn. Keep the existing creation/cleanup consistency controls. The disposal harness must retain and await actual waitUntil promises.
- Extend `namespace-backup.test.ts` with an interactive write during every page, exceeding the old three-attempt limit. Run this new control against unchanged pre-change source before removing readSession and record session_preempted. Candidate must export once, with unchanged captured rows and a correct digest. Count page reads to prove no repeated scan and assert live writes complete during the scan and multipart upload, before export completion.
- Exercise stale/disposed lease reads, invalid tables and invalid cursors/limits. They must fail before SQL access; an old lease and its delayed cleanup must not read or remove a new snapshot.
- Remove a physical snapshot table while its generation remains active; propagate the SQLite error and require export abort. Exercise page failure and multipart failure: no latest-pointer publication, settled uploads before abort, lease disposal and attempted physical cleanup. Keep exact dump/restore and cross-table consistency assertions, including nullable data and source rowid collisions already covered by the suite.
- Extend the existing `write-attribution-workerd-test.mjs` with an isolated snapshot namespace to exercise actual RPC lease methods and disposal without changing the existing attribution fixtures. Assert delayed page calls survive real R2-shaped awaits across multiple chunks and event turns while an open live writer exists, and add zero billed writes; test disposal and stale generation over real workerd RPC.
- Run affected Orez suites, workerd proof, lint, formatting and package types. Update Contrast's scheduler integration tests for the actual changed contract and subrequest count; run final static checks. No additional production traffic for this proposal. A reviewed implementation still needs explicit upstream publication authorization.

## files and review decision

Orez: `packages/orez-lite/src/cf-do/{worker,namespace-backup,lite-data-worker}.ts`, their existing backup/worker tests, `src/cloudflare-runtime.ts`, and the existing workerd fixture. Contrast: `src/deploy/{cloudflareDataWorker,namespaceBackupSchedule}.ts` and their existing scheduler tests, scripts/ops/cf-backup-drill.ts, test/cf-backup-drill.test.ts, and scripts/ops/cf-backup-coverage.ts including historical summary interpretation. Durable rationale belongs in `docs/sync/trade-offs.md` once implemented.

Requested reviewer decision: approve the snapshot lease as the sole bounded page-reading capability, including its generation invalidation and admission-free consistency argument, or identify the missing invariant before implementation. Review must cover the assembled owner and consumer contract once. The unchanged 50-client run proceeds independently as soon as its diagnostic deployment is live.
