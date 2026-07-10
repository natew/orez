# zero sync-server rewrite design review — 2026-07-09

## verdict

**HOLD production-composition steps 2–4.** The reference core's final-state
cursor-diff algorithm is sound under its stated transaction assumptions, and
the two-transaction application-error path does not expose partially committed
row state. The production primitive is not ready to compose as written,
however. There is one immediately reproducible format mismatch that can make it
silently drop every ordinary production change, and the dual-consumer
retention/epoch contract is not yet implementable from the current plan without
either replaying work to zero-cache or forcing cursor clients into repeated
snapshots.

Keep `cursorDiffPatch()` as the small delta reducer, but add a production-log
adapter and an integration lane over the real `DoBackend`/`_zero_changes`
before building the soot endpoint.

## findings (severity ordered)

### BLOCKER 1 — production table names do not match the primitive's lookup keys; changes are silently skipped

`cursorDiffPatch()` does an exact lookup and treats a miss as a non-synced
table (`src/cf-do/cursor-pull.ts:58-64`). Production pg-proxy writes are logged
with schema-qualified names: `trackingRequest()` emits `<schema>.<tableName>`
(`src/pg-proxy-do-backend.ts:6060-6067`), the DO
reader preserves that string (`src/cf-do/worker.ts:942-956`), and an existing
integration assertion pins `public.foo` / `public.bar`
(`src/worker/worker-integration.test.ts:173-188`). The Zero schema maps used by
the reference/harness are keyed by client table names such as `item`, not
`public.item`.

I reproduced the current behavior directly:

```text
change.tableName = "public.item"
tables = {item: {primaryKey: ["id"]}}
cursorDiffPatch(...) => []
```

If the host then returns the captured high watermark, that row is skipped
forever. This is not just a `public.` stripping issue: the adapter must map
production server identity -> physical SQLite table -> Zero/client table name
so future server/client name mappings cannot corrupt patches.

Required before soot endpoint work:

- Make table identity explicit in the primitive or add a production adapter
  that normalizes `_zero_changes.table_name` and emits the client table name.
- Reject/metric an unknown _published_ table instead of silently treating it as
  an internal table. Internal-table filtering should use an explicit allowlist
  or classifier.
- Add a fixture using actual production-shaped `public.<name>` change rows. The
  current seven tests all use unqualified names
  (`src/cf-do/cursor-pull.test.ts:8-19`).

### BLOCKER 2 — the shared-log dual-consumer retention contract is not defined yet

The plan says cursor pulls will use the floor maintained by zero-cache's
existing retention contract during coexistence, then switch to cursor-owned
retention (`plans/zero-server-rewrite.md:176-180`,
`plans/zero-server-rewrite.md:213-221`). Current zero-cache retention physically
deletes `_zero_changes` as soon as its durable replication ack is confirmed
(`src/replication/change-tracker.ts:343-375`). Conversely, zero-cache reconnect
defines "pending" from `MIN(watermark)` in that same physical table
(`src/replication/change-tracker.ts:397-411`). There is no second-consumer
cursor or durable physical-retention floor.

That creates two incompatible naive implementations:

- Keep zero-cache's current purge: correctness is possible only if the cursor
  endpoint atomically derives/persists the purged prefix and snapshots every
  client below it. Under active zero-cache, the embed can purge before the next
  HTTP poll, so most changed pulls can degrade to full snapshots. The current
  delta/load results do not measure this coexistence mode.
- Retain rows for cursor clients: zero-cache reconnect sees those retained rows
  as pending and re-streams already-confirmed history, recreating the write-cost
  problem the durable-stream contract fixed.

Pick and document the rollout behavior before step 2. The lowest-machinery
coexistence choice is probably: zero-cache remains the sole physical-retention
owner; the cursor endpoint derives a cursor floor atomically and accepts
snapshot fallback during the short overlap. Measure snapshot frequency and
bytes under a live embed. If cursor retention is required during overlap, add a
persisted zero-cache-confirmed watermark and teach replication resume to skip
cursor-retained rows; do not infer zero-cache pending state from table `MIN`.

Whichever option is chosen needs a test with an active zero-cache consumer that
acks/purges while an HTTP cursor client pauses and resumes. None of the
`orez-local`/`orez-cf` lanes use the production log or a second consumer.

### BLOCKER 3 — the reference epoch marker cannot be copied to production, and revocation ordering is unresolved

The reference log admits `op = 'marker'` and `invalidate()` inserts one before
raising the floor (`src/sync-server/sync-server.ts:169-177`,
`src/sync-server/sync-server.ts:213-227`). Production `_zero_changes` permits
only `INSERT|UPDATE|DELETE` (`src/cf-do/watermark.ts:24-27`), yet build-plan
step 2 still says "one marker row + floor bump"
(`plans/zero-server-rewrite.md:181-184`). Copying the reference implementation
will fail the CHECK constraint. Raising only a separate floor is also
insufficient if the watermark does not move: `handlePull()` returns
`unchanged` before consulting the floor (`src/sync-server/sync-server.ts:274-298`).

The production design needs a concrete, atomically ordered epoch mechanism:

- a valid monotonic cookie change that cannot confuse zero-cache's consumer;
- a separate logical cursor floor/epoch (never physical deletion of
  zero-cache-unconfirmed rows);
- floor/epoch evaluation before the same-cookie fast path; and
- an atomic or crash-recoverable relationship between the access change and
  epoch publication.

The last point matters because soot proves project access from the control
graph and then disables the project client (`~/soot/src/zero/client.tsx:384-427`),
while project data/epoch lives in another DO. A crash after revocation commits
but before the project epoch bump leaves a caught-up client with a valid old
cookie. A 403 on later pulls also does not itself prove that already-cached rows
were cleared. Define and test the revocation outcome explicitly: server denies
new access, the active client stops reconnecting, and local project queries can
no longer expose the revoked project's cached rows.

### HIGH 4 — cookie correctness depends on an uncapped, atomic change read; the build plan does not pin this

The reference core captures `current`, reads all `watermark > cookie` rows, and
live-reads their final state in one SQLite transaction
(`src/sync-server/sync-server.ts:270-300`,
`src/sync-server/sync-server.ts:318-352`). That is correct. The production
primitive has no watermark on its input/output and cannot tell whether its
`changes` array is complete (`src/cf-do/cursor-pull.ts:47-75`). Existing readers
already have caps: the DO `/changes` endpoint slices at 10,000
(`src/cf-do/worker.ts:580-589`) and the generic tracker defaults to 50,000
(`src/replication/change-tracker.ts:275-283`).

If the soot endpoint reads a capped prefix but returns
`DurableWatermarkState.current()`, every omitted change is permanently skipped.
The endpoint contract must require one of:

- fetch every change through a captured high watermark in the same
  `transactionSync`, then return that high watermark; or
- paginate, returning the last included watermark as the cookie and never the
  global current watermark until the full prefix is processed.

Add an over-limit regression and a concurrent-write regression. Also cap by
response bytes, not only row count, so a retained backlog cannot exceed Worker
response/CPU limits.

### HIGH 5 — live-read is sound, but the production tracker does not always provide every touched primary key

The reducer correctly needs both new and old PKs for a PK-changing update
(`src/cf-do/cursor-pull.ts:34-45`). The main DO pg-proxy tracking path obtains
the post-write `RETURNING *` row and records `old_data = null` for every
non-delete operation (`src/cf-do/worker.ts:622-639`). Thus a PK-changing UPDATE
through the production pg-proxy identifies only the new PK; the old client row
is never deleted. The cursor unit test uses an `oldData` image that this main
production path does not produce (`src/cf-do/cursor-pull.test.ts:62-75`).

Either make published primary keys immutable and enforce/test that invariant,
or teach the tracker to capture the old PK before an UPDATE. Do not claim
general PK-update support until the production-log test, rather than the
synthetic reducer test, passes.

There is a related completeness condition behind "skip non-synced tables".
Production tracking is pg-proxy instrumentation, not the reference core's
per-table SQLite triggers. A write to one table can change another synced table
through a SQLite trigger/materialization. The worker's manual
`message -> channel/thread` derived-change hook proves that this class exists
(`src/cf-do/worker.ts:667-719`). Filtering an actually non-synced log row is
fine; assuming the target statement's table is the complete change set is not.
Inventory every installed trigger/cascade/derived update in the soot project
namespace and add a production-log test for each cross-table side effect.

### HIGH 6 — authorization/visibility is outside the harness, but it is a launch-critical server contract

The stock target deploys `ANYONE_CAN_DO_ANYTHING`
(`harness/src/fixture.ts:425-432`,
`harness/src/targets/stock-zero.ts:124-127`), and the orez target supplies no
`visible()` filter (`harness/src/targets/orez-local.ts:44-50`). The smoke even
requires every differently authenticated user to see the same global project
and member counts (`harness/src/smoke.ts:139-157`). Therefore the green lanes
provide no evidence for project membership, role differences, join, revoke, or
token/auth refresh.

Build-plan step 3 must preserve the current edge authorization for every pull
and push, not only trust the client-side control query. Add at least owner,
editor, viewer, foreign-user, membership-add, membership-revoke, and stale-token
cases. The revoke assertion must include clearing/inaccessibility of already
cached data, not merely receiving a 401/403.

### MEDIUM 7 — the green shape/sweep lanes currently hide unexpected mutation failures

The client implementation of `task.toggle` reads `tx.query.task`
(`harness/src/fixture.ts:412-416`), but with the pinned client this is undefined.
In my run, both stock and orez clients repeatedly logged:

```text
ApplicationError: undefined is not an object (evaluating 'tx.query.task')
```

Shapes invokes two toggles (`harness/src/shapes.ts:119-120`) but then accepts
all settled ack results, although its comment says only the duplicate create
should reject (`harness/src/shapes.ts:132-152`). Sweep generates the same broken
mutation (`harness/src/sweep.ts:325-330`) and also accepts every settled result
without checking which rejected (`harness/src/sweep.ts:597-612`). Both lanes
still report PASS because equal failure on both sides looks like conformance.
Bench likewise counts `mutationErrors` but never fails when it is nonzero
(`harness/src/bench.ts:105-137`, `harness/src/bench.ts:185-213`).

Fix the mutator, assert exact expected success/error outcomes per write, and
make any unexpected mutation error fail every lane. Until then, toggle-driven
boolean/window churn and part of the claimed application-error coverage are
vacuous.

### MEDIUM 8 — application-error database semantics are safe, but the wire result diverges from stock

The two-transaction implementation has the right database net effect:
mutator rows + LMID commit together, an app error aborts both, and the second
transaction advances LMID without the rows
(`src/sync-server/sync-server.ts:370-409`). A crash between the transactions
leaves neither rows nor LMID, so replay is valid; a crash after the second
leaves rollback + LMID durable. I do not see a partial-row divergence window
given synchronous serialized SQLite transactions.

There are still client-observable response differences:

- The error result omits the protocol's `message` field and puts the string only
  in `details` (`src/sync-server/sync-server.ts:411-419`). The stock client then
  reports `Unknown application error: app`; this occurred in the shapes run.
  The raw unit test currently pins that incomplete shape
  (`src/sync-server/sync-server.test.ts:312-329`).
- A replay returns `{}` success rather than `error: 'alreadyProcessed'`
  (`src/sync-server/sync-server.ts:376-418`). This usually settles the client
  promise equivalently, but it is a protocol-level divergence and loses the
  original error after a response-loss/retry window.

Return `{error:'app', message, details?}` and add an actual stock-client
transport assertion for the rejected server promise. Add a failpoint test at
both transaction boundaries; the current direct test proves rollback but does
not simulate process loss.

### MEDIUM 9 — runtime request validation is too weak for a public endpoint

`handlePull()` validates only the two ID strings, not that cookie is null or a
finite non-negative integer (`src/sync-server/sync-server.ts:264-280`).
`handlePush()` checks only group/string + mutations/array and mutation type
(`src/sync-server/sync-server.ts:355-369`); it ignores `pushVersion` and does not
runtime-validate client IDs, integer/sequential mutation IDs, name, or args.
A fractional/string ID can be stored into the LMID column and leave that client
group unable to make normal progress. Stock clients are well-formed, but the
production route is authenticated public input. Parse the v51 schemas (or
equivalent strict validators) before any claim/LMID write and return the same
unsupported-version/error class as stock.

### LOW 10 — reference retention only runs after pushes

Size-bounded pruning is invoked only at the end of `handlePush()`
(`src/sync-server/sync-server.ts:425-435`). A workload made solely of
upstream/admin SQL writes grows `_zsync_changes` without bound and never raises
the floor. This does not affect today's production shared log, where
zero-cache owns purge, but it contradicts the generic reference core's stated
size bound and can invalidate long-running local/phase-3 assumptions. Prune on
pull, on a write hook, or via an explicit maintenance method exercised by the
host.

## verdict by review question

### a. Protocol correctness

**Reference algorithm: conditional pass. Production composition: hold.**

- Cookie = captured high watermark is correct only when all changes through
  that watermark and all live reads share one atomic transaction. Pin the
  no-truncation/pagination rule.
- The retention boundary (`cookie >= floor` may diff; below snapshots) is
  correct in the reference core. Production has no cursor-owned durable floor
  yet.
- The two-transaction app-error database state is safe across the inter-tx
  crash window. Fix error/replay wire fidelity and add failpoint coverage.
- Reference epoch invalidation works because its marker advances the watermark.
  The same mechanism is incompatible with production `_zero_changes`; resolve
  epoch representation and access-change ordering before implementation.

### b. `cursor-pull.ts` production assumptions

**Live-read: approve with strict transaction/completeness preconditions.
Current production adapter/filtering: reject.**

Live-read avoids row-image fidelity and correctly coalesces repeated changes,
but only after the full touched-PK set is known. Today schema-qualified table
names are silently skipped, pg-proxy UPDATE lacks old PK images, and
cross-table trigger/derived effects require an explicit completeness audit.
Filtering internal rows by the sync table spec is not itself sufficient proof
that all synced side effects were logged.

### c. Soot endpoint build plan / dual consumer

**Hold steps 2–4 until a coexistence contract is chosen and tested.**

The current table cannot simultaneously mean "rows pending for zero-cache"
and "rows retained for cursor clients" because zero-cache derives pending state
from physical rows. For a short rollout, let zero-cache own physical purge and
snapshot cursor clients below an atomically derived floor; measure the expected
snapshot amplification. A longer dual-consumer period needs separate durable
consumer progress and replication-resume changes. Epoch invalidation must never
delete zero-cache-unconfirmed rows.

Before the flip, add one soot/orez integration test that uses the actual project
`ZeroSqlDO` + pg-proxy log, active zero-cache purge, production table mappings,
LMID rows, and the exact HTTP endpoint transaction.

### d. Conformance gaps

**Strong query/load backbone; not a production-flip gate yet.** Required missing
dimensions:

1. Production `_zero_changes` composition, including qualified names, old PKs,
   cross-table effects, capped backlog, and active zero-cache purge.
2. Permissions/visibility and connected-client revocation with cached-data
   clearing.
3. Reconnect/resume with the same durable storage key and cookie, server/DO
   restart, response loss around push, future-cookie 409 recovery, retention
   fallback, and epoch fallback through a real stock client. Current targets
   use `kvStore: 'mem'` and a new unique storage key per client
   (`harness/src/targets/orez-local.ts:95-110`), so a late client is a fresh
   snapshot test, not resume.
4. Real multi-tab/shared-client-group behavior. The direct unit test manually
   supplies two client IDs (`src/sync-server/sync-server.test.ts:355-373`) but
   no stock clients share persisted group state or race pushes/pulls.
5. A `@rocicorp/zero` 1.7 canary lane. M1 explicitly leaves it open
   (`plans/zero-conformance-harness.md:277-292`) and the runbook pins only 1.6.1
   (`harness/README.md:40-44`).
6. Unexpected mutation failures must fail the lane; fix `task.toggle` before
   treating current shapes/sweep results as full mutation coverage.

## evidence run for this review

- `bunx vitest run src/sync-server/sync-server.test.ts src/cf-do/cursor-pull.test.ts`
  — **26/26 passed**.
- `cd harness && bun src/smoke.ts --target orez-local`
  — **PASS**, 10 clients, 100 server acks, cross-client + late-client oracle
  equality.
- `PATH=~/.local/share/mise/installs/node/24/bin:$PATH bun src/shapes.ts`
  — **PASS**, 22/22 shapes and incremental==fresh, but exposed the unexpected
  `task.toggle` errors described above.
- Same Node 24 path, `bun src/sweep.ts --rounds 10`
  — **PASS**, seed `2113581689`, 40 shapes / 10 rounds, also repeatedly exposed
  the ignored `task.toggle` errors.

Node 24 is required for the checked-in native Zero SQLite binary in this
worktree (module ABI 137); the machine's default Node 25 uses ABI 141.
