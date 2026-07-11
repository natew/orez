# Draft zbugs report: writeMutationResult is not replay-idempotent (custom mutators + push)

For natew to file at bugs.rocicorp.dev — found on @rocicorp/zero 1.7.0 in
production (sootbean.com) during factory stress testing on 2026-07-11.

---

**Title:** Push processing 500s with `UNIQUE constraint failed` when recording
a mutation result for an already-recorded (clientGroupID, clientID, mutationID)

**Version:** @rocicorp/zero 1.7.0, custom mutators with a push endpoint
(zero-server `zql-database`), SQLite-backed upstream.

**What happens:** `writeMutationResult` (zero-server/src/zql-database) does a
bare `INSERT INTO <upstreamSchema>.mutations (clientGroupID, clientID,
mutationID, result)`. When a client resends a mutation id that already has a
recorded result — which the protocol allows and which happens in practice
whenever an app-level error path triggers the "retry without mutator" flow, or
a client replays after a dropped response — the INSERT hits
`UNIQUE constraint failed: mutations.clientGroupID, mutations.clientID,
mutations.mutationID (SQLITE_CONSTRAINT_PRIMARYKEY)` and the entire push fails
with `kind=PushFailed reason=database`.

**Impact:** one replayed mutation id poisons the whole push body. The
PushFailed response also carries no `mutations` array, so any infrastructure
that validates the push-response shape rejects it; the client then retries the
same mutation id and the failure loops.

**Observed sequence (production):** app-level error on mutation N → zero
retries without mutator and records the app error result for N → client
resends N (expected: alreadyProcessed) → `writeMutationResult` re-INSERTs →
UNIQUE constraint 500 → `PushFailed reason=database` for the entire batch.

**Expected:** recording a result for an already-present (clientGroupID,
clientID, mutationID) should be idempotent — `ON CONFLICT DO NOTHING` (or
upsert with the same result), matching the alreadyProcessed semantics the rest
of the push path implements.

**Workaround we ship:** treat structured PushFailed bodies as forwardable
rather than a transport error, and fix the app-level error paths so replays
are rare. The bare INSERT remains a latent footgun for any custom-mutator app.
