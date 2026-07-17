# Orez consistency checker and validation architecture

Status: accepted interface for the first executable slice. This document narrows
the broader research in `plans/zero-conformance-harness.md` to claims the system
can prove from black-box observations.

## The contract

Orez is an asynchronous client cache over a transactional authority. A client
may read an older committed snapshot while another client's acknowledged write
is still propagating. The validation suite therefore does not claim global
linearizability or strict serializability for cache reads.

The suite keeps three histories separate:

1. **Authority history.** Mutation invocations, server outcomes, LMIDs, direct
   authoritative reads, and transaction-hook faults. This history can make
   atomicity, durability, idempotency, and serializability claims.
2. **Client observation history.** Complete materialized-query observations,
   client cookies, client-group identity, resets, and convergence barriers.
   This history can make monotonic-session, read-your-writes, atomic visibility,
   authorization, and eventual-convergence claims. Wall-clock completion order
   across clients is not a serialization order.
3. **Differential trace.** Generated input and normalized outputs from the
   TypeScript reference, stock Zero, and Rust implementation. This proves
   semantic equivalence for the exercised corpus, not a general consistency
   model.

Mixing these histories would make stale but valid cache reads look like
transactional anomalies. Every checker declares which history it consumes.

## Guarantees

The acceptance gate checks these properties:

| Property                        | Evidence and checker                                                                                                                                                                                                  |
| ------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| mutation atomicity              | a known-success mutation commits its rows and LMID together; a known-failed mutation commits neither, except the specified app-error LMID advance; custom authority checker plus transaction-hook probes              |
| exactly-once application        | replaying one `(clientID, mutationID)` never applies its effect twice; an indeterminate first response may leave zero or one effect before replay and exactly one after successful replay; custom LMID/oracle checker |
| no partial client transaction   | a full-scope complete query observation never contains a strict subset of rows tagged with one committed transaction ID; custom observation checker                                                                   |
| per-client monotonic snapshots  | within one snapshot generation, a client's authoritative watermark never decreases; a lower watermark requires an explicit generation-reset event; `checkSnapshotMonotonicity`                                        |
| read your successful append     | after a successful mutation outcome, the originating client's later full-scope observation retains that unique append unless a recorded later operation removes it; custom session checker                            |
| eventual convergence            | after quiescence and a proven barrier, non-writing clients, the writer, and a fresh late client equal the normalized authority result; custom convergence checker                                                     |
| query correctness               | stock Zero, Orez, and Rust results equal for the same seed, data, writes, query AST, and phase; existing differential corpus and generated sweeps                                                                     |
| incremental correctness         | an incrementally maintained view equals a fresh hydrate after the same writes; existing sweep and query lanes                                                                                                         |
| authorization                   | observations contain all and only authorized rows; grant and revoke effects hold after the corresponding barrier; permissions checker                                                                                 |
| transactional dependency safety | the dedicated authority/list-append workload is serializable; Elle list-append checker                                                                                                                                |

Liveness is reported separately. Timeouts, recovery duration, and propagation
percentiles must never be translated into a safety result.

### Barrier definition

A convergence barrier is evidence, not a delay:

1. stop workload generation and heal every active fault;
2. resolve all known mutation outcomes, replaying indeterminate mutations with
   the same LMID where required;
3. commit a unique sentinel after those operations;
4. wait until every original client reports a complete full-scope observation
   containing the sentinel and a watermark at or beyond its commit;
5. hydrate a fresh client and read the authority;
6. compare all normalized views and record the proof vector in the barrier's
   terminal event.

Two clients agreeing with each other is insufficient. A barrier without a
fresh client and authority comparison is invalid.

## Operation-history schema v1

The executable types and validators live in
`harness/src/consistency/history.ts`. Runtime artifacts use this layout:

```text
harness/results/consistency/<run-id>/
  manifest.json
  history.jsonl
  schedule.json
  checks.json
```

`manifest.json` contains `schemaVersion: 1`, run ID, seed and its source,
versioned workload, target name and immutable build identity, and an exact
replay command plus relevant environment. Secrets are omitted. `history.jsonl`
contains one `HistoryEvent` per line in index order. `schedule.json` contains
the generated logical fault schedule and delivery receipts. `checks.json`
contains checker version, declared input history, result, violations, and any
Elle report paths.

Every logical operation has an `invoke` event and exactly one terminal event:

- `ok`: known to have succeeded;
- `fail`: known to have had no effect, or a specified application rejection;
- `info`: outcome is indeterminate, normally because the response was lost.

The pair shares `opId`, `process`, and `kind`. `index` is contiguous from zero
and run-relative `relativeMicros` never decreases. It is a safe integer because
a run cannot approach JavaScript's multi-century microsecond limit. A process
is a logically single-threaded history stream. Concurrent requests from one
real client use distinct process IDs while retaining the same `clientId` and
`clientGroupId`.

`transaction` contains list-append micro-operations for the Elle workload.
Completed reads contain their observed lists; invoked reads use `null`.
`snapshot` stores a target-normalized `generation` and authoritative
`watermark`. The watermark is a canonical decimal string in `0..=i64::MAX` and
checkers compare it with `BigInt`; a JavaScript number would corrupt valid i64
cookies above `Number.MAX_SAFE_INTEGER`. The raw protocol cookie can remain in metadata
for diagnosis, but checkers do not compare opaque JSON cookies directly.
The Elle projection maps process strings to consecutive integers in first-seen
order because Jepsen histories require numeric logical processes.

Runtime failures keep the whole directory as an untracked CI artifact. A
minimized permanent repro is committed at:

```text
harness/regressions/consistency/v1/<descriptive-name>.json
```

The committed envelope contains the manifest fields, minimized schedule and
input, expected failing checker, and fixed replay command. Regressions run
before fresh generation. A corrupt regression, missing target build identity,
or non-working replay command fails the lane rather than being skipped.

The Rust `sync-core` property lane may retain its typed trace array because the
TypeScript oracle already consumes it. Its failure envelope uses
`schemaVersion`, `kind: "sync-core-differential"`, seed, generator version,
exact command/environment, typed trace, and failing step. That input trace is
not mislabeled as a black-box observed history.

## Checker selection

### Elle CLI

Elle is used for one dedicated transactional workload. Each key stores a list;
every append value is globally unique for that key; transactions append to and
read several keys; reads return complete lists. `projectElleListAppend` projects
every event carrying list-append micro-ops (the atomic-visibility lane's `read`
and `mutation` kinds as well as the dedicated `transaction` kind) to Jepsen
JSON. When the workload runs against a store that already holds unrelated seed
rows, `src/consistency/elle-project.ts` (driven by
`scripts/elle/check-history.sh`) restricts each observed list to the values
appended within the history, so elle analyzes the tracked list-append
sub-history embedded in the store. That
restriction means elle checks dependency safety among the tracked appends and
does not detect a read of a value no transaction appended.

The checker invocation must explicitly name the model because elle-cli defaults
to strict serializability:

```sh
java -jar "$ELLE_CLI_JAR" \
  --model list-append \
  --consistency-models serializable \
  history.elle.json
```

`serializable` is the initial gate. Realtime and process-order variants are not
enabled for asynchronous cache observations. The projection can include
authority transactions and client cache transactions, but its result means
only that the observed snapshots admit a serial order. Per-client monotonicity
and read-your-writes remain explicit custom checks.

Pin the elle-cli source revision, standalone-JAR SHA-256, Java major version,
and Elle dependency revision in the runner. Before checking Orez, run one
known-valid fixture and one known-invalid cycle fixture. Treat `false`,
`unknown`, process failure, timeout, malformed output, or a self-test mismatch
as lane failure. Keep Elle's anomaly JSON/graphs with `checks.json`; manually
review a newly reported anomaly before filing a product bug, as Elle's own
documentation recommends.

Pin the official elle-cli 0.1.9 release asset
`elle-cli-bin-0.1.9.zip` at SHA-256
`7bb21b1c68580cd63816abee7655c68023b837bcca91eac9025674e4fe1ff12c`.
Its `target/elle-cli-0.1.9-standalone.jar` must have SHA-256
`c9ba9b9fd32640e73d632cb5f15069c162ba6528a67f27a878767187c59f539a`.
The source revision is `0e3fd6ea923f8c2f1ee89f153e0e413530b1fa43` and the
embedded dependencies are Elle 0.2.6 and Jepsen 0.3.11. Java 17 cannot load
the current transitive Jepsen dependency graph, so the runner requires Java 21. The official JAR returned `true`/exit 0 for the valid fixture and
`false`/`G1c`/exit 1 for the cycle fixture.

Elle does not check query equivalence, permissions, client cookies, LMIDs,
partial poke application, or convergence. Extending the projection to pretend
it does is out of scope.

### Custom TypeScript checkers

TypeScript owns protocol and cache properties because the harness already
observes stock Zero clients and target-specific barriers there. Keep checkers
pure over saved artifacts so a failure can be rechecked without rerunning the
system. The first executable slice validates event pairing, process
single-threading, ordering, snapshot generations/watermarks, unique append
identities, and Elle projection.

Subsequent custom checkers consume normalized evidence for atomic visibility,
LMID/effect cardinality, session visibility, convergence/oracle equality,
permissions, and schedule delivery. Each checker has a positive fixture and a
single-property mutant that it must reject. A lane with zero reads, zero
successful mutations, zero non-writing clients, or an unfired scheduled fault
is invalid rather than passing vacuously.

### Rust state-machine checker

Rust owns fast internal state-machine exploration in
`crates/sync-core/tests/differential.rs`: generated typed operations execute
against Rust and the TypeScript reference, pull patches are normalized, and
cookies/LMIDs are compared under their documented representation rules.
Property generation shrinks traces and prints an exact seed replay.

This lane should add narrow invariants close to the engine: patch application
equals a fresh store, cookie watermarks do not regress, rejected mutations do
not leak rows, and LMID never acknowledges ahead of its effect. It does not
reimplement Elle's dependency graph or the browser client's cache semantics.

## Deterministic fault model

Schedules are generated from the run seed and logical operation count. A fault
is armed before a named hook or operation index, records when it fired, and is
healed by another logical step. Sleeps may enforce an already-triggered outage
window, but a sleep is never the trigger condition.

The required matrix is:

| Boundary             | Faults                                                                                      | Required proof                                                                                              |
| -------------------- | ------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| mutation transaction | before mutation; after write before commit; after commit before response                    | rollback before commit; durable row+LMID after commit; replay remains single-application                    |
| pull transaction     | during transaction; after commit before response                                            | client claim/cookie transaction is atomic and retry is safe                                                 |
| transport            | drop push response; pause one client's pulls; disconnect mid-pull; duplicate/replay request | indeterminate outcome classification, no duplicate effect, other clients progress, resumed client converges |
| authority process    | native process kill and restart over the same SQLite file                                   | durable state and LMIDs survive; cookies remain valid or reset explicitly                                   |
| client lifecycle     | close/reopen persisted group; concurrent tabs; lost local state; stale/future cookie        | group identity and per-client LMIDs remain correct; reset is explicit                                       |
| Cloudflare           | injected error/quota, deterministic in-memory teardown, platform eviction lane              | transaction rollback and durable object reconstruction without pretending a real SIGKILL hook exists        |

Every scheduled fault has exactly one arm receipt and one fire receipt; healed
faults also have one heal receipt. Missing or duplicate receipts fail schedule
validation. Clock skew is excluded until a guarantee depends on wall clocks.
Database partitions are excluded for embedded SQLite and Durable Objects
because there is no separate database network boundary.

## Validation ladder

### Pull request gate

- history schema and pure-checker tests, including one mutant per property;
- committed consistency repros first;
- Rust state-machine differential with fixed cases and replayable seeds;
- clean small history against the reference TypeScript core and Rust local;
- tiny Elle workload plus known-valid and known-invalid self-tests;
- existing query shapes, randomized differential, permissions, storm, and
  storage-transaction lanes appropriate to the changed package.

Keep PR workloads bounded and deterministic. Fresh randomized CI seeds are
useful only when the exact seed and complete artifact upload are guaranteed.

### Nightly gate

- larger Rust case count and multiple recorded seeds;
- stock Zero, Orez local, and Rust local with the same list-append and custom
  histories;
- the complete deterministic fault matrix and reconnect/client-group corpus;
- full Elle analysis with anomaly artifacts;
- larger query generator coverage, 100-client storm, load grid, and longevity
  sample;
- replay every newly emitted failure once before reporting it.

The credentialed deployed Cloudflare lane runs on a separate schedule and is
compared with the same local contract. It must not make pull requests depend on
external credentials or a mutable deployment.

### Release review

A release candidate is acceptable only when:

1. all required checkers identify their input history and return true;
2. Elle did not return unknown and both self-tests behaved as expected;
3. every scheduled fault fired and healed as specified;
4. all histories contain non-writing and fresh-client observations;
5. convergence barriers include authority and sentinel proof;
6. failures replay from the saved artifact and permanent regressions run first;
7. target build identities and checker versions are immutable;
8. no skipped, TODO, corrupt, or vacuous cases are counted as passes;
9. the stock Zero reference passes the workload before an Orez divergence is
   attributed to Orez;
10. a reviewer inspects new anomaly witnesses, minimized traces, and the exact
    fault delivery evidence rather than relying on a green summary alone.

## Integration boundaries

`SyncTarget` remains the lifecycle and authority adapter. History recording
wraps its clients, `sql`, oracle, restart, and injectable-fetch seams without
adding checker logic to target implementations. Workloads emit canonical
events. Checkers read completed artifacts. Reporters serialize results.

The query generator keeps its existing `GenSpec` and replay path. It may adopt
the common manifest later, but consistency history v1 does not require a broad
rewrite of `sweep.ts`. The Rust property trace also remains independent. These
boundaries keep one event contract while avoiding a framework migration before
the first real faulted history exists.
