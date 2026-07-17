# Testing

This is an honest assessment of what is tested, how, and what is not. The short
version: the baseline pull and push engine is genuinely differential-tested
against the TypeScript reference and randomized-model-verified; the query-aware
layer has a deterministic seeded oracle (named already-transformed ZQL ASTs,
membership, multi-table writes, in-place permission-transform replacement) plus
the live cross-check against stock zero-cache; the Cloudflare host has a real
workerd fault, soak, and qualification matrix; and the jepsen-style ambitions
were scoped down to what this system honestly needs — a composed fault nemesis
with arm/fire/heal receipts, an engine mutation matrix that proves each lane can
go red, and the pinned Elle checker running on a real recorded workload history
— rather than a full Jepsen rig.

## The three test tiers

Testing is layered. Each tier exercises a different boundary.

1. **Rust cargo suites** (`crates/sync-core/tests/`) test the engine as pure
   logic against an in-memory rusqlite host.
2. **sync-cf-host workerd lanes** (`packages/sync-cf-host/`) test the compiled
   WASM engine inside a real local Durable Object.
3. **The harness** (`harness/`) runs full end-to-end conformance and
   qualification against multiple sync targets, including a differential against
   real zero-cache.

## Cargo suites

Every cargo suite drives the engine through a synchronous in-memory rusqlite host
defined in `crates/sync-core/tests/common/mod.rs`. rusqlite is a dev-dependency
only, so it never ships. There are over 120 test functions across the suite;
three are `#[ignore]` measurements. Run them with `cargo test --workspace`
(requires `bun` on PATH, because the differential lane shells out to the
TypeScript reference).

The suites that matter most:

- **`differential.rs`** is the one true cross-implementation lane. A seeded
  PRNG generates an operation trace (put, del, reject, upstream SQL, pull,
  invalidate), and both the Rust engine and the TypeScript reference core
  (`src/sync-server/sync-server.ts`, via `crates/sync-core/ts-oracle/run-oracle.ts`)
  execute the same trace. Pull responses are compared semantically: cookie
  exact, `unchanged` exact, `rowsPatch` order-independent. It runs 8 seeds of
  200 steps each.
- **`reference_delta.rs`** ports the reference core's own 28 delta tests
  verbatim: cookie validation, push validation, snapshot and unchanged and 409,
  cursor diffs including float fidelity and primary-key changes, app-error
  last-mutation-id semantics, replay idempotency, retention floor, epoch
  invalidation, and per-user visibility.
- **`model.rs`** and **`query_model.rs`** are randomized convergence models (24
  seeds each). They interleave pushes, upstream writes, invalidations, and
  cap changes across several clients, continuously assert invariants (the cookie
  never exceeds the watermark, never regresses, acks never lead their row
  effects), and check that every client converges to a fresh-client oracle
  snapshot. Their oracle is internal (a fresh snapshot), so these are property
  tests, not a second implementation.
- **`upstream.rs`** and **`paged_snapshot.rs`** cover the ingest apply path:
  ordered changes, watermark idempotency, full-image updates and deletes,
  out-of-order rejection, schema drift, legacy metadata migration, staged
  snapshot generations, durable resume cursors, catch-up overlap, atomic
  cutover, and epoch invalidation of pre-cutover clients.
- **`upstream_corpus.rs`** is 7 behavioral CDC contracts adapted from Turso's
  pinned CDC integration suite and Electric's transaction-fragmentation suite.
  Because Orez captures changes with database-scoped triggers rather than a
  connection-scoped CDC pragma, they assert the equivalent durable effects: a
  failed upstream transaction rolls back rows, change log, and cursor together; a
  successful batch commits them together; a no-op commit and a rolled-back write
  emit no CDC and leave the connection usable; trigger CDC never captures its own
  metadata writes and is visible across independent connections; schema drift
  rolls back with an idempotent legacy-metadata upgrade; and a transaction larger
  than the pull cap converges without loss or duplication.
- **The query suites** (`query_ast.rs`, `query_chat_shapes.rs`,
  `query_chat_permissions.rs`, `query_membership.rs`, `query_related.rs`,
  `query_pull.rs`, `query_narrowing.rs`, `query_windowed_corpus.rs`,
  `soot_composition.rs`) are about 62 tests covering AST validation and SQL
  compilation, Chat's real query shapes, Chat permission transforms with
  explicit deny cases, per-group membership refcounts, related and windowed
  subqueries, and the wire-level query-aware pull path.
- **`schema_hardening.rs`** is a security regression suite for DDL and trigger
  injection, proving hostile table and column names cannot inject.

## sync-native cargo tests

The native host has its own cargo coverage, separate from the sync-core engine
suites. `crates/sync-native/tests/library_api.rs` drives the real axum router
through `tower`'s `oneshot`: health, a fresh snapshot pull, push-then-pull,
app-error last-mutation-id semantics, per-namespace isolation, and the
server-owned admin-transaction protocol on `/admin/sql` end to end. That protocol
is worker-owned: the namespace thread runs the actual `BEGIN`/`COMMIT`/`ROLLBACK`
and flips its scheduler state only after that SQL succeeds, so the integration
tests assert commit and rollback both excluding concurrent pull/push, wrong-id
and duplicate-begin conflicts, malformed begin/end and transaction-control-in-a-
query rejection (including comment-hidden control SQL), and a lost admin client
reclaimed by the transaction lease. The router tests also prove that default
hosts mint distinct 256-bit admin
tokens, missing or incorrect credentials cannot execute admin SQL, every
browser-origin admin request is denied, unknown browser origins cannot sync,
and the exact allowed origin completes both preflight and pull with restricted
CORS headers. The same router suite covers application-owned push settlement:
exact acknowledgement validation, admin gating, cross-client diff and snapshot
visibility, effects-before-LMID watermark order, and idempotent
`alreadyProcessed` recovery without a duplicate LMID row.
`crates/sync-native/src/namespace.rs` adds
worker-level unit tests for the same scheduler where the lease and connection are
directly controllable: a failed begin or failed commit that recovers instead of
wedging, lease reclaim of a lost client, disconnect mid-step, and a plain-job
flood that cannot starve the lease. `crates/sync-native/src/seed.rs` keeps its
inline golden-seed test. The running binary's behavior over a real socket
(graceful shutdown, the wake WebSocket under load) is still exercised only by the
`rust-local` harness lanes.

## sync-cf-host workerd lanes

These run the compiled WASM engine inside a local Durable Object via
`wrangler dev --local`. None of them need real Cloudflare. `bun run test` runs
the whole sequence; the lanes:

- **`config.test.mjs`** and **`write-safeguards.test.mjs`** are pure unit tests
  (no workerd). They cover config validation (the mutators-versus-mutateUrl
  invariants, upstream and retry bounds) and the circuit-breaker math (rolling
  trip, exponential backoff, recovery, persisted cooldown, billable-cursor
  accounting).
- **`platform-test.mjs`** is the M0 platform contract: async transactions across
  `await`, application errors and JS exceptions and Rust panics each rolling back
  every effect and last-mutation-id, full value round-trip including large exact
  decimals and float fidelity, and idle-teardown eviction.
- **`integration-test.mjs`** is the production integration suite (50-plus
  assertions): snapshot pull, query-aware named-query resolution and member
  filtering, client-authored raw AST rejected, writer enable and disable, fault
  injection at five boundary points, deferred effects running only after commit,
  and wake WebSocket delivery.
- **`ingest-test.mjs`** covers upstream ingest end to end: keyset-paged
  retention-gap recovery, staged generation resume, adaptive page limits, the
  billable-versus-logical write budget, delegated push with injected transient
  failures and a bounded retry cap, and the runaway lane where a feed whose
  cursor stalls trips the breaker to a 429
  `ingestCursorStalled` and then recovers through the admin route.
- **`restart-test.mjs`** is a regression for a real bug: admin-set namespace
  knobs must survive a workerd restart, so a query-aware namespace does not
  silently fall back to baseline after eviction.

## The harness

`harness/` runs the same test scripts against several `SyncTarget`s: real
zero-cache with embedded Postgres (`stock-zero`), the TypeScript core
(`orez-local`, `orez-cf`), and the Rust engine as native binary (`rust-local`)
and WASM DO (`rust-cf`). Every lane writes through sync and to the upstream
store, requires convergence, and compares against a fresh oracle read.

**Conformance lanes** are the CI acceptance gate: `smoke.ts`,
`multi-project-mount.ts`, `shapes.ts` (a 22-query cross-implementation
differential), `sweep.ts` (seeded randomized differential), `propagation.ts`
(wake latency), `queries.ts`, `query-diff.ts` (query membership differential
against stock zero-cache), `permissions.ts`, and `storm.ts` (100 clients).

**Qualification lanes** are the M6 matrix, dispatched by
`harness/src/m6-runner.ts` as a native suite and a CF suite: protocol fuzz
(10,000 seeded malformed cases, every one must return 4xx), eviction, retention
reconnect, query tab churn, clock skew (plus or minus 24 hours), storage faults,
backup and restore, and for the CF suite also WASM memory soak, push memory
soak, and a rollback drill. The 2026-07-10 re-qualification recorded native 7 of
7 and CF 9 of 9.

**The portable upstream corpus** (`harness/src/upstream-corpus.ts`) replays the
same behavioral scenarios across four hosts (the TypeScript oracle, stock
zero-cache, native Rust `sync-native`, and the Rust CF Durable Object), comparing
public observations (query results and durable rows) rather than any
implementation's private CVR/CDC representation. **The generated state machine**
(`harness/src/state-machine.ts`) is an Electric-style lifecycle model for the
Rust hosts: a deterministic seeded trace mixes writes, desired-query changes,
retention pruning, lost responses, and server and client restarts, and every
operation compares live client views to an authoritative SQL oracle; a failure
emits the seed, the full trace, and a delta-debugged (shrunk) reproducer under
`harness/regressions/`. Its `--nemesis` mode composes a second fault axis on
top: one-shot engine faults armed at named push/pull boundaries, a client
transport pause held open across an engine-fault arm and a server restart (two
fault classes active at once), and a full-prune-plus-restart step that proves
the durable watermark keeps the served cookie monotonic over the same SQLite
file. Every scheduled fault carries receipts — arm, then fired or a documented
cancellation; pauses also record heal — and a generated schedule whose faults
never fire, or whose pause never fires or heals, fails as invalid instead of
passing vacuously (replays and shrink candidates are judged on execution
alone). See `docs/sync/nemesis-red-proof.md` for the red proofs.

**The engine mutation matrix** (`harness/mutants/`, runner
`harness/scripts/mutation-matrix.ts`, results `docs/sync/mutation-matrix.md`)
keeps 14 known engine bugs as compile-checked patches and records which lane
catches which, each verdict verified against the lane's actual failure output.
It is the proof that the net can catch bugs at all: every mutant is caught by
at least one suite, the former system-level blind spots (swallowed rollbacks,
ignored visibility, capped-diff cut ordering, non-durable watermark) are now
each caught by a dedicated system lane, and a new lane earns its place by
going red on at least one mutant.

CI runs these as release-blocking jobs. The `rust-local-faults` job runs the
pinned corpus ledger, the portable corpus across the TypeScript oracle, stock
zero-cache, and native Rust, rust-local transaction and storage faults, and the
state machine against `rust-local`. The `sync-cf-host` job's `rust-cf continuous
fault and lifecycle gates` step runs the portable corpus against `rust-cf` plus
protocol fuzz, reconnect, eviction, storage faults, backup/restore, and the state
machine against `rust-cf`; both jobs upload their seeds and minimized traces as
artifacts. The heavier `harness/scripts/nightly.sh` cron lane widens the grid:
the full four-host corpus, the M6 qualification matrix, and the state machine at
80 steps across several seeds against both `rust-local` and `rust-cf`.

`harness/src/soot-deployed-conformance.ts` is a faithful port of soot's own
integration test, run against the deployed worker in soot's dialect. It exists
because the generic fixture lanes cannot target soot's exact composition.

## Data-worker CDC and Chat compatibility

The root TypeScript suite covers the Postgres-to-DO-SQLite path separately from
the Rust engine. Run it from the repository root with `bun run test`. The most
relevant files are:

- `src/cf-do/cdc.test.ts`: real-SQLite logical CDC, including full row images,
  failed multi-row statements, SQLite transaction rollback, primary-key
  changes, schema changes, and indirect writes from business triggers. The
  failed-statement, rollback, and primary-key cases are adapted from Turso's CDC
  integration suite.
- `src/cf-do/worker-cdc.test.ts`: staging, transaction grouping, publish versus
  rollback-only capture, and DDL integration at the `ZeroDO` boundary.
- `src/cf-do/tx-journal.test.ts`: commit, rollback, abandoned-owner recovery,
  row-image undo, and full-table fallback for unclassified writes.
- `src/pg-proxy-do-backend.test.ts` and
  `src/pg-sqlite-compiler/integration.test.ts`: protocol transaction behavior
  and the final Postgres-to-SQLite compilation boundary.

`scripts/test-chat-e2e.ts` is the compatibility harness for Chat against the
local orez crates. It snapshots the Chat tree with `git archive` into a
disposable workspace, installs the local orez and bedrock-sqlite dist, and runs
Chat's full Playwright integration suite. The 2026-07-16 run was fully green:
78 passed, 1 skipped, zero failures or flakes, after fixing an e2e-only auth
rate-limit collision in Chat (all local Playwright workers shared one anonymous
localhost rate-limit key; production limits unchanged). Earlier
point-in-time results (the 2026-07-13 setup-only qualification at 125,402
billable rows) are superseded by that green full run.

## What is not covered

State this plainly rather than implying blanket coverage.

1. **The permission-transform computation has no oracle differential.** The
   `differential.rs` oracle now generates a query axis — named
   already-transformed ZQL ASTs, query registration/removal, membership
   changes, and multi-table writes, including replacing a query's permission
   transform in place — alongside the baseline put/del/reject/upstream/pull/
   invalidate ops, and shrinks failures to minimal traces. What it feeds the
   engine are *already-transformed* ASTs: the computation of a transform from a
   policy is proven by the permissions lane (`permissions.ts --target
   rust-local`, with a `visible()` policy and a red-proof against mutant P1)
   and the live `query-diff.ts` comparison, not by the deterministic oracle.
2. **`sync-wasm` is covered at its boundary, not exhaustively.** Three Node
   wasm tests drive a real SQLite adapter through the exported push, pull,
   error, preflight, and finalize boundary, and a native race drives 256 engine
   writes from eight OS threads through one namespace to prove the
   single-writer invariant (gap-free LMIDs, effect-before-ack, one executing
   worker). Both have recorded red mutations
   (`docs/sync/sync-wasm-red-proof.md`). Everything else in the WASM path is
   still exercised indirectly through the workerd lanes and `rust-cf`.
3. **`sync-native`'s real-socket behavior is harness-only.** The crate now has
   cargo coverage (see "sync-native cargo tests"): a library-API integration
   suite over the axum router and worker-level unit tests for the
   admin-transaction scheduler. What those do not cover is the running binary
   over a real TCP socket, graceful shutdown, and the wake WebSocket under load;
   that behavior is exercised only by the `rust-local` harness lanes.
4. **Elle now runs the pinned checker on a real recorded workload.** The
   `rust-local` CI job records the atomic-visibility lane's history, then
   `scripts/elle/check-history.sh` projects it and runs the pinned elle-cli
   0.1.9 standalone jar (verified by SHA-256, Java 21) with `--model
   list-append --consistency-models serializable`, failing the job on `false`,
   `unknown`, or malformed output. `projectElleListAppend`
   (`src/consistency/history.ts`) projects every event carrying list-append
   micro-ops (the atomic-visibility lane's `read` and `mutation` kinds, not just
   the dedicated `transaction` kind), and `src/consistency/elle-project.ts`
   restricts each observed list to the values appended within the history so the
   tracked list-append sub-history embedded in a seeded store is what elle
   analyzes. `serializable` is the honest model here: the workload's none-or-all
   visibility means an empty read orders before the append and a full read after
   it, so a serial order exists; a torn read surfaces as a G-single anomaly (red
   proof in `docs/sync/lane-red-proof.md`). Realtime/strict variants are not
   claimed for asynchronous cache reads. Bounds worth stating: elle checks only
   the atomic-visibility workload (one authoritative multi-key append plus
   non-writing complete-list reads), and because it drops values outside the
   append universe it does not detect a read of a value no transaction appended.
   `scripts/elle/self-test.sh` remains a separate checker-boundary self-test
   over toy fixtures. Still missing: there is no network fault injection (no
   partition, latency, or packet-drop lanes). Faults in the real harness are
   process-kill, injected DO storage faults (`storage-faults.ts`), eviction
   (`eviction.ts`), and the admin-transaction lease and rollback paths only.
5. **The upstream IVM fuzzer was not ported.** `protocol-fuzz.ts` fuzzes
   malformed input structurally (every case must 4xx); it does not fuzz
   semantically valid query shapes for correctness. The real IVM fuzzer still
   lives in Rocicorp's `mono` repo and is run out of band.
6. **`soot-deployed-conformance.ts` and `query-security.ts` are manual lanes.**
   They are not in CI, `m6-runner`, or the nightly script. Their green results
   are recorded point-in-time results, not continuously enforced gates.
7. **One lane is known-fragile.** `plans/rust-sync-m6-qualification.md` notes
   the `rust-cf` query-diff differential was held red for an intermittent
   `allProjects` completion stall, which is in tension with the same document's
   green summary table. Treat the CF query-diff lane as not fully settled.
8. **Scale and longevity numbers are historical single runs**, not gated
   assertions. Hours-long longevity and larger load grids are listed as
   remaining work.
