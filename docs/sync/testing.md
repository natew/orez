# Testing

This is an honest assessment of what is tested, how, and what is not. The short
version: the baseline pull and push engine is genuinely differential-tested
against the TypeScript reference and randomized-model-verified; the query-aware
and permission layer is heavily unit-tested in Rust and cross-checked live
against stock zero-cache but has no deterministic oracle; the Cloudflare host has
a real workerd fault, soak, and qualification matrix; and the ambitious
jepsen-style harness that was scoped early was deliberately not built.

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
only, so it never ships. There are roughly 115 test functions across the suite;
two are `#[ignore]` measurements. Run them with `cargo test --workspace`
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
- **`upstream.rs`** covers the ingest apply path: ordered pages,
  watermark-idempotency, full-image updates and deletes, out-of-order rejection,
  schema-drift classification, legacy meta migration, and atomic retention-gap
  snapshot replacement.
- **The query suites** (`query_ast.rs`, `query_chat_shapes.rs`,
  `query_chat_permissions.rs`, `query_membership.rs`, `query_related.rs`,
  `query_pull.rs`, `query_narrowing.rs`, `query_windowed_corpus.rs`,
  `soot_composition.rs`) are about 62 tests covering AST validation and SQL
  compilation, Chat's real query shapes, Chat permission transforms with
  explicit deny cases, per-group membership refcounts, related and windowed
  subqueries, and the wire-level query-aware pull path.
- **`schema_hardening.rs`** is a security regression suite for DDL and trigger
  injection, proving hostile table and column names cannot inject.

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
- **`ingest-test.mjs`** covers upstream ingest end to end: retention-gap
  snapshot recovery, the billable-versus-logical write budget, delegated push
  with injected transient failures and a bounded retry cap, and the runaway lane
  where a feed whose cursor stalls trips the breaker to a 429
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

`harness/src/soot-deployed-conformance.ts` is a faithful port of soot's own
integration test, run against the deployed worker in soot's dialect. It exists
because the generic fixture lanes cannot target soot's exact composition.

## What is not covered

State this plainly rather than implying blanket coverage.

1. **The differential oracle covers only the baseline engine.** The
   `differential.rs` oracle exercises put, del, reject, upstream, pull, and
   invalidate on one fixture table. The query-aware and permission compiler has
   no TypeScript-oracle differential. Its correctness rests on Rust-internal
   assertions plus the live `query-diff.ts` comparison against stock zero-cache,
   which is a live-server comparison, not a deterministic oracle.
2. **`sync-wasm` has no tests of its own.** There are no `wasm_bindgen_test`
   cases and no `crates/sync-wasm/tests/`. The WASM engine is exercised only
   indirectly, through the workerd lanes and the `rust-cf` harness target.
3. **`sync-native` has no cargo integration tests.** It has one inline
   golden-seed test. The binary's behavior is covered only by the `rust-local`
   harness lanes.
4. **There is no jepsen or elle-level checker.** The linearizability and
   transactional-anomaly harness sketched in
   `plans/zero-conformance-harness.md` was deliberately dropped. There is no
   history export, no anomaly checker, and no network fault injection (no
   partition, latency, or packet-drop lanes). Faults in the real harness are
   process-kill and injected DO storage faults only (`eviction.ts`,
   `storage-faults.ts`).
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
