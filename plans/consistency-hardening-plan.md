# Consistency hardening plan

**Status: VALIDATED and in execution (2026-07-16).** Every falsifiable claim
below was re-derived by direct inspection on 2026-07-16 (owner: this plan's
executing session). Verification notes are inline. Execution assignments are
at the bottom.

## What we actually need to prove

Orez is not a distributed database and this plan does not chase a Jepsen
linearizability result. Zero already documents its own consistency model; orez
is an asynchronous client cache over a transactional authority, and
`plans/consistency-validation-architecture.md` already refuses to claim global
linearizability. That framing stands.

What orez _does_ own is a database's core job: it decides what a query returns
and whether a mutation commits. If those are wrong, they are wrong. So the bar
is these four properties, in priority order:

1. **Named queries with ZQL return the right data and the right shape.**
2. **Custom mutators commit or roll back correctly**, exactly once, with the
   LMID and the rows moving together.
3. **No data loss** across restart, eviction, reconnect, retention, and
   storage faults.
4. **Ordering holds** given the above: cookies never regress, an ack never
   leads its effect, patches never tear.

Explicitly deprioritized: strict serializability/CAP claims, and the
Postgres-to-SQLite compiler (we are moving off it, so its 910-fixture
"doesn't throw" ratchet is no longer worth hardening).

## The optimistic-store trap (test-design rule)

Zero clients keep an in-browser db that applies mutations instantly and
locally, before the server confirms anything. A read issued on the mutating
client right after a mutation observes that optimistic overlay, not the
engine. **Any check of properties 1, 2, or 4 must observe server-confirmed
state**: a read after the server ack for that mutation, a non-writing client's
view, or the authority db directly. A lane whose only observations come from
the mutating client's own pre-ack view is vacuous by construction. This is the
concrete reason behind the architecture doc's rule that a run with zero
non-writing clients is invalid rather than passing.

## Current state (verified 2026-07-16)

The design work is excellent and the execution stops one step short. Three
gaps, each re-verified by direct inspection:

1. **The query-aware layer has no deterministic oracle.** VERIFIED: the
   `differential.rs` `Op` enum is exactly Put/Del/Reject/Upstream/Pull/
   Invalidate on one fixture table — no query axis, no ZQL. The query layer
   (`crates/sync-core/src/query/`: compile 690 + membership 992 + transaction
   998 lines) is covered by hand-written cargo tests (`query_pull.rs`,
   `query_related.rs`, `query_windowed_corpus.rs`, …) plus the _live_
   comparison against stock zero-cache (`harness/src/query-diff.ts`,
   `shapes.ts`), which needs booted Postgres + zero-cache and cannot shrink or
   run per-PR at volume. This is property #1 with the weakest coverage.

2. **The mutator-correctness checkers never run against the engine.**
   VERIFIED: `grep -rn "atomic-visibility-lane\|exactly-once-lmid-lane"
.github harness/scripts harness/package.json` returns nothing. CI runs only
   the checker _self-tests_ (`ci.yml` "consistency checker self-tests" step).
   The lane drivers `harness/src/atomic-visibility-lane.ts` and
   `harness/src/exactly-once-lmid-lane.ts` are built, self-tested, and
   orphaned. Template for wiring: `permission-transition-lane.ts` IS wired
   (`ci.yml:136`).

3. **Nothing has ever proven the harness can fail.** VERIFIED: both
   `harness/regressions/sweep/v1/` and `crates/sync-core/proptest-regressions/`
   contain only READMEs. The one committed regression is an _upstream_
   zero-cache bug (#6121) the sweep structurally could not have generated
   (`upstream-parity/README.md`). We cannot distinguish "no bugs" from "holes
   in the net."

Also verified: `docs/sync/testing.md` §4 claims "no operation-history export
for an external consistency checker" while `harness/src/consistency/history.ts:683`
exports `projectElleListAppend` (called only by its own test) — reconcile
before publishing docs. `scripts/elle/self-test.sh:4` says outright it does
not check an Orez workload.

Lower priority but real: `crates/sync-wasm` has zero tests of its own (the
entire Cloudflare production path); the single-writer invariant in
`crates/sync-native/src/namespace.rs` is asserted by comment and never raced;
the heavy lanes run via cron on a personal Mac writing untracked logs.

## Work, in order

Items 1–3 run in parallel (see assignments). The draft's argument that bug
injection must strictly precede the rest dissolves once there are three
executors: the matrix calibrates the other two lanes' work and re-runs cheaply
as they land.

### 1. Prove the net catches bugs (mutation testing the system)

Introduce known bugs into the Rust engine one at a time, run the full suite
against each, and record which lane catches which and which nothing catches.
Target the four properties:

- query: drop a row from a `related` result; off-by-one a `limit`; ignore one
  `AND` branch of a filter; return the right rows in the wrong `orderBy`;
  leak a row past a permission filter; return the wrong shape for `one()`.
- mutator: skip the LMID write; write the LMID without the rows; commit rows
  without the LMID; double-apply a replayed mutation; swallow a rollback.
- loss/order: regress a cookie; ack before the effect lands; drop one patch
  entry; lose a row on retention prune.

Deliverable: a committed matrix of mutant → lanes that caught it, with the
uncaught ones named as work. A mutant nothing catches is the highest-value
signal in this entire plan. Keep the mutants as a runnable suite, not a
one-off. Do not skip mutants that seem obviously covered; those are exactly
where the surprises live. Mutants that only corrupt the _optimistic_ client
path are out of scope; the matrix targets the engine's authoritative results.

### 2. Give the query-aware layer a deterministic oracle

Property #1, biggest surface, weakest coverage. Extend the
`differential.rs` TypeScript-oracle lane past the baseline engine to the
query-aware pull path: named queries, ZQL ASTs, membership, permission
transforms, related/windowed subqueries. The generator needs a query axis and
a multi-table fixture. The live `query-diff.ts` comparison stays valuable and
is not a substitute.

While here, add the nullable-column start-cursor axis the sweep generator is
missing (documented in `upstream-parity/README.md`; it is why #6121 had to be
caught by the metamorphic guard instead).

### 3. Wire the orphan consistency lanes into CI

Run `atomic-visibility-lane.ts` and `exactly-once-lmid-lane.ts` against
`rust-local` per-PR and `rust-cf` nightly, following the wired
`permission-transition-lane.ts` as the template. Properties #2 and #4; the
hard part is already written.

Then confirm they are non-vacuous: a lane with zero reads, zero successful
mutations, zero non-writing clients, or an unfired scheduled fault is invalid
rather than passing. Check the fault receipts, do not trust the exit code.
Per the optimistic-store rule above, confirm the checkers' observations come
from server-confirmed views, not the mutating client's local overlay.

### 4. Compose the nemesis

Faults today are one-shot at named boundaries. Real yield comes from
overlapping, randomized, healed faults: kill during a storage fault during a
reconnect, with a client offline past retention. The fault points in
`crates/sync-native/src/fault.rs` are well placed already; the schedule is
what needs to compose. This is property #3.

### 5. Elle on a real workload

Lower priority than it looks, because strict serializability is not the bar.
Either point `projectElleListAppend` at a real list-append workload for
mutator dependency safety, or drop the pretense and delete the CI job. A
green Elle job that checks nothing is worse than no Elle job.

### 6. Test sync-wasm; race the single-writer invariant

`crates/sync-wasm` is the CF production path with no tests of its own.
`namespace.rs` claims exactly one thread touches a namespace's db; spawn
concurrent threads and prove it rather than asserting it in a comment.

### 7. Move the heavy lanes into auditable CI

`m6-runner --suite all`, the rust-cf sweep, and the state machine at 80 steps
run on a personal Mac via `harness/scripts/nightly.sh` and write untracked
logs. Evidence nobody can audit is not evidence.

## Docs site ownership

The docs site should own the consistency story publicly.

- `plans/consistency-validation-architecture.md` stays the private working
  document; a cleaned-up public version ships under `site/data/docs/`
  (alongside the existing `testing.mdx`), deployed by the existing
  `orez-lite-evidence` job's `wrangler deploy --name orez-docs` step.
- The public version states the contract (asynchronous cache over a
  transactional authority, the four properties above, what is deliberately not
  claimed) and drops internal scheduling, retracted-run history, and
  lane-by-lane budgets.
- Keep the honesty. A public page that claims more than the repo proves is a
  regression, not a launch.
- Reconcile `docs/sync/testing.md` §4 against the landed `history.ts` export
  before publishing.

## Execution status (2026-07-16, end of day)

Items 1–3 and the docs work are DONE and merged to local main (commit
`65f7e91`, not yet pushed):

- **Item 3** — `test/wire-consistency-lanes` @ ce43931: both lanes run
  against rust-local in the PR CI job with uploaded evidence; red-proofed in
  `docs/sync/lane-red-proof.md`. Rust-cf nightly coverage landed in
  `.github/workflows/nightly.yml` @ 8afc103.
- **Item 2** — `test/query-differential-oracle` @ 19d9003: deterministic
  ZQL oracle in `crates/sync-core/tests/differential.rs` +
  `ts-oracle/run-oracle.ts`; red-proofed with shrinking in
  `docs/sync/query-oracle-red-proof.md`; nullable start-cursor axis added to
  the sweep generator.
- **Item 1** — 14-mutant matrix committed: `harness/mutants/`,
  `harness/scripts/mutation-matrix.ts`, results and analysis in
  `docs/sync/mutation-matrix.md`.
- Docs: `docs/sync/testing.md` §4 reconciled; public page
  `site/data/docs/consistency.mdx` render-verified.

### Follow-ups the matrix produced (next work, in order)

1. **O1 cargo hole closed on `test/coverage-engine-invariants`.** A dedicated
   invariant test now covers watermark monotonicity across a full prune and
   restart over the same SQLite file. A system state-machine step remains open.
2. **Exactly-once workload never issues an app-error mutation**, so
   swallowed rollbacks (M3) are invisible at system level.
3. **No rust-local lane runs a visibility policy** (P1 invisible at system
   level); port a permissions workload to rust-local.
4. **Capped diff cargo coverage closed on `test/coverage-engine-invariants`.**
   Dedicated tests cut between row effects and LMIDs with a one-row cap. A
   capped system lane remains open.
5. **Rust-cf nightly coverage closed on main @ 8afc103.**
6. Items 4–6 of this plan (nemesis composition, Elle-on-real-workload
   decision, and sync-wasm tests) remain unstarted. Item 7's auditable heavy
   lanes landed on main @ 8afc103.

## Rules

- A lane that cannot fail is not a lane. Prove each new check goes red on a
  real mutant before calling it done.
- No vacuous passes, no skipped cases counted as green, no point-in-time
  result described as a gate.
- Observations for correctness checks come from server-confirmed state, never
  the mutating client's optimistic in-browser db (see the trap above).
- Conventional commits. No publishing. No pushes to main without permission.
