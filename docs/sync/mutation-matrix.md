# Engine mutation matrix — 2026-07-16

Which lanes catch which known engine bugs. Mutants live in
`harness/mutants/` (14 patches, one defect each, all compile-checked);
the runner is `harness/scripts/mutation-matrix.ts`. Every cell below is a
verified verdict: each CAUGHT was confirmed against the lane's actual failure
output, not just its exit code (see "vacuity incident" below for why).

Run provenance: engine tree = main @ 261e27d merged with
`test/wire-consistency-lanes` @ ce43931 and `test/query-differential-oracle`
@ 19d9003. Six lanes from run `run-2026-07-16-v2`; the two consistency lanes
from `run-2026-07-16-v3` after the seed fix. O1, M4, and O2 were re-run across
all lanes after adding the engine-invariant tests in
`run-2026-07-16-engine-invariants-v3`. All lanes were green at baseline in
each cited run.

Replay: `cd harness && bun scripts/mutation-matrix.ts` (clean crates/ tree
required; ~35 min).

## Matrix

Lanes: cargo = `cargo test -p sync-core` (unit + TS-oracle differentials),
smoke/state-machine/metamorphic/eviction/sweep = harness system lanes against
`rust-local`, atomic-vis / exactly-once = recorded-history consistency lanes
against `rust-local`.

| mutant                           | cargo  | smoke  | state-machine | metamorphic | eviction | sweep  | atomic-vis | exactly-once |
| -------------------------------- | ------ | ------ | ------------- | ----------- | -------- | ------ | ---------- | ------------ |
| Q1 AND branch dropped            | CAUGHT | ·      | ·             | ·           | ·        | ·      | ·          | ·            |
| Q2 orderBy inverted              | CAUGHT | ·      | ·             | ·           | ·        | ·      | ·          | ·            |
| Q3 limit off-by-one              | CAUGHT | ·      | ·             | ·           | ·        | ·      | ·          | ·            |
| Q4 related window drops last row | CAUGHT | ·      | ·             | ·           | ·        | ·      | ·          | ·            |
| M1 rows commit, LMID skipped     | CAUGHT | CAUGHT | CAUGHT        | ·           | CAUGHT   | CAUGHT | ·          | CAUGHT       |
| M2 replay double-applies         | CAUGHT | ·      | CAUGHT        | ·           | ·        | ·      | ·          | CAUGHT       |
| M3 rollback swallowed            | CAUGHT | ·      | ·             | ·           | ·        | ·      | ·          | ·            |
| M4 LMID advances, no change row  | CAUGHT | ·      | ·             | ·           | ·        | ·      | ·          | ·            |
| L1 prune without floor raise     | CAUGHT | ·      | CAUGHT        | ·           | ·        | ·      | ·          | ·            |
| L2 snapshot omits first row      | CAUGHT | CAUGHT | ·             | ·           | CAUGHT   | CAUGHT | ·          | ·            |
| L3 diff omits first changed row  | CAUGHT | CAUGHT | ·             | ·           | CAUGHT   | CAUGHT | CAUGHT     | ·            |
| O1 non-durable watermark         | CAUGHT | ·      | ·             | ·           | ·        | ·      | ·          | ·            |
| O2 acks beyond the diff cap      | CAUGHT | ·      | ·             | ·           | ·        | ·      | ·          | ·            |
| P1 snapshot ignores visible()    | CAUGHT | ·      | ·             | ·           | ·        | ·      | ·          | ·            |

## Findings, in order of importance

1. **The O1 cargo hole is closed; system lanes still miss it.** The dedicated
   engine-invariant test advances the watermark, fully prunes the change log,
   reopens the same SQLite file, and requires the durable high-water to keep
   the cookie monotonic. O1 regresses the reopened watermark from 1 to 0, so
   `cargo test -p sync-core` now catches it. Every system lane still passes the
   mutant because none empties the log and restarts over the same store.

2. **Seven mutants are caught only by `cargo test -p sync-core`** (Q1–Q4,
   M4, O1, O2). The cargo suite includes hand-written query tests and
   the deterministic TS-oracle differentials. It is the single load-bearing
   net for query shape correctness, durable watermark retention, and
   capped-diff ordering. No system lane duplicates it. Remaining
   system-level blind spots:
   - **M4 / O2:** deterministic cargo tests now use
     `Caps { max_change_rows: 1, ... }` to cut between effects and LMIDs and to
     prove an LMID-only rejected push still advances the cookie. No system lane
     pulls with caps small enough to hit either cut path.
   - **Q1–Q4:** sweep at 5 rounds / seed 42 never trips on pure query-shape
     bugs; the deterministic oracle (which shrank both of its red-proof
     mutants to minimal traces) is the effective generative net.

   Two former blind spots CLOSED at the system level (run `verify-d`,
   2026-07-16, after `test/coverage-lane-gaps`):
   - **M3 (swallowed rollback)** is now CAUGHT by the exactly-once lane: the
     workload issues a deterministically rejected mutation and the checker
     requires zero row effects plus the app-error LMID advance ("after
     authority does not show one application and LMID 2").
   - **P1 (visibility ignored)** is now CAUGHT by `permissions.ts --target
     rust-local`: the native fixture host configures a `visible()` policy
     and the lane fails on the leaked row ("owner projects: expected
     [perm-project], got [perm-foreign, perm-project]"). The permissions
     lane is registered in the matrix runner.

3. **The mutator/loss core is genuinely well-covered.** M1 (rows without
   LMID) went red in six independent lanes; L2/L3 (dropped snapshot/diff
   rows) in five each. These are the failures that would corrupt user data,
   and the net is dense exactly there.

4. **Metamorphic catches nothing here, structurally.** It checks
   self-consistency (same query asked two equivalent ways), and a compiler
   bug applied uniformly to both sides of every pair stays self-consistent.
   It earns its keep against asymmetric bugs (it caught upstream #6121); it
   is not a query-correctness oracle and should not be read as one.

## The vacuity incident

The first v2 run reported the two consistency lanes catching all 14 mutants
in under a second each. Every one of those catches was fake: the runner
reused one `--seed`, the lanes derive their results directory from the seed
and refuse to overwrite it, so every post-baseline invocation crashed at
startup before touching the engine. A lane that always fails is exactly as
worthless as a lane that cannot fail. The runner now derives a unique seed
per invocation, and the verdicts above were re-verified against the lanes'
actual violation output (e.g. L3's atomic-visibility catch reads "atomic
group … partially visible … missing effects", a real checker verdict).

## Keeping this honest

- A new lane earns a column by going red on at least one mutant here.
- A new engine invariant earns a mutant that violates it.
- Re-run after material engine changes; stale patches (`git apply` failure)
  are regenerated at the same site, never skipped.
