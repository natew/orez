# Engine mutation matrix — 2026-07-16

Which lanes catch which known engine bugs. Mutants live in
`harness/mutants/` (10 patches, one defect each, all compile-checked);
the runner is `harness/scripts/mutation-matrix.ts`. Every cell below is a
verified verdict: each CAUGHT was confirmed against the lane's actual failure
output, not just its exit code (see "vacuity incident" below for why).

Run provenance: engine tree = main @ 261e27d merged with
`test/wire-consistency-lanes` @ ce43931 and `test/query-differential-oracle`
@ 19d9003. Six lanes from run `run-2026-07-16-v2`; the two consistency lanes
from `run-2026-07-16-v3` after the seed fix. O1 and M4 were re-run across all
lanes after adding the engine-invariant tests in
`run-2026-07-16-engine-invariants-v3`. All lanes were green at baseline in each
cited run.

This historical full-matrix run predates the single query-pull mode. The table
retains only lanes and mutants that still exist.

Replay: `cd harness && bun scripts/mutation-matrix.ts` (clean crates/ tree
required; ~35 min).

## Matrix

Lanes: cargo = `cargo test -p sync-core` (unit + TS-oracle differentials),
smoke/state-machine/metamorphic/eviction/sweep = harness system lanes against
`rust-local`, atomic-vis / exactly-once = recorded-history consistency lanes
against `rust-local`.

Full-matrix run `2026-07-17T08-01-29-065Z`: every cell evaluated, all retained
lanes green at baseline, every retained mutant caught by at least one lane.

| mutant                           | cargo  | smoke  | state-machine | metamorphic | eviction | sweep  | atomic-vis | exactly-once | permissions |
| -------------------------------- | ------ | ------ | ------------- | ----------- | -------- | ------ | ---------- | ------------ | ----------- |
| Q1 AND branch dropped            | CAUGHT | ·      | ·             | ·           | ·        | ·      | ·          | ·            | ·           |
| Q2 orderBy inverted              | CAUGHT | ·      | ·             | ·           | ·        | ·      | ·          | ·            | ·           |
| Q3 limit off-by-one              | CAUGHT | ·      | ·             | ·           | ·        | ·      | ·          | ·            | ·           |
| Q4 related window drops last row | CAUGHT | ·      | ·             | ·           | ·        | ·      | ·          | ·            | ·           |
| M1 rows commit, LMID skipped     | CAUGHT | CAUGHT | CAUGHT        | ·           | CAUGHT   | CAUGHT | ·          | CAUGHT       | ·           |
| M2 replay double-applies         | CAUGHT | ·      | CAUGHT        | ·           | ·        | ·      | ·          | CAUGHT       | ·           |
| M3 rollback swallowed            | CAUGHT | ·      | ·             | ·           | ·        | ·      | ·          | CAUGHT       | ·           |
| M4 LMID advances, no change row  | CAUGHT | ·      | ·             | ·           | ·        | ·      | ·          | ·            | ·           |
| L1 prune without floor raise     | CAUGHT | ·      | CAUGHT        | ·           | ·        | ·      | ·          | ·            | ·           |
| O1 non-durable watermark         | CAUGHT | ·      | CAUGHT        | ·           | ·        | ·      | ·          | ·            | ·           |

A caution for future full runs: sweep lanes executed under an installed mutant
write minimized divergence fixtures into
`harness/regressions/sweep/v1/` — those are mutant-induced, tracked per mutant
in the run directory's `*-untracked` files, and must be deleted, not committed. `·` = run and not
caught; `CAUGHT` = run and caught.

## Findings, in order of importance

1. **The O1 system hole is closed (2026-07-16).** The state-machine lane now
   catches O1 through a `fullPruneRestart` step in its required prefix: it reads
   the server-confirmed watermark via a raw null-cookie pull, empties the change
   log to the head over the new `/{ns}/admin/prune-to-head` route, restarts the
   native process over the same SQLite file, and fails on a served-cookie
   regression (`served watermark regressed across full prune + restart: 20 -> 0`
   under O1, green at baseline). The step sits in both the lifecycle prefix
   (matrix `state-machine` lane, seed 7) and the nemesis prefix. See
   `docs/sync/nemesis-red-proof.md`. `cargo test -p sync-core`'s
   engine-invariant test still covers the same property in isolation.

2. **Four mutants are caught only by `cargo test -p sync-core`** (Q1–Q4).
   The cargo suite includes hand-written query tests and the deterministic
   TS-oracle differentials. It is the single load-bearing net for query shape
   correctness. Remaining system-level blind spots:
   - **Q1–Q4:** sweep at 5 rounds / seed 42 never trips on pure query-shape
     bugs; the deterministic oracle (which shrank both of its red-proof
     mutants to minimal traces) is the effective generative net.

   One former blind spot CLOSED at the system level (run `verify-d`,
   2026-07-16, after `test/coverage-lane-gaps`):
   - **M3 (swallowed rollback)** is now CAUGHT by the exactly-once lane: the
     workload issues a deterministically rejected mutation and the checker
     requires zero row effects plus the app-error LMID advance ("after
     authority does not show one application and LMID 2").

3. **The mutator core is genuinely well-covered.** M1 (rows without LMID)
   went red in six independent lanes. This failure would corrupt user data,
   and its coverage remains broad.

4. **Metamorphic catches nothing here, structurally.** It checks
   self-consistency (same query asked two equivalent ways), and a compiler
   bug applied uniformly to both sides of every pair stays self-consistent.
   It earns its keep against asymmetric bugs (it caught upstream #6121); it
   is not a query-correctness oracle and should not be read as one.

## The vacuity incident

The first v2 run reported the two consistency lanes catching every mutant
in under a second each. Every one of those catches was fake: the runner
reused one `--seed`, the lanes derive their results directory from the seed
and refuse to overwrite it, so every post-baseline invocation crashed at
startup before touching the engine. A lane that always fails is exactly as
worthless as a lane that cannot fail. The runner now derives a unique seed
per invocation, and the verdicts above were re-verified against each lane's
actual violation output.

## Keeping this honest

- A new lane earns a column by going red on at least one mutant here.
- A new engine invariant earns a mutant that violates it.
- Re-run after material engine changes; stale patches (`git apply` failure)
  are regenerated at the same site, never skipped.
