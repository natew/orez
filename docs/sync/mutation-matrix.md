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
`run-2026-07-16-engine-invariants-v3`. The capped-diff lane column is from
`run-2026-07-16-capped-diff` (targeted baseline/M4/O2 runs, after
`test/coverage-capped-diff`). All lanes were green at baseline in each cited run.

Replay: `cd harness && bun scripts/mutation-matrix.ts` (clean crates/ tree
required; ~35 min).

## Matrix

Lanes: cargo = `cargo test -p sync-core` (unit + TS-oracle differentials),
smoke/state-machine/metamorphic/eviction/sweep = harness system lanes against
`rust-local`, atomic-vis / exactly-once = recorded-history consistency lanes
against `rust-local`, capped-diff = `capped-diff-lane.ts` against `rust-local`
with `maxChangeRows: 1` (the only system lane that pulls with a cap small enough
to split a mutation's row effect from its lmid ack).

Full-matrix run `2026-07-17T08-01-29-065Z` — every cell evaluated, all ten
lanes green at baseline, every mutant caught by at least one lane.

| mutant                           | cargo  | smoke  | state-machine | metamorphic | eviction | sweep  | atomic-vis | exactly-once | permissions | capped-diff |
| -------------------------------- | ------ | ------ | ------------- | ----------- | -------- | ------ | ---------- | ------------ | ----------- | ----------- |
| Q1 AND branch dropped            | CAUGHT | ·      | ·             | ·           | ·        | ·      | ·          | ·            | ·           | ·           |
| Q2 orderBy inverted              | CAUGHT | ·      | ·             | ·           | ·        | ·      | ·          | ·            | ·           | ·           |
| Q3 limit off-by-one              | CAUGHT | ·      | ·             | ·           | ·        | ·      | ·          | ·            | ·           | ·           |
| Q4 related window drops last row | CAUGHT | ·      | ·             | ·           | ·        | ·      | ·          | ·            | ·           | ·           |
| M1 rows commit, LMID skipped     | CAUGHT | CAUGHT | CAUGHT        | ·           | CAUGHT   | CAUGHT | ·          | CAUGHT       | ·           | CAUGHT      |
| M2 replay double-applies         | CAUGHT | ·      | CAUGHT        | ·           | ·        | ·      | ·          | CAUGHT       | ·           | ·           |
| M3 rollback swallowed            | CAUGHT | ·      | ·             | ·           | ·        | ·      | ·          | CAUGHT       | ·           | ·           |
| M4 LMID advances, no change row  | CAUGHT | ·      | ·             | ·           | ·        | ·      | ·          | ·            | ·           | CAUGHT      |
| L1 prune without floor raise     | CAUGHT | ·      | CAUGHT        | ·           | ·        | ·      | ·          | ·            | ·           | ·           |
| L2 snapshot omits first row      | CAUGHT | CAUGHT | ·             | ·           | CAUGHT   | CAUGHT | ·          | ·            | ·           | ·           |
| L3 diff omits first changed row  | CAUGHT | CAUGHT | ·             | ·           | CAUGHT   | CAUGHT | CAUGHT     | ·            | ·           | CAUGHT      |
| O1 non-durable watermark         | CAUGHT | ·      | CAUGHT        | ·           | ·        | ·      | ·          | ·            | ·           | ·           |
| O2 acks beyond the diff cap      | CAUGHT | ·      | ·             | ·           | ·        | ·      | ·          | ·            | ·           | CAUGHT      |
| P1 snapshot ignores visible()    | CAUGHT | ·      | ·             | ·           | ·        | ·      | ·          | ·            | CAUGHT      | ·           |

The full run also confirmed the capped-diff lane catches M1 and L3 beyond its
designed M4/O2 targets (a skipped finalize and a dropped first diff entry both
break the effect-then-ack cut). A caution for future full runs: sweep lanes
executed under an installed mutant write minimized divergence fixtures into
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

   One former blind spot CLOSED at the system level (run
   `run-2026-07-16-capped-diff`, after `test/coverage-capped-diff`):
   - **M4 / O2** are now CAUGHT by the capped-diff lane. It runs a native host
     with `--max-change-rows 1`, then, as a non-writing observer in the writer's
     client group, raw-pulls twice off the pull dialect. The one-row cap admits
     the probe's row effect on the first diff and holds its lmid ack for the
     second. M4 (no lmid change row) goes red because the ack ships on no pull
     ("the probe's lmid ack never shipped … the effect committed but its ack is
     unreachable"). O2 (acks beyond the cut) goes red because the first diff
     carries the ack together with the effect ("first capped diff delivered the
     probe effect AND lmid ack 2 (baseline ack 1); an ack led its effect under a
     one-row cap"). All observations come from server-confirmed pull responses,
     never the writer's optimistic cache.

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
