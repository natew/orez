# Engine mutants

Known bugs, kept as a runnable suite. Each patch introduces exactly one
plausible engine defect targeting one of the four properties in
`plans/consistency-hardening-plan.md` (query correctness, mutator
commit/rollback, data loss, ordering). `scripts/mutation-matrix.ts` applies
them one at a time, runs every rust-capable lane, and records which lanes
catch which — the committed matrix lives at `docs/sync/mutation-matrix.md`.

A mutant nothing catches is the product: it names a hole in the net. Do not
delete an uncaught mutant; fix the net until it goes red, then keep the
mutant here so the catch is re-provable.

`expectedLanes` in `manifest.json` records the pre-run hypothesis of which
lanes should catch each mutant. The matrix records what actually happened;
disagreement between the two is signal in both directions.

Patches are unified diffs against the engine source and will go stale as the
engine moves. `git apply` failing on a patch means it needs regenerating, not
skipping: re-derive the same defect at the same site.
