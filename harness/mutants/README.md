# Engine and host mutants

Known bugs, kept as a runnable suite. Each patch introduces exactly one
plausible defect in the Rust engine or TypeScript host. The corpus covers the
four properties in `plans/consistency-hardening-plan.md` (query correctness,
mutator commit/rollback, data loss, ordering) plus host-owned observations such
as namespace backups. `scripts/mutation-matrix.ts` applies them one at a time,
runs every compatible lane, and records which lanes catch which. The committed
matrix lives at `docs/sync/mutation-matrix.md`.

A mutant nothing catches is the product: it names a hole in the net. Do not
delete an uncaught mutant; fix the net until it goes red, then keep the
mutant here so the catch is re-provable.

`target` selects the compatible build and lane family. `expectedLanes` records which
lanes should catch each mutant. The matrix records what actually happened;
disagreement between the two is signal in both directions.

The runner requires a one-to-one inventory across `manifest.json`,
`expected.json`, and `patches/`. A missing or orphaned entry fails before any
build starts, so a stale expectation cannot silently disappear from the gate.

Patches are unified diffs against the named source file and will go stale as
the implementation moves. `git apply` failing on a patch means it needs
regenerating, not skipping: re-derive the same defect at the same site.
