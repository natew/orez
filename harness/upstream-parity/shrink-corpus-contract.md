# Contract: shrink-to-repro + replay-first query differential corpus

Ranked gap #2 from `ledger.json`. Task `t-mrgtvjd6-q750` (child of
`t-mrgqyuci-23qp0`). Ports the useful subset of upstream `chinook/fuzz/shrink.ts`
+ `regressions.ts` into the orez harness as a black-box query differential
corpus. **This document is the contract for high review — no sweep integration
lands until approved.**

## 1. Current contract (read-only audit, unchanged at HEAD)

**Divergence detection** (`sweep.ts` `compareAll`, line 568): for each live
spec, `canonical(stockViews[i].rows()) !== canonical(otherViews[i].rows())`.
`canonical()` sorts object keys; equality is the conformance property.

**Failure artifact** (`sweep.ts` `recordDivergence`, line 481): writes
`sweep-seed<seed>-r<round>-q<specIndex>.json` =
`{ seed, rounds, queriesPerRound, against, replay, phase, round, specIndex, spec, left, right }`.
`spec` is the full random `GenSpec`; `left`/`right` are the two canonical result
strings truncated to 2000 chars. Seed-keyed (no timestamp), so re-running the
same seed overwrites.

**Generator**: `genSpec()` → `GenSpec` (from `fixture.ts`), accumulated in
`allSpecs`. Grammar: where (cmp / and / or trees), exists, orderBy (always
id-tiebroken), limit, start cursor, related (depth ≤ 2), one().

**Gaps confirmed**: (a) the artifact carries the FULL random spec — no
minimization; (b) sweep never loads/replays a committed corpus, so a past
divergence is not a permanent guard.

## 2. Minimal slice (two pure modules + one small integration)

### 2a. `harness/src/spec-shrink.ts` — PURE, no target

- `constructCount(spec: GenSpec): number` — complexity measure: 1 per where
  condition node (recursively for and/or children), per orderBy term, per
  `limit`/`start`/`one` flag, per `related` child (recursively into its sub).
  Mirrors upstream `shrink.ts:constructCount`. Used to (a) report repro size and
  (b) assert the shrinker strictly reduced.
- `oneStepShrinks(spec: GenSpec): GenSpec[]` — every structurally-smaller spec by
  ONE simplification, each strictly reducing `constructCount`:
  drop `limit`; drop `start`; drop `one`; drop the whole `orderBy` (and drop its
  last term while length > 1, keeping the id tiebreak invariant); drop each
  `exists` entry; drop each `related` child and recurse into each child's sub;
  drop the whole `where`, or simplify it (drop an and/or child keeping ≥1; a lone
  child unwraps; recurse). Mirrors upstream `shrink.ts:oneStepShrinks`, adapted
  from AST to `GenSpec`. PURE — only removes/simplifies, never rewrites a
  correlation, so every candidate is still a buildable spec.
- `shrinkSpec(spec, stillDiverges: (s: GenSpec) => Promise<boolean>): Promise<GenSpec>`
  — greedy delta-debug to a fixpoint: apply the first one-step shrink that still
  reproduces (`stillDiverges` true), repeat until none does. Parameterized by the
  predicate, so the module has ZERO target/@rocicorp/zero dependency.

### 2b. `harness/src/spec-corpus.ts` — PURE parser + fail-loud loader

- `CorpusEntry` schema (one committed regression):
  `{ kind: 'sweep-divergence', id, note, spec: GenSpec, against?: string,
     seed: number, sourceFingerprint: string, minimizedConstructCount: number,
     expectConverge: true, replay: string }`.
  Sources are NOT stored (regenerated from the deterministic SEED); the entry is
  just the minimized spec + provenance. Reuses the metamorphic fixture
  provenance discipline (full SHA-256 SEED fingerprint, safe-int fields).
- `parseCorpusEntry(json: string): CorpusEntry` — validates and THROWS on any
  corruption (wrong kind, missing/invalid spec, non-64-hex fingerprint,
  non-safe-int seed/count). Same shape as `metamorphic.ts:parseFixture`.
- `loadCorpus(dir): CorpusEntry[]` — loads every `*.json`; a corrupt file
  **THROWS (fails loud)**, deliberately DIVERGING from upstream
  `regressions.ts:loadRegressions` which silently skips corrupt files. This
  matches the arch worker's schema invariant "permanent repros must be replayable
  and corrupt ones fail."

### 2c. sweep.ts integration (smallest, one path)

- **Replay-first (GATING)**: before the random rounds, `loadCorpus()`; for each
  entry verify the SEED fingerprint (else fail loud), materialize its minimized
  spec on stock + the target, and assert they CONVERGE (`expectConverge: true`).
  A committed entry is a divergence that was investigated/fixed; if it diverges
  again the fix regressed → gating fail. Corrupt corpus → gating fail.
- **Shrink-on-find (artifact only)**: when the random sweep finds a NEW
  divergence, `shrinkSpec` it (with a bounded budget) using
  `stillDiverges = (s) => materialize s on both, canonical-compare` and write a
  minimal `CorpusEntry` artifact (replay-first schema). The sweep already fails
  on a new divergence; the shrink only makes the artifact minimal + replayable. A
  human triages/commits the artifact into the corpus after a fix.

## 3. The four required guarantees

- **Anti-corruption**: `parseCorpusEntry` throws on any invalid field;
  `loadCorpus` fails loud on a corrupt file (NOT skip). Mutant-tested.
- **Exact replay**: an entry carries `seed` + full-SHA-256 `sourceFingerprint` +
  the minimized `spec`; replay verifies the fingerprint before boot (a SEED edit
  fails loud) and re-materializes the exact spec. No stored sources; nothing
  reinterprets silently.
- **Minimization invariants**: `oneStepShrinks` candidates each strictly reduce
  `constructCount` (bounded below by 0 → terminates); `shrinkSpec` only keeps a
  candidate that still reproduces, so the result is a local minimum that still
  diverges. Mutant-tested with a synthetic `stillDiverges`.
- **CI/nightly boundary**: replay-first corpus convergence is a GATING check on
  every push (fast: minimal specs). The random sweep stays as-is (gating: no new
  divergence). Shrink runs only on the failure path, bounded by a budget
  (`SHRINK_BUDGET`, e.g. 40 candidate evaluations), and its artifact write is
  non-gating (the divergence already failed the run). Bigger sweeps + artifact
  upload stay on nightly.

## 4. Pure test plan (no target, gating-safe)

`harness/src/spec-shrink.selftest.ts` + corpus cases in the same or a sibling
file:
- shrinker: on a large synthetic spec with a `stillDiverges` predicate that only
  depends on (say) one where-leaf, assert `shrinkSpec` reduces to a minimum
  containing that leaf, `constructCount` strictly drops each step, and it
  terminates; assert every `oneStepShrinks` candidate has a strictly smaller
  `constructCount`.
- parser: `parseCorpusEntry` accepts a well-formed entry and rejects each
  corruption (wrong kind, bad spec, short/non-hex/uppercase fingerprint,
  non-safe-int seed/count); `loadCorpus` throws on a corrupt file.

## 5. Scope boundaries

- Do NOT edit `crates/sync-core`.
- Avoid fixture / atomic-workload files while the medium worker owns them
  (`fixture.ts`, `fixture-data.ts`, atomic workload lanes).
- Avoid `plans/consistency-validation-architecture.md` and
  `harness/src/history-schema.ts` (arch worker).
- Reuse `metamorphic.ts` provenance helpers (fingerprint discipline,
  parser/mutant pattern) rather than duplicating.
- Non-gating for the shrink/artifact write; gating only for corpus-convergence
  replay + the existing sweep.
