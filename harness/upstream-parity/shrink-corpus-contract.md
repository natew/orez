# Contract: shrink-to-repro + replay-first query differential corpus

Ranked gap #2 from `ledger.json`. Task `t-mrgtvjd6-q750` (child of
`t-mrgqyuci-23qp0`). Ports the useful subset of upstream `chinook/fuzz/shrink.ts`
+ `regressions.ts` into the orez harness. **Contract for high review — NO sweep
integration lands until approved.** Revised per high review R1–R6.

## 1. Current contract (read-only audit, unchanged at HEAD)

- **Divergence** = `canonical(stockViews[i].rows()) !== canonical(otherViews[i].rows())`
  (`sweep.ts` `compareAll`:568). `canonical()` sorts object keys but NOT the row
  array, so row ORDER is part of the compared value.
- **Failure artifact** (`recordDivergence`:481): `sweep-seed<seed>-r<round>-q<idx>.json`
  = `{seed, rounds, queriesPerRound, against, replay, phase, round, specIndex, spec, left, right}`.
  `spec` is the full random `GenSpec`; `left`/`right` are canonical strings
  truncated to 2000 chars. `phase` is already one of `hydrate` / `post-writes` /
  `incremental==fresh:<target>`.
- **Generator**: `genSpec()` → `GenSpec`, accumulated in `allSpecs`.
- **Gaps**: full unshrunk spec; no committed-corpus replay.

## 2. Phase honesty (R1) — v1 handles hydrate cross-target ONLY

The three failure phases are not equally replayable from (seed, spec):

| phase | comparison | replayable from seed+spec? | v1 |
| --- | --- | --- | --- |
| `hydrate` | cross-target (stock vs orez) | YES — fully determined by fresh seed state | **shrink + exact replay** |
| `post-writes` | cross-target | NO — view mounted before the round's deterministic writes, maintained incrementally | store full, `exactReplayable:false` |
| `incremental==fresh:<target>` | single-target (maintained vs late fresh view) | NO — history-dependent, and not cross-target | store full, `exactReplayable:false` |

v1 shrinks and exact-replays ONLY `hydrate` + `comparisonKind:'cross-target'`.
Fresh-current-state materialization is **never** labelled an exact replay of a
post-write/incremental failure. Extending to the other phases needs a typed
write-trace + view-mount ordering recreated per candidate — explicitly out of
scope, called out as the follow-up gap.

## 3. `harness/src/spec-shrink.ts` — PURE, no target (R3, R4)

- `constructCount(spec: GenSpec): number` — 1 per where condition node
  (recursively into and/or children), **per `exists` entry PLUS its nested where
  nodes**, per NON-id orderBy term, per `limit`/`start`/`one` flag, per `related`
  child (recursively into its sub). Mirrors upstream `constructCount`.
- `oneStepShrinks(spec): GenSpec[]` — every candidate strictly reduces
  `constructCount`, in a **deterministic order**, **canonical-deduped**:
  drop `limit`; drop `start`; drop `one`; **remove a non-id orderBy term ONLY
  when `spec.start === undefined`** (a start cursor seeks in the orderBy order and
  references those columns, so order is load-bearing while a start is present —
  `start` is dropped first as its own step) while **always retaining the trailing
  `id` tie-break** (never drop the whole orderBy or the last id term — that is the
  total-order invariant; dropping it manufactures false divergence); drop each
  `exists` entry; drop each `related` child and recurse into its sub; drop the
  whole `where`, or simplify it (drop an and/or child keeping ≥1; a lone child
  unwraps; recurse). PURE — only removes/simplifies, never rewrites a correlation;
  every candidate stays buildable.
- `shrinkSpec(spec, stillDiverges, budget): Promise<{ spec, evaluations, complete }>`
  — greedy delta-debug with an explicit evaluation `budget`. `complete: true` ONLY
  when a full candidate pass reaches a fixpoint (no candidate reproduces).
  Exhausting the budget first returns `complete: false` (recorded as
  `minimizationComplete: false`; **never** claimed as a local minimum).
  `evaluations` = candidate `stillDiverges` calls made. Candidate order is
  deterministic and canonical-deduped so replays are reproducible. Parameterized
  by `stillDiverges`, so the module has ZERO target/@rocicorp/zero dependency.

## 4. `harness/src/spec-corpus.ts` — PURE parser + fail-loud loader (R2, R4, R5, R6)

- ONE unified schema (replaces `recordDivergence`; no two schemas coexist):
  ```
  SweepDivergence {
    kind: 'sweep-divergence'
    id, note                         // nonempty strings
    phase: 'hydrate' | 'post-writes' | 'incremental'
    comparisonKind: 'cross-target' | 'single-target'
    exactReplayable: boolean          // true ONLY for hydrate + cross-target
    minimized: boolean                // true ONLY if shrink completed
    minimizationComplete: boolean
    spec: GenSpec
    against: string                   // REQUIRED (target the divergence was seen on)
    seed: number                      // safe non-negative int, reject -0
    sourceFingerprint: string         // lowercase 64-hex full SHA-256 of SEED
    minimizedConstructCount: number   // MUST equal constructCount(spec)
    leftHash, rightHash: string       // full SHA-256 of each canonical result
    leftPreview, rightPreview: string // truncated evidence (kept, not dropped)
    fullSidecar?: string              // optional path to untruncated results
    expectConverge: true              // a committed entry is a FIXED divergence
    replay: string                    // the --replay-corpus command below
  }
  ```
- `parseCorpusEntry(json): SweepDivergence` — FULL RECURSIVE validation, throws on
  any corruption: `kind`/`phase`/`comparisonKind` in range; nonempty
  `id`/`note`/`replay`/`against`; recursive `GenSpec` validation (where tree,
  orderBy `[col,'asc'|'desc']` pairs, exists, start shape, related recursively,
  one); lowercase 64-hex `sourceFingerprint`; `seed`/`minimizedConstructCount`
  safe non-negative integers with `Object.is(-0)` rejected; `expectConverge ===
  true`; and `minimizedConstructCount === constructCount(spec)` (the record's own
  claim is cross-checked). Same discipline as `metamorphic.ts:parseFixture`.
- `loadCorpus(dir): SweepDivergence[]` — reads `*.json` in SORTED path order; a
  corrupt file **THROWS (fails loud)** (diverges from upstream `loadRegressions`
  which skips — matches "corrupt repros must fail").
- **Dedicated versioned path `harness/regressions/sweep/v1/`** so the loader never
  sees the metamorphic `known-gap-*.json` (which live in `harness/regressions/`)
  and never mis-parses them as corrupt sweep entries.

## 5. Integration + CLI (R1, R2, R6)

- **Replace `recordDivergence` entirely** with a single writer emitting the
  unified `SweepDivergence` to `sweep/v1/`. Hydrate cross-target findings are
  shrunk (honest `minimized`/`minimizationComplete`); post-writes/incremental are
  written full with `exactReplayable:false`, preserving `leftHash`/`rightHash` +
  previews (old divergence evidence is NOT dropped).
- **Shrink budget is GLOBAL per run** — bound to the first divergence per run (or
  one shared evaluation budget across the run), NOT 40 per `compareAll` finding.
- **`stillDiverges`** (the integration's predicate): materialize the candidate on
  both fresh-seed clients and canonical-compare; a build error or timeout counts
  as **non-reproduction** (returns false, conservatively rejecting the shrink);
  **always destroys the candidate views** (no leak) in a finally.
- **`--replay-corpus <file>` CLI** — exact replay of ONE entry, NO random rounds,
  cwd-stable (resolve the file + regressions dir from `import.meta.dirname`).
  `against` is REQUIRED and must equal `entry.against` (else exit 2). Verifies the
  SEED fingerprint before boot, materializes the minimized hydrate spec on stock +
  target on fresh seed state, asserts CONVERGE. This IS the entry's `replay`
  command. exit 0 = still converges; exit 1 = re-diverged (regression); exit 2 =
  corrupt/target-mismatch/fingerprint-mismatch/non-exactReplayable entry.
- **Corpus preload** (loading the whole corpus before a random sweep, replaying
  each exactReplayable entry) is a gate OF THE SWEEP LANE, distinct from the
  single-entry command.

## 6. CI / nightly boundary (R2, and the boot-cost point)

- **PR-gating**: the PURE `spec-shrink` + `spec-corpus` mutant tests only (no
  boot, no Node/native prereqs). These gate every push.
- **Nightly (until boot cost is measured)**: the credential-free live corpus
  replay + the sweep corpus-preload. Live stock-zero boot (embedded pg +
  zero-cache, Node 22/24 + native binding) dominates wall-clock regardless of spec
  size, so it does not belong in the PR gate merely because specs are small.

## 7. Anti-vacuity (R6)

Sweep is currently green — there is NO committed fixed-divergence entry to seed
the corpus, so v1 lands as **infrastructure-only**. The corpus loader reports an
explicit status `anti-vacuity: 0 exact-replayable entries (infrastructure-only)`
rather than a vacuous green, and the corpus gate is a no-op-with-status until a
real divergence is found, fixed, and its minimized entry committed. The pure
mutant tests are the non-vacuous proof of the machinery meanwhile.

## 8. Guarantees mapping

- **Anti-corruption**: full recursive validation + fail-loud loader, mutant-tested.
- **Exact replay**: only claimed for hydrate cross-target; seed + full fingerprint
  verified before boot + minimized spec; `--replay-corpus` single-entry command;
  no stored sources; `against` required and enforced.
- **Minimization invariants**: strict `constructCount` reduction each step →
  terminates; `complete:true` only at a real fixpoint; budget exhaustion recorded
  as `minimizationComplete:false`; deterministic, canonical-deduped candidate order.
- **CI/nightly boundary**: pure mutants gate PR; live corpus nightly.

## 9. Pure test plan (no target, gates PR)

`harness/src/spec-shrink.selftest.ts`:
- shrinker: a synthetic `stillDiverges` depending on one where-leaf → `shrinkSpec`
  reduces to a minimum still containing it; every `oneStepShrinks` candidate has a
  strictly smaller `constructCount`; deterministic + deduped candidate list;
  budget exhaustion returns `complete:false`; id tie-break always retained; order
  term never removed while a start is present.
- parser: `parseCorpusEntry` accepts a well-formed entry and rejects each
  corruption (wrong kind/phase/comparisonKind, bad recursive spec, short/non-hex/
  uppercase fingerprint, `-0`/unsafe/negative seed or count, mismatched
  `minimizedConstructCount`, missing required field, `expectConverge!==true`);
  `loadCorpus` throws on a corrupt file and sorts paths.

## 10. Scope boundaries

- No `crates/sync-core`; avoid fixture / atomic-workload files (medium owns);
  avoid `plans/consistency-validation-architecture.md` + `harness/src/history-schema.ts`
  (arch worker). Reuse `metamorphic.ts` provenance helpers.
