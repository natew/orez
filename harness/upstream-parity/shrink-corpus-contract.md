# Contract: shrink-to-repro + replay-first query differential corpus

Ranked gap #2 from `ledger.json`. Task `t-mrgtvjd6-q750`. Ports the useful subset
of upstream `chinook/fuzz/shrink.ts` + `regressions.ts`. **High conditionally
approved the PURE modules + hydrate-round0 infrastructure slice once R2 R1–R4 are
incorporated (below); the LIVE sweep integration returns for separate code
review.** Revised through high R1 (R1–R6) and R2 (R1–R4).

## 1. Current contract (read-only audit, unchanged at HEAD)

- **Divergence** = `canonical(stockViews[i].rows()) !== canonical(otherViews[i].rows())`
  (`sweep.ts` `compareAll`:568). `canonical()` sorts object keys but NOT the row
  array, so row ORDER is compared.
- **Failure artifact** (`recordDivergence`:481): `sweep-seed<seed>-r<round>-q<idx>.json`
  = `{seed, rounds, queriesPerRound, against, replay, phase, round, specIndex, spec, left, right}`.
- **Generator**: `genSpec()` → `GenSpec`, accumulated in `allSpecs`.

## 2. Phase honesty (R1×2) — exact replay ⇔ hydrate + ROUND 0 + cross-target

Only **round-0** hydrate is fresh seed state. Round N hydrate runs AFTER the
writes of rounds 0..N-1, so it is history-dependent like post-writes.

| phase         | comparison                                    | round | replayable from (seed, spec)?               | v1                        |
| ------------- | --------------------------------------------- | ----- | ------------------------------------------- | ------------------------- |
| `hydrate`     | cross-target                                  | **0** | YES — fresh seed state                      | **shrink + exact replay** |
| `hydrate`     | cross-target                                  | >0    | NO — after prior-round writes               | store, nonexact           |
| `post-writes` | cross-target                                  | any   | NO — view mounted before the round's writes | store, nonexact           |
| `incremental` | single-target (maintained vs late fresh view) | any   | NO — history-dependent                      | store, nonexact           |

v1 shrinks and exact-replays ONLY `phase==='hydrate' && round===0 &&
comparisonKind==='cross-target'`. All other divergences are stored with
`exactReplayable:false`, full result hashes + previews, and their full seeded
sweep replay command. Fresh-current-state materialization is **never** labelled
an exact replay of a history-dependent failure. Carrying/applying the exact prior
write state per candidate (typed write-trace + view-mount ordering) is the
explicit follow-up gap. **Every artifact preserves `round`/`specIndex`/`rounds`/
`queriesPerRound` provenance, exact or not.**

## 3. `harness/src/spec-shrink.ts` — PURE, no target (R1.R3/R4)

- `constructCount(spec): number` — 1 per where condition node (recursively into
  and/or children), **per `exists` entry PLUS its nested where nodes**, per NON-id
  orderBy term, per `limit`/`start`/`one` flag, per `related` child (recursively).
- `oneStepShrinks(spec): GenSpec[]` — every candidate strictly reduces
  `constructCount`, in a **deterministic order**, **canonical-deduped**: drop
  `limit`; drop `start`; drop `one`; **remove a non-id orderBy term ONLY when
  `spec.start === undefined`** (drop `start` first; the cursor references order
  columns) while **always retaining the trailing `id` tie-break** (never drop the
  whole orderBy or the last id term — the total-order invariant; dropping it
  manufactures false divergence); drop each `exists`; drop each `related` child and
  recurse; drop the whole `where` or simplify it (drop an and/or child keeping ≥1;
  lone child unwraps; recurse). PURE — only removes/simplifies.
- `shrinkSpec(spec, stillDiverges, budget): Promise<{ spec, evaluations, complete }>`
  — greedy delta-debug with explicit evaluation `budget`. `complete:true` ONLY when
  a full candidate pass reaches a fixpoint. Budget exhaustion → `complete:false`
  (recorded `minimizationComplete:false`; never a claimed minimum). Deterministic,
  canonical-deduped candidate order. Zero target/@rocicorp/zero dependency.

## 4. `harness/src/spec-corpus.ts` — PURE parser + fail-loud loader (R1.R2/R4/R5/R6, R2.R1/R2/R4)

The schema graph (columns + kinds + relationship CARDINALITY) lives in the pure,
zero-free `harness/src/fixture-graph.ts`, shared by the shrinker and the parser so
the frozen grammar has one source of truth.

- ONE unified schema (replaces `recordDivergence`), AS IMPLEMENTED:
  ```
  SweepDivergence {
    schemaVersion: 1
    kind: 'sweep-divergence'
    id                                // safe token /^[A-Za-z0-9][A-Za-z0-9._-]*$/ ; === filename basename
    note                              // nonempty NUL-free string
    phase: 'hydrate' | 'post-writes' | 'incremental'
    comparisonKind: 'cross-target' | 'single-target'
    round, specIndex, rounds, queriesPerRound: number   // provenance, ALWAYS; safe non-neg ints
    exactReplayable: boolean          // === (phase==='hydrate' && round===0 && comparisonKind==='cross-target')
    minimizationComplete: boolean     // false whenever !exactReplayable
    spec: GenSpec                     // validated against the exact v1 generator grammar
    against: string                   // ∈ {orez-local, orez-cf, rust-local, rust-cf} (NOT stock-zero)
    observedTarget: string            // the target the comparison ran on (no fault attribution):
                                      //   cross-target === against; single-target ∈ {stock-zero, against}
    seed: number                      // NUMERIC generator seed (distinct from sourceFingerprint)
    sourceFingerprint: string         // FIXED fixture-data SEED digest (lowercase 64-hex SHA-256);
                                      //   the loader compares it to the CURRENT digest, not just format
    constructCount: number            // === constructCount(spec) of the stored spec
    originalConstructCount?: number   // pre-shrink count (>= constructCount), reduction evidence
    leftHash, rightHash: string       // lowercase 64-hex SHA-256 of each canonical result; MUST DIFFER
    leftPreview, rightPreview: string // truncated evidence strings (kept)
    fullSidecar?: string              // OPTIONAL safe repo-relative path (not absolute, no '..'/backslash/NUL)
    expectConverge: true
    replay: string                    // REQUIRED, phase-specific, EXACT-equal to buildReplayCommand(entry)
  }
  ```
  `replay` is `--replay-corpus regressions/sweep/v1/<id>.json --against <against>`
  when `exactReplayable`, else the full seeded sweep command
  (`bun src/sweep.ts --seed <seed> --against <against> --rounds <rounds> --queriesPerRound <q>`).
  A nonexact entry never carries a `--replay-corpus` command that would only exit 2.
- `parseCorpusEntry(json, {expectedFingerprint?})` — throws on ANY corruption. Beyond the
  field/format checks above: no unknown keys at any object level; FULLY RECURSIVE
  `GenSpec` grammar — column-kind ops/values (id `=,!=` string / `IN` 2..4 strings;
  string `LIKE/ILIKE/!=`; number comparison + finite; boolean `=,!=`; nullable
  `IS/IS NOT null` or `</>` finite; json `IS/IS NOT null`); orderBy exactly
  `[[id,asc]]` or `[[nonId,asc|desc],[id,asc]]` (json not orderable); and/or 2..3
  children within depth; exists ≤ 1; related ≤ 2 root / ≤ 1 nested, depth ≤ 2,
  unique rels, **cardinality-exact subs** (a `one`-relation sub is `{one:true,related?}`;
  a `many`-relation sub is absent or `{orderBy(required),where?,limit?,related?}` with
  no `one`); limits `1..8` root / `1..4` sub; root `one` XOR `limit`; start only on
  root task with orderBy `[[rank,dir],[id,asc]]`, row exactly `{rank:number, id:string}`,
  inclusive `true`|absent. Cross-field: exactReplayable ⇔ hydrate+round0+cross;
  incremental ⇔ single-target; post-writes ⇒ cross-target; `minimizationComplete`
  false when nonexact; target-role invariants; `constructCount === constructCount(spec)`;
  `replay` EXACT-equal to the canonical builder. Provenance: `rounds*queriesPerRound`
  safe-integer (overflow guard for every phase), then phase-sensitive
  `specIndex < (round+1)*queriesPerRound` (hydrate/post) / `< rounds*queriesPerRound`
  (incremental). If `expectedFingerprint` is passed, `sourceFingerprint` must equal it.
- `loadCorpus(dir)` — a **missing directory is the deterministic empty infrastructure
  state (`[]`, no throw)**; otherwise reads `*.json` in SORTED filename order, parses
  each against the CURRENT fixture fingerprint, then rejects DUPLICATE ids (before the
  per-file check) and any file whose basename != its id; a corrupt file THROWS.
- `assertValidSpec(spec)` is exported so the selftest proves **executable closure**:
  every `oneStepShrinks(big)` candidate passes the parser's grammar validator.

## 5. Live integration (DESIGN ONLY — returns for code high review)

- **Replace `recordDivergence`** with one writer emitting `SweepDivergence` to
  `harness/regressions/sweep/v1/`. Deterministic, collision-safe id/path
  `sweep-<phase>-r<round>-q<specIndex>-<sha256(canonical{seed,spec}).slice>` and it
  **REFUSES to overwrite** a file whose content differs (never silently clobbers).
- Shrink applies to the first **ELIGIBLE** divergence per run (hydrate + round 0 +
  cross-target), under one **global** evaluation budget — not the first divergence
  of any phase, not 40 per `compareAll` finding.
- `compareAll` stays SYNCHRONOUS (detects + records the eligible spec into a list);
  the ASYNC `shrinkSpec` runs as a POST-STEP after the round, since `stillDiverges`
  materializes on live targets. `stillDiverges` treats build/timeout as
  NON-reproduction (returns false) and ALWAYS destroys candidate views in `finally`.
- A **build preflight** (`buildGenerated(spec)` in the lane, which has zero) runs
  before target boot as an extra guard on any replayed/shrunk spec.

## 6. CI / nightly boundary (R1.R2 + R2.R3)

ONE path, no contradiction: corpus preload is **opt-in behind a `--corpus` flag /
a dedicated nightly command**. The ordinary PR `sweep.ts` run does NOT preload the
corpus, so no new PR-gating live work is added (the PR sweep already boots stock;
per the manager the live corpus stays scheduled until boot cost is measured, and
boot-cost alone is not the sole rationale). A missing/empty `sweep/v1/` dir yields
an explicit `infrastructure-only` status, never a vacuous green.

- **PR-gating**: the PURE `spec-shrink` + `spec-corpus` mutant tests (no boot).
- **Nightly**: `sweep.ts --corpus` (preload + replay each exactReplayable entry,
  assert converge) + the sweep itself.

## 7. Anti-vacuity (R1.R6)

Sweep is green — no fixed-divergence entry exists, so v1 lands **infrastructure-
only**: `loadCorpus` on the (absent) `sweep/v1/` dir returns `[]` and the corpus
gate reports `anti-vacuity: 0 exact-replayable entries (infrastructure-only)`.
The pure mutant tests are the non-vacuous proof of the machinery until a real
divergence is found, fixed, and its minimized round-0 entry committed.

## 8. Pure test plan (no target, gates PR)

`harness/src/spec-shrink.selftest.ts`:

- shrinker: synthetic `stillDiverges` on one where-leaf → `shrinkSpec` reduces to a
  minimum still containing it; every `oneStepShrinks` candidate strictly smaller;
  deterministic + deduped; budget exhaustion → `complete:false`; id tie-break
  always retained; order term never removed while a start is present.
- parser: `parseCorpusEntry` accepts a well-formed exact + a well-formed nonexact
  entry, and rejects every corruption (schemaVersion, kind/phase/comparisonKind,
  non-boolean flags, bad recursive spec, missing id-tiebreak, unknown table/column/
  relationship, short/non-hex/uppercase hashes, `-0`/unsafe/negative ints,
  mismatched `minimizedConstructCount`, exactReplayable/phase/round contradiction,
  minimized≠minimizationComplete, nonexact carrying `--replay-corpus`, unsafe
  `fullSidecar`); `loadCorpus` returns `[]` for a missing dir and throws on a
  corrupt file; sorts paths.

## 9. Scope boundaries

- No `crates/sync-core`; avoid fixture / atomic-workload files (medium owns);
  avoid `plans/consistency-validation-architecture.md` + `harness/src/history-schema.ts`
  (arch worker). Reuse `metamorphic.ts` provenance helpers.
