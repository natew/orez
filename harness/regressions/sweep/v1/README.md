# sweep query differential corpus (v1)

Committed minimized `SweepDivergence` repros (`<id>.json`), loaded and validated by
`harness/src/spec-corpus.ts` (`loadCorpus`). Each entry is a query that PREVIOUSLY
diverged between stock-zero and an orez target and, after a fix, must now converge
(`expectConverge: true`).

**Currently empty — infrastructure only.** The sweep is green, so there is no
fixed-divergence to seed the corpus yet. `loadCorpus` treats an empty/missing dir
as the deterministic empty state (returns `[]`), never a vacuous green; the pure
`spec-shrink.selftest.ts` mutant tests are the non-vacuous proof of the machinery.

Only `hydrate` + round 0 + cross-target entries are exact-replayable and shrinkable
(round-0 hydrate is the only fresh-seed state). See
`../../../upstream-parity/shrink-corpus-contract.md` for the schema, the exact v1
generator grammar the parser enforces, and the replay/exit semantics.
