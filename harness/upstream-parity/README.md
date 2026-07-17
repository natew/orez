# upstream-parity: Orez ↔ rocicorp/mono Zero conformance ledger

Machine-readable accounting of upstream ZQL/Zero conformance assets versus the
Orez black-box harness, so parity is auditable and drift is explicit. Owned by
the upstream-parity worker (agentbus task `t-mrgqz3g5-6030`).

## files

- **`ledger.json`** — the map. `meta` (audited + baseline SHAs, protocol, pin),
  `drift` (the 7 commits baseline→audited, each classified), `fuzzModules` (all
  20 fuzz files individually), `conformanceSuites` (selected surfaces),
  `invariants`, `regressionCorpusSchema`, `coverageModel`, `rankedGaps`,
  `knownXfails`.
- **`inventory.json`** — exhaustive per-file accounting: all **24**
  `zql-integration-tests` files individually, the **143** zero-cache files by
  category, **9** zero-protocol, **20** fuzz files, with audited totals and
  aggregates.
- **`verify.ts`** — the evidence + drift script (below).

## the two audited SHAs

- **audited upstream:** `7139287da3c84ec5050c1eff0d9444d912d462aa`
  (`origin/main`, 2026-07-11) — what this ledger describes.
- **Orez baseline:** `6d84471c5c556599edfb9328d102316446db35a2` (2026-07-09) —
  the mono HEAD the harness was originally built against, retained ONLY so drift
  is explicit. Do not re-pin work to it.

The 7-commit drift is entirely query-semantics + zero-cache connection/DDL
robustness; the fuzz generator directory is byte-identical across the range.

## status vocabulary (conservative, not green-inflated)

`upstream-baseline` · `ported-black-box` · `equivalent` · `equivalent-partial`
(behavior re-expressed but with a named missing piece) · `portable-gap`
(portable + valuable + not yet built) · `not-applicable` (with reason).

**Test portability and behavioral coverage are tracked separately.** A test
being implementation-coupled (in-process, drives mono's module graph) does not
make its behavior uncovered: e.g. most of `zero-cache` is in-process tests, but
view-syncer's downstream/poke contract, the read-authorizer's visibility, and
custom-mutator push are all re-expressed black-box in the Orez lanes. See
`inventory.json.zeroCache.behavioralCoverageSummary`.

## the finding that drove the first implementation

`#6121` (mono `d4f33d6a6`, 2026-07-10, _after_ the 1.7.0 pin): a `start()` cursor
anchored on a NULL-sorted row compiled `col > NULL` (SQL NULL → matches nothing)
and silently returned empty.

**What the lane actually found (2026-07-11, verified deterministic):** running
metamorphic against the **stock zero-cache 1.7.0 reference**, `startSuffix`
flagged one shape — `start(dueAt asc, after {dueAt:null, id:t1}, exclusive)` —
where stock returns `[]` but the correct answer is the 47-row ordered suffix.
That is confirmed #6121 in the reference's **server-side sqlite table-source**.
The **orez-local lane passes the same shape**: it ships full snapshots and
materializes client-side, so it never pushes the start into the buggy sqlite
fetch. Orez's own Rust server compiler also handles NULL cursors correctly and
is tested (`crates/sync-core/tests/query_ast.rs::start_cursor_null_ordering`).

Why the stock-vs-Orez **differential** does not gate it: the sweep generator now
has a nullable-column start-cursor axis, but suppresses that axis before
materializing against the 1.7.0 stock pin. The pin can return wrong rows for the
initial query and can crash its view-syncer when later edits flow through the
pipeline, so no cross-target comparison is possible. The generator reports how
many candidates it suppressed. The metamorphic guard keeps the axis covered
**with no oracle** against a single target. Its distinct, structural value
remains the harder class the differential is blind to by construction, where
both targets share the same wrong behavior, plus running against any one target
(including CF) with no reference implementation. See `../regressions/` for the
recorded repro.

## stock-pin generator suppressions

The randomized sweep always boots the pinned stock zero-cache as its reference.
Generated shapes that crash that reference must be excluded before
materialization; classifying the resulting disconnect as an expected failure
would hide every later comparison in the run.

- **Null-anchored `start()`**: suppressed because stock 1.7.0 contains #6121 and
  its IVM can fail with `Bound should be set` after edits. The deterministic
  Rust/TypeScript differential and the metamorphic `startSuffix` relation cover
  the cursor behavior without this stock pin.
- **Explicit nullable-bound take window**: a task query with `.limit(n)` ordered
  by `dueAt asc` places a null-sorted row at the window bound in the seeded data.
  On sweep seed `29558895429`, a later task edit made stock 1.7.0 lose that bound
  and assert in `zql/src/ivm/take.js`. The same mirrored write trace remained
  healthy on `orez-local`; the failure response and view-syncer shutdown came
  only from `stock-zero`. Removing the limit or ordering by `rank`, `id`, or
  `dueAt desc` passed, while removing filters and relationships still crashed,
  which isolates the suppressed shape to the nullable ascending limit. The
  generator applies this exclusion to root and related task windows. It still
  exercises unbounded `dueAt asc` ordering and limited windows on other orders.

Both suppressions are temporary compatibility gaps. Remove their flags and the
matching generator or coverage logic when the stock oracle advances to a
version that serves and incrementally maintains these shapes.

The oracle-free technique is **metamorphic self-consistency** (upstream's own,
`fuzz/metamorphic.ts`): a transform whose result relationship is known without an
oracle, checked against a single target.

## the metamorphic guard (`../src/metamorphic.ts`)

Pure checker (no server, no `@rocicorp/zero` import). Ports upstream's
always-true invariants (`redundantConjunct`, `andReorder`, `largeLimit`) and
adds two **computed** relations upstream's metamorphic layer lacks (it only
exercises non-binding start/limit):

- **`limitPrefix`** — `Q.limit(n)` equals `Q` (no limit) truncated to `n`.
- **`startSuffix`** — `Q.start(cursor)` equals `Q` (start+limit removed) sliced
  at the cursor. It trusts the engine's ORDER (the well-tested path) and tests
  only that `start` positions correctly within it, so it cannot false-positive
  from a hand-rolled NULL comparator while still catching a cursor that lands at
  the wrong position — exactly #6121.

### checker validation vs product conformance (kept strictly separate)

- **`../src/metamorphic.selftest.ts` — checker validation.** Feeds the pure
  relations the output a correct and a buggy engine would each produce and
  asserts pass/**fail**. Green, deterministic, no server. This is the mutation
  proof that the guard is non-vacuous; it is safe to gate. `bun src/metamorphic.selftest.ts`
- **`../src/metamorphic-lane.ts` — product/reference conformance. NON-GATING.**
  Runs the same relations against a live target. Because the 1.7.0 pin may
  genuinely contain #6121, a FAIL here is a **classified known-gap/repro**
  (written to `../regressions/` in the same schema `--replay` reads), never
  dressed green via an expected-failure, and it is **not** wired into the gating
  CI harness job. Run it manually or in a nightly audit.
  `bun src/metamorphic-lane.ts --against orez-local`
  (`--mutate startSuffix` plants #6121 live to prove the wiring catches it; it
  writes no artifact.) The stock-zero reference target spawns zero-cache under
  Node, and `@rocicorp/zero-sqlite3` supports **Node 22.x/24.x, not 25.x**, so
  run it under a supported Node — `mise x node@22 -- bun src/metamorphic-lane.ts
--against stock-zero`. Installing deps under that Node
  (`mise x node@22 -- bun install`, frozen is fine) provides the packaged
  zero-sqlite3 binding automatically; **no manual node-gyp build is needed.** The
  embedded-postgres dylib/soname links are recreated automatically at boot
  (`src/targets/stock-zero.ts`).
- **`--replay <fixture.json>` — stable known-gap replay.** Executes EXACTLY the
  recorded spec + relation from a committed fixture (not the generator), gated by
  a SEED fingerprint. Exit semantics: a recorded `fail` that still **REPRODUCES
  exits 1** (the known product failure is real, never greened); a recorded `pass`
  that still holds exits 0; a corrupt/inapplicable fixture, an explicit
  `--against` that differs from `fixture.target`, a SEED fingerprint or row-count
  mismatch, or an outcome that no longer matches the record all exit 2. It writes
  no artifact and never regenerates the corpus. Full prerequisites +
  exit-semantics table are in the fixture itself
  (`../regressions/known-gap-zql-6121-null-start-cursor.json`).

## verify / drift

```sh
bun harness/upstream-parity/verify.ts                # verify inventory + drift
bun harness/upstream-parity/verify.ts --allow-drift  # ignore SHA drift; still
                                                     # fail on a count/set mismatch
MONO=/path/to/mono bun harness/upstream-parity/verify.ts
```

It asserts the inventory against the real mono checkout at the audited SHA (the
exact 24-file set, the 143/47 zero-cache counts, 9 zero-protocol, 19 fuzz
modules, aggregate arithmetic, and that every `ported-black-box` artifact the
ledger names exists), and flags any commit on `origin/main` past the audited
SHA that touches conformance test/fuzz files. A stale audit or unacknowledged
drift exits non-zero — so parity cannot silently decay. Intended for a
non-gating nightly audit.
