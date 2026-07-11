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
where stock returns `[]` but the correct answer is the 46-row ordered suffix.
That is confirmed #6121 in the reference's **server-side sqlite table-source**.
The **orez-local lane passes the same shape**: it ships full snapshots and
materializes client-side, so it never pushes the start into the buggy sqlite
fetch. Orez's own Rust server compiler also handles NULL cursors correctly and
is tested (`crates/sync-core/tests/query_ast.rs::start_cursor_null_ordering`).

Why the stock-vs-Orez **differential** misses it: the sweep generator has **no
nullable-column start-cursor axis**, so this shape is never generated. Had it
been, stock (`[]`) and orez-local (rows) would DIVERGE and the differential
would also flag it — so #6121 is a _generator-coverage_ miss, not a case of both
sides being wrong. The metamorphic guard caught it **with no oracle** by
exercising that axis on a single target. Its distinct, structural value remains
the harder class the differential is blind to _by construction_ — where both
targets share the same wrong behavior — plus running against any one target
(incl. CF) with no reference impl. See `../regressions/` for the recorded repro.

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
  (written to `../regressions/`), never dressed green via an expected-failure,
  and it is **not** wired into the gating CI harness job. Run it manually or in a
  nightly audit. `bun src/metamorphic-lane.ts --against orez-local`
  (`--mutate startSuffix` plants #6121 live to prove the wiring catches it.)
  The stock-zero reference target needs a **supported Node** for the spawned
  zero-cache (`@rocicorp/zero-sqlite3` = Node 22.x/24.x, not 25.x) and its built
  native binding, so run it as
  `mise x node@22 -- bun src/metamorphic-lane.ts --against stock-zero`; if the
  binding is missing, build it once with
  `cd node_modules/@rocicorp/zero-sqlite3 && mise x node@22 -- npm run install`.
  The embedded-postgres dylib/soname links are recreated automatically at boot
  (`src/targets/stock-zero.ts`). Full prerequisites are in
  `../regressions/known-gap-zql-6121-null-start-cursor.json`.

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
