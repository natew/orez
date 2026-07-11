// metamorphic relations for the query generator: semantically-invariant
// transforms whose result relationship to the original is known WITHOUT an
// oracle. ported black-box from rocicorp/mono
// packages/zql-integration-tests/src/chinook/fuzz/metamorphic.ts
// (audited at origin/main 7139287da3c84ec5050c1eff0d9444d912d462aa;
//  see harness/upstream-parity/ledger.json).
//
// WHY this exists alongside the differential (shapes.ts / sweep.ts): it is
// ORACLE-FREE — it checks a single target's self-consistency under a transform
// whose result relationship is known, so it needs no second implementation or
// pg oracle and runs against any one target (incl. CF). Upstream's own doc:
// metamorphic is "checked IVM-vs-IVM (no oracle needed) ... a different failure
// mode than the differential oracle: engine self-inconsistency under a
// transform the answer is known to be invariant under." It catches two things a
// stock-vs-orez differential can miss: (1) a bug in an axis the differential's
// GENERATOR never emits, and (2) the harder class where BOTH targets share the
// same wrong behavior (a differential is blind to shared bugs by construction).
//
// EMPIRICALLY (2026-07-11) it found #6121 (null-safe start constraints, lands
// AFTER the 1.7.0 pin) — a case (1) bug: a start cursor anchored on a NULL-
// sorted row returns EMPTY on the stock zero-cache reference (server-side sqlite
// table-source), while orez-local returns the correct suffix (it ships full
// snapshots and materializes client-side, so it does not push the start into
// the buggy sqlite fetch). The stock-vs-orez differential misses it because the
// sweep generator has no nullable-column start-cursor axis; had it emitted one,
// the two targets would DIVERGE (stock=[], orez=rows) and the differential would
// also catch it. The metamorphic guard caught it with no oracle by exercising
// that axis on a single target. See harness/regressions/ for the recorded repro.
//
// This module is PURE (no target boot, no @rocicorp/zero import): it produces
// transformed GenSpecs and a `relate()` that decides pass/fail/skip from two
// already-materialized result sets. That keeps the checker itself unit-testable
// and mutation-provable (metamorphic.selftest.ts) independent of any server.

import { canonical } from './canonical.js'

import type { GenSpec, GenWhere } from './fixture.js'

// upstream's largeLimit uses 100_000 (>= their whole fixture). the orez fixture
// is ~70 rows + churn; 1_000_000 is unambiguously non-binding.
const LARGE_LIMIT = 1_000_000

export type Relation =
  // upstream's four, minus flip semantics (no wire analog); the always-true
  // invariant transforms:
  | 'redundantConjunct' // AND `id IS NOT null` (PK is non-null) must not filter
  | 'andReorder' // reordering an AND's conjuncts is invariant
  | 'largeLimit' // a limit >= the result size is a no-op
  // stronger COMPUTED relations upstream's metamorphic layer does not do (it
  // only exercises NON-binding start/limit). these bind the window/cursor and
  // are the catchers for the NULL-cursor / window semantics (#6121):
  | 'limitPrefix' // Q.limit(n) == (Q without limit) truncated to n
  | 'startSuffix' // Q.start(cursor) == (Q without start) sliced at the cursor

// the closed vocabulary — used to validate CLI --mutate so a typo cannot
// silently run unmutated and claim a wiring proof.
export const RELATIONS: readonly Relation[] = [
  'redundantConjunct',
  'andReorder',
  'largeLimit',
  'limitPrefix',
  'startSuffix',
]

export type RelResult = 'pass' | 'fail' | 'skip'

export type RelateOutcome = {
  result: RelResult
  detail?: string
  // for a fail: what the relation says the base result should have been
  expected?: unknown
}

export type MetamorphicCheck = {
  relation: Relation
  // the original generated query
  base: GenSpec
  // the transformed query to ALSO materialize
  variant: GenSpec
  // decide the invariant from the two materialized result sets
  relate: (baseRows: unknown, variantRows: unknown) => RelateOutcome
}

// ---------------------------------------------------------------------------
// pure spec transforms (never mutate the input)
// ---------------------------------------------------------------------------

const NOT_NULL_ID: GenWhere = { op: 'cmp', col: 'id', cmp: 'IS NOT', value: null }

// AND an always-true `id IS NOT null` onto the root where. `id` is the primary
// key of every fixture table, so it is non-null and the conjunct cannot filter.
export function withRedundantConjunct(spec: GenSpec): GenSpec {
  const where: GenWhere = !spec.where
    ? NOT_NULL_ID
    : spec.where.op === 'and'
      ? { op: 'and', children: [...spec.where.children, NOT_NULL_ID] }
      : { op: 'and', children: [spec.where, NOT_NULL_ID] }
  return { ...spec, where }
}

// reverse the conjuncts of a root AND (AND is commutative). null if the root
// where is not an AND of >= 2 conjuncts.
export function withReversedAnd(spec: GenSpec): GenSpec | null {
  if (spec.where?.op !== 'and' || spec.where.children.length < 2) return null
  return { ...spec, where: { op: 'and', children: [...spec.where.children].reverse() } }
}

export function withLargeLimit(spec: GenSpec): GenSpec {
  return { ...spec, limit: LARGE_LIMIT }
}

export function withoutLimit(spec: GenSpec): GenSpec {
  const copy = { ...spec }
  delete copy.limit
  return copy
}

export function withoutStartAndLimit(spec: GenSpec): GenSpec {
  const copy = { ...spec }
  delete copy.start
  delete copy.limit
  return copy
}

// ---------------------------------------------------------------------------
// computed expectations (pure): trust the ENGINE'S ordering (the reference,
// well-tested path) and test only that limit/start POSITION within it. this
// deliberately avoids reimplementing NULL-vs-value comparison, so the checker
// cannot produce false positives from a hand-rolled comparator — while still
// catching a start cursor that lands at the wrong position (exactly #6121).
// ---------------------------------------------------------------------------

function asRows(v: unknown): Record<string, unknown>[] | null {
  return Array.isArray(v) ? (v as Record<string, unknown>[]) : null
}

export function expectedLimitPrefix(
  referenceRows: unknown,
  limit: number
): RelateOutcome {
  const ref = asRows(referenceRows)
  if (ref === null) return { result: 'skip', detail: 'reference not an array' }
  return { result: 'pass', expected: ref.slice(0, limit) }
}

// expected result of Q.start(cursor, {inclusive?}).limit(limit?), computed from
// the reference (Q with start AND limit removed, i.e. the full ordered result).
// the cursor row carries the PK `id`; we locate it in the reference by id (a
// total order, since orderBy always ends with the PK), then slice. SKIP when
// the cursor row is absent from the reference (a churned/dead cursor), because
// the expected boundary then needs the NULL-aware comparator we refuse to
// reimplement.
export function expectedStartSuffix(
  referenceRows: unknown,
  start: { row: Record<string, unknown>; inclusive?: boolean },
  limit: number | undefined
): RelateOutcome {
  const ref = asRows(referenceRows)
  if (ref === null) return { result: 'skip', detail: 'reference not an array' }
  const cursorId = start.row.id
  if (cursorId === undefined) return { result: 'skip', detail: 'cursor row has no id' }
  const idx = ref.findIndex((r) => r?.id === cursorId)
  if (idx < 0) return { result: 'skip', detail: 'cursor row absent from reference' }
  const from = start.inclusive ? idx : idx + 1
  let suffix = ref.slice(from)
  if (limit !== undefined) suffix = suffix.slice(0, limit)
  return { result: 'pass', expected: suffix }
}

// ---------------------------------------------------------------------------
// relation assembly
// ---------------------------------------------------------------------------

function equalRelate(baseRows: unknown, variantRows: unknown): RelateOutcome {
  return canonical(baseRows) === canonical(variantRows)
    ? { result: 'pass' }
    : { result: 'fail', detail: 'transform changed the result', expected: variantRows }
}

// compare the base result against a computed expectation; skip propagates.
function computedRelate(compute: (variantRows: unknown) => RelateOutcome) {
  return (baseRows: unknown, variantRows: unknown): RelateOutcome => {
    const exp = compute(variantRows)
    if (exp.result !== 'pass') return exp // skip / (already a fail is impossible here)
    return canonical(baseRows) === canonical(exp.expected)
      ? { result: 'pass' }
      : {
          result: 'fail',
          detail: 'base does not match the computed window',
          expected: exp.expected,
        }
  }
}

// every metamorphic check applicable to `spec`. relations that bind an array
// result are guarded off `one()` (a singular result is not sliceable). the
// root result of every non-one spec is a total order (orderBy always ends with
// the PK), so limitPrefix/startSuffix positions are unambiguous.
export function metamorphicChecks(spec: GenSpec): MetamorphicCheck[] {
  const checks: MetamorphicCheck[] = []
  const singular = spec.one === true

  // redundantConjunct — always applicable (every table has a non-null PK `id`).
  checks.push({
    relation: 'redundantConjunct',
    base: spec,
    variant: withRedundantConjunct(spec),
    relate: equalRelate,
  })

  // andReorder — only when the root where is an AND of >= 2 conjuncts.
  const reversed = withReversedAnd(spec)
  if (reversed) {
    checks.push({
      relation: 'andReorder',
      base: spec,
      variant: reversed,
      relate: equalRelate,
    })
  }

  // largeLimit — non-binding take; only when no limit is present and the result
  // is an array.
  if (spec.limit === undefined && !singular) {
    checks.push({
      relation: 'largeLimit',
      base: spec,
      variant: withLargeLimit(spec),
      relate: equalRelate,
    })
  }

  // limitPrefix — BINDING. Q.limit(n) is a prefix of Q without the limit.
  // require a real limit, an array result, and no start (start+limit is handled
  // by startSuffix, which removes both).
  if (spec.limit !== undefined && !singular && spec.start === undefined) {
    const n = spec.limit
    checks.push({
      relation: 'limitPrefix',
      base: spec,
      variant: withoutLimit(spec),
      relate: computedRelate((variantRows) => expectedLimitPrefix(variantRows, n)),
    })
  }

  // startSuffix — BINDING, the NULL-cursor blind-spot catcher. Q.start(cursor)
  // (optionally with a limit) equals Q with start+limit removed, sliced at the
  // cursor position. array result only.
  if (spec.start !== undefined && !singular) {
    const start = spec.start
    const limit = spec.limit
    checks.push({
      relation: 'startSuffix',
      base: spec,
      variant: withoutStartAndLimit(spec),
      relate: computedRelate((variantRows) =>
        expectedStartSuffix(variantRows, start, limit)
      ),
    })
  }

  return checks
}

// ---------------------------------------------------------------------------
// committed known-gap fixtures (regressions/*.json). a fixture pins ONE
// recorded spec + relation + target + expected outcome so the metamorphic-lane
// --replay path executes EXACTLY that (not the mutable generator) and asserts
// it still holds. parsing is PURE (takes the JSON text, no fs) so it is unit-
// and mutant-testable in metamorphic.selftest.ts. a corrupt/mismatched fixture
// THROWS — replay must fail loud, never silently fall through to regeneration.
// ---------------------------------------------------------------------------

export type KnownGapFixture = {
  id: string
  relation: Relation
  target: string
  spec: GenSpec
  // the outcome the record asserts for this spec+relation (a known-gap records
  // a reproduced 'fail'). replay compares the live outcome to this.
  expectOutcome: RelResult
  // deterministic fingerprint of the fixture-data SEED the record was captured
  // against. replay recomputes it and fails loud on any mismatch, so a SEED edit
  // can never silently reinterpret the repro against different data.
  sourceFingerprint: string
  // the row count the relation's computed window should have (a cross-check that
  // the reproduced result matches what was recorded).
  expectedRowCount?: number
  // the reference query (base minus start/limit) the relation derives. replay
  // asserts the live-derived variant deep-equals this, so a stale record is
  // caught even if the fingerprint somehow matched.
  reference?: GenSpec
}

export function parseFixture(json: string): KnownGapFixture {
  const o = JSON.parse(json) as Record<string, unknown>
  if (o.kind !== 'metamorphic-known-gap') {
    throw new Error(
      `fixture kind must be "metamorphic-known-gap", got ${JSON.stringify(o.kind)}`
    )
  }
  if (typeof o.id !== 'string' || o.id.length === 0) throw new Error('fixture missing id')
  if (!RELATIONS.includes(o.relation as Relation)) {
    throw new Error(
      `fixture relation invalid: ${JSON.stringify(o.relation)} (valid: ${RELATIONS.join(', ')})`
    )
  }
  if (typeof o.target !== 'string' || o.target.length === 0)
    throw new Error('fixture missing target')
  const spec = o.spec as GenSpec | undefined
  if (!spec || typeof spec !== 'object' || typeof spec.table !== 'string') {
    throw new Error('fixture missing/invalid spec (need an object with a string table)')
  }
  if (
    o.expectOutcome !== 'pass' &&
    o.expectOutcome !== 'fail' &&
    o.expectOutcome !== 'skip'
  ) {
    throw new Error(
      `fixture expectOutcome must be pass|fail|skip, got ${JSON.stringify(o.expectOutcome)}`
    )
  }
  if (
    typeof o.sourceFingerprint !== 'string' ||
    !/^[0-9a-f]{64}$/.test(o.sourceFingerprint)
  ) {
    throw new Error(
      'fixture sourceFingerprint must be a lowercase 64-hex SHA-256 (full SEED digest)'
    )
  }
  if (
    o.expectedRowCount !== undefined &&
    (typeof o.expectedRowCount !== 'number' ||
      !Number.isSafeInteger(o.expectedRowCount) ||
      o.expectedRowCount < 0)
  ) {
    throw new Error(
      'fixture expectedRowCount must be a non-negative safe integer when present'
    )
  }
  const reference = o.reference as GenSpec | undefined
  if (
    reference !== undefined &&
    (typeof reference !== 'object' || typeof reference.table !== 'string')
  ) {
    throw new Error(
      'fixture reference must be a spec (object with a string table) when present'
    )
  }
  // the recorded relation must actually apply to the recorded spec, else the
  // fixture is internally inconsistent (a corrupt record).
  const relations = metamorphicChecks(spec).map((c) => c.relation)
  if (!relations.includes(o.relation as Relation)) {
    throw new Error(
      `fixture relation ${o.relation} does not apply to its spec (spec yields: ${relations.join(', ') || 'none'})`
    )
  }
  return {
    id: o.id,
    relation: o.relation as Relation,
    target: o.target,
    spec,
    expectOutcome: o.expectOutcome,
    sourceFingerprint: o.sourceFingerprint,
    expectedRowCount: o.expectedRowCount as number | undefined,
    reference,
  }
}

// map (live outcome, recorded expectation) to a replay verdict + process exit.
// PURE so metamorphic.selftest.ts can mutant-test the decision without a target.
// semantics (per guardrail): a REPRODUCED known product failure must NOT be
// dressed green — expected 'fail' that still fails exits 1 (REPRODUCED). a
// recorded 'pass' that still passes exits 0. any MISMATCH (live != recorded, incl.
// upstream fixing the bug) exits 2 so the record is flagged as stale, never
// silently accepted.
export type ReplayVerdict = {
  exitCode: 0 | 1 | 2
  status: 'confirmed-pass' | 'reproduced' | 'mismatch' | 'inconclusive'
  message: string
}

export function replayVerdict(
  liveOutcome: RelResult,
  expectOutcome: RelResult
): ReplayVerdict {
  if (liveOutcome !== expectOutcome) {
    return {
      exitCode: 2,
      status: 'mismatch',
      message: `MISMATCH: recorded ${expectOutcome} but observed ${liveOutcome} — the record is stale (upstream may be fixed); update the fixture`,
    }
  }
  if (expectOutcome === 'fail') {
    return {
      exitCode: 1,
      status: 'reproduced',
      message:
        'REPRODUCED: the recorded known product failure still holds (non-gating; NOT dressed green)',
    }
  }
  if (expectOutcome === 'pass') {
    return {
      exitCode: 0,
      status: 'confirmed-pass',
      message: 'the recorded pass still holds',
    }
  }
  return {
    exitCode: 2,
    status: 'inconclusive',
    message: `recorded ${expectOutcome} is not assertable as a product behavior`,
  }
}
