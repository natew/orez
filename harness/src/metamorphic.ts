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
