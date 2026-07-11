// CHECKER VALIDATION (mutation proof) for metamorphic.ts. this is NOT a
// product/reference conformance test — it never boots a target. it feeds the
// pure relation logic the result a CORRECT engine and a BUGGY engine would
// each produce, and asserts the checker passes the first and FAILS the second.
// its job is to prove the guard is non-vacuous: it actually catches the
// NULL-cursor bug class (#6121) and does not rubber-stamp.
//
// It is deterministic and dependency-free, so it is safe to gate in CI. Keep it
// distinct from metamorphic-lane.ts, which runs the same relations against a
// real target and may legitimately go red on the 1.7.0 pin.
//
//   bun src/metamorphic.selftest.ts
import { metamorphicChecks, type Relation } from './metamorphic.js'

import type { GenSpec, GenWhere } from './fixture.js'

let failed = 0
function assert(cond: boolean, msg: string) {
  if (cond) {
    console.log(`  ok   ${msg}`)
  } else {
    console.error(` FAIL  ${msg}`)
    failed++
  }
}

// pick one named check out of metamorphicChecks(spec)
function checkFor(spec: GenSpec, relation: Relation) {
  const c = metamorphicChecks(spec).find((x) => x.relation === relation)
  if (!c) throw new Error(`expected a ${relation} check for spec ${JSON.stringify(spec)}`)
  return c
}
const relationsOf = (spec: GenSpec) =>
  metamorphicChecks(spec)
    .map((c) => c.relation)
    .sort()

// the shared reference result: a task table ordered by (dueAt asc, id asc).
// SQLite/zql sort NULLs FIRST ascending, so the NULL-dueAt rows are the head of
// the walk — the region #6121 got wrong.
const REF = [
  { id: 't1', dueAt: null, title: 'a' },
  { id: 't2', dueAt: null, title: 'b' },
  { id: 't3', dueAt: 100, title: 'c' },
  { id: 't4', dueAt: 200, title: 'd' },
  { id: 't5', dueAt: 300, title: 'e' },
]
const dueOrder: GenSpec['orderBy'] = [
  ['dueAt', 'asc'],
  ['id', 'asc'],
]

console.log('[selftest] startSuffix — the NULL-cursor blind-spot catcher')
{
  // exclusive start anchored ON a NULL-sorted row: must continue through the
  // rest of the NULL group into the non-NULL rows.
  const spec: GenSpec = {
    table: 'task',
    orderBy: dueOrder,
    start: { row: { dueAt: null, id: 't1' } },
  }
  const c = checkFor(spec, 'startSuffix')
  const correct = REF.slice(1) // [t2,t3,t4,t5]
  assert(
    c.relate(correct, REF).result === 'pass',
    'correct engine → pass (continues past null anchor)'
  )
  // #6121: `col > NULL` matched nothing, so the continuation came back EMPTY.
  assert(c.relate([], REF).result === 'fail', 'MUTANT #6121 empty-continuation → fail')
  // off-by-one: skipped one row too many.
  assert(
    c.relate(REF.slice(2), REF).result === 'fail',
    'MUTANT off-by-one (skips into non-null) → fail'
  )
  // dropped just the null group: returns only the non-null tail.
  assert(
    c.relate([REF[2], REF[3], REF[4]], REF).result === 'fail',
    'MUTANT dropped-null-group → fail'
  )
}
{
  // inclusive start anchored on a NULL-sorted row: must INCLUDE the anchor.
  const spec: GenSpec = {
    table: 'task',
    orderBy: dueOrder,
    start: { row: { dueAt: null, id: 't2' }, inclusive: true },
  }
  const c = checkFor(spec, 'startSuffix')
  assert(
    c.relate(REF.slice(1), REF).result === 'pass',
    'inclusive correct → pass (anchor included)'
  )
  assert(
    c.relate(REF.slice(2), REF).result === 'fail',
    'MUTANT inclusive-treated-as-exclusive → fail'
  )
}
{
  // start + limit: suffix then take n.
  const spec: GenSpec = {
    table: 'task',
    orderBy: dueOrder,
    start: { row: { dueAt: null, id: 't1' } },
    limit: 2,
  }
  const c = checkFor(spec, 'startSuffix')
  assert(c.relate([REF[1], REF[2]], REF).result === 'pass', 'start+limit correct → pass')
  assert(
    c.relate([REF[1], REF[2], REF[3]], REF).result === 'fail',
    'MUTANT start+limit over-take → fail'
  )
}
{
  // dead cursor (not in the reference) → SKIP, never a false positive.
  const spec: GenSpec = {
    table: 'task',
    orderBy: dueOrder,
    start: { row: { dueAt: 999, id: 'gone' } },
  }
  const c = checkFor(spec, 'startSuffix')
  assert(c.relate([], REF).result === 'skip', 'dead cursor → skip (soundly declines)')
}

console.log('[selftest] limitPrefix — window truncation')
{
  const spec: GenSpec = {
    table: 'task',
    orderBy: [
      ['rank', 'asc'],
      ['id', 'asc'],
    ],
    limit: 3,
  }
  const c = checkFor(spec, 'limitPrefix')
  assert(c.relate(REF.slice(0, 3), REF).result === 'pass', 'first n rows → pass')
  assert(
    c.relate([REF[0], REF[1], REF[3]], REF).result === 'fail',
    'MUTANT wrong-row-in-window → fail'
  )
  assert(
    c.relate(REF.slice(0, 4), REF).result === 'fail',
    'MUTANT limit-not-applied → fail'
  )
}

console.log(
  '[selftest] equal-invariant relations (redundantConjunct / andReorder / largeLimit)'
)
{
  const andWhere: GenWhere = {
    op: 'and',
    children: [
      { op: 'cmp', col: 'done', cmp: '=', value: false },
      { op: 'cmp', col: 'rank', cmp: '>', value: 5 },
    ],
  }
  const spec: GenSpec = { table: 'task', where: andWhere, orderBy: [['id', 'asc']] }
  for (const rel of ['redundantConjunct', 'andReorder', 'largeLimit'] as const) {
    const c = checkFor(spec, rel)
    assert(c.relate(REF, REF).result === 'pass', `${rel}: identical result → pass`)
    assert(
      c.relate(REF, REF.slice(1)).result === 'fail',
      `${rel}: MUTANT transform changed result → fail`
    )
  }
}

console.log('[selftest] check generation is non-vacuous (right relations, right guards)')
{
  const startSpec: GenSpec = {
    table: 'task',
    orderBy: dueOrder,
    start: { row: { dueAt: null, id: 't1' } },
  }
  assert(
    relationsOf(startSpec).includes('startSuffix'),
    'start spec yields a startSuffix check'
  )

  const limitSpec: GenSpec = { table: 'task', orderBy: [['id', 'asc']], limit: 4 }
  const limitRels = relationsOf(limitSpec)
  assert(limitRels.includes('limitPrefix'), 'limit spec yields a limitPrefix check')
  assert(
    !limitRels.includes('largeLimit'),
    'limit spec does NOT get largeLimit (a limit is already present)'
  )

  const andSpec: GenSpec = {
    table: 'task',
    where: {
      op: 'and',
      children: [
        { op: 'cmp', col: 'done', cmp: '=', value: true },
        { op: 'cmp', col: 'rank', cmp: '<', value: 9 },
      ],
    },
    orderBy: [['id', 'asc']],
  }
  assert(
    relationsOf(andSpec).includes('andReorder'),
    'and-where spec yields an andReorder check'
  )

  // singular (one()) specs never get sliceable-array relations.
  const oneStart: GenSpec = {
    table: 'task',
    orderBy: dueOrder,
    start: { row: { dueAt: null, id: 't1' } },
    one: true,
  }
  const oneRels = relationsOf(oneStart)
  assert(
    !oneRels.includes('startSuffix'),
    'one() spec does NOT get startSuffix (result is not an array)'
  )
  assert(
    oneRels.length > 0,
    'one() spec still gets at least redundantConjunct (never zero checks)'
  )

  // a bare spec still gets redundantConjunct + largeLimit — never zero checks.
  const bare: GenSpec = { table: 'project', orderBy: [['id', 'asc']] }
  assert(
    metamorphicChecks(bare).length >= 2,
    'bare spec still produces checks (redundantConjunct + largeLimit)'
  )
}

console.log('')
if (failed) {
  console.error(
    `[selftest] FAIL — ${failed} assertion(s) failed. The metamorphic checker is broken or vacuous.`
  )
  process.exit(1)
}
console.log(
  '[selftest] PASS — every mutant is caught; every correct engine passes; check generation is non-vacuous.'
)
process.exit(0)
