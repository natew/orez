// CHECKER VALIDATION (mutation proof) for spec-shrink.ts + spec-corpus.ts. PURE:
// no target, no @rocicorp/zero boot. Gates PR. Proves the shrinker minimizes
// while strictly reducing complexity and stays IN GRAMMAR (every candidate passes
// the corpus validator), and that the parser rejects every corruption family of
// the frozen v1 generator grammar. See upstream-parity/shrink-corpus-contract.md.
//
//   bun src/spec-shrink.selftest.ts
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { canonical } from './canonical.js'
import {
  assertValidSpec,
  buildReplayCommand,
  currentSeedFingerprint,
  loadCorpus,
  parseCorpusEntry,
} from './spec-corpus.js'
import { constructCount, oneStepShrinks, shrinkSpec } from './spec-shrink.js'

import type { GenSpec, GenWhere } from './fixture.js'

let failed = 0
function assert(cond: boolean, msg: string) {
  if (cond) console.log(`  ok   ${msg}`)
  else {
    console.error(` FAIL  ${msg}`)
    failed++
  }
}
const throwsSpec = (spec: unknown, label: string) => {
  let threw = false
  try {
    assertValidSpec(spec)
  } catch {
    threw = true
  }
  assert(threw, `spec rejected: ${label}`)
}
const okSpec = (spec: unknown, label: string) => {
  let ok = false
  try {
    assertValidSpec(spec)
    ok = true
  } catch (e) {
    console.error('  (' + label + ' threw: ' + e + ')')
  }
  assert(ok, `spec accepted: ${label}`)
}

// a VALID, richly-decorated project spec (many-rel task/member subs, a nested
// one-rel user sub, exists, and/or-3 where, orderBy, limit).
const big: GenSpec = {
  table: 'project',
  where: {
    op: 'and',
    children: [
      { op: 'cmp', col: 'name', cmp: 'LIKE', value: '%x%' },
      { op: 'cmp', col: 'ownerId', cmp: '=', value: 'u1' },
      { op: 'cmp', col: 'name', cmp: 'ILIKE', value: '%a%' },
    ],
  },
  exists: [
    { rel: 'members', where: { op: 'cmp', col: 'userId', cmp: '=', value: 'u2' } },
  ],
  orderBy: [
    ['name', 'desc'],
    ['id', 'asc'],
  ],
  limit: 3,
  related: [
    {
      rel: 'tasks',
      sub: {
        where: { op: 'cmp', col: 'done', cmp: '=', value: true },
        orderBy: [
          ['rank', 'desc'],
          ['id', 'asc'],
        ],
        limit: 2,
      },
    },
    {
      rel: 'members',
      sub: { orderBy: [['id', 'asc']], related: [{ rel: 'user', sub: { one: true } }] },
    },
  ],
}

console.log('[spec] grammar closure — valid shapes accepted')
okSpec(big, 'big (many/one rels, exists, and-3, orderBy, limit)')
okSpec(
  {
    table: 'member',
    orderBy: [['id', 'asc']],
    related: [{ rel: 'user', sub: { one: true } }],
  },
  'one-rel nested {one:true} sub'
)
okSpec(
  {
    table: 'project',
    orderBy: [['id', 'asc']],
    related: [{ rel: 'tasks', sub: { orderBy: [['id', 'asc']] } }],
  },
  'many-rel sub with orderBy'
)
okSpec(
  { table: 'project', orderBy: [['id', 'asc']], related: [{ rel: 'tasks' }] },
  'many-rel with absent sub'
)
okSpec(
  {
    table: 'task',
    orderBy: [
      ['rank', 'asc'],
      ['id', 'asc'],
    ],
    start: { row: { rank: 1, id: 't1' } },
  },
  'task start cursor (rank,id)'
)

console.log('[spec] grammar closure — invalid shapes rejected')
throwsSpec({ table: 'nope', orderBy: [['id', 'asc']] }, 'unknown table')
throwsSpec({ table: 'project' }, 'root missing orderBy')
throwsSpec({ table: 'project', orderBy: [] }, 'root empty orderBy')
throwsSpec(
  {
    table: 'project',
    orderBy: [
      ['id', 'asc'],
      ['id', 'asc'],
    ],
  },
  'root duplicate-id orderBy'
)
throwsSpec({ table: 'project', orderBy: [['id', 'desc']] }, 'root id desc')
throwsSpec(
  {
    table: 'project',
    orderBy: [
      ['name', 'asc'],
      ['ownerId', 'asc'],
      ['id', 'asc'],
    ],
  },
  'orderBy > 1 non-id term'
)
throwsSpec(
  {
    table: 'task',
    orderBy: [
      ['meta', 'asc'],
      ['id', 'asc'],
    ],
  },
  'json column ordering'
)
throwsSpec(
  { table: 'project', orderBy: [['id', 'asc']], one: true, limit: 2 },
  'root one + limit'
)
throwsSpec(
  { table: 'project', orderBy: [['id', 'asc']], one: false },
  'one must be true or absent'
)
throwsSpec({ table: 'project', orderBy: [['id', 'asc']], limit: 99 }, 'root limit > 8')
throwsSpec(
  {
    table: 'project',
    orderBy: [['id', 'asc']],
    where: {
      op: 'and',
      children: [
        { op: 'cmp', col: 'name', cmp: 'LIKE', value: 'a' },
        { op: 'cmp', col: 'name', cmp: 'LIKE', value: 'b' },
        { op: 'cmp', col: 'name', cmp: 'LIKE', value: 'c' },
        { op: 'cmp', col: 'name', cmp: 'LIKE', value: 'd' },
      ],
    },
  },
  'and with 4 children'
)
throwsSpec(
  {
    table: 'member',
    orderBy: [['id', 'asc']],
    where: { op: 'cmp', col: 'id', cmp: 'IN', value: ['a'] },
  },
  'IN length 1'
)
throwsSpec(
  {
    table: 'member',
    orderBy: [['id', 'asc']],
    where: { op: 'cmp', col: 'id', cmp: 'IN', value: ['a', 'b', 'c', 'd', 'e'] },
  },
  'IN length 5'
)
throwsSpec(
  {
    table: 'project',
    orderBy: [['id', 'asc']],
    related: [{ rel: 'tasks', sub: { one: true } }],
  },
  'many-relation sub {one:true}'
)
throwsSpec(
  {
    table: 'member',
    orderBy: [['id', 'asc']],
    related: [{ rel: 'user', sub: { one: true, orderBy: [['id', 'asc']], limit: 1 } }],
  },
  'one-relation sub with where/order/limit'
)
throwsSpec(
  { table: 'member', orderBy: [['id', 'asc']], related: [{ rel: 'user' }] },
  'one-relation missing required {one:true} sub'
)
throwsSpec(
  {
    table: 'project',
    orderBy: [['id', 'asc']],
    related: [{ rel: 'tasks', sub: { limit: 2 } }],
  },
  'many-relation sub missing orderBy'
)
throwsSpec(
  {
    table: 'project',
    orderBy: [['id', 'asc']],
    related: [{ rel: 'members' }, { rel: 'tasks' }, { rel: 'members' }],
  },
  'related > max / duplicate'
)
throwsSpec(
  {
    table: 'project',
    orderBy: [['id', 'asc']],
    exists: [{ rel: 'members' }, { rel: 'tasks' }],
  },
  'exists > 1'
)
throwsSpec(
  {
    table: 'task',
    orderBy: [
      ['dueAt', 'asc'],
      ['id', 'asc'],
    ],
    start: { row: { dueAt: null, id: 't1' } },
  },
  'start on non-(rank) order column'
)
throwsSpec(
  { table: 'project', orderBy: [['id', 'asc']], exists: [] },
  'root exists:[] empty array'
)
throwsSpec(
  { table: 'project', orderBy: [['id', 'asc']], related: [] },
  'root related:[] empty array'
)
throwsSpec(
  {
    table: 'member',
    orderBy: [['id', 'asc']],
    related: [{ rel: 'user', sub: { one: true, related: [] } }],
  },
  'nested related:[] empty array'
)

console.log('[spec-shrink] constructCount + oneStepShrinks invariants + closure')
{
  // where and(3)=4; exists 1+1=2; orderBy non-id 1; limit 1;
  // tasks 1 + (where1 + rank1 + limit1)=3 => 4; members 1 + (user 1+1)=2 => 3. total 4+2+1+1+4+3 = 15
  assert(
    constructCount(big) === 15,
    `constructCount(big) === 15 (got ${constructCount(big)})`
  )
  const cands = oneStepShrinks(big)
  const base = constructCount(big)
  assert(cands.length > 0, 'oneStepShrinks produces candidates')
  assert(
    cands.every((c) => constructCount(c) < base),
    'every candidate is strictly smaller'
  )
  const keys = cands.map(canonical)
  assert(new Set(keys).size === keys.length, 'candidates are canonical-deduped')
  assert(
    canonical(keys) === canonical(oneStepShrinks(big).map(canonical)),
    'oneStepShrinks is deterministic'
  )
  // EXECUTABLE closure: every shrink candidate is itself valid grammar
  let allValid = true
  for (const c of cands) {
    try {
      assertValidSpec(c)
    } catch (e) {
      allValid = false
      console.error(
        '  (out-of-grammar candidate: ' + JSON.stringify(c) + ' -> ' + e + ')'
      )
      break
    }
  }
  assert(
    allValid,
    'every oneStepShrinks(big) candidate passes the corpus grammar validator'
  )
}
{
  const withStart: GenSpec = {
    table: 'task',
    orderBy: [
      ['rank', 'desc'],
      ['id', 'asc'],
    ],
    start: { row: { rank: 1, id: 't1' } },
  }
  const cands = oneStepShrinks(withStart)
  assert(
    cands.every(
      (c) => c.start === undefined || (c.orderBy ?? []).some(([col]) => col === 'rank')
    ),
    'order term is not removed while a start is present'
  )
  assert(
    cands.some((c) => c.start === undefined),
    'start itself is a shrink candidate'
  )
  assert(
    cands.every((c) => {
      try {
        assertValidSpec(c)
        return true
      } catch {
        return false
      }
    }),
    'start-spec shrink candidates stay in grammar'
  )
}

console.log('[spec-shrink] shrinkSpec — minimizes to a fixpoint, honest budget')
{
  const hasOwner = (w: GenWhere | undefined): boolean =>
    !w ? false : w.op === 'cmp' ? w.col === 'ownerId' : w.children.some(hasOwner)
  const stillDiverges = async (s: GenSpec) => hasOwner(s.where)
  const r = await shrinkSpec(big, stillDiverges, 1000)
  assert(r.complete, 'shrinkSpec reaches a fixpoint (complete)')
  assert(
    hasOwner(r.spec.where),
    'shrunk spec still contains the divergence cause (ownerId leaf)'
  )
  assert(
    constructCount(r.spec) === 1,
    `shrunk to the minimal ownerId-only spec (cc=${constructCount(r.spec)})`
  )
  okSpec(r.spec, 'shrunk minimal spec is valid grammar')
  const tiny = await shrinkSpec(big, stillDiverges, 2)
  assert(
    !tiny.complete,
    'budget exhaustion returns complete:false (not a claimed minimum)'
  )
  for (const bad of [-1, 1.5, -0, Number.NaN, Infinity, 2 ** 53]) {
    let threw = false
    try {
      await shrinkSpec(big, stillDiverges, bad)
    } catch {
      threw = true
    }
    assert(threw, `shrinkSpec rejects invalid budget ${String(bad)}`)
  }
}

console.log('[spec-shrink] shrinkSpec — minimizes a nested exists.where cause')
{
  // divergence needs the exists relationship PLUS the userId leaf inside a 3-way
  // AND. Dropping the whole exists loses reproduction, so the shrinker MUST keep
  // the exists while reducing its where to the necessary leaf.
  const existsSpec: GenSpec = {
    table: 'project',
    orderBy: [['id', 'asc']],
    exists: [
      {
        rel: 'members',
        where: {
          op: 'and',
          children: [
            { op: 'cmp', col: 'userId', cmp: '=', value: 'u1' },
            { op: 'cmp', col: 'projectId', cmp: '!=', value: 'p9' },
            { op: 'cmp', col: 'id', cmp: '=', value: 'm1' },
          ],
        },
      },
    ],
  }
  const hasUserId = (w: GenWhere | undefined): boolean =>
    !w ? false : w.op === 'cmp' ? w.col === 'userId' : w.children.some(hasUserId)
  const needsExistsUserId = async (s: GenSpec) => {
    const e = (s.exists ?? [])[0]
    return !!e && hasUserId(e.where)
  }
  const r = await shrinkSpec(existsSpec, needsExistsUserId, 1000)
  assert(r.complete, 'exists.where shrink reaches a fixpoint')
  assert((r.spec.exists ?? []).length === 1, 'exists entry is RETAINED (not dropped)')
  const e = r.spec.exists![0]!
  assert(
    !!e.where && e.where.op === 'cmp' && e.where.col === 'userId',
    'exists.where minimized to the necessary userId leaf'
  )
  assert(
    constructCount(r.spec) === 2,
    `minimal = exists(1) + 1 leaf = 2 (got ${constructCount(r.spec)})`
  )
  okSpec(r.spec, 'minimized exists spec is valid grammar')
}

// ---------------------------------------------------------------------------
console.log('[spec-corpus] parseCorpusEntry — well-formed accepts')
const HEX64 = currentSeedFingerprint()
const HEX64B = 'a'.repeat(64)
const specA: GenSpec = {
  table: 'task',
  where: { op: 'cmp', col: 'done', cmp: '=', value: false },
  orderBy: [['id', 'asc']],
}

function exactEntry(): Record<string, unknown> {
  const id = 'sweep-hydrate-r0-q0-abc'
  const e: Record<string, unknown> = {
    schemaVersion: 1,
    kind: 'sweep-divergence',
    id,
    note: 'hydrate round-0 cross-target',
    phase: 'hydrate',
    comparisonKind: 'cross-target',
    round: 0,
    specIndex: 0,
    rounds: 10,
    queriesPerRound: 4,
    exactReplayable: true,
    minimizationComplete: true,
    spec: specA,
    against: 'orez-local',
    observedTarget: 'orez-local',
    seed: 42,
    sourceFingerprint: HEX64,
    constructCount: constructCount(specA),
    leftHash: HEX64,
    rightHash: HEX64B,
    leftPreview: '[]',
    rightPreview: '[{"id":"t1"}]',
    expectConverge: true,
  }
  e.replay = buildReplayCommand({
    exactReplayable: true,
    id,
    against: 'orez-local',
    seed: 42,
    rounds: 10,
    queriesPerRound: 4,
  })
  return e
}
function nonexactEntry(): Record<string, unknown> {
  const id = 'sweep-incremental-r10-q2-def'
  const e: Record<string, unknown> = {
    schemaVersion: 1,
    kind: 'sweep-divergence',
    id,
    note: 'incremental single-target (nonexact)',
    phase: 'incremental',
    comparisonKind: 'single-target',
    round: 10,
    specIndex: 2,
    rounds: 10,
    queriesPerRound: 4,
    exactReplayable: false,
    minimizationComplete: false,
    spec: specA,
    against: 'orez-local',
    observedTarget: 'stock-zero',
    seed: 7,
    sourceFingerprint: HEX64,
    constructCount: constructCount(specA),
    leftHash: HEX64,
    rightHash: HEX64B,
    leftPreview: '[]',
    rightPreview: '[]',
    expectConverge: true,
  }
  e.replay = buildReplayCommand({
    exactReplayable: false,
    id,
    against: 'orez-local',
    seed: 7,
    rounds: 10,
    queriesPerRound: 4,
  })
  return e
}
{
  let a = false
  try {
    const p = parseCorpusEntry(JSON.stringify(exactEntry()))
    a = p.exactReplayable && p.constructCount === 1
  } catch (e) {
    console.error('  (exact parse threw: ' + e + ')')
  }
  assert(a, 'well-formed EXACT entry parses')
  let b = false
  try {
    parseCorpusEntry(JSON.stringify(nonexactEntry()))
    b = true
  } catch (e) {
    console.error('  (nonexact parse threw: ' + e + ')')
  }
  assert(b, 'well-formed NONEXACT (incremental single-target) entry parses')
}

console.log('[spec-corpus] parseCorpusEntry — every corruption family THROWS')
const NUL = String.fromCharCode(0)
const rejects = (
  base: () => Record<string, unknown>,
  mut: (o: Record<string, unknown>) => void,
  label: string
) => {
  const o = JSON.parse(JSON.stringify(base()))
  mut(o)
  let threw = false
  try {
    parseCorpusEntry(JSON.stringify(o))
  } catch {
    threw = true
  }
  assert(threw, `rejects: ${label}`)
}
const rx = (mut: (o: Record<string, unknown>) => void, label: string) =>
  rejects(exactEntry, mut, label)

rx((o) => (o.schemaVersion = 2), 'wrong schemaVersion')
rx((o) => (o.kind = 'other'), 'wrong kind')
rx((o) => (o.extra = 1), 'unknown top-level key')
rx((o) => delete o.id, 'missing id')
rx((o) => (o.id = 'bad id!'), 'unsafe id token')
rx((o) => (o.note = `x${NUL}y`), 'NUL in note')
rx((o) => (o.phase = 'bogus'), 'bad phase')
rx((o) => (o.comparisonKind = 'bogus'), 'bad comparisonKind')
rx((o) => (o.exactReplayable = 'yes'), 'non-boolean flag')
rx((o) => (o.sourceFingerprint = 'abc'), 'short fingerprint')
rx((o) => (o.leftHash = HEX64.toUpperCase()), 'uppercase leftHash')
rx((o) => (o.rightHash = HEX64), 'leftHash === rightHash (not a divergence)')
rx((o) => (o.round = 1.5), 'non-integer round')
rx((o) => (o.constructCount = 999), 'constructCount != constructCount(spec)')
rx((o) => (o.rounds = 0), 'rounds must be > 0')
rx(
  (o) => (o.rounds = Number.MAX_SAFE_INTEGER),
  'round0 overflow (rounds*queriesPerRound unsafe)'
)
rx((o) => (o.specIndex = 4), 'specIndex out of phase-sensitive range (round 0 < qpr)')
rx((o) => (o.expectConverge = false), 'expectConverge not true')
rx((o) => delete o.replay, 'replay REQUIRED')
rx(
  (o) => (o.replay = 'bun src/sweep.ts --replay-corpus wrong.json --against orez-local'),
  'stale replay command'
)
rx((o) => (o.fullSidecar = '../escape.json'), 'unsafe fullSidecar (traversal)')
rx((o) => (o.fullSidecar = '/etc/x'), 'absolute fullSidecar')
rx((o) => (o.against = 'stock-zero'), 'against cannot be stock-zero')
rx((o) => (o.against = 'not-a-target'), 'unknown against target')
rx((o) => (o.observedTarget = 'orez-cf'), 'cross-target observedTarget != against')
rx((o) => (o.round = 1), 'hydrate round!=0 contradicts exactReplayable')
rx((o) => (o.exactReplayable = false), 'exactReplayable false but hydrate+round0+cross')
rejects(
  nonexactEntry,
  (o) => (o.minimizationComplete = true),
  'nonexact with minimizationComplete=true'
)
rejects(
  nonexactEntry,
  (o) => (o.comparisonKind = 'cross-target'),
  'incremental not single-target'
)
// fingerprint comparison (only when expected passed)
{
  const o = exactEntry()
  o.sourceFingerprint = HEX64B
  let threw = false
  try {
    parseCorpusEntry(JSON.stringify(o), { expectedFingerprint: HEX64 })
  } catch {
    threw = true
  }
  assert(threw, 'rejects: sourceFingerprint != current fixture digest (when compared)')
}
// -0 must be injected via raw JSON text (JSON.stringify(-0) serializes to "0").
{
  const raw = JSON.stringify(exactEntry()).replace('"seed":42', '"seed":-0')
  let threw = false
  try {
    parseCorpusEntry(raw)
  } catch {
    threw = true
  }
  assert(threw, 'rejects: negative-zero seed (raw -0 in JSON text)')
}

// ---------------------------------------------------------------------------
console.log(
  '[spec-corpus] loadCorpus — missing dir, sorted, basename==id, duplicate, corrupt'
)
// a valid exact entry with a chosen id (replay derives from the id).
function entryWithId(id: string): Record<string, unknown> {
  const e = exactEntry()
  e.id = id
  e.replay = buildReplayCommand({
    exactReplayable: true,
    id,
    against: 'orez-local',
    seed: 42,
    rounds: 10,
    queriesPerRound: 4,
  })
  return e
}
const load = (dir: string): { ok: boolean; err?: unknown; out?: { id: string }[] } => {
  try {
    return { ok: true, out: loadCorpus(dir) }
  } catch (err) {
    return { ok: false, err }
  }
}
{
  const missing = join(tmpdir(), `no-such-corpus-${'x'.repeat(6)}`)
  assert(
    loadCorpus(missing).length === 0,
    'missing corpus dir returns [] (infrastructure-only)'
  )

  // sorted order across two DIFFERENT valid canonical files
  const d1 = mkdtempSync(join(tmpdir(), 'corpus-sorted-'))
  try {
    writeFileSync(join(d1, 'sweep-beta.json'), JSON.stringify(entryWithId('sweep-beta')))
    writeFileSync(
      join(d1, 'sweep-alpha.json'),
      JSON.stringify(entryWithId('sweep-alpha'))
    )
    const r = load(d1)
    assert(
      r.ok &&
        canonical(r.out!.map((e) => e.id)) === canonical(['sweep-alpha', 'sweep-beta']),
      'loadCorpus returns entries in sorted filename order'
    )
  } finally {
    rmSync(d1, { recursive: true, force: true })
  }

  // basename != id (single file, no duplicate) reaches the basename diagnostic
  const d2 = mkdtempSync(join(tmpdir(), 'corpus-basename-'))
  try {
    writeFileSync(join(d2, 'renamed.json'), JSON.stringify(entryWithId('sweep-x')))
    assert(!load(d2).ok, 'loadCorpus rejects a filename basename != id')
  } finally {
    rmSync(d2, { recursive: true, force: true })
  }

  // two DIFFERENTLY-named files with the SAME id reach the duplicate diagnostic
  const d3 = mkdtempSync(join(tmpdir(), 'corpus-dup-'))
  try {
    writeFileSync(join(d3, 'dup-a.json'), JSON.stringify(entryWithId('sweep-dup')))
    writeFileSync(join(d3, 'dup-b.json'), JSON.stringify(entryWithId('sweep-dup')))
    const r = load(d3)
    assert(
      !r.ok && String((r.err as Error).message).includes('duplicate corpus id'),
      'loadCorpus rejects duplicate ids (reaches the duplicate diagnostic)'
    )
  } finally {
    rmSync(d3, { recursive: true, force: true })
  }

  // a well-named entry loads (fingerprint matches); a corrupt file fails loud
  const d4 = mkdtempSync(join(tmpdir(), 'corpus-ok-'))
  try {
    const e = entryWithId('sweep-ok')
    writeFileSync(join(d4, 'sweep-ok.json'), JSON.stringify(e))
    assert(
      loadCorpus(d4).length === 1,
      'loadCorpus loads a well-named entry (fingerprint matches)'
    )
    writeFileSync(join(d4, 'corrupt.json'), '{ not json')
    assert(!load(d4).ok, 'loadCorpus fails loud on a corrupt file')
  } finally {
    rmSync(d4, { recursive: true, force: true })
  }
}

// ---------------------------------------------------------------------------
console.log('')
if (failed) {
  console.error(`[spec-shrink.selftest] FAIL — ${failed} assertion(s) failed.`)
  process.exit(1)
}
console.log(
  '[spec-shrink.selftest] PASS — shrinker minimizes + stays in grammar; parser rejects every corruption family.'
)
process.exit(0)
