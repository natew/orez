// PURE parser + fail-loud loader for the sweep query differential corpus, ported
// black-box from rocicorp/mono chinook/fuzz/regressions.ts (audited at
// origin/main 7139287da3). One committed entry is a minimized query spec that
// PREVIOUSLY diverged and (after a fix) must now converge — a permanent guard.
//
// Validation is FULLY RECURSIVE and pinned to the EXACT v1 sweep generator
// grammar (column-kind ops/values, relationship CARDINALITY, orderBy/where/
// exists/related/limit/one closure bounds, the (rank,id) start-cursor shape, no
// unknown keys, safe id/target/path tokens). Every corruption THROWS (unlike
// upstream loadRegressions which skips — matches "corrupt repros must fail").
// The loader compares the recorded SEED fingerprint against the CURRENT fixture
// digest so a fixture-data change fails loud. Mutant-tested in
// spec-shrink.selftest.ts. See upstream-parity/shrink-corpus-contract.md.
import { createHash } from 'node:crypto'
import { existsSync, readdirSync, readFileSync } from 'node:fs'
import { basename, isAbsolute, join } from 'node:path'

import { canonical } from './canonical.js'
import { SEED } from './fixture-data.js'
import { type Card, COLUMN_KIND, RELATIONSHIPS } from './fixture-graph.js'
import { constructCount } from './spec-shrink.js'

import type { GenSpec, GenWhere } from './fixture.js'

// generator closure (sweep.ts): root limit<=8, sub limit<=4; root related<=2,
// nested<=1; related nesting depth<=2; root where depth<=2, sub/exists<=1;
// exists<=1; and/or children 2..3; IN 2..4; orderBy exact shape (<=1 non-id,
// json not orderable); root `one` XOR `limit`.
const ROOT_LIMIT_MAX = 8
const SUB_LIMIT_MAX = 4
const AND_OR_MAX = 3
const AGAINST_TARGETS = ['orez-local', 'orez-cf', 'rust-local', 'rust-cf'] as const
const OBSERVED_TARGETS = ['stock-zero', ...AGAINST_TARGETS] as const
const SAFE_TOKEN = /^[A-Za-z0-9][A-Za-z0-9._-]*$/

export type Phase = 'hydrate' | 'post-writes' | 'incremental'
export type ComparisonKind = 'cross-target' | 'single-target'

export type SweepDivergence = {
  schemaVersion: 1
  kind: 'sweep-divergence'
  id: string
  note: string
  phase: Phase
  comparisonKind: ComparisonKind
  round: number
  specIndex: number
  rounds: number
  queriesPerRound: number
  exactReplayable: boolean
  minimizationComplete: boolean
  spec: GenSpec
  against: string
  observedTarget: string
  seed: number
  sourceFingerprint: string
  constructCount: number
  originalConstructCount?: number
  leftHash: string
  rightHash: string
  leftPreview: string
  rightPreview: string
  fullSidecar?: string
  expectConverge: true
  replay: string
}

const ENTRY_KEYS = [
  'schemaVersion',
  'kind',
  'id',
  'note',
  'phase',
  'comparisonKind',
  'round',
  'specIndex',
  'rounds',
  'queriesPerRound',
  'exactReplayable',
  'minimizationComplete',
  'spec',
  'against',
  'observedTarget',
  'seed',
  'sourceFingerprint',
  'constructCount',
  'originalConstructCount',
  'leftHash',
  'rightHash',
  'leftPreview',
  'rightPreview',
  'fullSidecar',
  'expectConverge',
  'replay',
] as const

// deterministic full SHA-256 of the fixed fixture-data SEED (matches
// metamorphic-lane.seedFingerprint). the loader compares a recorded fingerprint
// against this so a SEED edit fails loud instead of silently reinterpreting.
export function currentSeedFingerprint(): string {
  return createHash('sha256').update(canonical(SEED)).digest('hex')
}

// the ONE canonical replay command builder — shared by writer, parser, selftest.
export function buildReplayCommand(e: {
  exactReplayable: boolean
  id: string
  against: string
  seed: number
  rounds: number
  queriesPerRound: number
}): string {
  return e.exactReplayable
    ? `bun src/sweep.ts --replay-corpus regressions/sweep/v1/${e.id}.json --against ${e.against}`
    : `bun src/sweep.ts --seed ${e.seed} --against ${e.against} --rounds ${e.rounds} --queriesPerRound ${e.queriesPerRound}`
}

// ---------------------------------------------------------------------------
// validation helpers
// ---------------------------------------------------------------------------
function fail(msg: string): never {
  throw new Error(`corpus entry invalid: ${msg}`)
}
const isHex64 = (v: unknown): v is string =>
  typeof v === 'string' && /^[0-9a-f]{64}$/.test(v)
const NUL = String.fromCharCode(0)
const hasNul = (s: string) => s.includes(NUL)
const isNonEmptyStr = (v: unknown): v is string =>
  typeof v === 'string' && v.length > 0 && !hasNul(v)
const isStr = (v: unknown): v is string => typeof v === 'string' && !hasNul(v)
const isSafeNonNegInt = (v: unknown): v is number =>
  typeof v === 'number' && Number.isSafeInteger(v) && v >= 0 && !Object.is(v, -0)
const isFiniteNum = (v: unknown): v is number =>
  typeof v === 'number' && Number.isFinite(v)
const isPlainObject = (v: unknown): v is Record<string, unknown> =>
  !!v && typeof v === 'object' && !Array.isArray(v)

function onlyKeys(o: Record<string, unknown>, allowed: readonly string[], path: string) {
  const set = new Set(allowed)
  for (const k of Object.keys(o))
    if (!set.has(k)) fail(`${path}: unknown key ${JSON.stringify(k)}`)
}

function validateCmp(
  table: string,
  col: unknown,
  cmp: unknown,
  value: unknown,
  path: string
) {
  if (typeof col !== 'string') fail(`${path}: col must be a string`)
  const kind = COLUMN_KIND[table]?.[col]
  if (!kind) fail(`${path}: unknown column ${JSON.stringify(col)} on ${table}`)
  if (typeof cmp !== 'string') fail(`${path}: cmp must be a string`)
  const bad = () =>
    fail(`${path}: op ${JSON.stringify(cmp)} / value invalid for ${col} (${kind})`)
  switch (kind) {
    case 'id':
      if (cmp === 'IN') {
        if (
          !Array.isArray(value) ||
          value.length < 2 ||
          value.length > 4 ||
          !value.every(isStr)
        )
          bad()
      } else if (cmp === '=' || cmp === '!=') {
        if (!isStr(value)) bad()
      } else bad()
      return
    case 'string':
      if (!['LIKE', 'ILIKE', '!='].includes(cmp) || !isStr(value)) bad()
      return
    case 'number':
      if (!['>', '<', '>=', '<='].includes(cmp) || !isFiniteNum(value)) bad()
      return
    case 'boolean':
      if (!['=', '!='].includes(cmp) || typeof value !== 'boolean') bad()
      return
    case 'nullableNumber':
      if (cmp === 'IS' || cmp === 'IS NOT') {
        if (value !== null) bad()
      } else if (cmp === '<' || cmp === '>') {
        if (!isFiniteNum(value)) bad()
      } else bad()
      return
    case 'json':
      if (!(cmp === 'IS' || cmp === 'IS NOT') || value !== null) bad()
      return
  }
}

function validateWhere(
  w: unknown,
  table: string,
  path: string,
  maxDepth: number
): asserts w is GenWhere {
  if (!isPlainObject(w)) fail(`${path}: where must be an object`)
  if (w.op === 'cmp') {
    onlyKeys(w, ['op', 'col', 'cmp', 'value'], path)
    validateCmp(table, w.col, w.cmp, w.value, path)
    return
  }
  if (w.op === 'and' || w.op === 'or') {
    onlyKeys(w, ['op', 'children'], path)
    if (maxDepth < 1) fail(`${path}: where nesting exceeds generator depth bound`)
    if (
      !Array.isArray(w.children) ||
      w.children.length < 2 ||
      w.children.length > AND_OR_MAX
    ) {
      fail(`${path}: ${w.op} needs 2..${AND_OR_MAX} children`)
    }
    w.children.forEach((c, i) =>
      validateWhere(c, table, `${path}.${w.op}[${i}]`, maxDepth - 1)
    )
    return
  }
  fail(`${path}: unknown where op ${JSON.stringify(w.op)}`)
}

// exact generator orderBy shape: [['id','asc']] OR [[nonId,'asc'|'desc'],['id','asc']].
function validateOrderByShape(ob: unknown, table: string, path: string) {
  if (!Array.isArray(ob) || ob.length === 0)
    fail(`${path}.orderBy is required and non-empty`)
  if (ob.length > 2)
    fail(`${path}.orderBy has ${ob.length} terms (generator emits 1 or 2)`)
  for (const [i, term] of ob.entries()) {
    if (!Array.isArray(term) || term.length !== 2)
      fail(`${path}.orderBy[${i}] must be [col, dir]`)
  }
  const last = ob[ob.length - 1]
  if (last[0] !== 'id' || last[1] !== 'asc')
    fail(`${path}.orderBy must end with ['id','asc']`)
  if (ob.length === 2) {
    const [col, dir] = ob[0]
    if (col === 'id') fail(`${path}.orderBy first term cannot be id`)
    const kind = COLUMN_KIND[table]?.[col as string]
    if (!kind) fail(`${path}.orderBy: unknown column ${JSON.stringify(col)} on ${table}`)
    if (kind === 'json') fail(`${path}.orderBy: json column ${col} is not orderable`)
    if (dir !== 'asc' && dir !== 'desc') fail(`${path}.orderBy dir must be asc|desc`)
  }
}

function validateRelatedList(
  related: unknown,
  table: string,
  path: string,
  nestLevel: number
) {
  if (!Array.isArray(related)) fail(`${path}.related must be an array`)
  if (related.length === 0) fail(`${path}.related must be absent, not an empty array`)
  const max = nestLevel === 0 ? 2 : 1
  if (related.length > max)
    fail(`${path}.related has ${related.length} entries (max ${max})`)
  if (nestLevel >= 2 && related.length > 0)
    fail(`${path}.related exceeds nesting depth 2`)
  const seen = new Set<string>()
  for (const [i, r] of related.entries()) {
    if (!isPlainObject(r)) fail(`${path}.related[${i}] must be an object`)
    onlyKeys(r, ['rel', 'sub'], `${path}.related[${i}]`)
    if (typeof r.rel !== 'string') fail(`${path}.related[${i}].rel must be a string`)
    const info = RELATIONSHIPS[table]?.[r.rel]
    if (!info)
      fail(
        `${path}.related[${i}]: unknown relationship ${JSON.stringify(r.rel)} on ${table}`
      )
    if (seen.has(r.rel))
      fail(`${path}.related: duplicate relationship ${JSON.stringify(r.rel)}`)
    seen.add(r.rel)
    validateRelatedSub(
      r.sub,
      info.card,
      info.child,
      `${path}.related[${i}].sub`,
      nestLevel + 1
    )
  }
}

// cardinality-exact sub grammar: a ONE-relation sub is {one:true, related?}; a
// MANY-relation sub is absent OR {orderBy(required), where?, limit?, related?}.
function validateRelatedSub(
  sub: unknown,
  card: Card,
  childTable: string,
  path: string,
  nestLevel: number
) {
  if (card === 'one') {
    if (!isPlainObject(sub)) fail(`${path}: a one-relation requires {one:true}`)
    onlyKeys(sub, ['one', 'related'], path)
    if (sub.one !== true) fail(`${path}.one must be true for a one-relation`)
    if (sub.related !== undefined)
      validateRelatedList(sub.related, childTable, path, nestLevel)
    return
  }
  if (sub === undefined) return // many-relation sub may be absent
  if (!isPlainObject(sub)) fail(`${path} must be an object`)
  onlyKeys(sub, ['where', 'orderBy', 'limit', 'related'], path)
  if (sub.one !== undefined) fail(`${path}: a many-relation sub cannot set one`)
  validateOrderByShape(sub.orderBy, childTable, path)
  if (sub.where !== undefined) validateWhere(sub.where, childTable, `${path}.where`, 1)
  if (
    sub.limit !== undefined &&
    (!isSafeNonNegInt(sub.limit) || sub.limit < 1 || sub.limit > SUB_LIMIT_MAX)
  ) {
    fail(`${path}.limit must be an int in 1..${SUB_LIMIT_MAX}`)
  }
  if (sub.related !== undefined)
    validateRelatedList(sub.related, childTable, path, nestLevel)
}

export function assertValidSpec(spec: unknown): asserts spec is GenSpec {
  if (!isPlainObject(spec)) fail('spec must be an object')
  onlyKeys(
    spec,
    ['table', 'where', 'orderBy', 'limit', 'one', 'related', 'exists', 'start'],
    'spec'
  )
  if (typeof spec.table !== 'string' || !COLUMN_KIND[spec.table])
    fail(`unknown table ${JSON.stringify(spec.table)}`)
  const table = spec.table
  validateOrderByShape(spec.orderBy, table, 'spec') // root orderBy is ALWAYS emitted
  if (spec.where !== undefined) validateWhere(spec.where, table, 'spec.where', 2)
  if (
    spec.limit !== undefined &&
    (!isSafeNonNegInt(spec.limit) || spec.limit < 1 || spec.limit > ROOT_LIMIT_MAX)
  ) {
    fail(`spec.limit must be an int in 1..${ROOT_LIMIT_MAX}`)
  }
  if (spec.one !== undefined) {
    if (spec.one !== true) fail('spec.one must be true or absent')
    if (spec.limit !== undefined) fail('spec.one and spec.limit are mutually exclusive')
  }
  if (spec.related !== undefined) validateRelatedList(spec.related, table, 'spec', 0)
  if (spec.exists !== undefined) {
    if (!Array.isArray(spec.exists)) fail('spec.exists must be an array')
    if (spec.exists.length === 0) fail('spec.exists must be absent, not an empty array')
    if (spec.exists.length > 1)
      fail(`spec.exists has ${spec.exists.length} entries (max 1)`)
    for (const [i, e] of spec.exists.entries()) {
      if (!isPlainObject(e)) fail(`spec.exists[${i}] must be an object`)
      onlyKeys(e, ['rel', 'where'], `spec.exists[${i}]`)
      if (typeof e.rel !== 'string') fail(`spec.exists[${i}].rel must be a string`)
      const info = RELATIONSHIPS[table]?.[e.rel]
      if (!info)
        fail(
          `spec.exists[${i}]: unknown relationship ${JSON.stringify(e.rel)} on ${table}`
        )
      if (e.where !== undefined)
        validateWhere(e.where, info.child, `spec.exists[${i}].where`, 1)
    }
  }
  if (spec.start !== undefined) {
    // sweep emits start ONLY on root task, orderBy exactly [[rank,dir],[id,asc]],
    // row exactly {rank: finite number, id: nonempty string}, inclusive true|absent.
    if (table !== 'task') fail('start is only valid on table task')
    const ob = spec.orderBy as [string, string][]
    if (
      ob.length !== 2 ||
      ob[0]![0] !== 'rank' ||
      (ob[0]![1] !== 'asc' && ob[0]![1] !== 'desc')
    ) {
      fail("start requires orderBy exactly [['rank','asc'|'desc'],['id','asc']]")
    }
    if (!isPlainObject(spec.start)) fail('spec.start must be an object')
    onlyKeys(spec.start, ['row', 'inclusive'], 'spec.start')
    if (spec.start.inclusive !== undefined && spec.start.inclusive !== true)
      fail('spec.start.inclusive must be true or absent')
    if (!isPlainObject(spec.start.row)) fail('spec.start.row must be an object')
    onlyKeys(spec.start.row, ['rank', 'id'], 'spec.start.row')
    if (!isFiniteNum(spec.start.row.rank))
      fail('spec.start.row.rank must be a finite number')
    if (!isNonEmptyStr(spec.start.row.id))
      fail('spec.start.row.id must be a nonempty string')
  }
}

// ---------------------------------------------------------------------------
export function parseCorpusEntry(
  json: string,
  opts?: { expectedFingerprint?: string }
): SweepDivergence {
  const o = JSON.parse(json) as Record<string, unknown>
  if (!isPlainObject(o)) fail('entry must be an object')
  onlyKeys(o, ENTRY_KEYS, 'entry')
  if (o.schemaVersion !== 1)
    fail(`schemaVersion must be 1, got ${JSON.stringify(o.schemaVersion)}`)
  if (o.kind !== 'sweep-divergence')
    fail(`kind must be "sweep-divergence", got ${JSON.stringify(o.kind)}`)
  if (!isNonEmptyStr(o.id) || !SAFE_TOKEN.test(o.id))
    fail('id must be a safe token [A-Za-z0-9][A-Za-z0-9._-]*')
  if (!isNonEmptyStr(o.note)) fail('note must be a nonempty (NUL-free) string')
  if (
    typeof o.against !== 'string' ||
    !(AGAINST_TARGETS as readonly string[]).includes(o.against)
  )
    fail(`against must be one of ${AGAINST_TARGETS.join(', ')} (not stock-zero)`)
  if (
    typeof o.observedTarget !== 'string' ||
    !(OBSERVED_TARGETS as readonly string[]).includes(o.observedTarget)
  )
    fail(`observedTarget must be one of ${OBSERVED_TARGETS.join(', ')}`)
  if (o.phase !== 'hydrate' && o.phase !== 'post-writes' && o.phase !== 'incremental')
    fail(`bad phase ${JSON.stringify(o.phase)}`)
  if (o.comparisonKind !== 'cross-target' && o.comparisonKind !== 'single-target')
    fail(`bad comparisonKind ${JSON.stringify(o.comparisonKind)}`)
  for (const k of ['exactReplayable', 'minimizationComplete'] as const) {
    if (typeof o[k] !== 'boolean') fail(`${k} must be boolean`)
  }
  if (o.expectConverge !== true) fail('expectConverge must be true')
  for (const k of [
    'round',
    'specIndex',
    'rounds',
    'queriesPerRound',
    'seed',
    'constructCount',
  ] as const) {
    if (!isSafeNonNegInt(o[k]))
      fail(`${k} must be a safe non-negative integer (reject -0)`)
  }
  if (
    o.originalConstructCount !== undefined &&
    !isSafeNonNegInt(o.originalConstructCount)
  )
    fail('originalConstructCount must be a safe non-negative integer when present')
  if (!isHex64(o.sourceFingerprint)) fail('sourceFingerprint must be lowercase 64-hex')
  if (!isHex64(o.leftHash) || !isHex64(o.rightHash))
    fail('leftHash/rightHash must be lowercase 64-hex')
  if (o.leftHash === o.rightHash) fail('leftHash === rightHash: not a divergence')
  if (!isStr(o.leftPreview) || !isStr(o.rightPreview))
    fail('leftPreview/rightPreview must be NUL-free strings')
  if (o.fullSidecar !== undefined) {
    if (!isNonEmptyStr(o.fullSidecar))
      fail('fullSidecar must be a nonempty NUL-free string when present')
    if (
      isAbsolute(o.fullSidecar) ||
      o.fullSidecar.includes('\\') ||
      o.fullSidecar.split('/').includes('..')
    )
      fail(
        'fullSidecar must be a safe repo-relative path (no absolute, no backslash, no "..")'
      )
  }
  if (!isNonEmptyStr(o.replay))
    fail('replay is REQUIRED (a permanent repro without a replay command is invalid)')

  assertValidSpec(o.spec)
  const spec = o.spec as GenSpec

  const rounds = o.rounds as number
  const queriesPerRound = o.queriesPerRound as number
  const round = o.round as number
  const specIndex = o.specIndex as number
  if (rounds <= 0) fail('rounds must be > 0')
  if (queriesPerRound <= 0) fail('queriesPerRound must be > 0')
  // guard multiplication overflow for EVERY phase (a huge rounds*queriesPerRound
  // can overflow even when a single round's batch is safe), THEN bound the round,
  // THEN derive the phase-sensitive live-spec ceiling (views accumulate, so at
  // round r up to (r+1)*queriesPerRound live specs; incremental runs after all
  // rounds).
  const totalSpecs = rounds * queriesPerRound
  if (!Number.isSafeInteger(totalSpecs))
    fail('rounds*queriesPerRound overflows the safe-integer range')
  if (o.phase === 'incremental') {
    if (round !== rounds)
      fail(`incremental round must equal rounds (${rounds}), got ${round}`)
  } else if (round >= rounds) {
    fail(`${o.phase} round ${round} must be < rounds ${rounds}`)
  }
  const liveAtRound =
    o.phase === 'incremental' ? totalSpecs : (round + 1) * queriesPerRound
  if (specIndex >= liveAtRound)
    fail(
      `specIndex ${specIndex} out of range for ${o.phase} round ${round} (< ${liveAtRound})`
    )

  const exact = o.exactReplayable as boolean
  const wantExact =
    o.phase === 'hydrate' && round === 0 && o.comparisonKind === 'cross-target'
  if (exact !== wantExact)
    fail(
      `exactReplayable must be ${wantExact} for phase=${o.phase} round=${round} comparison=${o.comparisonKind}`
    )
  if ((o.phase === 'incremental') !== (o.comparisonKind === 'single-target'))
    fail('incremental iff single-target')
  if (o.phase === 'post-writes' && o.comparisonKind !== 'cross-target')
    fail('post-writes must be cross-target')
  if (!exact && (o.minimizationComplete as boolean))
    fail('non-exact entries are never minimized (minimizationComplete must be false)')
  if (o.comparisonKind === 'cross-target' && o.observedTarget !== o.against)
    fail('cross-target observedTarget must equal against')
  if (
    o.comparisonKind === 'single-target' &&
    o.observedTarget !== 'stock-zero' &&
    o.observedTarget !== o.against
  )
    fail('single-target observedTarget must be stock-zero or against')
  const cc = constructCount(spec)
  if (o.constructCount !== cc)
    fail(`constructCount ${o.constructCount} != constructCount(spec) ${cc}`)
  if (o.originalConstructCount !== undefined && (o.originalConstructCount as number) < cc)
    fail('originalConstructCount must be >= constructCount(spec)')
  const want = buildReplayCommand({
    exactReplayable: exact,
    id: o.id,
    against: o.against,
    seed: o.seed as number,
    rounds,
    queriesPerRound,
  })
  if (o.replay !== want)
    fail(`replay command stale — expected exactly:\n  ${want}\ngot:\n  ${o.replay}`)
  if (
    opts?.expectedFingerprint !== undefined &&
    o.sourceFingerprint !== opts.expectedFingerprint
  ) {
    fail(
      `sourceFingerprint ${o.sourceFingerprint} != current fixture digest ${opts.expectedFingerprint} — the fixture-data SEED changed; re-capture`
    )
  }

  return o as unknown as SweepDivergence
}

// ---------------------------------------------------------------------------
export function corpusDir(): string {
  return join(import.meta.dirname, '..', 'regressions', 'sweep', 'v1')
}

// A MISSING directory is the deterministic empty infrastructure state (returns
// []); a corrupt file, basename != id, duplicate id, or a fingerprint mismatch
// against the current fixture THROWS. Paths are read in sorted order.
export function loadCorpus(
  dir = corpusDir(),
  opts?: { expectedFingerprint?: string }
): SweepDivergence[] {
  if (!existsSync(dir)) return []
  const expectedFingerprint = opts?.expectedFingerprint ?? currentSeedFingerprint()
  const names = readdirSync(dir)
    .filter((n) => n.endsWith('.json'))
    .sort()
  const parsed = names.map((n) => ({
    n,
    e: parseCorpusEntry(readFileSync(join(dir, n), 'utf-8'), { expectedFingerprint }),
  }))
  // reject duplicate ids BEFORE the per-file basename check, so two same-id
  // files reach the duplicate diagnostic (a differently-named file would
  // otherwise trip basename first and hide the duplicate).
  const seen = new Set<string>()
  for (const { e } of parsed) {
    if (seen.has(e.id)) fail(`duplicate corpus id ${e.id}`)
    seen.add(e.id)
  }
  for (const { n, e } of parsed) {
    if (`${e.id}.json` !== basename(n)) fail(`file ${n} basename must equal id ${e.id}`)
  }
  return parsed.map((p) => p.e) // sorted by filename
}
