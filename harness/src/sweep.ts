// M3 sweep lane: seeded RANDOMIZED cross-implementation differential (the
// harness's take on upstream mono's coverage-driven fuzzer, reframed as
// black-box wire-level conformance). every round generates random query
// shapes over the fixture schema — cmp/and/or trees, exists, orderBy, limit,
// related windows, one() — materializes them through the ONE `generated`
// named query on stock-zero AND an orez target, then runs random writes
// (custom mutators + upstream sql) and requires every live view to stay
// canonically equal. views accumulate across rounds, so early shapes keep
// being incrementally maintained under later churn. at the end, fresh late
// clients rematerialize every spec (incremental == fresh).
//
// deterministic per seed. a divergence writes a self-validated SweepDivergence
// to regressions/sweep/v1/<id>.json (spec-corpus.ts). the FIRST eligible
// (round-0 hydrate cross-target) divergence is delta-debugged to a minimal
// still-reproducing spec (spec-shrink.ts) before it is written — the only phase
// that replays exactly from (seed, spec). every other divergence is stored
// non-exact with its full seeded replay command.
//
//   bun src/sweep.ts                                  # random seed, orez-local
//   bun src/sweep.ts --seed 12345 --rounds 20
//   bun src/sweep.ts --against orez-cf --rounds 8
//   bun src/sweep.ts --replay-corpus regressions/sweep/v1/<id>.json  # one entry
//   bun src/sweep.ts --corpus                         # nightly: preload + replay
//   bun src/sweep.ts --inject --seed 42 --queriesPerRound 4  # proof seam
import { readFileSync } from 'node:fs'
import { isAbsolute, join } from 'node:path'
import { parseArgs } from 'node:util'

import { canonical } from './canonical.js'
import {
  type GenSpec,
  type GenSubSpec,
  type GenWhere,
  SEED,
  mutators,
  queries,
} from './fixture.js'
import { assertServerOutcome, type ExpectedServerOutcome } from './server-outcome.js'
import {
  buildSweepDivergence,
  corpusDir,
  currentSeedFingerprint,
  loadCorpus,
  parseCorpusEntry,
  type Phase,
  writeCorpusEntry,
} from './spec-corpus.js'
import { constructCount, shrinkSpec } from './spec-shrink.js'
import { sweepPairwiseCoverage } from './sweep-coverage.js'
import { startStockZero } from './targets/stock-zero.js'

import type { FixtureZero, SyncTarget } from './target.js'

const { values: args } = parseArgs({
  options: {
    against: { type: 'string', default: 'orez-local' },
    seed: { type: 'string', default: String(Math.floor(Math.random() * 2 ** 31)) },
    rounds: { type: 'string', default: '12' },
    queriesPerRound: { type: 'string', default: '4' },
    // generate the specs and print grammar-axis coverage without booting
    // targets — for verifying generator changes actually produce the axes
    dry: { type: 'boolean', default: false },
    // replay ONE committed known-gap corpus entry (no random rounds): boot fresh
    // targets, materialize its spec on both, assert CONVERGE. exit 0 converge, 1
    // regressed, 2 corrupt/target-mismatch/fingerprint-mismatch.
    'replay-corpus': { type: 'string' },
    // NIGHTLY-only: before the random rounds, replay each committed exact
    // corpus entry and assert convergence. ordinary PR sweep does NOT preload.
    corpus: { type: 'boolean', default: false },
    // demo/proof: make the orez side drop its first result row so a divergence
    // is injected reproducibly (exercises the writer + shrink path on a green
    // sweep). never for normal runs.
    inject: { type: 'boolean', default: false },
  },
})

const SWEEP_SEED = Number(args.seed)
const ROUNDS = Number(args.rounds)
const QUERIES_PER_ROUND = Number(args.queriesPerRound)

// ---------------------------------------------------------------------------
// seeded generator
// ---------------------------------------------------------------------------

function mulberry32(seed: number) {
  let a = seed
  return () => {
    a |= 0
    a = (a + 0x6d2b79f5) | 0
    let t = Math.imul(a ^ (a >>> 15), 1 | a)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}
const rng = mulberry32(SWEEP_SEED)
const pick = <T>(arr: readonly T[]): T => arr[Math.floor(rng() * arr.length)]!
const chance = (p: number) => rng() < p
const int = (min: number, max: number) => min + Math.floor(rng() * (max - min + 1))

// live id pools, maintained as the write script mutates state. queries may
// also reference dead ids (that's a valid shape); pools keep SOME hits alive.
const pools = {
  user: SEED.user.map((u) => u.id),
  project: SEED.project.map((p) => p.id),
  member: SEED.member.map((m) => m.id),
  task: SEED.task.map((t) => t.id),
}
const taskDone = new Map(SEED.task.map((task) => [task.id, task.done]))

const TITLE_PATTERNS = ['%fix%', '%ux%', '%🚀%', '%sync%', 'fix%', '%triage%']
const NAME_PATTERNS = ['%a%', '%fix%', '%Zen%', '%ütopia%', '%x %']

type TableName = keyof typeof pools

// column pools per table: [col, kind]
const COLUMNS: Record<
  TableName,
  [string, 'id' | 'string' | 'number' | 'boolean' | 'nullableNumber' | 'json'][]
> = {
  user: [
    ['id', 'id'],
    ['name', 'string'],
  ],
  project: [
    ['id', 'id'],
    ['ownerId', 'id'],
    ['name', 'string'],
  ],
  member: [
    ['id', 'id'],
    ['projectId', 'id'],
    ['userId', 'id'],
  ],
  task: [
    ['id', 'id'],
    ['projectId', 'id'],
    ['title', 'string'],
    ['rank', 'number'],
    ['done', 'boolean'],
    ['dueAt', 'nullableNumber'],
    ['meta', 'json'],
  ],
}

// which pool an id-kind column draws from
const ID_POOL: Record<string, TableName> = {
  id: 'user', // overridden per table below
  ownerId: 'user',
  userId: 'user',
  projectId: 'project',
}

const RELS: Record<TableName, { rel: string; to: TableName; kind: 'one' | 'many' }[]> = {
  user: [],
  project: [
    { rel: 'members', to: 'member', kind: 'many' },
    { rel: 'tasks', to: 'task', kind: 'many' },
  ],
  member: [
    { rel: 'user', to: 'user', kind: 'one' },
    { rel: 'project', to: 'project', kind: 'one' },
  ],
  task: [{ rel: 'project', to: 'project', kind: 'one' }],
}

function idValue(table: TableName, col: string): string {
  const pool = col === 'id' ? pools[table] : pools[ID_POOL[col]!]
  return pick(pool)
}

function genCmp(table: TableName): GenWhere {
  const [col, kind] = pick(COLUMNS[table])
  switch (kind) {
    case 'id': {
      const op = pick(['=', '!=', 'IN'] as const)
      if (op === 'IN') {
        const n = int(2, 4)
        return {
          op: 'cmp',
          col,
          cmp: 'IN',
          value: Array.from({ length: n }, () => idValue(table, col)),
        }
      }
      return { op: 'cmp', col, cmp: op, value: idValue(table, col) }
    }
    case 'string':
      return chance(0.6)
        ? {
            op: 'cmp',
            col,
            cmp: pick(['LIKE', 'ILIKE'] as const),
            value: pick(table === 'task' ? TITLE_PATTERNS : NAME_PATTERNS),
          }
        : { op: 'cmp', col, cmp: '!=', value: 'nonexistent' }
    case 'number':
      return {
        op: 'cmp',
        col,
        cmp: pick(['>', '<', '>=', '<='] as const),
        value: Math.round((rng() * 24 - 4) * 100) / 100,
      }
    case 'boolean':
      return { op: 'cmp', col, cmp: pick(['=', '!='] as const), value: chance(0.5) }
    case 'nullableNumber':
      return chance(0.4)
        ? { op: 'cmp', col, cmp: pick(['IS', 'IS NOT'] as const), value: null }
        : {
            op: 'cmp',
            col,
            cmp: pick(['<', '>'] as const),
            value: 1750000000000 + Math.floor(rng() * 10_000_000_000),
          }
    case 'json':
      return { op: 'cmp', col, cmp: pick(['IS', 'IS NOT'] as const), value: null }
  }
}

function genWhere(table: TableName, depth: number): GenWhere {
  if (depth === 0 || chance(0.5)) return genCmp(table)
  return {
    op: pick(['and', 'or'] as const),
    children: Array.from({ length: int(2, 3) }, () => genWhere(table, depth - 1)),
  }
}

function genOrderBy(table: TableName): [string, 'asc' | 'desc'][] {
  const orderable = COLUMNS[table].filter(([, kind]) => kind !== 'json')
  const order: [string, 'asc' | 'desc'][] = []
  if (chance(0.7)) {
    const [col] = pick(orderable)
    if (col !== 'id') order.push([col, pick(['asc', 'desc'] as const)])
  }
  // deterministic tiebreak ALWAYS: limit windows with ties must select the
  // same rows on both engines
  order.push(['id', 'asc'])
  return order
}

function genSub(
  to: TableName,
  kind: 'one' | 'many',
  depth: number
): GenSubSpec | undefined {
  const nestedRels = RELS[to]
  const nest = (): GenSubSpec['related'] => {
    if (depth >= 2 || nestedRels.length === 0 || !chance(0.5)) return undefined
    const r = pick(nestedRels)
    return [{ rel: r.rel, sub: genSub(r.to, r.kind, depth + 1) }]
  }
  if (kind === 'one') {
    const sub: GenSubSpec = { one: true }
    const related = nest()
    if (related) sub.related = related
    return sub
  }
  if (chance(0.3)) return undefined
  const sub: GenSubSpec = {}
  if (chance(0.5)) sub.where = genWhere(to, 1)
  sub.orderBy = genOrderBy(to)
  if (chance(0.5)) sub.limit = int(1, 4)
  const related = nest()
  if (related) sub.related = related
  return sub
}

function genSpec(): GenSpec {
  const table = pick(['project', 'task', 'member', 'user'] as const)
  const spec: GenSpec = { table }
  if (chance(0.75)) spec.where = genWhere(table, 2)
  const rels = RELS[table]
  if (rels.length > 0 && chance(0.4)) {
    const e = pick(rels)
    spec.exists = [{ rel: e.rel, where: chance(0.7) ? genWhere(e.to, 1) : undefined }]
  }
  spec.orderBy = genOrderBy(table)
  // start() cursor: seek past a real seed row. Generate both the numeric rank
  // shape and a nullable dueAt cursor so NULL ordering remains in the ordinary
  // differential corpus instead of being reachable only by metamorphic tests.
  if (table === 'task' && chance(0.25)) {
    if (chance(0.35)) {
      const cursor = pick(SEED.task.filter((row) => row.dueAt === null))
      spec.orderBy = [
        ['dueAt', pick(['asc', 'desc'] as const)],
        ['id', 'asc'],
      ]
      spec.start = {
        row: { dueAt: null, id: cursor.id },
        inclusive: chance(0.3) || undefined,
      }
    } else {
      const cursor = pick(SEED.task)
      spec.orderBy = [
        ['rank', pick(['asc', 'desc'] as const)],
        ['id', 'asc'],
      ]
      spec.start = {
        row: { rank: cursor.rank, id: cursor.id },
        inclusive: chance(0.3) || undefined,
      }
    }
  }
  if (chance(0.4)) spec.limit = int(1, 8)
  if (rels.length > 0 && chance(0.6)) {
    const count = chance(0.3) && rels.length > 1 ? 2 : 1
    const chosen = [...rels].sort(() => rng() - 0.5).slice(0, count)
    spec.related = chosen.map((r) => ({ rel: r.rel, sub: genSub(r.to, r.kind, 1) }))
  }
  if (spec.limit === undefined && chance(0.15)) spec.one = true
  return spec
}

// ---------------------------------------------------------------------------
// random write script (mirrored onto both targets)
// ---------------------------------------------------------------------------

type Write =
  | {
      kind: 'mutate'
      label: string
      expected: ExpectedServerOutcome
      make: (z: FixtureZero) => { client: Promise<unknown>; server: Promise<unknown> }
    }
  | { kind: 'sql'; label: string; sql: string }

let writeSeq = 0

function genWrites(round: number): Write[] {
  const writes: Write[] = []
  const count = int(1, 4)
  for (let i = 0; i < count; i++) {
    const roll = rng()
    if (roll < 0.2) {
      const id = `wp-${round}-${writeSeq++}`
      pools.project.push(id)
      const ownerId = pick(pools.user)
      writes.push({
        kind: 'mutate',
        label: `project.create ${id}`,
        expected: 'success',
        make: (z) =>
          z.mutate(mutators.project.create({ id, ownerId, name: `sweep ${id}` })),
      })
    } else if (roll < 0.4) {
      const id = `wt-${round}-${writeSeq++}`
      pools.task.push(id)
      const projectId = pick(pools.project)
      const rank = Math.round((rng() * 24 - 4) * 100) / 100
      const done = chance(0.4)
      taskDone.set(id, done)
      const meta = chance(0.5) ? { round, tag: pick(['a', 'b', '✅']) } : undefined
      const dueAt = chance(0.6)
        ? 1750000000000 + Math.floor(rng() * 10_000_000_000)
        : undefined
      writes.push({
        kind: 'mutate',
        label: `task.create ${id}`,
        expected: 'success',
        make: (z) =>
          z.mutate(
            mutators.task.create({
              id,
              projectId,
              title: `sweep fix ${id}`,
              rank,
              done,
              meta,
              dueAt,
            })
          ),
      })
    } else if (roll < 0.55) {
      const id = pick(pools.task)
      const done = !taskDone.get(id)!
      taskDone.set(id, done)
      writes.push({
        kind: 'mutate',
        label: `task.toggle ${id}`,
        expected: 'success',
        make: (z) => z.mutate(mutators.task.toggle({ id, done })),
      })
    } else if (roll < 0.7) {
      const id = pick(pools.task)
      const rank = chance(0.3) ? int(90, 110) : Math.round((rng() * 24 - 4) * 100) / 100
      writes.push({
        kind: 'mutate',
        label: `task.setRank ${id} ${rank}`,
        expected: 'success',
        make: (z) => z.mutate(mutators.task.setRank({ id, rank })),
      })
    } else if (roll < 0.8) {
      const id = `wm-${round}-${writeSeq++}`
      pools.member.push(id)
      const projectId = pick(pools.project)
      const userId = pick(pools.user)
      writes.push({
        kind: 'mutate',
        label: `member.add ${id}`,
        expected: 'success',
        make: (z) => z.mutate(mutators.member.add({ id, projectId, userId })),
      })
    } else if (roll < 0.87 && pools.member.length > 3) {
      const idx = int(0, pools.member.length - 1)
      const id = pools.member.splice(idx, 1)[0]!
      writes.push({
        kind: 'mutate',
        label: `member.remove ${id}`,
        expected: 'success',
        make: (z) => z.mutate(mutators.member.remove({ id })),
      })
    } else if (roll < 0.94 && pools.task.length > 10) {
      const idx = int(0, pools.task.length - 1)
      const id = pools.task.splice(idx, 1)[0]!
      taskDone.delete(id)
      writes.push({
        kind: 'sql',
        label: `sql delete task ${id}`,
        sql: `DELETE FROM task WHERE id = '${id}'`,
      })
    } else {
      const id = pick(pools.task)
      const flip = chance(0.5)
      writes.push({
        kind: 'sql',
        label: `sql dueAt ${flip ? 'null' : 'set'} ${id}`,
        sql: flip
          ? `UPDATE task SET "dueAt" = NULL WHERE id = '${id}'`
          : `UPDATE task SET "dueAt" = ${1750000000000 + Math.floor(rng() * 10_000_000_000)} WHERE id = '${id}'`,
      })
    }
  }
  return writes
}

// ---------------------------------------------------------------------------
// harness plumbing
// ---------------------------------------------------------------------------

async function startAgainst(name: string): Promise<SyncTarget> {
  if (name === 'orez-local') {
    return (await import('./targets/orez-local.js')).startOrezLocal({
      pullIntervalMs: 150,
    })
  }
  if (name === 'rust-local') {
    return (await import('./targets/rust-local.js')).startRustLocal({
      pullIntervalMs: 150,
    })
  }
  if (name === 'rust-cf') {
    return (await import('./targets/rust-cf.js')).startRustCf({ pullIntervalMs: 150 })
  }
  if (name === 'orez-cf') {
    return (await import('./targets/orez-cf.js')).startOrezCf({ pullIntervalMs: 150 })
  }
  throw new Error(`unknown --against target '${name}'`)
}

// a corrupted target can wedge zero's push pipeline so client/server ack
// promises never settle (found via a sabotage run) — every await on them
// must be timeboxed or the lane hangs instead of failing
async function withTimeout<T>(
  promise: Promise<T>,
  ms: number,
  label: string
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`timeout ${ms}ms: ${label}`)), ms)
      }),
    ])
  } finally {
    clearTimeout(timer)
  }
}

async function eventually(check: () => void, timeoutMs: number, label: string) {
  const start = Date.now()
  let lastError: unknown
  while (Date.now() - start < timeoutMs) {
    try {
      check()
      return
    } catch (error) {
      lastError = error
      await new Promise((r) => setTimeout(r, 50))
    }
  }
  throw new Error(`timeout (${timeoutMs}ms): ${label}: ${lastError}`)
}

type LiveView = {
  specIndex: number
  spec: GenSpec
  rows: () => unknown
  complete: () => boolean
  destroy: () => void
}

// `dropFirst` (only under --inject) drops the first ROOT row on the orez side so
// a cross-target divergence is injected reproducibly on a green sweep — it must
// apply to EVERY orez materialization (initial views AND shrink candidates) so
// stillDiverges sees the same injection the round-0 hydrate compare did.
function materializeSpec(
  zero: FixtureZero,
  spec: GenSpec,
  specIndex: number,
  dropFirst = false
): LiveView {
  const view = zero.materialize(queries.generated(spec) as never)
  let rows: unknown = null
  let complete = false
  view.addListener((data: unknown, resultType: string) => {
    let snapshot: unknown = JSON.parse(JSON.stringify(data ?? null))
    if (dropFirst && Array.isArray(snapshot)) snapshot = snapshot.slice(1)
    rows = snapshot
    if (resultType === 'complete') complete = true
  })
  return {
    specIndex,
    spec,
    rows: () => rows,
    complete: () => complete,
    destroy: () => view.destroy(),
  }
}

// the ONE divergence writer (replaces the legacy recordDivergence): builds a
// self-validated SweepDivergence and writes it to regressions/sweep/v1/<id>.json
// under a deterministic, collision-safe, no-overwrite id. run-shape provenance
// (seed/rounds/queriesPerRound/against) comes from the module constants.
function emitDivergence(input: {
  phase: Phase
  comparisonKind: 'cross-target' | 'single-target'
  round: number
  specIndex: number
  spec: GenSpec
  observedTarget: string
  leftRows: unknown
  rightRows: unknown
  note: string
  originalConstructCount?: number
  minimizationComplete: boolean
}): string {
  const entry = buildSweepDivergence({
    ...input,
    rounds: ROUNDS,
    queriesPerRound: QUERIES_PER_ROUND,
    seed: SWEEP_SEED,
    against: args.against!,
  })
  return writeCorpusEntry(corpusDir(), entry)
}

// ---------------------------------------------------------------------------
// run
// ---------------------------------------------------------------------------

if (args.dry) {
  const specs = Array.from({ length: ROUNDS * QUERIES_PER_ROUND }, () => genSpec())
  const pairwise = sweepPairwiseCoverage(specs)
  const has = (test: (s: GenSpec) => boolean) => specs.filter(test).length
  const hasNested = (s: GenSpec) =>
    (s.related ?? []).some((r) => (r.sub?.related ?? []).length > 0)
  console.log(
    JSON.stringify(
      {
        seed: SWEEP_SEED,
        specs: specs.length,
        where: has((s) => !!s.where),
        exists: has((s) => !!s.exists),
        limit: has((s) => s.limit !== undefined),
        one: has((s) => !!s.one),
        related: has((s) => !!s.related),
        nestedRelated: has(hasNested),
        start: has((s) => !!s.start),
        pairwise: {
          hit: pairwise.hit,
          total: pairwise.total,
          percent: pairwise.percent,
          byAxisPair: pairwise.byAxisPair,
          missing: pairwise.missing.slice(0, 20),
          missingTruncated: Math.max(0, pairwise.missing.length - 20),
        },
      },
      null,
      2
    )
  )
  for (const s of specs.slice(0, 3)) console.log(JSON.stringify(s))
  process.exit(0)
}

// --replay-corpus <path>: replay ONE committed exact entry (no random rounds).
// boot fresh targets, materialize its minimized spec on stock-zero + its target,
// assert CONVERGE. exit 0 converge, 1 regressed, 2 corrupt / fingerprint /
// target / not-exact / inconclusive. the parser (with the current fixture
// fingerprint) is the grammar + anti-corruption preflight before any boot.
if (args['replay-corpus'] !== undefined) {
  const p = args['replay-corpus']
  const file = isAbsolute(p) ? p : join(import.meta.dirname, '..', p)
  let entry: ReturnType<typeof parseCorpusEntry>
  try {
    entry = parseCorpusEntry(readFileSync(file, 'utf-8'), {
      expectedFingerprint: currentSeedFingerprint(),
    })
  } catch (error) {
    console.error(`[replay] corrupt/invalid corpus entry ${file}: ${error}`)
    process.exit(2)
  }
  if (!entry.exactReplayable) {
    console.error(
      `[replay] ${entry.id} is not exact-replayable (phase=${entry.phase} round=${entry.round} ${entry.comparisonKind}); only hydrate+round0+cross-target entries replay exactly`
    )
    process.exit(2)
  }
  if (entry.against !== args.against) {
    console.error(
      `[replay] target mismatch: entry recorded against ${entry.against}, run invoked --against ${args.against}`
    )
    process.exit(2)
  }
  console.log(
    `[replay] ${entry.id}: materializing minimized spec (constructCount ${entry.constructCount}) on stock-zero + ${entry.against}, expecting CONVERGE`
  )
  const [rStock, rOther] = await Promise.all([
    startStockZero(),
    startAgainst(entry.against),
  ])
  try {
    // --inject reactivates the orez-side fault so the exit-1 (REGRESSED) path is
    // deterministically testable; a plain replay (no inject) expects CONVERGE.
    const sv = materializeSpec(rStock.createClient('user-1'), entry.spec, 0)
    const ov = materializeSpec(rOther.createClient('user-1'), entry.spec, 0, args.inject)
    await eventually(
      () => {
        if (!sv.complete() || !ov.complete()) throw new Error('replay views not complete')
      },
      120_000,
      `replay ${entry.id} hydration`
    )
    const left = canonical(sv.rows())
    const right = canonical(ov.rows())
    sv.destroy()
    ov.destroy()
    await Promise.allSettled([rStock.close(), rOther.close()])
    if (left === right) {
      console.log(`[replay] CONVERGE ${entry.id}`)
      process.exit(0)
    }
    console.error(
      `[replay] REGRESSED ${entry.id}: committed repro diverged again\n  stock-zero: ${left.slice(0, 400)}\n  ${entry.against}: ${right.slice(0, 400)}`
    )
    process.exit(1)
  } catch (error) {
    // build/timeout during replay is inconclusive, not a clean converge/regress
    await Promise.allSettled([rStock.close(), rOther.close()])
    console.error(`[replay] inconclusive for ${entry.id}: ${error}`)
    process.exit(2)
  }
}

const t0 = Date.now()
console.log(
  `[sweep] seed=${SWEEP_SEED} rounds=${ROUNDS} queries/round=${QUERIES_PER_ROUND} against=${args.against}`
)
const [stock, other] = await Promise.all([startStockZero(), startAgainst(args.against!)])

const failures: string[] = []
const knownGaps: string[] = []
try {
  const stockZero = stock.createClient('user-1')
  const otherZero = other.createClient('user-1')

  // convergence beacon: sentinel project per round must reach both targets
  const beacons = [stockZero, otherZero].map((z) => {
    const view = z.materialize(queries.allProjects())
    let ids = new Set<string>()
    view.addListener((data) => {
      ids = new Set((data as readonly { id: string }[]).map((r) => r.id))
    })
    return { has: (id: string) => ids.has(id), destroy: () => view.destroy() }
  })

  const stockViews: LiveView[] = []
  const otherViews: LiveView[] = []
  const allSpecs: GenSpec[] = []
  let emptyAtHydrate = 0

  // the FIRST eligible (hydrate + round 0 + cross-target) divergence per run is
  // captured here and shrunk+written in a POST-STEP after the round-0 hydrate
  // compare (compareAll stays synchronous; shrinkSpec is async because it
  // materializes candidates on the live targets). all other divergences are
  // written immediately as non-exact entries.
  let pendingShrink: { specIndex: number; spec: GenSpec } | null = null
  // set true once the round-0 shrink post-step runs — used to make --inject
  // anti-vacuous (an inject run that shrinks nothing is a false green).
  let shrankEligible = false

  // stock zero-cache at the pinned version returns wrong results for a start
  // cursor anchored on a NULL-sorted row (upstream #6121, fixed upstream after
  // the 1.7.0 pin; committed repro:
  // regressions/known-gap-zql-6121-null-start-cursor.json). orez implements
  // the post-fix semantics, so a cross-target divergence on that axis is the
  // known upstream gap, not an orez failure: record the artifact, report it,
  // keep the run green. DELETE this classifier when the zero pin advances
  // past the fix — the axis then diffs at full strength again.
  const isKnown6121Gap = (spec: GenSpec) =>
    spec.start != null &&
    (spec.orderBy ?? []).some(([column]) => spec.start!.row[column] === null)

  const compareAll = (phase: Phase, round: number) => {
    let diverged = 0
    for (let i = 0; i < stockViews.length; i++) {
      const leftRows = stockViews[i]!.rows()
      const rightRows = otherViews[i]!.rows()
      const left = canonical(leftRows)
      const right = canonical(rightRows)
      if (left !== right) {
        if (isKnown6121Gap(allSpecs[i]!)) {
          const file = emitDivergence({
            phase,
            comparisonKind: 'cross-target',
            round,
            specIndex: i,
            spec: allSpecs[i]!,
            observedTarget: args.against!,
            leftRows,
            rightRows,
            note: `known upstream gap #6121 (null-anchored start cursor) at ${phase} round ${round} spec ${i} vs ${args.against}`,
            minimizationComplete: false,
          })
          knownGaps.push(
            `[${phase} r${round}] spec ${i} diverged on the known #6121 axis (artifact ${file})`
          )
          continue
        }
        diverged++
        const eligible = phase === 'hydrate' && round === 0
        if (eligible && pendingShrink === null) {
          // defer: the minimized entry is shrunk + written in the post-step.
          pendingShrink = { specIndex: i, spec: allSpecs[i]! }
          failures.push(
            `[hydrate r0] spec ${i} diverged (eligible — shrinking to a minimal round-0 repro)\n  spec: ${JSON.stringify(allSpecs[i])}\n  stock-zero: ${left.slice(0, 300)}\n  ${other.name}: ${right.slice(0, 300)}`
          )
          continue
        }
        const file = emitDivergence({
          phase,
          comparisonKind: 'cross-target',
          round,
          specIndex: i,
          spec: allSpecs[i]!,
          observedTarget: args.against!,
          leftRows,
          rightRows,
          note: `sweep ${phase} divergence at round ${round} spec ${i} vs ${args.against}`,
          minimizationComplete: false,
        })
        failures.push(
          `[${phase} r${round}] spec ${i} diverged (artifact ${file})\n  spec: ${JSON.stringify(allSpecs[i])}\n  stock-zero: ${left.slice(0, 300)}\n  ${other.name}: ${right.slice(0, 300)}`
        )
      }
    }
    return diverged
  }

  // materialize one spec on both fresh (round-0) clients, wait for hydration,
  // return each side's rows + whether they diverge — or null if the spec fails
  // to BUILD or does not hydrate within the timeout (both treated as
  // non-reproduction). ALWAYS destroys the candidate views. inject drops the
  // orez side's first row so stillDiverges matches the live compare.
  const evalSpec = async (
    spec: GenSpec
  ): Promise<{ left: unknown; right: unknown; diverges: boolean } | null> => {
    let sv: LiveView | undefined
    let ov: LiveView | undefined
    try {
      sv = materializeSpec(stockZero, spec, -1)
      ov = materializeSpec(otherZero, spec, -1, args.inject)
      await eventually(
        () => {
          if (!sv!.complete() || !ov!.complete()) throw new Error('candidate incomplete')
        },
        30_000,
        'shrink candidate hydration'
      )
      const left = sv.rows()
      const right = ov.rows()
      return { left, right, diverges: canonical(left) !== canonical(right) }
    } catch {
      return null // build error or timeout: not a reproduction
    } finally {
      sv?.destroy()
      ov?.destroy()
    }
  }
  const stillDiverges = async (spec: GenSpec): Promise<boolean> =>
    (await evalSpec(spec))?.diverges === true

  // one global evaluation budget for the run's single shrink (not per finding).
  const SHRINK_EVAL_BUDGET = 300

  const shrinkAndEmit = async (p: { specIndex: number; spec: GenSpec }) => {
    const originalConstructCount = constructCount(p.spec)
    const result = await shrinkSpec(p.spec, stillDiverges, SHRINK_EVAL_BUDGET)
    const minCount = constructCount(result.spec)
    // recapture the MINIMIZED spec's still-divergent rows on the same fresh
    // (round-0) state for the stored hashes/previews. round-0 hydrate is
    // deterministic, so a spec shrinkSpec accepted must reproduce here.
    const rec = await evalSpec(result.spec)
    if (!rec || !rec.diverges) {
      throw new Error(
        `minimized spec failed to reproduce on recapture (nondeterministic round-0 state?): ${JSON.stringify(result.spec)}`
      )
    }
    const file = emitDivergence({
      phase: 'hydrate',
      comparisonKind: 'cross-target',
      round: 0,
      specIndex: p.specIndex,
      spec: result.spec,
      observedTarget: args.against!,
      leftRows: rec.left,
      rightRows: rec.right,
      note: `minimized hydrate r0 divergence vs ${args.against}: constructCount ${originalConstructCount} -> ${minCount} in ${result.evaluations} evals (${result.complete ? 'complete' : 'budget-exhausted'})`,
      originalConstructCount,
      minimizationComplete: result.complete,
    })
    console.log(
      `[sweep] shrank eligible divergence spec ${p.specIndex}: constructCount ${originalConstructCount}->${minCount}, ${result.evaluations} evals, complete=${result.complete}, wrote ${file}`
    )
  }

  // NIGHTLY-only (--corpus): before the random rounds, replay each committed
  // EXACT entry for this target from the fresh seed state and assert it still
  // converges (the fix that closed the original divergence must hold). the
  // ordinary PR sweep does NOT preload, so no new PR-gating live work is added.
  if (args.corpus) {
    const all = loadCorpus()
    const exact = all.filter((e) => e.exactReplayable && e.against === args.against)
    const skipped = all.length - exact.length
    if (exact.length === 0) {
      console.log(
        `[sweep] corpus preload: anti-vacuity: 0 exact-replayable entries for ${args.against} (infrastructure-only)${skipped ? `, ${skipped} for other targets/phases skipped` : ''}`
      )
    } else {
      console.log(
        `[sweep] corpus preload: replaying ${exact.length} exact entries for ${args.against}${skipped ? ` (${skipped} for other targets/phases skipped)` : ''}`
      )
      const pv = exact.map((e) => ({
        e,
        sv: materializeSpec(stockZero, e.spec, -1),
        ov: materializeSpec(otherZero, e.spec, -1),
      }))
      await eventually(
        () => {
          for (const q of pv)
            if (!q.sv.complete() || !q.ov.complete())
              throw new Error(`corpus ${q.e.id} not complete`)
        },
        120_000,
        'corpus preload hydration'
      )
      for (const q of pv) {
        if (canonical(q.sv.rows()) !== canonical(q.ov.rows())) {
          failures.push(
            `[corpus] REGRESSED ${q.e.id}: committed exact repro diverged again\n  replay: ${q.e.replay}`
          )
        } else {
          console.log(`[sweep] corpus ${q.e.id}: converged`)
        }
        q.sv.destroy()
        q.ov.destroy()
      }
    }
  }

  for (let round = 0; round < ROUNDS; round++) {
    // new shapes this round
    for (let k = 0; k < QUERIES_PER_ROUND; k++) {
      const spec = genSpec()
      const specIndex = allSpecs.length
      allSpecs.push(spec)
      stockViews.push(materializeSpec(stockZero, spec, specIndex))
      otherViews.push(materializeSpec(otherZero, spec, specIndex, args.inject))
    }

    await eventually(
      () => {
        for (const v of [...stockViews, ...otherViews]) {
          if (!v.complete()) throw new Error(`spec ${v.specIndex} not complete`)
        }
      },
      60_000,
      `round ${round} hydration`
    )

    for (let k = allSpecs.length - QUERIES_PER_ROUND; k < allSpecs.length; k++) {
      const rows = stockViews[k]!.rows()
      if (rows === null || (Array.isArray(rows) && rows.length === 0)) emptyAtHydrate++
    }

    compareAll('hydrate', round)

    // POST-STEP: shrink + write the first eligible (round-0 hydrate cross-target)
    // divergence. runs here, before this round's writes are applied, so the live
    // clients are still at fresh seed state — the only state a round-0 hydrate
    // repro is exact against.
    if (round === 0 && pendingShrink) {
      await shrinkAndEmit(pendingShrink)
      pendingShrink = null
      shrankEligible = true
    }

    // mirrored random writes through each target's own client + upstream sql
    const writes = genWrites(round)
    const acks: Promise<void>[] = []
    for (const write of writes) {
      if (write.kind === 'mutate') {
        for (const [targetName, zero] of [
          [stock.name, stockZero],
          [other.name, otherZero],
        ] as const) {
          const req = write.make(zero)
          acks.push(
            assertServerOutcome(
              req.server,
              write.expected,
              `${targetName} ${write.label}`
            )
          )
          await withTimeout(req.client, 15_000, `client apply: ${write.label}`)
        }
      } else {
        await stock.sql(write.sql)
        await other.sql(write.sql)
      }
    }
    await withTimeout(Promise.all(acks), 30_000, `round ${round} server acks`)

    // sentinel barrier: both targets must see this round's marker project
    const sentinel = `sentinel-${SWEEP_SEED}-${round}`
    pools.project.push(sentinel)
    for (const zero of [stockZero, otherZero]) {
      const req = zero.mutate(
        mutators.project.create({ id: sentinel, ownerId: 'u0', name: sentinel })
      )
      await withTimeout(
        assertServerOutcome(req.server, 'success', `sentinel ${sentinel}`),
        30_000,
        `sentinel ${sentinel} server ack`
      )
    }
    await eventually(
      () => {
        for (const beacon of beacons) {
          if (!beacon.has(sentinel)) throw new Error(`sentinel ${sentinel} not visible`)
        }
      },
      60_000,
      `round ${round} sentinel convergence`
    )

    const diverged = compareAll('post-writes', round)
    console.log(
      `[sweep] round ${round}: ${allSpecs.length} live shapes, ${writes.length} writes (${writes.map((w) => w.label).join('; ')}), ${diverged} divergences`
    )
  }

  // --inject is a proof seam, never a normal run: it MUST produce an eligible
  // round-0 hydrate divergence to shrink. if it didn't (every round-0 spec
  // returned empty or a one() object, so dropping the orez first row was a
  // no-op), the run vacuously "passed" without exercising the writer/shrink
  // path — fail loudly rather than report a false green.
  if (args.inject && !shrankEligible) {
    failures.push(
      '[inject] vacuous: no eligible round-0 hydrate divergence was produced (every round-0 spec returned empty or a one() object). pick a seed/queriesPerRound with nonempty array hydration, e.g. --seed 42 --queriesPerRound 4'
    )
  }

  // incremental == fresh: late clients rematerialize EVERY spec from scratch
  const lateChecks = [
    { target: stock, views: stockViews },
    { target: other, views: otherViews },
  ]
  for (const { target, views } of lateChecks) {
    // inject drops the orez side's first row; apply it to the late fresh views of
    // the orez target too, so a green single-target incremental check stays green
    // (both maintained and fresh drop the same first row).
    const injectThis = target === other && args.inject
    const late = target.createClient('user-1')
    const lateViews = allSpecs.map((spec, i) =>
      materializeSpec(late, spec, i, injectThis)
    )
    await eventually(
      () => {
        for (const v of lateViews)
          if (!v.complete()) throw new Error(`late spec ${v.specIndex}`)
      },
      120_000,
      `${target.name} late hydration`
    )
    for (let i = 0; i < allSpecs.length; i++) {
      const freshRows = lateViews[i]!.rows()
      const maintainedRows = views[i]!.rows()
      const fresh = canonical(freshRows)
      const maintained = canonical(maintainedRows)
      if (fresh !== maintained) {
        const file = emitDivergence({
          phase: 'incremental',
          comparisonKind: 'single-target',
          round: ROUNDS,
          specIndex: i,
          spec: allSpecs[i]!,
          observedTarget: target.name,
          leftRows: maintainedRows,
          rightRows: freshRows,
          note: `incremental==fresh divergence on ${target.name} spec ${i}: maintained view != a late fresh rematerialization`,
          minimizationComplete: false,
        })
        failures.push(
          `[incremental==fresh] ${target.name} spec ${i} (artifact ${file})\n  spec: ${JSON.stringify(allSpecs[i])}\n  maintained: ${maintained.slice(0, 300)}\n  fresh: ${fresh.slice(0, 300)}`
        )
      }
    }
    lateViews.forEach((v) => v.destroy())
  }

  const total = allSpecs.length
  const pairwise = sweepPairwiseCoverage(allSpecs)
  console.log(
    `[sweep] coverage: ${total} shapes, ${total - emptyAtHydrate}/${total} returned data at hydrate; pairwise ${pairwise.hit}/${pairwise.total} (${pairwise.percent}%)`
  )
  const weakest = [...pairwise.byAxisPair]
    .sort((a, b) => a.percent - b.percent || a.axes.join().localeCompare(b.axes.join()))
    .slice(0, 3)
    .map((entry) => `${entry.axes.join('×')} ${entry.hit}/${entry.total}`)
  console.log(`[sweep] weakest axis pairs: ${weakest.join(', ')}`)

  beacons.forEach((b) => b.destroy())
  stockViews.forEach((v) => v.destroy())
  otherViews.forEach((v) => v.destroy())
} catch (error) {
  failures.push(`fatal: ${error}`)
} finally {
  await Promise.allSettled([stock.close(), other.close()])
}

if (knownGaps.length > 0) {
  console.log(
    `[sweep] ${knownGaps.length} divergences on the known #6121 axis (upstream, not orez):`
  )
  for (const g of knownGaps) console.log(g)
}
if (failures.length > 0) {
  console.error(`[sweep] FAIL seed=${SWEEP_SEED} — ${failures.length} failures:`)
  for (const f of failures) console.error(f)
  console.error(
    `[sweep] replay: bun src/sweep.ts --seed ${SWEEP_SEED} --rounds ${ROUNDS} --queriesPerRound ${QUERIES_PER_ROUND} --against ${args.against}`
  )
  process.exit(1)
}
console.log(
  `[sweep] PASS seed=${SWEEP_SEED}: ${ROUNDS} rounds x ${QUERIES_PER_ROUND} shapes vs ${other.name}, all equal in ${Date.now() - t0}ms`
)
process.exit(0)
