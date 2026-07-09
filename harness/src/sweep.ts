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
// deterministic per seed: a divergence prints the seed + writes a replay
// artifact to regressions/. re-run any run with --seed.
//
//   bun src/sweep.ts                                  # random seed, orez-local
//   bun src/sweep.ts --seed 12345 --rounds 20
//   bun src/sweep.ts --against orez-cf --rounds 8
import { mkdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
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
import { startStockZero } from './targets/stock-zero.js'

import type { FixtureZero, SyncTarget } from './target.js'

const { values: args } = parseArgs({
  options: {
    against: { type: 'string', default: 'orez-local' },
    seed: { type: 'string', default: String(Math.floor(Math.random() * 2 ** 31)) },
    rounds: { type: 'string', default: '12' },
    queriesPerRound: { type: 'string', default: '4' },
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

function genSub(to: TableName, kind: 'one' | 'many'): GenSubSpec | undefined {
  if (kind === 'one') return { one: true }
  if (chance(0.3)) return undefined
  const sub: GenSubSpec = {}
  if (chance(0.5)) sub.where = genWhere(to, 1)
  sub.orderBy = genOrderBy(to)
  if (chance(0.5)) sub.limit = int(1, 4)
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
  if (chance(0.4)) spec.limit = int(1, 8)
  if (rels.length > 0 && chance(0.6)) {
    const count = chance(0.3) && rels.length > 1 ? 2 : 1
    const chosen = [...rels].sort(() => rng() - 0.5).slice(0, count)
    spec.related = chosen.map((r) => ({ rel: r.rel, sub: genSub(r.to, r.kind) }))
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
        make: (z) =>
          z.mutate(mutators.project.create({ id, ownerId, name: `sweep ${id}` })),
      })
    } else if (roll < 0.4) {
      const id = `wt-${round}-${writeSeq++}`
      pools.task.push(id)
      const projectId = pick(pools.project)
      const rank = Math.round((rng() * 24 - 4) * 100) / 100
      const done = chance(0.4)
      const meta = chance(0.5) ? { round, tag: pick(['a', 'b', '✅']) } : undefined
      const dueAt = chance(0.6)
        ? 1750000000000 + Math.floor(rng() * 10_000_000_000)
        : undefined
      writes.push({
        kind: 'mutate',
        label: `task.create ${id}`,
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
      writes.push({
        kind: 'mutate',
        label: `task.toggle ${id}`,
        make: (z) => z.mutate(mutators.task.toggle({ id })),
      })
    } else if (roll < 0.7) {
      const id = pick(pools.task)
      const rank = chance(0.3) ? int(90, 110) : Math.round((rng() * 24 - 4) * 100) / 100
      writes.push({
        kind: 'mutate',
        label: `task.setRank ${id} ${rank}`,
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
        make: (z) => z.mutate(mutators.member.add({ id, projectId, userId })),
      })
    } else if (roll < 0.87 && pools.member.length > 3) {
      const idx = int(0, pools.member.length - 1)
      const id = pools.member.splice(idx, 1)[0]!
      writes.push({
        kind: 'mutate',
        label: `member.remove ${id}`,
        make: (z) => z.mutate(mutators.member.remove({ id })),
      })
    } else if (roll < 0.94 && pools.task.length > 10) {
      const idx = int(0, pools.task.length - 1)
      const id = pools.task.splice(idx, 1)[0]!
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

function materializeSpec(zero: FixtureZero, spec: GenSpec, specIndex: number): LiveView {
  const view = zero.materialize(queries.generated(spec) as never)
  let rows: unknown = null
  let complete = false
  view.addListener((data: unknown, resultType: string) => {
    rows = JSON.parse(JSON.stringify(data ?? null))
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

const REGRESSIONS_DIR = join(import.meta.dirname, '..', 'regressions')

function recordDivergence(entry: {
  phase: string
  round: number
  specIndex: number
  spec: GenSpec
  left: string
  right: string
}) {
  mkdirSync(REGRESSIONS_DIR, { recursive: true })
  const file = join(
    REGRESSIONS_DIR,
    `sweep-seed${SWEEP_SEED}-r${entry.round}-q${entry.specIndex}.json`
  )
  writeFileSync(
    file,
    JSON.stringify(
      {
        seed: SWEEP_SEED,
        rounds: ROUNDS,
        queriesPerRound: QUERIES_PER_ROUND,
        against: args.against,
        replay: `bun src/sweep.ts --seed ${SWEEP_SEED} --rounds ${ROUNDS} --queriesPerRound ${QUERIES_PER_ROUND} --against ${args.against}`,
        ...entry,
      },
      null,
      2
    )
  )
  return file
}

// ---------------------------------------------------------------------------
// run
// ---------------------------------------------------------------------------

const t0 = Date.now()
console.log(
  `[sweep] seed=${SWEEP_SEED} rounds=${ROUNDS} queries/round=${QUERIES_PER_ROUND} against=${args.against}`
)
const [stock, other] = await Promise.all([startStockZero(), startAgainst(args.against!)])

const failures: string[] = []
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

  const compareAll = (phase: string, round: number) => {
    let diverged = 0
    for (let i = 0; i < stockViews.length; i++) {
      const left = canonical(stockViews[i]!.rows())
      const right = canonical(otherViews[i]!.rows())
      if (left !== right) {
        diverged++
        const file = recordDivergence({
          phase,
          round,
          specIndex: i,
          spec: allSpecs[i]!,
          left: left.slice(0, 2000),
          right: right.slice(0, 2000),
        })
        failures.push(
          `[${phase} r${round}] spec ${i} diverged (artifact ${file})\n  spec: ${JSON.stringify(allSpecs[i])}\n  stock-zero: ${left.slice(0, 300)}\n  ${other.name}: ${right.slice(0, 300)}`
        )
      }
    }
    return diverged
  }

  for (let round = 0; round < ROUNDS; round++) {
    // new shapes this round
    for (let k = 0; k < QUERIES_PER_ROUND; k++) {
      const spec = genSpec()
      const specIndex = allSpecs.length
      allSpecs.push(spec)
      stockViews.push(materializeSpec(stockZero, spec, specIndex))
      otherViews.push(materializeSpec(otherZero, spec, specIndex))
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

    // mirrored random writes through each target's own client + upstream sql
    const writes = genWrites(round)
    const acks: Promise<unknown>[] = []
    for (const write of writes) {
      if (write.kind === 'mutate') {
        for (const zero of [stockZero, otherZero]) {
          const req = write.make(zero)
          acks.push(req.server)
          await withTimeout(req.client, 15_000, `client apply: ${write.label}`)
        }
      } else {
        await stock.sql(write.sql)
        await other.sql(write.sql)
      }
    }
    await withTimeout(Promise.allSettled(acks), 30_000, `round ${round} server acks`)

    // sentinel barrier: both targets must see this round's marker project
    const sentinel = `sentinel-${SWEEP_SEED}-${round}`
    pools.project.push(sentinel)
    for (const zero of [stockZero, otherZero]) {
      const req = zero.mutate(
        mutators.project.create({ id: sentinel, ownerId: 'u0', name: sentinel })
      )
      await withTimeout(req.server, 30_000, `sentinel ${sentinel} server ack`)
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

  // incremental == fresh: late clients rematerialize EVERY spec from scratch
  const lateChecks = [
    { target: stock, views: stockViews },
    { target: other, views: otherViews },
  ]
  for (const { target, views } of lateChecks) {
    const late = target.createClient('user-1')
    const lateViews = allSpecs.map((spec, i) => materializeSpec(late, spec, i))
    await eventually(
      () => {
        for (const v of lateViews)
          if (!v.complete()) throw new Error(`late spec ${v.specIndex}`)
      },
      120_000,
      `${target.name} late hydration`
    )
    for (let i = 0; i < allSpecs.length; i++) {
      const fresh = canonical(lateViews[i]!.rows())
      const maintained = canonical(views[i]!.rows())
      if (fresh !== maintained) {
        const file = recordDivergence({
          phase: `incremental==fresh:${target.name}`,
          round: ROUNDS,
          specIndex: i,
          spec: allSpecs[i]!,
          left: maintained.slice(0, 2000),
          right: fresh.slice(0, 2000),
        })
        failures.push(
          `[incremental==fresh] ${target.name} spec ${i} (artifact ${file})\n  spec: ${JSON.stringify(allSpecs[i])}\n  maintained: ${maintained.slice(0, 300)}\n  fresh: ${fresh.slice(0, 300)}`
        )
      }
    }
    lateViews.forEach((v) => v.destroy())
  }

  const total = allSpecs.length
  console.log(
    `[sweep] coverage: ${total} shapes, ${total - emptyAtHydrate}/${total} returned data at hydrate`
  )

  beacons.forEach((b) => b.destroy())
  stockViews.forEach((v) => v.destroy())
  otherViews.forEach((v) => v.destroy())
} catch (error) {
  failures.push(`fatal: ${error}`)
} finally {
  await Promise.allSettled([stock.close(), other.close()])
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
