// Portable behavioral scenarios adapted from the pinned upstream corpus.
// This intentionally compares public observations (query results and durable
// rows), not any upstream implementation's private CVR/CDC representation.
import { parseArgs } from 'node:util'

import { canonical } from './canonical.js'
import { mutators, queries } from './fixture.js'
import { assertServerOutcome } from './server-outcome.js'

import type { FixtureZero, SyncTarget } from './target.js'

type HostID = 'typescript-oracle' | 'stock-zero' | 'sync-native' | 'rust-cf'
type Row = { id: string }

const ALL_HOSTS: HostID[] = ['typescript-oracle', 'stock-zero', 'sync-native', 'rust-cf']

const { values: args } = parseArgs({
  options: {
    hosts: { type: 'string', default: ALL_HOSTS.join(',') },
  },
})

const hosts = args.hosts!.split(',').map((host) => host.trim()) as HostID[]
for (const host of hosts) {
  if (!ALL_HOSTS.includes(host)) throw new Error(`unknown corpus host '${host}'`)
}

async function start(host: HostID): Promise<SyncTarget> {
  switch (host) {
    case 'typescript-oracle':
      return (await import('./targets/orez-local.js')).startOrezLocal({
        pullIntervalMs: 75,
      })
    case 'stock-zero':
      return (await import('./targets/stock-zero.js')).startStockZero()
    case 'sync-native':
      return (await import('./targets/rust-local.js')).startRustLocal({
        pullIntervalMs: 75,
        queryAware: true,
      })
    case 'rust-cf':
      return (await import('./targets/rust-cf.js')).startRustCf({
        pullIntervalMs: 150,
        queryAware: true,
      })
  }
}

function watch(zero: FixtureZero, projectIDs: string[]) {
  const view = zero.materialize(queries.tasksInProjects({ projectIds: projectIDs }), {
    ttl: 0,
  })
  let rows: Row[] = []
  let complete = false
  let destroyed = false
  view.addListener((data, resultType) => {
    rows = JSON.parse(JSON.stringify(data)) as Row[]
    if (resultType === 'complete') complete = true
  })
  return {
    snapshot: () => ({ complete, ids: rows.map(({ id }) => id).sort() }),
    destroy: () => {
      if (destroyed) return
      destroyed = true
      view.destroy()
    },
  }
}

async function eventually(
  check: () => void | Promise<void>,
  label: string,
  timeoutMs = 60_000
) {
  const started = Date.now()
  let lastError: unknown
  while (Date.now() - started < timeoutMs) {
    try {
      await check()
      return
    } catch (error) {
      lastError = error
      await new Promise((resolve) => setTimeout(resolve, 25))
    }
  }
  throw new Error(`timeout waiting for ${label}: ${String(lastError)}`)
}

async function oracleIDs(target: SyncTarget, projectIDs: string[]) {
  const quoted = projectIDs.map((id) => `'${id}'`).join(',')
  const rows = (await target.oracle(
    `SELECT id FROM task WHERE "projectId" IN (${quoted}) ORDER BY id`
  )) as Row[]
  return rows.map(({ id }) => id).sort()
}

async function requireOracle(
  target: SyncTarget,
  view: ReturnType<typeof watch>,
  projectIDs: string[],
  label: string
) {
  await eventually(async () => {
    const got = view.snapshot()
    if (!got.complete) throw new Error('view incomplete')
    const want = await oracleIDs(target, projectIDs)
    if (canonical(got.ids) !== canonical(want))
      throw new Error(`got ${canonical(got.ids)}, want ${canonical(want)}`)
  }, label)
  return view.snapshot().ids
}

type Observation = { scenario: string; phase?: string; value: unknown }

async function run(host: HostID): Promise<Observation[]> {
  const target = await start(host)
  const observations: Observation[] = []
  let first: FixtureZero | undefined
  let late: FixtureZero | undefined
  let reopened: FixtureZero | undefined
  const views: { destroy(): void }[] = []
  try {
    first = target.createClient('corpus-user', { storageKey: 'corpus-first' })

    const p0 = watch(first, ['p0'])
    views.push(p0)
    observations.push({
      scenario: 'zero.view-syncer.initial-hydration',
      value: await requireOracle(target, p0, ['p0'], `${host} initial hydration`),
    })

    const p1 = watch(first, ['p1'])
    views.push(p1)
    observations.push({
      scenario: 'zero.view-syncer.desired-query-change',
      phase: 'add',
      value: await requireOracle(target, p1, ['p1'], `${host} add desired query`),
    })

    const request = first.mutate(
      mutators.task.create({
        id: 'corpus-catch-up-task',
        projectId: 'p0',
        title: 'portable corpus catch-up',
        rank: 7.25,
        done: false,
        meta: { source: 'upstream-corpus' },
      })
    )
    await request.client
    await assertServerOutcome(request.server, 'success', `${host} corpus write`)
    observations.push({
      scenario: 'zero.view-syncer.catch-up-client',
      phase: 'maintained',
      value: await requireOracle(target, p0, ['p0'], `${host} maintained catch-up`),
    })

    late = target.createClient('corpus-user', { storageKey: 'corpus-late' })
    const lateP0 = watch(late, ['p0'])
    views.push(lateP0)
    observations.push({
      scenario: 'zero.view-syncer.catch-up-client',
      phase: 'fresh',
      value: await requireOracle(target, lateP0, ['p0'], `${host} fresh catch-up`),
    })

    p0.destroy()
    observations.push({
      scenario: 'zero.view-syncer.desired-query-change',
      phase: 'remove',
      value: await requireOracle(target, p1, ['p1'], `${host} surviving desired query`),
    })

    await first.close()
    first = undefined
    reopened = target.createClient('corpus-user', { storageKey: 'corpus-reopened' })
    const reopenedP1 = watch(reopened, ['p1'])
    views.push(reopenedP1)
    observations.push({
      scenario: 'zero.view-syncer.client-reconnect',
      value: await requireOracle(target, reopenedP1, ['p1'], `${host} reconnect`),
    })

    return observations
  } finally {
    for (const view of views) view.destroy()
    await Promise.allSettled(
      [first, late, reopened]
        .filter((client): client is FixtureZero => !!client)
        .map((client) => client.close())
    )
    await target.close()
  }
}

let baseline: string | undefined
for (const host of hosts) {
  console.log(`[upstream-corpus] START ${host}`)
  const observations = await run(host)
  const encoded = canonical(observations)
  if (baseline === undefined) baseline = encoded
  else if (encoded !== baseline) {
    throw new Error(
      `${host} observable trace diverged\nfirst host: ${baseline}\n${host}: ${encoded}`
    )
  }
  console.log(`[upstream-corpus] PASS ${host}: ${observations.length} observations`)
}

console.log(
  `[upstream-corpus] PASS: one portable trace matched across ${hosts.join(', ')}`
)
