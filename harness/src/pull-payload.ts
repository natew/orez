// pull payload lane: how many bytes a cold client is sent for ONE registered
// query, measured through a real @rocicorp/zero client against each host.
//
// each rust host resolves the client's desired queries and sends their
// membership. the lane keeps query shape and data fixed so payload differences
// reflect the host runtime rather than different sync contracts.
//
// It reports the first pull (the cache miss a fresh install pays) and the
// steady-state pull that follows, broken down by table, so a table nothing
// queried is visible as its own line rather than buried in a total.
//
//   bun src/pull-payload.ts --against rust-local
//   bun src/pull-payload.ts --against rust-cf
import { parseArgs } from 'node:util'

import { queries } from './fixture.js'

import type { FixtureZero, SyncTarget } from './target.js'

const { values: args } = parseArgs({
  options: {
    against: { type: 'string', default: 'rust-local' },
    json: { type: 'boolean', default: false },
  },
})

type PullSample = {
  bytes: number
  patches: number
  unchanged: boolean
  byTable: Record<string, number>
}

const samples: PullSample[] = []

function record(response: unknown): void {
  if (!response || typeof response !== 'object') return
  const body = response as Record<string, unknown>
  const patch = Array.isArray(body.rowsPatch) ? body.rowsPatch : []
  const byTable: Record<string, number> = {}
  for (const entry of patch) {
    if (!entry || typeof entry !== 'object') continue
    const table = (entry as { tableName?: unknown }).tableName
    if (typeof table !== 'string') continue
    byTable[table] = (byTable[table] ?? 0) + 1
  }
  samples.push({
    bytes: JSON.stringify(response).length,
    patches: patch.length,
    unchanged: body.unchanged === true,
    byTable,
  })
}

async function startTarget(): Promise<SyncTarget> {
  if (args.against === 'rust-local') {
    return (await import('./targets/rust-local.js')).startRustLocal({
      pullIntervalMs: 100,
      onPull: (observation) => record(observation.response),
    })
  }
  if (args.against === 'rust-cf') {
    return (await import('./targets/rust-cf.js')).startRustCf({
      pullIntervalMs: 300,
      onPull: (observation) => record(observation.response),
    })
  }
  throw new Error(
    `pull-payload --against must be rust-local or rust-cf (got '${args.against}')`
  )
}

function materialize(zero: FixtureZero, query: unknown) {
  const view = zero.materialize(query as never)
  let rows: { id: string }[] = []
  let complete = false
  view.addListener((data: unknown, resultType: string) => {
    rows = JSON.parse(JSON.stringify(data)) as { id: string }[]
    if (resultType === 'complete') complete = true
  })
  return {
    get complete() {
      return complete
    },
    count: () => rows.length,
    destroy: () => view.destroy(),
  }
}

async function eventually(check: () => void, label: string, timeoutMs = 30_000) {
  const start = Date.now()
  let lastError: unknown
  while (Date.now() - start < timeoutMs) {
    try {
      check()
      return
    } catch (error) {
      lastError = error
      await new Promise((resolve) => setTimeout(resolve, 25))
    }
  }
  throw new Error(`timeout waiting for ${label}: ${String(lastError)}`)
}

const target = await startTarget()
let failed = false
try {
  // one narrow query, the shape a screen actually registers: the tasks of a
  // single project. every other row the host sends is a row nothing asked for.
  const zero = target.createClient('u0')
  const view = materialize(zero, queries.tasksInProjects({ projectIds: ['p1'] }))
  await eventually(() => {
    if (!view.complete) throw new Error('view not complete')
  }, 'registered query completes')

  const wanted = await target.oracle(`SELECT id FROM task WHERE "projectId" = 'p1'`)
  if (wanted.length === 0) throw new Error('fixture has no p1 tasks to query')
  if (view.count() !== wanted.length) {
    throw new Error(
      `view holds ${view.count()} rows, oracle says ${wanted.length} — the lane is not measuring a settled view`
    )
  }

  // let the poll produce at least one steady-state pull after the cold one
  const coldSamples = samples.length
  await eventually(() => {
    if (samples.length <= coldSamples) throw new Error('no follow-up pull yet')
  }, 'steady-state pull')

  const cold = samples.reduce((largest, sample) =>
    sample.bytes > largest.bytes ? sample : largest
  )
  const warm = samples.filter((sample) => sample.unchanged)
  const wantedTables = Object.entries(cold.byTable)
  const extra = wantedTables
    .filter(([table]) => table !== 'task')
    .reduce((total, [, count]) => total + count, 0)

  const report = {
    target: args.against,
    queryRows: wanted.length,
    coldPullBytes: cold.bytes,
    coldPullPatches: cold.patches,
    coldPullByTable: cold.byTable,
    rowsBeyondTheQueriedTable: extra,
    warmPulls: warm.length,
    warmPullBytes: warm[0]?.bytes ?? null,
    totalPulls: samples.length,
  }
  if (args.json) console.log(JSON.stringify(report))
  else {
    console.log(`pull payload against ${args.against}`)
    console.log(`  registered query matches ${wanted.length} task rows`)
    console.log(`  cold pull: ${cold.bytes} bytes, ${cold.patches} row patches`)
    for (const [table, count] of wantedTables.sort((a, b) => b[1] - a[1])) {
      console.log(`    ${table}: ${count} rows`)
    }
    console.log(`  rows outside the queried table: ${extra}`)
    console.log(
      `  warm unchanged pull: ${warm[0]?.bytes ?? 'none observed'} bytes (${warm.length} of ${samples.length} pulls)`
    )
  }
  view.destroy()
} catch (error) {
  failed = true
  console.error(`pull-payload failed: ${String(error)}`)
} finally {
  await target.close()
}
process.exit(failed ? 1 : 0)
