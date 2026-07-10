// stock-zero cross-differential for query-aware membership: every corpus query
// materialized on a rust-local --query-aware client (server-side membership +
// correlated-subquery row sync) must equal the same query on stock zero-cache,
// which also computes membership server-side. this is the query-aware analogue
// of shapes.ts (which runs rust-local BASELINE, i.e. client-side query eval);
// here the SERVER decides membership on both sides. read-only hydrate diff.
//
//   bun src/query-diff.ts   # needs Node for stock-zero
import { parseArgs } from 'node:util'

import { canonical } from './canonical.js'
import { queries, queryCorpus } from './fixture.js'
import { startStockZero } from './targets/stock-zero.js'

import type { FixtureZero, SyncTarget } from './target.js'
import type { HttpPullObservation } from './observed-fetch.js'

const { values: args } = parseArgs({
  options: { against: { type: 'string', default: 'rust-local' } },
})
const cfObservations: HttpPullObservation[] = []
const differentialCorpus = queryCorpus

async function startRustTarget(): Promise<SyncTarget> {
  if (args.against === 'rust-local') {
    return (await import('./targets/rust-local.js')).startRustLocal({
      queryAware: true,
      pullIntervalMs: 150,
    })
  }
  if (args.against === 'rust-cf') {
    return (await import('./targets/rust-cf.js')).startRustCf({
      queryAware: true,
      pullIntervalMs: 300,
      onPull(observation) {
        cfObservations.push(observation)
        if (observation.status !== 200) {
          console.error('[query-diff] rust-cf pull error', observation)
        }
      },
    })
  }
  throw new Error(`query-diff --against must be rust-local or rust-cf`)
}

function invokeQuery(name: string, args: unknown) {
  const def = (queries as unknown as Record<string, (args?: unknown) => unknown>)[name]!
  return args === undefined ? def() : def(args)
}

type CorpusViews = Map<string, { rows: () => unknown; complete: () => boolean }>

function materializeCorpus(zero: FixtureZero): {
  views: CorpusViews
  destroy: () => void
} {
  const views: CorpusViews = new Map()
  const destroys: Array<() => void> = []
  for (const { name, args } of differentialCorpus) {
    const view = zero.materialize(invokeQuery(name, args) as never)
    let rows: unknown = null
    let complete = false
    view.addListener((data: unknown, resultType: string) => {
      rows = JSON.parse(JSON.stringify(data ?? null))
      if (resultType === 'complete') complete = true
    })
    views.set(name, { rows: () => rows, complete: () => complete })
    destroys.push(() => view.destroy())
  }
  return { views, destroy: () => destroys.forEach((d) => d()) }
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

const t0 = Date.now()
console.log(`[query-diff] booting stock-zero and ${args.against} --query-aware...`)
const [stock, rust] = await Promise.all([startStockZero(), startRustTarget()])
const targets: SyncTarget[] = [stock, rust]

let failed = false
try {
  const stockViews = materializeCorpus(stock.createClient('user-1'))
  const rustViews = materializeCorpus(rust.createClient('user-1'))

  await eventually(
    () => {
      for (const { name } of differentialCorpus) {
        if (!stockViews.views.get(name)!.complete())
          throw new Error(`stock-zero ${name} incomplete`)
        if (!rustViews.views.get(name)!.complete())
          throw new Error(`${args.against} ${name} incomplete`)
      }
    },
    90_000,
    'both targets complete on every corpus query'
  )

  const failures: string[] = []
  for (const { name } of differentialCorpus) {
    const left = canonical(stockViews.views.get(name)!.rows())
    const right = canonical(rustViews.views.get(name)!.rows())
    if (left !== right) {
      failures.push(
        `${name} diverged:\n  stock-zero: ${left?.slice(0, 400)}\n  ${args.against}(qa): ${right?.slice(0, 400)}`
      )
    }
  }

  const empty = differentialCorpus.filter(({ name }) => {
    const rows = stockViews.views.get(name)!.rows()
    return rows == null || (Array.isArray(rows) && rows.length === 0)
  })

  if (failures.length > 0) {
    for (const failure of failures) console.error('[query-diff] DIVERGENCE:', failure)
    throw new Error(`${failures.length}/${differentialCorpus.length} corpus queries diverged`)
  }

  stockViews.destroy()
  rustViews.destroy()
  console.log(
    `[query-diff] PASS: ${differentialCorpus.length} supported corpus queries equal ` +
      `(${differentialCorpus.length - empty.length} return data) ` +
      `stock-zero == ${args.against} --query-aware in ${Date.now() - t0}ms`
  )
} catch (error) {
  failed = true
  console.error('[query-diff] FAIL:', error)
  if (args.against === 'rust-cf') {
    console.error('[query-diff] first rust-cf pulls:', cfObservations.slice(0, 5))
    console.error('[query-diff] last rust-cf pulls:', cfObservations.slice(-5))
  }
} finally {
  for (const target of targets) await target.close()
}

process.exit(failed ? 1 : 0)
