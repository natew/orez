// M4 load lane: N clients hydrate, W writers push custom mutations at a
// target rate for a duration, every row's end-to-end propagation lag is
// measured (issue -> seen by ALL clients), then the run quiesces and the
// full cross-client + oracle correctness checks run. correctness under load
// is the point; the numbers are the scaling curve.
//
//   bun src/bench.ts --target orez-local --clients 20 --writers 5 --rate 10 --duration 15
import { parseArgs } from 'node:util'
import { mutators, queries } from './fixture.js'
import type { FixtureZero, SyncTarget } from './target.js'
import { startStockZero } from './targets/stock-zero.js'

const { values: args } = parseArgs({
  options: {
    target: { type: 'string', default: 'stock-zero' },
    clients: { type: 'string', default: '20' },
    writers: { type: 'string', default: '5' },
    rate: { type: 'string', default: '10' }, // mutations/sec per writer
    duration: { type: 'string', default: '15' }, // seconds
    label: { type: 'string', default: '' },
  },
})

const CLIENTS = Number(args.clients)
const WRITERS = Math.min(Number(args.writers), CLIENTS)
const RATE = Number(args.rate)
const DURATION_S = Number(args.duration)

async function startTarget(name: string): Promise<SyncTarget> {
  if (name === 'stock-zero') return startStockZero()
  if (name === 'orez-local') return (await import('./targets/orez-local.js')).startOrezLocal()
  throw new Error(`unknown target '${name}'`)
}

function percentile(sorted: number[], p: number) {
  if (sorted.length === 0) return 0
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))]!
}

function watchFirstSeen(zero: FixtureZero, firstSeen: Map<string, number>[]) {
  const seen = new Map<string, number>()
  firstSeen.push(seen)
  const view = zero.materialize(queries.allProjects())
  let complete = false
  view.addListener((data, resultType) => {
    const now = Date.now()
    for (const row of data as readonly { id: string }[]) {
      if (!seen.has(row.id)) seen.set(row.id, now)
    }
    if (resultType === 'complete') complete = true
  })
  return {
    get complete() {
      return complete
    },
    get count() {
      return seen.size
    },
    destroy: () => view.destroy(),
  }
}

async function eventually(check: () => void, timeoutMs: number, label: string) {
  const start = Date.now()
  let lastError: unknown
  while (Date.now() - start < timeoutMs) {
    try {
      check()
      return Date.now() - start
    } catch (error) {
      lastError = error
      await new Promise((r) => setTimeout(r, 100))
    }
  }
  throw new Error(`timeout (${timeoutMs}ms) waiting for ${label}: ${lastError}`)
}

const t0 = Date.now()
const target = await startTarget(args.target!)

let failed = false
try {
  // hydrate
  const firstSeen: Map<string, number>[] = []
  const zeros: FixtureZero[] = []
  const watchers: ReturnType<typeof watchFirstSeen>[] = []
  const tH0 = Date.now()
  for (let i = 0; i < CLIENTS; i++) {
    const zero = target.createClient(`user-${i}`)
    zeros.push(zero)
    watchers.push(watchFirstSeen(zero, firstSeen))
  }
  const hydrateMs = await eventually(
    () => {
      for (const w of watchers) if (!w.complete) throw new Error('not complete')
    },
    60_000,
    'hydration'
  )

  // write phase
  const issuedAt = new Map<string, number>()
  const ackLatencies: number[] = []
  const serverAcks: Promise<unknown>[] = []
  const intervalMs = 1000 / RATE
  const tW0 = Date.now()
  let mutationErrors = 0
  await Promise.all(
    Array.from({ length: WRITERS }, async (_, w) => {
      let seq = 0
      while (Date.now() - tW0 < DURATION_S * 1000) {
        const id = `p-${w}-${seq++}`
        const issued = Date.now()
        issuedAt.set(id, issued)
        const req = zeros[w]!.mutate(
          mutators.project.create({ id, ownerId: `user-${w}`, name: `bench ${id}` })
        )
        serverAcks.push(
          req.server.then(
            () => ackLatencies.push(Date.now() - issued),
            () => mutationErrors++
          )
        )
        await req.client
        const elapsed = Date.now() - issued
        if (elapsed < intervalMs) await new Promise((r) => setTimeout(r, intervalMs - elapsed))
      }
    })
  )
  await Promise.all(serverAcks)
  const writeWallMs = Date.now() - tW0
  const written = issuedAt.size

  // convergence: every client sees every written row
  const convergeMs = await eventually(
    () => {
      for (const seen of firstSeen) {
        for (const id of issuedAt.keys()) {
          if (!seen.has(id)) throw new Error(`row ${id} not yet on all clients`)
        }
      }
    },
    120_000,
    'post-write convergence'
  )

  // propagation lag: issue -> seen by ALL clients
  const lags: number[] = []
  for (const [id, issued] of issuedAt) {
    let latest = 0
    for (const seen of firstSeen) latest = Math.max(latest, seen.get(id) ?? 0)
    lags.push(latest - issued)
  }
  lags.sort((a, b) => a - b)
  ackLatencies.sort((a, b) => a - b)

  // correctness: oracle compare on a sample client + fresh late joiner
  const oracleCount = Number(
    (await target.oracle(`SELECT count(*) AS n FROM project`))[0]!.n
  )
  if (watchers[0]!.count !== oracleCount) {
    throw new Error(`client 0 sees ${watchers[0]!.count} projects, oracle has ${oracleCount}`)
  }
  const late = target.createClient('late-bench')
  const lateWatch = watchFirstSeen(late, [])
  const lateHydrateMs = await eventually(
    () => {
      if (!lateWatch.complete || lateWatch.count !== oracleCount) {
        throw new Error(`late client at ${lateWatch.count}/${oracleCount}`)
      }
    },
    60_000,
    'late hydration'
  )
  lateWatch.destroy()

  const metrics = await target.metrics()
  const result = {
    target: target.name,
    label: args.label || undefined,
    clients: CLIENTS,
    writers: WRITERS,
    ratePerWriter: RATE,
    durationS: DURATION_S,
    written,
    mutationErrors,
    hydrateMs,
    writeWallMs,
    ackP50: percentile(ackLatencies, 50),
    ackP95: percentile(ackLatencies, 95),
    ackP99: percentile(ackLatencies, 99),
    propagationP50: percentile(lags, 50),
    propagationP95: percentile(lags, 95),
    propagationP99: percentile(lags, 99),
    convergeMs,
    lateHydrateMs,
    oracleProjects: oracleCount,
    serverRssMb: metrics.serverRssMb,
    totalMs: Date.now() - t0,
  }
  console.log(`[bench] ${JSON.stringify(result)}`)
  console.log(
    `[bench] PASS ${target.name}: ${CLIENTS} clients, ${written} writes @ ${WRITERS}x${RATE}/s — ` +
      `ack p50/p95 ${result.ackP50}/${result.ackP95}ms, propagation p50/p95 ${result.propagationP50}/${result.propagationP95}ms, ` +
      `late hydrate ${lateHydrateMs}ms`
  )
  for (const w of watchers) w.destroy()
} catch (error) {
  failed = true
  console.error(`[bench] FAIL:`, error)
} finally {
  await target.close()
}

process.exit(failed ? 1 : 0)
