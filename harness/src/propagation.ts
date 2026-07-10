// propagation lane: measures cross-client wake latency on the native host
// (rust-local) differentially against stock zero-cache's websocket push.
//
// the wake target runs with a deliberately LARGE safety-poll interval, so any
// sub-second convergence PROVES the wake channel drove it, not the poll (the
// plan's "no lane converges via the safety poll"). the gate is native wake
// propagation p95 < 100 ms; stock zero-cache's websocket push is the baseline
// the differential reports against.
//
//   bun src/propagation.ts                       # rust-local vs stock-zero
//   bun src/propagation.ts --baseline none       # wake gate only (no stock)
//   bun src/propagation.ts --clients 20 --writes 30
import { parseArgs } from 'node:util'

import { mutators, queries } from './fixture.js'
import { startStockZero } from './targets/stock-zero.js'

import type { FixtureZero, SyncTarget } from './target.js'

const { values: args } = parseArgs({
  options: {
    against: { type: 'string', default: 'rust-local' },
    baseline: { type: 'string', default: 'stock-zero' },
    clients: { type: 'string', default: '10' },
    writes: { type: 'string', default: '20' },
    'safety-poll-ms': { type: 'string', default: '10000' },
    'spacing-ms': { type: 'string', default: '150' },
  },
})

const CLIENTS = Number(args.clients)
const WRITES = Number(args.writes)
const SAFETY_POLL_MS = Number(args['safety-poll-ms'])
const SPACING_MS = Number(args['spacing-ms'])
const GATE_P95_MS = 100

function percentile(sorted: number[], p: number) {
  if (sorted.length === 0) return 0
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))]!
}

// first-seen timestamp per project id, for every reader
function watchFirstSeen(zero: FixtureZero) {
  const seen = new Map<string, number>()
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
    seenAt: (id: string) => seen.get(id),
    destroy: () => view.destroy(),
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
      await new Promise((r) => setTimeout(r, 20))
    }
  }
  throw new Error(`timeout (${timeoutMs}ms) waiting for ${label}: ${lastError}`)
}

type Measurement = {
  label: string
  latencies: number[]
  p50: number
  p95: number
  p99: number
  max: number
  readers: number
  writes: number
}

// one writer creates WRITES unique projects, spaced out; every reader records
// when it first sees each id. latency = seen - issued, across all reader/id
// pairs. a large spacing keeps writes from overlapping so each measures a
// clean single-write propagation.
async function measure(target: SyncTarget, label: string): Promise<Measurement> {
  const writer = target.createClient('prop-writer')
  const readers: FixtureZero[] = []
  const watchers: ReturnType<typeof watchFirstSeen>[] = []
  for (let i = 0; i < CLIENTS; i++) {
    const reader = target.createClient(`prop-reader-${i}`)
    readers.push(reader)
    watchers.push(watchFirstSeen(reader))
  }

  await eventually(
    () => {
      for (const w of watchers) if (!w.complete) throw new Error('reader not complete')
    },
    60_000,
    `${label} hydration`
  )

  const issuedAt = new Map<string, number>()
  const prefix = `prop-${label}-${Date.now().toString(36)}`
  for (let i = 0; i < WRITES; i++) {
    const id = `${prefix}-${i}`
    issuedAt.set(id, Date.now())
    const request = writer.mutate(
      mutators.project.create({ id, ownerId: 'prop-writer', name: `propagation ${i}` })
    )
    await request.client
    await new Promise((r) => setTimeout(r, SPACING_MS))
  }

  // wait until every reader has seen every id, then compute latencies
  const ids = [...issuedAt.keys()]
  await eventually(
    () => {
      for (const w of watchers) {
        for (const id of ids)
          if (w.seenAt(id) === undefined) throw new Error(`missing ${id}`)
      }
    },
    Math.max(30_000, SAFETY_POLL_MS + 10_000),
    `${label} full propagation`
  )

  const latencies: number[] = []
  for (const w of watchers) {
    for (const id of ids) {
      latencies.push(Math.max(0, w.seenAt(id)! - issuedAt.get(id)!))
    }
  }
  latencies.sort((a, b) => a - b)

  for (const w of watchers) w.destroy()

  return {
    label,
    latencies,
    p50: percentile(latencies, 50),
    p95: percentile(latencies, 95),
    p99: percentile(latencies, 99),
    max: latencies[latencies.length - 1] ?? 0,
    readers: CLIENTS,
    writes: WRITES,
  }
}

async function startWakeTarget(name: string): Promise<SyncTarget> {
  if (name === 'rust-local') {
    return (await import('./targets/rust-local.js')).startRustLocal({
      pullIntervalMs: SAFETY_POLL_MS,
    })
  }
  if (name === 'rust-cf') {
    return (await import('./targets/rust-cf.js')).startRustCf({
      pullIntervalMs: SAFETY_POLL_MS,
    })
  }
  throw new Error(`propagation --against must be rust-local or rust-cf, got '${name}'`)
}

const t0 = Date.now()
console.log(
  `[propagation] against=${args.against} baseline=${args.baseline} ` +
    `clients=${CLIENTS} writes=${WRITES} safetyPoll=${SAFETY_POLL_MS}ms`
)

let failed = false
const targets: SyncTarget[] = []
try {
  const wake = await startWakeTarget(args.against!)
  targets.push(wake)
  const wakeResult = await measure(wake, args.against!)
  console.log(
    `[propagation] ${wakeResult.label} wake latency (ms): ` +
      `p50=${wakeResult.p50} p95=${wakeResult.p95} p99=${wakeResult.p99} max=${wakeResult.max} ` +
      `(${wakeResult.readers} readers x ${wakeResult.writes} writes)`
  )

  // wake-driven proof: with a large safety poll, sub-poll convergence can only
  // come from the wake channel. the max latency must sit well under the poll.
  if (wakeResult.max >= SAFETY_POLL_MS / 2) {
    throw new Error(
      `converged via the safety poll: max latency ${wakeResult.max}ms is not far below ` +
        `the ${SAFETY_POLL_MS}ms poll — the wake channel did not drive convergence`
    )
  }
  if (wakeResult.p95 >= GATE_P95_MS) {
    throw new Error(
      `wake propagation p95 ${wakeResult.p95}ms exceeds the ${GATE_P95_MS}ms gate`
    )
  }

  if (args.baseline !== 'none') {
    const baseline = await startStockZero()
    targets.push(baseline)
    const baseResult = await measure(baseline, args.baseline!)
    console.log(
      `[propagation] ${baseResult.label} websocket latency (ms): ` +
        `p50=${baseResult.p50} p95=${baseResult.p95} p99=${baseResult.p99} max=${baseResult.max}`
    )
    console.log(
      `[propagation] differential: ${wakeResult.label} wake p95 ${wakeResult.p95}ms vs ` +
        `${baseResult.label} websocket p95 ${baseResult.p95}ms ` +
        `(delta ${wakeResult.p95 - baseResult.p95}ms)`
    )
  }

  console.log(
    `[propagation] PASS ${args.against}: wake-driven, p95 ${wakeResult.p95}ms < ${GATE_P95_MS}ms, ` +
      `no safety-poll convergence (total ${Date.now() - t0}ms)`
  )
} catch (error) {
  failed = true
  console.error('[propagation] FAIL:', error)
} finally {
  for (const target of targets) await target.close()
}

process.exit(failed ? 1 : 0)
