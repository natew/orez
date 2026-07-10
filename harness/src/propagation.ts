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
import { assertServerOutcome } from './server-outcome.js'
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
// wake propagation budget: native localhost is 100ms; the CF host over WAN is
// the plan's storm-load budget of one second.
const GATE_P95_MS = args.against === 'rust-cf' ? 1000 : 100

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
  // commit -> seen: pure cross-client wake propagation (the gated metric)
  wakeP50: number
  wakeP95: number
  wakeP99: number
  // issue -> seen: user-perceived single-write latency (isolated, no backlog)
  fullP95: number
  fullMax: number
  readers: number
  writes: number
}

// one writer creates WRITES unique projects, one fully-committed at a time;
// every reader records when it first sees each id. we measure two things per
// reader/id pair: wake latency (server-commit -> seen, the pure cross-client
// propagation) and full latency (issue -> seen). the writer AWAITS each
// server ack before the next mutation, so a single writer never queues
// serialized pushes — otherwise issued->seen is dominated by push backlog
// (pronounced over WAN), not wake propagation.
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
  const committedAt = new Map<string, number>()
  const prefix = `prop-${label}-${Date.now().toString(36)}`
  for (let i = 0; i < WRITES; i++) {
    const id = `${prefix}-${i}`
    issuedAt.set(id, Date.now())
    const request = writer.mutate(
      mutators.project.create({ id, ownerId: 'prop-writer', name: `propagation ${i}` })
    )
    await request.client
    // await the SERVER ack so each write is fully committed before the next.
    // records the commit instant so we can segment pure commit->seen wake
    // latency from the writer's own push round trip.
    await assertServerOutcome(request.server, 'success', id)
    committedAt.set(id, Date.now())
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

  const wakeLatencies: number[] = []
  const fullLatencies: number[] = []
  for (const w of watchers) {
    for (const id of ids) {
      const seen = w.seenAt(id)!
      wakeLatencies.push(Math.max(0, seen - committedAt.get(id)!))
      fullLatencies.push(Math.max(0, seen - issuedAt.get(id)!))
    }
  }
  wakeLatencies.sort((a, b) => a - b)
  fullLatencies.sort((a, b) => a - b)

  for (const w of watchers) w.destroy()

  return {
    label,
    wakeP50: percentile(wakeLatencies, 50),
    wakeP95: percentile(wakeLatencies, 95),
    wakeP99: percentile(wakeLatencies, 99),
    fullP95: percentile(fullLatencies, 95),
    fullMax: fullLatencies[fullLatencies.length - 1] ?? 0,
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
  const wakeTarget = await startWakeTarget(args.against!)
  targets.push(wakeTarget)
  const wakeResult = await measure(wakeTarget, args.against!)
  console.log(
    `[propagation] ${wakeResult.label} wake latency commit->seen (ms): ` +
      `p50=${wakeResult.wakeP50} p95=${wakeResult.wakeP95} p99=${wakeResult.wakeP99} ` +
      `| full issue->seen p95=${wakeResult.fullP95} ` +
      `(${wakeResult.readers} readers x ${wakeResult.writes} writes)`
  )

  // wake-driven proof: with a large safety poll, sub-poll convergence can only
  // come from the wake channel. the max full latency must sit well under the
  // poll (each write is server-committed, so no push backlog inflates it).
  if (wakeResult.fullMax >= SAFETY_POLL_MS / 2) {
    throw new Error(
      `converged via the safety poll: max full latency ${wakeResult.fullMax}ms is not far ` +
        `below the ${SAFETY_POLL_MS}ms poll — the wake channel did not drive convergence`
    )
  }
  if (wakeResult.wakeP95 >= GATE_P95_MS) {
    throw new Error(
      `wake propagation p95 ${wakeResult.wakeP95}ms exceeds the ${GATE_P95_MS}ms gate`
    )
  }

  if (args.baseline !== 'none') {
    const baseline = await startStockZero()
    targets.push(baseline)
    const baseResult = await measure(baseline, args.baseline!)
    console.log(
      `[propagation] ${baseResult.label} websocket latency commit->seen (ms): ` +
        `p50=${baseResult.wakeP50} p95=${baseResult.wakeP95} p99=${baseResult.wakeP99}`
    )
    console.log(
      `[propagation] differential: ${wakeResult.label} wake p95 ${wakeResult.wakeP95}ms vs ` +
        `${baseResult.label} websocket p95 ${baseResult.wakeP95}ms ` +
        `(delta ${wakeResult.wakeP95 - baseResult.wakeP95}ms)`
    )
  }

  console.log(
    `[propagation] PASS ${args.against}: wake-driven, commit->seen p95 ${wakeResult.wakeP95}ms ` +
      `< ${GATE_P95_MS}ms, no safety-poll convergence (total ${Date.now() - t0}ms)`
  )
} catch (error) {
  failed = true
  console.error('[propagation] FAIL:', error)
} finally {
  for (const target of targets) await target.close()
}

process.exit(failed ? 1 : 0)
