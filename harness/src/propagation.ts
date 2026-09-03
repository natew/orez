// propagation lane: measures cross-client wake latency on the native host
// (rust-local) differentially against stock zero-cache's websocket push.
//
// the wake target runs with a deliberately LARGE safety-poll interval, so any
// sub-second convergence PROVES the wake channel drove it, not the poll (the
// plan's "no lane converges via the safety poll"). the gate is native wake
// reach median < 50 ms, plus every reader converging far under the safety poll;
// stock zero-cache's websocket push is the baseline the differential reports
// against.
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
// wake REACH budget: commit -> the first reader that sees it, which is the part
// the server controls. native localhost is 50ms; the CF host over WAN gets half
// the plan's one-second storm-load budget.
//
// the MEDIAN of the per-write reaches is what is gated. reach has one sample
// per write, so at the default 20 writes a "p95" is index 19 of 20, which is
// the max: one scheduling hiccup anywhere in the run sets it. the median over
// 20 writes is stable (4-6ms locally under full load, against a 50ms budget),
// and the failure this gate exists to catch — a dead wake channel, leaving the
// 10s safety poll to converge — misses the budget by two orders of magnitude.
//
// this used to gate the p95 across every reader/write pair, and that number is
// dominated by how many zero clients the machine is running, not by the server.
// measured on one machine against one server, median commit->seen spread
// between the first and last of N readers: N=1 0ms, N=2 1ms, N=5 4ms, N=10
// 24ms, N=20 36ms, while the first reader stayed at 3-8ms throughout. running
// each reader in its own process does not change that (it is CPU contention
// between the readers, not a shared event loop), so on a 4-vCPU CI runner with
// eleven clients the tail reached p95=201ms with p50=22ms. gating the tail
// there gates the runner. the tail is still asserted, below, against the safety
// poll: every reader must converge far under it, which is what proves the wake
// channel drove convergence at all.
const GATE_REACH_MS = args.against === 'rust-cf' ? 500 : 50

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
  // commit -> first reader that sees it: how fast the server gets a wake out,
  // one sample per write (the gated metric)
  reachP50: number
  reachP95: number
  reachMax: number
  // commit -> seen, every reader x write pair: reported, not gated. scales with
  // how many clients the measuring machine runs, so it says as much about the
  // runner as about the server.
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
  const reachLatencies: number[] = []
  for (const id of ids) {
    const committed = committedAt.get(id)!
    const perReader = watchers.map((w) => Math.max(0, w.seenAt(id)! - committed))
    reachLatencies.push(Math.min(...perReader))
    wakeLatencies.push(...perReader)
    for (const w of watchers) {
      fullLatencies.push(Math.max(0, w.seenAt(id)! - issuedAt.get(id)!))
    }
  }
  wakeLatencies.sort((a, b) => a - b)
  fullLatencies.sort((a, b) => a - b)
  reachLatencies.sort((a, b) => a - b)

  for (const w of watchers) w.destroy()

  return {
    label,
    reachP50: percentile(reachLatencies, 50),
    reachP95: percentile(reachLatencies, 95),
    reachMax: reachLatencies[reachLatencies.length - 1] ?? 0,
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
    `[propagation] ${wakeResult.label} wake reach commit->first reader (ms): ` +
      `p50=${wakeResult.reachP50} p95=${wakeResult.reachP95} max=${wakeResult.reachMax} ` +
      `| all readers commit->seen p50=${wakeResult.wakeP50} p95=${wakeResult.wakeP95} ` +
      `p99=${wakeResult.wakeP99} | full issue->seen p95=${wakeResult.fullP95} ` +
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
  if (wakeResult.reachP50 >= GATE_REACH_MS) {
    throw new Error(
      `median wake reach ${wakeResult.reachP50}ms exceeds the ${GATE_REACH_MS}ms gate`
    )
  }

  if (args.baseline !== 'none') {
    const baseline = await startStockZero()
    targets.push(baseline)
    const baseResult = await measure(baseline, args.baseline!)
    console.log(
      `[propagation] ${baseResult.label} websocket reach commit->first reader (ms): ` +
        `p50=${baseResult.reachP50} p95=${baseResult.reachP95} max=${baseResult.reachMax}`
    )
    console.log(
      `[propagation] differential: ${wakeResult.label} median wake reach ` +
        `${wakeResult.reachP50}ms vs ${baseResult.label} median websocket reach ` +
        `${baseResult.reachP50}ms (delta ${wakeResult.reachP50 - baseResult.reachP50}ms)`
    )
  }

  console.log(
    `[propagation] PASS ${args.against}: wake-driven, median commit->first reader ` +
      `${wakeResult.reachP50}ms < ${GATE_REACH_MS}ms, no safety-poll convergence ` +
      `(total ${Date.now() - t0}ms)`
  )
} catch (error) {
  failed = true
  console.error('[propagation] FAIL:', error)
} finally {
  for (const target of targets) await target.close()
}

process.exit(failed ? 1 : 0)
