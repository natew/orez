// Single-project width lane: N stock Zero clients hold one reactive project
// query on one server instance while a small writer set mutates the same task
// set at a sustainable rate. Every round must settle on every non-writing
// client and the authority before the next round starts; a fresh late client
// then proves the final state hydrates without relying on an existing cache.
//
//   bun src/storm.ts --target orez-local --clients 100
//   bun src/storm.ts --target orez-cf --clients 100 --pull-interval 1000
import { parseArgs } from 'node:util'

import { mutators, queries } from './fixture.js'
import { assertServerOutcome } from './server-outcome.js'

import type { FixtureZero, SyncTarget } from './target.js'

const { values: args } = parseArgs({
  options: {
    target: { type: 'string', default: 'orez-local' },
    clients: { type: 'string', default: '100' },
    writers: { type: 'string', default: '5' },
    rounds: { type: 'string', default: '5' },
    'ops-per-writer': { type: 'string', default: '4' },
    rate: { type: 'string', default: '1' },
    'pull-interval': { type: 'string' },
    label: { type: 'string', default: '' },
  },
})

const TARGET = args.target!
const CLIENTS = Number(args.clients)
const WRITERS = Math.min(Number(args.writers), CLIENTS)
const ROUNDS = Number(args.rounds)
const OPS_PER_WRITER = Number(args['ops-per-writer'])
const RATE_PER_WRITER = Number(args.rate)
const PULL_INTERVAL_MS = args['pull-interval']
  ? Number(args['pull-interval'])
  : TARGET === 'orez-cf'
    ? 1_000
    : 250

for (const [name, value] of Object.entries({
  clients: CLIENTS,
  writers: WRITERS,
  rounds: ROUNDS,
  opsPerWriter: OPS_PER_WRITER,
  ratePerWriter: RATE_PER_WRITER,
  pullIntervalMs: PULL_INTERVAL_MS,
})) {
  if (!Number.isFinite(value) || value <= 0) throw new Error(`${name} must be positive`)
}
if (!Number.isInteger(CLIENTS) || !Number.isInteger(WRITERS)) {
  throw new Error('clients and writers must be integers')
}
if (!Number.isInteger(ROUNDS) || !Number.isInteger(OPS_PER_WRITER)) {
  throw new Error('rounds and ops-per-writer must be integers')
}

async function startTarget(): Promise<SyncTarget> {
  if (TARGET === 'orez-local') {
    const { startOrezLocal } = await import('./targets/orez-local.js')
    return startOrezLocal({ pullIntervalMs: PULL_INTERVAL_MS })
  }
  if (TARGET === 'orez-cf') {
    const { startOrezCf } = await import('./targets/orez-cf.js')
    return startOrezCf({ pullIntervalMs: PULL_INTERVAL_MS })
  }
  throw new Error(`storm target must be orez-local or orez-cf, got '${TARGET}'`)
}

type TaskRow = {
  id: string
  title: string
  rank: number
  done: boolean
}

type ExpectedEvent = TaskRow & { key: string }

function percentile(sorted: number[], p: number) {
  if (sorted.length === 0) return 0
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))]!
}

function percentiles(values: number[]) {
  const sorted = [...values].sort((a, b) => a - b)
  return {
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
  }
}

async function eventually(check: () => void, timeoutMs: number, label: string) {
  const started = Date.now()
  let lastError: unknown
  while (Date.now() - started < timeoutMs) {
    try {
      check()
      return Date.now() - started
    } catch (error) {
      lastError = error
      await new Promise((resolve) => setTimeout(resolve, 50))
    }
  }
  throw new Error(`timeout (${timeoutMs}ms) waiting for ${label}: ${String(lastError)}`)
}

function taskMatches(actual: TaskRow | undefined, expected: TaskRow) {
  return (
    actual?.id === expected.id &&
    actual.title === expected.title &&
    actual.rank === expected.rank &&
    actual.done === expected.done
  )
}

function watchProject(
  zero: FixtureZero,
  events: ExpectedEvent[],
  seenAt: Map<string, number> | undefined
) {
  const view = zero.materialize(queries.projectById({ id: 'p0' }))
  let complete = false
  let callbacks = 0
  let tasks = new Map<string, TaskRow>()
  let destroyed = false
  view.addListener((data, resultType) => {
    callbacks++
    const project =
      data === undefined
        ? undefined
        : (JSON.parse(JSON.stringify(data)) as { tasks?: TaskRow[] })
    tasks = new Map((project?.tasks ?? []).map((task) => [task.id, task]))
    if (seenAt) {
      const now = Date.now()
      for (const event of events) {
        if (!seenAt.has(event.key) && taskMatches(tasks.get(event.id), event)) {
          seenAt.set(event.key, now)
        }
      }
    }
    if (resultType === 'complete') complete = true
  })
  return {
    get complete() {
      return complete
    },
    get callbacks() {
      return callbacks
    },
    task(id: string) {
      return tasks.get(id)
    },
    destroy() {
      if (destroyed) return
      destroyed = true
      view.destroy()
    },
  }
}

function normalizedOracleTasks(rows: Record<string, unknown>[]) {
  return rows.map((row) => ({
    id: String(row.id),
    title: String(row.title),
    rank: Number(row.rank),
    done: row.done === true || row.done === 1 || row.done === '1',
  }))
}

function sortedTasks(tasks: Iterable<TaskRow>) {
  return [...tasks].sort((a, b) => a.id.localeCompare(b.id))
}

const timeoutMs = TARGET === 'orez-cf' ? 180_000 : 60_000
const runID = `${Date.now().toString(36)}-${Math.floor(Math.random() * 1e6).toString(36)}`
const prefix = `storm-${runID}`
const intervalMs = 1_000 / RATE_PER_WRITER
const events: ExpectedEvent[] = []
const expectedTasks = new Map<string, TaskRow>()
const issuedAt = new Map<string, number>()
const ackLatencies: number[] = []
const propagationLatencies: number[] = []
const roundResults: Array<Record<string, unknown>> = []
const t0 = Date.now()
const target = await startTarget()
const clients: FixtureZero[] = []
const seenByClient: Map<string, number>[] = []
const watchers: ReturnType<typeof watchProject>[] = []
let failed = false

try {
  for (let i = 0; i < CLIENTS; i++) {
    const zero = target.createClient(`storm-user-${i}`)
    const seen = new Map<string, number>()
    clients.push(zero)
    seenByClient.push(seen)
    watchers.push(watchProject(zero, events, seen))
  }

  const hydrateMs = await eventually(
    () => {
      for (const [index, watcher] of watchers.entries()) {
        if (!watcher.complete) throw new Error(`client ${index} is not complete`)
      }
    },
    timeoutMs,
    `${CLIENTS}-client hydration`
  )
  console.log(`[storm] ${target.name} hydrated ${CLIENTS} clients in ${hydrateMs}ms`)

  for (let round = 0; round < ROUNDS; round++) {
    const roundStarted = Date.now()
    const roundEvents: ExpectedEvent[] = []
    const roundAcks: number[] = []
    let lastIssuedAt = roundStarted

    await Promise.all(
      Array.from({ length: WRITERS }, async (_, writer) => {
        const zero = clients[writer]!
        for (let slot = 0; slot < OPS_PER_WRITER; slot++) {
          const taskID = `${prefix}-w${writer}-s${slot}`
          const previous = expectedTasks.get(taskID)
          let next: TaskRow
          let request: ReturnType<FixtureZero['mutate']>
          let operation: string
          const issued = Date.now()

          if (round === 0) {
            next = {
              id: taskID,
              title: `storm ${writer}.${slot}`,
              rank: writer * OPS_PER_WRITER + slot,
              done: false,
            }
            operation = 'task.create'
            const key = `r${round}:${taskID}:create`
            const event = { ...next, key }
            expectedTasks.set(taskID, next)
            events.push(event)
            roundEvents.push(event)
            issuedAt.set(key, issued)
            lastIssuedAt = Math.max(lastIssuedAt, issued)
            request = zero.mutate(mutators.task.create({ ...next, projectId: 'p0' }))
          } else if (round % 2 === 1) {
            if (!previous) throw new Error(`missing task ${taskID} before toggle`)
            next = { ...previous, done: !previous.done }
            operation = 'task.toggle'
            const key = `r${round}:${taskID}:done=${next.done}`
            const event = { ...next, key }
            expectedTasks.set(taskID, next)
            events.push(event)
            roundEvents.push(event)
            issuedAt.set(key, issued)
            lastIssuedAt = Math.max(lastIssuedAt, issued)
            request = zero.mutate(mutators.task.toggle({ id: taskID, done: next.done }))
          } else {
            if (!previous) throw new Error(`missing task ${taskID} before rank update`)
            next = { ...previous, rank: round * 1_000 + writer * OPS_PER_WRITER + slot }
            operation = 'task.setRank'
            const key = `r${round}:${taskID}:rank=${next.rank}`
            const event = { ...next, key }
            expectedTasks.set(taskID, next)
            events.push(event)
            roundEvents.push(event)
            issuedAt.set(key, issued)
            lastIssuedAt = Math.max(lastIssuedAt, issued)
            request = zero.mutate(mutators.task.setRank({ id: taskID, rank: next.rank }))
          }

          const ack = assertServerOutcome(
            request.server,
            'success',
            `${operation} ${taskID}`
          ).then(() => {
            const latency = Date.now() - issued
            roundAcks.push(latency)
            ackLatencies.push(latency)
          })
          await request.client
          const nextIssueAt = roundStarted + (slot + 1) * intervalMs
          const delay = nextIssueAt - Date.now()
          if (delay > 0) await new Promise((resolve) => setTimeout(resolve, delay))
          await ack
        }
      })
    )

    const settleMs = await eventually(
      () => {
        for (const [clientIndex, seen] of seenByClient.entries()) {
          for (const event of roundEvents) {
            if (!seen.has(event.key)) {
              throw new Error(`client ${clientIndex} has not seen ${event.key}`)
            }
          }
        }
        for (const [clientIndex, watcher] of watchers.entries()) {
          for (const expected of expectedTasks.values()) {
            if (!taskMatches(watcher.task(expected.id), expected)) {
              throw new Error(`client ${clientIndex} state differs for ${expected.id}`)
            }
          }
        }
      },
      timeoutMs,
      `round ${round} convergence`
    )

    const oracle = normalizedOracleTasks(
      await target.oracle(
        `SELECT id, title, rank, done FROM task WHERE id LIKE '${prefix}-%' ORDER BY id`
      )
    )
    const expected = sortedTasks(expectedTasks.values())
    if (JSON.stringify(oracle) !== JSON.stringify(expected)) {
      throw new Error(
        `round ${round} oracle divergence:\n got ${JSON.stringify(oracle)}\nwant ${JSON.stringify(expected)}`
      )
    }

    const roundPropagation: number[] = []
    for (const event of roundEvents) {
      const latest = Math.max(...seenByClient.map((seen) => seen.get(event.key) ?? 0))
      const latency = latest - issuedAt.get(event.key)!
      roundPropagation.push(latency)
      propagationLatencies.push(latency)
    }
    const result = {
      round,
      operation: round === 0 ? 'create' : round % 2 === 1 ? 'toggle' : 'setRank',
      mutations: roundEvents.length,
      ack: percentiles(roundAcks),
      propagation: percentiles(roundPropagation),
      settleAfterLastIssueMs: Date.now() - lastIssuedAt,
      convergencePollMs: settleMs,
      roundWallMs: Date.now() - roundStarted,
    }
    roundResults.push(result)
    console.log(`[storm] round PASS ${JSON.stringify(result)}`)
  }

  // Final all-client equality is deliberately repeated after the round gates:
  // it catches late rebase/rollback effects that appeared after a round passed.
  await eventually(
    () => {
      for (const [clientIndex, watcher] of watchers.entries()) {
        for (const expected of expectedTasks.values()) {
          if (!taskMatches(watcher.task(expected.id), expected)) {
            throw new Error(`final client ${clientIndex} differs for ${expected.id}`)
          }
        }
      }
    },
    timeoutMs,
    'final all-client equality'
  )

  const late = target.createClient('storm-late-client')
  const lateWatcher = watchProject(late, events, undefined)
  const lateHydrateMs = await eventually(
    () => {
      if (!lateWatcher.complete) throw new Error('late client is not complete')
      for (const expected of expectedTasks.values()) {
        if (!taskMatches(lateWatcher.task(expected.id), expected)) {
          throw new Error(`late client differs for ${expected.id}`)
        }
      }
    },
    timeoutMs,
    'fresh late-client equality'
  )
  lateWatcher.destroy()

  const metrics = await target.metrics()
  const result = {
    target: target.name,
    label: args.label || undefined,
    clients: CLIENTS,
    writers: WRITERS,
    rounds: ROUNDS,
    opsPerWriter: OPS_PER_WRITER,
    ratePerWriter: RATE_PER_WRITER,
    aggregateRate: WRITERS * RATE_PER_WRITER,
    pullIntervalMs: PULL_INTERVAL_MS,
    mutations: issuedAt.size,
    hydrateMs,
    ack: percentiles(ackLatencies),
    propagation: percentiles(propagationLatencies),
    lateHydrateMs,
    reactiveReadCallbacks: watchers.reduce((sum, watcher) => sum + watcher.callbacks, 0),
    serverRssMb: metrics.serverRssMb,
    roundResults,
    totalMs: Date.now() - t0,
  }
  console.log(`[storm] ${JSON.stringify(result)}`)
  console.log(
    `[storm] PASS ${target.name}: ${CLIENTS} clients on p0, ${issuedAt.size} mixed mutations — ` +
      `ack p50/p95/p99 ${result.ack.p50}/${result.ack.p95}/${result.ack.p99}ms, ` +
      `propagation p50/p95/p99 ${result.propagation.p50}/${result.propagation.p95}/${result.propagation.p99}ms`
  )
} catch (error) {
  failed = true
  console.error('[storm] FAIL:', error)
} finally {
  for (const watcher of watchers) watcher.destroy()
  await target.close()
}

process.exit(failed ? 1 : 0)
