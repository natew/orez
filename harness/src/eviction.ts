// Fault lane for authority restarts while stock Zero clients stay alive:
// - local: SIGKILL a child-process sync server mid-churn, then reopen the same
//   file-backed SQLite database on a new PID.
// - CF: stop interval polling, idle beyond the harness DO's in-memory teardown
//   window, prove its boot ID changed, then resume the same clients.
// Both paths pin all-client + oracle + fresh-client convergence, zero 409s,
// and non-regressing request/response cookies.
//
//   bun src/eviction.ts --target local
//   bun src/eviction.ts --target cf
import { statSync } from 'node:fs'
import { parseArgs } from 'node:util'

import { mutators, queries } from './fixture.js'
import { assertServerOutcome } from './server-outcome.js'
import { startOrezCf } from './targets/orez-cf.js'
import { startOrezLocalProcess } from './targets/orez-local-process.js'

import type { HttpPullObservation } from './observed-fetch.js'
import type { FixtureZero, SyncTarget } from './target.js'

const { values: args } = parseArgs({
  options: {
    target: { type: 'string', default: 'local' },
    clients: { type: 'string', default: '10' },
    'idle-ms': { type: 'string' },
  },
})

const CLIENTS = Number(args.clients)
if (!Number.isInteger(CLIENTS) || CLIENTS <= 1) {
  throw new Error('clients must be an integer greater than one')
}

type TaskProject = { tasks?: Array<{ id: string }> }

function watchTaskIDs(zero: FixtureZero) {
  const view = zero.materialize(queries.projectById({ id: 'p0' }))
  let complete = false
  let ids = new Set<string>()
  let destroyed = false
  view.addListener((data, resultType) => {
    const project =
      data === undefined ? undefined : (JSON.parse(JSON.stringify(data)) as TaskProject)
    ids = new Set((project?.tasks ?? []).map(({ id }) => id))
    if (resultType === 'complete') complete = true
  })
  return {
    get complete() {
      return complete
    },
    hasEvery(expected: Iterable<string>) {
      for (const id of expected) if (!ids.has(id)) return false
      return true
    },
    destroy() {
      if (destroyed) return
      destroyed = true
      view.destroy()
    },
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

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string) {
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(`timeout waiting for ${label}`)),
          timeoutMs
        )
      }),
    ])
  } finally {
    clearTimeout(timer)
  }
}

function successfulPull(observation: HttpPullObservation) {
  if (observation.status !== 200) return undefined
  const body = observation.body as { clientID?: unknown; cookie?: unknown }
  const response = observation.response as { cookie?: unknown } | undefined
  if (typeof body.clientID !== 'string' || typeof response?.cookie !== 'number') {
    return undefined
  }
  return {
    at: observation.at,
    clientID: body.clientID,
    requestCookie: body.cookie,
    responseCookie: response.cookie,
  }
}

function assertPullHistory(
  observations: HttpPullObservation[],
  clientIDs: string[],
  boundaryAt: number,
  requireNetworkFailure: boolean
) {
  const staleResponses = observations.filter(({ status }) => status === 409)
  if (staleResponses.length > 0) {
    throw new Error(`observed ${staleResponses.length} stale-cookie 409 response(s)`)
  }
  const networkFailures = observations.filter(({ error }) => error !== undefined)
  if (requireNetworkFailure && networkFailures.length === 0) {
    throw new Error('process outage produced no observed pull failure')
  }

  const successful = observations.map(successfulPull).filter((pull) => pull !== undefined)
  for (const clientID of clientIDs) {
    const pulls = successful.filter((pull) => pull.clientID === clientID)
    if (!pulls.some(({ at }) => at < boundaryAt)) {
      throw new Error(`client ${clientID} has no successful pull before boundary`)
    }
    if (!pulls.some(({ at }) => at >= boundaryAt)) {
      throw new Error(`client ${clientID} has no successful pull after boundary`)
    }

    let lastRequest = -1
    let lastResponse = -1
    for (const pull of pulls) {
      if (pull.requestCookie !== null && typeof pull.requestCookie === 'number') {
        if (pull.requestCookie < lastRequest) {
          throw new Error(
            `client ${clientID} request cookie regressed ${lastRequest} -> ${pull.requestCookie}`
          )
        }
        lastRequest = pull.requestCookie
      }
      if (pull.responseCookie < lastResponse) {
        throw new Error(
          `client ${clientID} response cookie regressed ${lastResponse} -> ${pull.responseCookie}`
        )
      }
      if (
        typeof pull.requestCookie === 'number' &&
        pull.responseCookie < pull.requestCookie
      ) {
        throw new Error(
          `client ${clientID} response ${pull.responseCookie} behind request ${pull.requestCookie}`
        )
      }
      lastResponse = pull.responseCookie
    }
  }
  return { successful: successful.length, networkFailures: networkFailures.length }
}

async function issueTasks(
  clients: FixtureZero[],
  prefix: string,
  start: number,
  count: number,
  intervalMs: number,
  expected: Set<string>,
  outcomes: Promise<void>[]
) {
  for (let offset = 0; offset < count; offset++) {
    const index = start + offset
    const id = `${prefix}-${index}`
    expected.add(id)
    const request = clients[index % Math.min(2, clients.length)]!.mutate(
      mutators.task.create({
        id,
        projectId: 'p0',
        title: `eviction ${index}`,
        rank: index,
        done: false,
      })
    )
    outcomes.push(assertServerOutcome(request.server, 'success', `task.create ${id}`))
    await request.client
    await new Promise((resolve) => setTimeout(resolve, intervalMs))
  }
}

async function assertConverged(
  target: SyncTarget,
  watchers: ReturnType<typeof watchTaskIDs>[],
  expected: Set<string>,
  prefix: string,
  timeoutMs: number
) {
  const convergeMs = await eventually(
    () => {
      for (const [index, watcher] of watchers.entries()) {
        if (!watcher.complete) throw new Error(`client ${index} is not complete`)
        if (!watcher.hasEvery(expected))
          throw new Error(`client ${index} is missing tasks`)
      }
    },
    timeoutMs,
    'all-client convergence'
  )
  const oracle = await target.oracle(
    `SELECT id FROM task WHERE id LIKE '${prefix}-%' ORDER BY id`
  )
  const got = oracle.map(({ id }) => String(id)).sort()
  const want = [...expected].sort()
  if (JSON.stringify(got) !== JSON.stringify(want)) {
    throw new Error(
      `oracle differs: got ${JSON.stringify(got)}, want ${JSON.stringify(want)}`
    )
  }
  return convergeMs
}

async function lateClientCheck(
  target: SyncTarget,
  expected: Set<string>,
  timeoutMs: number
) {
  const late = target.createClient('eviction-late')
  const watcher = watchTaskIDs(late)
  try {
    return await eventually(
      () => {
        if (!watcher.complete) throw new Error('late client is not complete')
        if (!watcher.hasEvery(expected)) throw new Error('late client is missing tasks')
      },
      timeoutMs,
      'late-client hydration'
    )
  } finally {
    watcher.destroy()
  }
}

async function runLocal() {
  const observations: HttpPullObservation[] = []
  const target = await startOrezLocalProcess({
    pullIntervalMs: 100,
    onPull: (observation) => observations.push(observation),
  })
  const clients: FixtureZero[] = []
  const watchers: ReturnType<typeof watchTaskIDs>[] = []
  const expected = new Set<string>()
  const outcomes: Promise<void>[] = []
  const prefix = `evict-local-${Date.now().toString(36)}`
  try {
    for (let i = 0; i < CLIENTS; i++) {
      const zero = target.createClient(`evict-user-${i}`)
      clients.push(zero)
      watchers.push(watchTaskIDs(zero))
    }
    await eventually(
      () => {
        for (const watcher of watchers) {
          if (!watcher.complete) throw new Error('initial client incomplete')
        }
      },
      60_000,
      'initial process-backed hydration'
    )
    if (statSync(target.databaseFile).size === 0)
      throw new Error('authority file is empty')

    const churn = issueTasks(clients, prefix, 0, 30, 200, expected, outcomes)
    await new Promise((resolve) => setTimeout(resolve, 1_200))
    const crashStartedAt = Date.now()
    const pids = await target.crashAndRestart(1_500)
    const restartedAt = Date.now()
    await churn
    await withTimeout(Promise.all(outcomes), 120_000, 'outage mutation recovery')
    const convergeMs = await assertConverged(target, watchers, expected, prefix, 120_000)
    const clientIDs = clients.map(({ clientID }) => clientID)
    await eventually(
      () => {
        assertPullHistory(observations, clientIDs, restartedAt, true)
      },
      120_000,
      'post-restart pulls from every client'
    )
    const history = assertPullHistory(observations, clientIDs, restartedAt, true)
    const lateHydrateMs = await lateClientCheck(target, expected, 60_000)
    console.log(
      `[eviction] local PASS pid ${pids.before}->${pids.after}, outage=${restartedAt - crashStartedAt}ms, ` +
        `${expected.size} writes, converge=${convergeMs}ms, late=${lateHydrateMs}ms, ` +
        `pulls=${history.successful}, failed-pulls=${history.networkFailures}, 409s=0, cookies monotone`
    )
  } finally {
    for (const watcher of watchers) watcher.destroy()
    await target.close()
  }
}

async function runCf() {
  const observations: HttpPullObservation[] = []
  // no interval: after the initial pull and pre-idle writes, the DO sees no
  // traffic until this lane explicitly resumes it.
  const target = await startOrezCf({
    pullIntervalMs: 0,
    onPull: (observation) => observations.push(observation),
  })
  const clients: FixtureZero[] = []
  const watchers: ReturnType<typeof watchTaskIDs>[] = []
  const expected = new Set<string>()
  const outcomes: Promise<void>[] = []
  const prefix = `evict-cf-${Date.now().toString(36)}`
  try {
    for (let i = 0; i < CLIENTS; i++) {
      const zero = target.createClient(`hibernate-user-${i}`)
      clients.push(zero)
      watchers.push(watchTaskIDs(zero))
    }
    await eventually(
      () => {
        for (const watcher of watchers) {
          if (!watcher.complete) throw new Error('initial CF client incomplete')
        }
      },
      120_000,
      'initial CF hydration'
    )

    await issueTasks(clients, prefix, 0, 10, 150, expected, outcomes)
    await withTimeout(Promise.all(outcomes), 120_000, 'pre-idle CF mutations')
    await target.pull()
    await assertConverged(target, watchers, expected, prefix, 120_000)
    const before = await target.hibernationStatus()
    const idleMs = args['idle-ms']
      ? Number(args['idle-ms'])
      : before.idleTeardownMs + 1_000
    if (!Number.isFinite(idleMs) || idleMs < before.idleTeardownMs) {
      throw new Error(`idle-ms must be at least ${before.idleTeardownMs}`)
    }
    console.log(`[eviction] CF idling ${idleMs}ms past teardown window...`)
    await new Promise((resolve) => setTimeout(resolve, idleMs))
    const resumedAt = Date.now()
    const after = await target.hibernationStatus()
    if (after.bootID === before.bootID) {
      throw new Error(`DO boot ID did not change after ${idleMs}ms idle`)
    }

    await issueTasks(clients, prefix, 10, 10, 150, expected, outcomes)
    await withTimeout(Promise.all(outcomes), 120_000, 'post-idle CF mutations')
    await target.pull()
    const convergeMs = await assertConverged(target, watchers, expected, prefix, 120_000)
    const clientIDs = clients.map(({ clientID }) => clientID)
    await eventually(
      () => {
        assertPullHistory(observations, clientIDs, resumedAt, false)
      },
      120_000,
      'post-hibernation pulls from every client'
    )
    const history = assertPullHistory(observations, clientIDs, resumedAt, false)
    const lateHydrateMs = await lateClientCheck(target, expected, 120_000)
    console.log(
      `[eviction] CF PASS boot ${before.bootID}->${after.bootID}, idle=${idleMs}ms, ` +
        `${expected.size} writes, converge=${convergeMs}ms, late=${lateHydrateMs}ms, ` +
        `pulls=${history.successful}, 409s=0, cookies monotone`
    )
  } finally {
    for (const watcher of watchers) watcher.destroy()
    await target.close()
  }
}

let failed = false
try {
  if (args.target === 'local') await runLocal()
  else if (args.target === 'cf') await runCf()
  else throw new Error(`target must be local or cf, got '${args.target}'`)
} catch (error) {
  failed = true
  console.error('[eviction] FAIL:', error)
}

process.exit(failed ? 1 : 0)
