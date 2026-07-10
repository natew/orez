// Persisted-client reconnect/resume lane against the sqlite reference target.
// Exercises the stock Zero client's real Replicache DAG across close/reopen,
// retention + epoch snapshot fallback, lost push responses, HTTP host restart,
// and future-cookie invalidation/reload.
//
//   bun src/reconnect.ts
//   bun src/reconnect.ts --target rust-local
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { parseArgs } from 'node:util'

import { mutators, queries } from './fixture.js'
import { persistentKVStoreProvider } from './persistent-kv.js'
import { assertServerOutcome } from './server-outcome.js'
import { startOrezLocal } from './targets/orez-local.js'

import type { FixtureZero } from './target.js'

const { values: cli } = parseArgs({
  options: { target: { type: 'string', default: 'orez-local' } },
})

// the only shape both targets' onPull observations share (orez-local emits
// {body,response}; the spawned rust-local emits an observedPullFetch record).
// the lane only inspects the request body and the response rowsPatch.
type Observed = { body: unknown; response?: unknown }

type ProjectRow = { id: string }

function watchProjects(zero: FixtureZero) {
  const view = zero.materialize(queries.allProjects())
  let rows: ProjectRow[] = []
  let complete = false
  let destroyed = false
  view.addListener((data, resultType) => {
    rows = JSON.parse(JSON.stringify(data)) as ProjectRow[]
    if (resultType === 'complete') complete = true
  })
  return {
    get complete() {
      return complete
    },
    has(id: string) {
      return rows.some((row) => row.id === id)
    },
    destroy() {
      if (destroyed) return
      destroyed = true
      view.destroy()
    },
  }
}

async function eventually(check: () => void, label: string, timeoutMs = 30_000) {
  const started = Date.now()
  let lastError: unknown
  while (Date.now() - started < timeoutMs) {
    try {
      check()
      return
    } catch (error) {
      lastError = error
      await new Promise((resolve) => setTimeout(resolve, 50))
    }
  }
  throw new Error(`timeout waiting for ${label}: ${String(lastError)}`)
}

async function withTimeout<T>(promise: Promise<T>, label: string, timeoutMs = 30_000) {
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

function pullBody(observation: Observed) {
  return observation.body as {
    clientID?: string
    clientGroupID?: string
    cookie?: number | null
  }
}

function isSnapshot(observation: Observed) {
  const rowsPatch = (observation.response as { rowsPatch?: Array<{ op?: string }> })
    ?.rowsPatch
  return rowsPatch?.[0]?.op === 'clear'
}

const storageDir = mkdtempSync(join(tmpdir(), 'zharness-persist-'))
const kvStore = persistentKVStoreProvider(storageDir)
const storageKey = `zharness-resume-${Date.now()}`
const pulls: Observed[] = []
const observe = (observation: Observed) => {
  pulls.push(observation)
}
const referenceOpts = { pullIntervalMs: 100, retainChanges: 2, onPull: observe }
const target =
  cli.target === 'rust-local'
    ? await (await import('./targets/rust-local.js')).startRustLocal(referenceOpts)
    : cli.target === 'rust-cf'
      ? await (await import('./targets/rust-cf.js')).startRustCf(referenceOpts)
      : await startOrezLocal(referenceOpts)
const views: ReturnType<typeof watchProjects>[] = []

try {
  const first = target.createClient('u0', { kvStore, storageKey })
  const firstView = watchProjects(first)
  views.push(firstView)
  await eventually(() => {
    if (!firstView.complete) throw new Error('first client is not complete')
    if (!firstView.has('p0')) throw new Error('first client is missing seed data')
  }, 'first persisted hydration')

  const firstClientID = first.clientID
  const firstGroupID = await first.clientGroupID
  const beforeClose = first.mutate(
    mutators.project.create({
      id: 'resume-before-close',
      ownerId: 'u0',
      name: 'persisted before close',
    })
  )
  await beforeClose.client
  await withTimeout(
    assertServerOutcome(beforeClose.server, 'success', 'resume-before-close'),
    'first authoritative mutation'
  )
  await eventually(() => {
    if (!firstView.has('resume-before-close')) throw new Error('mutation row not visible')
  }, 'first mutation visibility')

  // Replicache persists on an idle scheduler (1s idle + 500ms throttle), not
  // synchronously in Zero.close(). Let that real scheduler flush the latest
  // server cookie before simulating process exit.
  await new Promise((resolve) => setTimeout(resolve, 2_000))

  firstView.destroy()
  await first.close()
  const resumePullStart = pulls.length

  // The closed client misses enough upstream writes to fall below the retained
  // floor. Restart the HTTP host without touching sqlite, then reopen from the
  // same on-disk DAG: its first request must carry the persisted non-null cookie
  // and recover through a snapshot.
  for (let i = 0; i < 8; i++) {
    await target.sql(
      `INSERT INTO project (id, "ownerId", name) VALUES ('resume-offline-${i}', 'u0', 'offline ${i}')`
    )
  }
  await target.restart()

  let clientStateNotFound = false
  const resumed = target.createClient('u0', {
    kvStore,
    storageKey,
    onClientStateNotFound: () => {
      clientStateNotFound = true
    },
  })
  const resumedView = watchProjects(resumed)
  views.push(resumedView)
  const resumedClientID = resumed.clientID
  const resumedGroupID = await resumed.clientGroupID
  if (resumedClientID === firstClientID) throw new Error('reopen reused the old clientID')
  if (resumedGroupID !== firstGroupID) {
    throw new Error(`reopen forked client group ${firstGroupID} -> ${resumedGroupID}`)
  }

  await eventually(() => {
    if (!resumedView.complete) throw new Error('resumed client is not complete')
    if (!resumedView.has('resume-before-close')) throw new Error('persisted row missing')
    if (!resumedView.has('resume-offline-7')) throw new Error('offline row missing')
    const firstResumePull = pulls
      .slice(resumePullStart)
      .find((observation) => pullBody(observation).clientID === resumedClientID)
    if (!firstResumePull) throw new Error('no pull from resumed client')
    if (pullBody(firstResumePull).cookie === null) {
      throw new Error('resumed client sent a fresh null cookie')
    }
    if (!isSnapshot(firstResumePull))
      throw new Error('stale retained cookie did not snapshot')
  }, 'persisted cookie retention recovery')
  console.log('[reconnect] persisted cookie + group resume + retention snapshot PASS')

  // Commit the mutation but destroy its HTTP response. The stock client must
  // reconnect and settle .server through replay/LMID recovery, without a
  // duplicate row or a permanently pending mutation.
  await target.dropNextPushResponse()
  const lostResponse = resumed.mutate(
    mutators.project.create({
      id: 'resume-lost-response',
      ownerId: 'u0',
      name: 'response deliberately lost',
    })
  )
  await lostResponse.client
  await withTimeout(
    assertServerOutcome(lostResponse.server, 'success', 'resume-lost-response'),
    'lost-response mutation recovery'
  )
  await eventually(() => {
    if (!resumedView.has('resume-lost-response')) throw new Error('recovered row missing')
  }, 'lost-response row convergence')
  const recoveredRows = await target.oracle(
    `SELECT id FROM project WHERE id = 'resume-lost-response'`
  )
  if (recoveredRows.length !== 1) throw new Error('lost-response mutation was duplicated')
  console.log('[reconnect] lost push response mutation recovery PASS')

  const epochPullStart = pulls.length
  await target.invalidate()
  await eventually(() => {
    const snapshot = pulls
      .slice(epochPullStart)
      .find(
        (observation) =>
          pullBody(observation).clientID === resumedClientID && isSnapshot(observation)
      )
    if (!snapshot) throw new Error('epoch did not force a real-client snapshot')
    if (!resumedView.has('resume-lost-response'))
      throw new Error('epoch lost current rows')
  }, 'epoch snapshot fallback')
  console.log('[reconnect] epoch snapshot fallback PASS')

  // Simulate a restored/reset server whose watermark is behind the persisted
  // client. The transport emits InvalidConnectionRequestBaseCookie; Zero drops
  // the durable DB and invokes the host reload hook. Recreating the client then
  // performs a fresh null-cookie snapshot.
  await target.resetCursor()
  await eventually(() => {
    if (!clientStateNotFound) throw new Error('future-cookie invalidation not surfaced')
  }, 'future-cookie client invalidation')
  resumedView.destroy()
  await resumed.close()

  const reloadPullStart = pulls.length
  const reloaded = target.createClient('u0', { kvStore, storageKey })
  const reloadedView = watchProjects(reloaded)
  views.push(reloadedView)
  const reloadedClientID = reloaded.clientID
  const reloadedGroupID = await reloaded.clientGroupID
  if (reloadedGroupID === resumedGroupID) {
    throw new Error('future-cookie database drop retained the old client group')
  }
  await eventually(() => {
    if (!reloadedView.complete) throw new Error('reloaded client is not complete')
    if (!reloadedView.has('resume-lost-response'))
      throw new Error('reload snapshot missing row')
    const freshPull = pulls
      .slice(reloadPullStart)
      .find((observation) => pullBody(observation).clientID === reloadedClientID)
    if (!freshPull) throw new Error('no pull from reloaded client')
    if (pullBody(freshPull).cookie !== null)
      throw new Error('reloaded client was not fresh')
    if (!isSnapshot(freshPull)) throw new Error('reloaded client did not snapshot')
  }, 'future-cookie reload snapshot')
  console.log('[reconnect] future-cookie 409 invalidation + fresh reload PASS')

  console.log('[reconnect] PASS: persisted resume + faults + snapshot fallbacks')
} finally {
  for (const view of views) view.destroy()
  await target.close()
  rmSync(storageDir, { recursive: true, force: true })
}
