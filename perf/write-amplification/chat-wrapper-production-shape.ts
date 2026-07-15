import { readFileSync, rmSync, writeFileSync } from 'node:fs'
import { resolve } from 'node:path'

import { DoBackend } from '../../src/pg-proxy-do-backend.js'
import {
  PRODUCTION_SHAPE_FIXTURE,
  PRODUCTION_SHAPE_TABLE_COUNTS,
  productionShapeDDL,
} from './production-shape-fixture.js'

type ProfileSummary = {
  rowsWritten: number
  measuredStatements: number
  topRoutes: unknown[]
  topStatements: unknown[]
  topTables: unknown[]
}

type ProfileReport = Record<string, ProfileSummary>

type PhaseMeasurements = {
  source?: ProfileSummary
  cache?: ProfileSummary
}

type CacheState = {
  ready: boolean
  bootPending: boolean
  bootAttempts: number
  failures: number
  backoffUntil: number
}

function literal(value: string): string {
  return `'${value.replaceAll("'", "''")}'`
}

function quotedIdentifier(value: string): string {
  return `"${value.replaceAll('"', '""')}"`
}

async function json<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, init)
  const text = await response.text()
  if (!response.ok) {
    throw new Error(`${response.status} ${url}: ${text}`)
  }
  return JSON.parse(text) as T
}

const port = 9_980 + Math.floor(Math.random() * 15)
const persistencePath = `/tmp/orez-chat-wrapper-profile-${crypto.randomUUID()}`
const baseURL = `http://127.0.0.1:${port}`
const wrangler = Bun.spawn(
  [
    'bunx',
    'wrangler',
    'dev',
    '--config',
    'wrangler.chat-wrapper-production-shape.toml',
    '--local',
    '--persist-to',
    persistencePath,
    '--port',
    String(port),
  ],
  { cwd: import.meta.dir, stdout: 'ignore', stderr: 'inherit' }
)

const profilePath = (kind: 'source' | 'cache', instance: string, path: string) =>
  `${baseURL}/__profile/${kind}/${encodeURIComponent(instance)}${path}`

async function sourceExec(
  instance: string,
  sql: string,
  params: unknown[] = []
): Promise<{ rows: Array<Record<string, unknown>>; rowsWritten: number }> {
  return json(profilePath('source', instance, '/__profile_exec'), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ sql, params }),
  })
}

async function cacheExec(
  instance: string,
  sql: string,
  params: unknown[] = []
): Promise<{ rows: Array<Record<string, unknown>>; rowsWritten: number }> {
  return json(profilePath('cache', instance, '/__profile_exec'), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ sql, params }),
  })
}

async function seed(instance: string) {
  const backend = new DoBackend(profilePath('source', instance, ''), 'postgres', 'chat')
  await backend.waitReady
  await backend.exec(productionShapeDDL())
  for (const [table, count] of PRODUCTION_SHAPE_TABLE_COUNTS) {
    for (let offset = 0; offset < count; offset += 100) {
      const values = Array.from({ length: Math.min(100, count - offset) }, (_, i) => {
        const row = offset + i
        return `(${literal(`${table}-${row}`)}, ${literal(`group-${row % 17}`)}, ${literal(`2026-01-${String((row % 28) + 1).padStart(2, '0')} 00:00:00+00`)}, ${literal(`kind-${row % 7}`)})`
      })
      await backend.exec(
        `INSERT INTO ${table} (id, group_id, created_at, kind) VALUES ${values.join(',')}`
      )
    }
  }

  const tables = await backend.query<{ count: number }>(
    `SELECT COUNT(*) AS count FROM sqlite_master WHERE type = 'table' AND name IN (${PRODUCTION_SHAPE_TABLE_COUNTS.map(([table]) => literal(table)).join(',')})`
  )
  const indexes = await backend.query<{ count: number }>(
    `SELECT COUNT(*) AS count FROM sqlite_master WHERE type = 'index' AND tbl_name IN (${PRODUCTION_SHAPE_TABLE_COUNTS.map(([table]) => literal(table)).join(',')})`
  )
  let rows = 0
  for (const [table] of PRODUCTION_SHAPE_TABLE_COUNTS) {
    const result = await backend.query<{ count: number }>(
      `SELECT COUNT(*) AS count FROM ${table}`
    )
    rows += Number(result.rows[0]?.count)
  }
  const actual = {
    tables: Number(tables.rows[0]?.count),
    rows,
    indexes: Number(indexes.rows[0]?.count),
  }
  if (JSON.stringify(actual) !== JSON.stringify(PRODUCTION_SHAPE_FIXTURE)) {
    throw new Error(`seed verification failed: ${JSON.stringify(actual)}`)
  }
  await backend.close()
}

async function setPhase(instance: string, phase: string) {
  await Promise.all(
    (['source', 'cache'] as const).map((kind) =>
      json(
        profilePath(kind, instance, `/__profile_phase?phase=${encodeURIComponent(phase)}`)
      )
    )
  )
}

async function cacheState(instance: string): Promise<CacheState> {
  return json(profilePath('cache', instance, '/__profile_state'))
}

async function waitForState(
  instance: string,
  description: string,
  predicate: (state: CacheState) => boolean,
  timeoutMs = 120_000
): Promise<CacheState> {
  const deadline = Date.now() + timeoutMs
  let state = await cacheState(instance)
  while (!predicate(state)) {
    if (Date.now() >= deadline) {
      throw new Error(`${description} timed out: ${JSON.stringify(state)}`)
    }
    await Bun.sleep(50)
    state = await cacheState(instance)
  }
  return state
}

async function kickBoot(instance: string): Promise<number> {
  const response = await fetch(profilePath('cache', instance, '/keepalive'))
  await response.text()
  return response.status
}

async function boot(instance: string): Promise<CacheState> {
  const status = await kickBoot(instance)
  if (status !== 200 && status !== 202) {
    throw new Error(`unexpected keepalive status ${status}`)
  }
  return waitForState(instance, 'wrapper boot', (state) => state.ready)
}

async function stop(instance: string) {
  await json(profilePath('cache', instance, '/__profile_stop'), { method: 'POST' })
}

async function dropReplica(instance: string) {
  return json<{ dropped: number }>(
    profilePath('cache', instance, '/__profile_drop_replica'),
    { method: 'POST' }
  )
}

async function forceBootFailures(instance: string, count: number) {
  await json(profilePath('cache', instance, `/__profile_fail_boots?count=${count}`), {
    method: 'POST',
  })
}

async function report(instance: string, phase: string) {
  const [source, cache] = await Promise.all([
    json<ProfileReport>(profilePath('source', instance, '/__profile_report')),
    json<ProfileReport>(profilePath('cache', instance, '/__profile_report')),
  ])
  return { source: source[phase], cache: cache[phase] }
}

async function fullReport(instance: string) {
  const [source, cache] = await Promise.all([
    json<ProfileReport>(profilePath('source', instance, '/__profile_report')),
    json<ProfileReport>(profilePath('cache', instance, '/__profile_report')),
  ])
  return { source, cache }
}

async function setupReady(instance: string) {
  await setPhase(instance, 'seed')
  await seed(instance)
  await setPhase(instance, 'setup')
  await boot(instance)
  await setPhase(instance, 'faultSetup')
  await stop(instance)
}

async function cleanBootScenario() {
  const instance = 'wrapper-clean'
  await setPhase(instance, 'seed')
  await seed(instance)
  await setPhase(instance, 'cleanBoot')
  const state = await boot(instance)
  const measurements = await report(instance, 'cleanBoot')
  await setPhase(instance, 'cleanup')
  await stop(instance)
  return { state, measurements }
}

async function intactRestartScenario() {
  const instance = 'wrapper-restart'
  await setupReady(instance)
  await setPhase(instance, 'intactRestart')
  const state = await boot(instance)
  const measurements = await report(instance, 'intactRestart')
  await setPhase(instance, 'cleanup')
  await stop(instance)
  return { state, measurements }
}

async function schemaTagResetScenario() {
  const instance = 'wrapper-schema-reset'
  await setupReady(instance)
  await json(profilePath('cache', instance, '/__profile_stale_schema_tag'), {
    method: 'POST',
  })
  await setPhase(instance, 'schemaTagReset')
  const state = await boot(instance)
  const measurements = await report(instance, 'schemaTagReset')
  await setPhase(instance, 'cleanup')
  await stop(instance)
  return { state, measurements }
}

async function partialReplicaScenario() {
  const instance = 'wrapper-partial'
  await setupReady(instance)
  await cacheExec(instance, 'DELETE FROM "_zero.versionHistory"')
  await setPhase(instance, 'partialReplicaRepair')
  const state = await boot(instance)
  const measurements = await report(instance, 'partialReplicaRepair')
  await setPhase(instance, 'cleanup')
  await stop(instance)
  return { state, measurements }
}

async function cdcChangeLogTable(instance: string): Promise<string> {
  // graceful stop removes zero-cache's subscription tables. recreate the exact
  // retained table shape a killed generation leaves behind so the wrapper's
  // boot repair sees the production crash state rather than a clean shutdown.
  const name = 'chat_0/cdc_changeLog'
  await sourceExec(
    instance,
    `CREATE TABLE IF NOT EXISTS ${quotedIdentifier(name)} (watermark TEXT NOT NULL, pos INTEGER NOT NULL, change TEXT, PRIMARY KEY (watermark, pos))`
  )
  return name
}

async function poisonedChangeLogScenario() {
  const instance = 'wrapper-poison'
  await setupReady(instance)
  const changeLog = await cdcChangeLogTable(instance)
  await sourceExec(
    instance,
    `INSERT INTO ${quotedIdentifier(changeLog)} (watermark, pos, change) VALUES (?, ?, ?)`,
    ['profile-poison', 0, JSON.stringify({ tag: 'begin' })]
  )
  await setPhase(instance, 'poisonedChangeLogRepair')
  const state = await boot(instance)
  const measurements = await report(instance, 'poisonedChangeLogRepair')
  await setPhase(instance, 'cleanup')
  await stop(instance)
  return { changeLog, state, measurements }
}

async function retainedChangeStreamerScenario() {
  const instance = 'wrapper-retained-cdc'
  await setupReady(instance)
  const changeLog = await cdcChangeLogTable(instance)
  const retainedRows = PRODUCTION_SHAPE_FIXTURE.rows
  for (let offset = 0; offset < retainedRows; offset += 100) {
    const count = Math.min(100, retainedRows - offset)
    const values = Array.from({ length: count }, (_, index) => {
      const row = offset + index
      return `(${literal(`profile-retained-${String(row).padStart(8, '0')}`)}, 0, ${literal(JSON.stringify({ tag: 'data', row }))})`
    })
    await sourceExec(
      instance,
      `INSERT INTO ${quotedIdentifier(changeLog)} (watermark, pos, change) VALUES ${values.join(',')}`
    )
  }
  await json(profilePath('cache', instance, '/__profile_drop_replica'), {
    method: 'POST',
  })
  await setPhase(instance, 'retainedChangeStreamerClear')
  const state = await boot(instance)
  const measurements = await report(instance, 'retainedChangeStreamerClear')
  await setPhase(instance, 'cleanup')
  await stop(instance)
  return { changeLog, retainedRows, state, measurements }
}

async function nullReplicaRankScenario() {
  const instance = 'wrapper-null-rank'
  await setPhase(instance, 'seed')
  await seed(instance)
  await setPhase(instance, 'setup')
  await boot(instance)
  const replicas = await sourceExec(
    instance,
    "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'chat_%replicas' ORDER BY name LIMIT 1"
  )
  const replicaTable = replicas.rows[0]?.name
  if (typeof replicaTable !== 'string') throw new Error('replicas table was not created')
  await setPhase(instance, 'faultSetup')
  const before = await sourceExec(
    instance,
    `SELECT id, rank FROM ${quotedIdentifier(replicaTable)} ORDER BY rank DESC`
  )
  if (!before.rows.length) throw new Error('replicas table has no active row to poison')
  const replicaId = String(before.rows[0]?.id)
  await sourceExec(
    instance,
    `UPDATE ${quotedIdentifier(replicaTable)} SET rank = NULL WHERE id = ?`,
    [replicaId]
  )
  await setPhase(instance, 'nullReplicaRankRepair')
  await json(profilePath('cache', instance, '/__profile_heal_null_rank'), {
    method: 'POST',
  })
  const measurements = await report(instance, 'nullReplicaRankRepair')
  const after = await sourceExec(
    instance,
    `SELECT id, rank FROM ${quotedIdentifier(replicaTable)} WHERE id = ?`,
    [replicaId]
  )
  await setPhase(instance, 'cleanup')
  await stop(instance)
  return {
    replicaTable,
    replicaId,
    before: before.rows[0],
    after: after.rows[0],
    measurements,
  }
}

async function unhealedNullRankProbeScenario() {
  const instance = 'wrapper-null-rank-crash'
  await setupReady(instance)
  await sourceExec(
    instance,
    `INSERT INTO chat_0_replicas (id, rank, slot, version) VALUES (?, NULL, ?, ?)`,
    ['profile-null-rank', 'profile-null-rank-slot', '00']
  )
  // DoBackend's current serial emulation fills an explicitly NULL rank during
  // INSERT. Corrupt the persisted row afterward to reproduce the pre-fix data.
  await sourceExec(instance, `UPDATE chat_0_replicas SET rank = NULL WHERE id = ?`, [
    'profile-null-rank',
  ])
  const poisoned = await sourceExec(
    instance,
    `SELECT id, rank FROM chat_0_replicas WHERE id = ?`,
    ['profile-null-rank']
  )
  await json(profilePath('cache', instance, '/__profile_skip_rank_heal'), {
    method: 'POST',
  })
  await setPhase(instance, 'unhealedNullRankProbe')
  await kickBoot(instance)
  const outcome = await waitForState(
    instance,
    'unhealed NULL-rank probe',
    (state) => state.ready || (state.failures >= 1 && !state.bootPending),
    10_000
  )
  const measurements = await report(instance, 'unhealedNullRankProbe')
  await setPhase(instance, 'cleanup')
  await stop(instance)
  return { poisoned: poisoned.rows[0], outcome, measurements }
}

async function alarmRetryScenario() {
  const instance = 'wrapper-alarm-retry'
  await setPhase(instance, 'seed')
  await seed(instance)
  await json(profilePath('cache', instance, '/__profile_fail_boots?count=2'), {
    method: 'POST',
  })
  await setPhase(instance, 'alarmRetry')

  await kickBoot(instance)
  const firstFailure = await waitForState(
    instance,
    'first forced wrapper failure',
    (state) => state.failures === 1 && !state.bootPending
  )
  await Bun.sleep(250)
  const withoutRequest = await cacheState(instance)
  if (withoutRequest.bootAttempts !== firstFailure.bootAttempts) {
    throw new Error('failed wrapper boot retried without a new request')
  }

  await kickBoot(instance)
  const secondFailure = await waitForState(
    instance,
    'second forced wrapper failure',
    (state) => state.failures === 2 && !state.bootPending
  )
  await kickBoot(instance)
  const ready = await waitForState(
    instance,
    'backoff-carried wrapper recovery',
    (state) => state.ready,
    30_000
  )
  const measurements = await report(instance, 'alarmRetry')
  await setPhase(instance, 'cleanup')
  await stop(instance)
  return { firstFailure, withoutRequest, secondFailure, ready, measurements }
}

function phaseRows(measurements: PhaseMeasurements) {
  return {
    source: measurements.source?.rowsWritten ?? 0,
    cache: measurements.cache?.rowsWritten ?? 0,
  }
}

function addRows(...values: Array<{ source: number; cache: number }>) {
  return values.reduce(
    (sum, value) => ({
      source: sum.source + value.source,
      cache: sum.cache + value.cache,
    }),
    { source: 0, cache: 0 }
  )
}

function consecutiveFailureSchedule(windowMs: number) {
  const attempts: Array<{ attempt: number; atMs: number; nextDelayMs: number }> = []
  let atMs = 0
  for (let attempt = 1; atMs <= windowMs; attempt++) {
    const nextDelayMs = attempt < 2 ? 0 : Math.min(15_000 * 2 ** (attempt - 2), 300_000)
    attempts.push({ attempt, atMs, nextDelayMs })
    atMs += nextDelayMs
  }
  return attempts
}

async function failedScheduleAttempt(instance: string, phase: string) {
  const before = await cacheState(instance)
  await setPhase(instance, phase)
  const startedAt = Date.now()
  await kickBoot(instance)
  const state = await waitForState(
    instance,
    phase,
    (candidate) =>
      candidate.bootAttempts === before.bootAttempts + 1 &&
      candidate.failures === before.failures + 1 &&
      !candidate.bootPending
  )
  return {
    durationMs: Date.now() - startedAt,
    state,
    measurements: await report(instance, phase),
  }
}

async function readyScheduleAttempt(instance: string, phase: string) {
  const before = await cacheState(instance)
  await setPhase(instance, phase)
  const startedAt = Date.now()
  await kickBoot(instance)
  const state = await waitForState(
    instance,
    phase,
    (candidate) => candidate.ready && candidate.bootAttempts === before.bootAttempts + 1,
    30_000
  )
  return {
    durationMs: Date.now() - startedAt,
    state,
    measurements: await report(instance, phase),
  }
}

async function resetScheduleReplica(instance: string, phase: string) {
  await setPhase(instance, phase)
  const startedAt = Date.now()
  await stop(instance)
  const result = await dropReplica(instance)
  return { durationMs: Date.now() - startedAt, ...result }
}

async function historicalAttemptScheduleScenario() {
  const instance = 'wrapper-attempt-schedule'
  await setPhase(instance, 'seed')
  await seed(instance)

  await forceBootFailures(instance, 2)
  const firstCycle = {
    failed1: await failedScheduleAttempt(instance, 'schedule.first.failed1'),
    failed2: await failedScheduleAttempt(instance, 'schedule.first.failed2'),
    ready: await readyScheduleAttempt(instance, 'schedule.first.ready'),
  }

  const secondReset = await resetScheduleReplica(instance, 'schedule.second.reset')
  await forceBootFailures(instance, 2)
  const stableTwoFailureCycle = {
    failed1: await failedScheduleAttempt(instance, 'schedule.two.failed1'),
    failed2: await failedScheduleAttempt(instance, 'schedule.two.failed2'),
    ready: await readyScheduleAttempt(instance, 'schedule.two.ready'),
  }

  const thirdReset = await resetScheduleReplica(instance, 'schedule.one.reset')
  await forceBootFailures(instance, 1)
  const stableOneFailureCycle = {
    failed1: await failedScheduleAttempt(instance, 'schedule.one.failed1'),
    ready: await readyScheduleAttempt(instance, 'schedule.one.ready'),
  }

  const cycleRows = (cycle: {
    failed1: { measurements: PhaseMeasurements; durationMs: number }
    failed2?: { measurements: PhaseMeasurements; durationMs: number }
    ready: { measurements: PhaseMeasurements; durationMs: number }
  }) =>
    addRows(
      phaseRows(cycle.failed1.measurements),
      ...(cycle.failed2 ? [phaseRows(cycle.failed2.measurements)] : []),
      phaseRows(cycle.ready.measurements)
    )
  const cycleDuration = (cycle: {
    failed1: { durationMs: number }
    failed2?: { durationMs: number }
    ready: { durationMs: number }
  }) =>
    cycle.failed1.durationMs + (cycle.failed2?.durationMs ?? 0) + cycle.ready.durationMs

  // nearest integer form of the incident ratio: 47 attempts and 19 full
  // materializations. distribute 28 failures across the 19 successful cycles
  // as nine two-failure cycles and ten one-failure cycles.
  const modeledRows = addRows(
    cycleRows(firstCycle),
    ...Array.from({ length: 8 }, () => cycleRows(stableTwoFailureCycle)),
    ...Array.from({ length: 10 }, () => cycleRows(stableOneFailureCycle))
  )
  const modeledDurationMs =
    cycleDuration(firstCycle) +
    8 * cycleDuration(stableTwoFailureCycle) +
    10 * cycleDuration(stableOneFailureCycle) +
    8 * secondReset.durationMs +
    10 * thirdReset.durationMs
  const incidentRows = { source: 514_346, cache: 486_876 }
  const firstCycleRows = cycleRows(firstCycle)
  const stableMaterializationRows = cycleRows(stableOneFailureCycle)
  const cacheMatchedMaterializations =
    1 + (incidentRows.cache - firstCycleRows.cache) / stableMaterializationRows.cache
  const cacheMatchedRows = {
    source:
      firstCycleRows.source +
      (cacheMatchedMaterializations - 1) * stableMaterializationRows.source,
    cache: incidentRows.cache,
  }
  const consecutiveFailuresIn25Minutes = consecutiveFailureSchedule(25 * 60_000)

  const allMeasurements = await fullReport(instance)
  await setPhase(instance, 'cleanup')
  await stop(instance)
  return {
    firstCycle,
    stableTwoFailureCycle,
    stableOneFailureCycle,
    resets: { secondReset, thirdReset },
    allMeasurements,
    model: {
      attempts: 47,
      fullMaterializations: 19,
      twoFailureCycles: 9,
      oneFailureCycles: 10,
      rows: modeledRows,
      durationMs: modeledDurationMs,
      incidentRows,
      difference: {
        source: modeledRows.source - incidentRows.source,
        cache: modeledRows.cache - incidentRows.cache,
      },
      cacheMatched: {
        fullMaterializations: cacheMatchedMaterializations,
        rows: cacheMatchedRows,
        difference: {
          source: cacheMatchedRows.source - incidentRows.source,
          cache: 0,
        },
      },
      consecutiveFailureBackoff: {
        windowMs: 25 * 60_000,
        attempts: consecutiveFailuresIn25Minutes.length,
        schedule: consecutiveFailuresIn25Minutes,
      },
    },
  }
}

try {
  for (let attempt = 0; ; attempt++) {
    try {
      if ((await fetch(`${baseURL}/health`)).ok) break
    } catch {}
    if (attempt >= 300) throw new Error('workerd did not become ready')
    await Bun.sleep(100)
  }

  const scenarioRuns = {
    cleanBoot: cleanBootScenario,
    intactRestart: intactRestartScenario,
    schemaTagReset: schemaTagResetScenario,
    partialReplicaRepair: partialReplicaScenario,
    poisonedChangeLogRepair: poisonedChangeLogScenario,
    retainedChangeStreamerClear: retainedChangeStreamerScenario,
    nullReplicaRankRepair: nullReplicaRankScenario,
    unhealedNullRankProbe: unhealedNullRankProbeScenario,
    alarmRetry: alarmRetryScenario,
    historicalAttemptSchedule: historicalAttemptScheduleScenario,
  }
  const requestedScenario = process.env.OREZ_PROFILE_SCENARIO
  if (requestedScenario && !(requestedScenario in scenarioRuns)) {
    throw new Error(`unknown OREZ_PROFILE_SCENARIO ${requestedScenario}`)
  }
  const scenarios: Record<string, unknown> = {}
  for (const [name, run] of Object.entries(scenarioRuns)) {
    if (requestedScenario && name !== requestedScenario) continue
    scenarios[name] = await run()
  }
  const result = {
    measuredAt: new Date().toISOString(),
    fixture: PRODUCTION_SHAPE_FIXTURE,
    build: JSON.parse(
      readFileSync(
        resolve(import.meta.dir, '.generated-chat-wrapper/build-meta.json'),
        'utf8'
      )
    ),
    ...scenarios,
  }
  const serialized = JSON.stringify(result, null, 2)
  if (process.env.OREZ_PROFILE_OUTPUT) {
    writeFileSync(resolve(process.env.OREZ_PROFILE_OUTPUT), serialized + '\n')
  }
  console.log(serialized)
} finally {
  wrangler.kill()
  await wrangler.exited
  rmSync(persistencePath, { recursive: true, force: true })
}
