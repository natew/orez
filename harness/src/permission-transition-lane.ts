// Live lane for the frozen populated-cache permission-transition v1 profile.
// ONE http host serves two child namespaces via createSyncServerMount — each its
// own in-process sqlite db + createSyncServer behind the fixture visible()
// policy — so identical protected ids never collide and the two namespaces are
// honestly the same host, not two processes. nsA runs a grant then a revoke;
// nsB stays authorized. A disjoint sentinel scope permanently grants every
// participant; its fresh marker at each epoch, observed complete by every
// original AND fresh client, proves the client is live so "no protected rows"
// is a real revoke and not a lagging cache. Named views drive sync; pre-armed
// raw zql views read the local cache only. Empty fault schedule.
//
//   bun src/permission-transition-lane.ts --target orez-local --seed example
import { Database } from 'bun:sqlite'
import { execFileSync } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { createServer, type Server } from 'node:http'
import { basename, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { parseArgs } from 'node:util'

import { Zero } from '@rocicorp/zero'

import {
  createSyncServer,
  createSyncServerMount,
  type SyncDb,
  type SyncServer,
} from '../../src/sync-server/sync-server'
import { FAULT_SCHEDULE_SCHEMA_VERSION } from './consistency/fault-schedule.js'
import { writePermissionArtifacts } from './consistency/permission-artifacts.js'
import {
  derivePermissionScenario,
  permissionReplayCommand,
  projectProtectedObservation,
  sentinelAclRows,
  validatePermissionScenario,
  type ProtectedObservation,
} from './consistency/permission-transition-workload.js'
import {
  checkPermissionTransition,
  classifyPermissionOutcome,
  PERMISSION_CHECKS_SCHEMA_VERSION,
  PERMISSION_HISTORY_SCHEMA_VERSION,
  PERMISSION_TRANSITION_PROFILE,
  PERMISSION_TRANSITION_PROFILE_VERSION,
  type PermissionEpoch,
  type PermissionEvent,
} from './consistency/permission-transition.js'
import { TABLES, executeMutator, seedSqlite, userIDFromAuth } from './fixture-data.js'
import { fixtureVisibility } from './fixture-visibility.js'
import { mutators, queries, schema, zql } from './fixture.js'
import { ensureHttpPullTransport } from './vendor/httpPullTransport.js'

const { values: args } = parseArgs({
  options: {
    target: { type: 'string', default: 'orez-local' },
    seed: { type: 'string' },
    replay: { type: 'boolean', default: false },
    'results-dir': { type: 'string' },
  },
})

if (args.target !== 'orez-local') {
  throw new Error(
    `permission-transition lane runs same-host child namespaces on orez-local only, not ${args.target}`
  )
}

const seed = args.seed ?? `run-${process.pid}-${process.hrtime.bigint()}`
if (!/^[A-Za-z0-9._:-]+$/.test(seed)) {
  throw new Error(
    'seed must contain only letters, digits, dot, underscore, colon, or dash'
  )
}
const scenario = derivePermissionScenario(seed)
validatePermissionScenario(scenario)
const scenarioId = `permission-transition-${scenario.digest}`
const defaultResultsName = args.replay
  ? `${scenarioId}-replay-${randomUUID().slice(0, 8)}`
  : scenarioId
const resultsDir =
  args['results-dir'] ??
  join('target', 'consistency', 'permission-transition', defaultResultsName)

let build = 'unknown'
try {
  build = execFileSync('git', ['rev-parse', 'HEAD'], {
    cwd: fileURLToPath(new URL('../..', import.meta.url)),
    encoding: 'utf8',
  }).trim()
} catch {
  // provenance is best-effort; the lane still runs outside a git checkout
}

const PT = scenario.protectedIds
const SN = scenario.sentinel

// ---- the host: two child namespaces on one process ------------------------

type Namespace = { id: string; sqlite: Database; db: SyncDb; sync: SyncServer }

function sqliteDb(sqlite: Database): SyncDb {
  return {
    exec(sql, params = []) {
      sqlite.query(sql).run(...(params as never[]))
    },
    all(sql, params = []) {
      return sqlite.query(sql).all(...(params as never[])) as Record<string, unknown>[]
    },
    transaction<T>(fn: () => T): T {
      return sqlite.transaction(fn)() as T
    },
  }
}

// Seed a namespace: base fixture (harmless — invisible to our principals), then
// the protected scope (marker in project name + task title) and the disjoint
// sentinel scope (sn-project owned by a non-participant, permanently granting
// every participant via membership). The protected subject membership is
// present only when authorized at start.
function makeNamespace(
  id: string,
  marker: string,
  subjectPrincipal: string,
  sentinelMembers: string[],
  subjectAuthorized: boolean
): Namespace {
  const sqlite = new Database(':memory:')
  const db = sqliteDb(sqlite)
  seedSqlite(db)
  const sync = createSyncServer({
    db,
    tables: TABLES,
    mutate: executeMutator,
    visible: fixtureVisibility,
  })

  db.exec(`INSERT INTO project (id, "ownerId", name) VALUES (?, ?, ?)`, [
    PT.project,
    scenario.principals.owner,
    marker,
  ])
  db.exec(
    `INSERT INTO task (id, "projectId", title, rank, done, meta, "dueAt")
     VALUES (?, ?, ?, 1, 0, NULL, NULL)`,
    [PT.task, PT.project, marker]
  )
  if (subjectAuthorized) {
    db.exec(`INSERT INTO member (id, "projectId", "userId") VALUES (?, ?, ?)`, [
      PT.member,
      PT.project,
      subjectPrincipal,
    ])
  }

  db.exec(`INSERT INTO project (id, "ownerId", name) VALUES (?, 'sentinel-holder', ?)`, [
    SN.project,
    scenario.sentinelMarkers[0],
  ])
  db.exec(
    `INSERT INTO task (id, "projectId", title, rank, done, meta, "dueAt")
     VALUES (?, ?, ?, 1, 0, NULL, NULL)`,
    [SN.task, SN.project, scenario.sentinelMarkers[0]]
  )
  const grantees =
    id === scenario.namespaces.transition
      ? [scenario.principals.owner, scenario.principals.subjectTransition]
      : [scenario.principals.subjectStable]
  sentinelMembers.forEach((memberId, index) => {
    db.exec(`INSERT INTO member (id, "projectId", "userId") VALUES (?, ?, ?)`, [
      memberId,
      SN.project,
      grantees[index]!,
    ])
  })

  return { id, sqlite, db, sync }
}

const nsA = makeNamespace(
  scenario.namespaces.transition,
  scenario.markers.transition,
  scenario.principals.subjectTransition,
  SN.members.transition,
  false
)
const nsB = makeNamespace(
  scenario.namespaces.stable,
  scenario.markers.stable,
  scenario.principals.subjectStable,
  SN.members.stable,
  true
)
const namespaces = new Map<string, Namespace>([
  [nsA.id, nsA],
  [nsB.id, nsB],
])

type PullEcho = { databaseID: string; clientID: string; clientGroupID: string }
const echoes: PullEcho[] = []

const mount = createSyncServerMount({
  pathPrefix: '/',
  server(databaseID) {
    const ns = namespaces.get(databaseID)
    if (!ns) throw new Error(`unknown namespace ${databaseID}`)
    return ns.sync
  },
})

const httpServer: Server = createServer(async (req, res) => {
  try {
    if (req.method !== 'POST') {
      res.statusCode = 404
      res.end()
      return
    }
    const url = new URL(req.url ?? '/', 'http://localhost')
    const userID = userIDFromAuth(req.headers.authorization)
    if (!userID) {
      res.statusCode = 401
      res.end(JSON.stringify({ error: 'missing auth' }))
      return
    }
    const chunks: Buffer[] = []
    for await (const chunk of req) chunks.push(chunk as Buffer)
    const body = JSON.parse(Buffer.concat(chunks).toString() || 'null')
    const route = mount.match(url.pathname)
    if (!route) {
      res.statusCode = 404
      res.end()
      return
    }
    const response = mount.handle(route, body, userID)
    if (
      route.operation === 'pull' &&
      body &&
      typeof body.clientID === 'string' &&
      typeof body.clientGroupID === 'string'
    ) {
      echoes.push({
        databaseID: route.databaseID,
        clientID: body.clientID,
        clientGroupID: body.clientGroupID,
      })
    }
    res.setHeader('content-type', 'application/json')
    res.end(JSON.stringify(response))
  } catch (error) {
    const status = (error as { status?: number }).status ?? 500
    res.statusCode = status
    res.end(JSON.stringify({ error: String(error) }))
  }
})

await new Promise<void>((resolve) => httpServer.listen(0, '127.0.0.1', resolve))
const address = httpServer.address()
if (!address || typeof address === 'string') throw new Error('missing host address')
const host = `http://127.0.0.1:${address.port}`

const transports = new Map<string, ReturnType<typeof ensureHttpPullTransport>>()
function transportFor(namespaceId: string) {
  const origin = `${host}/${namespaceId}`
  let transport = transports.get(origin)
  if (!transport) {
    transport = ensureHttpPullTransport({ origin, pullIntervalMs: 100 })
    transports.set(origin, transport)
  }
  return transport
}

// ---- client watcher: named protected + sentinel views + raw local views ---

type Row = Record<string, unknown>
const clone = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T

type ClientWatcher = {
  id: string
  namespace: string
  principal: string
  clientId: string
  groupId: string
  storageKey: string
  fresh: boolean
  settledAt(epoch: PermissionEpoch): boolean
  observation(origin: 'named' | 'raw'): ProtectedObservation
  echoed(): boolean
  destroy(): Promise<void>
}

async function createClient(
  id: string,
  namespaceId: string,
  principal: string,
  fresh: boolean
): Promise<ClientWatcher> {
  const origin = `${host}/${namespaceId}`
  transportFor(namespaceId)
  const storageKey = `permission-transition-${id}-${scenario.digest}`
  const zero = new Zero({
    server: origin,
    userID: principal,
    auth: `token-${principal}`,
    schema,
    mutators,
    kvStore: 'mem',
    storageKey,
  })
  const clientId = zero.clientID
  const groupId = await zero.clientGroupID

  const named = zero.materialize(queries.projectById({ id: PT.project }))
  const sentinel = zero.materialize(queries.projectById({ id: SN.project }))
  const rawProject = zero.materialize(zql.project.where('id', PT.project))
  const rawMember = zero.materialize(zql.member.where('projectId', PT.project))
  const rawTask = zero.materialize(zql.task.where('projectId', PT.project))

  let namedRow: Row | undefined
  let namedComplete = false
  let sentinelRow: Row | undefined
  let sentinelComplete = false
  let rawP: Row[] = []
  let rawM: Row[] = []
  let rawT: Row[] = []

  named.addListener((data, resultType) => {
    namedRow = data ? clone(data as Row) : undefined
    if (resultType === 'complete') namedComplete = true
  })
  sentinel.addListener((data, resultType) => {
    sentinelRow = data ? clone(data as Row) : undefined
    if (resultType === 'complete') sentinelComplete = true
  })
  rawProject.addListener((data) => (rawP = clone(data as Row[])))
  rawMember.addListener((data) => (rawM = clone(data as Row[])))
  rawTask.addListener((data) => (rawT = clone(data as Row[])))

  const relatedTasks = (row: Row | undefined): Row[] => (row?.tasks as Row[]) ?? []
  const relatedMembers = (row: Row | undefined): Row[] => (row?.members as Row[]) ?? []

  return {
    id,
    namespace: namespaceId,
    principal,
    clientId,
    groupId,
    storageKey,
    fresh,
    settledAt(epoch) {
      if (!namedComplete || !sentinelComplete) return false
      const task = relatedTasks(sentinelRow).find((t) => t.id === SN.task)
      return task?.title === scenario.sentinelMarkers[epoch]
    },
    observation(origin) {
      if (origin === 'named') {
        return {
          project: namedRow
            ? [{ id: String(namedRow.id), marker: String(namedRow.name) }]
            : [],
          member: relatedMembers(namedRow).map((m) => ({ id: String(m.id) })),
          task: relatedTasks(namedRow).map((t) => ({
            id: String(t.id),
            marker: String(t.title),
          })),
        }
      }
      return {
        project: rawP.map((r) => ({ id: String(r.id), marker: String(r.name) })),
        member: rawM.map((r) => ({ id: String(r.id) })),
        task: rawT.map((r) => ({ id: String(r.id), marker: String(r.title) })),
      }
    },
    echoed() {
      return echoes.some((e) => e.clientID === clientId && e.clientGroupID === groupId)
    },
    async destroy() {
      named.destroy()
      sentinel.destroy()
      rawProject.destroy()
      rawMember.destroy()
      rawTask.destroy()
      await zero.close()
    },
  }
}

// ---- typed history recording ----------------------------------------------

const events: PermissionEvent[] = []
let seq = 0
function emit(spec: Omit<PermissionEvent, 'v' | 'index' | 'host'>): void {
  events.push({
    v: PERMISSION_HISTORY_SCHEMA_VERSION,
    host,
    index: seq++,
    ...spec,
  } as PermissionEvent)
}

function eventually(check: () => void, label: string, timeoutMs = 30_000): Promise<void> {
  const started = Date.now()
  return new Promise((resolve, reject) => {
    const poll = () => {
      try {
        check()
        resolve()
      } catch (error) {
        if (Date.now() - started >= timeoutMs) {
          reject(new Error(`timeout waiting for ${label}: ${String(error)}`))
          return
        }
        setTimeout(poll, 25)
      }
    }
    poll()
  })
}

async function settle(clients: ClientWatcher[], epoch: PermissionEpoch): Promise<void> {
  await eventually(() => {
    for (const client of clients) {
      if (!client.settledAt(epoch))
        throw new Error(`${client.id} not live at epoch ${epoch}`)
      if (!client.echoed()) throw new Error(`${client.id} pull identity not yet echoed`)
    }
  }, `epoch ${epoch} liveness`)
}

function snapshot(clients: ClientWatcher[], epoch: PermissionEpoch): void {
  for (const client of clients) {
    for (const origin of ['named', 'raw'] as const) {
      const { rows, markers } = projectProtectedObservation(client.observation(origin))
      emit({
        type: 'client',
        opId: `${client.id}-${origin}-${epoch}`,
        epoch,
        namespace: client.namespace,
        principal: client.principal,
        clientId: client.clientId,
        groupId: client.groupId,
        storageKey: client.storageKey,
        origin,
        rows,
        markers,
        complete: true,
        fresh: client.fresh,
        pullEchoed: client.echoed(),
      })
    }
  }
}

function recordSentinelAcl(epoch: PermissionEpoch): void {
  for (const [ns, role] of [
    [nsA, 'transition'],
    [nsB, 'stable'],
  ] as const) {
    const observed = ns.db
      .all(`SELECT id FROM member WHERE "projectId" = ?`, [SN.project])
      .map((row) => `member:${row.id as string}`)
      .sort()
    const expected = sentinelAclRows(scenario, role)
    if (JSON.stringify(observed) !== JSON.stringify(expected)) {
      throw new Error(
        `sentinel ACL for ${ns.id} is ${JSON.stringify(observed)}, expected ${JSON.stringify(expected)}`
      )
    }
    emit({
      type: 'authority',
      scope: 'sentinel-acl',
      opId: `acl-${role}-${epoch}`,
      epoch,
      namespace: ns.id,
      rows: observed,
    })
  }
}

function adminChange(action: 'grant' | 'revoke', epoch: PermissionEpoch): void {
  let phase: 'ok' | 'info' = 'ok'
  let sqlReturned = true
  try {
    if (action === 'grant') {
      nsA.db.exec(`INSERT INTO member (id, "projectId", "userId") VALUES (?, ?, ?)`, [
        PT.member,
        PT.project,
        scenario.principals.subjectTransition,
      ])
    } else {
      nsA.db.exec(`DELETE FROM member WHERE id = ?`, [PT.member])
    }
  } catch {
    // an ambiguous admin error is inconclusive, not an authoritative change
    phase = 'info'
    sqlReturned = false
  }
  const count = Number(
    nsA.db.all(
      `SELECT COUNT(*) AS c FROM member WHERE "projectId" = ? AND "userId" = ?`,
      [PT.project, scenario.principals.subjectTransition]
    )[0]!.c
  )
  const authorityOp = `membership-${epoch}`
  emit({
    type: 'authority',
    scope: 'protected-membership',
    opId: authorityOp,
    epoch,
    namespace: nsA.id,
    principal: scenario.principals.subjectTransition,
    count,
  })
  emit({
    type: 'change',
    opId: action,
    epoch,
    namespace: nsA.id,
    principal: scenario.principals.subjectTransition,
    action,
    phase,
    sqlReturned,
    authorityRef: authorityOp,
  })
}

function advanceSentinel(epoch: PermissionEpoch): void {
  for (const ns of [nsA, nsB]) {
    ns.db.exec(`UPDATE task SET title = ? WHERE id = ?`, [
      scenario.sentinelMarkers[epoch],
      SN.task,
    ])
  }
}

async function pullBoth(): Promise<void> {
  await Promise.all([...transports.values()].map((transport) => transport.pull()))
}

// ---- drive the epochs ------------------------------------------------------

const allClients: ClientWatcher[] = []
try {
  const owner = await createClient('cAo', nsA.id, scenario.principals.owner, false)
  const subject = await createClient(
    'cAs',
    nsA.id,
    scenario.principals.subjectTransition,
    false
  )
  const stable = await createClient(
    'cBs',
    nsB.id,
    scenario.principals.subjectStable,
    false
  )
  const originals = [owner, subject, stable]
  allClients.push(...originals)

  // epoch 0: initial isolation
  await settle(originals, 0)
  recordSentinelAcl(0)
  snapshot(originals, 0)
  emit({
    type: 'barrier',
    opId: 'barrier-0',
    epoch: 0,
    marker: scenario.sentinelMarkers[0],
    complete: true,
    observers: [...new Set(originals.map((c) => c.clientId))].sort(),
  })

  // epoch 1: grant the subject, advance the sentinel, add fresh clients
  adminChange('grant', 1)
  advanceSentinel(1)
  await pullBoth()
  const freshGrant = [
    await createClient('fAs1', nsA.id, scenario.principals.subjectTransition, true),
    await createClient('fBs1', nsB.id, scenario.principals.subjectStable, true),
  ]
  allClients.push(...freshGrant)
  const atGrant = [...originals, ...freshGrant]
  await settle(atGrant, 1)
  recordSentinelAcl(1)
  snapshot(atGrant, 1)
  emit({
    type: 'barrier',
    opId: 'barrier-1',
    epoch: 1,
    marker: scenario.sentinelMarkers[1],
    complete: true,
    observers: [...new Set(atGrant.map((c) => c.clientId))].sort(),
  })

  // epoch 2: revoke the subject, advance the sentinel, add fresh clients
  adminChange('revoke', 2)
  advanceSentinel(2)
  await pullBoth()
  const freshRevoke = [
    await createClient('fAs2', nsA.id, scenario.principals.subjectTransition, true),
    await createClient('fBs2', nsB.id, scenario.principals.subjectStable, true),
  ]
  allClients.push(...freshRevoke)
  const atRevoke = [...originals, ...freshRevoke]
  await settle(atRevoke, 2)
  recordSentinelAcl(2)
  snapshot(atRevoke, 2)
  emit({
    type: 'barrier',
    opId: 'barrier-2',
    epoch: 2,
    marker: scenario.sentinelMarkers[2],
    complete: true,
    observers: [...new Set(atRevoke.map((c) => c.clientId))].sort(),
  })

  const outcome = checkPermissionTransition(events)
  const result = classifyPermissionOutcome(events, outcome)
  const replay = permissionReplayCommand(args.target!, seed)

  await writePermissionArtifacts({
    resultsDir,
    manifest: {
      schemaVersion: PERMISSION_CHECKS_SCHEMA_VERSION,
      kind: 'orez-permission-transition',
      runId: basename(resultsDir),
      seed: {
        value: seed,
        source: args.replay ? 'replay' : args.seed ? 'fixed' : 'random',
      },
      profile: {
        name: PERMISSION_TRANSITION_PROFILE.name,
        version: PERMISSION_TRANSITION_PROFILE_VERSION,
        historySchemaVersion: PERMISSION_HISTORY_SCHEMA_VERSION,
        checksSchemaVersion: PERMISSION_CHECKS_SCHEMA_VERSION,
      },
      host,
      namespaces: scenario.namespaces,
      target: { name: args.target, build },
      replay: { command: replay },
    },
    history: events,
    schedule: {
      schemaVersion: FAULT_SCHEDULE_SCHEMA_VERSION,
      faultsRequired: false,
      plans: [],
      receipts: [],
    },
    checks: {
      schemaVersion: PERMISSION_CHECKS_SCHEMA_VERSION,
      kind: 'orez-permission-transition-checks',
      result,
      checks: [
        {
          name: PERMISSION_TRANSITION_PROFILE.name,
          version: String(PERMISSION_TRANSITION_PROFILE_VERSION),
          valid: outcome.valid,
          violations: outcome.violations,
        },
      ],
    },
  })

  console.log(`[permission-transition] ${result.toUpperCase()} ${resultsDir}`)
  console.log(`[permission-transition] replay: ${replay}`)
  if (result !== 'pass') {
    throw new Error(
      `permission transition ${result}:\n${outcome.violations.join('\n') || '(no violations)'}`
    )
  }
} finally {
  for (const client of allClients) await client.destroy()
  for (const transport of transports.values()) transport.uninstall()
  await new Promise<void>((resolve, reject) =>
    httpServer.close((error) => (error ? reject(error) : resolve()))
  )
  nsA.sqlite.close()
  nsB.sqlite.close()
}
