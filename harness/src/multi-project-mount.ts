// runtime proof for the executor-backed zero-http mount:
// - the stock Zero client + vendored http-pull transport produce the same
//   protocol responses against plain /pull|push and mounted /p-alpha/pull|push
// - two mounted project databases keep rows, cookies, LMIDs, and group owners
//   independent even when their client/group IDs are identical
import { Database } from 'bun:sqlite'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'

import { Zero } from '@rocicorp/zero'

import {
  createZeroHttpMount,
  type ZeroHttpSyncDb as SyncDb,
} from '../../src/zero-http/mount.js'
import { createHarnessSyncServer, type HarnessSyncServer } from './executor-host.js'
import { seedSqlite, userIDFromAuth } from './fixture-data.js'
import { mutators, queries, schema } from './fixture.js'
import { assertServerOutcome } from './server-outcome.js'
import { installHttpPullTransport } from './vendor/httpPullTransport.js'

type Observation = {
  databaseID: string
  operation: 'pull' | 'push'
  response: unknown
}

type Patch = { op?: string; value?: { id?: unknown } }

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

function makeProject(): { sqlite: Database; db: SyncDb; sync: HarnessSyncServer } {
  const sqlite = new Database(':memory:')
  const db = sqliteDb(sqlite)
  seedSqlite(db)
  return {
    sqlite,
    db,
    sync: createHarnessSyncServer(db),
  }
}

function json(
  res: Parameters<Parameters<typeof createServer>[0]>[1],
  value: unknown,
  status = 200
) {
  res.statusCode = status
  res.setHeader('content-type', 'application/json')
  res.end(JSON.stringify(value))
}

function eventually(check: () => void, label: string, timeoutMs = 15_000) {
  const started = Date.now()
  return new Promise<void>((resolve, reject) => {
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

function normalizeClientIDs(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(normalizeClientIDs)
  if (!value || typeof value !== 'object') return value

  const record = value as Record<string, unknown>
  const normalized: Record<string, unknown> = {}
  for (const [key, child] of Object.entries(record)) {
    if (key === 'clientID') {
      normalized.clientID = '$client'
    } else if (key === 'lastMutationIDChanges') {
      normalized.lastMutationIDChanges = Object.values(
        child as Record<string, unknown>
      ).sort()
    } else {
      normalized[key] = normalizeClientIDs(child)
    }
  }
  return normalized
}

const plain = makeProject()
const projects = new Map<string, ReturnType<typeof makeProject>>()
const observations: Observation[] = []
const mount = createZeroHttpMount({
  pathPrefix: '/p-',
  authenticate: () => ({ userID: 'harness' }),
  server(databaseID) {
    let project = projects.get(databaseID)
    if (!project) {
      project = makeProject()
      projects.set(databaseID, project)
    }
    return project.sync
  },
})

const httpServer = createServer(async (req, res) => {
  try {
    if (req.method !== 'POST') {
      json(res, { error: 'not found' }, 404)
      return
    }
    const url = new URL(req.url ?? '/', 'http://localhost')
    const userID = userIDFromAuth(req.headers.authorization)
    if (!userID) {
      json(res, { error: 'missing auth' }, 401)
      return
    }
    const chunks: Buffer[] = []
    for await (const chunk of req) chunks.push(chunk as Buffer)
    const body = JSON.parse(Buffer.concat(chunks).toString() || 'null')

    let databaseID: string
    let operation: 'pull' | 'push'
    let response: unknown
    if (url.pathname === '/pull' || url.pathname === '/push') {
      databaseID = 'plain'
      operation = url.pathname === '/pull' ? 'pull' : 'push'
      response =
        operation === 'pull'
          ? await plain.sync.handlePull(body, { userID })
          : await plain.sync.handlePush(body, { userID })
    } else {
      const route = mount.match(url.pathname)
      if (!route) {
        json(res, { error: 'not found' }, 404)
        return
      }
      databaseID = route.databaseID
      operation = route.operation
      response = await mount.handle(route, body, { userID })
    }
    observations.push({ databaseID, operation, response })
    json(res, response)
  } catch (error) {
    json(res, { error: String(error) }, (error as { status?: number }).status ?? 500)
  }
})

await new Promise<void>((resolve) => httpServer.listen(0, '127.0.0.1', resolve))
const address = httpServer.address()
if (!address || typeof address === 'string') throw new Error('missing harness address')
const baseURL = `http://127.0.0.1:${address.port}`

async function runClient(databaseID: 'plain' | 'alpha') {
  const origin = databaseID === 'plain' ? baseURL : `${baseURL}/p-${databaseID}`
  const transport = installHttpPullTransport({ origin })
  const zero = new Zero({
    server: origin,
    userID: 'u1',
    auth: 'token-u1',
    schema,
    mutators,
    kvStore: 'mem',
    storageKey: `mount-parity-${databaseID}`,
  })
  const view = zero.materialize(queries.allProjects())
  let rows: { id: string; name: string }[] = []
  let complete = false
  view.addListener((value, resultType) => {
    rows = JSON.parse(JSON.stringify(value))
    complete = resultType === 'complete'
  })

  try {
    await eventually(() => {
      assert.equal(complete, true)
      assert.equal(rows.length, 12)
    }, `${databaseID} initial hydration`)

    const created = zero.mutate(
      mutators.project.create({
        id: 'transport-parity',
        ownerId: 'u1',
        name: 'transport parity',
      })
    )
    await created.client
    await assertServerOutcome(created.server, 'success', `${databaseID} project.create`)
    await transport.pull()
    await eventually(() => {
      assert.equal(
        rows.some((row) => row.id === 'transport-parity'),
        true
      )
    }, `${databaseID} mutation convergence`)

    const lane = observations.filter((entry) => entry.databaseID === databaseID)
    const initial = lane.find(
      (entry) =>
        entry.operation === 'pull' &&
        Array.isArray((entry.response as { rowsPatch?: unknown[] }).rowsPatch) &&
        (entry.response as { rowsPatch: { op?: string }[] }).rowsPatch[0]?.op === 'clear'
    )
    const pushed = lane.find((entry) => entry.operation === 'push')
    const changed = lane.find(
      (entry) =>
        entry.operation === 'pull' &&
        ((entry.response as { rowsPatch?: Patch[] }).rowsPatch ?? []).some(
          (patch) => patch.op === 'put' && patch.value?.id === 'transport-parity'
        )
    )
    assert(initial && pushed && changed, `${databaseID} missing protocol observation`)
    return [initial.response, pushed.response, changed.response].map(normalizeClientIDs)
  } finally {
    view.destroy()
    await zero.close()
    transport.uninstall()
  }
}

try {
  const plainTranscript = await runClient('plain')
  const mountedTranscript = await runClient('alpha')
  assert.deepEqual(mountedTranscript, plainTranscript)

  async function post(path: string, body: unknown, userID: string) {
    const response = await fetch(`${baseURL}${path}`, {
      method: 'POST',
      headers: {
        authorization: `Bearer token-${userID}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify(body),
    })
    return { status: response.status, body: await response.text() }
  }

  const malformed = { clientID: 'c', clientGroupID: 'g', cookie: 'bad' }
  assert.deepEqual(
    await post('/p-alpha/pull', malformed, 'u1'),
    await post('/pull', malformed, 'u1')
  )
  const future = { clientID: 'future', clientGroupID: 'future', cookie: 99 }
  assert.deepEqual(
    await post('/p-alpha/pull', future, 'u1'),
    await post('/pull', future, 'u1')
  )

  const pull = (cookie: number | null) => ({
    clientID: 'same-client',
    clientGroupID: 'same-group',
    cookie,
  })
  const push = (label: string) => ({
    clientGroupID: 'same-group',
    mutations: [
      {
        type: 'custom',
        id: 1,
        clientID: 'same-client',
        name: 'project.create',
        args: [{ id: 'project-only', ownerId: 'owner', name: label }],
      },
    ],
    pushVersion: 1,
  })

  const redInitial = await post('/p-red/pull', pull(null), 'red-user')
  const blueInitial = await post('/p-blue/pull', pull(null), 'blue-user')
  assert.equal(JSON.parse(redInitial.body).cookie, 0)
  assert.equal(JSON.parse(blueInitial.body).cookie, 0)
  assert.equal((await post('/p-red/push', push('red only'), 'red-user')).status, 200)
  assert.equal(await projects.get('red')!.sync.watermark(), 2)
  assert.equal(await projects.get('blue')!.sync.watermark(), 0)
  assert.deepEqual(JSON.parse((await post('/p-blue/pull', pull(0), 'blue-user')).body), {
    cookie: 0,
    unchanged: true,
  })
  assert.equal((await post('/p-blue/push', push('blue only'), 'blue-user')).status, 200)

  const redChanged = JSON.parse((await post('/p-red/pull', pull(0), 'red-user')).body)
  const blueChanged = JSON.parse((await post('/p-blue/pull', pull(0), 'blue-user')).body)
  assert.equal(redChanged.cookie, 2)
  assert.equal(blueChanged.cookie, 2)
  assert.deepEqual(redChanged.lastMutationIDChanges, { 'same-client': 1 })
  assert.deepEqual(blueChanged.lastMutationIDChanges, { 'same-client': 1 })
  assert.equal(
    projects.get('red')!.db.all(`SELECT name FROM project WHERE id = 'project-only'`)[0]!
      .name,
    'red only'
  )
  assert.equal(
    projects.get('blue')!.db.all(`SELECT name FROM project WHERE id = 'project-only'`)[0]!
      .name,
    'blue only'
  )
  const forbidden = await post('/p-red/pull', pull(0), 'blue-user')
  assert.equal(forbidden.status, 403)
  assert.match(forbidden.body, /different user/)

  console.log(
    '[multi-project-mount] PASS: plain/mounted transport parity + independent red/blue cursor state'
  )
} finally {
  await new Promise<void>((resolve, reject) =>
    httpServer.close((error) => (error ? reject(error) : resolve()))
  )
  plain.sqlite.close()
  for (const project of projects.values()) project.sqlite.close()
}
