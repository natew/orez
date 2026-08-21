import assert from 'node:assert/strict'
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'

import { findPort } from '../../src/port.ts'

const fixture = mkdtempSync(join(import.meta.dirname, '.application-push-workerd-'))
const port = await findPort(0)
const inspectorPort = await findPort(0)

writeFileSync(
  join(fixture, 'worker.ts'),
  `import { createOrezDataWorker } from '../src/cf-do/lite-data-worker.js'

const schema = {
  version: 'application-push-v1',
  schema: {
    tables: {
      alpha: {
        name: 'alpha',
        columns: { id: { type: 'string' as const } },
        primaryKey: ['id'] as const,
      },
    },
    relationships: { alpha: {} },
  },
  publicTables: [{ table: 'alpha', publicTable: 'alpha' }],
  async migrate({ client }) {
    await client.exec('CREATE TABLE IF NOT EXISTS alpha (id TEXT PRIMARY KEY)')
    await client.registerTables([{ table: 'alpha', publicTable: 'alpha' }])
  },
}

const dataWorker = createOrezDataWorker({
  name: 'pushproof',
  schema,
  applicationPush: async ({ input, instance, applicationSql }) => {
    if (!input || typeof input !== 'object' || !('kind' in input)) {
      return new Response('push kind is required', { status: 400 })
    }
    const sql = applicationSql()
    if (input.kind === 'read') {
      return Response.json({
        instance,
        sqlNamespace: sql.namespace,
        rows: await sql.query('SELECT id FROM alpha ORDER BY id'),
      })
    }
    if (input.kind !== 'write' || !('id' in input) || typeof input.id !== 'string') {
      return new Response('write id is required', { status: 400 })
    }
    await sql.exec('INSERT INTO alpha (id) VALUES (?)', [input.id])
    return Response.json(
      { id: input.id, instance, sqlNamespace: sql.namespace },
      { status: 201, headers: { 'x-application-push': 'local' } }
    )
  },
  async routes({ request, url, executeApplicationPush }) {
    const [namespace, action, extra] = url.pathname.split('/').filter(Boolean)
    if (
      request.method !== 'POST' ||
      action !== 'application-push' ||
      extra !== undefined ||
      !namespace ||
      !/^proj-[A-Za-z0-9_-]+$/.test(namespace)
    ) {
      return null
    }
    return executeApplicationPush(await request.json(), namespace)
  },
})

export const ZeroDO = dataWorker.ZeroDO
export default dataWorker
`
)

writeFileSync(
  join(fixture, 'wrangler.toml'),
  `name = "orez-application-push"
main = "worker.ts"
compatibility_date = "2026-06-01"
compatibility_flags = ["nodejs_compat"]

[vars]
OREZ_DO_WRITE_BUDGET_ADMIN_TOKEN = "application-push-admin"

[[durable_objects.bindings]]
name = "ZERO_SQL_DO"
class_name = "ZeroDO"

[[migrations]]
tag = "v1"
new_sqlite_classes = ["ZeroDO"]
`
)

const server = Bun.spawn(
  [
    'bunx',
    'wrangler',
    'dev',
    '--config',
    'wrangler.toml',
    '--local',
    '--port',
    String(port),
    '--inspector-port',
    String(inspectorPort),
  ],
  { cwd: fixture, stdout: 'inherit', stderr: 'inherit' }
)
const base = `http://127.0.0.1:${port}`
const namespace = 'proj-application-push'

const status = () =>
  fetch(`${base}/${namespace}/_orez/status`, {
    headers: { 'x-orez-admin-token': 'application-push-admin' },
  }).then((response) => response.json())

try {
  for (let attempt = 0; ; attempt++) {
    try {
      await fetch(`${base}/readiness`)
      break
    } catch {}
    if (server.exitCode !== null) {
      throw new Error(`application push workerd fixture exited with ${server.exitCode}`)
    }
    if (attempt >= 200) {
      throw new Error('application push workerd fixture did not become ready')
    }
    await Bun.sleep(100)
  }

  const migration = await fetch(`${base}/${namespace}/_orez/schema/migrate`, {
    method: 'POST',
  })
  assert.equal(migration.status, 200)
  const before = await status()
  const write = await fetch(`${base}/${namespace}/application-push`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ kind: 'write', id: 'alpha-1' }),
  })
  assert.equal(write.status, 201)
  assert.equal(write.headers.get('x-application-push'), 'local')
  assert.deepEqual(await write.json(), {
    id: 'alpha-1',
    instance: `ns:${namespace}`,
    sqlNamespace: `ns:${namespace}`,
  })

  const read = await fetch(`${base}/${namespace}/application-push`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ kind: 'read' }),
  })
  assert.equal(read.status, 200)
  assert.deepEqual(await read.json(), {
    instance: `ns:${namespace}`,
    sqlNamespace: `ns:${namespace}`,
    rows: [{ id: 'alpha-1' }],
  })
  const after = await status()
  assert.equal(
    after.requestsSinceBoot.applicationSqlSessions -
      before.requestsSinceBoot.applicationSqlSessions,
    2
  )
} finally {
  server.kill()
  await server.exited
  rmSync(fixture, { recursive: true, force: true })
}
