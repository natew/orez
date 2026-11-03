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
import { ZeroDO as BudgetBase } from '../src/cf-do/worker.js'

export class BudgetProbe extends BudgetBase {
  constructor(ctx, env) {
    super(ctx, { ...env, OREZ_DO_WRITE_BUDGET_ROWS: '56' })
  }
  async setup() {
    await this.applicationSqlLocalClient('budget').exec(
      'CREATE TABLE IF NOT EXISTS comment (id TEXT PRIMARY KEY, post INTEGER NOT NULL)'
    )
    const client = this.applicationSqlLocalClient('budget')
    await client.registerTables([{ table: 'comment', publicTable: 'comment', publish: false }])
    await client.exec("INSERT INTO comment VALUES ('kept', 1) RETURNING id")
    this.sql.exec('CREATE TABLE budget_padding (id INTEGER PRIMARY KEY)')
    // real metered writes put the next journal marker + RETURNING insert over 56.
    const padding = 52 - this.writeBudget.status().windowRows
    if (padding < 0) throw new Error('budget fixture setup exceeded its priming target')
    if (padding) this.sql.exec('WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < ?) INSERT INTO budget_padding SELECT i FROM n', padding)
    return this.bootID
  }
  async write(at) {
    try {
      await this.applicationSqlLocalClient('budget').transaction(null, async (tx) => {
        for (let i = 0; i < 5; i++) {
          await tx.exec('INSERT INTO comment (id, post) VALUES (?, 1) ON CONFLICT (id) DO NOTHING RETURNING id AS id', [\`c-\${at}-\${i}\`])
        }
      })
      return { ok: true, bootID: this.bootID }
    } catch (error) {
      return { error: String(error), bootID: this.bootID }
    }
  }
  async state() {
    const budget = await this.fetch(new Request('https://fixture.invalid/_orez/write-budget')).then(r => r.json())
    return {
      bootID: this.bootID,
      comments: this.ctx.storage.sql.exec('SELECT count(*) AS n FROM comment').one().n,
      tripped: budget.tripped,
      trippedStatement: budget.trippedStatement,
    }
  }
}

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
export default {
  fetch(request, env, ctx) {
    const url = new URL(request.url)
    if (url.pathname.startsWith('/budget/')) {
      const object = env.BUDGET.get(env.BUDGET.idFromName('budget'))
      if (url.pathname === '/budget/setup') return object.setup().then(Response.json)
      if (url.pathname === '/budget/write') return object.write(url.searchParams.get('at')).then(Response.json)
      return object.state().then(Response.json)
    }
    return dataWorker.fetch(request, env, ctx)
  },
}
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

[[durable_objects.bindings]]
name = "BUDGET"
class_name = "BudgetProbe"

[[migrations]]
tag = "v1"
new_sqlite_classes = ["ZeroDO", "BudgetProbe"]
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

  const budget = (path) =>
    fetch(`${base}/budget/${path}`).then((response) => response.json())
  const bootID = await budget('setup')
  const trip = await budget('write?at=1')
  assert.match(trip.error, /WriteBudgetExceededError/)
  assert.equal(trip.bootID, bootID)
  const tripped = await budget('state')
  assert.equal(tripped.tripped, true)
  assert.equal(tripped.bootID, bootID)
  assert.equal(tripped.comments, 1)
  assert.match(tripped.trippedStatement.sql, /INSERT INTO comment.*RETURNING/)
  const refused = await budget('write?at=2')
  assert.match(refused.error, /WriteBudgetExceededError/)
  assert.deepEqual(await budget('state'), tripped)
} finally {
  server.kill()
  await server.exited
  rmSync(fixture, { recursive: true, force: true })
}
