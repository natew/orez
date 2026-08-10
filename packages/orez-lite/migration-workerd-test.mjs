import assert from 'node:assert/strict'
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'

import { findPort } from '../../src/port.ts'
import { defineCloudflareConfig } from './src/cf-deploy/config.ts'
import { buildMigrationModuleSource } from './src/cf-deploy/migration.ts'

const fixture = mkdtempSync(join(import.meta.dirname, '.migration-workerd-'))
const port = await findPort(0)
const inspectorPort = await findPort(0)

writeFileSync(
  join(fixture, 'schema.ts'),
  'export const schema = { tables: {}, relationships: {} }\n'
)
writeFileSync(
  join(fixture, 'migration.ts'),
  buildMigrationModuleSource(defineCloudflareConfig('contrast'), {
    mode: 'native',
    schemaVersion: 'migration-cost-v1',
    schemaImportSpecifier: './schema.js',
    nativeSqlStatements: [
      ...Array.from({ length: 588 - 3 }, (_, index) => ({
        id: `0000_history/migration.sql:${index}`,
        sql: '-- historical statement retained for migration identity',
      })),
      {
        id: '0001_retired/migration.sql:0',
        sql: 'DROP TABLE IF EXISTS retired',
      },
      {
        id: '0002_alpha/migration.sql:0',
        sql: 'CREATE TABLE alpha (id TEXT PRIMARY KEY)',
      },
      {
        id: '0003_beta/migration.sql:0',
        sql: 'CREATE TABLE beta (id TEXT PRIMARY KEY)',
      },
    ],
    expectedTables: [
      {
        name: 'alpha',
        columns: [{ name: 'id', notNull: true, primaryKeyOrder: 1, sqlType: 'text' }],
      },
      {
        name: 'beta',
        columns: [{ name: 'id', notNull: true, primaryKeyOrder: 1, sqlType: 'text' }],
      },
    ],
  })
)
writeFileSync(
  join(fixture, 'worker.ts'),
  `import { createOrezDataWorker } from '../src/cf-do/lite-data-worker.js'
import { orezAppSchema } from './migration.js'

const notificationAttempts = new Map<string, number>()
const dataWorker = createOrezDataWorker({
  name: 'contrast',
  schema: orezAppSchema,
  applicationSqlDidCommit({ instance }) {
    notificationAttempts.set(instance, (notificationAttempts.get(instance) ?? 0) + 1)
  },
})

export const ZeroDO = dataWorker.ZeroDO

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url)
    const [action, namespace] = url.pathname.slice(1).split('/')
    if (!namespace) return new Response('missing namespace', { status: 400 })
    const instance = namespace.startsWith('ns:') ? namespace : 'ns:' + namespace
    if (action === 'seed') {
      const client = dataWorker.applicationSqlClient(env, namespace)
      await client.transaction(
        () => { throw new Error('migration cost fixture does not compile query ASTs') },
        async (tx) => {
          await tx.exec(
            'CREATE TABLE IF NOT EXISTS "__contrast_cf_migrations" (id TEXT PRIMARY KEY, applied_at INTEGER NOT NULL)'
          )
          await tx.exec(
            'INSERT INTO "__contrast_cf_migrations" (id, applied_at) VALUES (?, ?)',
            ['0001_retired/migration.sql:0:previous-hash', 1]
          )
          for (let index = 0; index < 1000; index++) {
            await tx.exec(
              'INSERT INTO "__contrast_cf_migrations" (id, applied_at) VALUES (?, ?)',
              ['historical-' + String(index).padStart(4, '0') + ':hash', 1]
            )
          }
        }
      )
      notificationAttempts.set(instance, 0)
      return Response.json({ ok: true })
    }
    if (action === 'migrate') {
      const result = await dataWorker.ensureNamespaceSchema(env, namespace, { force: true })
      return Response.json(result)
    }
    if (action === 'status') {
      return dataWorker.fetch(
        new Request('https://fixture.invalid/' + namespace + '/_orez/status', {
          headers: { 'x-orez-admin-token': 'migration-cost-admin' },
        }),
        env,
        ctx
      )
    }
    if (action === 'observe') {
      const client = dataWorker.applicationSqlClient(env, namespace)
      const [tables, ledger] = await Promise.all([
        client.query(
          "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('alpha', 'beta') ORDER BY name"
        ),
        client.query('SELECT COUNT(*) AS count FROM "__contrast_cf_migrations"'),
      ])
      return Response.json({
        tables,
        ledger: Number(ledger[0]?.count ?? 0),
        callbacks: notificationAttempts.get(instance) ?? 0,
      })
    }
    return new Response('not found', { status: 404 })
  },
}
`
)
writeFileSync(
  join(fixture, 'wrangler.toml'),
  `name = "orez-migration-cost"
main = "worker.ts"
compatibility_date = "2026-06-01"
compatibility_flags = ["nodejs_compat"]

[vars]
OREZ_DO_WRITE_BUDGET_ADMIN_TOKEN = "migration-cost-admin"

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

try {
  for (let attempt = 0; ; attempt++) {
    try {
      await fetch(`${base}/missing/readiness`)
      break
    } catch {}
    if (server.exitCode !== null) {
      throw new Error(`migration workerd fixture exited with ${server.exitCode}`)
    }
    if (attempt >= 200) throw new Error('migration workerd fixture did not become ready')
    await Bun.sleep(100)
  }

  const namespace = `proj-ledger-cost-${crypto.randomUUID()}`
  assert.equal((await fetch(`${base}/seed/${namespace}`, { method: 'POST' })).status, 200)
  const before = await fetch(`${base}/status/${namespace}`).then((response) =>
    response.json()
  )
  assert.equal(
    (await fetch(`${base}/migrate/${namespace}`, { method: 'POST' })).status,
    200
  )
  const after = await fetch(`${base}/status/${namespace}`).then((response) =>
    response.json()
  )
  const observation = await fetch(`${base}/observe/${namespace}`).then((response) =>
    response.json()
  )
  const cost = {
    rowsRead: after.sqlBillingSinceBoot.rowsRead - before.sqlBillingSinceBoot.rowsRead,
    rowsWritten:
      after.sqlBillingSinceBoot.rowsWritten - before.sqlBillingSinceBoot.rowsWritten,
    sessions:
      after.requestsSinceBoot.applicationSqlSessions -
      before.requestsSinceBoot.applicationSqlSessions,
    statements:
      after.requestsSinceBoot.sqlStatements - before.requestsSinceBoot.sqlStatements,
    callbacks: observation.callbacks,
  }
  assert.deepEqual(observation, {
    tables: [{ name: 'alpha' }, { name: 'beta' }],
    ledger: 1_003,
    callbacks: 0,
  })
  // the 588 current ids take 19 primary-key probe statements. unmatched
  // historical rows do not add reads; the previous scan read all 1,001 rows
  // once per session while opening prepare + one session per file + finalize.
  assert.deepEqual(cost, {
    rowsRead: 979,
    rowsWritten: 69,
    sessions: 2,
    statements: 93,
    callbacks: 0,
  })
} finally {
  server.kill()
  await server.exited
  rmSync(fixture, { recursive: true, force: true })
}
