import { createOrezDataWorker } from '../src/cf-do/lite-data-worker.js'
import { createApplicationSqlClient } from '../src/cf-do/application-sql.js'
import { orezAppSchema } from './migration.js'

const notificationAttempts = new Map<string, number>()
const dataWorker = createOrezDataWorker({
  name: 'contrast',
  schema: orezAppSchema,
  applicationSqlDidCommit({ instance }) {
    notificationAttempts.set(instance, (notificationAttempts.get(instance) ?? 0) + 1)
  },
})

export class ZeroDO extends dataWorker.ZeroDO {
  resetForProof() { this.ctx.abort('snapshot restart proof') }
  storageTables() {
    return this.ctx.storage.sql.exec("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '_cf_%' AND name NOT LIKE '__miniflare_%'").toArray().map((row) => row.name)
  }
  async checkCurrentSchema(instance) {
    return orezAppSchema.migrate({ instance, client: this.applicationSqlLocalClient(instance) })
  }
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url)
    const [action, namespace] = url.pathname.slice(1).split('/')
    if (!namespace) return new Response('missing namespace', { status: 400 })
    const instance = namespace.startsWith('ns:') ? namespace : 'ns:' + namespace
    if (action === 'backup-proof') {
      const client = dataWorker.applicationSqlClient(env, namespace)
      const stub = env.ZERO_SQL_DO.get(env.ZERO_SQL_DO.idFromName(instance))
      await client.exec('CREATE TABLE payload (id INTEGER PRIMARY KEY, body TEXT)')
      await client.exec("WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 8192) INSERT INTO payload SELECT i, printf('%4096s', 'x') FROM n")
      const markerQuery = 'SELECT write_seq FROM _orez_backup_meta WHERE id = 1'
      const beforeMarker = await client.query(markerQuery)
      const budget = () => stub.fetch(new Request('https://fixture.invalid/_orez/write-budget')).then(r => r.json())
      const before = await budget()
      const started = performance.now()
      const snapshot = await env.ZERO_SQL_DO.get(env.ZERO_SQL_DO.idFromName(instance)).backupSnapshot({ markerTable: '_orez_backup_meta', excludedTables: ['_orez_backup_meta'] })
      const copyMs = performance.now() - started
      const after = await budget()
      const afterMarker = await client.query(markerQuery)
      await client.exec("UPDATE payload SET body = 'changed' WHERE id = 1")
      const copied = await client.query('SELECT count(*) AS rows, sum(length(c1)) AS bytes FROM "_orez_bk_' + snapshot.id + '_payload"')
      const triggers = await client.query("SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name GLOB '_orez_bk_*_payload'")
      await stub.backupSnapshotDrop(snapshot.id)
      snapshot.lease[Symbol.dispose]()
      const dropped = await client.query("SELECT name FROM sqlite_master WHERE name GLOB '_orez_bk_*'")
      const abandoned = await stub.backupSnapshot({ markerTable: '_orez_backup_meta', excludedTables: ['_orez_backup_meta'] })
      abandoned.lease[Symbol.dispose]()
      let residue = []
      const disposalDeadline = Date.now() + 5000
      do {
        residue = await client.query("SELECT name FROM sqlite_master WHERE name GLOB '_orez_bk_*'")
        if (!residue.length) break
        await new Promise(resolve => setTimeout(resolve, 10))
      } while (Date.now() < disposalDeadline)
      if (residue.length) throw new Error('disposed backup lease left snapshot tables behind')
      const old = await stub.backupSnapshot({ markerTable: '_orez_backup_meta', excludedTables: ['_orez_backup_meta'] })
      try { await stub.resetForProof() } catch {}
      const fresh = env.ZERO_SQL_DO.get(env.ZERO_SQL_DO.idFromName(instance))
      const freshClient = dataWorker.applicationSqlClient(env, namespace)
      const newer = await fresh.backupSnapshot({ markerTable: '_orez_backup_meta', excludedTables: ['_orez_backup_meta'] })
      if (old.id === newer.id) throw new Error('restart reused a snapshot identity')
      let oldReadRejected = false
      try { await freshClient.query('SELECT * FROM "_orez_bk_' + old.id + '_payload" LIMIT 1') } catch (error) {
        if (!String(error).includes('no such table: _orez_bk_' + old.id + '_payload')) throw error
        oldReadRejected = true
      }
      if (!oldReadRejected) throw new Error('old snapshot read survived restart')
      await fresh.backupSnapshotDrop(old.id)
      const newerRows = await freshClient.query('SELECT count(*) AS rows FROM "_orez_bk_' + newer.id + '_payload"')
      if (newerRows[0].rows !== 8192) throw new Error('old cleanup removed the new snapshot')
      await fresh.backupSnapshotDrop(newer.id)
      try { old.lease[Symbol.dispose]() } catch {}
      newer.lease[Symbol.dispose]()

      // a restore import is namespace maintenance like the export copy: its
      // rows never meter, and a tripped circuit stops application writes but
      // not the restore that recovers the object.
      const freshBudget = () => fresh.fetch(new Request('https://fixture.invalid/_orez/write-budget')).then(r => r.json())
      const admin = { 'x-orez-admin-token': env.OREZ_DO_WRITE_BUDGET_ADMIN_TOKEN }
      const importBefore = await freshBudget()
      await fresh.orezImportBatch([
        { sql: 'CREATE TABLE imported (id INTEGER PRIMARY KEY, body TEXT)' },
        { sql: "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 2048) INSERT INTO imported SELECT i, 'restored' FROM n" },
      ])
      const importAfter = await freshBudget()
      const trip = await fresh.fetch(new Request('https://fixture.invalid/_orez/write-budget/trip', { method: 'POST', headers: admin }))
      if (!trip.ok) throw new Error('force trip failed: ' + trip.status)
      await fresh.orezImportBatch([{ sql: "INSERT INTO imported (id, body) VALUES (0, 'restored while tripped')" }])
      let trippedApplicationWrite = null
      try { await freshClient.exec("INSERT INTO imported (id, body) VALUES (-1, 'application')") } catch (error) { trippedApplicationWrite = String(error) }
      const reopen = await fresh.fetch(new Request('https://fixture.invalid/_orez/write-budget/reopen', { method: 'POST', headers: admin }))
      if (!reopen.ok) throw new Error('reopen failed: ' + reopen.status)
      const imported = await freshClient.query('SELECT count(*) AS rows FROM imported')

      return Response.json({ copyMs, beforeMarker, afterMarker, before, after, copied, triggers, dropped, importBefore, importAfter, trippedApplicationWrite, imported })
    }
    if (action === 'seed') {
      // a namespace with ledger history but no reconciled schema. application
      // sql over rpc would converge the schema first, so the history lands the
      // way a restore does: straight into the object, past that check.
      const stub = env.ZERO_SQL_DO.get(env.ZERO_SQL_DO.idFromName(instance))
      await stub.orezImportBatch([
        {
          sql: 'CREATE TABLE IF NOT EXISTS "__contrast_cf_migrations" (id TEXT PRIMARY KEY, applied_at INTEGER NOT NULL)',
        },
        {
          sql: 'INSERT INTO "__contrast_cf_migrations" (id, applied_at) VALUES (?, ?)',
          params: ['0001_retired/migration.sql:0:previous-hash', 1],
        },
        ...Array.from({ length: 1000 }, (_, index) => ({
          sql: 'INSERT INTO "__contrast_cf_migrations" (id, applied_at) VALUES (?, ?)',
          params: ['historical-' + String(index).padStart(4, '0') + ':hash', 1],
        })),
      ])
      notificationAttempts.set(instance, 0)
      return Response.json({ ok: true })
    }
    if (action === 'first-touch') {
      // a never-reconciled namespace reached only by raw application sql
      const client = dataWorker.applicationSqlClient(env, namespace)
      return Response.json(
        await client.query(
          "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('alpha', 'beta') ORDER BY name"
        )
      )
    }
    if (action === 'unprovisioned-read') {
      // a read that declines to provision answers no rows and leaves the
      // namespace without a schema
      const client = createApplicationSqlClient(env.ZERO_SQL_DO, 'ns:' + namespace, { provision: false })
      return Response.json(
        await client.query("SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('alpha', 'beta') ORDER BY name")
      )
    }
    if (action === 'storage-tables') {
      // raw storage of the named object, read without any admission
      const stub = env.ZERO_SQL_DO.get(env.ZERO_SQL_DO.idFromName('ns:' + namespace))
      return Response.json(await stub.storageTables())
    }
    if (action === 'migrate') {
      const result = await dataWorker.ensureNamespaceSchema(env, namespace, { force: true })
      return Response.json(result)
    }
    if (action === 'current-check') {
      const stub = env.ZERO_SQL_DO.get(env.ZERO_SQL_DO.idFromName(instance))
      return Response.json(await stub.checkCurrentSchema(instance))
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
