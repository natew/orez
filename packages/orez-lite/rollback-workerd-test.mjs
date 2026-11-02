import assert from 'node:assert/strict'
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'

import { unstable_dev } from 'wrangler'

const scratch = join(process.env.HOME, '.scratch/orez-tx-rollback')
mkdirSync(scratch, { recursive: true })
const fixture = mkdtempSync(join(scratch, 'workerd-'))
const baseline = process.env.OREZ_ROLLBACK_BASELINE === '1'
writeFileSync(
  join(fixture, 'worker.ts'),
  `
import { ZeroDO, createApplicationSqlClient } from ${JSON.stringify(join(import.meta.dirname, 'src/cf-do/worker.ts'))}

export class Data extends ZeroDO {
  async setup(retained) {
    const sql = this.ctx.storage.sql
    this.watermarks.ensureTables()
    sql.exec('CREATE TABLE item (id INTEGER PRIMARY KEY, body TEXT)')
    sql.exec('CREATE TABLE audit (id INTEGER PRIMARY KEY, body TEXT)')
    sql.exec('CREATE TRIGGER audit_item AFTER INSERT ON item BEGIN INSERT INTO audit VALUES (NEW.id, NEW.body); END')
    sql.exec('CREATE TABLE "set" (rowid TEXT, _rowid_ TEXT, oid TEXT)')
    sql.exec('CREATE TABLE identityless (rowid TEXT, _rowid_ TEXT, oid TEXT, body TEXT)')
    if (retained) sql.exec("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x < ?) INSERT INTO identityless SELECT x, x, x, 'seed' FROM n", retained)
    sql.exec('CREATE TABLE unique_only (rowid TEXT, _rowid_ TEXT, oid TEXT, key TEXT COLLATE NOCASE NOT NULL, body TEXT)')
    sql.exec('CREATE UNIQUE INDEX unique_only_key ON unique_only(key COLLATE BINARY)')
    sql.exec('CREATE TABLE __app_cf_migrations (id TEXT PRIMARY KEY, applied_at INTEGER NOT NULL)')
    sql.exec("INSERT INTO __app_cf_migrations VALUES ('old', 1)")
    sql.exec('CREATE TABLE parent (id INTEGER PRIMARY KEY)')
    sql.exec('CREATE TABLE private_child (id INTEGER PRIMARY KEY, parentId INTEGER REFERENCES parent(id) ON DELETE CASCADE)')
    sql.exec('CREATE TABLE child (id INTEGER PRIMARY KEY, parentId INTEGER REFERENCES parent(id) ON DELETE CASCADE)')
    sql.exec('CREATE TABLE registered (id INTEGER PRIMARY KEY, body TEXT)')
    sql.exec('CREATE TABLE declared_private (id INTEGER PRIMARY KEY, body TEXT)')
    if (retained) sql.exec("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x < ?) INSERT INTO item SELECT -x, 'seed' FROM n", retained)
    await this.applicationSqlLocalClient('setup').registerTables([
      { table: 'registered', publicTable: 'registered' },
      { table: 'declared_private', publicTable: 'declared_private', publish: false },
    ])
  }
  async attempt(table, returning) {
    return attempt(this.applicationSqlLocalClient('local'), table, returning)
  }
  state() {
    const sql = this.ctx.storage.sql
    return {
      item: sql.exec('SELECT count(*) AS n FROM item').one().n,
      audit: sql.exec('SELECT count(*) AS n FROM audit').one().n,
      registered: sql.exec('SELECT count(*) AS n FROM registered').one().n,
      billed: this.writeBudget.status().billableRows,
      registrations: sql.exec('SELECT physical_table FROM _orez_cdc_tables ORDER BY physical_table').toArray().map(row => row.physical_table),
      feed: sql.exec("SELECT table_name AS name FROM _zero_changes ORDER BY watermark").toArray(),
    }
  }
  async ledgerRollback() {
    const client = this.applicationSqlLocalClient('ledger')
    let error
    try {
      await client.transaction(null, async tx => {
        await tx.exec("DELETE FROM __app_cf_migrations WHERE id = 'old'")
        await tx.exec("INSERT INTO __app_cf_migrations VALUES ('new', 2)")
        throw new Error('ledger rollback')
      })
    } catch (caught) { error = String(caught) }
    return { error, rows: this.ctx.storage.sql.exec('SELECT * FROM __app_cf_migrations ORDER BY id').toArray() }
  }
  async uniqueRollback() {
    const client = this.applicationSqlLocalClient('unique-only')
    await client.exec("INSERT INTO unique_only VALUES ('r', 'r', 'r', 'a', 'lower'), ('r', 'r', 'r', 'A', 'upper')")
    let error
    try {
      await client.transaction(null, async tx => {
        await tx.exec("UPDATE unique_only SET body = 'changed' WHERE key = 'a' COLLATE BINARY")
        await tx.exec("DELETE FROM unique_only WHERE key = 'A' COLLATE BINARY")
        await tx.exec("INSERT INTO unique_only VALUES ('r', 'r', 'r', 'new', 'new')")
        throw new Error('unique rollback')
      })
    } catch (caught) { error = String(caught) }
    return { error, rows: this.ctx.storage.sql.exec('SELECT key, body FROM unique_only ORDER BY key COLLATE BINARY').toArray() }
  }
  async indexChangeRollback() {
    const client = this.applicationSqlLocalClient('index-change')
    await client.exec('CREATE TABLE changing_key (rowid TEXT, _rowid_ TEXT, oid TEXT, first_key TEXT NOT NULL, next_key TEXT NOT NULL, body TEXT)')
    await client.exec('CREATE UNIQUE INDEX first_key_index ON changing_key(first_key)')
    await client.exec("INSERT INTO changing_key VALUES ('r', 'r', 'r', 'first', 'next', 'seed')")
    await client.exec('DROP INDEX first_key_index')
    await client.exec('CREATE UNIQUE INDEX next_key_index ON changing_key(next_key)')
    let error
    try {
      await client.transaction(null, async tx => {
        await tx.exec("UPDATE changing_key SET body = 'changed' WHERE next_key = 'next'")
        await tx.exec("INSERT INTO changing_key VALUES ('r', 'r', 'r', 'second', 'second', 'new')")
        throw new Error('changed index rollback')
      })
    } catch (caught) { error = String(caught) }
    const rows = this.ctx.storage.sql.exec('SELECT first_key, next_key, body FROM changing_key').toArray()
    await client.exec('DROP INDEX next_key_index')
    const captured = this.cdc.ensureTable({ physicalTableName: 'changing_key', tableName: 'changing_key', publish: false }, true)
    const triggers = this.ctx.storage.sql.exec("SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = 'changing_key'").toArray()
    const registrations = this.ctx.storage.sql.exec("SELECT physical_table FROM _orez_cdc_tables WHERE physical_table = 'changing_key'").toArray()
    return { error, rows, captured, triggers, registrations }
  }
  async triggerDdlRollback() {
    const client = this.applicationSqlLocalClient('trigger-ddl')
    for (const table of ['parse_source', 'parse_b', 'parse_c']) await client.exec('CREATE TABLE ' + table + ' (id INTEGER PRIMARY KEY)')
    let error
    try {
      await client.transaction(null, async tx => {
        await tx.exec('CREATE TRIGGER parse_trigger AFTER INSERT ON parse_source BEGIN INSERT INTO parse_b VALUES (new.id); SELECT CASE WHEN new.id > 0 THEN 1 ELSE 0 END; INSERT INTO parse_c VALUES (new.id); END')
        throw new Error('trigger ddl rollback')
      })
    } catch (caught) { error = String(caught) }
    return { error, triggers: this.ctx.storage.sql.exec("SELECT name FROM sqlite_master WHERE name = 'parse_trigger'").toArray() }
  }
  async fkRollback() {
    const client = this.applicationSqlLocalClient('fk')
    const sql = this.ctx.storage.sql
    await client.exec('INSERT INTO parent VALUES (1)')
    sql.exec('INSERT INTO private_child VALUES (10, 1)')
    let deadRows = 0
    const mutate = async (tx, child) => {
      await tx.exec('INSERT INTO child VALUES (?, 1)', [child], { table: 'child', publicTable: 'child', kind: 'insert' })
      await tx.exec('DELETE FROM parent WHERE id = 1', [], { table: 'parent', publicTable: 'parent', kind: 'delete' })
      deadRows += sql.exec('SELECT count(*) AS n FROM _zero_pending_changes WHERE publish = 0 AND undoable = 0').one().n
    }
    try {
      await client.transaction(null, async tx => {
        await mutate(tx, 1)
        throw new Error('fk rollback')
      })
    } catch (error) { if (!String(error).includes('fk rollback')) throw error }
    const rolledBack = { parent: sql.exec('SELECT count(*) AS n FROM parent').one().n, child: sql.exec('SELECT count(*) AS n FROM child').one().n, privateChild: sql.exec('SELECT count(*) AS n FROM private_child').one().n }
    await client.transaction(null, tx => mutate(tx, 2))
    return { rolledBack, deadRows, committed: { parent: sql.exec('SELECT count(*) AS n FROM parent').one().n, child: sql.exec('SELECT count(*) AS n FROM child').one().n, privateChild: sql.exec('SELECT count(*) AS n FROM private_child').one().n } }
  }
  async oddRollback() {
    const client = this.applicationSqlLocalClient('identityless')
    const before = this.ctx.storage.sql.exec('SELECT count(*) AS n FROM identityless').one().n
    const protectedBefore = this.state().item
    let error
    try {
      await client.transaction(null, async tx => {
        await tx.exec("INSERT INTO item VALUES (30, 'protected')")
        for (let i = 0; i < 3; i++) await tx.exec('/* odd */ INSERT /* target */ INTO"main"."identityless" VALUES (?, ?, ?, ?)', ['x', 'x', 'x', 'x'])
        throw new Error('identityless rollback')
      })
    } catch (caught) { error = String(caught) }
    return { before, after: this.ctx.storage.sql.exec('SELECT count(*) AS n FROM identityless').one().n, protectedBefore, protectedAfter: this.state().item, error }
  }
  async promote() {
    const client = this.applicationSqlLocalClient('promote')
    await client.exec('INSERT INTO item VALUES (20, ?)', ['private'])
    try {
      await client.transaction(null, async tx => {
        await tx.exec('ALTER TABLE item ADD COLUMN discarded TEXT')
        throw new Error('schema rollback')
      })
    } catch (error) { if (!String(error).includes('schema rollback')) throw error }
    this.cdc.reload()
    await client.exec('INSERT INTO item VALUES (21, ?)', ['public'], { table: 'item', publicTable: 'published_item', kind: 'insert' })
    await client.exec('INSERT INTO declared_private VALUES (21, ?)', ['private'], { table: 'declared_private', publicTable: 'declared_private', kind: 'insert' })
  }
}
async function attempt(client, table, returning) {
  try {
    await client.transaction(null, async tx => {
      for (let i=1; i<=3; i++) {
        if (returning) await tx.query('/* raw */ WITH value AS (SELECT ? AS id) INSERT INTO ' + table + ' SELECT id, ? FROM value WHERE true ON CONFLICT (id) DO UPDATE SET body = excluded.body RETURNING id', [i, 'attempt'])
        else await tx.exec('INSERT INTO ' + table + ' VALUES (?, ?)', [i, 'attempt'])
      }
      throw new Error('intentional rollback')
    })
  } catch (error) { return String(error) }
  throw new Error('transaction unexpectedly succeeded')
}
export default {
  async fetch(request, env) {
    const url = new URL(request.url)
    const id = url.searchParams.get('id')
    const object = env.DATA.get(env.DATA.idFromName(id))
    if (url.pathname === '/setup') { await object.setup(Number(url.searchParams.get('retained'))); return Response.json(await object.state()) }
    if (url.pathname === '/attempt') {
      const table = url.searchParams.get('table') || 'item'
      const returning = url.searchParams.get('returning') === '1'
      const error = url.searchParams.get('remote') === '1'
        ? await attempt(createApplicationSqlClient(env.DATA, id), table, returning)
        : await object.attempt(table, returning)
      return Response.json({ error, ...await object.state() })
    }
    if (url.pathname === '/index-change') return Response.json(await object.indexChangeRollback())
    if (url.pathname === '/trigger-ddl') return Response.json(await object.triggerDdlRollback())
    if (url.pathname === '/ledger') return Response.json(await object.ledgerRollback())
    if (url.pathname === '/unique') return Response.json(await object.uniqueRollback())
    if (url.pathname === '/fk') return Response.json(await object.fkRollback())
    if (url.pathname === '/odd') return Response.json(await object.oddRollback())
    if (url.pathname === '/promote') { await object.promote(); return Response.json(await object.state()) }
    return Response.json(await object.state())
  }
}
`
)
writeFileSync(
  join(fixture, 'wrangler.toml'),
  `
name = "orez-rollback-proof"
main = "worker.ts"
compatibility_date = "2026-06-01"
compatibility_flags = ["nodejs_compat"]
[alias]
"orez-sync-cf-host/transaction-query" = ${JSON.stringify(join(import.meta.dirname, '../sync-cf-host/src/transaction-query.ts'))}
[vars]
OREZ_DO_WRITE_BUDGET_ROWS = "1000000"
[[durable_objects.bindings]]
name = "DATA"
class_name = "Data"
[[migrations]]
tag = "v1"
new_sqlite_classes = ["Data"]
`
)
const server = await unstable_dev(join(fixture, 'worker.ts'), {
  config: join(fixture, 'wrangler.toml'),
  local: true,
  persist: false,
  logLevel: 'error',
  experimental: { disableExperimentalWarning: true, disableDevRegistry: true },
})
const get = async (path, params) => {
  const response = await server.fetch(
    'http://example.com/' + path + '?' + new URLSearchParams(params)
  )
  const body = await response.text()
  assert.equal(response.status, 200, body)
  return JSON.parse(body)
}
try {
  const costs = []
  for (const remote of ['0', '1']) {
    for (const returning of ['0', '1']) {
      for (const retained of [0, 1000]) {
        const id = `proof-${remote}-${returning}-${retained}`
        const before = await get('setup', { id, retained: String(retained) })
        const after = await get('attempt', { id, remote, returning })
        assert.match(after.error, /intentional rollback/)
        assert.equal(after.registered, 0)
        assert.equal(after.item, retained + (baseline ? 3 : 0))
        assert.equal(after.audit, retained + (baseline ? 3 : 0))
        const registered = await get('attempt', {
          id,
          remote,
          returning,
          table: 'registered',
        })
        assert.equal(registered.registered, 0)
        let warmRowsWritten
        if (!baseline) {
          assert.deepEqual(after.registrations, [
            'audit',
            'declared_private',
            'item',
            'registered',
          ])
          const warm = await get('attempt', { id, remote, returning })
          assert.equal(warm.item, retained)
          assert.equal(warm.audit, retained)
          warmRowsWritten = warm.billed - registered.billed
          assert.equal(after.billed - before.billed, 66)
          assert.equal(warmRowsWritten, 52)
          const odd = await get('odd', { id })
          assert.match(odd.error, /identityless rollback/)
          assert.equal(odd.after, odd.before + 3)
          assert.equal(odd.protectedAfter, odd.protectedBefore)
          const promoted = await get('promote', { id })
          assert.deepEqual(promoted.feed, [{ name: 'published_item' }])
        }
        costs.push({
          remote,
          returning,
          retained,
          rowsWritten: after.billed - before.billed,
          remaining: after.item - retained,
          warmRowsWritten,
        })
      }
    }
  }
  if (!baseline) {
    const id = 'review-regressions'
    await get('setup', { id, retained: '0' })
    const ledger = await get('ledger', { id })
    assert.match(ledger.error, /ledger rollback/)
    assert.deepEqual(ledger.rows, [{ id: 'old', applied_at: 1 }])
    const unique = await get('unique', { id })
    assert.match(unique.error, /unique rollback/)
    assert.deepEqual(unique.rows, [
      { key: 'A', body: 'upper' },
      { key: 'a', body: 'lower' },
    ])
    const changedIndex = await get('index-change', { id })
    assert.match(changedIndex.error, /changed index rollback/)
    assert.deepEqual(changedIndex.rows, [
      { first_key: 'first', next_key: 'next', body: 'seed' },
    ])
    assert.equal(changedIndex.captured, false)
    assert.deepEqual(changedIndex.triggers, [])
    assert.deepEqual(changedIndex.registrations, [])
    const triggerDdl = await get('trigger-ddl', { id })
    assert.match(triggerDdl.error, /trigger ddl rollback/)
    assert.deepEqual(triggerDdl.triggers, [])
    const fk = await get('fk', { id })
    assert.deepEqual(fk, {
      rolledBack: { parent: 1, child: 0, privateChild: 1 },
      deadRows: 0,
      committed: { parent: 0, child: 0, privateChild: 0 },
    })
  }
  for (let i = 0; i < costs.length; i += 2)
    assert.equal(costs[i].rowsWritten, costs[i + 1].rowsWritten)
  console.log(JSON.stringify({ baseline, costs }, null, 2))
} finally {
  await server.stop()
  rmSync(fixture, { recursive: true, force: true })
}
