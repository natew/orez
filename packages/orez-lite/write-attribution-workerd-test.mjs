import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { findPort } from '../../src/port.ts'
import {
  assertNoForbiddenAttributionFields,
  assertWriteAttributionReconciles,
} from './src/cf-do/write-attribution.ts'

const fixture = mkdtempSync(join(tmpdir(), 'orez-write-attribution-workerd-'))
const adminToken = 'write-attribution-admin'
const FIXTURES = [
  'private-update',
  'public-insert',
  'public-update',
  'public-delete',
  'indexed-update',
  'fk-cascade',
  'trigger-side',
  'multi-statement',
  'rollback',
  'duplicate-commit-retry',
]

writeFileSync(
  join(fixture, 'worker.ts'),
  `import { createOrezDataWorker } from ${JSON.stringify(
    join(import.meta.dirname, 'src/cf-do/lite-data-worker.js')
  )}

const schema = {
  version: 'write-attribution-v1',
  schema: {
    tables: {
      item: {
        name: 'item',
        columns: { id: { type: 'string' as const }, name: { type: 'string' as const } },
        primaryKey: ['id'] as const,
      },
      indexed_item: {
        name: 'indexed_item',
        columns: {
          id: { type: 'string' as const },
          name: { type: 'string' as const },
          createdAt: { type: 'number' as const },
        },
        primaryKey: ['id'] as const,
      },
      parent: {
        name: 'parent',
        columns: { id: { type: 'string' as const } },
        primaryKey: ['id'] as const,
      },
      child: {
        name: 'child',
        columns: {
          id: { type: 'string' as const },
          parentId: { type: 'string' as const },
        },
        primaryKey: ['id'] as const,
      },
      audit: {
        name: 'audit',
        columns: {
          id: { type: 'string' as const },
          itemId: { type: 'string' as const },
        },
        primaryKey: ['id'] as const,
      },
    },
    relationships: { item: {}, indexed_item: {}, parent: {}, child: {}, audit: {} },
  },
  publicTables: [
    { table: 'item', publicTable: 'item' },
    { table: 'indexed_item', publicTable: 'indexed_item' },
    { table: 'parent', publicTable: 'parent' },
    { table: 'child', publicTable: 'child' },
    { table: 'audit', publicTable: 'audit' },
  ],
  async migrate({ client }) {
    await client.exec('CREATE TABLE IF NOT EXISTS item (id TEXT PRIMARY KEY, name TEXT)')
    await client.exec(
      'CREATE TABLE IF NOT EXISTS indexed_item (id TEXT PRIMARY KEY, name TEXT, createdAt INTEGER)'
    )
    await client.exec('CREATE INDEX IF NOT EXISTS idx_indexed_item_name ON indexed_item(name)')
    await client.exec(
      'CREATE TABLE IF NOT EXISTS parent (id TEXT PRIMARY KEY)'
    )
    await client.exec(
      'CREATE TABLE IF NOT EXISTS child (id TEXT PRIMARY KEY, parentId TEXT REFERENCES parent(id) ON DELETE CASCADE)'
    )
    await client.exec(
      'CREATE TABLE IF NOT EXISTS audit (id TEXT PRIMARY KEY, itemId TEXT)'
    )
    await client.exec(
      'CREATE TABLE IF NOT EXISTS secret (id TEXT PRIMARY KEY, token TEXT)'
    )
    await client.exec(\`CREATE TRIGGER IF NOT EXISTS item_audit AFTER INSERT ON item BEGIN
      INSERT INTO audit (id, itemId) VALUES (NEW.id || '-a', NEW.id);
    END\`)
    await client.registerTables([
      { table: 'item', publicTable: 'item' },
      { table: 'indexed_item', publicTable: 'indexed_item' },
      { table: 'parent', publicTable: 'parent' },
      { table: 'child', publicTable: 'child' },
      { table: 'audit', publicTable: 'audit' },
      { table: 'secret', publicTable: 'secret', publish: false },
    ])
  },
}

const unusedCompiler = () => {
  throw new Error('write attribution fixture does not compile query ASTs')
}

const dataWorker = createOrezDataWorker({
  name: 'writeattr',
  schema,
  async routes({ request, url, applicationSql, env, instance }) {
    const parts = url.pathname.split('/').filter(Boolean)
    const sql = applicationSql()
    if (request.method === 'POST' && parts[0] === 'snapshot-lease') {
      const owner = env.ZERO_SQL_DO.get(env.ZERO_SQL_DO.idFromName(instance))
      await sql.transaction(unusedCompiler, async (tx) => {
        await tx.exec("INSERT INTO item (id, name) VALUES ('a', 'a'), ('b', 'b'), ('c', 'c')")
      })
      const snapshot = await owner.backupSnapshot({ markerTable: 'missing', excludedTables: [] })
      const writer = await owner.applicationSqlSession('snapshot-open-writer')
      const status = async () => {
        const response = await owner.fetch(new Request('https://fixture/_orez/status', {
          headers: { 'x-orez-admin-token': env.OREZ_DO_WRITE_BUDGET_ADMIN_TOKEN },
        }))
        if (!response.ok) throw new Error('snapshot status failed: ' + response.status)
        return response.json()
      }
      const chunks = []
      const states = []
      try {
        await writer.begin()
        await writer.exec("UPDATE item SET name = 'dirty'")
        const before = await status()
        for (let round = 0; round < 4; round++) {
          let cursor = 0
          const values = []
          for (let page = 0; page < 3; page++) {
            const key = 'snapshot/' + round + '/' + page
            await env.BACKUP_FILES.put(key, JSON.stringify(values))
            const stored = await env.BACKUP_FILES.get(key)
            if (!stored || await stored.text() !== JSON.stringify(values)) throw new Error('R2 await failed')
            const rows = await snapshot.lease.readPage('item', cursor, 1)
            if (rows.length !== 1) throw new Error('snapshot page missing')
            cursor = rows[0].__orez_backup_rowid
            values.push(rows[0].c1)
            states.push(await status())
            await env.BACKUP_FILES.delete(key)
          }
          chunks.push(values)
        }
        const after = await status()
        await writer.rollback()
        await owner.backupSnapshotDrop(snapshot.id)
        let stale = false
        try { await snapshot.lease.readPage('item', 0, 1) }
        catch (error) { stale = String(error).includes('no longer active') }
        return Response.json({ chunks, states, before, after, stale })
      } finally {
        await writer.rollback()
        writer[Symbol.dispose]()
        await owner.backupSnapshotDrop(snapshot.id)
        snapshot.lease[Symbol.dispose]()
      }
    }
    if (request.method === 'GET' && parts[0] === 'feed-ids') {
      const rows = await sql.query(
        'SELECT table_name AS tableName, op FROM _zero_changes ORDER BY watermark'
      )
      return Response.json(rows)
    }
    if (request.method !== 'POST' || parts[0] !== 'fixture' || !parts[1]) return null
    const name = parts[1]
    await request.json().catch(() => ({}))
    const suffix = crypto.randomUUID()
    const meta = (table, kind) => ({ table, publicTable: table, kind })
    try {
      if (name === 'warmup') {
        await sql.transaction(unusedCompiler, async (tx) => {
          await tx.exec("INSERT OR IGNORE INTO secret (id, token) VALUES ('s1', 'seed')")
          await tx.exec(
            "INSERT OR IGNORE INTO indexed_item (id, name, createdAt) VALUES ('idx1', 'alpha', 1)",
            [],
            meta('indexed_item', 'insert')
          )
        })
        return Response.json({ ok: true, fixture: name })
      }
      if (name === 'private-update') {
        await sql.transaction(unusedCompiler, async (tx) => {
          await tx.exec('UPDATE secret SET token = ? WHERE id = ?', ['t' + suffix, 's1'])
        })
      } else if (name === 'public-insert') {
        await sql.transaction(unusedCompiler, async (tx) => {
          await tx.exec(
            'INSERT INTO item (id, name) VALUES (?, ?)',
            ['ins-' + suffix, 'n'],
            meta('item', 'insert')
          )
        })
      } else if (name === 'public-update') {
        await sql.transaction(unusedCompiler, async (tx) => {
          await tx.exec(
            "INSERT OR IGNORE INTO item (id, name) VALUES ('upd-' || ?, 'old')",
            [suffix],
            meta('item', 'insert')
          )
          await tx.exec(
            "UPDATE item SET name = 'new-' || ? WHERE id = 'upd-' || ?",
            [suffix, suffix],
            meta('item', 'update')
          )
        })
      } else if (name === 'public-delete') {
        await sql.transaction(unusedCompiler, async (tx) => {
          await tx.exec(
            "INSERT INTO item (id, name) VALUES ('del-' || ?, 'gone')",
            [suffix],
            meta('item', 'insert')
          )
          await tx.exec(
            "DELETE FROM item WHERE id = 'del-' || ?",
            [suffix],
            meta('item', 'delete')
          )
        })
      } else if (name === 'indexed-update') {
        await sql.transaction(unusedCompiler, async (tx) => {
          await tx.exec(
            'UPDATE indexed_item SET name = ? WHERE id = ?',
            ['beta-' + suffix, 'idx1'],
            meta('indexed_item', 'update')
          )
        })
      } else if (name === 'fk-cascade') {
        await sql.transaction(unusedCompiler, async (tx) => {
          await tx.exec(
            "INSERT INTO parent (id) VALUES ('p-' || ?)",
            [suffix],
            meta('parent', 'insert')
          )
          await tx.exec(
            "INSERT INTO child (id, parentId) VALUES ('c-' || ?, 'p-' || ?)",
            [suffix, suffix],
            meta('child', 'insert')
          )
        })
        await sql.transaction(unusedCompiler, async (tx) => {
          await tx.exec(
            "DELETE FROM parent WHERE id = 'p-' || ?",
            [suffix],
            meta('parent', 'delete')
          )
        })
      } else if (name === 'trigger-side') {
        await sql.transaction(unusedCompiler, async (tx) => {
          await tx.exec(
            'INSERT INTO item (id, name) VALUES (?, ?)',
            ['trg-' + suffix, 'triggered'],
            meta('item', 'insert')
          )
        })
      } else if (name === 'multi-statement') {
        await sql.transaction(unusedCompiler, async (tx) => {
          await tx.exec(
            'INSERT INTO item (id, name) VALUES (?, ?)',
            ['ms-' + suffix, 'one'],
            meta('item', 'insert')
          )
          await tx.exec(
            'UPDATE item SET name = ? WHERE id = ?',
            ['two', 'ms-' + suffix],
            meta('item', 'update')
          )
        })
      } else if (name === 'rollback') {
        await sql.transaction(unusedCompiler, async (tx) => {
          await tx.exec(
            'INSERT INTO item (id, name) VALUES (?, ?)',
            ['rb-' + suffix, 'nope'],
            meta('item', 'insert')
          )
          throw new Error('rollback fixture')
        })
      } else if (name === 'duplicate-commit-retry') {
        await sql.transaction(unusedCompiler, async (tx) => {
          await tx.exec(
            'INSERT INTO item (id, name) VALUES (?, ?)',
            ['dup-' + suffix, 'first'],
            meta('item', 'insert')
          )
        })
        await sql.transaction(unusedCompiler, async (tx) => {
          await tx.exec(
            'INSERT OR IGNORE INTO item (id, name) VALUES (?, ?)',
            ['dup-' + suffix, 'second'],
            meta('item', 'insert')
          )
        })
      } else {
        return Response.json({ error: 'unknown fixture' }, { status: 404 })
      }
      return Response.json({ ok: true, fixture: name })
    } catch (error) {
      if (name === 'rollback' && error instanceof Error && error.message === 'rollback fixture') {
        const leftover = await sql.query('SELECT id FROM item WHERE id = ?', ['rb-' + suffix])
        return Response.json({ ok: true, fixture: name, rolledBack: leftover.length === 0 })
      }
      return Response.json(
        { error: error instanceof Error ? error.message : String(error) },
        { status: 500 }
      )
    }
  },
})

export const ZeroDO = dataWorker.ZeroDO
export default dataWorker
`
)

function parseTransactionSamples(text) {
  const events = []
  for (const line of text.split('\n')) {
    const start = line.indexOf('{')
    if (start < 0) continue
    try {
      const parsed = JSON.parse(line.slice(start))
      if (parsed?.event === 'orez_sql_transaction_sample') events.push(parsed)
    } catch {}
  }
  return events
}

async function runWorker(sampleRate) {
  const port = await findPort(0)
  const inspectorPort = await findPort(0)
  writeFileSync(
    join(fixture, 'wrangler.toml'),
    `name = "orez-write-attribution"
main = "worker.ts"
compatibility_date = "2026-06-01"
compatibility_flags = ["nodejs_compat"]

[vars]
OREZ_DO_WRITE_BUDGET_ADMIN_TOKEN = "${adminToken}"
OREZ_SQL_TELEMETRY_SAMPLE_RATE = "${sampleRate}"

[[durable_objects.bindings]]
name = "ZERO_SQL_DO"
class_name = "ZeroDO"

[[r2_buckets]]
binding = "BACKUP_FILES"
bucket_name = "snapshot-fixture"

[[migrations]]
tag = "v1"
new_sqlite_classes = ["ZeroDO"]
`
  )

  let logText = ''
  const decoder = new TextDecoder()
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
    { cwd: fixture, stdout: 'pipe', stderr: 'pipe' }
  )
  const collect = async (stream) => {
    const reader = stream.getReader()
    for (;;) {
      const { done, value } = await reader.read()
      if (done) break
      logText += decoder.decode(value)
    }
  }
  void collect(server.stdout)
  void collect(server.stderr)
  const base = `http://127.0.0.1:${port}`
  const namespace = 'test-write-attr'
  const headers = {
    'x-orez-admin-token': adminToken,
    'content-type': 'application/json',
    'x-orez-ns': namespace,
  }
  const status = () =>
    fetch(`${base}/${namespace}/_orez/status`, { headers }).then((response) =>
      response.json()
    )
  const changes = () =>
    fetch(`${base}/feed-ids?ns=${namespace}`, { headers }).then((response) =>
      response.json()
    )

  try {
    for (let attempt = 0; ; attempt++) {
      try {
        await fetch(`${base}/readiness`)
        break
      } catch {}
      if (server.exitCode !== null) {
        throw new Error(
          `write attribution workerd fixture exited with ${server.exitCode}\n${logText}`
        )
      }
      if (attempt >= 200) {
        throw new Error(
          'write attribution workerd fixture did not become ready\n' + logText
        )
      }
      await Bun.sleep(100)
    }

    const migration = await fetch(`${base}/${namespace}/_orez/schema/migrate`, {
      method: 'POST',
    })
    assert.equal(migration.status, 200, await migration.text())
    const warmup = await fetch(`${base}/fixture/warmup?ns=${namespace}`, {
      method: 'POST',
      headers,
      body: JSON.stringify({ suffix: 'warm' }),
    })
    assert.equal(warmup.status, 200, await warmup.text())

    const snapshotNamespace = 'test-snapshot-lease'
    const snapshotMigration = await fetch(
      `${base}/${snapshotNamespace}/_orez/schema/migrate`,
      { method: 'POST' }
    )
    assert.equal(snapshotMigration.status, 200, await snapshotMigration.text())
    const snapshotResponse = await fetch(
      `${base}/snapshot-lease?ns=${snapshotNamespace}`,
      {
        method: 'POST',
        headers: { ...headers, 'x-orez-ns': snapshotNamespace },
      }
    )
    const snapshotBody = await snapshotResponse.text()
    assert.equal(snapshotResponse.status, 200, snapshotBody)
    const snapshot = JSON.parse(snapshotBody)
    assert.deepEqual(
      snapshot.chunks,
      Array.from({ length: 4 }, () => ['a', 'b', 'c'])
    )
    assert.equal(snapshot.stale, true)
    assert.equal(
      snapshot.after.sqlBillingSinceBoot.rowsWritten,
      snapshot.before.sqlBillingSinceBoot.rowsWritten
    )
    assert.equal(
      snapshot.after.requestsSinceBoot.applicationSqlReadSessions,
      snapshot.before.requestsSinceBoot.applicationSqlReadSessions
    )
    for (const state of snapshot.states) {
      assert.equal(state.applicationSql.writerActive, true)
      assert.equal(state.applicationSql.activeReaders, 0)
      assert.equal(state.applicationSql.queuedReaders, 0)
      assert.equal(state.applicationSql.queuedWriters, 0)
    }
    console.log(
      JSON.stringify({
        event: 'snapshot_lease_workerd',
        sampleRate,
        chunks: snapshot.chunks.length,
        pages: snapshot.states.length,
        rowsWritten: 0,
        staleRejected: snapshot.stale,
      })
    )

    const receipts = []
    for (const name of FIXTURES) {
      const before = await status()
      const eventsBefore = parseTransactionSamples(logText).length
      const response = await fetch(`${base}/fixture/${name}?ns=${namespace}`, {
        method: 'POST',
        headers,
        body: JSON.stringify({ suffix: randomUUID() }),
      })
      const body = await response.json()
      assert.equal(response.status, 200, `${name}: ${JSON.stringify(body)}`)
      if (name === 'rollback') assert.equal(body.rolledBack, true)
      const after = await status()
      const billingDelta =
        after.sqlBillingSinceBoot.rowsWritten - before.sqlBillingSinceBoot.rowsWritten
      let writes = []
      for (let attempt = 0; attempt < 50; attempt++) {
        const events = parseTransactionSamples(logText).slice(eventsBefore)
        writes = events.filter((row) => row.name === 'application_sql_write')
        if (
          writes.length > 0 &&
          writes.reduce((sum, event) => sum + event.physicalTotal, 0) === billingDelta
        ) {
          break
        }
        await Bun.sleep(50)
      }
      receipts.push({
        name,
        billingDelta,
        event: writes.at(-1) ?? null,
        events: writes,
        body,
      })
    }

    return {
      receipts,
      changefeed: await changes(),
      billing: (await status()).sqlBillingSinceBoot,
      logText,
    }
  } finally {
    server.kill()
    await server.exited
    rmSync(join(fixture, '.wrangler'), { recursive: true, force: true })
  }
}

function assertEnabledReceipts(receipts, pass) {
  for (const receipt of receipts) {
    assert.ok(receipt.event, `${pass} ${receipt.name} missing transaction sample`)
    for (const event of receipt.events) {
      assertNoForbiddenAttributionFields(event)
      assertWriteAttributionReconciles(event)
    }
    const physicalSum = receipt.events.reduce(
      (sum, event) => sum + event.physicalTotal,
      0
    )
    assert.equal(
      physicalSum,
      receipt.billingDelta,
      `${pass} ${receipt.name} physicalTotal ${physicalSum} != billing ${receipt.billingDelta}`
    )
    assert.equal(receipt.event.sampleRate, 1)
    assert.equal(receipt.event.namespaceClass, 'test')
    assert.equal(receipt.event.logSampling, 'workers_observability_may_sample_or_drop')
    assert.equal(typeof receipt.event.workerVersion, 'string')
    assert.equal(typeof receipt.event.observedAt, 'number')
    assert.equal(typeof receipt.event.processStartedAt, 'number')
    if (receipt.name === 'rollback') {
      assert.equal(receipt.event.outcome, 'rolled_back')
      assert.equal(receipt.event.rustVisibleRows, 0)
      assert.equal(receipt.event.breakdown.zeroChanges, 0)
      assert.equal(receipt.event.breakdown.backupMeta, 0)
    } else {
      assert.equal(receipt.event.outcome, 'committed')
      assert.ok(
        receipt.events.some((event) => event.breakdown.backupMeta >= 1),
        `${pass} ${receipt.name} missing _orez_backup_meta rows in physicalTotal`
      )
    }
    if (receipt.name === 'private-update') {
      const row = receipt.event.breakdown.application.find(
        (entry) => entry.table === 'secret' && entry.op === 'UPDATE'
      )
      assert.ok(row, 'private-update missing secret UPDATE')
      assert.equal(row.visibility, 'private')
      assert.equal(receipt.event.rustVisibleRows, 0)
    }
    if (receipt.name === 'indexed-update') {
      const row = receipt.event.breakdown.application.find(
        (entry) => entry.table === 'indexed_item'
      )
      assert.ok(row, 'indexed-update missing indexed_item')
      assert.ok(row.indexRows >= 1, 'indexed-update expected measurable index rows')
    }
    if (receipt.name === 'fk-cascade') {
      const child = receipt.event.breakdown.application.find(
        (entry) => entry.table === 'child' && entry.op === 'DELETE'
      )
      assert.ok(child, 'fk-cascade missing child DELETE')
      assert.ok(child.logicalRows >= 1)
    }
    if (receipt.name === 'trigger-side') {
      const audit = receipt.event.breakdown.application.find(
        (entry) => entry.table === 'audit' && entry.op === 'INSERT'
      )
      assert.ok(audit, 'trigger-side missing audit INSERT')
    }
    if (receipt.name === 'duplicate-commit-retry') {
      const retry = receipt.event
      assert.equal(retry.rustVisibleRows, 0)
    }
  }
}

const enabledFirst = await runWorker(1)
assertEnabledReceipts(enabledFirst.receipts, 'pass-1')
const enabledSecond = await runWorker(1)
assertEnabledReceipts(enabledSecond.receipts, 'pass-2')
assert.deepEqual(
  enabledFirst.receipts.map((receipt) => [
    receipt.name,
    receipt.event.physicalTotal,
    receipt.event.logicalTotal,
    receipt.event.breakdown,
  ]),
  enabledSecond.receipts.map((receipt) => [
    receipt.name,
    receipt.event.physicalTotal,
    receipt.event.logicalTotal,
    receipt.event.breakdown,
  ])
)

const disabled = await runWorker(0)
assert.deepEqual(disabled.changefeed, enabledFirst.changefeed)
assert.equal(disabled.billing.rowsWritten, enabledFirst.billing.rowsWritten)
assert.equal(parseTransactionSamples(disabled.logText).length, 0)

console.log(
  JSON.stringify(
    {
      fixtures: enabledFirst.receipts.map((receipt) => ({
        name: receipt.name,
        physicalTotal: receipt.event.physicalTotal,
        logicalTotal: receipt.event.logicalTotal,
        rustVisibleRows: receipt.event.rustVisibleRows,
        billingDelta: receipt.billingDelta,
        complete: receipt.event.complete,
        breakdown: receipt.event.breakdown,
      })),
      disabledRowsWritten: disabled.billing.rowsWritten,
      enabledRowsWritten: enabledFirst.billing.rowsWritten,
      changefeedEqual: true,
    },
    null,
    2
  )
)

rmSync(fixture, { recursive: true, force: true })
