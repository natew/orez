/**
 * tx-journal tests: atomic commit/rollback bookkeeping for DoBackend's
 * emulated pg transactions, and crash recovery for transactions killed
 * mid-flight (DO eviction / deploy upgrade-kill).
 *
 * the harness runs the REAL journal core against a real sqlite database
 * (bedrock-sqlite standing in for DO SqlStorage), and the kill-mid-tx cases
 * drive it through a real DoBackend exactly like the deployed wiring — the
 * "kill" is abandoning the client mid-transaction while the storage
 * survives, which is precisely what a DO eviction does.
 */

import { createServer, type Server } from 'node:http'

// @ts-expect-error - CJS module
import BedrockSqlite from 'bedrock-sqlite'
import { afterEach, describe, expect, it } from 'vitest'

import { DoBackend } from '../pg-proxy-do-backend.js'
import { createLocalSqlBackend } from '../worker/local-sql-backend.js'
import { TransactionalCdc } from './cdc.js'
import {
  appendPendingChange,
  deletePendingChanges,
  ensurePendingChangesTable,
  rollbackPendingChanges,
} from './row-undo.js'
import {
  TX_MANIFEST_DDL,
  TX_MANIFEST_TABLE,
  commitTxJournal,
  recoverTxJournal,
  rollbackTxJournal,
  snapshotSideEffectWriteTables,
  snapshotTxSchema,
  upgradeToTableSnapshot,
} from './tx-journal.js'

import type { DurableSqlStorage } from './watermark.js'

const BetterSqlite3 = BedrockSqlite.Database

function createSqliteStorage() {
  const nativeDb = new BetterSqlite3(':memory:')
  const exec = (sql: string, ...params: unknown[]) => {
    const stmt = nativeDb.prepare(sql)
    const rows: Array<Record<string, unknown>> = stmt.reader
      ? stmt.all(...params)
      : (stmt.run(...params), [])
    return {
      toArray: () => rows,
      one: () => rows[0],
      columnNames: stmt.reader ? stmt.columns().map((c: any) => c.name) : [],
    }
  }
  const journal: DurableSqlStorage = { exec }
  return {
    nativeDb,
    exec,
    journal,
    transactionSync<T>(fn: () => T): T {
      return nativeDb.transaction(fn)()
    },
    tables(): string[] {
      return exec("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
        .toArray()
        .map((row) => String(row.name))
    },
    rows(table: string): Array<Record<string, unknown>> {
      return exec(`SELECT * FROM "${table}" ORDER BY 1`).toArray()
    },
  }
}

type SqliteStorage = ReturnType<typeof createSqliteStorage>

function snapshotTx(
  storage: SqliteStorage,
  txID: string,
  table: string,
  opts?: { owner?: string; exists?: boolean }
) {
  const exists = opts?.exists ?? true
  const snapshot = exists ? `_orez_tx_${txID}_0_${table}` : null
  storage.transactionSync(() => {
    storage.exec(
      `CREATE TABLE IF NOT EXISTS "${TX_MANIFEST_TABLE}" (seq INTEGER PRIMARY KEY AUTOINCREMENT, tx_id TEXT NOT NULL, owner TEXT NOT NULL DEFAULT 'default', original TEXT NOT NULL, snapshot TEXT)`
    )
    if (snapshot) {
      storage.exec(`CREATE TABLE "${snapshot}" AS SELECT * FROM "${table}"`)
    }
    storage.exec(
      `INSERT INTO "${TX_MANIFEST_TABLE}" (tx_id, owner, original, snapshot) VALUES (?, ?, ?, ?)`,
      txID,
      opts?.owner ?? 'default',
      table,
      snapshot
    )
  })
  return snapshot
}

describe('tx-journal core', () => {
  it('does not introspect Cloudflare hidden tables for referential actions', () => {
    const storage = createSqliteStorage()
    storage.exec('CREATE TABLE usageState (accountId TEXT PRIMARY KEY)')
    storage.exec('CREATE TABLE _cf_KV (key TEXT PRIMARY KEY, value BLOB)')
    let hiddenIntrospection = 0
    const guarded: DurableSqlStorage = {
      exec(sql, ...params) {
        if (/^PRAGMA foreign_key_list\("_cf_/i.test(sql)) {
          hiddenIntrospection++
          throw new Error('not authorized: SQLITE_AUTH')
        }
        return storage.exec(sql, ...params)
      },
    }

    expect(snapshotSideEffectWriteTables(guarded, 'tx1', 'usageState')).toBe(false)
    expect(hiddenIntrospection).toBe(0)
  })

  it('snapshots only transitive trigger and foreign-key targets', () => {
    const storage = createSqliteStorage()
    storage.exec('PRAGMA foreign_keys = ON')
    storage.exec('CREATE TABLE item (id INTEGER PRIMARY KEY, body TEXT)')
    storage.exec('CREATE TABLE audit (id INTEGER PRIMARY KEY, item_id INTEGER)')
    storage.exec('CREATE TABLE stats (id INTEGER PRIMARY KEY, writes INTEGER)')
    storage.exec(
      'CREATE TABLE child (' +
        'id INTEGER PRIMARY KEY, item_id INTEGER REFERENCES item(id) ON DELETE CASCADE)'
    )
    storage.exec('CREATE TABLE unrelated (id INTEGER PRIMARY KEY, body TEXT)')
    storage.exec(
      `CREATE TRIGGER item_audit AFTER INSERT ON item BEGIN
         INSERT INTO audit (id, item_id) VALUES (NEW.id, NEW.id)
           ON CONFLICT (id) DO UPDATE SET item_id = excluded.item_id;
       END`
    )
    storage.exec(
      `CREATE TRIGGER audit_stats AFTER INSERT ON audit BEGIN
         UPDATE stats SET writes = writes + 1 WHERE id = 1;
       END`
    )
    storage.exec(TX_MANIFEST_DDL)
    storage.exec(
      `INSERT INTO "${TX_MANIFEST_TABLE}" (tx_id, owner, original, snapshot) VALUES (?, ?, ?, '')`,
      'tx-targeted',
      'orez-embed',
      'item'
    )

    expect(snapshotSideEffectWriteTables(storage.journal, 'tx-targeted', 'item')).toBe(
      true
    )
    const manifest = storage
      .exec(
        `SELECT original, snapshot FROM "${TX_MANIFEST_TABLE}" WHERE tx_id = ? ORDER BY original`,
        'tx-targeted'
      )
      .toArray()
    expect(manifest.map((row) => String(row.original))).toEqual([
      'audit',
      'child',
      'item',
      'stats',
    ])
    expect(manifest.every((row) => String(row.snapshot).startsWith('_orez_tx_'))).toBe(
      true
    )
    expect(storage.tables()).toContain('unrelated')
    expect(manifest.some((row) => String(row.original) === 'unrelated')).toBe(false)
  })

  it('falls back to all tables for a trigger target it cannot parse', () => {
    const storage = createSqliteStorage()
    storage.exec('CREATE TABLE item (id INTEGER PRIMARY KEY)')
    storage.exec('CREATE TABLE audit (id INTEGER PRIMARY KEY)')
    storage.exec('CREATE TABLE unrelated (id INTEGER PRIMARY KEY)')
    storage.exec(
      `CREATE TRIGGER item_audit AFTER INSERT ON item BEGIN
         INSERT INTO 'audit' VALUES (NEW.id);
       END`
    )
    storage.exec(TX_MANIFEST_DDL)
    storage.exec(
      `INSERT INTO "${TX_MANIFEST_TABLE}" (tx_id, owner, original, snapshot) VALUES (?, ?, ?, '')`,
      'tx-fallback',
      'orez-embed',
      'item'
    )

    expect(snapshotSideEffectWriteTables(storage.journal, 'tx-fallback', 'item')).toBe(
      true
    )
    expect(
      storage
        .exec(
          `SELECT original FROM "${TX_MANIFEST_TABLE}" WHERE tx_id = ? ORDER BY original`,
          'tx-fallback'
        )
        .toArray()
        .map((row) => String(row.original))
    ).toEqual(['audit', 'item', 'unrelated'])
  })

  it('restores tables, data, indexes, triggers, and views after transactional DDL', () => {
    const storage = createSqliteStorage()
    storage.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL)')
    storage.exec('CREATE TABLE audit (item_id INTEGER, value TEXT)')
    storage.exec('CREATE INDEX items_value ON items(value)')
    storage.exec(
      'CREATE TRIGGER items_audit AFTER INSERT ON items BEGIN INSERT INTO audit VALUES (NEW.id, NEW.value); END'
    )
    storage.exec('CREATE VIEW item_values AS SELECT value FROM items')
    storage.exec("INSERT INTO items VALUES (1, 'before')")
    storage.exec('CREATE TABLE _zero_changes (watermark INTEGER PRIMARY KEY, value TEXT)')
    storage.exec("INSERT INTO _zero_changes VALUES (1, 'framework')")

    storage.transactionSync(() =>
      snapshotTxSchema(storage.journal, 'schema-tx', 'embed', ['items'])
    )
    storage.exec('DROP VIEW item_values')
    storage.exec('DROP TABLE items')
    storage.exec('CREATE TABLE items (id TEXT PRIMARY KEY, replacement INTEGER)')
    storage.exec("INSERT INTO items VALUES ('after', 2)")
    storage.exec('CREATE TABLE created_in_tx (id INTEGER)')
    storage.exec("UPDATE _zero_changes SET value = 'still-framework'")

    storage.transactionSync(() => rollbackTxJournal(storage.journal, 'schema-tx'))

    expect(storage.rows('items')).toEqual([{ id: 1, value: 'before' }])
    expect(storage.rows('audit')).toEqual([{ item_id: 1, value: 'before' }])
    expect(storage.rows('item_values')).toEqual([{ value: 'before' }])
    expect(storage.rows('_zero_changes')).toEqual([
      { watermark: 1, value: 'still-framework' },
    ])
    expect(
      storage
        .exec(
          "SELECT type, name FROM sqlite_master WHERE name IN ('items_value', 'items_audit') ORDER BY name"
        )
        .toArray()
    ).toEqual([
      { type: 'trigger', name: 'items_audit' },
      { type: 'index', name: 'items_value' },
    ])
    expect(
      storage.exec("SELECT 1 FROM sqlite_master WHERE name = 'created_in_tx'").toArray()
    ).toEqual([])
  })

  it('recovers DDL started against an empty schema after its owner dies', () => {
    const storage = createSqliteStorage()
    storage.transactionSync(() => snapshotTxSchema(storage.journal, 'dead-ddl', 'embed'))
    storage.exec('CREATE TABLE partial (id INTEGER PRIMARY KEY)')
    storage.exec('INSERT INTO partial VALUES (1)')

    expect(
      storage.transactionSync(() => recoverTxJournal(storage.journal, 'embed'))
    ).toEqual(['dead-ddl'])
    expect(
      storage.exec("SELECT 1 FROM sqlite_master WHERE name = 'partial'").toArray()
    ).toEqual([])
  })

  it('commits transactional DDL and removes its recovery image', () => {
    const storage = createSqliteStorage()
    storage.exec('CREATE TABLE original (id INTEGER PRIMARY KEY)')
    storage.transactionSync(() =>
      snapshotTxSchema(storage.journal, 'commit-ddl', 'embed', ['original'])
    )
    storage.exec('ALTER TABLE original ADD COLUMN value TEXT')
    storage.transactionSync(() => commitTxJournal(storage.journal, 'commit-ddl'))

    expect(
      storage
        .exec('PRAGMA table_info(original)')
        .toArray()
        .map((row) => row.name)
    ).toEqual(['id', 'value'])
    expect(recoverTxJournal(storage.journal, 'embed')).toEqual([])
    expect(
      storage
        .exec("SELECT name FROM sqlite_master WHERE name GLOB '_orez_tx_undo_*'")
        .toArray()
    ).toEqual([])
  })

  it('commit drops snapshots and manifest rows, keeps the data', () => {
    const storage = createSqliteStorage()
    storage.exec('CREATE TABLE items (id TEXT PRIMARY KEY, body TEXT)')
    storage.exec("INSERT INTO items VALUES ('a', 'one')")
    snapshotTx(storage, 'tx1', 'items')
    storage.exec("INSERT INTO items VALUES ('b', 'two')")

    storage.transactionSync(() => commitTxJournal(storage.journal, 'tx1'))

    expect(storage.rows('items').map((row) => row.id)).toEqual(['a', 'b'])
    expect(
      storage
        .tables()
        .filter((name) => name.startsWith('_orez_tx_') && name !== TX_MANIFEST_TABLE)
    ).toEqual([])
    expect(storage.rows(TX_MANIFEST_TABLE)).toEqual([])
  })

  it('rollback restores snapshotted tables and drops tables created in-tx', () => {
    const storage = createSqliteStorage()
    storage.exec('CREATE TABLE items (id TEXT PRIMARY KEY, body TEXT)')
    storage.exec("INSERT INTO items VALUES ('a', 'one')")
    snapshotTx(storage, 'tx1', 'items')
    storage.exec("UPDATE items SET body = 'mutated' WHERE id = 'a'")
    storage.exec("INSERT INTO items VALUES ('b', 'two')")
    snapshotTx(storage, 'tx1', 'created_in_tx', { exists: false })
    storage.exec('CREATE TABLE created_in_tx (id TEXT)')

    storage.transactionSync(() => rollbackTxJournal(storage.journal, 'tx1'))

    expect(storage.rows('items')).toEqual([{ id: 'a', body: 'one' }])
    expect(storage.tables()).not.toContain('created_in_tx')
    expect(
      storage
        .tables()
        .filter((name) => name.startsWith('_orez_tx_') && name !== TX_MANIFEST_TABLE)
    ).toEqual([])
  })

  it('rollback does not fire table triggers while restoring', () => {
    const storage = createSqliteStorage()
    storage.exec('CREATE TABLE items (id TEXT PRIMARY KEY)')
    storage.exec('CREATE TABLE audit (id TEXT)')
    storage.exec(
      'CREATE TRIGGER items_audit AFTER INSERT ON items BEGIN INSERT INTO audit VALUES (new.id); END'
    )
    storage.exec("INSERT INTO items VALUES ('a')")
    expect(storage.rows('audit')).toHaveLength(1)

    snapshotTx(storage, 'tx1', 'items')
    storage.exec("INSERT INTO items VALUES ('b')")
    expect(storage.rows('audit')).toHaveLength(2)

    storage.transactionSync(() => rollbackTxJournal(storage.journal, 'tx1'))

    // restore re-inserted row 'a' but the trigger must not have re-fired
    expect(storage.rows('items').map((row) => row.id)).toEqual(['a'])
    expect(storage.rows('audit')).toHaveLength(2)
    // and the trigger survives for future writes
    storage.exec("INSERT INTO items VALUES ('c')")
    expect(storage.rows('audit')).toHaveLength(3)
  })

  it.each(['rollback', 'recovery'] as const)(
    'restores cyclic cascading foreign keys during %s',
    (mode) => {
      const storage = createSqliteStorage()
      storage.exec('PRAGMA foreign_keys = ON')
      storage.exec(
        'CREATE TABLE a (' +
          'id INTEGER PRIMARY KEY, ' +
          'bid INTEGER REFERENCES b(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)'
      )
      storage.exec(
        'CREATE TABLE b (' +
          'id INTEGER PRIMARY KEY, ' +
          'aid INTEGER REFERENCES a(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)'
      )
      storage.transactionSync(() => {
        storage.exec('INSERT INTO a VALUES (1, NULL)')
        storage.exec('INSERT INTO b VALUES (1, 1)')
        storage.exec('UPDATE a SET bid = 1 WHERE id = 1')
      })
      snapshotTx(storage, 'cycle-tx', 'a', { owner: 'orez-embed' })
      snapshotTx(storage, 'cycle-tx', 'b', { owner: 'orez-embed' })
      storage.exec('UPDATE a SET bid = NULL')
      storage.exec('UPDATE b SET aid = NULL')

      if (mode === 'rollback') {
        storage.transactionSync(() => rollbackTxJournal(storage.journal, 'cycle-tx'))
      } else {
        expect(
          storage.transactionSync(() => recoverTxJournal(storage.journal, 'orez-embed'))
        ).toEqual(['cycle-tx'])
      }

      expect(storage.rows('a')).toEqual([{ id: 1, bid: 1 }])
      expect(storage.rows('b')).toEqual([{ id: 1, aid: 1 }])
    }
  )

  it('recovery rolls back only the requested owner and sweeps unreferenced snapshots', () => {
    const storage = createSqliteStorage()
    storage.exec('CREATE TABLE embed_table (id TEXT)')
    storage.exec('CREATE TABLE app_table (id TEXT)')
    storage.exec("INSERT INTO embed_table VALUES ('clean')")
    storage.exec("INSERT INTO app_table VALUES ('clean')")

    snapshotTx(storage, 'embed-tx', 'embed_table', { owner: 'orez-embed' })
    storage.exec("INSERT INTO embed_table VALUES ('partial')")
    snapshotTx(storage, 'app-tx', 'app_table', { owner: 'default' })
    storage.exec("INSERT INTO app_table VALUES ('in-flight')")

    // pre-journal leftover: snapshot table with no manifest row
    storage.exec('CREATE TABLE _orez_tx_dead_0_old (id TEXT)')

    const recovered = storage.transactionSync(() =>
      recoverTxJournal(storage.journal, 'orez-embed')
    )

    expect(recovered).toEqual(['embed-tx'])
    // embed's partial tx rolled back
    expect(storage.rows('embed_table').map((row) => row.id)).toEqual(['clean'])
    // the other owner's live tx untouched (writes + snapshot intact)
    expect(storage.rows('app_table').map((row) => row.id)).toEqual(['clean', 'in-flight'])
    expect(storage.tables()).toContain('_orez_tx_app-tx_0_app_table')
    // unreferenced leftover swept
    expect(storage.tables()).not.toContain('_orez_tx_dead_0_old')
  })

  it('recovery without owner rolls back every journaled tx', () => {
    const storage = createSqliteStorage()
    storage.exec('CREATE TABLE items (id TEXT)')
    snapshotTx(storage, 'tx1', 'items', { owner: 'a' })
    storage.exec("INSERT INTO items VALUES ('x')")

    const recovered = storage.transactionSync(() => recoverTxJournal(storage.journal))

    expect(recovered).toEqual(['tx1'])
    expect(storage.rows('items')).toEqual([])
  })

  it('runs row-journal recovery before clearing its manifest marker', () => {
    const storage = createSqliteStorage()
    storage.exec('CREATE TABLE items (id TEXT PRIMARY KEY, body TEXT)')
    storage.exec("INSERT INTO items VALUES ('a', 'before')")
    storage.exec(
      `CREATE TABLE "${TX_MANIFEST_TABLE}" (seq INTEGER PRIMARY KEY AUTOINCREMENT, tx_id TEXT NOT NULL, owner TEXT NOT NULL DEFAULT 'default', original TEXT NOT NULL, snapshot TEXT)`
    )
    storage.exec(
      `INSERT INTO "${TX_MANIFEST_TABLE}" (tx_id, owner, original, snapshot) VALUES (?, ?, ?, ?)`,
      'row-tx',
      'default',
      'items',
      ''
    )
    storage.exec("UPDATE items SET body = 'after' WHERE id = 'a'")

    const recovered = storage.transactionSync(() =>
      recoverTxJournal(storage.journal, undefined, (txID) => {
        expect(txID).toBe('row-tx')
        expect(storage.rows(TX_MANIFEST_TABLE)).toHaveLength(1)
        storage.exec("UPDATE items SET body = 'before' WHERE id = 'a'")
      })
    )

    expect(recovered).toEqual(['row-tx'])
    expect(storage.rows('items')).toEqual([{ id: 'a', body: 'before' }])
    expect(storage.rows(TX_MANIFEST_TABLE)).toEqual([])
  })

  it('recovery is a no-op on a store with no journal', () => {
    const storage = createSqliteStorage()
    storage.exec('CREATE TABLE items (id TEXT)')
    expect(storage.transactionSync(() => recoverTxJournal(storage.journal))).toEqual([])
    expect(storage.tables()).toContain('items')
  })
})

// ── kill-mid-tx through a real DoBackend over HTTP (the ZeroSqlDO shape) ──

let servers: Server[] = []

afterEach(async () => {
  await Promise.all(
    servers.map((server) => new Promise<void>((resolve) => server.close(() => resolve())))
  )
  servers = []
})

/**
 * a fake ZeroDO: serves /exec, /batch, /commit-tx, /rollback-tx, /recover-txs
 * against real sqlite using the same journal core the production DO uses.
 */
function startSqliteDoServer(storage: SqliteStorage): Promise<string> {
  const cdc = new TransactionalCdc(storage.journal)
  const execute = (statement: any) => {
    const track = statement.track
    const transactionID = String(statement.transactionID || track?.transactionID || '')
    if (track?.physicalTableName) {
      cdc.ensureTable({
        physicalTableName: track.physicalTableName,
        tableName: track.tableName,
        publish: false,
        ...(track.rowColumns?.length ? { columns: track.rowColumns } : null),
      })
    }
    const cursor = storage.exec(
      String(statement.sql ?? ''),
      ...(Array.isArray(statement.params) ? statement.params : [])
    )
    const rows = cursor.toArray()
    const captured = track ? cdc.drain() : []
    if (captured.length > 0) {
      ensurePendingChangesTable(storage.journal)
      for (const change of captured) {
        appendPendingChange(storage.journal, {
          transactionID,
          physicalTableName: change.physicalTableName,
          tableName: change.tableName,
          publish: false,
          op: change.op,
          rowData: change.rowData,
          oldData: change.oldData,
          rowJournal: change.rowJournal,
          oldJournal: change.oldJournal,
          newRowid: change.newRowid,
          oldRowid: change.oldRowid,
          undoable: true,
        })
      }
    }
    return {
      rows: track?.returnRows ? rows : track ? [] : rows,
      columns: track?.returnRows ? cursor.columnNames : track ? [] : cursor.columnNames,
      capturedChanges: 0,
    }
  }
  const server = createServer((req, res) => {
    const url = new URL(req.url || '/', 'http://127.0.0.1')
    let body = ''
    req.on('data', (chunk) => {
      body += chunk
    })
    req.on('end', () => {
      const parsed = body ? JSON.parse(body) : {}
      const respond = (status: number, payload: unknown) => {
        res.statusCode = status
        res.setHeader('content-type', 'application/json')
        res.end(JSON.stringify(payload))
      }
      try {
        switch (url.pathname) {
          case '/exec': {
            const result = parsed.track
              ? storage.transactionSync(() => execute(parsed))
              : execute(parsed)
            respond(200, result)
            return
          }
          case '/batch': {
            const statements = Array.isArray(parsed.statements) ? parsed.statements : []
            const results = storage.transactionSync(() =>
              statements
                .map((statement: any) =>
                  typeof statement === 'string' ? { sql: statement } : statement
                )
                .filter((statement: any) => statement?.sql?.trim())
                .map((statement: any) => execute(statement))
            )
            respond(200, { results })
            return
          }
          case '/commit-tx': {
            storage.transactionSync(() => {
              const transactionID = String(parsed.transactionID)
              deletePendingChanges(storage.journal, transactionID)
              commitTxJournal(storage.journal, transactionID)
            })
            respond(200, { ok: true })
            return
          }
          case '/rollback-tx': {
            storage.transactionSync(() => {
              const transactionID = String(parsed.transactionID)
              rollbackPendingChanges(storage.journal, transactionID)
              rollbackTxJournal(storage.journal, transactionID)
              deletePendingChanges(storage.journal, transactionID)
            })
            respond(200, { ok: true })
            return
          }
          case '/recover-txs': {
            const transactionIDs = storage.transactionSync(() =>
              recoverTxJournal(
                storage.journal,
                parsed.owner === undefined ? undefined : String(parsed.owner),
                (transactionID) => {
                  rollbackPendingChanges(storage.journal, transactionID)
                  deletePendingChanges(storage.journal, transactionID)
                }
              )
            )
            respond(200, { ok: true, transactionIDs })
            return
          }
          default:
            respond(404, { error: 'not found' })
        }
      } catch (err: any) {
        respond(500, { error: err.message })
      }
    })
  })
  servers.push(server)
  return new Promise((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', () => {
      const addr = server.address()
      if (!addr || typeof addr === 'string') {
        reject(new Error('no tcp port'))
        return
      }
      resolve(`http://127.0.0.1:${addr.port}`)
    })
  })
}

describe('kill-mid-tx crash recovery (DoBackend over HTTP)', () => {
  it('a tx abandoned mid-flight is invisible after recovery; a committed tx survives', async () => {
    const storage = createSqliteStorage()
    storage.exec('CREATE TABLE "changeLog" (watermark TEXT, pos INTEGER, change TEXT)')
    storage.exec('CREATE TABLE "replicationState" ("lastWatermark" TEXT)')
    storage.exec(`INSERT INTO "replicationState" VALUES ('100')`)
    const url = await startSqliteDoServer(storage)

    // generation 1: the change-streamer's storer writes a replicated tx —
    // changeLog entries plus the watermark advance — then the DO is killed
    // before COMMIT (deploy upgrade-kill). writes apply eagerly, so without
    // recovery the partial tx persists and poisons catchup.
    const gen1 = new DoBackend(url, 'zero_cdb', 'zero', { txOwner: 'orez-embed' })
    await gen1.waitReady
    await gen1.exec('BEGIN')
    await gen1.query(
      `INSERT INTO "changeLog" (watermark, pos, change) VALUES ($1, $2, $3)`,
      ['101', 0, '{"tag":"begin"}']
    )
    await gen1.query(
      `INSERT INTO "changeLog" (watermark, pos, change) VALUES ($1, $2, $3)`,
      ['101', 1, '{"tag":"insert"}']
    )
    await gen1.query(`UPDATE "replicationState" SET "lastWatermark" = $1`, ['101'])
    // KILL: no COMMIT, client state gone, storage survives
    expect(storage.rows('changeLog')).toHaveLength(2)

    // generation 2 boots and recovers its dead predecessor's transactions
    // before opening any pg session (zero-cache-embed-cf does exactly this).
    const resp = await fetch(`${url}/recover-txs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ owner: 'orez-embed' }),
    })
    const recovered = (await resp.json()) as { transactionIDs: string[] }
    expect(recovered.transactionIDs).toHaveLength(1)

    // the partial tx is invisible: changeLog rolled back, watermark restored
    expect(storage.rows('changeLog')).toHaveLength(0)
    expect(storage.rows('replicationState')).toEqual([{ lastWatermark: '100' }])
    expect(
      storage
        .tables()
        .filter((name) => name.startsWith('_orez_tx_') && name !== TX_MANIFEST_TABLE)
    ).toEqual([])

    // generation 2 re-applies the tx and commits — this time it sticks
    const gen2 = new DoBackend(url, 'zero_cdb', 'zero', { txOwner: 'orez-embed' })
    await gen2.waitReady
    await gen2.exec('BEGIN')
    await gen2.query(
      `INSERT INTO "changeLog" (watermark, pos, change) VALUES ($1, $2, $3)`,
      ['101', 0, '{"tag":"begin"}']
    )
    await gen2.query(`UPDATE "replicationState" SET "lastWatermark" = $1`, ['101'])
    await gen2.exec('COMMIT')

    expect(storage.rows('changeLog')).toHaveLength(1)
    expect(storage.rows('replicationState')).toEqual([{ lastWatermark: '101' }])
    expect(storage.rows(TX_MANIFEST_TABLE)).toEqual([])

    // a later recovery pass must not touch the committed data
    const resp2 = await fetch(`${url}/recover-txs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ owner: 'orez-embed' }),
    })
    expect(((await resp2.json()) as { transactionIDs: string[] }).transactionIDs).toEqual(
      []
    )
    expect(storage.rows('changeLog')).toHaveLength(1)
  })
})

describe('kill-mid-tx crash recovery (embed-local backend)', () => {
  it('allows embed schema migrations while preserving unrelated committed data on rollback', async () => {
    const storage = createSqliteStorage()
    storage.exec('CREATE TABLE migration_target (id INTEGER PRIMARY KEY, value TEXT)')
    storage.exec("INSERT INTO migration_target VALUES (1, 'before')")
    storage.exec('CREATE TABLE unrelated (id INTEGER PRIMARY KEY, value TEXT)')
    storage.exec("INSERT INTO unrelated VALUES (1, 'before')")
    storage.exec('CREATE TABLE tx_dml (id INTEGER PRIMARY KEY, value TEXT)')
    storage.exec("INSERT INTO tx_dml VALUES (1, 'before')")
    const local = createLocalSqlBackend({
      exec: storage.exec,
      transactionSync: storage.transactionSync,
    })
    const backend = new DoBackend('https://orez-do-backend.local', 'zero_cvr', 'zero', {
      fetch: local.fetch,
      txOwner: 'orez-embed',
    })
    await backend.waitReady

    await backend.query('BEGIN')
    await backend.query('ALTER TABLE migration_target ADD COLUMN extra TEXT')
    await backend.query('CREATE TABLE created_by_migration (id INTEGER PRIMARY KEY)')
    await backend.query("UPDATE migration_target SET value = 'during', extra = 'x'")
    await backend.query("UPDATE tx_dml SET value = 'during'")
    // Simulate an independently committed application write while the schema
    // transaction is open. Rollback must not replace an unrelated table from
    // a database-wide data snapshot.
    storage.exec("UPDATE unrelated SET value = 'committed elsewhere'")
    await backend.query('ROLLBACK')

    expect(
      storage
        .exec('PRAGMA table_info(migration_target)')
        .toArray()
        .map((row) => row.name)
    ).toEqual(['id', 'value'])
    expect(storage.rows('migration_target')).toEqual([{ id: 1, value: 'before' }])
    expect(storage.rows('unrelated')).toEqual([{ id: 1, value: 'committed elsewhere' }])
    expect(storage.rows('tx_dml')).toEqual([{ id: 1, value: 'before' }])
    expect(
      storage
        .exec("SELECT 1 FROM sqlite_master WHERE name = 'created_by_migration'")
        .toArray()
    ).toEqual([])
  })

  it('does not attach a committed autocommit row to the next transaction', async () => {
    const storage = createSqliteStorage()
    storage.exec('CREATE TABLE "cvr_rows" (id TEXT PRIMARY KEY, version TEXT)')
    storage.exec(`INSERT INTO "cvr_rows" VALUES ('r1', 'v1')`)
    const local = createLocalSqlBackend({
      exec: storage.exec,
      transactionSync: storage.transactionSync,
    })
    const backend = new DoBackend('https://orez-do-backend.local', 'zero_cvr', 'zero', {
      fetch: local.fetch,
      txOwner: 'orez-embed',
    })
    await backend.waitReady

    await backend.exec('BEGIN')
    await backend.query(`UPDATE "cvr_rows" SET version = $1 WHERE id = $2`, ['v2', 'r1'])
    await backend.exec('COMMIT')
    await backend.query(`UPDATE "cvr_rows" SET version = $1 WHERE id = $2`, ['v3', 'r1'])
    await backend.exec('BEGIN')
    await backend.query(`UPDATE "cvr_rows" SET version = $1 WHERE id = $2`, ['v4', 'r1'])
    await backend.exec('ROLLBACK')

    expect(storage.rows('cvr_rows')).toEqual([{ id: 'r1', version: 'v3' }])
  })

  it('re-derives its cdc cache when a batch aborts', async () => {
    const storage = createSqliteStorage()
    storage.exec('CREATE TABLE "cvr_rows" (id TEXT PRIMARY KEY, version TEXT)')
    const local = createLocalSqlBackend({
      exec: storage.exec,
      transactionSync: storage.transactionSync,
    })
    const post = (path: string, body: unknown) =>
      local.fetch(`https://orez-do-backend.local${path}`, {
        method: 'POST',
        body: JSON.stringify(body),
      })
    const track = {
      physicalTableName: 'cvr_rows',
      tableName: 'cvr_rows',
      operation: 'INSERT' as const,
    }

    // the batch registers cvr_rows for capture, installing its triggers, then
    // trips the primary key. transactionSync() rolls the triggers back while
    // the CDC object still remembers installing them.
    const failed = await post('/batch', {
      statements: [
        {
          sql: `INSERT INTO "cvr_rows" VALUES ('r1', 'v1')`,
          track,
          transactionID: 'tx-a',
        },
        {
          sql: `INSERT INTO "cvr_rows" VALUES ('r1', 'v2')`,
          track,
          transactionID: 'tx-a',
        },
      ],
    })
    expect(failed.status).toBe(500)
    expect(storage.rows('cvr_rows')).toHaveLength(0)

    // a stale cache would short-circuit ensureTable, leave the table with no
    // trigger, and record nothing to roll this write back with.
    const ok = await post('/batch', {
      statements: [
        {
          sql: `INSERT INTO "cvr_rows" VALUES ('r2', 'v1')`,
          track,
          transactionID: 'tx-b',
        },
      ],
    })
    expect(ok.status).toBe(200)
    expect(storage.rows('cvr_rows')).toHaveLength(1)

    await post('/rollback-tx', { transactionID: 'tx-b' })
    expect(storage.rows('cvr_rows')).toHaveLength(0)
  })

  it('the local cvr/cdb store recovers an abandoned tx at embed boot', async () => {
    const storage = createSqliteStorage()
    storage.exec('CREATE TABLE "cvr_rows" (id TEXT PRIMARY KEY, version TEXT)')
    storage.exec(`INSERT INTO "cvr_rows" VALUES ('r1', 'v1')`)
    const local = createLocalSqlBackend({
      exec: storage.exec,
      transactionSync: storage.transactionSync,
    })

    const gen1 = new DoBackend('https://orez-do-backend.local', 'zero_cvr', 'zero', {
      fetch: local.fetch,
      txOwner: 'orez-embed',
    })
    await gen1.waitReady
    await gen1.exec('BEGIN')
    await gen1.query(`UPDATE "cvr_rows" SET version = $1 WHERE id = $2`, ['v2', 'r1'])
    await gen1.query(`INSERT INTO "cvr_rows" (id, version) VALUES ($1, $2)`, ['r2', 'v2'])
    // KILL mid-tx
    expect(storage.rows('cvr_rows')).toHaveLength(2)

    // next embed boot recovers before opening sessions
    expect(local.recoverOrphanedTransactions()).toHaveLength(1)
    expect(storage.rows('cvr_rows')).toEqual([{ id: 'r1', version: 'v1' }])

    // a clean commit afterwards persists
    const gen2 = new DoBackend('https://orez-do-backend.local', 'zero_cvr', 'zero', {
      fetch: local.fetch,
      txOwner: 'orez-embed',
    })
    await gen2.waitReady
    await gen2.exec('BEGIN')
    await gen2.query(`UPDATE "cvr_rows" SET version = $1 WHERE id = $2`, ['v3', 'r1'])
    await gen2.exec('COMMIT')
    expect(storage.rows('cvr_rows')).toEqual([{ id: 'r1', version: 'v3' }])
    expect(local.recoverOrphanedTransactions()).toEqual([])
    expect(storage.rows('cvr_rows')).toEqual([{ id: 'r1', version: 'v3' }])
  })

  it('rejects change-tracking requests (tracking belongs to the shared upstream db)', async () => {
    const storage = createSqliteStorage()
    const local = createLocalSqlBackend({
      exec: storage.exec,
      transactionSync: storage.transactionSync,
    })
    const resp = await local.fetch('https://orez-do-backend.local/exec', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: 'SELECT 1',
        track: { tableName: 'x', operation: 'INSERT' },
      }),
    })
    expect(resp.status).toBe(500)
    expect(((await resp.json()) as { error: string }).error).toContain('change tracking')
  })
})

describe('snapshot escalation naming', () => {
  it('gives colliding table names distinct snapshots', () => {
    const storage = createSqliteStorage()
    // `a-b` and `a_b` sanitize to the same identifier, so a name derived from
    // the table would put both tables in one snapshot.
    storage.exec('CREATE TABLE "a-b" (v TEXT)')
    storage.exec('CREATE TABLE "a_b" (v TEXT)')
    storage.exec(`INSERT INTO "a-b" VALUES ('dash')`)
    storage.exec(`INSERT INTO "a_b" VALUES ('underscore')`)
    storage.exec(TX_MANIFEST_DDL)
    for (const table of ['a-b', 'a_b']) {
      storage.exec(
        `INSERT INTO "${TX_MANIFEST_TABLE}" (tx_id, owner, original, snapshot) VALUES (?, ?, ?, ?)`,
        'tx-x',
        'orez-embed',
        table,
        ''
      )
      upgradeToTableSnapshot(storage.journal, 'tx-x', table)
      storage.exec(`INSERT INTO "${table}" VALUES ('written')`)
    }

    const snapshots = storage
      .exec(`SELECT snapshot FROM "${TX_MANIFEST_TABLE}" WHERE tx_id = 'tx-x'`)
      .toArray()
      .map((row) => String(row.snapshot))
    expect(new Set(snapshots).size).toBe(2)

    storage.transactionSync(() => rollbackTxJournal(storage.journal, 'tx-x'))
    expect(storage.rows('a-b')).toEqual([{ v: 'dash' }])
    expect(storage.rows('a_b')).toEqual([{ v: 'underscore' }])
  })
})
