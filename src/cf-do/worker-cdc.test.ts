// @ts-expect-error - CJS module
import BedrockSqlite from 'bedrock-sqlite'
import { describe, expect, it, vi } from 'vitest'

import { TransactionalCdc } from './cdc.js'
import { DurableWatermarkState } from './watermark.js'

vi.mock('cloudflare:workers', () => ({
  DurableObject: class {
    constructor(ctx: unknown) {
      this.ctx = ctx
    }
  },
}))

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
      columnNames: stmt.reader ? stmt.columns().map((column: any) => column.name) : [],
    }
  }
  return { nativeDb, sql: { exec } }
}

async function createWorkerCore() {
  const { ZeroDO } = await import('./worker.js')
  const storage = createSqliteStorage()
  const zero = Object.create(ZeroDO.prototype) as any
  zero.sql = storage.sql
  zero.cdc = new TransactionalCdc(storage.sql)
  zero.watermarks = new DurableWatermarkState(storage.sql)
  zero.writeBudget = { recordLogical() {} }
  zero.tableSchemas = new Map()
  zero.schemaTables = new Set<string>()
  zero.pendingChangesSchemaReady = false
  zero.applicationSqlTurnWaiters = []
  // A real transaction boundary: an abort has to roll the SQLite side back, or
  // the cache-staleness regressions below cannot be observed at all.
  const runTransaction = <T>(work: () => T): T => {
    storage.nativeDb.exec('BEGIN')
    try {
      const result = work()
      storage.nativeDb.exec('COMMIT')
      return result
    } catch (error) {
      storage.nativeDb.exec('ROLLBACK')
      throw error
    }
  }
  zero.ctx = {
    storage: {
      transaction: async <T>(work: () => T) => runTransaction(work),
      transactionSync: runTransaction,
    },
  }
  return { ...storage, zero }
}

function batchRequest(statements: unknown[]) {
  return new Request('http://do/batch', {
    method: 'POST',
    body: JSON.stringify({ statements }),
  })
}

const ITEM_TRACK = {
  physicalTableName: 'item',
  tableName: 'public.item',
  operation: 'INSERT' as const,
  rowColumns: ['id', 'body'],
}

describe('ZeroDO transactional CDC integration', () => {
  it('publishes a tracked write and its business-trigger side effect exactly once', async () => {
    const { sql, zero } = await createWorkerCore()
    sql.exec('CREATE TABLE channel (id TEXT PRIMARY KEY, message_count INTEGER NOT NULL)')
    sql.exec('CREATE TABLE message (id TEXT PRIMARY KEY, channel_id TEXT NOT NULL)')
    zero.cdc.syncTables([
      { physicalTableName: 'channel', tableName: 'public.channel' },
      { physicalTableName: 'message', tableName: 'public.message' },
    ])
    sql.exec("INSERT INTO channel VALUES ('general', 0)")
    zero.cdc.drain()
    sql.exec(
      `CREATE TRIGGER message_count AFTER INSERT ON message BEGIN
         UPDATE channel SET message_count = message_count + 1 WHERE id = NEW.channel_id;
       END`
    )

    const result = zero.executeSQL(
      "INSERT INTO message VALUES ('m1', 'general') RETURNING *",
      [],
      {
        physicalTableName: 'message',
        tableName: 'public.message',
        operation: 'INSERT',
        rowColumns: ['id', 'channel_id'],
        returnRows: true,
      }
    )

    expect(result).toMatchObject({
      rows: [{ id: 'm1', channel_id: 'general' }],
      affectedRows: 1,
      capturedChanges: 2,
    })
    const changes = zero.readChangesSince(0)
    expect(changes).toHaveLength(2)
    expect(changes).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          tableName: 'public.message',
          op: 'INSERT',
          rowData: { id: 'm1', channel_id: 'general' },
        }),
        expect.objectContaining({
          tableName: 'public.channel',
          op: 'UPDATE',
          rowData: { id: 'general', message_count: 1 },
          oldData: { id: 'general', message_count: 0 },
        }),
      ])
    )
  })

  it('keeps trigger-captured rows pending until the emulated transaction commits', async () => {
    const { sql, zero } = await createWorkerCore()
    sql.exec('CREATE TABLE item (id TEXT PRIMARY KEY, body TEXT)')

    zero.executeSQL(
      "INSERT INTO item VALUES ('a', 'pending') RETURNING *",
      [],
      {
        physicalTableName: 'item',
        tableName: 'public.item',
        operation: 'INSERT',
        rowColumns: ['id', 'body'],
      },
      'tx-commit'
    )
    expect(zero.readChangesSince(0)).toEqual([])
    expect(zero.commitPendingTrackedChanges('tx-commit')).toBe(1)
    expect(zero.readChangesSince(0)).toMatchObject([
      { tableName: 'public.item', op: 'INSERT', rowData: { id: 'a', body: 'pending' } },
    ])

    zero.executeSQL(
      "UPDATE item SET body = 'rolled back' WHERE id = 'a' RETURNING *",
      [],
      {
        physicalTableName: 'item',
        tableName: 'public.item',
        operation: 'UPDATE',
        rowColumns: ['id', 'body'],
      },
      'tx-rollback'
    )
    expect(sql.exec("SELECT body FROM item WHERE id = 'a'").one()).toEqual({
      body: 'rolled back',
    })
    expect(zero.rollbackPendingTrackedChanges('tx-rollback')).toBe(1)
    expect(sql.exec("SELECT body FROM item WHERE id = 'a'").one()).toEqual({
      body: 'pending',
    })
    expect(zero.deletePendingTrackedChanges('tx-rollback')).toBe(1)
    expect(zero.commitPendingTrackedChanges('tx-rollback')).toBe(0)
    expect(zero.readChangesSince(0)).toHaveLength(1)
  })

  it('uses private-table row images for rollback without publishing them', async () => {
    const { sql, zero } = await createWorkerCore()
    sql.exec('CREATE TABLE private_note (id TEXT PRIMARY KEY, body TEXT)')

    zero.executeSQL(
      "INSERT INTO private_note VALUES ('n1', 'private') RETURNING *",
      [],
      {
        physicalTableName: 'private_note',
        tableName: 'public.private_note',
        operation: 'INSERT',
        rowColumns: ['id', 'body'],
        publish: false,
      },
      'tx-private-commit'
    )
    expect(zero.commitPendingTrackedChanges('tx-private-commit')).toBe(0)
    expect(zero.readChangesSince(0)).toEqual([])
    expect(sql.exec('SELECT * FROM private_note').toArray()).toEqual([
      { id: 'n1', body: 'private' },
    ])

    zero.executeSQL(
      "UPDATE private_note SET body = 'discarded' WHERE id = 'n1' RETURNING *",
      [],
      {
        physicalTableName: 'private_note',
        tableName: 'public.private_note',
        operation: 'UPDATE',
        rowColumns: ['id', 'body'],
        publish: false,
      },
      'tx-private-rollback'
    )
    expect(zero.rollbackPendingTrackedChanges('tx-private-rollback')).toBe(1)
    zero.deletePendingTrackedChanges('tx-private-rollback')
    expect(sql.exec('SELECT * FROM private_note').toArray()).toEqual([
      { id: 'n1', body: 'private' },
    ])
    expect(zero.readChangesSince(0)).toEqual([])
  })

  it('rolls back an unannotated write to a registered private application table', async () => {
    const { sql, zero } = await createWorkerCore()
    sql.exec('CREATE TABLE private_note (id TEXT PRIMARY KEY, body TEXT)')

    await zero.applicationSqlRegisterTables([
      { table: 'private_note', publicTable: 'private.private_note', publish: false },
    ])
    await zero.applicationSqlBegin('application-private-rollback')
    await zero.applicationSqlSessionExec(
      'application-private-rollback',
      "INSERT INTO private_note VALUES ('n1', 'discarded')"
    )
    await zero.applicationSqlRollback('application-private-rollback')

    expect(sql.exec('SELECT * FROM private_note').toArray()).toEqual([])
    expect(zero.readChangesSince(0)).toEqual([])
  })

  it('defers the application schema snapshot until the session changes schema', async () => {
    const { sql, zero } = await createWorkerCore()
    sql.exec('CREATE TABLE item (id TEXT PRIMARY KEY, body TEXT)')

    await zero.applicationSqlBegin('application-schema-only')
    await zero.applicationSqlSessionQuery('application-schema-only', 'SELECT * FROM item')
    await zero.applicationSqlSessionExec(
      'application-schema-only',
      'CREATE TABLE IF NOT EXISTS item (id TEXT PRIMARY KEY, body TEXT)'
    )
    await zero.applicationSqlSessionRegisterTables('application-schema-only', [
      { table: 'item', publicTable: 'public.item' },
    ])

    expect(
      sql
        .exec(
          "SELECT name FROM sqlite_master WHERE type = 'table' AND name = '_orez_tx_schema'"
        )
        .toArray()
    ).toEqual([])

    await zero.applicationSqlSessionExec(
      'application-schema-only',
      'ALTER TABLE item ADD COLUMN extra TEXT'
    )
    expect(
      sql
        .exec("SELECT name FROM _orez_tx_schema WHERE tx_id = 'application-schema-only'")
        .toArray().length
    ).toBeGreaterThan(0)

    await zero.applicationSqlRollback('application-schema-only')
    expect(
      sql
        .exec('PRAGMA table_info(item)')
        .toArray()
        .map((column) => column.name)
    ).toEqual(['id', 'body'])
  })

  it('recovers an interrupted application session when the Durable Object is recreated', async () => {
    const { sql, nativeDb, zero } = await createWorkerCore()
    sql.exec('CREATE TABLE private_note (id TEXT PRIMARY KEY, body TEXT)')
    await zero.applicationSqlRegisterTables([
      { table: 'private_note', publicTable: 'private.private_note', publish: false },
    ])
    await zero.applicationSqlBegin('application-restart')
    await zero.applicationSqlSessionExec(
      'application-restart',
      "INSERT INTO private_note VALUES ('n1', 'interrupted')"
    )

    const { ZeroDO } = await import('./worker.js')
    let recovery: Promise<void> | undefined
    const transaction = <T>(work: () => T): T => {
      nativeDb.exec('BEGIN')
      try {
        const value = work()
        nativeDb.exec('COMMIT')
        return value
      } catch (error) {
        nativeDb.exec('ROLLBACK')
        throw error
      }
    }
    new ZeroDO(
      {
        storage: {
          sql,
          transaction: async <T>(work: () => T) => transaction(work),
          transactionSync: transaction,
        },
        blockConcurrencyWhile(work: () => Promise<void>) {
          recovery = work()
        },
      } as any,
      { OREZ_DO_WRITE_BUDGET_DISABLED: 'true' } as any
    )
    await recovery

    expect(sql.exec('SELECT * FROM private_note').toArray()).toEqual([])
  })

  it('captures a published side effect even when the initiating table is private', async () => {
    const { sql, zero } = await createWorkerCore()
    sql.exec('CREATE TABLE channel (id TEXT PRIMARY KEY, touched INTEGER NOT NULL)')
    sql.exec('CREATE TABLE private_event (id TEXT PRIMARY KEY, channel_id TEXT NOT NULL)')
    zero.cdc.syncTables([{ physicalTableName: 'channel', tableName: 'public.channel' }])
    sql.exec("INSERT INTO channel VALUES ('general', 0)")
    zero.cdc.drain()
    sql.exec(
      `CREATE TRIGGER private_event_touch AFTER INSERT ON private_event BEGIN
         UPDATE channel SET touched = touched + 1 WHERE id = NEW.channel_id;
       END`
    )

    const result = zero.executeSQL(
      "INSERT INTO private_event VALUES ('e1', 'general')",
      [],
      undefined,
      'tx-private'
    )

    expect(result).toMatchObject({ capturedChanges: 1 })
    expect(zero.readChangesSince(0)).toEqual([])
    expect(zero.commitPendingTrackedChanges('tx-private')).toBe(1)
    expect(zero.readChangesSince(0)).toMatchObject([
      {
        tableName: 'public.channel',
        op: 'UPDATE',
        rowData: { id: 'general', touched: 1 },
        oldData: { id: 'general', touched: 0 },
      },
    ])
  })

  it('executes captured-table DDL and resumes CDC with the new row shape', async () => {
    const { sql, zero } = await createWorkerCore()
    sql.exec('CREATE TABLE item (id TEXT PRIMARY KEY, removed TEXT, body TEXT)')
    zero.cdc.syncTables([{ physicalTableName: 'item', tableName: 'public.item' }])

    expect(() =>
      zero.executeSQL('ALTER TABLE "item" DROP COLUMN "removed"')
    ).not.toThrow()
    const result = zero.executeSQL(
      "INSERT INTO item (id, body) VALUES ('a', 'new shape') RETURNING *",
      [],
      {
        physicalTableName: 'item',
        tableName: 'public.item',
        operation: 'INSERT',
        rowColumns: ['id', 'body'],
      }
    )

    expect(result).toMatchObject({ capturedChanges: 1 })
    expect(zero.readChangesSince(0)).toMatchObject([
      {
        tableName: 'public.item',
        op: 'INSERT',
        rowData: { id: 'a', body: 'new shape' },
      },
    ])
  })
})

describe('ZeroDO cache state across an aborted storage transaction', () => {
  it('still captures a table whose registration a failed batch rolled back', async () => {
    const { sql, zero } = await createWorkerCore()
    sql.exec('CREATE TABLE item (id TEXT PRIMARY KEY, body TEXT UNIQUE)')

    // The batch registers `item`, installing its triggers and metadata, and
    // then trips the UNIQUE constraint. ctx.storage.transaction() rolls all of
    // it back, triggers included, while the CDC object still remembers
    // registering and verifying the table.
    const failed = await zero.handleBatch(
      batchRequest([
        { sql: "INSERT INTO item VALUES ('a', 'one')", track: ITEM_TRACK },
        { sql: "INSERT INTO item VALUES ('b', 'one')", track: ITEM_TRACK },
      ])
    )
    expect(failed.status).toBe(500)
    expect(sql.exec('SELECT count(*) AS c FROM item').one()).toEqual({ c: 0 })
    expect(
      sql.exec("SELECT count(*) AS c FROM sqlite_master WHERE type = 'trigger'").one()
    ).toEqual({ c: 0 })

    // A stale "registered and verified" cache would short-circuit ensureTable,
    // leave the table with no trigger, and drop this write from the changefeed.
    const ok = await zero.handleBatch(
      batchRequest([{ sql: "INSERT INTO item VALUES ('c', 'two')", track: ITEM_TRACK }])
    )
    expect(ok.status).toBe(200)
    expect(zero.readChangesSince(0)).toMatchObject([
      { tableName: 'public.item', op: 'INSERT', rowData: { id: 'c', body: 'two' } },
    ])
  })

  it('rebuilds the pending-changes and watermark tables a failed batch rolled back', async () => {
    const { sql, zero } = await createWorkerCore()
    sql.exec('CREATE TABLE item (id TEXT PRIMARY KEY, body TEXT UNIQUE)')

    // This batch creates _zero_pending_changes and _zero_changes as a side
    // effect of tracking, then aborts. Both CREATE TABLEs roll back while the
    // readiness flags still claim the tables exist.
    const failed = await zero.handleBatch(
      batchRequest([
        {
          sql: "INSERT INTO item VALUES ('a', 'one')",
          track: ITEM_TRACK,
          transactionID: 'tx-1',
        },
        { sql: "INSERT INTO item VALUES ('b', 'one')", track: ITEM_TRACK },
      ])
    )
    expect(failed.status).toBe(500)
    expect(zero.pendingChangesSchemaReady).toBe(false)

    // Stale flags would make these writes fail with "no such table".
    const ok = await zero.handleBatch(
      batchRequest([
        {
          sql: "INSERT INTO item VALUES ('c', 'two')",
          track: ITEM_TRACK,
          transactionID: 'tx-2',
        },
        { sql: "INSERT INTO item VALUES ('d', 'three')", track: ITEM_TRACK },
      ])
    )
    expect(ok.status).toBe(200)
    expect(zero.commitPendingTrackedChanges('tx-2')).toBe(1)
    expect(
      zero
        .readChangesSince(0)
        .map((change: any) => change.rowData.id)
        .sort()
    ).toEqual(['c', 'd'])
  })
})

describe('ZeroDO tracked writes on a table CDC cannot undo', () => {
  it('upgrades the row-journal marker to a real table snapshot', async () => {
    const { sql, zero } = await createWorkerCore()
    const { TX_MANIFEST_DDL, TX_MANIFEST_TABLE, rollbackTxJournal } =
      await import('./tx-journal.js')
    // No primary key, and every rowid alias is shadowed by a real column, so
    // there is no stable identity to undo a row by.
    sql.exec('CREATE TABLE weird (rowid TEXT, _rowid_ TEXT, oid TEXT, body TEXT)')
    sql.exec("INSERT INTO weird VALUES ('r1', 'r2', 'r3', 'before')")

    // DoBackend marked the table row-journaled before asking the DO whether it
    // could capture it.
    sql.exec(TX_MANIFEST_DDL)
    sql.exec(
      `INSERT INTO "${TX_MANIFEST_TABLE}" (tx_id, owner, original, snapshot) VALUES (?, ?, ?, ?)`,
      'tx-weird',
      'orez-embed',
      'weird',
      ''
    )

    zero.executeSQL(
      "INSERT INTO weird VALUES ('r4', 'r5', 'r6', 'written')",
      [],
      {
        physicalTableName: 'weird',
        tableName: 'public.weird',
        operation: 'INSERT',
        rowColumns: ['body'],
      },
      'tx-weird'
    )
    expect(sql.exec('SELECT count(*) AS c FROM weird').one()).toEqual({ c: 2 })

    // The empty marker promised a row-level rollback nothing can perform, so
    // the worker took the table copy the journal would otherwise have taken.
    const manifest = sql
      .exec(`SELECT snapshot FROM "${TX_MANIFEST_TABLE}" WHERE tx_id = 'tx-weird'`)
      .toArray()
    expect(manifest).toHaveLength(1)
    expect(String(manifest[0].snapshot)).not.toBe('')

    rollbackTxJournal(zero.sql, 'tx-weird')
    expect(sql.exec('SELECT body FROM weird').toArray()).toEqual([{ body: 'before' }])
  })
})

describe('ZeroDO triggered writes to private tables', () => {
  it.each(['rollback', 'recovery'] as const)(
    'restores unpublished side effects during %s',
    async (mode) => {
      const { sql, zero } = await createWorkerCore()
      const { TX_MANIFEST_DDL, TX_MANIFEST_TABLE, recoverTxJournal, rollbackTxJournal } =
        await import('./tx-journal.js')
      sql.exec('PRAGMA foreign_keys = ON')
      sql.exec('CREATE TABLE item (id INTEGER PRIMARY KEY, body TEXT)')
      sql.exec(
        'CREATE TABLE xorezYaudit (' +
          'id INTEGER PRIMARY KEY, item_id INTEGER NOT NULL, ' +
          'note TEXT)'
      )
      sql.exec(
        'CREATE TABLE azeroXprivate (' +
          'id INTEGER PRIMARY KEY, item_id INTEGER NOT NULL, ' +
          'note TEXT)'
      )
      sql.exec("INSERT INTO item VALUES (1, 'kept')")
      sql.exec("INSERT INTO xorezYaudit VALUES (1, 1, 'kept')")
      sql.exec("INSERT INTO azeroXprivate VALUES (1, 1, 'kept')")
      zero.cdc.syncTables([{ physicalTableName: 'item', tableName: 'public.item' }])
      // Creating the business trigger after CDC gives SQLite the adverse
      // trigger order: the private child write happens before the parent CDC
      // row is staged. private_audit is deliberately not registered.
      sql.exec(
        `CREATE TRIGGER xorezYcdcZhidden AFTER INSERT ON item BEGIN
           INSERT INTO xorezYaudit (item_id, note) VALUES (NEW.id, 'private');
           INSERT INTO azeroXprivate (item_id, note) VALUES (NEW.id, 'private');
         END`
      )

      const txID = `tx-trigger-${mode}`
      sql.exec(TX_MANIFEST_DDL)
      sql.exec(
        `INSERT INTO "${TX_MANIFEST_TABLE}" (tx_id, owner, original, snapshot) VALUES (?, ?, ?, ?)`,
        txID,
        'orez-embed',
        'item',
        ''
      )
      zero.executeSQL(
        "INSERT INTO item VALUES (2, 'rolled back') RETURNING *",
        [],
        {
          physicalTableName: 'item',
          tableName: 'public.item',
          operation: 'INSERT',
          rowColumns: ['id', 'body'],
        },
        txID
      )
      expect(sql.exec('SELECT count(*) AS c FROM item').one()).toEqual({ c: 2 })
      expect(sql.exec('SELECT count(*) AS c FROM xorezYaudit').one()).toEqual({ c: 2 })
      expect(sql.exec('SELECT count(*) AS c FROM azeroXprivate').one()).toEqual({ c: 2 })
      expect(
        sql
          .exec(
            `SELECT undoable FROM _zero_pending_changes WHERE transaction_id = ?`,
            txID
          )
          .toArray()
      ).toEqual([{ undoable: 0 }])

      await zero.atomically(() => {
        const beforeRollback = (id: string) => zero.rollbackPendingTrackedChanges(id)
        if (mode === 'rollback') {
          beforeRollback(txID)
          rollbackTxJournal(zero.sql, txID)
        } else {
          expect(recoverTxJournal(zero.sql, 'orez-embed', beforeRollback)).toEqual([txID])
        }
        zero.deletePendingTrackedChanges(txID)
      })

      expect(sql.exec('SELECT * FROM item ORDER BY id').toArray()).toEqual([
        { id: 1, body: 'kept' },
      ])
      expect(sql.exec('SELECT * FROM xorezYaudit ORDER BY id').toArray()).toEqual([
        { id: 1, item_id: 1, note: 'kept' },
      ])
      expect(sql.exec('SELECT * FROM azeroXprivate ORDER BY id').toArray()).toEqual([
        { id: 1, item_id: 1, note: 'kept' },
      ])
    }
  )
})

describe('ZeroDO implicit foreign-key side effects', () => {
  it.each(['rollback', 'recovery'] as const)(
    'restores a cascading WITHOUT ROWID key update during %s',
    async (mode) => {
      const { sql, zero } = await createWorkerCore()
      const { TX_MANIFEST_DDL, TX_MANIFEST_TABLE, recoverTxJournal, rollbackTxJournal } =
        await import('./tx-journal.js')
      sql.exec('PRAGMA foreign_keys = ON')
      sql.exec('CREATE TABLE parent (id INTEGER PRIMARY KEY)')
      sql.exec(
        'CREATE TABLE child (' +
          'parent_id INTEGER PRIMARY KEY REFERENCES parent(id) ON UPDATE CASCADE' +
          ') WITHOUT ROWID'
      )
      sql.exec('INSERT INTO parent VALUES (1)')
      sql.exec('INSERT INTO child VALUES (1)')
      zero.cdc.syncTables([
        { physicalTableName: 'parent', tableName: 'public.parent' },
        { physicalTableName: 'child', tableName: 'public.child' },
      ])

      const txID = `tx-cascade-${mode}`
      sql.exec(TX_MANIFEST_DDL)
      sql.exec(
        `INSERT INTO "${TX_MANIFEST_TABLE}" (tx_id, owner, original, snapshot) VALUES (?, ?, ?, ?)`,
        txID,
        'orez-embed',
        'parent',
        ''
      )
      zero.executeSQL(
        'UPDATE parent SET id = 2 RETURNING *',
        [],
        {
          physicalTableName: 'parent',
          tableName: 'public.parent',
          operation: 'UPDATE',
          rowColumns: ['id'],
        },
        txID
      )
      expect(sql.exec('SELECT * FROM parent').toArray()).toEqual([{ id: 2 }])
      expect(sql.exec('SELECT * FROM child').toArray()).toEqual([{ parent_id: 2 }])
      expect(
        sql
          .exec(
            `SELECT undoable FROM _zero_pending_changes WHERE transaction_id = ? ORDER BY id`,
            txID
          )
          .toArray()
      ).toEqual([{ undoable: 0 }, { undoable: 0 }])

      await zero.atomically(() => {
        const beforeRollback = (id: string) => zero.rollbackPendingTrackedChanges(id)
        if (mode === 'rollback') {
          beforeRollback(txID)
          rollbackTxJournal(zero.sql, txID)
        } else {
          expect(recoverTxJournal(zero.sql, 'orez-embed', beforeRollback)).toEqual([txID])
        }
        zero.deletePendingTrackedChanges(txID)
      })
      expect(sql.exec('SELECT * FROM parent').toArray()).toEqual([{ id: 1 }])
      expect(sql.exec('SELECT * FROM child').toArray()).toEqual([{ parent_id: 1 }])
    }
  )
})

describe('ZeroDO snapshot feed timestamp fidelity', () => {
  // The sync-cf-host rust engine ingests /snapshot for initial sync of any
  // namespace whose change log has been pruned below the client cursor (every
  // prod project namespace with history). pg timestamp/timestamptz columns are
  // declared `number` in the zero schema but the DO stores them as postgres
  // timestamp TEXT, so the snapshot must forward that text verbatim for the
  // engine to decode it — never coerce it with Number() into NaN/null.
  async function snapshotFor(rows: Array<Record<string, unknown>>) {
    const { sql, zero } = await createWorkerCore()
    zero.tableSchemas = new Map()
    zero.schemaTables = new Set<string>()
    zero.ensureSchemaTables({
      tables: {
        message: {
          primaryKey: ['id'],
          columns: {
            id: { type: 'string' },
            createdAt: { type: 'number' },
          },
        },
      },
    })
    for (const row of rows) {
      sql.exec(
        'INSERT INTO "message" ("id", "createdAt") VALUES (?, ?)',
        row.id,
        row.createdAt
      )
    }
    expect(sql.exec('SELECT * FROM "message" ORDER BY "id"').toArray()).toEqual(rows)
    expect(
      sql
        .exec('SELECT * FROM "message" ORDER BY "id"')
        .toArray()
        .map((row) => zero.normalizeRow('message', row))
    ).toEqual(rows)
    const response = await zero.handleSnapshot()
    const body = (await response.json()) as {
      tables: Record<string, Array<Record<string, unknown>>>
    }
    return body.tables.message
  }

  it('forwards postgres timestamp text (client epoch-ms form) instead of nulling it', async () => {
    const rows = await snapshotFor([
      { id: 'm1', createdAt: '2026-07-11 13:34:46.000+00' },
    ])
    expect(rows).toEqual([{ id: 'm1', createdAt: '2026-07-11 13:34:46.000+00' }])
  })

  it('forwards CURRENT_TIMESTAMP text (server default form) instead of nulling it', async () => {
    const rows = await snapshotFor([{ id: 'm2', createdAt: '2026-07-11 13:34:46' }])
    expect(rows).toEqual([{ id: 'm2', createdAt: '2026-07-11 13:34:46' }])
  })

  it('still coerces a genuine numeric timestamp to a number', async () => {
    const rows = await snapshotFor([{ id: 'm3', createdAt: 1_783_776_886_000 }])
    expect(rows).toEqual([{ id: 'm3', createdAt: 1_783_776_886_000 }])
  })
})

describe('ZeroDO legacy snapshot feed', () => {
  it('fails closed when a table read errors', async () => {
    const { sql, zero } = await createWorkerCore()
    zero.ensureSchemaTables({
      tables: {
        item: {
          primaryKey: ['id'],
          columns: { id: { type: 'string' } },
        },
      },
    })
    const exec = sql.exec
    sql.exec = (statement: string, ...params: unknown[]) => {
      if (statement === 'SELECT * FROM "item"')
        throw new Error('injected legacy snapshot read failure')
      return exec(statement, ...params)
    }

    const response = await zero.fetch(new Request('http://do/snapshot'))

    expect(response.status).toBe(500)
    expect(await response.json()).toEqual({
      error: 'injected legacy snapshot read failure',
    })
  })
})

describe('ZeroDO changes feed', () => {
  it('bounds the SQL read with the requested limit and preserves the response shape', async () => {
    const { sql, zero } = await createWorkerCore()
    for (const id of ['a', 'b', 'c']) {
      zero.appendTrackedChange({
        tableName: 'item',
        op: 'INSERT',
        rowData: { id },
        oldData: null,
      })
    }
    const changeReads: Array<{ statement: string; params: unknown[] }> = []
    const exec = sql.exec
    sql.exec = (statement: string, ...params: unknown[]) => {
      if (
        statement.startsWith(
          'SELECT watermark, table_name, op, row_data, old_data FROM _zero_changes'
        )
      ) {
        changeReads.push({ statement, params })
      }
      return exec(statement, ...params)
    }

    const response = await zero.fetch(
      new Request('http://do/changes?watermark=0&limit=2')
    )

    expect(response.status).toBe(200)
    expect(await response.json()).toEqual({
      watermark: 3,
      changes: [
        {
          watermark: 1,
          tableName: 'item',
          op: 'INSERT',
          rowData: { id: 'a' },
          oldData: null,
        },
        {
          watermark: 2,
          tableName: 'item',
          op: 'INSERT',
          rowData: { id: 'b' },
          oldData: null,
        },
      ],
    })
    expect(changeReads).toEqual([
      {
        statement:
          'SELECT watermark, table_name, op, row_data, old_data FROM _zero_changes WHERE watermark > ? ORDER BY watermark LIMIT ?',
        params: [0, 2],
      },
    ])
  })
})

describe('ZeroDO paged snapshot feed', () => {
  async function page(
    zero: any,
    table: string,
    limit: number,
    cursor?: string
  ): Promise<{
    status: number
    body: {
      watermark?: number
      rows?: Array<Record<string, unknown>>
      nextCursor?: string | null
      error?: string
    }
  }> {
    const url = new URL('http://do/snapshot')
    url.searchParams.set('table', table)
    url.searchParams.set('limit', String(limit))
    if (cursor !== undefined) url.searchParams.set('cursor', cursor)
    const response = await zero.fetch(new Request(url))
    return { status: response.status, body: await response.json() }
  }

  it('returns bounded single-key pages with an opaque resume cursor and current watermark', async () => {
    const { sql, zero } = await createWorkerCore()
    zero.ensureSchemaTables({
      tables: {
        item: {
          primaryKey: ['id'],
          columns: { id: { type: 'string' }, label: { type: 'string' } },
        },
      },
    })
    for (const id of ['e', 'a', 'd', 'b', 'c']) {
      sql.exec('INSERT INTO item (id, label) VALUES (?, ?)', id, `label-${id}`)
    }
    zero.watermarks.ensureTables()
    zero.watermarks.mark(37)

    const legacyResponse = await zero.fetch(new Request('http://do/snapshot'))
    expect(legacyResponse.status).toBe(200)
    expect(await legacyResponse.json()).toMatchObject({
      watermark: 37,
      tables: { item: expect.arrayContaining([{ id: 'a', label: 'label-a' }]) },
    })

    const first = await page(zero, 'item', 2)
    expect(first).toEqual({
      status: 200,
      body: {
        watermark: 37,
        rows: [
          { id: 'a', label: 'label-a' },
          { id: 'b', label: 'label-b' },
        ],
        nextCursor: JSON.stringify(['b']),
      },
    })

    const second = await page(zero, 'item', 2, first.body.nextCursor!)
    expect(second.body.rows).toEqual([
      { id: 'c', label: 'label-c' },
      { id: 'd', label: 'label-d' },
    ])
    expect(second.body.nextCursor).toBe(JSON.stringify(['d']))

    const last = await page(zero, 'item', 2, second.body.nextCursor!)
    expect(last).toEqual({
      status: 200,
      body: {
        watermark: 37,
        rows: [{ id: 'e', label: 'label-e' }],
        nextCursor: null,
      },
    })
  })

  it('uses lexicographic keyset paging for composite primary keys', async () => {
    const { sql, zero } = await createWorkerCore()
    zero.ensureSchemaTables({
      tables: {
        pair: {
          primaryKey: ['group', 'id'],
          columns: {
            group: { type: 'string' },
            id: { type: 'number' },
            value: { type: 'string' },
          },
        },
      },
    })
    for (const [group, id] of [
      ['b', 2],
      ['a', 2],
      ['b', 1],
      ['a', 1],
    ] as const) {
      sql.exec(
        'INSERT INTO pair ("group", id, value) VALUES (?, ?, ?)',
        group,
        id,
        `${group}${id}`
      )
    }

    const first = await page(zero, 'pair', 2)
    expect(first.body.rows).toEqual([
      { group: 'a', id: 1, value: 'a1' },
      { group: 'a', id: 2, value: 'a2' },
    ])
    expect(first.body.nextCursor).toBe(JSON.stringify(['a', 2]))
    const second = await page(zero, 'pair', 2, first.body.nextCursor!)
    expect(second.body.rows).toEqual([
      { group: 'b', id: 1, value: 'b1' },
      { group: 'b', id: 2, value: 'b2' },
    ])
    expect(second.body.nextCursor).toBeNull()
  })

  it('rejects malformed page requests and unknown tables', async () => {
    const { zero } = await createWorkerCore()
    zero.ensureSchemaTables({
      tables: {
        item: {
          primaryKey: ['id'],
          columns: { id: { type: 'string' } },
        },
      },
    })

    const cases = [
      new URL('http://do/snapshot?limit=2'),
      new URL('http://do/snapshot?table=item&limit=0'),
      new URL('http://do/snapshot?table=item&limit=1.5'),
      new URL('http://do/snapshot?table=item&limit=10001'),
      new URL('http://do/snapshot?table=item&limit=2&cursor=not-json'),
      new URL(
        `http://do/snapshot?table=item&limit=2&cursor=${encodeURIComponent(JSON.stringify(['a', 'extra']))}`
      ),
      new URL('http://do/snapshot?table=missing&limit=2'),
    ]
    for (const url of cases) {
      const response = await zero.fetch(new Request(url))
      expect(response.status, url.toString()).toBe(400)
      expect((await response.json()).error, url.toString()).toBeTypeOf('string')
    }
  })

  it('fails closed when the bounded SELECT errors', async () => {
    const { sql, zero } = await createWorkerCore()
    zero.ensureSchemaTables({
      tables: {
        item: {
          primaryKey: ['id'],
          columns: { id: { type: 'string' } },
        },
      },
    })
    const exec = sql.exec
    sql.exec = (statement: string, ...params: unknown[]) => {
      if (statement.startsWith('SELECT * FROM "item"'))
        throw new Error('injected paged snapshot read failure')
      return exec(statement, ...params)
    }

    const response = await zero.fetch(
      new Request('http://do/snapshot?table=item&limit=2')
    )
    expect(response.status).toBe(500)
    expect(await response.json()).toEqual({
      error: 'injected paged snapshot read failure',
    })
  })
})
