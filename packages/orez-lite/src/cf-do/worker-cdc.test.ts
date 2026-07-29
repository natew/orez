// @ts-expect-error - CJS module
import BedrockSqlite from 'bedrock-sqlite'
import { describe, expect, it, vi } from 'vitest'

import { TransactionalCdc } from './cdc.js'
import { rollbackTxJournal } from './tx-journal.js'
import { DurableWatermarkState } from './watermark.js'

vi.mock('cloudflare:workers', () => ({
  DurableObject: class {
    constructor(ctx: unknown) {
      this.ctx = ctx
    }
  },
  RpcTarget: class {},
}))

const BetterSqlite3 = BedrockSqlite.Database

function createSqliteStorage() {
  const nativeDb = new BetterSqlite3(':memory:')
  const exec = (sql: string, ...params: unknown[]) => {
    const stmt = nativeDb.prepare(sql)
    let rows: Array<Record<string, unknown>> = []
    let rowsWritten = 0
    if (stmt.reader) {
      rows = stmt.all(...params)
    } else {
      rowsWritten = Number(stmt.run(...params).changes)
    }
    return {
      toArray: () => rows,
      one: () => rows[0],
      columnNames: stmt.reader ? stmt.columns().map((column: any) => column.name) : [],
      rowsWritten,
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
  zero.applicationSqlWriter = null
  zero.applicationSqlReaders = new Set()
  zero.applicationSqlQueue = []
  zero.applicationSqlDidCommit = () => {}
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

  it('rolls back an active application write when its capability is disposed', async () => {
    const { sql, zero } = await createWorkerCore()
    sql.exec('CREATE TABLE private_note (id TEXT PRIMARY KEY, body TEXT)')

    const registration = await zero.applicationSqlSession(
      'application-private-registration'
    )
    await registration.begin()
    await registration.registerTables([
      { table: 'private_note', publicTable: 'private.private_note', publish: false },
    ])
    await registration.commit()
    const session = await zero.applicationSqlSession('application-private-rollback')
    await session.begin()
    await session.exec("INSERT INTO private_note VALUES ('n1', 'discarded')")
    session[Symbol.dispose]()

    expect(sql.exec('SELECT * FROM private_note').toArray()).toEqual([])
    expect(zero.readChangesSince(0)).toEqual([])
  })

  it('defers the application schema snapshot until the session changes schema', async () => {
    const { sql, zero } = await createWorkerCore()
    sql.exec('CREATE TABLE item (id TEXT PRIMARY KEY, body TEXT)')

    const session = await zero.applicationSqlSession('application-schema-only')
    await session.begin()
    await session.query('SELECT * FROM item')
    await session.exec('CREATE TABLE IF NOT EXISTS item (id TEXT PRIMARY KEY, body TEXT)')
    await session.registerTables([{ table: 'item', publicTable: 'public.item' }])

    expect(
      sql
        .exec(
          "SELECT name FROM sqlite_master WHERE type = 'table' AND name = '_orez_tx_schema'"
        )
        .toArray()
    ).toEqual([])

    await session.exec('ALTER TABLE item ADD COLUMN extra TEXT')
    expect(
      sql
        .exec("SELECT name FROM _orez_tx_schema WHERE tx_id = 'application-schema-only'")
        .toArray().length
    ).toBeGreaterThan(0)

    await session.rollback()
    expect(
      sql
        .exec('PRAGMA table_info(item)')
        .toArray()
        .map((column) => column.name)
    ).toEqual(['id', 'body'])
  })

  it('does not rewrite identical application table registrations after a cold start', async () => {
    const { sql, zero } = await createWorkerCore()
    sql.exec('CREATE TABLE item (id TEXT PRIMARY KEY, body TEXT)')

    const initial = await zero.applicationSqlSession('application-register-initial')
    await initial.begin()
    await initial.registerTables([{ table: 'item', publicTable: 'public.item' }])
    await initial.commit()

    // a new CDC instance has persisted registrations but no verified-table
    // cache, matching a durable object cold start.
    zero.cdc = new TransactionalCdc(sql)
    const before = Number(sql.exec('SELECT total_changes() AS value').one()?.value)

    const repeated = await zero.applicationSqlSession('application-register-repeated')
    await repeated.begin()
    await repeated.registerTables([{ table: 'item', publicTable: 'public.item' }])
    await repeated.commit()

    const after = Number(sql.exec('SELECT total_changes() AS value').one()?.value)
    expect(after - before).toBe(0)
  })

  it('commits a read-only application session while the write circuit is tripped', async () => {
    const { sql, zero } = await createWorkerCore()
    sql.exec('CREATE TABLE item (id TEXT PRIMARY KEY)')
    zero.writeBudget = {
      assertOpen() {
        throw new Error('row write budget exceeded')
      },
      recordLogical() {},
    }

    const session = await zero.applicationSqlSession('application-read-only')
    await session.begin()
    await expect(session.query('SELECT id FROM item')).resolves.toEqual([])
    await expect(session.commit()).resolves.toBeUndefined()
  })

  it('recovers an interrupted write whose foreign-key parent table is missing', async () => {
    const { sql, nativeDb, zero } = await createWorkerCore()
    // workerd always runs durable-object SQLite with foreign_keys on
    sql.exec('PRAGMA foreign_keys = ON')
    sql.exec('CREATE TABLE parent (id TEXT PRIMARY KEY)')
    sql.exec(
      'CREATE TABLE child (id TEXT PRIMARY KEY, ' +
        'parentId TEXT REFERENCES parent(id) ON DELETE CASCADE)'
    )
    sql.exec("INSERT INTO parent VALUES ('p1')")
    const registration = await zero.applicationSqlSession('application-fk-registration')
    await registration.begin()
    await registration.registerTables([{ table: 'child', publicTable: 'public.child' }])
    await registration.commit()
    const session = await zero.applicationSqlSession('application-fk-interrupted')
    await session.begin()
    await session.exec("INSERT INTO child VALUES ('c1', 'p1')")

    // the 2026-07-23 incident shape: by the time recovery runs, the parent
    // table is gone (dropped under a foreign_keys toggle mid-rebuild), so with
    // enforcement on EVERY statement on child fails at compile.
    sql.exec('PRAGMA foreign_keys = OFF')
    sql.exec('DROP TABLE parent')
    sql.exec('PRAGMA foreign_keys = ON')
    expect(() => sql.exec("DELETE FROM child WHERE id = 'c1'")).toThrow(/no such table/)

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
          get: async () => undefined,
          transaction: async <T>(work: () => T) => transaction(work),
          transactionSync: transaction,
        },
        blockConcurrencyWhile(work: () => Promise<void>) {
          recovery = work()
        },
      } as any,
      {} as any
    )
    await expect(recovery).resolves.toBeUndefined()

    expect(sql.exec('SELECT * FROM child').toArray()).toEqual([])
    expect(sql.exec('SELECT count(*) AS c FROM _zero_pending_changes').one()).toEqual({
      c: 0,
    })
    expect(sql.exec('PRAGMA foreign_keys').one()).toEqual({ foreign_keys: 1 })
  })

  it('recovers an interrupted application session before restoring a sticky write trip', async () => {
    const { sql, nativeDb, zero } = await createWorkerCore()
    sql.exec('CREATE TABLE private_note (id TEXT PRIMARY KEY, body TEXT)')
    const registration = await zero.applicationSqlSession(
      'application-restart-registration'
    )
    await registration.begin()
    await registration.registerTables([
      { table: 'private_note', publicTable: 'private.private_note', publish: false },
    ])
    await registration.commit()
    const session = await zero.applicationSqlSession('application-restart')
    await session.begin()
    await session.exec("INSERT INTO private_note VALUES ('n1', 'interrupted')")

    const { ZeroDO } = await import('./worker.js')
    let recovery: Promise<void> | undefined
    let tripDeleted = false
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
    const recreated = new ZeroDO(
      {
        storage: {
          sql,
          get: async (key: string) =>
            key === '_orez_write_budget_tripped_at'
              ? {
                  at: 1_000,
                  windowRows: 900,
                  budget: 300,
                  windowMs: 300_000,
                  statement: {
                    sql: 'UPDATE private_note SET body = ? WHERE id = ?',
                    rowsWritten: 22,
                  },
                }
              : undefined,
          delete: async (key: string) => {
            if (key === '_orez_write_budget_tripped_at') tripDeleted = true
          },
          transaction: async <T>(work: () => T) => transaction(work),
          transactionSync: transaction,
        },
        blockConcurrencyWhile(work: () => Promise<void>) {
          recovery = work()
        },
      } as any,
      {
        OREZ_DO_WRITE_BUDGET_ROWS: '300',
        OREZ_DO_WRITE_BUDGET_ADMIN_TOKEN: 'reopen-token',
      } as any
    )
    await expect(recovery).resolves.toBeUndefined()

    expect(sql.exec('SELECT * FROM private_note').toArray()).toEqual([])
    const status = await recreated.fetch(new Request('http://do/_orez/write-budget'))
    expect(await status.json()).toMatchObject({
      tripped: true,
      trippedWindowRows: 900,
      trippedStatement: {
        sql: 'UPDATE private_note SET body = ? WHERE id = ?',
        rowsWritten: 22,
      },
    })
    const reopened = await recreated.fetch(
      new Request('http://do/_orez/write-budget/reopen', {
        method: 'POST',
        headers: { 'x-orez-admin-token': 'reopen-token' },
      })
    )
    expect(reopened.status).toBe(200)
    expect(await reopened.json()).toMatchObject({ ok: true, tripped: false })
    expect(tripDeleted).toBe(true)
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

    // The transaction owner marked the table row-journaled before asking whether it
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
        // Was `undoable: 0`, when any snapshot in the statement marked every
        // captured row snapshot-owned. That was an implementation detail, not
        // the behaviour: `item` is captured, so row undo can restore it, and
        // only the two unregistered private targets ever needed a copy. The
        // behaviour is still asserted below, where all three tables come back.
      ).toEqual([{ undoable: 1 }])

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
        // Was `[0, 0]`, when both tables were snapshot-owned. `parent`
        // flipping to 1 is the change: it is captured, so row undo restores it
        // and it no longer needs a copy. `child` stays 0 on purpose, and that
        // is the load-bearing half -- it is reached by ON UPDATE CASCADE, and
        // rollback suspends TRIGGERS but cannot suspend a foreign key, so
        // undoing `parent` re-fires the action against `child`, whose own undo
        // then matches nothing. The behaviour both values encode is asserted
        // below, where parent and child are each back at 1.
      ).toEqual([{ undoable: 0 }, { undoable: 1 }])

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

  // Side-effect discovery follows ON DELETE actions only for a delete, so this
  // is the control that the narrowing did not take the cascade snapshot away
  // from the operation that actually fires it. The cascaded child rows carry no
  // before-image of their own; only the table snapshot brings them back.
  it.each(['rollback', 'recovery'] as const)(
    'restores rows erased by ON DELETE CASCADE and ON DELETE SET NULL during %s',
    async (mode) => {
      const { sql, zero } = await createWorkerCore()
      const { TX_MANIFEST_DDL, TX_MANIFEST_TABLE, recoverTxJournal, rollbackTxJournal } =
        await import('./tx-journal.js')
      sql.exec('PRAGMA foreign_keys = ON')
      sql.exec('CREATE TABLE agent (id INTEGER PRIMARY KEY, claim INTEGER)')
      sql.exec(
        'CREATE TABLE agentEvent (' +
          'id INTEGER PRIMARY KEY, ' +
          'agent_id INTEGER REFERENCES agent(id) ON DELETE CASCADE, body TEXT)'
      )
      sql.exec(
        'CREATE TABLE agentPin (' +
          'id INTEGER PRIMARY KEY, ' +
          'agent_id INTEGER REFERENCES agent(id) ON DELETE SET NULL, body TEXT)'
      )
      sql.exec('INSERT INTO agent VALUES (1, 100), (2, 200)')
      sql.exec("INSERT INTO agentEvent VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 2, 'other')")
      sql.exec("INSERT INTO agentPin VALUES (1, 1, 'pinned'), (2, 2, 'other')")
      zero.cdc.syncTables([{ physicalTableName: 'agent', tableName: 'public.agent' }])

      const txID = `tx-delete-cascade-${mode}`
      sql.exec(TX_MANIFEST_DDL)
      sql.exec(
        `INSERT INTO "${TX_MANIFEST_TABLE}" (tx_id, owner, original, snapshot) VALUES (?, ?, ?, ?)`,
        txID,
        'orez-embed',
        'agent',
        ''
      )
      zero.executeSQL(
        'DELETE FROM agent WHERE id = 1 RETURNING *',
        [],
        {
          physicalTableName: 'agent',
          tableName: 'public.agent',
          operation: 'DELETE',
          rowColumns: ['id', 'claim'],
        },
        txID
      )
      expect(sql.exec('SELECT id FROM agent ORDER BY id').toArray()).toEqual([{ id: 2 }])
      expect(sql.exec('SELECT id FROM agentEvent ORDER BY id').toArray()).toEqual([
        { id: 3 },
      ])
      expect(sql.exec('SELECT id, agent_id FROM agentPin ORDER BY id').toArray()).toEqual(
        [
          { id: 1, agent_id: null },
          { id: 2, agent_id: 2 },
        ]
      )

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

      expect(sql.exec('SELECT * FROM agent ORDER BY id').toArray()).toEqual([
        { id: 1, claim: 100 },
        { id: 2, claim: 200 },
      ])
      expect(sql.exec('SELECT * FROM agentEvent ORDER BY id').toArray()).toEqual([
        { id: 1, agent_id: 1, body: 'a' },
        { id: 2, agent_id: 1, body: 'b' },
        { id: 3, agent_id: 2, body: 'other' },
      ])
      expect(sql.exec('SELECT * FROM agentPin ORDER BY id').toArray()).toEqual([
        { id: 1, agent_id: 1, body: 'pinned' },
        { id: 2, agent_id: 2, body: 'other' },
      ])
    }
  )

  // The heartbeat shape that tripped soot's write budget: renewing one column
  // on the parent must not copy the child table it can only cascade into on a
  // delete, and the parent's own row still has to roll back.
  it('renews a parent column without snapshotting its ON DELETE CASCADE child', async () => {
    const { sql, zero } = await createWorkerCore()
    const { TX_MANIFEST_DDL, TX_MANIFEST_TABLE, rollbackTxJournal } =
      await import('./tx-journal.js')
    sql.exec('PRAGMA foreign_keys = ON')
    sql.exec('CREATE TABLE agent (id INTEGER PRIMARY KEY, claim INTEGER)')
    sql.exec(
      'CREATE TABLE agentEvent (' +
        'id INTEGER PRIMARY KEY, ' +
        'agent_id INTEGER REFERENCES agent(id) ON DELETE CASCADE, body TEXT)'
    )
    sql.exec('INSERT INTO agent VALUES (1, 100)')
    for (let id = 1; id <= 50; id++) {
      sql.exec('INSERT INTO agentEvent VALUES (?, 1, ?)', id, `event-${id}`)
    }
    zero.cdc.syncTables([{ physicalTableName: 'agent', tableName: 'public.agent' }])

    const txID = 'tx-claim-renewal'
    sql.exec(TX_MANIFEST_DDL)
    sql.exec(
      `INSERT INTO "${TX_MANIFEST_TABLE}" (tx_id, owner, original, snapshot) VALUES (?, ?, ?, ?)`,
      txID,
      'orez-embed',
      'agent',
      ''
    )
    zero.executeSQL(
      'UPDATE agent SET claim = 999 WHERE id = 1 RETURNING *',
      [],
      {
        physicalTableName: 'agent',
        tableName: 'public.agent',
        operation: 'UPDATE',
        rowColumns: ['id', 'claim'],
      },
      txID
    )

    const snapshots = sql
      .exec(`SELECT original, snapshot FROM "${TX_MANIFEST_TABLE}" WHERE tx_id = ?`, txID)
      .toArray()
    expect(snapshots.map((row) => String(row.original))).toEqual(['agent'])
    expect(String(snapshots[0].snapshot)).toBe('')
    expect(
      sql
        .exec("SELECT name FROM sqlite_master WHERE name GLOB '_orez_tx_undo_*'")
        .toArray()
    ).toEqual([])

    // The parent row rolls back from its own before-image instead.
    expect(
      sql
        .exec('SELECT undoable FROM _zero_pending_changes WHERE transaction_id = ?', txID)
        .toArray()
    ).toEqual([{ undoable: 1 }])
    await zero.atomically(() => {
      zero.rollbackPendingTrackedChanges(txID)
      rollbackTxJournal(zero.sql, txID)
      zero.deletePendingTrackedChanges(txID)
    })
    expect(sql.exec('SELECT * FROM agent').toArray()).toEqual([{ id: 1, claim: 100 }])
    expect(sql.exec('SELECT count(*) AS c FROM agentEvent').one()).toEqual({ c: 50 })
  })
})

describe('ZeroDO write budget stickiness', () => {
  // The circuit is only sticky if the trip reaches durable storage. It fires
  // during cursor consumption inside ctx.storage.transaction(), so a put made
  // in that scope is rolled back with the write. The HTTP handlers persist it
  // from their 429 sites, but the application SQL RPC surface has no response
  // to hang that on and it never crossed one — so a namespace that tripped on
  // soot's real write path came back OPEN on the next eviction, and the
  // 300,000-row circuit silently stopped being a circuit.
  function trippableWorker(core: { zero: any }) {
    const puts: Array<{ key: string; value: unknown }> = []
    const deferred: Array<() => Promise<void>> = []
    core.zero.ctx.storage.put = async (key: string, value: unknown) => {
      puts.push({ key, value })
    }
    // Stand in for workerd's scheduling: hold the callback so the test can
    // prove the put is NOT issued inside the transaction that is aborting.
    core.zero.ctx.blockConcurrencyWhile = (work: () => Promise<void>) => {
      deferred.push(work)
      return Promise.resolve()
    }
    return { puts, deferred }
  }

  it('defers the sticky trip out of the aborting transaction and keeps its count', async () => {
    const core = await createWorkerCore()
    const { RollingRowWriteBudget, WriteBudgetExceededError } =
      await import('../do-sql-tracking.js')
    const { puts, deferred } = trippableWorker(core)
    core.zero.writeBudget = new RollingRowWriteBudget({
      budgetRows: 3,
      windowMs: 300_000,
      now: () => 1_000,
    })

    expect(() =>
      core.zero.recordWriteBudgetRows(9, {
        sql: "UPDATE message SET text = 'private-value' WHERE id = 42",
        rowsWritten: 9,
      })
    ).toThrow(WriteBudgetExceededError)
    // Nothing yet: a put issued here would be rolled back with the write.
    expect(puts).toEqual([])
    expect(deferred).toHaveLength(1)

    await deferred[0]!()
    expect(puts).toEqual([
      {
        key: '_orez_write_budget_tripped_at',
        value: {
          at: 1_000,
          windowRows: 9,
          budget: 3,
          windowMs: 300_000,
          statement: {
            sql: 'UPDATE message SET text = ? WHERE id = ?',
            rowsWritten: 9,
          },
        },
      },
    ])
  })

  it('persists nothing while the circuit is open', async () => {
    const core = await createWorkerCore()
    const { RollingRowWriteBudget } = await import('../do-sql-tracking.js')
    const { puts, deferred } = trippableWorker(core)
    core.zero.writeBudget = new RollingRowWriteBudget({
      budgetRows: 100,
      windowMs: 300_000,
      now: () => 1_000,
    })
    core.zero.recordWriteBudgetRows(9)
    expect(deferred).toEqual([])
    expect(puts).toEqual([])
  })
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

describe('ZeroDO change feed type fidelity', () => {
  it('decodes schema JSON columns from trigger-captured storage text', async () => {
    const { zero } = await createWorkerCore()
    zero.ensureSchemaTables({
      tables: {
        item: {
          primaryKey: ['id'],
          columns: {
            id: { type: 'string' },
            meta: { type: 'json' },
          },
        },
      },
    })
    zero.cdc.syncTables([{ physicalTableName: 'item', tableName: 'public.item' }])

    zero.executeSQL(
      'INSERT INTO item (id, meta) VALUES (?, ?) RETURNING *',
      ['one', JSON.stringify({ tags: ['alpha', 2, true] })],
      {
        physicalTableName: 'item',
        tableName: 'public.item',
        operation: 'INSERT',
        rowColumns: ['id', 'meta'],
      }
    )

    const response = await zero.fetch(new Request('http://do/changes?watermark=0'))
    expect(response.status).toBe(200)
    await expect(response.json()).resolves.toMatchObject({
      changes: [
        {
          tableName: 'public.item',
          op: 'INSERT',
          rowData: { id: 'one', meta: { tags: ['alpha', 2, true] } },
          oldData: null,
        },
      ],
    })
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

/**
 * A table snapshot is copied at FIRST TOUCH, so it is a faithful
 * pre-transaction image only if nothing in the transaction wrote that table
 * first. These cover the ways that used to break, all of which end with a
 * rolled-back write surviving in committed data.
 */
describe('table snapshots never capture earlier writes from the same transaction', () => {
  const noteTrack = (operation: 'INSERT' | 'DELETE') => ({
    physicalTableName: 'note',
    tableName: 'public.note',
    operation,
    rowColumns: ['id', 'body'],
  })

  async function withCascade() {
    const core = await createWorkerCore()
    core.sql.exec('CREATE TABLE note (id TEXT PRIMARY KEY, body TEXT)')
    // Never registered with the CDC, so its cascade deletes cannot be undone
    // row by row and it is the one table that genuinely needs a snapshot.
    core.sql.exec(
      'CREATE TABLE noteTag (id TEXT PRIMARY KEY, ' +
        'noteId TEXT REFERENCES note(id) ON DELETE CASCADE)'
    )
    core.sql.exec("INSERT INTO note VALUES ('pre', 'committed before the tx')")
    return core
  }

  const noteIds = (sql: { exec: (statement: string) => { toArray(): any[] } }) =>
    sql
      .exec('SELECT id FROM note ORDER BY id')
      .toArray()
      .map((row) => String(row.id))

  const undoTableCount = (sql: { exec: (statement: string) => { toArray(): any[] } }) =>
    sql
      .exec(
        "SELECT name FROM sqlite_master WHERE type = 'table' " +
          "AND name LIKE '\\_orez\\_tx\\_undo\\_%' ESCAPE '\\'"
      )
      .toArray().length

  it('rolls back a clean write followed by a cascade write in one transaction', async () => {
    const { sql, zero } = await withCascade()

    // Statement 1 reaches nothing, so it takes no snapshot and is undone row by
    // row. Statement 2 reaches the uncoverable cascade child and does snapshot.
    // The child's snapshot is fine; `note`'s would have carried statement 1.
    zero.executeSQL(
      "INSERT INTO note VALUES ('new', 'written in tx') RETURNING *",
      [],
      noteTrack('INSERT'),
      'tx-mixed'
    )
    expect(noteIds(sql)).toEqual(['new', 'pre'])
    zero.executeSQL(
      "DELETE FROM note WHERE id = 'pre' RETURNING *",
      [],
      noteTrack('DELETE'),
      'tx-mixed'
    )

    zero.rollbackPendingTrackedChanges('tx-mixed')
    zero.deletePendingTrackedChanges('tx-mixed')
    rollbackTxJournal(sql, 'tx-mixed')

    expect(noteIds(sql)).toEqual(['pre'])
  })

  it('rolls back when an unparseable trigger forces the all-tables snapshot', async () => {
    const { sql, zero } = await withCascade()
    // The classifier cannot read a target out of this body, so the statement
    // falls to `mustSnapshotAll`. That path copies every table it can see,
    // which is how it used to reach `note` after statement 1 had written it.
    sql.exec("CREATE TRIGGER note_opaque AFTER DELETE ON note BEGIN SELECT 'INSERT'; END")

    zero.executeSQL(
      "INSERT INTO note VALUES ('new', 'written in tx') RETURNING *",
      [],
      noteTrack('INSERT'),
      'tx-all'
    )
    zero.executeSQL(
      "DELETE FROM note WHERE id = 'pre' RETURNING *",
      [],
      noteTrack('DELETE'),
      'tx-all'
    )

    zero.rollbackPendingTrackedChanges('tx-all')
    zero.deletePendingTrackedChanges('tx-all')
    rollbackTxJournal(sql, 'tx-all')

    expect(noteIds(sql)).toEqual(['pre'])
  })

  it('still snapshots a side-effect target the CDC cannot undo', async () => {
    const { sql, zero } = await withCascade()
    sql.exec("INSERT INTO noteTag VALUES ('t1', 'pre')")

    zero.executeSQL(
      "DELETE FROM note WHERE id = 'pre' RETURNING *",
      [],
      noteTrack('DELETE'),
      'tx-child'
    )
    // The cascade emptied it, and only a snapshot can bring it back.
    expect(sql.exec('SELECT id FROM noteTag').toArray()).toEqual([])
    expect(undoTableCount(sql)).toBeGreaterThan(0)

    zero.rollbackPendingTrackedChanges('tx-child')
    zero.deletePendingTrackedChanges('tx-child')
    rollbackTxJournal(sql, 'tx-child')

    expect(sql.exec('SELECT id FROM noteTag').toArray()).toEqual([{ id: 't1' }])
    expect(noteIds(sql)).toEqual(['pre'])
  })

  it('copies nothing for a synced write whose only side effect is the zsync journal', async () => {
    const { sql, zero } = await createWorkerCore()
    sql.exec('CREATE TABLE note (id TEXT PRIMARY KEY, body TEXT)')
    sql.exec(
      'CREATE TABLE _zsync_changes (watermark INTEGER PRIMARY KEY AUTOINCREMENT, ' +
        '"tableName" TEXT NOT NULL, "op" TEXT NOT NULL, "pk" TEXT)'
    )
    for (let index = 0; index < 200; index++) {
      sql.exec(
        `INSERT INTO _zsync_changes ("tableName", "op", "pk") VALUES ('note', 'row', '{}')`
      )
    }
    // the `_zsync_tr_*` trigger shape installed on every synced table.
    sql.exec(
      'CREATE TRIGGER "_zsync_tr_note_i" AFTER INSERT ON "note" BEGIN ' +
        `INSERT INTO _zsync_changes ("tableName", "op", "pk") VALUES ('note', 'row', '{}'); END`
    )

    zero.executeSQL(
      "INSERT INTO note VALUES ('new', 'written') RETURNING *",
      [],
      noteTrack('INSERT'),
      'tx-zsync'
    )

    // Before this fix the journal AND the written table were both copied, which
    // is what tripped three production apps' 300k-row write budgets.
    expect(undoTableCount(sql)).toBe(0)

    zero.rollbackPendingTrackedChanges('tx-zsync')
    zero.deletePendingTrackedChanges('tx-zsync')
    rollbackTxJournal(sql, 'tx-zsync')

    expect(noteIds(sql)).toEqual([])
    expect(sql.exec('SELECT COUNT(*) AS n FROM _zsync_changes').one()).toEqual({ n: 200 })
  })
})
