// @ts-expect-error - CJS module
import BedrockSqlite from 'bedrock-sqlite'
import { describe, expect, it, vi } from 'vitest'

import { TransactionalCdc } from './cdc.js'
import { DurableWatermarkState } from './watermark.js'

vi.mock('cloudflare:workers', () => ({ DurableObject: class {} }))

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
  zero.pendingChangesSchemaReady = false
  return { ...storage, zero }
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
