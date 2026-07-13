/**
 * Logical CDC tests run against real SQLite. The failed-statement,
 * transaction, and primary-key-update cases are adapted from Turso's CDC v2
 * integration suite; trigger side effects are Orez-specific because Turso's
 * bytecode CDC intentionally excludes trigger bodies.
 *
 * Upstream: https://github.com/tursodatabase/turso/blob/main/tests/integration/functions/test_cdc.rs
 */

// @ts-expect-error - CJS module
import BedrockSqlite from 'bedrock-sqlite'
import { describe, expect, it } from 'vitest'

import { TransactionalCdc } from './cdc.js'

import type { DurableSqlStorage } from './watermark.js'

const BetterSqlite3 = BedrockSqlite.Database

function createSqliteStorage() {
  const nativeDb = new BetterSqlite3(':memory:')
  const statements: string[] = []
  const exec = (sql: string, ...params: unknown[]) => {
    statements.push(sql)
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
  return { nativeDb, sql: { exec } as DurableSqlStorage, statements }
}

describe('TransactionalCdc', () => {
  it('captures full CRUD images, including both identities of a primary-key update', () => {
    const { sql } = createSqliteStorage()
    sql.exec('CREATE TABLE item (id INTEGER PRIMARY KEY, body TEXT, payload BLOB)')
    const cdc = new TransactionalCdc(sql)
    cdc.syncTables([
      {
        physicalTableName: 'item',
        tableName: 'public.item',
        columns: ['id', 'body', 'payload'],
      },
    ])

    sql.exec(
      "INSERT INTO item VALUES (1, 'one', x'00ff'), (2, 'two', NULL), (3, 'three', NULL)"
    )
    expect(cdc.drain()).toEqual([
      {
        physicalTableName: 'item',
        tableName: 'public.item',
        op: 'INSERT',
        rowData: { id: 1, body: 'one', payload: '\\x00ff' },
        oldData: null,
      },
      {
        physicalTableName: 'item',
        tableName: 'public.item',
        op: 'INSERT',
        rowData: { id: 2, body: 'two', payload: null },
        oldData: null,
      },
      {
        physicalTableName: 'item',
        tableName: 'public.item',
        op: 'INSERT',
        rowData: { id: 3, body: 'three', payload: null },
        oldData: null,
      },
    ])

    sql.exec("UPDATE item SET id = 4, body = 'moved' WHERE id = 1")
    expect(cdc.drain()).toEqual([
      {
        physicalTableName: 'item',
        tableName: 'public.item',
        op: 'UPDATE',
        rowData: { id: 4, body: 'moved', payload: '\\x00ff' },
        oldData: { id: 1, body: 'one', payload: '\\x00ff' },
      },
    ])

    sql.exec('DELETE FROM item WHERE id >= 3')
    expect(cdc.drain().map((change) => [change.op, change.oldData?.id])).toEqual([
      ['DELETE', 3],
      ['DELETE', 4],
    ])
  })

  it('leaves no row or CDC record from a failed multi-row statement', () => {
    const { sql } = createSqliteStorage()
    sql.exec('CREATE TABLE item (id INTEGER PRIMARY KEY, value TEXT UNIQUE)')
    const cdc = new TransactionalCdc(sql)
    cdc.syncTables([{ physicalTableName: 'item', tableName: 'public.item' }])
    sql.exec("INSERT INTO item VALUES (1, 'existing')")
    cdc.drain()

    expect(() => sql.exec("INSERT INTO item VALUES (2, 'ok'), (3, 'existing')")).toThrow()

    expect(sql.exec('SELECT id FROM item ORDER BY id').toArray()).toEqual([{ id: 1 }])
    expect(cdc.drain()).toEqual([])
  })

  it('uses SQLite transaction rollback as the CDC rollback boundary', () => {
    const { nativeDb, sql } = createSqliteStorage()
    sql.exec('CREATE TABLE item (id INTEGER PRIMARY KEY, body TEXT)')
    const cdc = new TransactionalCdc(sql)
    cdc.syncTables([{ physicalTableName: 'item', tableName: 'public.item' }])

    nativeDb.exec('BEGIN')
    sql.exec("INSERT INTO item VALUES (1, 'rolled back')")
    sql.exec("UPDATE item SET body = 'still rolled back' WHERE id = 1")
    nativeDb.exec('ROLLBACK')
    expect(cdc.drain()).toEqual([])

    nativeDb.exec('BEGIN')
    sql.exec("INSERT INTO item VALUES (2, 'committed')")
    nativeDb.exec('COMMIT')
    expect(cdc.drain()).toMatchObject([
      { tableName: 'public.item', op: 'INSERT', rowData: { id: 2 } },
    ])
  })

  it('captures arbitrary business-trigger side effects on another published table', () => {
    const { sql } = createSqliteStorage()
    sql.exec('CREATE TABLE channel (id TEXT PRIMARY KEY, message_count INTEGER NOT NULL)')
    sql.exec('CREATE TABLE message (id TEXT PRIMARY KEY, channel_id TEXT NOT NULL)')
    const cdc = new TransactionalCdc(sql)
    cdc.syncTables([
      { physicalTableName: 'channel', tableName: 'public.channel' },
      { physicalTableName: 'message', tableName: 'public.message' },
    ])
    sql.exec("INSERT INTO channel VALUES ('general', 0)")
    cdc.drain()
    sql.exec(
      `CREATE TRIGGER message_count AFTER INSERT ON message BEGIN
         UPDATE channel SET message_count = message_count + 1 WHERE id = NEW.channel_id;
       END`
    )

    sql.exec("INSERT INTO message VALUES ('m1', 'general')")
    const changes = cdc.drain()

    expect(changes).toHaveLength(2)
    expect(changes).toEqual(
      expect.arrayContaining([
        {
          physicalTableName: 'message',
          tableName: 'public.message',
          op: 'INSERT',
          rowData: { id: 'm1', channel_id: 'general' },
          oldData: null,
        },
        {
          physicalTableName: 'channel',
          tableName: 'public.channel',
          op: 'UPDATE',
          rowData: { id: 'general', message_count: 1 },
          oldData: { id: 'general', message_count: 0 },
        },
      ])
    )
  })

  it('skips no-op updates and removes stale triggers for unpublished tables', () => {
    const { sql } = createSqliteStorage()
    sql.exec('CREATE TABLE kept (id INTEGER PRIMARY KEY, value TEXT)')
    sql.exec('CREATE TABLE removed (id INTEGER PRIMARY KEY, value TEXT)')
    const cdc = new TransactionalCdc(sql)
    cdc.syncTables([
      { physicalTableName: 'kept', tableName: 'public.kept' },
      { physicalTableName: 'removed', tableName: 'public.removed' },
    ])
    sql.exec("INSERT INTO kept VALUES (1, 'same')")
    cdc.drain()

    sql.exec('UPDATE kept SET value = value WHERE id = 1')
    expect(cdc.drain()).toEqual([])

    cdc.syncTables([{ physicalTableName: 'kept', tableName: 'public.kept' }])
    sql.exec("INSERT INTO removed VALUES (1, 'private')")
    sql.exec("UPDATE kept SET value = 'changed' WHERE id = 1")
    expect(cdc.drain()).toMatchObject([
      { tableName: 'public.kept', op: 'UPDATE', rowData: { value: 'changed' } },
    ])
    expect(cdc.drain()).toEqual([])
  })

  it('preserves null columns when wide rows require multiple JSON calls', () => {
    const { sql } = createSqliteStorage()
    const columns = Array.from({ length: 51 }, (_, index) => `value_${index}`)
    sql.exec(
      `CREATE TABLE wide (id INTEGER PRIMARY KEY, ${columns
        .map((column) => `"${column}" TEXT`)
        .join(', ')})`
    )
    const cdc = new TransactionalCdc(sql)
    cdc.syncTables([{ physicalTableName: 'wide', tableName: 'public.wide' }])

    sql.exec('INSERT INTO wide (id, value_50) VALUES (1, NULL)')
    const [change] = cdc.drain()

    expect(change?.rowData).toHaveProperty('value_50', null)
    expect(Object.keys(change?.rowData ?? {})).toHaveLength(52)
  })

  it('does not re-introspect a verified table on every tracked write', () => {
    const { sql, statements } = createSqliteStorage()
    sql.exec('CREATE TABLE item (id INTEGER PRIMARY KEY, body TEXT)')
    const cdc = new TransactionalCdc(sql)
    const registration = {
      physicalTableName: 'item',
      tableName: 'public.item',
    }
    cdc.syncTables([registration])
    const before = statements.length

    expect(cdc.ensureTable(registration)).toBe(true)
    expect(cdc.ensureTable(registration)).toBe(true)

    expect(statements).toHaveLength(before)
  })

  it('rebuilds capture triggers after a table is dropped and recreated', () => {
    const { sql } = createSqliteStorage()
    sql.exec('CREATE TABLE item (id INTEGER PRIMARY KEY, old_value TEXT)')
    const cdc = new TransactionalCdc(sql)
    const registration = {
      physicalTableName: 'item',
      tableName: 'public.item',
    }
    cdc.syncTables([registration])

    sql.exec('DROP TABLE item')
    sql.exec('CREATE TABLE item (id INTEGER PRIMARY KEY, new_value TEXT)')
    cdc.invalidateSchema()
    expect(cdc.ensureTable(registration)).toBe(true)
    sql.exec("INSERT INTO item VALUES (1, 'new')")

    expect(cdc.drain()).toMatchObject([
      {
        physicalTableName: 'item',
        tableName: 'public.item',
        rowData: { id: 1, new_value: 'new' },
      },
    ])
  })

  it('suspends row-shape triggers while a captured column is dropped', () => {
    const { sql } = createSqliteStorage()
    sql.exec('CREATE TABLE item (id INTEGER PRIMARY KEY, removed TEXT, kept TEXT)')
    const cdc = new TransactionalCdc(sql)
    const registration = {
      physicalTableName: 'item',
      tableName: 'public.item',
    }
    cdc.syncTables([registration])

    const ddl = 'ALTER TABLE "item" DROP COLUMN "removed"'
    expect(cdc.capturesSchemaChange(ddl)).toBe(true)
    const suspended = cdc.beginSchemaChange(ddl)
    sql.exec(ddl)
    cdc.finishSchemaChange(suspended)
    sql.exec("INSERT INTO item VALUES (1, 'still here')")

    expect(cdc.drain()).toEqual([
      {
        physicalTableName: 'item',
        tableName: 'public.item',
        op: 'INSERT',
        rowData: { id: 1, kept: 'still here' },
        oldData: null,
      },
    ])
  })

  it('unregisters the old physical identity after a captured table is renamed', () => {
    const { sql } = createSqliteStorage()
    sql.exec('CREATE TABLE task (id INTEGER PRIMARY KEY, body TEXT)')
    const cdc = new TransactionalCdc(sql)
    cdc.syncTables([{ physicalTableName: 'task', tableName: 'public.task' }])

    const ddl = 'ALTER TABLE task RENAME TO pipe'
    const suspended = cdc.beginSchemaChange(ddl)
    sql.exec(ddl)
    cdc.finishSchemaChange(suspended)
    expect(cdc.capturesTable('public.task')).toBe(false)

    cdc.syncTables([{ physicalTableName: 'pipe', tableName: 'public.pipe' }])
    sql.exec("INSERT INTO pipe VALUES (1, 'renamed')")
    expect(cdc.drain()).toMatchObject([
      { tableName: 'public.pipe', op: 'INSERT', rowData: { id: 1, body: 'renamed' } },
    ])
  })
})
