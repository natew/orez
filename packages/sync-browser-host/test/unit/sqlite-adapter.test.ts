import { Database } from 'bun:sqlite'
import { expect, test } from 'bun:test'

import {
  assertConsumerSql,
  BedrockDirectSql,
  BedrockSyncDb,
} from '../../src/sqlite-adapter.js'

test('consumer SQL cannot own transactions or numbered parameters', () => {
  expect(() => assertConsumerSql('BEGIN')).toThrow('transaction SQL is host-owned')
  expect(() => assertConsumerSql('  SAVEPOINT nested')).toThrow(
    'transaction SQL is host-owned'
  )
  expect(() => assertConsumerSql('SELECT ?1')).toThrow(
    'numbered parameters are forbidden'
  )
  expect(() =>
    assertConsumerSql('CREATE TRIGGER changed AFTER INSERT ON item BEGIN SELECT 1; END')
  ).not.toThrow()
})

test('BedrockSyncDb reads int64 larger than MAX_SAFE_INTEGER without throwing', () => {
  const db = new Database(':memory:')
  db.exec('CREATE TABLE budget (id TEXT PRIMARY KEY, amountMinor INTEGER)')
  db.exec('INSERT INTO budget VALUES ("b1", 15017516016016000)')

  const syncDb = new BedrockSyncDb(db as unknown as any)
  const rows = syncDb.query('SELECT id, amountMinor FROM budget', [])
  expect(rows).toEqual([
    {
      columns: ['id', 'amountMinor'],
      values: [
        { kind: 'text', value: 'b1' },
        { kind: 'integer', value: '15017516016016000' },
      ],
    },
  ])
  db.close()
})

test('BedrockDirectSql reads and writes int64 values', () => {
  const db = new Database(':memory:')
  db.exec('CREATE TABLE budget (id TEXT PRIMARY KEY, amountMinor INTEGER)')
  const directSql = new BedrockDirectSql(db as unknown as any)
  const result = directSql.exec('INSERT INTO budget VALUES (?, ?)', [
    'b1',
    15017516016016000n,
  ])
  expect(result.changes).toBe(1)
  const rows = directSql.query('SELECT amountMinor FROM budget')
  expect(rows).toEqual([{ amountMinor: 15017516016016000n }])
  db.close()
})
