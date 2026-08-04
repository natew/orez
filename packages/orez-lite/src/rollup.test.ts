import { createSchema, number, string, table } from '@rocicorp/zero'
// @ts-expect-error - CJS module
import BedrockSqlite from 'bedrock-sqlite'
import { describe, expect, it } from 'vitest'

import { TransactionalCdc } from './cf-do/cdc.js'
import {
  count,
  defineRollups,
  rollupMigrationStatements,
  sum,
  withOptimisticRollups,
} from './rollup.js'

import type { DurableSqlStorage } from './cf-do/watermark.js'

const expense = table('expense')
  .columns({
    id: string(),
    accountId: string(),
    categoryId: string(),
    amount: number(),
    note: string(),
  })
  .primaryKey('id')

const categorySpend = table('categorySpend')
  .columns({
    accountId: string(),
    categoryId: string(),
    expenseCount: number(),
    spent: number(),
  })
  .primaryKey('accountId', 'categoryId')

const post = table('post')
  .columns({
    id: string(),
    title: string(),
    commentCount: number(),
  })
  .primaryKey('id')

const comment = table('comment')
  .columns({
    id: string(),
    postId: string(),
  })
  .primaryKey('id')

const schema = createSchema({
  tables: [expense, categorySpend, post, comment],
})

const rollups = defineRollups(schema, {
  categorySpend: {
    source: 'expense',
    target: 'categorySpend',
    mode: 'materialized',
    groupBy: {
      accountId: 'accountId',
      categoryId: 'categoryId',
    },
    aggregates: {
      expenseCount: count(),
      spent: sum('amount'),
    },
  },
  postCommentCount: {
    source: 'comment',
    target: 'post',
    mode: 'existing',
    groupBy: {
      postId: 'id',
    },
    aggregates: {
      commentCount: count(),
    },
  },
})

function createDatabase() {
  const db = new BedrockSqlite.Database(':memory:')
  db.exec(`
    CREATE TABLE expense (
      id TEXT PRIMARY KEY,
      accountId TEXT NOT NULL,
      categoryId TEXT NOT NULL,
      amount REAL NOT NULL,
      note TEXT NOT NULL
    );
    CREATE TABLE categorySpend (
      accountId TEXT NOT NULL,
      categoryId TEXT NOT NULL,
      expenseCount INTEGER NOT NULL,
      spent REAL NOT NULL,
      PRIMARY KEY (accountId, categoryId)
    );
    CREATE TABLE post (
      id TEXT PRIMARY KEY,
      title TEXT NOT NULL,
      commentCount INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE comment (
      id TEXT PRIMARY KEY,
      postId TEXT NOT NULL
    );
  `)
  return db
}

function rows(
  db: InstanceType<typeof BedrockSqlite.Database>,
  tableName: 'categorySpend' | 'post'
) {
  return db.prepare(`SELECT * FROM "${tableName}" ORDER BY 1, 2`).all()
}

function changes(db: InstanceType<typeof BedrockSqlite.Database>, sql: string) {
  const before = Number(db.prepare('SELECT total_changes() AS value').get().value)
  db.exec(sql)
  const after = Number(db.prepare('SELECT total_changes() AS value').get().value)
  return after - before
}

describe('Orez Lite rollups', () => {
  it('backfills and maintains materialized and existing-row aggregates', () => {
    const db = createDatabase()
    db.exec(`
      INSERT INTO expense VALUES
        ('e1', 'a1', 'food', 12.5, 'lunch'),
        ('e2', 'a1', 'food', 7.5, 'snack'),
        ('e3', 'a1', 'travel', 30, 'train');
      INSERT INTO categorySpend VALUES ('stale', 'stale', 99, 99);
      INSERT INTO post VALUES ('p1', 'First', 88), ('p2', 'Second', 88);
      INSERT INTO comment VALUES ('c1', 'p1'), ('c2', 'p1');
    `)

    for (const statement of rollupMigrationStatements(rollups)) {
      db.exec(statement)
    }

    expect(rows(db, 'categorySpend')).toEqual([
      {
        accountId: 'a1',
        categoryId: 'food',
        expenseCount: 2,
        spent: 20,
      },
      {
        accountId: 'a1',
        categoryId: 'travel',
        expenseCount: 1,
        spent: 30,
      },
    ])
    expect(rows(db, 'post')).toEqual([
      { id: 'p1', title: 'First', commentCount: 2 },
      { id: 'p2', title: 'Second', commentCount: 0 },
    ])

    expect(
      changes(db, `INSERT INTO expense VALUES ('e4', 'a1', 'food', 5, 'coffee')`)
    ).toBe(2)
    expect(changes(db, `UPDATE expense SET note = 'iced coffee' WHERE id = 'e4'`)).toBe(1)
    expect(changes(db, `UPDATE expense SET amount = 8 WHERE id = 'e4'`)).toBe(2)
    expect(changes(db, `UPDATE expense SET amount = 8 WHERE id = 'e4'`)).toBe(1)
    expect(
      changes(db, `UPDATE expense SET categoryId = 'travel', amount = 10 WHERE id = 'e4'`)
    ).toBe(3)
    expect(changes(db, `DELETE FROM expense WHERE id = 'e3'`)).toBe(2)
    expect(
      changes(db, `INSERT INTO expense VALUES ('e5', 'a1', 'other', 1, 'test')`)
    ).toBe(2)
    expect(changes(db, `DELETE FROM expense WHERE id = 'e5'`)).toBe(3)

    expect(rows(db, 'categorySpend')).toEqual([
      {
        accountId: 'a1',
        categoryId: 'food',
        expenseCount: 2,
        spent: 20,
      },
      {
        accountId: 'a1',
        categoryId: 'travel',
        expenseCount: 1,
        spent: 10,
      },
    ])

    expect(changes(db, `INSERT INTO comment VALUES ('c3', 'p2')`)).toBe(2)
    expect(changes(db, `UPDATE comment SET postId = 'p2' WHERE id = 'c1'`)).toBe(3)
    expect(changes(db, `DELETE FROM comment WHERE id = 'c2'`)).toBe(2)
    expect(rows(db, 'post')).toEqual([
      { id: 'p1', title: 'First', commentCount: 0 },
      { id: 'p2', title: 'Second', commentCount: 2 },
    ])
  })

  it('projects the same changes through a client transaction only', async () => {
    const tables: Record<string, Array<Record<string, unknown>>> = {
      expense: [
        {
          id: 'e1',
          accountId: 'a1',
          categoryId: 'food',
          amount: 12.5,
          note: 'lunch',
        },
        {
          id: 'e2',
          accountId: 'a1',
          categoryId: 'food',
          amount: 7.5,
          note: 'snack',
        },
        {
          id: 'e3',
          accountId: 'a1',
          categoryId: 'travel',
          amount: 30,
          note: 'train',
        },
      ],
      categorySpend: [
        {
          accountId: 'a1',
          categoryId: 'food',
          expenseCount: 2,
          spent: 20,
        },
        {
          accountId: 'a1',
          categoryId: 'travel',
          expenseCount: 1,
          spent: 30,
        },
      ],
      post: [
        { id: 'p1', title: 'First', commentCount: 2 },
        { id: 'p2', title: 'Second', commentCount: 0 },
      ],
      comment: [
        { id: 'c1', postId: 'p1' },
        { id: 'c2', postId: 'p1' },
      ],
    }
    const primaryKeys: Record<string, readonly string[]> = {
      expense: ['id'],
      categorySpend: ['accountId', 'categoryId'],
      post: ['id'],
      comment: ['id'],
    }
    const targetWrites: string[] = []
    const mutate = Object.fromEntries(
      Object.keys(tables).map((tableName) => {
        const findIndex = (input: Readonly<Record<string, unknown>>) =>
          tables[tableName]!.findIndex((row) =>
            primaryKeys[tableName]!.every((key) => Object.is(row[key], input[key]))
          )
        const recordWrite = (operation: string) => {
          if (tableName === 'categorySpend' || tableName === 'post') {
            targetWrites.push(`${tableName}.${operation}`)
          }
        }
        return [
          tableName,
          {
            async insert(input: Readonly<Record<string, unknown>>) {
              if (findIndex(input) !== -1) throw new Error('duplicate row')
              tables[tableName]!.push({ ...input })
              recordWrite('insert')
            },
            async upsert(input: Readonly<Record<string, unknown>>) {
              const index = findIndex(input)
              if (index === -1) tables[tableName]!.push({ ...input })
              else tables[tableName]![index] = { ...input }
              recordWrite('upsert')
            },
            async update(input: Readonly<Record<string, unknown>>) {
              const index = findIndex(input)
              if (index !== -1) {
                tables[tableName]![index] = {
                  ...tables[tableName]![index],
                  ...input,
                }
              }
              recordWrite('update')
            },
            async delete(input: Readonly<Record<string, unknown>>) {
              const index = findIndex(input)
              if (index !== -1) tables[tableName]!.splice(index, 1)
              recordWrite('delete')
            },
          },
        ]
      })
    )
    const transaction = {
      location: 'client',
      mutate,
      async run(query: unknown) {
        if (!query || typeof query !== 'object') return undefined
        const ast = Reflect.get(query, 'ast')
        if (!ast || typeof ast !== 'object') return undefined
        const tableName = Reflect.get(ast, 'table')
        const where = Reflect.get(ast, 'where')
        if (typeof tableName !== 'string') return undefined
        const matches = (
          row: Readonly<Record<string, unknown>>,
          condition: unknown
        ): boolean => {
          if (!condition || typeof condition !== 'object') return true
          const type = Reflect.get(condition, 'type')
          if (type === 'and') {
            const conditions = Reflect.get(condition, 'conditions')
            return (
              Array.isArray(conditions) &&
              conditions.every((nested) => matches(row, nested))
            )
          }
          if (type !== 'simple') return false
          const left = Reflect.get(condition, 'left')
          const right = Reflect.get(condition, 'right')
          if (!left || typeof left !== 'object' || !right || typeof right !== 'object') {
            return false
          }
          const column = Reflect.get(left, 'name')
          return (
            typeof column === 'string' &&
            Object.is(row[column], Reflect.get(right, 'value'))
          )
        }
        return tables[tableName]?.find((row) => matches(row, where))
      },
    }
    const tx = withOptimisticRollups(transaction, rollups)

    await tx.mutate.expense.insert({
      id: 'e4',
      accountId: 'a1',
      categoryId: 'food',
      amount: 5,
      note: 'coffee',
    })
    expect(tables.categorySpend).toEqual([
      {
        accountId: 'a1',
        categoryId: 'food',
        expenseCount: 3,
        spent: 25,
      },
      {
        accountId: 'a1',
        categoryId: 'travel',
        expenseCount: 1,
        spent: 30,
      },
    ])

    const beforeUnrelatedUpdate = targetWrites.length
    await tx.mutate.expense.update({ id: 'e4', note: 'iced coffee' })
    expect(targetWrites).toHaveLength(beforeUnrelatedUpdate)

    await tx.mutate.expense.update({ id: 'e4', amount: 8 })
    const beforeSameValueUpdate = targetWrites.length
    await tx.mutate.expense.update({ id: 'e4', amount: 8 })
    expect(targetWrites).toHaveLength(beforeSameValueUpdate)
    await tx.mutate.expense.update({
      id: 'e4',
      categoryId: 'travel',
      amount: 10,
    })
    await tx.mutate.expense.delete({ id: 'e3' })
    expect(tables.categorySpend).toEqual([
      {
        accountId: 'a1',
        categoryId: 'food',
        expenseCount: 2,
        spent: 20,
      },
      {
        accountId: 'a1',
        categoryId: 'travel',
        expenseCount: 1,
        spent: 10,
      },
    ])
    await tx.mutate.expense.delete({ id: 'e4' })
    expect(tables.categorySpend).toEqual([
      {
        accountId: 'a1',
        categoryId: 'food',
        expenseCount: 2,
        spent: 20,
      },
    ])

    await tx.mutate.comment.insert({ id: 'c3', postId: 'p2' })
    await tx.mutate.comment.update({ id: 'c1', postId: 'p2' })
    await tx.mutate.comment.delete({ id: 'c2' })
    expect(tables.post).toEqual([
      { id: 'p1', title: 'First', commentCount: 0 },
      { id: 'p2', title: 'Second', commentCount: 2 },
    ])

    const serverTransaction = { ...transaction, location: 'server' }
    expect(withOptimisticRollups(serverTransaction, rollups)).toBe(serverTransaction)
  })

  it('publishes an authoritative source write and generated rollup change together', () => {
    const db = createDatabase()
    const sql: DurableSqlStorage = {
      exec(sqlText, ...params) {
        const statement = db.prepare(sqlText)
        const resultRows: Array<Record<string, unknown>> = statement.reader
          ? statement.all(...params)
          : (statement.run(...params), [])
        return {
          toArray: () => resultRows,
          one: () => resultRows[0],
          columnNames: statement.reader
            ? statement
                .columns()
                .map((column: object) => String(Reflect.get(column, 'name')))
            : [],
        }
      },
    }
    const cdc = new TransactionalCdc(sql)
    cdc.syncTables([
      {
        physicalTableName: 'expense',
        tableName: 'public.expense',
      },
      {
        physicalTableName: 'categorySpend',
        tableName: 'public.categorySpend',
      },
    ])
    for (const statement of rollupMigrationStatements(rollups)) {
      sql.exec(statement)
    }
    cdc.drain()

    sql.exec(`INSERT INTO expense VALUES ('e1', 'a1', 'food', 12.5, 'lunch')`)

    expect(cdc.drain()).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          tableName: 'public.expense',
          op: 'INSERT',
          rowData: {
            id: 'e1',
            accountId: 'a1',
            categoryId: 'food',
            amount: 12.5,
            note: 'lunch',
          },
        }),
        expect.objectContaining({
          tableName: 'public.categorySpend',
          op: 'INSERT',
          rowData: {
            accountId: 'a1',
            categoryId: 'food',
            expenseCount: 1,
            spent: 12.5,
          },
        }),
      ])
    )
  })
})
