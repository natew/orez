import { Database } from 'bun:sqlite'
import { describe, expect, test } from 'bun:test'

import { createQueryCompiler } from 'orez-sync-cf-host/query-compiler'
import { executeTransactionQueryPlan } from 'orez-sync-cf-host/transaction-query'

const schema = {
  tables: {
    account: {
      name: 'account',
      serverName: 'accounts',
      columns: {
        id: { type: 'string' },
        balance: { type: 'number' },
      },
      primaryKey: ['id'],
    },
    entry: {
      name: 'entry',
      serverName: 'ledger',
      columns: {
        id: { type: 'number' },
        accountId: { type: 'string', serverName: 'account_id' },
        amount: { type: 'number' },
        note: { type: 'string' },
      },
      primaryKey: ['id'],
    },
  },
}

const ast = {
  table: 'account',
  where: {
    type: 'simple',
    op: '=',
    left: { type: 'column', name: 'id' },
    right: { type: 'literal', value: 'primary' },
  },
  related: [
    {
      correlation: { parentField: ['id'], childField: ['accountId'] },
      subquery: {
        table: 'entry',
        alias: 'entries',
        orderBy: [['id', 'asc']],
      },
    },
  ],
}

const format = {
  singular: true,
  relationships: { entries: { singular: false, relationships: {} } },
}

function database() {
  const db = new Database(':memory:')
  db.exec(`
    CREATE TABLE accounts (id TEXT PRIMARY KEY, balance REAL NOT NULL);
    CREATE TABLE ledger (
      id INTEGER PRIMARY KEY,
      account_id TEXT NOT NULL,
      amount REAL NOT NULL,
      note TEXT NOT NULL
    );
    INSERT INTO accounts VALUES ('primary', 105);
    INSERT INTO ledger VALUES
      (2, 'primary', 5, 'second'),
      (1, 'primary', 100, 'first');
  `)
  return db
}

describe('standalone query compiler', () => {
  test('explains how to load wasm when Bun has no preload', async () => {
    const child = Bun.spawn(
      [process.execPath, '-e', "await import('orez-sync-cf-host/query-compiler')"],
      {
        cwd: import.meta.dir,
        stdout: 'ignore',
        stderr: 'pipe',
      }
    )
    const [exitCode, stderr] = await Promise.all([
      child.exited,
      new Response(child.stderr).text(),
    ])

    expect(exitCode).not.toBe(0)
    expect(stderr).toContain(
      'see the orez-sync-cf-host README Query compiler runtimes matrix'
    )
  })

  test('compiles and materializes a related query outside a durable object', () => {
    const db = database()
    const compile = createQueryCompiler(schema)
    const plan = compile(ast, format)

    expect(
      executeTransactionQueryPlan(plan, (sql, params) => db.query(sql).all(...params), {
        queryName: 'standaloneAccount',
      })
    ).toEqual({
      id: 'primary',
      balance: 105,
      entries: [
        { id: 1, accountId: 'primary', amount: 100, note: 'first' },
        { id: 2, accountId: 'primary', amount: 5, note: 'second' },
      ],
    })
  })

  test('retains the materialization budget outside a durable object', () => {
    const db = database()
    const plan = createQueryCompiler(schema)(ast, format)

    expect(() =>
      executeTransactionQueryPlan(plan, (sql, params) => db.query(sql).all(...params), {
        queryName: 'standaloneBudget',
        budget: { maxSelects: 1 },
      })
    ).toThrow(
      expect.objectContaining({
        code: 'transaction_query_budget_exceeded',
        query: 'standaloneBudget',
        selects: 2,
      })
    )
  })
})
