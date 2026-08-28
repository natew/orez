import { Zero, createSchema, number, string, table } from '@rocicorp/zero'
import { afterEach, expect, test } from 'vitest'

import { count, defineAggregates, sum, withOptimisticAggregates } from './aggregate.js'

import type { Transaction } from '@rocicorp/zero'

const expense = table('expense')
  .columns({ id: string(), category: string(), amount: number() })
  .primaryKey('id')
const categorySpend = table('categorySpend')
  .columns({ category: string(), expenseCount: number(), spent: number() })
  .primaryKey('category')
const schema = createSchema({
  tables: [expense, categorySpend],
  enableLegacyQueries: true,
})
const aggregates = defineAggregates(schema, {
  categorySpend: {
    source: 'expense',
    target: 'categorySpend',
    mode: 'materialized',
    groupBy: { category: 'category' },
    columns: { expenseCount: count(), spent: sum('amount') },
  },
})

const mutators = {
  expense: {
    insert: async (
      transaction: Transaction<typeof schema>,
      value: { id: string; category: string; amount: number }
    ) => {
      const tx = withOptimisticAggregates(transaction, aggregates)
      await tx.mutate.expense.insert(value)
    },
  },
}

const clients: Array<Zero<typeof schema, typeof mutators>> = []
const silentLogSink = { log() {} }

afterEach(async () => {
  while (clients.length > 0) await clients.pop()?.close()
})

test('a real Zero client transaction updates a mounted materialized aggregate', async () => {
  const zero = new Zero({
    server: 'https://aggregate-zero.test',
    userID: 'test-user',
    schema,
    mutators,
    kvStore: 'mem',
    storageKey: 'aggregate-zero-client-transaction',
    logSink: silentLogSink,
  })
  clients.push(zero)
  const view = zero.query.categorySpend.materialize()

  const mutation = zero.mutate.expense.insert({
    id: 'expense-1',
    category: 'Food',
    amount: 4250,
  })
  await mutation.client

  expect(view.data).toEqual([
    expect.objectContaining({ category: 'Food', expenseCount: 1, spent: 4250 }),
  ])

  const secondMutation = zero.mutate.expense.insert({
    id: 'expense-2',
    category: 'Food',
    amount: 4250,
  })
  await secondMutation.client

  expect(view.data).toEqual([
    expect.objectContaining({ category: 'Food', expenseCount: 2, spent: 8500 }),
  ])
  view.destroy()
})
