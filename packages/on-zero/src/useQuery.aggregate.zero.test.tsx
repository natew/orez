// @vitest-environment jsdom

import { createBuilder, createSchema, number, string, table } from '@rocicorp/zero'
import { count, defineAggregates, sum } from 'orez-lite/aggregate'
import { act } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { afterEach, beforeEach, expect, test } from 'vitest'

import { createZeroClient } from './createZeroClient'
import { onMutationError } from './helpers/useMutation'
import { mutations } from './mutations'

import type { MutatorContext } from './types'

declare global {
  // eslint-disable-next-line no-var
  var IS_REACT_ACT_ENVIRONMENT: boolean | undefined
}
globalThis.IS_REACT_ACT_ENVIRONMENT = true

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
const builder = createBuilder(schema)
const aggregates = defineAggregates(schema, {
  categorySpend: {
    source: 'expense',
    target: 'categorySpend',
    mode: 'materialized',
    groupBy: { category: 'category' },
    columns: { expenseCount: count(), spent: sum('amount') },
  },
})
const expenseMutations = mutations(expense, () => true, {
  insert: async (
    context: MutatorContext,
    value: { id: string; category: string; amount: number }
  ) => {
    await context.tx.mutate.expense.insert(value)
  },
})
const categorySpendQuery = (args: { category: string }) =>
  builder.categorySpend.where('category', args.category)
const client = createZeroClient({
  schema,
  models: { expense: { mutate: expenseMutations } },
  groupedQueries: { expense: { categorySpendQuery } },
  aggregates,
  instanceName: 'aggregate-react-view-test',
})

let container: HTMLDivElement
let root: Root
let stopErrorSink: () => void
const silentLogSink = { log() {} }

beforeEach(() => {
  stopErrorSink = onMutationError(() => {})
  container = document.createElement('div')
  root = createRoot(container)
})

afterEach(async () => {
  const activeZero = await client.waitForZero()
  act(() => root.unmount())
  await activeZero.close()
  await new Promise((resolve) => setTimeout(resolve, 0))
  stopErrorSink()
})

test('a mounted useQuery view updates after a custom mutation projects an aggregate', async () => {
  let rows: readonly { category: string; expenseCount: number; spent: number }[] = []
  function Probe() {
    ;[rows] = client.useQuery(categorySpendQuery, { category: 'Food' })
    return null
  }

  await act(async () => {
    root.render(
      <client.ProvideZero
        server="https://on-zero-aggregate.test"
        userID="test-user"
        authData={{}}
        logSink={silentLogSink}
      >
        <Probe />
      </client.ProvideZero>
    )
  })
  await act(async () => {
    const mutate = Reflect.get(client.zero, 'mutate')
    const expenseMutate = Reflect.get(mutate, 'expense')
    const insert = Reflect.get(expenseMutate, 'insert')
    if (typeof insert !== 'function')
      throw new Error('expense insert mutator is unavailable')
    const result = Reflect.apply(insert, expenseMutate, [
      { id: 'expense-1', category: 'Food', amount: 4250 },
    ])
    if (!result || typeof result !== 'object') {
      throw new Error('expense insert did not return a mutation result')
    }
    await Reflect.get(result, 'client')
  })

  expect(rows).toEqual([
    expect.objectContaining({ category: 'Food', expenseCount: 1, spent: 4250 }),
  ])
})
