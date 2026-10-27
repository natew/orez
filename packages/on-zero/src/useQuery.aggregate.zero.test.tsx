// @vitest-environment jsdom

import { createBuilder, createSchema, number, string, table } from '@rocicorp/zero'
import { count, defineAggregates, sum } from 'orez-lite/aggregate'
import { createZeroClientTransport, type HttpPullTransport } from 'orez-lite/client'
import { act } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { afterEach, beforeEach, expect, test } from 'vitest'

import { onMutationError } from './helpers/useMutation'
import { createZeroClientWithDirectQueries } from './multi'
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
const client = createZeroClientWithDirectQueries({
  schema,
  models: { expense: { mutate: expenseMutations } },
  groupedQueries: { expense: { categorySpendQuery } },
  aggregates,
  instanceName: 'aggregate-react-view-test',
})

let container: HTMLDivElement
let root: Root
let stopErrorSink: () => void
let installedTransport: HttpPullTransport | undefined
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
  installedTransport?.uninstall()
  installedTransport = undefined
  await new Promise((resolve) => setTimeout(resolve, 0))
  stopErrorSink()
})

test.each(['context', 'direct'] as const)(
  'an errored %s query rematerializes when repaired data reaches the hook',
  async (mode) => {
    let repaired = false
    let hash = ''
    const fetch = async (_input: RequestInfo | URL, init?: RequestInit) => {
      const request = JSON.parse(String(init?.body))
      const puts = (request.queries?.patch ?? []).filter((op: any) => op.op === 'put')
      if (puts[0]) hash = puts[0].hash
      return new Response(
        JSON.stringify({
          cookie: repaired ? 2 : 1,
          lastMutationIDChanges: {},
          rowsPatch: repaired
            ? [
                {
                  op: 'put',
                  tableName: 'categorySpend',
                  value: { category: 'Food', expenseCount: 1, spent: 4250 },
                },
              ]
            : request.cookie === null
              ? [{ op: 'clear' }]
              : [],
          ...(request.queries || repaired
            ? {
                gotQueries: {
                  version: request.queries?.version ?? 1,
                  patch: repaired ? [{ op: 'put', hash }] : [],
                },
              }
            : {}),
          queryErrors: repaired
            ? []
            : puts.map((op: any) => ({ hash: op.hash, message: 'invalid stored value' })),
        }),
        { headers: { 'content-type': 'application/json' } }
      )
    }
    const plugin = createZeroClientTransport({ fetch })
    const origin = `https://query-error-recovery-${mode}.orez.test`
    installedTransport = plugin.install(origin)
    let onError: () => void = () => {}
    let onComplete: (rows: unknown) => void = () => {}
    const error = new Promise<void>((resolve) => {
      onError = resolve
    })
    const complete = new Promise<unknown>((resolve) => {
      onComplete = resolve
    })
    function Probe() {
      const useQuery = mode === 'context' ? client.useQuery : client.useQueryDirect
      const [rows, status] = useQuery(categorySpendQuery, { category: 'Food' })
      if (status.type === 'error') onError()
      if (status.type === 'complete') onComplete(rows)
      return null
    }
    await act(async () => {
      root.render(
        <client.ProvideZero
          server={origin}
          userID="query-recovery"
          authData={{}}
          logSink={silentLogSink}
          transport={plugin}
        >
          <Probe />
        </client.ProvideZero>
      )
    })
    await act(async () => {
      await error
    })
    repaired = true
    await act(async () => {
      await installedTransport?.pull()
    })
    let rows: unknown
    await act(async () => {
      rows = await complete
    })
    expect(rows).toEqual([
      expect.objectContaining({ category: 'Food', expenseCount: 1, spent: 4250 }),
    ])
  }
)

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
