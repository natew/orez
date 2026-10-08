// @vitest-environment node
//
// SSR (no window) path of useQuery. The factory must return an inert hook
// that produces the typed-contract empty shape per query:
//   plural   → [[], info]
//   singular → [undefined, info]
//
// Previously the SSR fast-path returned `[[], info]` for ALL queries, which
// broke hydration on any caller that destructured `[row] = useQuery(singular)`
// then read row.name / row.id (SSR saw row=[], client saw row=undefined → text
// content mismatch).

import { createBuilder, createSchema, number, string, table } from '@rocicorp/zero'
import { addContextToQuery } from '@rocicorp/zero/bindings'
import { expect, test } from 'vitest'

import { createZeroClient } from './createZeroClient'
import { zql } from './zql'

// guard: this test only validates the SSR branch
if (typeof window !== 'undefined') {
  throw new Error('useQuery.ssr.test.ts must run with @vitest-environment node')
}

const todoTable = table('todo')
  .columns({ id: string(), title: string(), createdAt: number() })
  .primaryKey('id')
const schema = createSchema({ tables: [todoTable] })

const allTodos = (_args: void) =>
  (zql as unknown as { todo: { orderBy: (k: string, d: string) => any } }).todo.orderBy(
    'createdAt',
    'desc'
  )
const oneTodo = (args: { id: string }) =>
  (zql as unknown as { todo: { where: (k: string, v: string) => any } }).todo
    .where('id', args.id)
    .one()

const invalidTodo = () => {
  // @ts-expect-error a function is not a JSON query literal.
  return createBuilder(schema).todo.where('id', () => 'x')
}

const client = createZeroClient({
  schema,
  models: {},
  groupedQueries: {
    todo: { allTodos, oneTodo, invalidTodo },
  },
  instanceName: 'ssr-shape-test',
})

test('useQuery SSR returns [] for plural queries', () => {
  // the SSR factory returns a plain function (no hooks), so call it directly
  // without React mounting — that's the whole point of the no-hooks fast path.
  const [data, info] = client.useQuery(allTodos)
  expect(Array.isArray(data)).toBe(true)
  expect((data as unknown[]).length).toBe(0)
  expect((data as unknown[]).filter(() => true)).toEqual([])
  expect((data as unknown[]).find(() => true)).toBeUndefined()
  expect(info?.type).toBe('unknown')
})

test('useQuery SSR returns undefined for singular .one() queries', () => {
  const [data, info] = client.useQuery(oneTodo, { id: 'x' })
  expect(data).toBeUndefined()
  expect(info?.type).toBe('unknown')
})

test('query arguments are refused with query and key before a request is created', () => {
  expect(() => {
    // @ts-expect-error a function cannot cross the query JSON boundary.
    client.getQuery(oneTodo, { id: () => 'x' })
  }).toThrow("Query 'todo.oneTodo' argument 'params.id' must be JSON: function")
  expect(client.getQuery(oneTodo, { id: 'x' }).args).toEqual({ id: 'x' })
})

test('query filter functions are refused before materialization admits their AST', () => {
  expect(() => addContextToQuery(client.getQuery(invalidTodo), {})).toThrow(
    "Query 'todo.invalidTodo' argument 'ast.where.right.value' must be JSON: function"
  )
  expect(() => addContextToQuery(client.getQuery(allTodos), {})).not.toThrow()
})
