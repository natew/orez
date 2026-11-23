import { expect, test } from 'vitest'

import { NativeZero } from './client.js'
import { RowTable } from './rows.js'

import type { JSONValue } from './types.js'

const issue = (id: number, title: string) => ({ id, title, meta: { tags: ['a'] } })
const comment = (id: number) => ({ id, body: `comment ${id}` })
const withComments = { relationships: { comments: {} } }

type Rows = readonly Record<string, unknown>[]
function rows(value: unknown): Rows {
  if (!Array.isArray(value)) throw new Error('expected a list result')
  return value
}

test('every watch reading an unchanged row gets the same frozen object', async () => {
  const published: unknown[][] = []
  const zero = new NativeZero({
    schema: { tables: {} },
    mutators: { touch: async () => {} },
    engine: {
      async execute(request) {
        const input = JSON.parse(request)
        switch (input.op) {
          case 'watch':
            return JSON.stringify({
              results: [
                {
                  id: input.id,
                  resultType: 'complete',
                  value:
                    input.id === 1
                      ? [
                          { ...issue(1, 'one'), comments: [comment(1), comment(2)] },
                          { ...issue(2, 'two'), comments: [] },
                        ]
                      : [
                          { ...issue(2, 'two'), comments: [] },
                          { ...issue(1, 'one'), comments: [comment(1), comment(2)] },
                        ],
                },
              ],
            })
          case 'beginMutation':
            return JSON.stringify({
              transaction: 'tx',
              mutation: { id: 1, clientID: 'client', name: 'touch', timestamp: 0 },
            })
          case 'publish':
            return JSON.stringify({ results: published.shift() ?? [] })
          case 'commitMutation':
            return JSON.stringify({ clientID: 'client' })
          case 'unwatch':
            return 'null'
          case 'pending':
            return '[]'
          default:
            throw new Error('unexpected engine operation ' + input.op)
        }
      },
      async close() {},
    },
  })
  const a: Rows[] = []
  const b: Rows[] = []
  const query = (table: string) => ({ ast: { table }, format: withComments })
  const stopA = zero.subscribe(query('a'), (value) => a.push(rows(value)))
  zero.subscribe(query('b'), (value) => b.push(rows(value)))
  await zero.pending()

  const [one, two] = a[0]!
  expect(b[0]![1]).toBe(one)
  expect(b[0]![0]).toBe(two)
  expect(Object.isFrozen(one)).toBe(true)
  expect(Object.isFrozen(one!.comments)).toBe(true)
  expect(Object.isFrozen(Reflect.get(Object(one!.meta), 'tags'))).toBe(true)
  expect(Object.isFrozen(a[0])).toBe(true)
  expect(two!.comments).toEqual([])
  expect(Object.isFrozen(two!.comments)).toBe(true)

  // watch a replaces row 1's title; watch b only moves its rows
  published.push([
    {
      id: 1,
      resultType: 'complete',
      patch: [{ ...issue(1, 'renamed'), comments: [comment(1), comment(2)] }, 1],
    },
    { id: 2, resultType: 'complete', patch: [1, 0] },
  ])
  await zero.mutate.touch()
  const [renamed, twoAgain] = a[1]!
  expect(renamed).not.toBe(one)
  expect(renamed!.comments).toBe(one!.comments)
  expect(twoAgain).toBe(two)
  expect(b[1]).toEqual([one, two])
  expect(b[1]![0]).toBe(one)
  expect(b[1]![1]).toBe(two)

  // a result type change with no row changes keeps the list
  published.push([{ id: 2, resultType: 'unknown', patch: [0, 1] }])
  await zero.mutate.touch()
  expect(b[2]).toBe(b[1])

  stopA()
  await zero.close()
})

test('the row table holds only rows some watch still reads', () => {
  const table = new RowTable()
  const format = withComments
  const first = table.result([{ ...issue(0, 'v0'), comments: [comment(0)] }], format)
  let held = first.held
  for (let version = 1; version <= 10_000; version++) {
    const next = table.patch(
      [{ ...issue(0, `v${version}`), comments: [comment(version)] }],
      held,
      format
    )
    table.release(held)
    held = next.held
  }
  // the row, its comment list and the comment
  expect(table.size).toBe(3)
  const other = table.result(
    [{ ...issue(0, 'v10000'), comments: [comment(10_000)] }] satisfies JSONValue,
    format
  )
  const patched = table.patch([0], held, format)
  expect(other.value).not.toBe(patched.value)
  expect(rows(other.value)[0]).toBe(rows(patched.value)[0])
  expect(table.size).toBe(3)
  table.release(other.held)
  table.release(patched.held)
  table.release(held)
  expect(table.size).toBe(0)
})
