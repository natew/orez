import { createBuilder, createSchema, number, string, table } from '@rocicorp/zero'
import { count, defineRollups } from 'orez-lite/rollup'
import { afterEach, describe, expect, test, vi } from 'vitest'

import { setSchema } from '../state'
import { createMutators } from './createMutators'

afterEach(() => {
  vi.useRealTimers()
})

describe('createMutators timeout guard', () => {
  test('releases the timeout when a mutation succeeds', async () => {
    vi.useFakeTimers()
    const mutators = createMutators({
      environment: 'server',
      authData: null,
      can: {} as never,
      models: {
        task: {
          mutate: {
            create: async () => {},
          },
        },
      } as never,
    }) as any

    await mutators.task.create({}, {})

    expect(vi.getTimerCount()).toBe(0)
  })

  test('releases the timeout when a mutation fails', async () => {
    vi.useFakeTimers()
    const mutators = createMutators({
      environment: 'server',
      authData: null,
      can: {} as never,
      models: {
        task: {
          mutate: {
            create: async () => {
              throw new Error('nope')
            },
          },
        },
      } as never,
    }) as any

    await expect(mutators.task.create({}, {})).rejects.toThrow('nope')

    expect(vi.getTimerCount()).toBe(0)
  })
})

test('createMutators installs optimistic rollups on client transactions', async () => {
  const comment = table('comment')
    .columns({ id: string(), postId: string() })
    .primaryKey('id')
  const post = table('post')
    .columns({ id: string(), commentCount: number() })
    .primaryKey('id')
  const schema = createSchema({ tables: [comment, post] })
  setSchema(schema, createBuilder(schema))
  const rollups = defineRollups(schema, {
    postCommentCount: {
      source: 'comment',
      target: 'post',
      mode: 'existing',
      groupBy: { postId: 'id' },
      aggregates: { commentCount: count() },
    },
  })
  const rows: Record<string, Array<Record<string, unknown>>> = {
    comment: [],
    post: [{ id: 'p1', commentCount: 0 }],
  }
  const mutate = {
    comment: {
      async insert(input: Readonly<Record<string, unknown>>) {
        rows.comment!.push({ ...input })
      },
      async upsert() {},
      async update() {},
      async delete() {},
    },
    post: {
      async insert() {},
      async upsert() {},
      async update(input: Readonly<Record<string, unknown>>) {
        const row = rows.post!.find((candidate) => candidate.id === input.id)
        if (row) Object.assign(row, input)
      },
      async delete() {},
    },
  }
  const transaction = {
    location: 'client',
    mutate,
    async run(query: unknown) {
      if (!query || typeof query !== 'object') return undefined
      const ast = Reflect.get(query, 'ast')
      if (!ast || typeof ast !== 'object') return undefined
      const tableName = Reflect.get(ast, 'table')
      const where = Reflect.get(ast, 'where')
      if (typeof tableName !== 'string' || !where || typeof where !== 'object') {
        return undefined
      }
      const left = Reflect.get(where, 'left')
      const right = Reflect.get(where, 'right')
      if (!left || typeof left !== 'object' || !right || typeof right !== 'object') {
        return undefined
      }
      const column = Reflect.get(left, 'name')
      const value = Reflect.get(right, 'value')
      return typeof column === 'string'
        ? rows[tableName]?.find((row) => Object.is(row[column], value))
        : undefined
    },
  }
  const mutators = createMutators({
    environment: 'client',
    authData: null,
    can: async () => {},
    rollups,
    models: {
      comment: {
        mutate: {
          add: async (ctx, input: { id: string; postId: string }) => {
            await ctx.tx.mutate.comment.insert(input)
          },
        },
      },
    },
  })

  await Reflect.apply(mutators.comment.add, null, [
    transaction,
    { id: 'c1', postId: 'p1' },
  ])

  expect(rows.post).toEqual([{ id: 'p1', commentCount: 1 }])
})
