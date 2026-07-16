import { describe, expect, test } from 'bun:test'

import {
  executeTransactionQueryPlan,
  TransactionQueryBudgetError,
} from './src/transaction-query.ts'

const text = (value) => ({ kind: 'literal', value: { kind: 'text', value } })
const parent = (field) => ({ kind: 'parent_field', field })

function plan({ rootSingular = false, childSingular = false } = {}) {
  return {
    rootTable: 'user',
    planHash: '0123456789abcdef',
    root: {
      table: 'user',
      singular: rootSingular,
      sql: 'root',
      bindings: [text('enabled')],
      columns: [
        { name: 'id', columnType: 'string' },
        { name: 'active', columnType: 'boolean' },
        { name: 'profile', columnType: 'json' },
      ],
      relationships: [
        {
          name: 'posts',
          node: {
            table: 'post',
            singular: childSingular,
            sql: 'child',
            bindings: [parent('id')],
            columns: [
              { name: 'id', columnType: 'string' },
              { name: 'authorId', columnType: 'string' },
              { name: 'rank', columnType: 'number' },
            ],
            relationships: [],
          },
        },
      ],
    },
  }
}

describe('transaction query materializer', () => {
  test('hydrates related rows and decodes logical column types', () => {
    const calls = []
    const result = executeTransactionQueryPlan(plan(), (sql, params) => {
      calls.push([sql, params])
      if (sql === 'root') {
        return [
          { id: 'u1', active: 1, profile: '{"theme":"dark"}' },
          { id: 'u2', active: 0, profile: null },
        ]
      }
      return params[0] === 'u1' ? [{ id: 'p1', authorId: 'u1', rank: 3 }] : []
    })

    expect(result).toEqual([
      {
        id: 'u1',
        active: true,
        profile: { theme: 'dark' },
        posts: [{ id: 'p1', authorId: 'u1', rank: 3 }],
      },
      { id: 'u2', active: false, profile: null, posts: [] },
    ])
    expect(calls).toEqual([
      ['root', ['enabled']],
      ['child', ['u1']],
      ['child', ['u2']],
    ])
  })

  test('hydrates recursively nested relationships', () => {
    const nested = plan()
    nested.root.relationships[0].node.relationships = [
      {
        name: 'comments',
        node: {
          table: 'comment',
          singular: false,
          sql: 'grandchild',
          bindings: [parent('id')],
          columns: [
            { name: 'id', columnType: 'string' },
            { name: 'postId', columnType: 'string' },
          ],
          relationships: [],
        },
      },
    ]
    const result = executeTransactionQueryPlan(nested, (sql, params) => {
      if (sql === 'root') return [{ id: 'u1', active: 1, profile: null }]
      if (sql === 'child') return [{ id: 'p1', authorId: params[0], rank: 3 }]
      return [{ id: 'c1', postId: params[0] }]
    })

    expect(result).toEqual([
      {
        id: 'u1',
        active: true,
        profile: null,
        posts: [
          {
            id: 'p1',
            authorId: 'u1',
            rank: 3,
            comments: [{ id: 'c1', postId: 'p1' }],
          },
        ],
      },
    ])
  })

  test('uses undefined for an empty singular root and null for a singular child', () => {
    expect(executeTransactionQueryPlan(plan({ rootSingular: true }), () => [])).toBe(
      undefined
    )

    const result = executeTransactionQueryPlan(plan({ childSingular: true }), (sql) =>
      sql === 'root' ? [{ id: 'u1', active: 1, profile: null }] : []
    )
    expect(result).toEqual([{ id: 'u1', active: true, profile: null, posts: null }])
  })

  test('aborts on the select budget with a registered query name', () => {
    expect(() =>
      executeTransactionQueryPlan(
        plan(),
        (sql) =>
          sql === 'root'
            ? [
                { id: 'u1', active: 1, profile: null },
                { id: 'u2', active: 1, profile: null },
              ]
            : [],
        { queryName: 'usersWithPosts', budget: { maxSelects: 2 } }
      )
    ).toThrow(TransactionQueryBudgetError)

    try {
      executeTransactionQueryPlan(
        plan(),
        (sql) =>
          sql === 'root'
            ? [
                { id: 'u1', active: 1, profile: null },
                { id: 'u2', active: 1, profile: null },
              ]
            : [],
        { queryName: 'usersWithPosts', budget: { maxSelects: 2 } }
      )
    } catch (error) {
      expect(error.code).toBe('transaction_query_budget_exceeded')
      expect(error.query).toBe('usersWithPosts')
      expect(error.selects).toBe(3)
      expect(error.message).toContain('maxSelects=2')
    }
  })

  test('aborts on row budget with the root table and plan hash', () => {
    try {
      executeTransactionQueryPlan(
        { ...plan(), root: { ...plan().root, relationships: [] } },
        () => [
          { id: 'u1', active: 1, profile: null },
          { id: 'u2', active: 1, profile: null },
        ],
        { budget: { maxRows: 1 } }
      )
    } catch (error) {
      expect(error.query).toBe('user:0123456789abcdef')
      expect(error.rows).toBe(2)
      expect(error.message).toContain('maxRows=1')
    }
  })

  test('rejects a malformed plan before executing SQL', () => {
    let executed = false
    expect(() =>
      executeTransactionQueryPlan(
        { ...plan(), root: { ...plan().root, relationships: [{ name: 'bad' }] } },
        () => {
          executed = true
          return []
        }
      )
    ).toThrow('compiled transaction query plan')
    expect(executed).toBe(false)
  })

  test('rejects an integer result that cannot round-trip through a Zero number', () => {
    const numeric = plan()
    numeric.root.relationships = []
    numeric.root.columns = [{ name: 'rank', columnType: 'number' }]
    expect(() =>
      executeTransactionQueryPlan(numeric, () => [{ rank: 9_007_199_254_740_992n }])
    ).toThrow('does not match schema type number')
  })
})
