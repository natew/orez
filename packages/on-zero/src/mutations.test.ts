import { AsyncLocalStorage } from 'node:async_hooks'
import { DatabaseSync, type SQLInputValue } from 'node:sqlite'

import { createSchema, string, table } from '@rocicorp/zero'
import {
  createSQLiteApplicationDatabase,
  createSyncExecutor,
  type ApplicationTransaction,
} from 'limesync'
import { createQueryCompiler } from 'limesync/cloudflare/query-compiler'
import { executeTransactionQueryPlan } from 'limesync/cloudflare/transaction-query'
import { afterAll, describe, expect, test, vi } from 'vitest'

import { createZeroServerBindings, setupAsyncLocalStorage } from './server'

const previousEnvironment = vi.hoisted(() => {
  const previous = process.env.VITE_ENVIRONMENT
  process.env.VITE_ENVIRONMENT = 'client'
  return previous
})

afterAll(() => {
  if (previousEnvironment === undefined) delete process.env.VITE_ENVIRONMENT
  else process.env.VITE_ENVIRONMENT = previousEnvironment
})

import { getMutationsPermissions } from './modelRegistry'
import { mutations } from './mutations'
import { serverWhere } from './serverWhere'

describe('mutations registry', () => {
  test('explicit opt-out removes generated CRUD from existing proxies and retains permissions', async () => {
    const record = table('closedRecord').columns({ id: string() }).primaryKey('id')
    const permission = serverWhere('closedRecord', () => true)
    const first = mutations(record, permission)
    expect(Object.keys(first).sort()).toEqual(['delete', 'insert', 'update', 'upsert'])
    const custom = vi.fn(async () => {})
    const closed = mutations(record, permission, { custom }, { crud: false })
    expect(closed).toBe(first)
    expect(Object.keys(closed)).toEqual(['custom'])
    expect('insert' in closed).toBe(false)
    expect(Reflect.get(closed, 'upsert')).toBeUndefined()
    expect(getMutationsPermissions('closedRecord')).toBe(permission)
    await closed.custom()
    expect(custom).toHaveBeenCalledOnce()
    const reopened = mutations(record, permission, {}, { crud: true })
    expect(Object.keys(reopened).sort()).toEqual([
      'custom',
      'delete',
      'insert',
      'update',
      'upsert',
    ])
  })

  test('opt-out preserves explicitly declared CRUD handlers', async () => {
    const record = table('customRecord').columns({ id: string() }).primaryKey('id')
    const permission = serverWhere('customRecord', () => true)
    const insert = vi.fn(async () => {})
    const custom = mutations(record, permission, { insert }, { crud: false })
    expect(Object.keys(custom)).toEqual(['insert'])
    await custom.insert()
    expect(insert).toHaveBeenCalledOnce()
  })

  test('re-registering a handler replaces it per key (HMR)', () => {
    const permissions = serverWhere('post', () => true)
    const v1 = async () => {}
    const v2 = async () => {}
    mutations('post', permissions, { custom: v1 })
    const proxy = mutations('post', permissions, { custom: v2 })
    // per-key merge must still take the newest registration for an edited
    // handler, otherwise HMR would pin the stale implementation
    expect(proxy.custom).toBe(v2)
  })
})

describe('generated CRUD authorization', () => {
  test('authorizes both sides of composite-key writes through the server executor', async () => {
    setupAsyncLocalStorage(AsyncLocalStorage)
    const document = table('document')
      .columns({
        workspace: string(),
        id: string(),
        ownerId: string(),
        title: string(),
      })
      .primaryKey('workspace', 'id')
    const schema = createSchema({
      tables: [document],
      relationships: [],
      enableLegacyQueries: true,
    })
    const sqlite = new DatabaseSync(':memory:')
    sqlite.exec(
      'CREATE TABLE document (workspace TEXT, id TEXT, ownerId TEXT, title TEXT, PRIMARY KEY (workspace, id))'
    )
    const compile = createQueryCompiler(schema)
    const query: ApplicationTransaction['query'] = async (sql, params = []) =>
      sqlite.prepare(sql).all(...params.map((value) => value as SQLInputValue)) as any
    const tx: ApplicationTransaction = {
      query,
      exec: async (sql, params = []) => ({
        changes: Number(
          sqlite.prepare(sql).run(...params.map((value) => value as SQLInputValue))
            .changes
        ),
      }),
      queryAst: async (ast, format, queryName) =>
        executeTransactionQueryPlan(
          compile(ast, format),
          (sql, params) =>
            sqlite.prepare(sql).all(...params.map((value) => value as SQLInputValue)),
          { queryName }
        ),
    }
    const database = createSQLiteApplicationDatabase({
      query,
      transaction: async (work) => {
        sqlite.exec('BEGIN')
        try {
          const result = await work(tx)
          sqlite.exec('COMMIT')
          return result
        } catch (error) {
          sqlite.exec('ROLLBACK')
          throw error
        }
      },
    })
    const permission = serverWhere('document', (eb, auth) =>
      eb.cmp(
        'ownerId',
        auth && 'id' in auth && typeof auth.id === 'string' ? auth.id : ''
      )
    )
    const bindings = createZeroServerBindings({
      schema,
      models: { document: { mutate: mutations(document, permission) } },
      createServerActions: () => ({}),
    })
    const executor = createSyncExecutor({
      database,
      schema,
      mutators: bindings.mutators,
      effects: {
        runBackground: (promise) => promise,
        report: (error) => {
          throw error
        },
      },
    })
    const server = bindings.server(executor)
    const auth = { authData: { id: 'alice' } }
    const own = { workspace: 'a', id: 'same', ownerId: 'alice', title: 'own' }
    const foreign = { workspace: 'b', id: 'same', ownerId: 'bob', title: 'foreign' }
    try {
      await server.mutate.document.insert(own, auth)
      await server.mutate.document.insert(foreign, { authData: { id: 'bob' } })
      await expect(
        server.mutate.document.insert({ ...foreign, id: 'forbidden' }, auth)
      ).rejects.toThrow()
      await expect(
        server.mutate.document.upsert({ ...foreign, ownerId: 'alice' }, auth)
      ).rejects.toThrow()
      await expect(
        server.mutate.document.update({ ...own, ownerId: 'bob' }, auth)
      ).rejects.toThrow()
      await expect(
        server.mutate.document.upsert({ ...own, ownerId: 'bob' }, auth)
      ).rejects.toThrow()
      await expect(
        server.mutate.document.upsert({ ...foreign, id: 'missing' }, auth)
      ).rejects.toThrow()
      await server.mutate.document.upsert({ ...own, title: 'updated' }, auth)
      await server.mutate.document.upsert({ ...own, title: 'updated' }, auth)
      await server.mutate.document.upsert({ ...own, id: 'new' }, auth)
      expect(
        sqlite.prepare('SELECT * FROM document ORDER BY workspace, id').all()
      ).toEqual([{ ...own, id: 'new' }, { ...own, title: 'updated' }, foreign])
      const closedBindings = createZeroServerBindings({
        schema,
        models: {
          document: {
            mutate: mutations(
              document,
              permission,
              {
                rename: async (
                  ctx,
                  input: { workspace: string; id: string; title: string }
                ) => {
                  await ctx.can(permission, input)
                  await ctx.tx.mutate.document.update(input)
                },
              },
              { crud: false }
            ),
          },
        },
        createServerActions: () => ({}),
      })
      expect(Object.keys(closedBindings.mutators)).toEqual(['document|rename'])
      const closedExecutor = createSyncExecutor({
        database,
        schema,
        mutators: closedBindings.mutators,
        effects: {
          runBackground: (promise) => promise,
          report: (error) => {
            throw error
          },
        },
      })
      for (const action of ['insert', 'update', 'delete', 'upsert']) {
        await expect(
          closedExecutor.execute(`document|${action}`, own, { userID: 'alice' })
        ).rejects.toThrow(`unknown mutator: document|${action}`)
      }
      await closedBindings
        .server(closedExecutor)
        .mutate.document.rename({ workspace: 'a', id: 'same', title: 'renamed' }, auth)
      expect(
        sqlite
          .prepare('SELECT title FROM document WHERE workspace = ? AND id = ?')
          .get('a', 'same')
      ).toEqual({ title: 'renamed' })
    } finally {
      sqlite.close()
    }
  })
})
