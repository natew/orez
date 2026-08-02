import { DatabaseSync } from 'node:sqlite'

import { createSchema, string, table } from '@rocicorp/zero'
import { afterEach, describe, expect, test, vi } from 'vitest'

import { executeCrud } from './crud.js'
import { MutationRetryError } from './errors.js'
import { createSyncExecutor, handleSyncExecutorPushRequest } from './executor.js'

import type {
  ApplicationDatabase,
  ApplicationTransaction,
  EffectScheduler,
  MutatorRegistry,
} from './types.js'

const item = table('item').columns({ id: string(), value: string() }).primaryKey('id')
const schema = createSchema({ tables: [item] })

const databases: DatabaseSync[] = []

afterEach(() => {
  for (const database of databases.splice(0)) database.close()
})

function sqliteDatabase(): { database: ApplicationDatabase; sqlite: DatabaseSync } {
  const sqlite = new DatabaseSync(':memory:')
  databases.push(sqlite)

  const applicationTransaction: ApplicationTransaction = {
    async exec(sql, params = []) {
      const result = sqlite.prepare(sql).run(...params)
      return { changes: Number(result.changes) }
    },
    async query<Row extends Record<string, unknown> = Record<string, unknown>>(
      sql: string,
      params: readonly unknown[] = []
    ): Promise<readonly Row[]> {
      return sqlite.prepare(sql).all(...params) as Row[]
    },
    async queryAst() {
      throw new Error('queryAst is not used by this fixture')
    },
  }

  const database: ApplicationDatabase = {
    dialect: 'sqlite',
    async transaction<Value>(
      work: (tx: ApplicationTransaction) => Value | Promise<Value>
    ): Promise<Value> {
      sqlite.exec('BEGIN')
      try {
        const value = await work(applicationTransaction)
        sqlite.exec('COMMIT')
        return value
      } catch (error) {
        sqlite.exec('ROLLBACK')
        throw error
      }
    },
    async query<Row extends Record<string, unknown> = Record<string, unknown>>(
      sql: string,
      params: readonly unknown[] = []
    ): Promise<readonly Row[]> {
      return sqlite.prepare(sql).all(...params) as Row[]
    },
  }

  return { database, sqlite }
}

const effects: EffectScheduler = {
  async runBackground(promise) {
    await promise
  },
  report(error) {
    throw error
  },
}

function push(name: string, id = 1) {
  return {
    pushVersion: 1,
    schemaVersion: 1,
    clientGroupID: 'group-1',
    mutations: [
      {
        type: 'custom',
        clientID: 'client-1',
        id,
        name,
        args: [{}],
        timestamp: 1,
      },
    ],
  }
}

describe('sync executor', () => {
  test('insert conflict keeps the existing row and commits the later insert', async () => {
    const { database, sqlite } = sqliteDatabase()
    sqlite.exec('CREATE TABLE item (id TEXT PRIMARY KEY, value TEXT NOT NULL)')
    sqlite.prepare('INSERT INTO item (id, value) VALUES (?, ?)').run('a', 'original')

    const mutators = {
      converge: async ({ tx }) => {
        await tx.mutate.item.insert({ id: 'a', value: 'replacement' })
        await tx.mutate.item.insert({ id: 'b', value: 'later' })
      },
    } satisfies MutatorRegistry<typeof schema>
    const executor = createSyncExecutor({ database, effects, mutators, schema })

    await expect(executor.push(push('converge'), { userID: 'user-1' })).resolves.toEqual({
      pushResponse: {
        mutations: [{ id: { clientID: 'client-1', id: 1 }, result: {} }],
      },
    })
    expect(sqlite.prepare('SELECT * FROM item ORDER BY id').all()).toEqual([
      { id: 'a', value: 'original' },
      { id: 'b', value: 'later' },
    ])
    expect(sqlite.prepare('SELECT lastMutationID FROM _zsync_clients').get()).toEqual({
      lastMutationID: 1,
    })
  })

  test('the push endpoint returns zero mutate response shape, not an internal wrapper', async () => {
    const { database, sqlite } = sqliteDatabase()
    sqlite.exec('CREATE TABLE item (id TEXT PRIMARY KEY, value TEXT NOT NULL)')
    let receivedClaims: unknown
    const mutators = {
      create: async ({ ctx, tx }) => {
        receivedClaims = ctx.claims
        await tx.mutate.item.insert({ id: 'a', value: 'v' })
      },
    } satisfies MutatorRegistry<typeof schema>
    const executor = createSyncExecutor({ database, effects, mutators, schema })

    const response = await handleSyncExecutorPushRequest({
      executor,
      request: new Request('https://example.test/push', {
        method: 'POST',
        body: JSON.stringify(push('create')),
      }),
      authData: { id: 'user-1', role: 'admin' },
    })

    // zero parses this with mutateResponseSchema, which accepts
    // {mutations:[...]} or {kind:'MutateResponse',...} and nothing else
    expect(await response.json()).toEqual({
      mutations: [{ id: { clientID: 'client-1', id: 1 }, result: {} }],
    })
    expect(receivedClaims).toEqual({
      userID: 'user-1',
      authData: { id: 'user-1', role: 'admin' },
    })
  })

  test('the push endpoint reports structured unsupported-version diagnostics', async () => {
    const { database, sqlite } = sqliteDatabase()
    sqlite.exec('CREATE TABLE item (id TEXT PRIMARY KEY, value TEXT NOT NULL)')
    const executor = createSyncExecutor({ database, effects, mutators: {}, schema })
    const callback = vi.fn()
    const body = { ...push('create'), pushVersion: 2 }

    const response = await handleSyncExecutorPushRequest({
      executor,
      request: new Request('https://example.test/push?appID=chat', {
        method: 'POST',
        body: JSON.stringify(body),
      }),
      authData: { id: 'user-1' },
      diagnostics: { argAllowlist: ['id'], callback },
    })

    expect(response.status).toBe(200)
    expect(callback).toHaveBeenCalledWith({
      request: expect.objectContaining({
        appID: 'chat',
        clientGroupID: 'group-1',
        mutationCount: 1,
      }),
      failure: {
        kind: 'unsupportedPushVersion',
        origin: 'response',
        reason: 'unsupportedPushVersion',
        message: null,
        status: null,
        mutationIDs: [{ id: 1, clientID: 'client-1' }],
      },
      mutationErrors: [],
    })
  })

  test('a mutation naming an inherited key is rejected, not resolved off the prototype', async () => {
    const { database, sqlite } = sqliteDatabase()
    sqlite.exec('CREATE TABLE item (id TEXT PRIMARY KEY, value TEXT NOT NULL)')
    const executor = createSyncExecutor({ database, effects, mutators: {}, schema })

    await expect(executor.push(push('toString'), { userID: 'user-1' })).resolves.toEqual({
      pushResponse: {
        mutations: [
          {
            id: { clientID: 'client-1', id: 1 },
            // an unnamed Error carries no details, and zero omits the field
            result: { error: 'app', message: 'unknown mutator: toString' },
          },
        ],
      },
    })
  })

  test('a crud payload naming an inherited key is rejected as an unknown column', async () => {
    const { database, sqlite } = sqliteDatabase()
    sqlite.exec('CREATE TABLE item (id TEXT PRIMARY KEY, value TEXT NOT NULL)')
    const mutators = {
      sneak: async ({ tx }) => {
        await tx.mutate.item.insert({ id: 'a', value: 'v', toString: 'x' } as never)
      },
    } satisfies MutatorRegistry<typeof schema>
    const executor = createSyncExecutor({ database, effects, mutators, schema })

    const result = await executor.push(push('sneak'), { userID: 'user-1' })
    expect(result.pushResponse).toMatchObject({
      mutations: [{ result: { error: 'app', message: 'unknown column: item.toString' } }],
    })
    expect(sqlite.prepare('SELECT * FROM item').all()).toEqual([])
  })

  test('wrapping an ordinary error keeps its structured details and name', async () => {
    const { database, sqlite } = sqliteDatabase()
    sqlite.exec('CREATE TABLE item (id TEXT PRIMARY KEY, value TEXT NOT NULL)')

    class ProfanityError extends Error {
      readonly details = { flaggedWords: ['badger'] }
      constructor() {
        super('profanity detected')
        this.name = 'ProfanityError'
      }
    }
    class EnsureError extends Error {
      constructor() {
        super('not authenticated')
        this.name = 'EnsureError'
      }
    }

    const mutators = {
      profane: async () => {
        throw new ProfanityError()
      },
      unauthed: async () => {
        throw new EnsureError()
      },
    } satisfies MutatorRegistry<typeof schema>
    const executor = createSyncExecutor({ database, effects, mutators, schema })

    const withDetails = await executor.push(push('profane'), { userID: 'user-1' })
    expect(withDetails.pushResponse).toMatchObject({
      mutations: [
        {
          result: {
            error: 'app',
            message: 'profanity detected',
            details: { flaggedWords: ['badger'] },
          },
        },
      ],
    })

    // no details payload, so the error name is the metadata zero would carry
    const named = await executor.push(push('unauthed', 2), { userID: 'user-1' })
    expect(named.pushResponse).toMatchObject({
      mutations: [
        {
          result: {
            error: 'app',
            message: 'not authenticated',
            details: { name: 'EnsureError' },
          },
        },
      ],
    })
  })

  test('an ordinary mutator error rolls back, advances the ledger, and unblocks the next id', async () => {
    const { database, sqlite } = sqliteDatabase()
    sqlite.exec('CREATE TABLE item (id TEXT PRIMARY KEY, value TEXT NOT NULL)')

    const mutators = {
      reject: async ({ tx }) => {
        await tx.mutate.item.insert({ id: 'a', value: 'rolled-back' })
        throw new Error('not authenticated')
      },
      create: async ({ tx }) => {
        await tx.mutate.item.insert({ id: 'b', value: 'later' })
      },
    } satisfies MutatorRegistry<typeof schema>
    const executor = createSyncExecutor({ database, effects, mutators, schema })

    await expect(executor.push(push('reject'), { userID: 'user-1' })).resolves.toEqual({
      pushResponse: {
        mutations: [
          {
            id: { clientID: 'client-1', id: 1 },
            result: { error: 'app', message: 'not authenticated' },
          },
        ],
      },
    })
    expect(sqlite.prepare('SELECT * FROM item').all()).toEqual([])
    expect(sqlite.prepare('SELECT lastMutationID FROM _zsync_clients').get()).toEqual({
      lastMutationID: 1,
    })

    // the next mutation must land: a stalled ledger would reject id 2 as
    // out-of-order and the client would retry the failed mutation forever
    await expect(executor.push(push('create', 2), { userID: 'user-1' })).resolves.toEqual(
      {
        pushResponse: {
          mutations: [{ id: { clientID: 'client-1', id: 2 }, result: {} }],
        },
      }
    )
    expect(sqlite.prepare('SELECT * FROM item').all()).toEqual([
      { id: 'b', value: 'later' },
    ])
  })

  test('a retryable rejection writes nothing at all and keeps the mutation id', async () => {
    const { database, sqlite } = sqliteDatabase()
    sqlite.exec('CREATE TABLE item (id TEXT PRIMARY KEY, value TEXT NOT NULL)')

    let overBudget = true
    const mutators = {
      spend: async ({ tx }) => {
        if (overBudget) {
          throw new MutationRetryError(300_000, 'cloud spend budget exceeded', {
            error: 'cloudSpendBudgetExceeded',
          })
        }
        await tx.mutate.item.insert({ id: 'a', value: 'within budget' })
      },
    } satisfies MutatorRegistry<typeof schema>
    const executor = createSyncExecutor({ database, effects, mutators, schema })

    await expect(
      executor.push(push('spend'), { userID: 'user-1' })
    ).rejects.toMatchObject({
      name: 'MutationRetryError',
      status: 429,
      retryAfterMs: 300_000,
      details: { error: 'cloudSpendBudgetExceeded' },
    })

    // the whole point of refusing a write is that refusing is free. an
    // acknowledged rejection would move the ledger and append its lmid change
    // row, so the mechanism that exists to stop spending would itself spend.
    expect(sqlite.prepare('SELECT COUNT(*) AS n FROM _zsync_changes').get()).toEqual({
      n: 0,
    })
    expect(sqlite.prepare('SELECT COUNT(*) AS n FROM _zsync_clients').get()).toEqual({
      n: 0,
    })
    expect(sqlite.prepare('SELECT * FROM item').all()).toEqual([])

    // and the mutation is not consumed: the same id applies once the reason to
    // refuse it goes away, instead of being silently dropped forever.
    overBudget = false
    await expect(executor.push(push('spend'), { userID: 'user-1' })).resolves.toEqual({
      pushResponse: {
        mutations: [{ id: { clientID: 'client-1', id: 1 }, result: {} }],
      },
    })
    expect(sqlite.prepare('SELECT * FROM item').all()).toEqual([
      { id: 'a', value: 'within budget' },
    ])
  })

  test('a retryable rejection answers the push endpoint with 429 and Retry-After', async () => {
    const { database, sqlite } = sqliteDatabase()
    sqlite.exec('CREATE TABLE item (id TEXT PRIMARY KEY, value TEXT NOT NULL)')

    const mutators = {
      spend: async () => {
        throw new MutationRetryError(300_000, 'cloud spend budget exceeded', {
          error: 'cloudSpendBudgetExceeded',
        })
      },
    } satisfies MutatorRegistry<typeof schema>
    const executor = createSyncExecutor({ database, effects, mutators, schema })

    const response = await handleSyncExecutorPushRequest({
      executor,
      request: new Request('https://example.test/push', {
        method: 'POST',
        body: JSON.stringify(push('spend')),
      }),
      authData: { id: 'user-1' } as never,
    })

    expect(response.status).toBe(429)
    // seconds, because that is what an HTTP client reads
    expect(response.headers.get('retry-after')).toBe('300')
    await expect(response.json()).resolves.toEqual({
      error: 'cloud spend budget exceeded',
      details: { error: 'cloudSpendBudgetExceeded' },
      retryAfterMs: 300_000,
    })
    expect(sqlite.prepare('SELECT COUNT(*) AS n FROM _zsync_changes').get()).toEqual({
      n: 0,
    })
  })

  test('replay acknowledges without invoking the mutator or effects again', async () => {
    const { database, sqlite } = sqliteDatabase()
    sqlite.exec('CREATE TABLE item (id TEXT PRIMARY KEY, value TEXT NOT NULL)')
    let mutationRuns = 0
    let effectRuns = 0
    const mutators = {
      create: async ({ tx, ctx }) => {
        mutationRuns++
        await tx.mutate.item.insert({ id: 'a', value: 'once' })
        ctx.defer(() => {
          effectRuns++
        })
      },
    } satisfies MutatorRegistry<typeof schema>
    const executor = createSyncExecutor({ database, effects, mutators, schema })

    await executor.push(push('create'), { userID: 'user-1' })
    const replay = await executor.push(push('create'), { userID: 'user-1' })

    expect(replay).toEqual({
      pushResponse: {
        mutations: [
          {
            id: { clientID: 'client-1', id: 1 },
            result: {
              error: 'alreadyProcessed',
              details:
                'Ignoring mutation from client-1 with ID 1 as it was already processed. Expected: 2',
            },
          },
        ],
      },
    })
    expect(mutationRuns).toBe(1)
    expect(effectRuns).toBe(1)
    expect(sqlite.prepare('SELECT * FROM item').all()).toEqual([
      { id: 'a', value: 'once' },
    ])
  })

  test('postgresql insert uses numbered bindings and skip-if-exists conflict SQL', async () => {
    const statements: Array<{ sql: string; params: readonly unknown[] }> = []
    const tx: ApplicationTransaction = {
      async exec(sql, params = []) {
        statements.push({ sql, params })
        return { changes: 0 }
      },
      async query() {
        return []
      },
      async queryAst() {
        throw new Error('unused')
      },
    }

    await executeCrud(tx, schema, 'postgresql', 'item', 'insert', {
      id: 'a',
      value: 'original',
    })

    expect(statements).toEqual([
      {
        sql: 'INSERT INTO "item" ("id", "value") VALUES ($1, $2) ON CONFLICT ("id") DO NOTHING',
        params: ['a', 'original'],
      },
    ])
  })

  // Every other postgresql test here asserts against a transaction that only
  // records SQL strings, so a statement PostgreSQL actually rejects still looks
  // correct. This one executes it. The preflight upsert's DO UPDATE predicate
  // was `WHERE "userID" IS NULL`, which PostgreSQL refuses as ambiguous between
  // the conflicting row and `excluded` — that error surfaced as a failed push
  // for every client, while SQLite accepted the same statement.
  test('postgresql preflight upsert executes against a real postgres', async () => {
    const { PGlite } = await import('@electric-sql/pglite')
    const pg = new PGlite()
    const run = async (sql: string, params: readonly unknown[] = []) =>
      (await pg.query(sql, params as unknown[])).rows as Record<string, unknown>[]

    const applicationTransaction: ApplicationTransaction = {
      async exec(sql, params = []) {
        await pg.query(sql, params as unknown[])
        return { changes: 0 }
      },
      async query(sql, params = []) {
        return (await run(sql, params)) as never
      },
      async queryAst() {
        throw new Error('queryAst is not used by this fixture')
      },
    }
    const database: ApplicationDatabase = {
      dialect: 'postgresql',
      async transaction(work) {
        await pg.query('BEGIN')
        try {
          const value = await work(applicationTransaction)
          await pg.query('COMMIT')
          return value
        } catch (error) {
          await pg.query('ROLLBACK')
          throw error
        }
      },
      async query(sql, params = []) {
        return (await run(sql, params)) as never
      },
    }

    await pg.query('CREATE TABLE item (id TEXT PRIMARY KEY, value TEXT NOT NULL)')
    const mutators = {
      create: async ({ tx }) => {
        await tx.mutate.item.insert({ id: 'a', value: 'v' })
      },
    } satisfies MutatorRegistry<typeof schema>
    const executor = createSyncExecutor({ database, effects, mutators, schema })

    await expect(executor.push(push('create'), { userID: 'user-1' })).resolves.toEqual({
      pushResponse: {
        mutations: [{ id: { clientID: 'client-1', id: 1 }, result: {} }],
      },
    })
    expect(await run('SELECT id, value FROM item')).toEqual([{ id: 'a', value: 'v' }])
    expect(await run('SELECT "userID" FROM "_zsync_clients"')).toEqual([
      { userID: 'user-1' },
    ])
    await pg.close()
  })

  test('accepts cleanup mutation id zero without dispatch or acknowledgement', async () => {
    const { database, sqlite } = sqliteDatabase()
    sqlite.exec('CREATE TABLE item (id TEXT PRIMARY KEY, value TEXT NOT NULL)')
    const executor = createSyncExecutor({
      database,
      effects,
      mutators: {},
      schema,
    })

    await expect(
      executor.push(
        {
          pushVersion: 1,
          clientGroupID: 'group-1',
          mutations: [
            {
              type: 'custom',
              clientID: 'client-1',
              id: 0,
              name: '_zero_cleanupResults',
              args: [{}],
            },
          ],
        },
        { userID: 'user-1' }
      )
    ).resolves.toEqual({ pushResponse: { mutations: [] } })
  })

  test('recognizes application errors created by another package instance', async () => {
    const { database, sqlite } = sqliteDatabase()
    sqlite.exec('CREATE TABLE item (id TEXT PRIMARY KEY, value TEXT NOT NULL)')
    class ForeignMutationApplicationError extends Error {
      readonly details = { reason: 'denied' }

      constructor() {
        super('permission denied')
        this.name = 'MutationApplicationError'
      }
    }
    const executor = createSyncExecutor({
      database,
      effects,
      mutators: {
        denied: async ({ tx }) => {
          await tx.mutate.item.insert({ id: 'rolled-back', value: 'no' })
          throw new ForeignMutationApplicationError()
        },
      },
      schema,
    })

    await expect(executor.push(push('denied'), { userID: 'user-1' })).resolves.toEqual({
      pushResponse: {
        mutations: [
          {
            id: { clientID: 'client-1', id: 1 },
            result: {
              error: 'app',
              message: 'permission denied',
              details: { reason: 'denied' },
            },
          },
        ],
      },
    })
    expect(sqlite.prepare('SELECT * FROM item').all()).toEqual([])
    expect(sqlite.prepare('SELECT lastMutationID FROM _zsync_clients').get()).toEqual({
      lastMutationID: 1,
    })
  })
})
