import { AsyncLocalStorage } from 'node:async_hooks'
import { mkdirSync } from 'node:fs'
import { resolve } from 'node:path'
import { DatabaseSync, type SQLInputValue } from 'node:sqlite'

import { executeTransactionQueryPlan } from './cloudflare-transaction-query.js'

import type {
  ApplicationSqlQueryCompiler,
  ApplicationSqlTransaction,
} from './cf-do/application-sql.js'
import type { TransactionQueryBudget } from './cloudflare-transaction-query.js'
import type { TransactionQueryFormat } from 'orez-sync-executor'

export type LocalApplicationSqlTransaction = Pick<
  ApplicationSqlTransaction,
  'exec' | 'execMany' | 'query' | 'queryAst'
>

export interface LocalApplicationSqlClient {
  transaction<Value>(
    compileQuery: ApplicationSqlQueryCompiler,
    work: (executor: LocalApplicationSqlTransaction) => Value | Promise<Value>,
    queryBudget?: Partial<TransactionQueryBudget>
  ): Promise<Value>
}

export interface LocalApplicationSqlClientFactory {
  (namespace: string): LocalApplicationSqlClient
  close(): Promise<void>
}

function sqliteBindings(params: readonly unknown[]): SQLInputValue[] {
  return params.map((param) => {
    if (typeof param === 'boolean') return param ? 1 : 0
    if (
      param === null ||
      typeof param === 'string' ||
      typeof param === 'number' ||
      typeof param === 'bigint' ||
      param instanceof Uint8Array
    ) {
      return param
    }
    throw new TypeError('application SQLite received an invalid binding')
  })
}

/** one cached connection and async transaction queue per namespace. */
export function createLocalApplicationSqlClientFactory(options: {
  dataDir: string
}): LocalApplicationSqlClientFactory {
  const dataDir = resolve(options.dataDir)
  const clients = new Map<string, LocalApplicationSqlClient>()
  const connections: DatabaseSync[] = []
  const queues = new Map<string, Promise<void>>()
  const context = new AsyncLocalStorage<{ active: boolean }>()
  let closed = false

  const factory: LocalApplicationSqlClientFactory = (namespace) => {
    if (closed) throw new Error('application SQLite factory is closed')
    if (!/^[A-Za-z0-9_-]{1,64}$/.test(namespace)) {
      throw new TypeError(`invalid application SQLite namespace: ${namespace}`)
    }
    const existing = clients.get(namespace)
    if (existing) return existing

    mkdirSync(dataDir, { recursive: true })
    const database = new DatabaseSync(resolve(dataDir, `${namespace}.sqlite`))
    database.exec('PRAGMA foreign_keys = ON')
    database.exec('PRAGMA journal_mode = WAL')
    // the native sync host writes this file from its own connection; wait for its
    // write lock as it waits for ours, instead of failing the mutation at once
    database.exec('PRAGMA busy_timeout = 5000')
    connections.push(database)

    const client: LocalApplicationSqlClient = {
      async transaction(compileQuery, work, queryBudget) {
        if (closed) throw new Error('application SQLite factory is closed')
        if (context.getStore()?.active) {
          throw new Error('nested SQLite transactions are not supported')
        }
        const previous = queues.get(namespace)
        let release!: () => void
        queues.set(
          namespace,
          new Promise<void>((resolveQueue) => {
            release = resolveQueue
          })
        )
        await previous
        const state = { active: true }
        try {
          return await context.run(state, async () => {
            const query = async <
              Row extends Record<string, unknown> = Record<string, unknown>,
            >(
              sql: string,
              params: readonly unknown[] = []
            ): Promise<Row[]> => {
              if (!state.active)
                throw new Error('application SQLite transaction has ended')
              return database.prepare(sql).all(...sqliteBindings(params)) as Row[]
            }
            const exec: LocalApplicationSqlTransaction['exec'] = async (
              sql,
              params = []
            ) => {
              if (!state.active)
                throw new Error('application SQLite transaction has ended')
              return {
                changes: Number(
                  database.prepare(sql).run(...sqliteBindings(params)).changes
                ),
              }
            }
            // take the write lock up front: a deferred transaction that reads and
            // then writes cannot wait out another writer, whatever the timeout
            database.exec('BEGIN IMMEDIATE')
            try {
              const value = await work({
                exec,
                async execMany(statements) {
                  const results = []
                  for (const statement of statements)
                    results.push(await exec(statement.sql, statement.params))
                  return results
                },
                query,
                async queryAst<Result>(
                  ast: unknown,
                  format: TransactionQueryFormat,
                  queryName?: string
                ) {
                  const plan = await compileQuery(ast, format)
                  if (!state.active)
                    throw new Error('application SQLite transaction has ended')
                  return executeTransactionQueryPlan<Result>(
                    plan,
                    (sql, params) => database.prepare(sql).all(...sqliteBindings(params)),
                    { queryName, budget: queryBudget }
                  )
                },
              })
              database.exec('COMMIT')
              return value
            } catch (error) {
              database.exec('ROLLBACK')
              throw error
            }
          })
        } finally {
          state.active = false
          release()
        }
      },
    }
    clients.set(namespace, client)
    return client
  }
  factory.close = async () => {
    if (context.getStore()?.active)
      throw new Error('cannot close application SQLite during a transaction')
    closed = true
    await Promise.all(queues.values())
    for (const database of connections.splice(0)) database.close()
  }
  return factory
}
