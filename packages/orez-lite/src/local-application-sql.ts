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

// startLocalSyncHost publishes these for its own process and the app it spawns.
// the host wakes clients only for writes it makes itself, so a commit from this
// separate connection must tell it, or open clients wait for their safety pull.
export const LOCAL_SYNC_NOTIFY_URL_ENV = 'OREZ_LOCAL_SYNC_NOTIFY_URL'
export const LOCAL_SYNC_ADMIN_TOKEN_ENV = 'OREZ_LOCAL_SYNC_ADMIN_TOKEN'

/** one notify in flight per namespace; commits during it coalesce into one more. */
function createLocalSyncNotifier(namespace: string) {
  let inFlight = false
  let pending = false
  const send = async () => {
    const url = process.env[LOCAL_SYNC_NOTIFY_URL_ENV]
    const token = process.env[LOCAL_SYNC_ADMIN_TOKEN_ENV]
    if (!url || !token) return
    inFlight = true
    try {
      const response = await fetch(
        `${url}/${encodeURIComponent(namespace)}/admin/notify`,
        { method: 'POST', headers: { 'x-admin-key': token } }
      )
      if (!response.ok) {
        console.error(
          `[orez-lite] local sync host refused notify for ${namespace}: ${response.status}`
        )
      }
    } catch (error) {
      console.error(`[orez-lite] local sync host notify failed for ${namespace}`, error)
    } finally {
      inFlight = false
    }
    if (pending) {
      pending = false
      void send()
    }
  }
  return () => {
    if (inFlight) pending = true
    else void send()
  }
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
    const notifySyncHost = createLocalSyncNotifier(namespace)
    const totalChanges = () =>
      Number(database.prepare('SELECT total_changes() AS count').get()?.count)

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
            const changesBefore = totalChanges()
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
              // a read-only transaction must not wake: clients would pull for nothing
              const wrote = totalChanges() !== changesBefore
              database.exec('COMMIT')
              if (wrote) notifySyncHost()
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
