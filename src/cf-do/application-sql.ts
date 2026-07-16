import type { DeferredEffect } from 'orez-sync-cf-host/post-commit'
import type { SqlStatementMetadata } from 'orez-sync-cf-host'
import type {
  CompiledTransactionQueryPlan,
  TransactionQueryBudget,
  TransactionQueryFormat,
} from 'orez-sync-cf-host/transaction-query'

export type ApplicationSqlQueryCompiler = (
  ast: unknown,
  format: TransactionQueryFormat
) => CompiledTransactionQueryPlan | Promise<CompiledTransactionQueryPlan>

export type ApplicationSqlTable = Pick<SqlStatementMetadata, 'table' | 'publicTable'> & {
  /** capture rollback images without publishing this table to Zero clients */
  publish?: boolean
}

export type ApplicationSqlTransaction = {
  exec(sql: string, params?: readonly unknown[], metadata?: SqlStatementMetadata): Promise<void>
  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[]
  ): Promise<Row[]>
  queryAst<Result = unknown>(
    ast: unknown,
    format: TransactionQueryFormat,
    queryName?: string
  ): Promise<Result>
  registerTables(tables: readonly ApplicationSqlTable[]): Promise<void>
}

export type ApplicationSqlTransactionContext = {
  defer(effect: DeferredEffect): void
}

export type ApplicationSqlTransactionWork<Value> = (
  tx: ApplicationSqlTransaction,
  context: ApplicationSqlTransactionContext
) => Value | Promise<Value>

/**
 * Private Durable Object RPC protocol. Transaction callbacks stay in the
 * caller: every SQL turn is tagged with a serialized session id, avoiding a
 * re-entrant RPC call into a Durable Object that is awaiting a callback.
 */
export type ApplicationSqlRpc = {
  applicationSqlQuery<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[]
  ): Promise<Row[]>
  applicationSqlExec(
    sql: string,
    params?: readonly unknown[],
    metadata?: SqlStatementMetadata
  ): Promise<void>
  applicationSqlRegisterTables(tables: readonly ApplicationSqlTable[]): Promise<void>
  applicationSqlBegin(sessionID: string): Promise<void>
  applicationSqlSessionQuery<Row extends Record<string, unknown> = Record<string, unknown>>(
    sessionID: string,
    sql: string,
    params?: readonly unknown[]
  ): Promise<Row[]>
  applicationSqlSessionExec(
    sessionID: string,
    sql: string,
    params?: readonly unknown[],
    metadata?: SqlStatementMetadata
  ): Promise<void>
  applicationSqlSessionQueryPlan<Result = unknown>(
    sessionID: string,
    plan: CompiledTransactionQueryPlan,
    queryName?: string,
    queryBudget?: Partial<TransactionQueryBudget>
  ): Promise<Result>
  applicationSqlSessionRegisterTables(
    sessionID: string,
    tables: readonly ApplicationSqlTable[]
  ): Promise<void>
  applicationSqlCommit(sessionID: string): Promise<void>
  applicationSqlRollback(sessionID: string): Promise<void>
}

export type ApplicationSqlDurableObjectNamespace = {
  idFromName(name: string): unknown
  get(id: unknown): ApplicationSqlRpc
}

export type ApplicationSqlClient = {
  readonly namespace: string
  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[]
  ): Promise<Row[]>
  exec(sql: string, params?: readonly unknown[], metadata?: SqlStatementMetadata): Promise<void>
  registerTables(tables: readonly ApplicationSqlTable[]): Promise<void>
  transaction<Value>(
    compileQuery: ApplicationSqlQueryCompiler,
    work: ApplicationSqlTransactionWork<Value>,
    queryBudget?: Partial<TransactionQueryBudget>
  ): Promise<Value>
}

export function createApplicationSqlClient(
  durableObjects: ApplicationSqlDurableObjectNamespace,
  namespace: string
): ApplicationSqlClient {
  if (!namespace) throw new TypeError('application SQLite namespace is required')
  const target = durableObjects.get(durableObjects.idFromName(namespace))
  return {
    namespace,
    query: (sql, params = []) => target.applicationSqlQuery(sql, params),
    exec: (sql, params = [], metadata) => target.applicationSqlExec(sql, params, metadata),
    registerTables: (tables) => target.applicationSqlRegisterTables(tables),
    async transaction(compileQuery, work, queryBudget) {
      const sessionID = crypto.randomUUID()
      const effects: DeferredEffect[] = []
      await target.applicationSqlBegin(sessionID)
      const tx: ApplicationSqlTransaction = {
        exec: (sql, params = [], metadata) =>
          target.applicationSqlSessionExec(sessionID, sql, params, metadata),
        query: (sql, params = []) => target.applicationSqlSessionQuery(sessionID, sql, params),
        async queryAst(ast, format, queryName) {
          const plan = await compileQuery(ast, format)
          return target.applicationSqlSessionQueryPlan(sessionID, plan, queryName, queryBudget)
        },
        registerTables: (tables) => target.applicationSqlSessionRegisterTables(sessionID, tables),
      }
      try {
        const value = await work(tx, { defer: (effect) => effects.push(effect) })
        await target.applicationSqlCommit(sessionID)
        for (const effect of effects) await effect()
        return value
      } catch (error) {
        await target.applicationSqlRollback(sessionID).catch(() => {})
        throw error
      }
    },
  }
}
