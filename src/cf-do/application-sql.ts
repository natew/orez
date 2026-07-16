import type { DeferredEffect } from 'orez-sync-cf-host/post-commit'
import type { SqlStatementMetadata } from 'orez-sync-cf-host'
import type {
  CompiledTransactionQueryPlan,
  TransactionQueryBudget,
  TransactionQueryFormat,
} from 'orez-sync-cf-host/transaction-query'

/** A SQLite query compiler owned by the application schema. */
export type ApplicationSqlQueryCompiler = (
  ast: unknown,
  format: TransactionQueryFormat
) => CompiledTransactionQueryPlan | Promise<CompiledTransactionQueryPlan>

/** Transaction-scoped SQLite operations exposed to trusted application code. */
export type ApplicationSqlTransaction = {
  exec(
    sql: string,
    params?: readonly unknown[],
    metadata?: SqlStatementMetadata
  ): Promise<void>
  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[]
  ): Promise<Row[]>
  queryAst<Result = unknown>(
    ast: unknown,
    format: TransactionQueryFormat,
    queryName?: string
  ): Promise<Result>
}

export type ApplicationSqlTransactionContext = {
  defer(effect: DeferredEffect): void
}

export type ApplicationSqlTransactionWork<Value> = (
  tx: ApplicationSqlTransaction,
  context: ApplicationSqlTransactionContext
) => Value | Promise<Value>

/**
 * RPC methods implemented by a ZeroSqlDO. They are reachable only through a
 * Durable Object binding. ZeroDO's public fetch surface does not expose them.
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
  applicationSqlTransaction<Value>(
    compileQuery: ApplicationSqlQueryCompiler,
    work: ApplicationSqlTransactionWork<Value>,
    queryBudget?: Partial<TransactionQueryBudget>
  ): Promise<Value>
}

export type ApplicationSqlDurableObjectNamespace = {
  idFromName(name: string): unknown
  get(id: unknown): ApplicationSqlRpc
}

/** A private client bound to exactly one authoritative SQLite namespace. */
export type ApplicationSqlClient = {
  readonly namespace: string
  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[]
  ): Promise<Row[]>
  exec(
    sql: string,
    params?: readonly unknown[],
    metadata?: SqlStatementMetadata
  ): Promise<void>
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
    transaction: (compileQuery, work, queryBudget) =>
      target.applicationSqlTransaction(compileQuery, work, queryBudget),
  }
}
