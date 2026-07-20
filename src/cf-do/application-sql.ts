import type {
  CompiledTransactionQueryPlan,
  TransactionQueryBudget,
} from 'orez-sync-cf-host/transaction-query'
import type {
  ExecResult,
  SqlStatementMetadata,
  TransactionQueryFormat,
} from 'orez-sync-executor'

export type ApplicationSqlQueryCompiler = (
  ast: unknown,
  format: TransactionQueryFormat
) => CompiledTransactionQueryPlan | Promise<CompiledTransactionQueryPlan>

export type ApplicationSqlTable = Pick<SqlStatementMetadata, 'table' | 'publicTable'> & {
  /** capture rollback images without publishing this table to Zero clients */
  publish?: boolean
}

export type ApplicationSqlExecResult = ExecResult

export type ApplicationSqlTransaction = {
  exec(
    sql: string,
    params?: readonly unknown[],
    metadata?: SqlStatementMetadata
  ): Promise<ApplicationSqlExecResult>
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

export type ApplicationSqlTransactionWork<Value> = (
  tx: ApplicationSqlTransaction
) => Value | Promise<Value>

/**
 * private durable object RPC protocol. the session capability is returned
 * before it asks for ownership. waiting retries hold no durable object state,
 * and request cancellation disposes an active session before rejecting work.
 */
export type ApplicationSqlSessionRpc = Disposable & {
  begin(): Promise<boolean>
  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[]
  ): Promise<Row[]>
  exec(
    sql: string,
    params?: readonly unknown[],
    metadata?: SqlStatementMetadata
  ): Promise<ApplicationSqlExecResult>
  queryPlan<Result = unknown>(
    plan: CompiledTransactionQueryPlan,
    queryName?: string,
    queryBudget?: Partial<TransactionQueryBudget>
  ): Promise<Result>
  registerTables(tables: readonly ApplicationSqlTable[]): Promise<void>
  commit(): Promise<void>
  rollback(): Promise<void>
}

export type ApplicationSqlRpc = {
  applicationSqlSession(sessionID: string): Promise<ApplicationSqlSessionRpc>
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
  exec(
    sql: string,
    params?: readonly unknown[],
    metadata?: SqlStatementMetadata
  ): Promise<ApplicationSqlExecResult>
  registerTables(tables: readonly ApplicationSqlTable[]): Promise<void>
  transaction<Value>(
    compileQuery: ApplicationSqlQueryCompiler,
    work: ApplicationSqlTransactionWork<Value>,
    queryBudget?: Partial<TransactionQueryBudget>
  ): Promise<Value>
}

export type ApplicationSqlClientOptions = {
  signal?: AbortSignal
}

async function withApplicationSqlSession<Value>(
  target: ApplicationSqlRpc,
  signal: AbortSignal | undefined,
  work: (session: ApplicationSqlSessionRpc) => Value | Promise<Value>
): Promise<Value> {
  using session = await target.applicationSqlSession(crypto.randomUUID())
  let rejectAbort: ((reason: unknown) => void) | undefined
  const aborted = signal
    ? new Promise<never>((_resolve, reject) => {
        rejectAbort = reject
      })
    : undefined
  void aborted?.catch(() => {})
  const abort = () => {
    try {
      session[Symbol.dispose]()
    } finally {
      rejectAbort?.(
        signal?.reason ??
          new DOMException('application SQLite request was canceled', 'AbortError')
      )
    }
  }
  signal?.addEventListener('abort', abort, { once: true })
  try {
    signal?.throwIfAborted()
    while (!(await session.begin())) {
      await new Promise((resolve) => setTimeout(resolve, 25))
      signal?.throwIfAborted()
    }
    const pendingWork = work(session)
    const value = aborted
      ? await Promise.race([Promise.resolve(pendingWork), aborted])
      : await pendingWork
    await session.commit()
    return value
  } catch (error) {
    await session.rollback().catch(() => {})
    throw error
  } finally {
    signal?.removeEventListener('abort', abort)
  }
}

export function createApplicationSqlClient(
  durableObjects: ApplicationSqlDurableObjectNamespace,
  namespace: string,
  options: ApplicationSqlClientOptions = {}
): ApplicationSqlClient {
  if (!namespace) throw new TypeError('application SQLite namespace is required')
  const target = durableObjects.get(durableObjects.idFromName(namespace))
  return {
    namespace,
    query: (sql, params = []) =>
      withApplicationSqlSession(target, options.signal, (session) =>
        session.query(sql, params)
      ),
    exec: (sql, params = [], metadata) =>
      withApplicationSqlSession(target, options.signal, (session) =>
        session.exec(sql, params, metadata)
      ),
    registerTables: (tables) =>
      withApplicationSqlSession(target, options.signal, (session) =>
        session.registerTables(tables)
      ),
    async transaction(compileQuery, work, queryBudget) {
      return withApplicationSqlSession(target, options.signal, async (session) => {
        const tx: ApplicationSqlTransaction = {
          exec: (sql, params = [], metadata) => session.exec(sql, params, metadata),
          query: (sql, params = []) => session.query(sql, params),
          async queryAst(ast, format, queryName) {
            const plan = await compileQuery(ast, format)
            return session.queryPlan(plan, queryName, queryBudget)
          },
          registerTables: (tables) => session.registerTables(tables),
        }
        return work(tx)
      })
    },
  }
}
