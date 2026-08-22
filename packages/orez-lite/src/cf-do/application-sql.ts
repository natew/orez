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
 * admission lane for one application SQLite session.
 *
 * a read session shares the database with every other read session and refuses
 * mutating SQL. a write session (the default) excludes every other session for
 * its whole life, which is what the row-undo journal needs to be able to roll
 * one transaction back without stepping on another's images.
 */
export type ApplicationSqlSessionPriority = 'background' | 'normal' | 'latency-sensitive'

export type ApplicationSqlSessionOptions = {
  readOnly?: boolean
  /**
   * latency-sensitive sessions enter ahead of queued normal work while keeping
   * FIFO order within their own class. use only for short control transactions
   * whose deadline protects correctness; active normal and latency-sensitive
   * transactions are never preempted. callers must bound this traffic because
   * sustained priority work can delay normal sessions.
   *
   * background is for consistent maintenance reads that may span network I/O.
   * they enter behind request work and a writer preempts an active background
   * reader, causing its next statement or commit to fail. the reader must treat
   * that failure as an abandoned operation rather than publish partial output.
   */
  priority?: ApplicationSqlSessionPriority
}

/**
 * private durable object RPC protocol. the session capability is returned
 * before it asks for ownership, and `begin()` resolves when the durable object
 * grants this session its turn in priority and arrival order. a cancellation
 * signal closes a queued session or rolls back an active session before
 * rejecting work.
 */
export type ApplicationSqlSessionRpc = Disposable & {
  begin(): Promise<void>
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
  applicationSqlSession(
    sessionID: string,
    options?: ApplicationSqlSessionOptions
  ): Promise<ApplicationSqlSessionRpc>
  applicationSqlQuery<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[],
    options?: Pick<ApplicationSqlSessionOptions, 'priority'>
  ): Promise<Row[]>
}

export type ApplicationSqlDurableObjectNamespace = {
  idFromName(name: string): unknown
  get(id: unknown): ApplicationSqlRpc
}

export type ApplicationSqlClient = {
  readonly namespace: string
  /**
   * Read-only: runs on the shared read lane, so a mutating statement is
   * rejected. A write (`INSERT ... RETURNING` included) belongs in exec() or
   * transaction(), which take the write lane.
   */
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
  /**
   * Same statements, read-only admission. Concurrent read transactions run
   * together instead of queueing behind each other, and no application-SQL
   * write session is admitted while any of them is open. The durable object's
   * own maintenance writes (transaction rollback, recovery) run outside this
   * queue, so that is admission-order fairness, not snapshot isolation.
   * A mutating statement is rejected rather than escalated.
   */
  readTransaction<Value>(
    compileQuery: ApplicationSqlQueryCompiler,
    work: ApplicationSqlTransactionWork<Value>,
    queryBudget?: Partial<TransactionQueryBudget>
  ): Promise<Value>
}

export type ApplicationSqlClientOptions = {
  signal?: AbortSignal
  priority?: ApplicationSqlSessionPriority
}

function canceled(signal: AbortSignal): unknown {
  return (
    signal.reason ??
    new DOMException('application SQLite request was canceled', 'AbortError')
  )
}

async function raceAbort<Value>(
  signal: AbortSignal | undefined,
  pending: Promise<Value>
): Promise<Value> {
  if (!signal) return pending
  // once the abort wins the race nothing observes `pending` again, and a
  // canceled session's admission is rejected by the durable object on rollback.
  void pending.catch(() => {})
  signal.throwIfAborted()
  let rejectAbort: (reason: unknown) => void = () => {}
  const aborted = new Promise<never>((_resolve, reject) => {
    rejectAbort = reject
  })
  void aborted.catch(() => {})
  const abort = () => rejectAbort(canceled(signal))
  signal.addEventListener('abort', abort, { once: true })
  try {
    return await Promise.race([pending, aborted])
  } finally {
    signal.removeEventListener('abort', abort)
  }
}

async function withApplicationSqlSession<Value>(
  target: ApplicationSqlRpc,
  signal: AbortSignal | undefined,
  sessionOptions: ApplicationSqlSessionOptions,
  work: (session: ApplicationSqlSessionRpc) => Value | Promise<Value>
): Promise<Value> {
  using session = await target.applicationSqlSession(crypto.randomUUID(), sessionOptions)
  try {
    // the durable object grants turns in priority and arrival order, so this
    // settles the moment the turn is this session's rather than on the next poll tick.
    // cancellation races the grant; rollback then drops the queued session.
    await raceAbort(signal, session.begin())
    const value = await raceAbort(signal, Promise.resolve(work(session)))
    signal?.throwIfAborted()
    await session.commit()
    return value
  } catch (error) {
    await session.rollback().catch(() => {})
    throw error
  }
}

export function createApplicationSqlClient(
  durableObjects: ApplicationSqlDurableObjectNamespace,
  namespace: string,
  options: ApplicationSqlClientOptions = {}
): ApplicationSqlClient {
  if (!namespace) throw new TypeError('application SQLite namespace is required')
  const target = durableObjects.get(durableObjects.idFromName(namespace))
  const session = <Value>(
    sessionOptions: ApplicationSqlSessionOptions,
    work: (session: ApplicationSqlSessionRpc) => Value | Promise<Value>
  ) =>
    withApplicationSqlSession(
      target,
      options.signal,
      options.priority
        ? { ...sessionOptions, priority: options.priority }
        : sessionOptions,
      work
    )
  const transaction = <Value>(
    sessionOptions: ApplicationSqlSessionOptions,
    compileQuery: ApplicationSqlQueryCompiler,
    work: ApplicationSqlTransactionWork<Value>,
    queryBudget?: Partial<TransactionQueryBudget>
  ) =>
    session(sessionOptions, (active) =>
      work({
        exec: (sql, params = [], metadata) => active.exec(sql, params, metadata),
        query: (sql, params = []) => active.query(sql, params),
        async queryAst(ast, format, queryName) {
          const plan = await compileQuery(ast, format)
          return active.queryPlan(plan, queryName, queryBudget)
        },
        registerTables: (tables) => active.registerTables(tables),
      })
    )
  return {
    namespace,
    // One statement is already atomic, so it needs no session round trips of
    // its own: the durable object opens, admits, runs and closes a read session
    // inside this single call. Cancellation only stops waiting for the answer,
    // which is free to abandon because a read leaves nothing behind.
    query: (sql, params = []) =>
      raceAbort(
        options.signal,
        target.applicationSqlQuery(
          sql,
          params,
          options.priority ? { priority: options.priority } : undefined
        )
      ),
    exec: (sql, params = [], metadata) =>
      session({}, (active) => active.exec(sql, params, metadata)),
    registerTables: (tables) => session({}, (active) => active.registerTables(tables)),
    transaction: (compileQuery, work, queryBudget) =>
      transaction({}, compileQuery, work, queryBudget),
    readTransaction: (compileQuery, work, queryBudget) =>
      transaction({ readOnly: true }, compileQuery, work, queryBudget),
  }
}
