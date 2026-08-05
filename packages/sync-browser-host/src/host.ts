import createSqliteModule from 'bedrock-sqlite/browser'
import { resolveQueryPatch } from 'orez-sync-cf-host/query-patch'
import { beginWriteSetCapture, createSyncExecutor } from 'orez-sync-executor/core'

import {
  engine_compile_query,
  engine_handle_query_pull,
  engine_init_query_schema,
  engine_init_schema,
  engine_prune,
} from './generated/sync_wasm.js'
import initSyncWasm from './generated/sync_wasm.js'
import { IndexedDbSnapshotStore } from './idb-snapshot.js'
import {
  BedrockDirectSql,
  BedrockMutatorSql,
  BedrockSyncDb,
  type BedrockBrowserModule,
} from './sqlite-adapter.js'

import type {
  BrowserSyncHost,
  BrowserSyncHostConfig,
  BrowserSyncHostDiagnostic,
  BrowserSyncHostDiagnostics,
  BrowserSyncHostOperation,
  BrowserSyncHostTransaction,
} from './types.js'
import type { Schema } from '@rocicorp/zero'
import type {
  ApplicationDatabase,
  ApplicationTransaction,
  AuthData,
  ExecResult,
  JsonValue,
  NormalizedClaims,
  PushResult,
  SqlStatementMetadata,
  SyncExecutor,
} from 'orez-sync-executor'

export type BrowserHostTestFaultPoint =
  | 'before_mutation'
  | 'after_app_write_before_sqlite_commit'
  | 'after_sqlite_commit_before_idb_commit'
  | 'after_idb_commit_before_response'
  | 'during_response_delivery'

export type BrowserHostTestHooks = {
  reach(point: BrowserHostTestFaultPoint): void | Promise<void>
}

let syncWasmReady: Promise<void> | undefined

function initializeSyncWasm(url: string | URL): Promise<void> {
  return (syncWasmReady ??= initSyncWasm({ module_or_path: url }).then(() => undefined))
}

function json(value: unknown, status = 200): Response {
  return Response.json(value, {
    status,
    headers: { 'cache-control': 'no-store' },
  })
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function statusOf(error: unknown): number {
  if (typeof error === 'object' && error !== null && 'status' in error) {
    const status = Number(error.status)
    if (Number.isInteger(status) && status >= 400 && status <= 599) return status
  }
  return 500
}

function requestError(message: string, status = 400): Error & { status: number } {
  return Object.assign(new Error(message), { status })
}

async function requestObject(request: Request): Promise<Record<string, unknown>> {
  let value: unknown
  try {
    value = await request.json()
  } catch {
    throw requestError('invalid JSON request body')
  }
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw requestError('request body must be a JSON object')
  }
  return value as Record<string, unknown>
}

function validateConfig<S extends Schema, A extends AuthData>(
  config: BrowserSyncHostConfig<S, A>
): void {
  if (!config.storageKey) throw new TypeError('storageKey must not be empty')
  if (!config.mutators) throw new TypeError('mutators are required')
  if (!config.queries || typeof config.queries !== 'object') {
    throw new TypeError(
      "queries is required: pass the app's Zero query registry (defineQueries result)"
    )
  }
  if (config.transactionQueryBudget) {
    for (const [name, value] of Object.entries(config.transactionQueryBudget)) {
      if (!Number.isSafeInteger(value) || Number(value) < 1) {
        throw new TypeError(
          `transactionQueryBudget.${name} must be a positive safe integer`
        )
      }
    }
  }
  const retainChanges = config.retainChanges ?? 4_096
  if (!Number.isSafeInteger(retainChanges) || retainChanges < 0) {
    throw new TypeError('retainChanges must be a non-negative safe integer')
  }
}

class BrowserSyncHostDiagnosticSink {
  constructor(
    private readonly storageKey: string,
    private readonly diagnostics: BrowserSyncHostDiagnostics | undefined
  ) {}

  enabled(): boolean {
    try {
      return this.diagnostics?.enabled() === true
    } catch (error) {
      console.error('browser sync host diagnostics enabled check failed', error)
      return false
    }
  }

  emit(diagnostic: Omit<BrowserSyncHostDiagnostic, 'storageKey' | 'timestamp'>): void {
    try {
      this.diagnostics?.callback({
        ...diagnostic,
        storageKey: this.storageKey,
        timestamp: Date.now(),
      })
    } catch (error) {
      console.error('browser sync host diagnostics callback failed', error)
    }
  }
}

class OperationQueue {
  #tail = Promise.resolve()
  #depth = 0
  #nextOperationId = 0

  constructor(private readonly diagnostics: BrowserSyncHostDiagnosticSink) {}

  get depth(): number {
    return this.#depth
  }

  run<Value>(
    kind: BrowserSyncHostOperation,
    operation: (operationId: number) => Value | Promise<Value>
  ): Promise<Value> {
    const operationId = ++this.#nextOperationId
    const queuedAt = performance.now()
    this.#depth++
    if (this.diagnostics.enabled()) {
      this.diagnostics.emit({
        stage: 'operation',
        phase: 'queued',
        operation: kind,
        operationId,
        transaction: null,
        queueDepth: this.#depth,
        waitMs: null,
        durationMs: null,
      })
    }
    const result = this.#tail.then(async () => {
      const startedAt = performance.now()
      if (this.diagnostics.enabled()) {
        this.diagnostics.emit({
          stage: 'operation',
          phase: 'started',
          operation: kind,
          operationId,
          transaction: null,
          queueDepth: this.#depth,
          waitMs: startedAt - queuedAt,
          durationMs: null,
        })
      }
      // a never-settling operation wedges every later queued call while the
      // event loop stays responsive, so the holder is named unconditionally
      // rather than behind the opt-in diagnostics sink. silent under 30s.
      const stallTimer = setInterval(() => {
        console.error(
          `[browser-sync-host] operation ${kind}#${operationId} has held the queue for ${Math.round((performance.now() - startedAt) / 1000)}s (depth ${this.#depth})`
        )
      }, 30_000)
      try {
        const value = await operation(operationId)
        if (this.diagnostics.enabled()) {
          this.diagnostics.emit({
            stage: 'operation',
            phase: 'completed',
            operation: kind,
            operationId,
            transaction: null,
            queueDepth: this.#depth,
            waitMs: startedAt - queuedAt,
            durationMs: performance.now() - startedAt,
          })
        }
        return value
      } catch (error) {
        if (this.diagnostics.enabled()) {
          this.diagnostics.emit({
            stage: 'operation',
            phase: 'failed',
            operation: kind,
            operationId,
            transaction: null,
            queueDepth: this.#depth,
            waitMs: startedAt - queuedAt,
            durationMs: performance.now() - startedAt,
            error: errorMessage(error),
          })
        }
        throw error
      } finally {
        clearInterval(stallTimer)
        this.#depth--
      }
    })
    this.#tail = result.then(
      () => undefined,
      () => undefined
    )
    return result
  }

  async drain(): Promise<void> {
    await this.#tail
  }
}

class BrowserSyncHostImpl<
  S extends Schema,
  A extends AuthData,
> implements BrowserSyncHost<S> {
  readonly executor: SyncExecutor<S>
  readonly #queue: OperationQueue
  readonly #diagnostics: BrowserSyncHostDiagnosticSink
  readonly #listeners = new Set<() => void>()
  readonly #directSql: BedrockDirectSql
  readonly #engineDb: BedrockSyncDb
  readonly #mutatorSql: BedrockMutatorSql
  readonly #rawExecutor: SyncExecutor<S>
  readonly #retainChanges: string
  #activeOperation: { id: number; kind: BrowserSyncHostOperation } | null = null
  #executorTransactionKind: 'direct' | 'mutation' = 'direct'
  #fatalError: Error | undefined
  #closed = false
  #closePromise: Promise<void> | undefined

  private constructor(
    private readonly config: BrowserSyncHostConfig<S, A>,
    private readonly module: BedrockBrowserModule,
    private readonly db: InstanceType<BedrockBrowserModule['Database']>,
    private readonly snapshots: IndexedDbSnapshotStore,
    private readonly hooks?: BrowserHostTestHooks
  ) {
    this.#diagnostics = new BrowserSyncHostDiagnosticSink(
      config.storageKey,
      config.diagnostics
    )
    this.#queue = new OperationQueue(this.#diagnostics)
    this.#directSql = new BedrockDirectSql(db)
    this.#engineDb = new BedrockSyncDb(db)
    this.#mutatorSql = new BedrockMutatorSql(
      this.#directSql,
      (ast, format) => engine_compile_query(config.schema, ast, format),
      config.transactionQueryBudget
    )
    const database: ApplicationDatabase = {
      dialect: 'sqlite',
      transaction: async <Value>(
        work: (tx: ApplicationTransaction) => Value | Promise<Value>
      ): Promise<Value> => {
        let applicationWrite = false
        const tx: ApplicationTransaction = {
          exec: async (sql, params, metadata) => {
            if (
              metadata !== undefined ||
              (!/^\s*CREATE\s+(?:SCHEMA|TABLE)\b/i.test(sql) &&
                !/\b_zsync_[A-Za-z0-9_]+\b/.test(sql))
            ) {
              applicationWrite = true
            }
            return this.#mutatorSql.exec(sql, params, metadata)
          },
          query: (sql, params) => this.#mutatorSql.query(sql, params),
          queryAst: (ast, format, queryName) =>
            this.#mutatorSql.queryAst(ast, format, queryName),
        }
        return this.#writeTransaction(
          () => (applicationWrite ? this.#executorTransactionKind : 'maintenance'),
          async () => {
            const value = await work(tx)
            if (applicationWrite && this.#executorTransactionKind === 'mutation') {
              await this.#reach('after_app_write_before_sqlite_commit')
            }
            return value
          }
        )
      },
      query: (sql, params) => Promise.resolve(this.#directSql.query(sql, params)),
    }
    this.#rawExecutor = createSyncExecutor({
      database,
      effects: {
        runBackground: (promise) => promise,
        report: (error) => console.error('browser sync deferred effect failed', error),
      },
      mutators: config.mutators,
      schema: config.schema,
    })
    this.executor = {
      schema: config.schema,
      push: (body, claims) => {
        this.#assertAccepting()
        return this.#runQueued('executor-push', () =>
          this.#runExecutorPush(body, claims, false)
        )
      },
      execute: (name, args, claims) => {
        this.#assertAccepting()
        return this.#runQueued('executor-execute', async () => {
          this.#executorTransactionKind = 'direct'
          await this.#rawExecutor.execute(name, args, claims)
          this.#notifyDataChanged()
        })
      },
      transaction: (claims, work) => {
        this.#assertAccepting()
        return this.#runQueued('executor-transaction', async () => {
          this.#executorTransactionKind = 'direct'
          const value = await this.#rawExecutor.transaction(claims, work)
          this.#notifyDataChanged()
          return value
        })
      },
      query: (claims, work) => {
        this.#assertAccepting()
        return this.#runQueued('executor-query', () =>
          this.#rawExecutor.query(claims, work)
        )
      },
    }
    this.#retainChanges = String(config.retainChanges ?? 4_096)
  }

  static async create<S extends Schema, A extends AuthData>(
    config: BrowserSyncHostConfig<S, A>,
    hooks?: BrowserHostTestHooks
  ): Promise<BrowserSyncHostImpl<S, A>> {
    validateConfig(config)
    const sqliteWasmUrl =
      config.assets?.sqliteWasmUrl ??
      new URL('./generated/sqlite3-browser.wasm', import.meta.url)
    const syncWasmUrl =
      config.assets?.syncWasmUrl ??
      new URL('./generated/sync_wasm_bg.wasm', import.meta.url)
    await initializeSyncWasm(syncWasmUrl)

    const module = (await createSqliteModule({
      locateFile: (path: string) =>
        path.endsWith('.wasm') ? String(sqliteWasmUrl) : path,
    })) as BedrockBrowserModule
    if (!module._memfs?.files) {
      throw new Error('Bedrock browser build does not expose its VFS snapshot surface')
    }

    const diagnostics = new BrowserSyncHostDiagnosticSink(
      config.storageKey,
      config.diagnostics
    )
    const snapshots = new IndexedDbSnapshotStore(config.storageKey)
    const restoreStartedAt = performance.now()
    if (diagnostics.enabled()) {
      diagnostics.emit({
        stage: 'restore',
        phase: 'started',
        operation: null,
        operationId: null,
        transaction: null,
        queueDepth: 0,
        waitMs: null,
        durationMs: null,
      })
    }
    try {
      await snapshots.restore(module)
      if (diagnostics.enabled()) {
        diagnostics.emit({
          stage: 'restore',
          phase: 'completed',
          operation: null,
          operationId: null,
          transaction: null,
          queueDepth: 0,
          waitMs: null,
          durationMs: performance.now() - restoreStartedAt,
        })
      }
    } catch (error) {
      if (diagnostics.enabled()) {
        diagnostics.emit({
          stage: 'restore',
          phase: 'failed',
          operation: null,
          operationId: null,
          transaction: null,
          queueDepth: 0,
          waitMs: null,
          durationMs: performance.now() - restoreStartedAt,
          error: errorMessage(error),
        })
      }
      throw error
    }
    const db = new module.Database('/project.db')
    const journalMode = String(db.pragma('journal_mode = DELETE', { simple: true }))
    if (journalMode.toLowerCase() !== 'delete') {
      db.close()
      await snapshots.close()
      throw new Error(`Bedrock SQLite did not enter DELETE journal mode: ${journalMode}`)
    }
    db.pragma('foreign_keys = ON')

    const host = new BrowserSyncHostImpl(config, module, db, snapshots, hooks)
    try {
      await host.#writeTransaction('bootstrap', async () => {
        config.initialize(host.#directSql)
        engine_init_schema(host.#engineDb, config.schema)
        engine_init_query_schema(host.#engineDb)
      })
      return host
    } catch (error) {
      await host.#closeResources()
      throw error
    }
  }

  #assertAvailable(): void {
    if (this.#fatalError) throw this.#fatalError
    if (this.#closed) throw new Error('browser sync host is closed')
  }

  #assertAccepting(): void {
    if (this.#closePromise) throw new Error('browser sync host is closed')
    this.#assertAvailable()
  }

  async #reach(point: BrowserHostTestFaultPoint): Promise<void> {
    await this.hooks?.reach(point)
  }

  async #runQueued<Value>(
    kind: BrowserSyncHostOperation,
    operation: () => Value | Promise<Value>
  ): Promise<Value> {
    return this.#queue.run(kind, async (operationId) => {
      this.#activeOperation = { id: operationId, kind }
      try {
        return await operation()
      } finally {
        this.#activeOperation = null
      }
    })
  }

  async #checkpoint(transaction: BrowserSyncHostTransaction): Promise<void> {
    const startedAt = performance.now()
    const operation = this.#activeOperation
    if (this.#diagnostics.enabled()) {
      this.#diagnostics.emit({
        stage: 'checkpoint',
        phase: 'started',
        operation: operation?.kind ?? null,
        operationId: operation?.id ?? null,
        transaction,
        queueDepth: this.#queue.depth,
        waitMs: null,
        durationMs: null,
      })
    }
    try {
      const stats = await this.snapshots.checkpoint(this.module)
      if (this.#diagnostics.enabled()) {
        this.#diagnostics.emit({
          stage: 'checkpoint',
          phase: 'completed',
          operation: operation?.kind ?? null,
          operationId: operation?.id ?? null,
          transaction,
          queueDepth: this.#queue.depth,
          waitMs: null,
          durationMs: performance.now() - startedAt,
          ...stats,
        })
      }
    } catch (error) {
      if (this.#diagnostics.enabled()) {
        this.#diagnostics.emit({
          stage: 'checkpoint',
          phase: 'failed',
          operation: operation?.kind ?? null,
          operationId: operation?.id ?? null,
          transaction,
          queueDepth: this.#queue.depth,
          waitMs: null,
          durationMs: performance.now() - startedAt,
          error: errorMessage(error),
        })
      }
      const fatal = new Error(
        `browser database checkpoint failed; host terminated: ${errorMessage(error)}`,
        { cause: error }
      )
      this.#fatalError = fatal
      this.db.close()
      throw fatal
    }
  }

  async #writeTransaction<Value>(
    kind:
      | 'bootstrap'
      | 'mutation'
      | 'pull'
      | 'maintenance'
      | 'direct'
      | (() => 'mutation' | 'maintenance' | 'direct'),
    operation: () => Value | Promise<Value>
  ): Promise<Value> {
    this.#assertAvailable()
    this.db.exec('BEGIN')
    let value: Value
    try {
      value = await operation()
      this.db.exec('COMMIT')
    } catch (error) {
      if (this.db.inTransaction) this.db.exec('ROLLBACK')
      throw error
    }
    const completedKind = typeof kind === 'function' ? kind() : kind
    if (completedKind === 'mutation') {
      await this.#reach('after_sqlite_commit_before_idb_commit')
    }
    await this.#checkpoint(completedKind)
    return value
  }

  async #auth(request: Request): Promise<{
    authData: A | null
    claims: NormalizedClaims
  }> {
    const authData = await this.config.authenticate(request)
    if (!authData || typeof authData.id !== 'string' || authData.id.length === 0) {
      throw requestError('missing authentication', 401)
    }
    if (!(await this.config.authorize(request, authData, this.config.storageKey))) {
      throw requestError('forbidden', 403)
    }
    const claims: Record<string, JsonValue> = { userID: authData?.id ?? 'anon' }
    if (authData) claims.authData = authData as unknown as JsonValue
    return { authData, claims: claims as NormalizedClaims }
  }

  async #runExecutorPush(
    body: unknown,
    claims: NormalizedClaims,
    testHooks: boolean
  ): Promise<PushResult> {
    this.#assertAvailable()
    this.#executorTransactionKind = 'mutation'
    let result: PushResult
    try {
      result = await this.#rawExecutor.push(body, claims)
    } finally {
      this.#executorTransactionKind = 'direct'
    }
    const mutationResults =
      'mutations' in result.pushResponse ? result.pushResponse.mutations : []
    if (mutationResults.length > 0) {
      await this.#writeTransaction('maintenance', () =>
        engine_prune(this.#engineDb, this.#retainChanges)
      )
    }
    const changed = mutationResults.some(
      (mutation) =>
        !('error' in mutation.result) || mutation.result.error !== 'alreadyProcessed'
    )
    if (changed) this.#notifyDataChanged()
    if (testHooks && mutationResults.length > 0) {
      await this.#reach('after_idb_commit_before_response')
    }
    return result
  }

  #resolvePullQueries(
    body: Record<string, unknown>,
    authData: A | null,
    transformVersion: number
  ): Record<string, unknown> {
    if (!body.queries) return body
    const queries = body.queries as { version?: unknown; patch?: unknown }
    if (!Array.isArray(queries.patch)) return body
    const patch = resolveQueryPatch(
      queries.patch,
      this.config.queries,
      authData,
      transformVersion,
      requestError
    )
    return { ...body, queries: { ...queries, patch } }
  }

  async handlePull(request: Request): Promise<Response> {
    try {
      this.#assertAccepting()
      const { authData, claims } = await this.#auth(request)
      const input = await requestObject(request)
      return await this.#runQueued('pull', async () => {
        this.#assertAvailable()
        const transformVersion =
          typeof this.config.queryTransformVersion === 'function'
            ? this.config.queryTransformVersion(authData)
            : (this.config.queryTransformVersion ?? 0)
        if (!Number.isSafeInteger(transformVersion) || transformVersion < 0) {
          throw new TypeError('queryTransformVersion must be a non-negative safe integer')
        }
        let body = this.#resolvePullQueries(input, authData, transformVersion)
        body = { ...body, _serverQueryTransformVersion: transformVersion }
        const response = await this.#writeTransaction('pull', () =>
          engine_handle_query_pull(
            this.#engineDb,
            this.config.schema,
            this.#retainChanges,
            body,
            claims.userID
          )
        )
        return json(response)
      })
    } catch (error) {
      return json({ error: errorMessage(error) }, statusOf(error))
    }
  }

  async handlePush(request: Request): Promise<Response> {
    try {
      this.#assertAccepting()
      const { claims } = await this.#auth(request)
      const body = await requestObject(request)
      return await this.#runQueued('push', async () => {
        this.#assertAvailable()
        await this.#reach('before_mutation')
        return json(await this.#runExecutorPush(body, claims, true))
      })
    } catch (error) {
      return json({ error: errorMessage(error) }, statusOf(error))
    }
  }

  fetch(request: Request): Promise<Response> {
    const pathname = new URL(request.url).pathname
    if (pathname === '/pull' && request.method === 'POST') {
      return this.handlePull(request)
    }
    if (pathname === '/push' && request.method === 'POST') {
      return this.handlePush(request)
    }
    return Promise.resolve(json({ error: 'not found' }, 404))
  }

  async query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params: readonly unknown[] = []
  ): Promise<Row[]> {
    this.#assertAccepting()
    return await this.#runQueued('query', () => {
      this.#assertAvailable()
      return this.#directSql.query<Row>(sql, params)
    })
  }

  async exec(
    sql: string,
    params: readonly unknown[] = [],
    metadata?: SqlStatementMetadata
  ): Promise<ExecResult> {
    this.#assertAccepting()
    return await this.#runQueued('exec', async () => {
      const result = await this.#writeTransaction('direct', async () => {
        const writeSet = await beginWriteSetCapture(
          this.config.schema,
          this.#mutatorSql,
          'sqlite'
        )
        const value = await writeSet.transaction.exec(sql, params, metadata)
        await writeSet.commit()
        return value
      })
      this.#notifyDataChanged()
      return result
    })
  }

  subscribe(listener: () => void): () => void {
    this.#assertAccepting()
    this.#listeners.add(listener)
    return () => this.#listeners.delete(listener)
  }

  #notifyDataChanged(): void {
    try {
      this.config.onDataChanged?.()
    } catch (error) {
      console.error('browser sync data-changed callback failed', error)
    }
    for (const listener of this.#listeners) {
      try {
        listener()
      } catch (error) {
        console.error('browser sync data-changed listener failed', error)
      }
    }
  }

  async #closeResources(): Promise<void> {
    this.#listeners.clear()
    if (this.db.open) this.db.close()
    await this.snapshots.close()
  }

  async #finishClose(): Promise<void> {
    await this.#queue.drain()
    this.#closed = true
    await this.#closeResources()
  }

  close(): Promise<void> {
    this.#closePromise ??= this.#finishClose()
    return this.#closePromise
  }
}

export function createBrowserSyncHostInternal<S extends Schema, A extends AuthData>(
  config: BrowserSyncHostConfig<S, A>,
  hooks?: BrowserHostTestHooks
): Promise<BrowserSyncHost<S>> {
  return BrowserSyncHostImpl.create(config, hooks)
}

export function createBrowserSyncHost<S extends Schema, A extends AuthData>(
  config: BrowserSyncHostConfig<S, A>
): Promise<BrowserSyncHost<S>> {
  return createBrowserSyncHostInternal(config)
}
