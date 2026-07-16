import createSqliteModule from 'bedrock-sqlite/browser'
import { createPostCommitEffects } from 'orez-sync-cf-host/post-commit'

import {
  engine_assemble_push_response,
  engine_compile_query,
  engine_finalize,
  engine_handle_pull,
  engine_handle_query_pull,
  engine_init_query_schema,
  engine_init_schema,
  engine_preflight,
  engine_prune,
  engine_push_validate,
  engine_record_app_error,
} from './generated/sync_wasm.js'
import initSyncWasm from './generated/sync_wasm.js'
import { IndexedDbSnapshotStore } from './idb-snapshot.js'
import {
  BedrockDirectSql,
  BedrockMutatorSql,
  BedrockSyncDb,
  type BedrockBrowserModule,
} from './sqlite-adapter.js'
import { isMutationApplicationError } from './types.js'

import type {
  ApplicationTransaction,
  BrowserSyncHost,
  BrowserSyncHostConfig,
  JsonValue,
  NormalizedClaims,
  PullCaps,
} from './types.js'
import type { SQLiteExecResult, SqlStatementMetadata } from 'orez-sync-cf-host'

const DEFAULT_CAPS: PullCaps = {
  maxChangeRows: 10_000,
  maxChangeBytes: 2_000_000,
}

type PushMutation = {
  id: string
  clientID: string
  name: string
  args: JsonValue[]
}

type PushPlan =
  | { kind: 'respond'; response: unknown }
  | { kind: 'process'; clientGroupID: string; mutations: PushMutation[] }

type Preflight = { kind: 'applied' } | { kind: 'replay'; expected: string }

type MutationResult = {
  clientID: string
  id: string
  result: Record<string, unknown>
}

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
  syncWasmReady ??= initSyncWasm({ module_or_path: url }).then(() => undefined)
  return syncWasmReady
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

function validateConfig(config: BrowserSyncHostConfig): PullCaps {
  if (!config.storageKey) throw new TypeError('storageKey must not be empty')
  if (!config.mutators) throw new TypeError('mutators are required')
  if (config.queryAware && !config.resolveQuery) {
    throw new TypeError('queryAware requires resolveQuery')
  }
  const caps = { ...DEFAULT_CAPS, ...config.caps }
  for (const [name, value] of Object.entries(caps)) {
    if (!Number.isSafeInteger(value) || value < 1) {
      throw new TypeError(`caps.${name} must be a positive safe integer`)
    }
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
  return caps
}

class OperationQueue {
  #tail = Promise.resolve()

  run<Value>(operation: () => Value | Promise<Value>): Promise<Value> {
    const result = this.#tail.then(operation)
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

class BrowserSyncHostImpl implements BrowserSyncHost {
  readonly #queue = new OperationQueue()
  readonly #listeners = new Set<() => void>()
  readonly #directSql: BedrockDirectSql
  readonly #engineDb: BedrockSyncDb
  readonly #mutatorSql: BedrockMutatorSql
  readonly #retainChanges: string
  #fatalError: Error | undefined
  #closed = false
  #closePromise: Promise<void> | undefined

  private constructor(
    private readonly config: BrowserSyncHostConfig,
    private readonly caps: PullCaps,
    private readonly module: BedrockBrowserModule,
    private readonly db: InstanceType<BedrockBrowserModule['Database']>,
    private readonly snapshots: IndexedDbSnapshotStore,
    private readonly hooks?: BrowserHostTestHooks
  ) {
    this.#directSql = new BedrockDirectSql(db)
    this.#engineDb = new BedrockSyncDb(db)
    this.#mutatorSql = new BedrockMutatorSql(
      this.#directSql,
      (ast, format) => engine_compile_query(config.schema, ast, format),
      config.transactionQueryBudget
    )
    this.#retainChanges = String(config.retainChanges ?? 4_096)
  }

  static async create(
    config: BrowserSyncHostConfig,
    hooks?: BrowserHostTestHooks
  ): Promise<BrowserSyncHostImpl> {
    const caps = validateConfig(config)
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

    const snapshots = new IndexedDbSnapshotStore(config.storageKey)
    await snapshots.restore(module)
    const db = new module.Database('/project.db')
    const journalMode = String(db.pragma('journal_mode = DELETE', { simple: true }))
    if (journalMode.toLowerCase() !== 'delete') {
      db.close()
      await snapshots.close()
      throw new Error(`Bedrock SQLite did not enter DELETE journal mode: ${journalMode}`)
    }
    db.pragma('foreign_keys = ON')

    const host = new BrowserSyncHostImpl(config, caps, module, db, snapshots, hooks)
    try {
      await host.#writeTransaction('bootstrap', async () => {
        config.initialize(host.#directSql)
        engine_init_schema(host.#engineDb, config.schema)
        if (config.queryAware || config.resolveQuery) {
          engine_init_query_schema(host.#engineDb)
        }
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

  async #checkpoint(): Promise<void> {
    try {
      await this.snapshots.checkpoint(this.module)
    } catch (error) {
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
    kind: 'bootstrap' | 'mutation' | 'pull' | 'maintenance' | 'direct',
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
    if (kind === 'mutation') {
      await this.#reach('after_sqlite_commit_before_idb_commit')
    }
    await this.#checkpoint()
    return value
  }

  async #claims(request: Request): Promise<NormalizedClaims> {
    const claims = await this.config.authenticate(request)
    if (!claims || typeof claims.userID !== 'string' || claims.userID.length === 0) {
      throw requestError('missing authentication', 401)
    }
    return claims
  }

  async #runEffectsAfterCommit(
    effects: ReturnType<typeof createPostCommitEffects>
  ): Promise<void> {
    await effects.runAfterCommit((error) => {
      console.error('browser sync deferred effect failed', error)
    })
  }

  #visibility(claims: NormalizedClaims): unknown {
    if (!this.config.visibility) return null
    return {
      rowLocal:
        typeof this.config.visibility.rowLocal === 'function'
          ? this.config.visibility.rowLocal(claims)
          : this.config.visibility.rowLocal,
      filters: Object.keys(this.config.schema.tables).flatMap((table) => {
        const filter = this.config.visibility?.filter(table, claims)
        return filter
          ? [{ table, sql: filter.sql, params: [...(filter.params ?? [])] }]
          : []
      }),
    }
  }

  async #resolvePullQueries(
    body: Record<string, unknown>,
    claims: NormalizedClaims,
    queryAware: boolean,
    transformVersion: number
  ): Promise<Record<string, unknown>> {
    if (!queryAware || !body.queries) return body
    const queries = body.queries as { version?: unknown; patch?: unknown }
    if (!Array.isArray(queries.patch)) return body

    const patch = []
    for (const operation of queries.patch) {
      if (!operation || typeof operation !== 'object') {
        patch.push(operation)
        continue
      }
      const entry = operation as Record<string, unknown>
      if (entry.op !== 'put') {
        patch.push(operation)
        continue
      }
      if (!this.config.resolveQuery || typeof entry.name !== 'string') {
        throw requestError('query put requires a server-resolved named query')
      }
      if (!Array.isArray(entry.args)) {
        throw requestError('named query args must be an array')
      }
      let ast: JsonValue
      try {
        ast = await this.config.resolveQuery(
          entry.name,
          entry.args as JsonValue[],
          claims
        )
      } catch {
        throw requestError(`unknown or unsupported named query: ${entry.name}`)
      }
      patch.push({ op: 'put', hash: entry.hash, ast, transformVersion })
    }
    return { ...body, queries: { ...queries, patch } }
  }

  async handlePull(request: Request): Promise<Response> {
    try {
      this.#assertAccepting()
      const claims = await this.#claims(request)
      const input = await requestObject(request)
      return await this.#queue.run(async () => {
        this.#assertAvailable()
        const queryAware =
          typeof this.config.queryAware === 'function'
            ? this.config.queryAware(claims)
            : (this.config.queryAware ?? Boolean(this.config.resolveQuery))
        const transformVersion = queryAware
          ? typeof this.config.queryTransformVersion === 'function'
            ? this.config.queryTransformVersion(claims)
            : (this.config.queryTransformVersion ?? 0)
          : 0
        if (!Number.isSafeInteger(transformVersion) || transformVersion < 0) {
          throw new TypeError('queryTransformVersion must be a non-negative safe integer')
        }
        let body = await this.#resolvePullQueries(
          input,
          claims,
          queryAware,
          transformVersion
        )
        if (queryAware) {
          body = { ...body, _serverQueryTransformVersion: transformVersion }
        }
        const response = await this.#writeTransaction('pull', () =>
          queryAware
            ? engine_handle_query_pull(
                this.#engineDb,
                this.config.schema,
                this.#retainChanges,
                body,
                claims.userID
              )
            : engine_handle_pull(
                this.#engineDb,
                this.config.schema,
                this.#visibility(claims),
                this.caps,
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
      const claims = await this.#claims(request)
      const body = await requestObject(request)
      return await this.#queue.run(async () => {
        this.#assertAvailable()
        const plan = engine_push_validate(body) as PushPlan
        if (plan.kind === 'respond') return json(plan.response)

        const results: MutationResult[] = []
        let changed = false
        for (const mutation of plan.mutations) {
          const effects = createPostCommitEffects()
          try {
            await this.#reach('before_mutation')
            const preflight = await this.#writeTransaction('mutation', async () => {
              effects.beginAttempt()
              const decision = engine_preflight(
                this.#engineDb,
                plan.clientGroupID,
                mutation.clientID,
                mutation.id,
                claims.userID
              ) as Preflight
              if (decision.kind === 'replay') return decision

              const mutator = this.config.mutators[mutation.name]
              if (!mutator) throw new Error(`unknown mutator: ${mutation.name}`)
              await mutator(this.#mutatorSql, mutation.args[0] ?? null, {
                claims,
                clientID: mutation.clientID,
                mutationID: mutation.id,
                defer: effects.defer,
              })
              engine_finalize(
                this.#engineDb,
                plan.clientGroupID,
                mutation.clientID,
                mutation.id
              )
              await this.#reach('after_app_write_before_sqlite_commit')
              return decision
            })

            if (preflight.kind === 'replay') {
              results.push({
                clientID: mutation.clientID,
                id: mutation.id,
                result: {
                  error: 'alreadyProcessed',
                  details: `Ignoring mutation from ${mutation.clientID} with ID ${mutation.id} as it was already processed. Expected: ${preflight.expected}`,
                },
              })
              continue
            }

            changed = true
            results.push({ clientID: mutation.clientID, id: mutation.id, result: {} })
            await this.#runEffectsAfterCommit(effects)
          } catch (error) {
            if (!isMutationApplicationError(error)) throw error
            await this.#writeTransaction('mutation', () =>
              engine_record_app_error(
                this.#engineDb,
                plan.clientGroupID,
                mutation.clientID,
                mutation.id,
                claims.userID
              )
            )
            changed = true
            results.push({
              clientID: mutation.clientID,
              id: mutation.id,
              result: {
                error: 'app',
                message: error.message,
                details: error.details,
              },
            })
          }
        }

        if (plan.mutations.length > 0) {
          await this.#writeTransaction('maintenance', () =>
            engine_prune(this.#engineDb, this.#retainChanges)
          )
        }
        if (changed) this.#notifyDataChanged()
        await this.#reach('after_idb_commit_before_response')
        return json(engine_assemble_push_response(results))
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
    return await this.#queue.run(() => {
      this.#assertAvailable()
      return this.#directSql.query<Row>(sql, params)
    })
  }

  async exec(
    sql: string,
    params: readonly unknown[] = [],
    metadata?: SqlStatementMetadata
  ): Promise<SQLiteExecResult> {
    this.#assertAccepting()
    return await this.#queue.run(async () => {
      const result = await this.#writeTransaction('direct', () =>
        this.#directSql.exec(sql, params, metadata)
      )
      this.#notifyDataChanged()
      return result
    })
  }

  async transaction<Value>(work: ApplicationTransaction<Value>): Promise<Value> {
    this.#assertAccepting()
    const effects = createPostCommitEffects()
    return await this.#queue.run(async () => {
      effects.beginAttempt()
      const result = await this.#writeTransaction('direct', () =>
        work(this.#mutatorSql, { defer: effects.defer })
      )
      this.#notifyDataChanged()
      await this.#runEffectsAfterCommit(effects)
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

export function createBrowserSyncHostInternal(
  config: BrowserSyncHostConfig,
  hooks?: BrowserHostTestHooks
): Promise<BrowserSyncHost> {
  return BrowserSyncHostImpl.create(config, hooks)
}

export function createBrowserSyncHost(
  config: BrowserSyncHostConfig
): Promise<BrowserSyncHost> {
  return createBrowserSyncHostInternal(config)
}
