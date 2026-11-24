import { dequal } from 'dequal'

import { diffPullQueries, pullQueryPatch, type PullState } from './pullQueries.js'
import { EMPTY_ROWS, RowTable, type Held } from './rows.js'
import { NativeSync, type SyncOptions } from './sync.js'
import { NativeView, queryTTL } from './view.js'

import type {
  ClientState,
  DesiredQuery,
  Engine,
  EngineRequest,
  Format,
  JSONValue,
  MaterializedView,
  Mutation,
  MutationPromise,
  MutationResultDetails,
  NativeMutate,
  Mutator,
  MutateRequest,
  MutatorRequestCreator,
  Mutators,
  PullResponse,
  Query,
  QueryError,
  QueryResultType,
  Schema,
  TableMutations,
  Transaction,
  Transport,
  TTL,
  WriteKind,
} from './types.js'

export type NativeZeroOptions<M extends Mutators = Mutators> = {
  engine: Engine
  schema: Schema
  mutators: M
  transport?: Transport
  requestTimeoutMs?: number
  onError?: (error: unknown) => void
  context?: unknown
}

type Subscription = {
  format?: Format
  listener: (value: unknown, resultType: QueryResultType, error?: QueryError) => void
  onError?: (error: QueryError) => void
  active: boolean
  // the last result delivered and its interned rows, which the next patch indexes
  value?: unknown
  held: Held
}

// the engine answers only watches whose result, result type or error changed. a
// list result already delivered comes as a patch: each entry is the index of an
// unchanged row in the previous result or the new row itself.
type Published = {
  results: (
    | { id: number; failure: string }
    | { id: number; value: JSONValue; resultType: QueryResultType; error?: string }
    | {
        id: number
        patch: (number | JSONValue)[]
        resultType: QueryResultType
        error?: string
      }
  )[]
}

// capture inputs before a queued operation can observe a caller's edits.
function snapshot<T>(value: T): T
function snapshot(value: unknown, depth?: number): unknown
function snapshot(value: unknown, depth = 0): unknown {
  if (depth > 128) throw new TypeError('JSON nesting exceeds 128 levels')
  if (typeof value === 'number' && !Number.isFinite(value))
    throw new TypeError('Native Zero values must contain finite numbers')
  if (Array.isArray(value))
    return Array.from(value, (item) => snapshot(item ?? null, depth + 1))
  if (value !== null && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value)
        .filter(([, item]) => item !== undefined)
        .map(([key, item]) => [key, snapshot(item, depth + 1)])
    )
  }
  return value
}

type RegisteredCreator = MutatorRequestCreator & {
  fn(options: { tx: Transaction; ctx: unknown; args: JSONValue | undefined }): unknown
}

function isRequestCreator(value: unknown): value is RegisteredCreator {
  return (
    typeof value === 'function' &&
    'mutatorName' in value &&
    typeof value.mutatorName === 'string' &&
    'fn' in value &&
    typeof value.fn === 'function'
  )
}

function isMutator(value: Mutators[string]): value is Mutator {
  return typeof value === 'function'
}

function hasRelationships(format?: Format): boolean {
  if (!format?.relationships) return false
  for (const _ in format.relationships) return true
  return false
}

function queryResult(value: JSONValue, format?: Format): unknown {
  if (format?.singular && value === null) return undefined
  if (!hasRelationships(format)) return value
  const relationships = format!.relationships!
  if (Array.isArray(value)) {
    const rowFormat: Format = { relationships }
    const out = new Array<unknown>(value.length)
    for (let i = 0; i < value.length; i++) out[i] = queryResult(value[i]!, rowFormat)
    return out
  }
  if (value && typeof value === 'object') {
    const result: Record<string, unknown> = { ...value }
    for (const name in relationships) {
      if (Object.hasOwn(value, name)) {
        result[name] = queryResult(Reflect.get(value, name), relationships[name])
      }
    }
    return result
  }
  return value
}

function sameRows(previous: unknown, next: unknown): boolean {
  if (!Array.isArray(previous) || !Array.isArray(next)) return false
  if (previous.length !== next.length) return false
  for (let i = 0; i < next.length; i++) if (previous[i] !== next[i]) return false
  return true
}

export class NativeZero<M extends Mutators = Mutators> {
  private tail: Promise<unknown> = Promise.resolve()
  private syncTail: Promise<unknown> = Promise.resolve()
  private closed = false
  private closing?: Promise<void>
  private deleting?: Promise<void>
  private abort = new AbortController()
  private subscriptions = new Map<number, Subscription>()
  private nextWatchID = 0
  private mutators = new Map<
    string,
    (tx: Transaction, args: JSONValue | undefined) => unknown
  >()
  private registeredCreators = new WeakSet<object>()
  private views = new Set<NativeView<unknown>>()
  private rows = new RowTable()
  private identity?: string
  private desiredViews = new Map<
    string,
    {
      desired: DesiredQuery
      count: number
      ttl: number
      timer?: ReturnType<typeof setTimeout>
    }
  >()
  private manualQueries?: readonly DesiredQuery[]
  private appliedQueries?: Pick<ClientState, 'queries' | 'queryVersion'>
  private managesQueries = false
  private queryRefreshQueued = false
  private sync?: NativeSync
  private serverResults = new Map<number, (result: MutationResultDetails) => void>()
  private requestTimeoutMs: number
  readonly mutate: NativeMutate<M>

  constructor(private options: NativeZeroOptions<M>) {
    this.requestTimeoutMs = options.requestTimeoutMs ?? 60_000
    if (!Number.isFinite(this.requestTimeoutMs) || this.requestTimeoutMs <= 0)
      throw new Error('Native Zero request timeout must be positive and finite')
    this.mutate = this.installMutators(options.mutators)
  }

  private installMutators(mutators: M): NativeMutate<M>
  private installMutators(mutators: Mutators): unknown {
    const register = (mutators: Mutators, prefix: string): Record<string, unknown> => {
      const functions: Record<string, unknown> = Object.create(null)
      const registry = mutators['~'] === 'MutatorRegistry'
      for (const [name, value] of Object.entries(mutators)) {
        if (registry && name === '~') continue
        const creator = isRequestCreator(value) ? value : undefined
        const key = creator?.mutatorName ?? prefix + name
        if (isMutator(value)) {
          if (this.mutators.has(key))
            throw new Error(`Duplicate native Zero mutator: ${key}`)
          if (creator) {
            const { fn } = creator
            this.mutators.set(key, (tx, args) =>
              fn({ tx, ctx: this.options.context, args })
            )
            this.registeredCreators.add(creator)
          } else this.mutators.set(key, value)
          functions[name] = (args?: JSONValue) => this.issueMutation(key, args)
        } else if (value && typeof value === 'object')
          functions[name] = register(value, prefix + name + '.')
        else throw new Error('Invalid native Zero mutator: ' + key)
      }
      return functions
    }
    const functions = register(mutators, '')
    const mutate = (request: string | MutateRequest, args?: JSONValue) => {
      if (typeof request === 'string') return this.issueMutation(request, args)
      if (!this.registeredCreators.has(request.mutator)) {
        throw new Error(
          `Mutator "${request.mutator?.mutatorName}" is not registered. Mutators must be registered with the Zero constructor before use.`
        )
      }
      return this.issueMutation(request.mutator.mutatorName, request.args)
    }
    for (const [name, value] of Object.entries(functions))
      Object.defineProperty(mutate, name, { value, enumerable: true })
    return mutate
  }

  private enqueue<T>(operation: () => Promise<T>): Promise<T> {
    if (this.closed) return Promise.reject(new Error('Native Zero client is closed'))
    const result = this.tail.then(operation)
    this.tail = result.catch(() => {})
    return result
  }

  private synchronize(operation: () => Promise<void>): Promise<void> {
    if (this.closed) return Promise.reject(new Error('Native Zero client is closed'))
    const result = this.syncTail.then(operation)
    this.syncTail = result.catch(() => {})
    return result
  }

  private async request<T>(request: EngineRequest): Promise<T> {
    try {
      return (await this.options.engine.execute(request)) as T
    } catch (error) {
      let activeQueries = this.desiredViews.size
      for (const query of this.manualQueries ?? []) {
        if (!this.desiredViews.has(query.hash)) activeQueries++
      }
      const message = error instanceof Error ? error.message : String(error)
      throw new Error(
        `${message} (op=${request.op}, activeWatches=${this.subscriptions.size}, activeQueries=${activeQueries})`,
        { cause: error }
      )
    }
  }

  private async requestState(
    request: EngineRequest,
    known = this.appliedQueries
  ): Promise<ClientState> {
    const { queries, ...state } = await this.request<
      Omit<ClientState, 'queries'> & Partial<Pick<ClientState, 'queries'>>
    >({ ...request, queryVersion: known?.queryVersion })
    if (queries !== undefined) return { ...state, queries }
    if (!known || known.queryVersion !== state.queryVersion)
      throw new Error('Native Zero omitted queries for an unknown version')
    return { ...state, queries: known.queries }
  }

  private query<T>(query: Query, transaction?: string): Promise<T>
  private async query(query: Query, transaction?: string): Promise<unknown> {
    return queryResult(
      await this.request<JSONValue>({
        op: 'query',
        ast: query.ast,
        format: query.format,
        transaction,
      }),
      query.format
    )
  }

  run<T = unknown>(query: Query): Promise<T> {
    return this.enqueue(() => this.query<T>(query))
  }

  get clientID(): string {
    if (!this.identity)
      throw new Error('Await NativeZero.ready() before reading clientID')
    return this.identity
  }

  get context(): unknown {
    return this.options.context
  }

  async ready(): Promise<this> {
    await this.state()
    return this
  }

  state(): Promise<ClientState> {
    return this.enqueue(async () => {
      const state = await this.request<ClientState>({ op: 'state' })
      this.identity = state.clientID
      return state
    })
  }

  pending(): Promise<Mutation[]> {
    return this.enqueue(() => this.request({ op: 'pending' }))
  }

  startSync(options: SyncOptions = {}): NativeSync {
    if (this.closed) throw new Error('Native Zero client is closed')
    if (!this.options.transport)
      throw new Error('Native Zero transport is not configured')
    this.sync?.stop()
    this.sync = new NativeSync(this, options, (error) => this.report(error))
    return this.sync
  }

  private async withSignal<T>(
    signal: AbortSignal | undefined,
    operation: (signal: AbortSignal) => Promise<T>
  ): Promise<T> {
    if (signal?.aborted || this.abort.signal.aborted)
      throw new Error('Native Zero sync stopped')
    const controller = new AbortController()
    let rejectAbort!: (error: Error) => void
    const cancelled = new Promise<never>((_resolve, reject) => {
      rejectAbort = reject
    })
    const abort = () => {
      rejectAbort(new Error('Native Zero sync stopped'))
      controller.abort()
    }
    const timeout = setTimeout(() => {
      rejectAbort(new Error('Native Zero request timed out'))
      controller.abort()
    }, this.requestTimeoutMs)
    signal?.addEventListener('abort', abort)
    this.abort.signal.addEventListener('abort', abort)
    try {
      return await Promise.race([operation(controller.signal), cancelled])
    } finally {
      clearTimeout(timeout)
      signal?.removeEventListener('abort', abort)
      this.abort.signal.removeEventListener('abort', abort)
    }
  }

  private async invoke(
    mutation: Mutation,
    transaction: string,
    reason: Transaction['reason']
  ) {
    const mutator = this.mutators.get(mutation.name)
    if (!mutator) {
      if (reason === 'rebase') {
        this.report(new Error(`Unknown native Zero mutator: ${mutation.name}`))
        return undefined
      }
      throw new Error(`Unknown native Zero mutator: ${mutation.name}`)
    }
    let active = true
    let operations: Promise<unknown> = Promise.resolve()
    const failures: { observed: boolean; failed: boolean; error?: unknown }[] = []
    const schedule = <T>(operation: () => Promise<T>): Promise<T> => {
      if (!active) return Promise.reject(new Error('Native Zero transaction is closed'))
      const result = operations.then(operation)
      const status: (typeof failures)[number] = { observed: false, failed: false }
      failures.push(status)
      operations = result.catch((error) => {
        status.failed = true
        status.error = error
      })
      return {
        [Symbol.toStringTag]: 'Promise',
        then(onfulfilled, onrejected) {
          status.observed = true
          return result.then(onfulfilled, onrejected)
        },
        catch(onrejected) {
          status.observed = true
          return result.catch(onrejected)
        },
        finally(onfinally) {
          status.observed = true
          return result.finally(onfinally)
        },
      }
    }
    const write = (kind: WriteKind, table: string) => (value: object) =>
      schedule(async () => {
        await this.request({ op: 'write', kind, table, value, transaction })
      })
    const tx: Transaction = {
      clientID: mutation.clientID,
      mutationID: mutation.id,
      location: 'client',
      reason,
      run: <T>(query: Query) => schedule(() => this.query<T>(query, transaction)),
      mutate: Object.fromEntries(
        Object.keys(this.options.schema.tables).map((table) => [
          table,
          {
            insert: write('insert', table),
            upsert: write('upsert', table),
            update: write('update', table),
            delete: write('delete', table),
          } satisfies TableMutations,
        ])
      ),
    }
    try {
      const result = await mutator(tx, mutation.args)
      active = false
      await operations
      const unobserved = failures.find((status) => status.failed && !status.observed)
      if (unobserved) throw unobserved.error
      return result
    } finally {
      active = false
      await operations
    }
  }

  private issueMutation(name: string, args?: JSONValue): MutationPromise {
    let settle!: (result: MutationResultDetails) => void
    const server = new Promise<MutationResultDetails>((resolve) => {
      settle = resolve
    })
    const operation = this.executeMutation(name, args, (mutation) => {
      if (this.closed)
        settle({
          type: 'error',
          error: { type: 'zero', message: 'Native Zero client is closed' },
        })
      else this.serverResults.set(mutation.id, settle)
    })
    const client = operation.then<MutationResultDetails, MutationResultDetails>(
      () => ({ type: 'success' }),
      (error) => {
        const result: MutationResultDetails = {
          type: 'error',
          error: {
            type: 'app',
            message: error instanceof Error ? error.message : String(error),
          },
        }
        settle(result)
        return result
      }
    )
    return Object.assign(operation, { client, server })
  }

  private async executeMutation(
    name: string,
    args: JSONValue | undefined,
    committed: (mutation: Mutation) => void
  ): Promise<unknown> {
    const request = {
      op: 'beginMutation',
      name,
      args: snapshot(args),
      timestamp: Date.now(),
    }
    return this.enqueue(async () => {
      if (!this.mutators.has(name))
        throw new Error(`Unknown native Zero mutator: ${name}`)
      const { transaction, mutation } = await this.request<{
        transaction: string
        mutation: Mutation
      }>(request)
      let result: unknown
      await this.publishThenCommit(
        transaction,
        async () => {
          result = await this.invoke(mutation, transaction, 'optimistic')
        },
        () =>
          this.request({
            op: 'commitMutation',
            transaction,
            queryVersion: this.appliedQueries?.queryVersion,
          })
      )
      committed(mutation)
      this.sync?.request()
      return result
    })
  }

  private async applyPull(response: PullResponse | string, state: ClientState) {
    const { transaction, pending, changed, lastMutationID } = await this.request<{
      transaction: string
      pending: Mutation[]
      changed: boolean
      lastMutationID: number
    }>({
      op: 'beginPull',
      response,
      baseCookie: state.cookie,
      queryVersion: state.queryVersion,
    })
    const committed = await this.publishThenCommit(
      transaction,
      async () => {
        for (const mutation of pending) await this.invoke(mutation, transaction, 'rebase')
      },
      () => this.requestState({ op: 'commitPull', transaction }, state),
      changed
    )
    for (const [id, settle] of this.serverResults) {
      if (id <= lastMutationID) {
        settle({ type: 'success' })
        this.serverResults.delete(id)
      }
    }
    return committed
  }

  // listeners get the transaction's rows before its commit writes them out; a failed
  // commit rolls back and republishes the rows that remain.
  private async publishThenCommit<T>(
    transaction: string,
    operation: () => Promise<void>,
    commit: () => Promise<T>,
    changed = true
  ) {
    let published = false
    try {
      await operation()
      if (changed) {
        published = true
        await this.publish(transaction)
      }
      return await commit()
    } catch (error) {
      await this.request({ op: 'rollback', transaction })
      if (published) await this.publish().catch((failure) => this.report(failure))
      throw error
    }
  }

  pull(queries?: readonly DesiredQuery[], signal?: AbortSignal): Promise<void> {
    const captured = snapshot(queries)
    return this.synchronize(async () => {
      const transport = this.options.transport
      if (!transport) throw new Error('Native Zero transport is not configured')
      if (captured !== undefined) {
        if (new Set(captured.map((query) => query.hash)).size !== captured.length)
          throw new Error('desired queries require distinct nonempty hashes')
        this.manualQueries = captured
        this.managesQueries = true
      }
      const state = await this.enqueue(() => {
        const desired = this.managesQueries
          ? new Map((this.manualQueries ?? []).map((query) => [query.hash, query]))
          : undefined
        for (const [hash, view] of this.desiredViews) {
          const manual = desired!.get(hash)
          if (manual && !dequal(manual, view.desired))
            throw new Error('Native Zero query hash refers to different queries')
          desired!.set(hash, view.desired)
        }
        const queries = desired ? [...desired.values()] : undefined
        if (
          this.appliedQueries &&
          (!queries || dequal(queries, this.appliedQueries.queries))
        )
          return this.requestState({ op: 'state' })
        return this.request<ClientState>({
          op: 'setQueries',
          queries,
        })
      })
      const patch = diffPullQueries(state, this.appliedQueries)
      const pullState: PullState = { ...state, [pullQueryPatch]: patch }
      try {
        const response = await this.withSignal(signal, (linked) =>
          transport.pull(pullState, state.queries, linked)
        )
        const committed = await this.enqueue(() => {
          if (signal?.aborted) throw new Error('Native Zero sync stopped')
          return this.applyPull(response, state)
        })
        const completed = new Set(committed.completedQueries)
        const previouslyCompleted = new Set(state.completedQueries)
        // a server authorization change can retire registrations during a pull.
        // register them again after the cleared replica has committed.
        const invalidated = state.queries.some(
          ({ hash }) =>
            (previouslyCompleted.has(hash) || Object.hasOwn(state.queryErrors, hash)) &&
            !completed.has(hash) &&
            !Object.hasOwn(committed.queryErrors, hash)
        )
        if (invalidated) {
          this.appliedQueries = undefined
          this.refreshQueries()
        } else if (patch) {
          this.appliedQueries = {
            queries: state.queries,
            queryVersion: state.queryVersion,
          }
        }
      } catch (error) {
        // the server may have applied an uncommitted response's query patch.
        // reconcile the complete desired set on the next pull.
        this.appliedQueries = undefined
        throw error
      }
    })
  }

  push(signal?: AbortSignal): Promise<void> {
    return this.synchronize(async () => {
      const transport = this.options.transport
      if (!transport) throw new Error('Native Zero transport is not configured')
      const { state, pending } = await this.enqueue(async () => ({
        state: await this.requestState({ op: 'state' }),
        pending: await this.request<Mutation[]>({ op: 'pending' }),
      }))
      if (state.recoveryRequired !== null)
        throw new Error('CLIENT_STATE_NOT_FOUND: pending writes require reconciliation')
      if (pending.length) {
        const outcomes = await this.withSignal(signal, (linked) =>
          transport.push(state, pending, linked)
        )
        if (outcomes) {
          for (const outcome of outcomes) {
            this.serverResults.get(outcome.id)?.(outcome.result)
            this.serverResults.delete(outcome.id)
          }
          const failed = outcomes.find((outcome) => outcome.result.type === 'error')
          if (failed?.result.type === 'error')
            throw new Error(`Orez mutation failed: ${failed.result.error.message}`)
        }
      }
    })
  }

  subscribe(query: Query, listener: (value: unknown) => void): () => void {
    if (this.closed) throw new Error('Native Zero client is closed')
    const id = ++this.nextWatchID
    const { ast, format } = snapshot({ ast: query.ast, format: query.format })
    this.watch(
      id,
      { format, listener, active: true, held: [] },
      { op: 'watch', id, ast, format }
    )
    return () => this.unwatch(id)
  }

  private watch(id: number, subscription: Subscription, request: EngineRequest) {
    this.subscriptions.set(id, subscription)
    void this.enqueue(async () =>
      this.deliver(await this.request<Published>(request))
    ).catch((error) => this.report(error))
  }

  private unwatch(id: number) {
    const subscription = this.subscriptions.get(id)
    if (!subscription) return
    subscription.active = false
    this.subscriptions.delete(id)
    this.rows.release(subscription.held)
    subscription.held = []
    if (!this.closed)
      void this.enqueue(() => this.request({ op: 'unwatch', id })).catch((error) =>
        this.report(error)
      )
  }

  materialize<T = unknown>(
    query: Query,
    options?: { hash?: string; ttl?: TTL }
  ): MaterializedView<T>
  materialize(
    query: Query,
    options: { hash?: string; ttl?: TTL } = {}
  ): MaterializedView<unknown> {
    if (this.closed) throw new Error('Native Zero client is closed')
    const hash = options.hash ?? query.hash?.()
    if (!hash) throw new Error('Native Zero materialization requires a query hash')
    let ttl = queryTTL(options.ttl)
    const captured = snapshot({ ast: query.ast, format: query.format })
    const desired: DesiredQuery = { hash, ast: captured.ast }
    const existing = this.desiredViews.get(hash)
    if (existing && !dequal(existing.desired, desired))
      throw new Error('Native Zero query hash refers to different queries')
    this.managesQueries = true
    if (existing?.timer !== undefined) clearTimeout(existing.timer)
    this.desiredViews.set(hash, {
      desired,
      count: (existing?.count ?? 0) + 1,
      ttl: existing && existing.count > 0 ? existing.ttl : 0,
    })
    const id = ++this.nextWatchID
    const subscription: Subscription = {
      format: captured.format,
      active: true,
      held: [],
      listener: (data, resultType, error) => view.update(data, resultType, error),
      onError: (error) => view.update(view.data, 'error', error),
    }
    const view = new NativeView<unknown>(
      captured.format?.singular ? undefined : EMPTY_ROWS,
      () => {
        this.unwatch(id)
        this.views.delete(view)
        const registered = this.desiredViews.get(hash)!
        registered.ttl = Math.max(registered.ttl, ttl)
        if (--registered.count === 0) {
          if (!this.closed && registered.ttl > 0) {
            registered.timer = setTimeout(() => {
              this.desiredViews.delete(hash)
              this.refreshQueries()
            }, registered.ttl)
          } else {
            this.desiredViews.delete(hash)
            this.refreshQueries()
          }
        }
      },
      (error) => this.report(error),
      (value) => {
        ttl = queryTTL(value)
      }
    )
    this.views.add(view)
    this.watch(id, subscription, {
      op: 'watch',
      id,
      ast: captured.ast,
      format: captured.format,
      hash,
    })
    if (!existing) this.refreshQueries()
    return view
  }

  private refreshQueries() {
    if (this.sync) {
      this.sync.request()
      return
    }
    if (this.closed || !this.options.transport || this.queryRefreshQueued) return
    this.queryRefreshQueued = true
    queueMicrotask(() => {
      this.queryRefreshQueued = false
      if (!this.closed) void this.pull().catch((error) => this.report(error))
    })
  }

  private report(error: unknown) {
    queueMicrotask(() => {
      if (this.options.onError) this.options.onError(error)
      else globalThis.console.error(error)
    })
  }

  private async publish(transaction?: string) {
    if (this.subscriptions.size)
      this.deliver(await this.request<Published>({ op: 'publish', transaction }))
  }

  private deliver({ results }: Published) {
    for (const result of results) {
      const subscription = this.subscriptions.get(result.id)
      if (!subscription?.active || this.closed) continue
      try {
        if ('failure' in result) throw new Error(result.failure)
        const error: QueryError | undefined =
          result.error === undefined ? undefined : { error: 'app', message: result.error }
        const next =
          'patch' in result
            ? this.rows.patch(
                result.patch,
                Array.isArray(subscription.value) ? subscription.held : undefined,
                subscription.format
              )
            : this.rows.result(result.value, subscription.format)
        this.rows.release(subscription.held)
        subscription.held = next.held
        // a result whose rows are all unchanged keeps its list
        if (!sameRows(subscription.value, next.value)) subscription.value = next.value
        subscription.listener(subscription.value, result.resultType, error)
      } catch (error) {
        if (subscription.onError)
          subscription.onError({
            error: 'app',
            message: error instanceof Error ? error.message : String(error),
          })
        else this.report(error)
      }
    }
  }

  close(): Promise<void> {
    return this.shutdown(false)
  }

  delete(): Promise<void> {
    if (this.deleting) return this.deleting
    if (this.closed)
      return Promise.reject(new Error('Native Zero client is already closed'))
    return this.shutdown(true)
  }

  private shutdown(remove: boolean): Promise<void> {
    if (this.closing) return this.closing
    this.closed = true
    for (const settle of this.serverResults.values())
      settle({
        type: 'error',
        error: { type: 'zero', message: 'Native Zero client is closed' },
      })
    this.serverResults.clear()
    this.sync?.stop()
    this.abort.abort()
    for (const view of [...this.views]) view.destroy()
    for (const registration of this.desiredViews.values()) {
      if (registration.timer !== undefined) clearTimeout(registration.timer)
    }
    this.desiredViews.clear()
    this.subscriptions.clear()
    this.rows.clear()
    const operation = this.tail.then(async () => {
      if (remove) await this.request({ op: 'delete' })
    })
    const close = () => this.options.engine.close()
    this.closing = operation.then(close, close)
    if (!remove) return this.closing
    this.deleting = operation.then(
      async () => {
        await this.closing
      },
      async (error) => {
        await this.closing
        throw error
      }
    )
    return this.deleting
  }
}
