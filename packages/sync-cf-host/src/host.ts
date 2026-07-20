import { DurableObject } from 'cloudflare:workers'
import { createSyncExecutor } from 'orez-sync-executor/core'

import { validatePullCaps, validateSyncHostConfig } from './config.js'
import { createQueryCompiler } from './query-compiler.js'
import {
  decodeSqlParams,
  SqlStorageDirect,
  SqlStorageMutatorTransaction,
  SqlStorageSyncDb,
} from './sql-storage-adapter.js'
import {
  engine_apply_snapshot_changes,
  engine_apply_snapshot_page,
  engine_apply_upstream,
  engine_begin_snapshot_generation,
  engine_finalize,
  engine_finalize_snapshot_generation,
  engine_handle_pull,
  engine_handle_query_pull,
  engine_init_query_schema,
  engine_init_schema,
  engine_invalidate,
  engine_memory_bytes,
  engine_preflight,
  engine_prune,
  engine_push_validate,
  engine_read_snapshot_progress,
  engine_state,
  engine_version,
} from './wasm.js'
import {
  IngestBreakerError,
  IngestCircuitBreaker,
  retryDelayMs,
  shouldRetryDelegatedPush,
} from './write-safeguards.js'

import type { PullCaps, SyncHostConfig, SyncHostEnv } from './types.js'
import type { Schema } from '@rocicorp/zero'
import type {
  ApplicationDatabase,
  ApplicationTransaction,
  JsonValue,
  NormalizedClaims,
  SyncExecutor,
} from 'orez-sync-executor'

const NAMESPACE_HEADER = 'x-orez-sync-namespace'
const UPSTREAM_PATH_HEADER = 'x-orez-sync-upstream-path'
const DEFAULT_SNAPSHOT_PAGE_ROWS = 2_000
const MIN_SNAPSHOT_PAGE_ROWS = 100
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

type DelegatedMutationResult = { id?: { clientID?: unknown; id?: unknown } }
type DelegatedPushBody = {
  mutations?: DelegatedMutationResult[]
  pushResponse?: unknown
  [key: string]: unknown
}

type EngineState = { watermark: string; floor: string; upstreamWatermark: string }
type UpstreamBatch = {
  watermark: number
  changes: Array<{
    watermark: number
    tableName: string
    op: string
    rowData: Record<string, unknown> | null
    oldData: Record<string, unknown> | null
  }>
}
type ApplyUpstreamResult = {
  watermark: number | string
  applied: number
  caughtUp: boolean
}
type SnapshotProgress = {
  generation: string
  startWatermark: string
  table: string | null
  cursor: string | null
  state: 'paging' | 'catching_up'
  catchupWatermark: string
}
type SnapshotPage = {
  watermark: number
  rows: Record<string, unknown>[]
  nextCursor: string | null
}
type SocketAttachment = { clientID: string }
type FaultPoint =
  | 'push_before_mutation'
  | 'push_after_write_before_commit'
  | 'push_after_commit_before_response'
  | 'pull_during_tx'
  | 'pull_after_commit'
type FaultKind = 'error' | 'quota'

type ForwardedSyncBody = {
  claims: NormalizedClaims
  body: Record<string, unknown>
}

type Counters = {
  pulls: number
  pushes: number
  resets: number
  applicationErrors: number
  invariantFailures: number
  retentionRuns: number
  queryRecompilations: number
  wasmBoundaryCalls: number
  wakeFrames: number
  wakeBatches: number
  externalEffectFailures: number
}

function freshCounters(): Counters {
  return {
    pulls: 0,
    pushes: 0,
    resets: 0,
    applicationErrors: 0,
    invariantFailures: 0,
    retentionRuns: 0,
    queryRecompilations: 0,
    wasmBoundaryCalls: 0,
    wakeFrames: 0,
    wakeBatches: 0,
    externalEffectFailures: 0,
  }
}

function json(value: unknown, status = 200): Response {
  return Response.json(value, {
    status,
    headers: { 'cache-control': 'no-store' },
  })
}

function isStructuredPushFailed(value: unknown): value is Record<string, unknown> {
  if (typeof value !== 'object' || value === null) return false
  const body = value as Record<string, unknown>
  return (
    body.kind === 'PushFailed' &&
    typeof body.origin === 'string' &&
    typeof body.reason === 'string' &&
    typeof body.message === 'string' &&
    Array.isArray(body.mutationIDs)
  )
}

function statusOf(error: unknown): number {
  if (typeof error === 'object' && error !== null && 'status' in error) {
    const status = Number(error.status)
    if (Number.isInteger(status) && status >= 400 && status <= 599) return status
  }
  return 500
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function errorBody(error: unknown): Record<string, unknown> {
  if (error instanceof IngestBreakerError) {
    return {
      error: error.error,
      windowRows: error.windowRows,
      budget: error.budget,
      retryAfterMs: error.retryAfterMs,
    }
  }
  return { error: errorMessage(error) }
}

function requestError(message: string, status = 400): Error & { status: number } {
  return Object.assign(new Error(message), { status })
}

async function requestJson(request: Request): Promise<unknown> {
  try {
    return await request.json()
  } catch {
    throw requestError('invalid JSON request body')
  }
}

async function requestObject(request: Request): Promise<Record<string, unknown>> {
  const value = await requestJson(request)
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw requestError('request body must be a JSON object')
  return value as Record<string, unknown>
}

function routeAfterNamespace(pathname: string): string {
  const [, , ...parts] = pathname.split('/')
  return `/${parts.join('/')}`
}

function jsonBodyRequest(request: Request, headers: Headers, body: unknown): Request {
  headers.delete('content-encoding')
  headers.delete('content-length')
  headers.set('content-type', 'application/json')
  return new Request(request.url, {
    method: request.method,
    headers,
    body: JSON.stringify(body),
  })
}

async function forwardedSyncRequest(
  request: Request
): Promise<{ claims: NormalizedClaims; request: Request }> {
  const value = await requestObject(request)
  const claims = value.claims
  const body = value.body
  const userID =
    claims && typeof claims === 'object' && !Array.isArray(claims)
      ? (claims as Record<string, unknown>).userID
      : null
  if (
    !claims ||
    typeof claims !== 'object' ||
    Array.isArray(claims) ||
    typeof userID !== 'string' ||
    userID.length === 0
  ) {
    throw requestError('missing normalized claims', 401)
  }
  if (!body || typeof body !== 'object' || Array.isArray(body)) {
    throw requestError('request body must be a JSON object')
  }

  const headers = new Headers(request.headers)
  return {
    claims: claims as NormalizedClaims,
    request: jsonBodyRequest(request, headers, body),
  }
}

async function namespaceHash(namespace: string): Promise<string> {
  const bytes = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(namespace))
  return Array.from(new Uint8Array(bytes).slice(0, 8), (byte) =>
    byte.toString(16).padStart(2, '0')
  ).join('')
}

function socketAttachment(socket: WebSocket): SocketAttachment | null {
  const value = socket.deserializeAttachment() as SocketAttachment | null
  return value && typeof value.clientID === 'string' ? value : null
}

// Closing a socket that is already closed/closing throws, and a throw inside a
// hibernatable WebSocket handler aborts the DO. Swallow it: the socket is going
// away regardless.
function socketCloseQuietly(socket: WebSocket, code: number, reason: string): void {
  try {
    socket.close(code, reason)
  } catch {
    // already closing/closed, or workerd rejected the code — nothing to do
  }
}

/**
 * Create the consumer-facing Worker router. Authentication happens here; the
 * Durable Object receives normalized claims inside the binding request body so
 * observability systems cannot record them as request-header metadata.
 */
export function createSyncWorker<Env extends SyncHostEnv, S extends Schema = Schema>(
  config: SyncHostConfig<Env, S>
): ExportedHandler<Env> {
  validateSyncHostConfig(config)
  return {
    async fetch(request, env): Promise<Response> {
      const namespace = config.namespace(request)
      if (!namespace) return new Response('orez sync-cf-host', { status: 200 })

      const route = routeAfterNamespace(new URL(request.url).pathname)
      const isAdmin = route.startsWith('/admin/')
      if (route === '/wake') {
        if (!(await config.authorizeWake(request, env))) {
          return json({ error: 'missing wake capability' }, 401)
        }
      } else if (route === '/notify') {
        if (!(await config.authorizeNotify(request, env))) {
          return json({ error: 'forbidden' }, 403)
        }
      } else if (isAdmin) {
        const authorized = config.authorizeAdmin
          ? await config.authorizeAdmin(request, env)
          : Boolean(env.ADMIN_KEY) && request.headers.get('x-admin-key') === env.ADMIN_KEY
        if (!authorized) return json({ error: 'forbidden' }, 403)
      }

      const headers = new Headers(request.headers)
      headers.delete(NAMESPACE_HEADER)
      headers.delete(UPSTREAM_PATH_HEADER)
      let forwardedBody: ForwardedSyncBody | null = null
      if (!isAdmin && route !== '/wake' && route !== '/notify') {
        const claims = await config.authenticate(request, env)
        if (!claims || typeof claims.userID !== 'string' || claims.userID.length === 0) {
          return json({ error: 'missing authentication' }, 401)
        }
        if (!(await config.authorize(request, claims, namespace, env))) {
          return json({ error: 'forbidden' }, 403)
        }
        if ((route === '/pull' || route === '/push') && request.method === 'POST') {
          try {
            const body = await requestObject(request)
            forwardedBody = { claims, body }
          } catch (error) {
            return json(errorBody(error), statusOf(error))
          }
        }
      }
      headers.set(NAMESPACE_HEADER, await namespaceHash(namespace))
      if (config.upstream) {
        const namespacePath =
          typeof config.upstream.namespacePath === 'function'
            ? config.upstream.namespacePath(namespace)
            : config.upstream.namespacePath
        if (!namespacePath.startsWith('/')) {
          return json({ error: 'upstream namespacePath must be an absolute path' }, 500)
        }
        headers.set(UPSTREAM_PATH_HEADER, namespacePath.replace(/\/$/, ''))
      }

      const forwarded = forwardedBody
        ? jsonBodyRequest(request, headers, forwardedBody)
        : new Request(request, { headers })
      const id = env.SYNC_DO.idFromName(namespace)
      return env.SYNC_DO.get(id).fetch(forwarded)
    },
  }
}

export interface SyncDurableObjectConstructor<Env extends SyncHostEnv> {
  new (ctx: DurableObjectState, env: Env): DurableObject<Env>
}

/** Create the namespace Durable Object class for one bundled consumer config. */
export function createSyncDurableObject<
  Env extends SyncHostEnv,
  S extends Schema = Schema,
>(config: SyncHostConfig<Env, S>): SyncDurableObjectConstructor<Env> {
  validateSyncHostConfig(config)
  const compileQuery = createQueryCompiler(config.schema)
  const defaultRetainChanges = String(config.retainChanges ?? 4_096)
  const caps: PullCaps = validatePullCaps({ ...DEFAULT_CAPS, ...config.caps })
  const idleTeardownMs = config.idleTeardownMs ?? 5_000
  // A CF fan-out wakes every client into an HTTP pull. Give concurrent writer
  // requests a real batching window so a storm burst creates one pull wave.
  const wakeCoalesceMs = config.wakeCoalesceMs ?? 25
  const upstreamIntervalMs = config.upstream?.intervalMs ?? 15_000
  const upstreamLimit = config.upstream?.changeLimit ?? 1_000
  const ingestBudgetRows = config.upstream?.ingestBudgetRows ?? 150_000
  const ingestBudgetWindowMs = config.upstream?.ingestBudgetWindowMs ?? 5 * 60_000
  const ingestBackoffMs = config.upstream?.ingestBackoffMs ?? 1_000
  const ingestMaxBackoffMs = config.upstream?.ingestMaxBackoffMs ?? 60_000
  const delegateMaxAttempts = config.delegatedPushRetry?.maxAttempts ?? 3
  const delegateInitialBackoffMs = config.delegatedPushRetry?.initialBackoffMs ?? 100
  const delegateMaxBackoffMs = config.delegatedPushRetry?.maxBackoffMs ?? 1_000
  const delegateTimeoutMs = config.delegatedPushRetry?.timeoutMs ?? 5_000

  return class SyncDurableObject extends DurableObject<Env> {
    readonly #engineDb: SqlStorageSyncDb
    readonly #directSql: SqlStorageDirect
    readonly #mutatorSql: SqlStorageMutatorTransaction
    readonly #executor: SyncExecutor<S> | null
    #executorBeforeCommitFault: FaultKind | null = null
    #bootID = crypto.randomUUID()
    #lastRequestAt = 0
    #hibernations = 0
    #dropNextPushResponse = false
    #counters = freshCounters()
    #pulling = new Set<string>()
    #wakeOrigins = new Set<string>()
    #wakeRecipients = new Set<WebSocket>()
    #wakePromise: Promise<void> | null = null
    #ingestPromise: Promise<number> | null = null
    #queryPullLocks = new Map<string, Promise<void>>()
    #recordingIngestBillable = false
    #ingestBreaker = new IngestCircuitBreaker({
      budgetRows: ingestBudgetRows,
      windowMs: ingestBudgetWindowMs,
      initialBackoffMs: ingestBackoffMs,
      maxBackoffMs: ingestMaxBackoffMs,
      now: () => Date.now(),
    })

    constructor(ctx: DurableObjectState, env: Env) {
      super(ctx, env)
      const recordRowsWritten = (rows: number) => {
        if (this.#recordingIngestBillable) this.#ingestBreaker.recordBillable(rows)
      }
      this.#engineDb = new SqlStorageSyncDb(ctx.storage.sql, recordRowsWritten)
      this.#directSql = new SqlStorageDirect(ctx.storage.sql, recordRowsWritten)
      this.#mutatorSql = new SqlStorageMutatorTransaction(
        this.#directSql,
        (ast, format) => this.#wasm(() => compileQuery(ast, format)),
        config.transactionQueryBudget
      )
      const database: ApplicationDatabase = {
        dialect: 'sqlite',
        transaction: async <Value>(
          work: (tx: ApplicationTransaction) => Value | Promise<Value>
        ): Promise<Value> =>
          this.ctx.storage.transaction(async () => {
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
            const value = await work(tx)
            if (applicationWrite && this.#executorBeforeCommitFault) {
              const fault = this.#executorBeforeCommitFault
              this.#executorBeforeCommitFault = null
              throw this.#faultError(fault, 'push_after_write_before_commit')
            }
            return value
          }),
        query: (sql, params) => this.#mutatorSql.query(sql, params),
      }
      this.#executor = config.mutators
        ? createSyncExecutor({
            database,
            effects: {
              runBackground: (promise) => this.ctx.waitUntil(promise),
              report: (error) => {
                this.#counters.externalEffectFailures++
                console.error(
                  JSON.stringify({
                    event: 'sync_external_effect_error',
                    hostVersion: config.hostVersion,
                    error: errorMessage(error),
                  })
                )
              },
            },
            mutators: config.mutators,
            schema: config.schema,
          })
        : null
      ctx.blockConcurrencyWhile(async () => {
        ctx.storage.transactionSync(() => {
          config.initialize(this.#directSql)
          this.#directSql.exec(`CREATE TABLE IF NOT EXISTS _zsync_host_control (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
          )`)
          this.#directSql.exec(
            "INSERT OR IGNORE INTO _zsync_host_control (key, value) VALUES ('writerEnabled', '1')"
          )
          const ingestBreakerReason = this.#controlGet('ingestBreakerReason')
          if (
            ingestBreakerReason === 'ingestBudgetExceeded' ||
            ingestBreakerReason === 'ingestCursorStalled'
          ) {
            this.#ingestBreaker.restore(
              ingestBreakerReason,
              Number(this.#controlGet('ingestBreakerRetryAt')),
              Number(this.#controlGet('ingestBreakerTrips'))
            )
          }
          this.#wasm(() => engine_init_schema(this.#engineDb, config.schema))
          if (config.queryAware || config.resolveQuery)
            this.#wasm(() => engine_init_query_schema(this.#engineDb))
        })
      })
    }

    #armUpstreamAlarm(): void {
      if (!config.upstream) return
      this.ctx.waitUntil(
        (async () => {
          if ((await this.ctx.storage.getAlarm()) === null) {
            await this.ctx.storage.setAlarm(Date.now() + upstreamIntervalMs)
          }
        })()
      )
    }

    #wasm<T>(call: () => T): T {
      this.#counters.wasmBoundaryCalls++
      return call()
    }

    async #acquireQueryPullLock(clientGroupID: string): Promise<() => void> {
      const previous = this.#queryPullLocks.get(clientGroupID)
      let release!: () => void
      const current = new Promise<void>((resolve) => {
        release = resolve
      })
      this.#queryPullLocks.set(clientGroupID, current)
      if (previous) await previous
      return () => {
        release()
        if (this.#queryPullLocks.get(clientGroupID) === current) {
          this.#queryPullLocks.delete(clientGroupID)
        }
      }
    }

    #simulateIdleTeardown(now: number): void {
      if (this.#lastRequestAt > 0 && now - this.#lastRequestAt >= idleTeardownMs) {
        this.#bootID = crypto.randomUUID()
        this.#hibernations++
        this.#counters = freshCounters()
        this.#pulling.clear()
        this.#wakeOrigins.clear()
        this.#wakeRecipients.clear()
        this.#wakePromise = null
      }
      this.#lastRequestAt = now
    }

    #visibility(claims: NormalizedClaims): unknown {
      if (!config.visibility || !this.#visibilityEnabled()) return null
      return {
        rowLocal:
          typeof config.visibility.rowLocal === 'function'
            ? config.visibility.rowLocal(claims)
            : config.visibility.rowLocal,
        filters: Object.keys(config.schema.tables).flatMap((table) => {
          const filter = config.visibility?.filter(table, claims)
          return filter
            ? [
                filter.kind === 'expression'
                  ? { kind: 'expression', table, expression: filter.expression }
                  : {
                      kind: 'raw',
                      table,
                      sql: filter.sql,
                      params: [...(filter.params ?? [])],
                    },
              ]
            : []
        }),
      }
    }

    // the admin-set namespace knobs (writer, visibility, query-aware,
    // retention) live in _zsync_host_control, NOT in instance fields: a real
    // eviction recreates this instance, and an in-memory override silently
    // reverting to the config default mid-run turns a query-aware namespace
    // back into a baseline one — the client keeps re-sending its desired-query
    // patch, the host ignores it, and every pull answers {unchanged:true}
    // forever.
    #controlGet(key: string): string | null {
      const row = this.#directSql.query<{ value: string }>(
        'SELECT value FROM _zsync_host_control WHERE key = ?',
        [key]
      )[0]
      return row?.value ?? null
    }

    #controlSet(key: string, value: string): void {
      this.#directSql.exec(
        'INSERT INTO _zsync_host_control (key, value) VALUES (?, ?) ' +
          'ON CONFLICT(key) DO UPDATE SET value = excluded.value',
        [key, value]
      )
    }

    #controlDelete(...keys: string[]): void {
      if (keys.length === 0) return
      this.#directSql.exec(
        `DELETE FROM _zsync_host_control WHERE key IN (${keys.map(() => '?').join(', ')})`,
        keys
      )
    }

    #persistIngestBreaker(): void {
      const status = this.#ingestBreaker.status()
      if (!status.reason || status.retryAt === null) return
      this.#controlSet('ingestBreakerReason', status.reason)
      this.#controlSet('ingestBreakerRetryAt', String(status.retryAt))
      this.#controlSet('ingestBreakerTrips', String(status.consecutiveTrips))
    }

    #recoverIngestBreaker(): void {
      const wasTripped = this.#ingestBreaker.status().reason !== null
      this.#ingestBreaker.recovered()
      if (wasTripped) {
        this.#controlDelete(
          'ingestBreakerReason',
          'ingestBreakerRetryAt',
          'ingestBreakerTrips'
        )
      }
    }

    #writerEnabled(): boolean {
      return this.#controlGet('writerEnabled') === '1'
    }

    #visibilityEnabled(): boolean {
      const value = this.#controlGet('visibilityEnabled')
      return value === null ? (config.visibilityEnabled ?? false) : value === '1'
    }

    #queryAwareOverride(): boolean | null {
      const value = this.#controlGet('queryAwareOverride')
      return value === null ? null : value === '1'
    }

    #retainChanges(): string {
      return this.#controlGet('retainChanges') ?? defaultRetainChanges
    }

    #takeFault(point: FaultPoint): FaultKind | null {
      if (this.#controlGet('faultPoint') !== point) return null
      const kind = this.#controlGet('faultKind')
      this.#directSql.exec(
        "DELETE FROM _zsync_host_control WHERE key IN ('faultPoint', 'faultKind')"
      )
      return kind === 'quota' ? 'quota' : 'error'
    }

    #faultError(kind: FaultKind, point: FaultPoint): Error & { status: number } {
      return requestError(
        `injected ${kind} fault at ${point}`,
        kind === 'quota' ? 507 : 500
      )
    }

    #engineState(): EngineState {
      return this.#wasm(() => engine_state(this.#engineDb)) as EngineState
    }

    #engineStateBestEffort(): EngineState | null {
      try {
        return this.#engineState()
      } catch {
        return null
      }
    }

    #serviceBinding(name = config.upstream?.binding): {
      fetch(input: string | Request, init?: RequestInit): Promise<Response>
    } {
      const value = name
        ? (this.env as unknown as Record<string, unknown>)[name]
        : undefined
      if (!value || typeof (value as { fetch?: unknown }).fetch !== 'function') {
        throw requestError(
          `missing upstream service binding: ${name ?? '(not configured)'}`,
          500
        )
      }
      return value as {
        fetch(input: string | Request, init?: RequestInit): Promise<Response>
      }
    }

    async #upstreamWriteBudgetStatus(): Promise<Response> {
      if (!config.upstream) return json({ error: 'upstream is not configured' }, 404)
      const path = this.#controlGet('upstreamPath')
      if (path === null) return json({ error: 'upstream path is not known yet' }, 409)
      const endpoint = new URL(`${path}/_orez/write-budget`, 'https://upstream.invalid')
      const response = await this.#serviceBinding().fetch(endpoint.toString(), {
        headers: { host: endpoint.host },
      })
      if (!response.ok) {
        return json(
          {
            error: 'upstream write-budget status unavailable',
            upstreamStatus: response.status,
          },
          502
        )
      }
      return json(await response.json())
    }

    #rememberUpstreamPath(request: Request): string | null {
      if (!config.upstream) return null
      const path = request.headers.get(UPSTREAM_PATH_HEADER)
      if (path !== null) {
        this.#controlSet('upstreamPath', path)
        return path
      }
      return this.#controlGet('upstreamPath')
    }

    #tripIngest(
      reason: 'ingestBudgetExceeded' | 'ingestCursorStalled',
      fields: Record<string, unknown>
    ): never {
      try {
        return this.#ingestBreaker.trip(reason)
      } catch (error) {
        this.#persistIngestBreaker()
        const status = this.#ingestBreaker.status()
        console.error(
          JSON.stringify({
            event: 'sync_upstream_ingest_breaker_tripped',
            ...status,
            reason,
            ...fields,
          })
        )
        throw error
      }
    }

    #recordIngestLogicalRows(rows: number): void {
      this.#ingestBreaker.recordLogical(rows)
    }

    #withIngestBilling<T>(fields: Record<string, unknown>, apply: () => T): T {
      this.#recordingIngestBillable = true
      try {
        return apply()
      } catch (error) {
        this.#recordingIngestBillable = false
        const status = this.#ingestBreaker.status()
        // a breaker thrown by the sql adapter crosses rust as an engine error,
        // so the durable breaker state is the authoritative classification.
        if (
          error instanceof IngestBreakerError ||
          (status.tripped && status.reason === 'ingestBudgetExceeded')
        ) {
          this.#persistIngestBreaker()
          console.error(
            JSON.stringify({
              event: 'sync_upstream_ingest_breaker_tripped',
              ...status,
              reason: 'ingestBudgetExceeded',
              ...fields,
            })
          )
        }
        throw error
      } finally {
        this.#recordingIngestBillable = false
      }
    }

    #snapshotProgress(): SnapshotProgress | null {
      return this.#wasm(() =>
        engine_read_snapshot_progress(this.#engineDb)
      ) as SnapshotProgress | null
    }

    #resetSnapshotBillingWindow(): void {
      // every page is an independently committed write unit. metering it in a
      // fresh window keeps a rebuild larger than the breaker ceiling resumable
      // while preserving the ceiling for each transaction.
      this.#ingestBreaker.reopen()
      this.#controlDelete(
        'ingestBreakerReason',
        'ingestBreakerRetryAt',
        'ingestBreakerTrips'
      )
    }

    #snapshotRetryLimit(
      error: unknown,
      limit: number,
      fields: Record<string, unknown>
    ): number {
      const status = statusOf(error)
      if (!(error instanceof IngestBreakerError) && status < 500) throw error
      if (limit <= MIN_SNAPSHOT_PAGE_ROWS) {
        throw Object.assign(
          new Error(
            `snapshot page failed at minimum limit ${MIN_SNAPSHOT_PAGE_ROWS}: ${errorMessage(error)}`
          ),
          { status, cause: error }
        )
      }
      const nextLimit = Math.max(MIN_SNAPSHOT_PAGE_ROWS, Math.floor(limit / 2))
      console.warn(
        JSON.stringify({
          event: 'sync_upstream_snapshot_page_retry',
          ...fields,
          limit,
          nextLimit,
          status,
          error: errorMessage(error),
        })
      )
      return nextLimit
    }

    async #fetchSnapshotPage(
      path: string,
      table: string,
      cursor: string | null,
      limit: number
    ): Promise<SnapshotPage> {
      const endpoint = new URL(`${path}/snapshot`, 'https://upstream.invalid')
      endpoint.searchParams.set('table', table)
      endpoint.searchParams.set('limit', String(limit))
      if (cursor !== null) endpoint.searchParams.set('cursor', cursor)
      const response = await this.#serviceBinding().fetch(endpoint.toString(), {
        headers: { host: endpoint.host },
      })
      if (!response.ok) {
        throw requestError(
          `upstream snapshot page returned ${response.status}`,
          response.status >= 500 ? 502 : response.status
        )
      }
      const page = (await response.json()) as Partial<SnapshotPage>
      if (
        !Number.isSafeInteger(page.watermark) ||
        Number(page.watermark) < 0 ||
        !Array.isArray(page.rows) ||
        (page.nextCursor !== null && typeof page.nextCursor !== 'string')
      ) {
        throw new Error('invalid upstream snapshot page response')
      }
      return page as SnapshotPage
    }

    async #beginSnapshotGeneration(path: string): Promise<{
      progress: SnapshotProgress
      page: SnapshotPage
      pageLimit: number
    }> {
      const table = Object.keys(config.schema.tables).sort()[0]
      if (!table) throw requestError('paged snapshots require a modeled table', 500)
      let pageLimit = DEFAULT_SNAPSHOT_PAGE_ROWS
      let page: SnapshotPage
      for (;;) {
        try {
          page = await this.#fetchSnapshotPage(path, table, null, pageLimit)
          break
        } catch (error) {
          pageLimit = this.#snapshotRetryLimit(error, pageLimit, {
            phase: 'snapshot_page_fetch',
            table,
            cursor: null,
          })
        }
      }
      this.#resetSnapshotBillingWindow()
      const progress = this.#withIngestBilling(
        {
          phase: 'snapshot_begin',
          table,
          startWatermark: page.watermark,
        },
        () =>
          this.ctx.storage.transactionSync(() =>
            this.#wasm(() =>
              engine_begin_snapshot_generation(
                this.#engineDb,
                config.schema,
                String(page.watermark)
              )
            )
          )
      ) as SnapshotProgress
      return { progress, page, pageLimit }
    }

    #ingest(upstreamPath?: string | null, forceSnapshot = false): Promise<number> {
      if (!config.upstream) {
        return forceSnapshot
          ? Promise.reject(requestError('upstream is not configured'))
          : Promise.resolve(0)
      }
      if (this.#ingestPromise) {
        return forceSnapshot
          ? this.#ingestPromise.then(() => this.#ingest(upstreamPath, true))
          : this.#ingestPromise
      }
      const path = upstreamPath ?? this.#controlGet('upstreamPath')
      if (path === null) {
        return forceSnapshot
          ? Promise.reject(requestError('upstream path is not available'))
          : Promise.resolve(0)
      }
      this.#ingestPromise = (async () => {
        let progress = this.#snapshotProgress()
        this.#ingestBreaker.assertReady()
        const startingWatermark = this.#engineState().watermark
        let total = 0
        let pendingPage: SnapshotPage | null = null
        let snapshotPageLimit = DEFAULT_SNAPSHOT_PAGE_ROWS
        let snapshotCompleted = false
        for (;;) {
          if (progress?.state === 'paging') {
            const activeProgress = progress
            const table = activeProgress.table
            if (table === null) {
              throw new Error(
                `snapshot generation ${activeProgress.generation} is paging without a table`
              )
            }
            let page: SnapshotPage | null = pendingPage
            try {
              page ??= await this.#fetchSnapshotPage(
                path,
                table,
                activeProgress.cursor,
                snapshotPageLimit
              )
              const pageToApply = page
              this.#resetSnapshotBillingWindow()
              const nextProgress = this.#withIngestBilling(
                {
                  phase: 'snapshot_page_apply',
                  generation: activeProgress.generation,
                  table,
                  cursor: activeProgress.cursor,
                  pageRows: pageToApply.rows.length,
                  pageLimit: snapshotPageLimit,
                },
                () =>
                  this.ctx.storage.transactionSync(() =>
                    this.#wasm(() =>
                      engine_apply_snapshot_page(
                        this.#engineDb,
                        config.schema,
                        activeProgress.generation,
                        table,
                        pageToApply.rows,
                        pageToApply.nextCursor
                      )
                    )
                  )
              ) as SnapshotProgress
              total += pageToApply.rows.length
              this.#recordIngestLogicalRows(pageToApply.rows.length)
              progress = nextProgress
              pendingPage = null
            } catch (error) {
              snapshotPageLimit = this.#snapshotRetryLimit(error, snapshotPageLimit, {
                phase: page === null ? 'snapshot_page_fetch' : 'snapshot_page_apply',
                generation: activeProgress.generation,
                table,
                cursor: activeProgress.cursor,
              })
              pendingPage = null
            }
            continue
          }

          if (progress?.state === 'catching_up') {
            const activeProgress = progress
            const cursor = activeProgress.catchupWatermark
            const endpoint = new URL(`${path}/changes`, 'https://upstream.invalid')
            endpoint.searchParams.set('since', cursor)
            endpoint.searchParams.set('limit', String(upstreamLimit))
            const response = await this.#serviceBinding().fetch(endpoint.toString(), {
              headers: { host: endpoint.host },
            })
            if (response.status === 410) {
              const begun = await this.#beginSnapshotGeneration(path)
              progress = begun.progress
              pendingPage = begun.page
              snapshotPageLimit = begun.pageLimit
              continue
            }
            if (!response.ok) {
              throw new Error(`upstream snapshot catch-up returned ${response.status}`)
            }
            const batch = (await response.json()) as UpstreamBatch
            if (!Number.isSafeInteger(batch.watermark) || !Array.isArray(batch.changes)) {
              throw new Error('invalid upstream changes response')
            }
            this.#resetSnapshotBillingWindow()
            const result = this.#withIngestBilling(
              {
                phase: 'snapshot_catchup',
                generation: activeProgress.generation,
                cursor,
                batchWatermark: batch.watermark,
                changeRows: batch.changes.length,
              },
              () =>
                this.ctx.storage.transactionSync(() =>
                  this.#wasm(() =>
                    engine_apply_snapshot_changes(
                      this.#engineDb,
                      config.schema,
                      activeProgress.generation,
                      batch
                    )
                  )
                )
            ) as ApplyUpstreamResult
            total += result.applied
            this.#recordIngestLogicalRows(result.applied)
            if (result.caughtUp) {
              this.#resetSnapshotBillingWindow()
              this.#withIngestBilling(
                {
                  phase: 'snapshot_finalize',
                  generation: activeProgress.generation,
                  watermark: result.watermark,
                },
                () =>
                  this.ctx.storage.transactionSync(() =>
                    this.#wasm(() =>
                      engine_finalize_snapshot_generation(
                        this.#engineDb,
                        config.schema,
                        activeProgress.generation,
                        String(result.watermark)
                      )
                    )
                  )
              )
              progress = null
              snapshotCompleted = true
              break
            }
            if (String(result.watermark) === cursor) {
              this.#tripIngest('ingestCursorStalled', {
                phase: 'snapshot_catchup',
                generation: activeProgress.generation,
                cursor,
                batchWatermark: batch.watermark,
                changeRows: batch.changes.length,
                applied: result.applied,
              })
            }
            progress = {
              ...progress,
              catchupWatermark: String(result.watermark),
            }
            continue
          }

          const cursor = this.#engineState().upstreamWatermark
          if (forceSnapshot) {
            forceSnapshot = false
            const begun = await this.#beginSnapshotGeneration(path)
            progress = begun.progress
            pendingPage = begun.page
            snapshotPageLimit = begun.pageLimit
            continue
          }
          const endpoint = new URL(`${path}/changes`, 'https://upstream.invalid')
          endpoint.searchParams.set('watermark', cursor)
          endpoint.searchParams.set('limit', String(upstreamLimit))
          const response = await this.#serviceBinding().fetch(endpoint.toString(), {
            headers: { host: endpoint.host },
          })
          if (response.status === 410) {
            const begun = await this.#beginSnapshotGeneration(path)
            progress = begun.progress
            pendingPage = begun.page
            snapshotPageLimit = begun.pageLimit
            continue
          }
          if (!response.ok) {
            throw new Error(`upstream changes returned ${response.status}`)
          }
          const batch = (await response.json()) as UpstreamBatch
          if (!Number.isSafeInteger(batch.watermark) || !Array.isArray(batch.changes)) {
            throw new Error('invalid upstream changes response')
          }
          const result = this.#withIngestBilling(
            {
              phase: 'changes',
              cursor,
              batchWatermark: batch.watermark,
              changeRows: batch.changes.length,
            },
            () =>
              this.ctx.storage.transactionSync(() =>
                this.#wasm(() =>
                  engine_apply_upstream(this.#engineDb, config.schema, batch)
                )
              )
          ) as ApplyUpstreamResult
          total += result.applied
          this.#recordIngestLogicalRows(result.applied)
          const nextCursor = this.#engineState().upstreamWatermark
          if (batch.changes.length > 0 && String(nextCursor) === String(cursor)) {
            this.#tripIngest('ingestCursorStalled', {
              phase: 'changes',
              cursor,
              batchWatermark: batch.watermark,
              changeRows: batch.changes.length,
              applied: result.applied,
            })
          }
          if (result.caughtUp) break
          // a page can legitimately apply zero rows while still advancing the
          // watermark: the engine consumes changes for tables this host does not
          // model (subset replica) without materializing them. only a page that
          // neither applied nor advanced is genuinely stalled.
          if (result.applied === 0 && String(nextCursor) === String(cursor)) {
            this.#tripIngest('ingestCursorStalled', {
              phase: 'changes',
              cursor,
              batchWatermark: batch.watermark,
              changeRows: batch.changes.length,
              applied: result.applied,
            })
          }
        }
        this.#recoverIngestBreaker()
        const endingWatermark = this.#engineState().watermark
        if (snapshotCompleted || total > 0 || endingWatermark !== startingWatermark) {
          await this.#enqueueWake('__upstream__')
        }
        return total
      })().finally(() => {
        this.#ingestPromise = null
      })
      return this.#ingestPromise
    }

    #ingestAfterCurrent(upstreamPath: string | null): Promise<number> {
      const current = this.#ingestPromise
      return current
        ? current.then(() => this.#ingest(upstreamPath))
        : this.#ingest(upstreamPath)
    }

    async #fetchDelegatedPush(
      endpoint: URL,
      headers: Headers,
      body: ArrayBuffer,
      provisioning = false
    ): Promise<Response> {
      const binding = this.#serviceBinding(
        config.mutateBinding ?? config.upstream?.binding
      )
      let lastError: unknown = null
      for (let attempt = 1; attempt <= delegateMaxAttempts; attempt++) {
        let response: Response | null = null
        try {
          response = await binding.fetch(endpoint.toString(), {
            method: 'POST',
            headers,
            body,
            signal: AbortSignal.timeout(
              provisioning ? Math.max(delegateTimeoutMs, 25_000) : delegateTimeoutMs
            ),
          })
        } catch (error) {
          lastError = error
        }
        if (
          !shouldRetryDelegatedPush(
            response?.status ?? null,
            attempt,
            delegateMaxAttempts
          )
        ) {
          if (response) return response
          throw lastError
        }
        await response?.body?.cancel()
        const delayMs = retryDelayMs(
          attempt,
          delegateInitialBackoffMs,
          delegateMaxBackoffMs
        )
        console.warn(
          JSON.stringify({
            event: 'sync_delegated_push_retry',
            attempt,
            maxAttempts: delegateMaxAttempts,
            status: response?.status ?? null,
            delayMs,
            error: response ? null : errorMessage(lastError),
          })
        )
        await scheduler.wait(delayMs)
      }
      throw lastError ?? new Error('delegated push retry exhausted')
    }

    #log(fields: Record<string, unknown>): void {
      console.log(
        JSON.stringify({
          event: 'sync_request',
          hostVersion: config.hostVersion,
          engineVersion: engine_version(),
          ...fields,
        })
      )
    }

    #enqueueWake(originClientID: string): Promise<void> {
      this.#wakeOrigins.add(originClientID)
      for (const socket of this.ctx.getWebSockets()) {
        const attachment = socketAttachment(socket)
        if (
          !attachment ||
          attachment.clientID === originClientID ||
          this.#pulling.has(attachment.clientID)
        ) {
          continue
        }
        this.#wakeRecipients.add(socket)
      }
      return this.#scheduleWake()
    }

    #scheduleWake(): Promise<void> {
      if (!this.#wakePromise) {
        const queuedAt = performance.now()
        this.#wakePromise = (async () => {
          await scheduler.wait(wakeCoalesceMs)
          const fanoutStarted = performance.now()
          const origins = this.#wakeOrigins
          this.#wakeOrigins = new Set()
          const recipients = this.#wakeRecipients
          this.#wakeRecipients = new Set()
          this.#counters.wakeBatches++
          let sent = 0
          const sockets = this.ctx.getWebSockets()
          for (const socket of recipients) {
            try {
              socket.send('wake')
              sent++
              this.#counters.wakeFrames++
            } catch {
              // A closing hibernating socket disappears from getWebSockets;
              // a race here is advisory and carries no correctness weight.
            }
          }
          console.log(
            JSON.stringify({
              event: 'sync_wake',
              hostVersion: config.hostVersion,
              socketCount: sockets.length,
              originCount: origins.size,
              sent,
              eligibleRecipients: recipients.size,
              coalesceMs: fanoutStarted - queuedAt,
              fanoutMs: performance.now() - fanoutStarted,
            })
          )
        })().finally(() => {
          this.#wakePromise = null
          if (this.#wakeOrigins.size > 0) void this.#scheduleWake()
        })
      }
      return this.#wakePromise
    }

    async #pull(
      request: Request,
      claims: NormalizedClaims,
      namespace: string
    ): Promise<Response> {
      this.#counters.pulls++
      this.#engineDb.resetStats()
      const started = performance.now()
      let transactionMs = 0
      let body: Record<string, unknown> | undefined
      try {
        body = await requestObject(request)
        const queryAware =
          this.#queryAwareOverride() ??
          (typeof config.queryAware === 'function'
            ? config.queryAware(claims)
            : (config.queryAware ?? Boolean(config.resolveQuery)))
        const transformVersion = queryAware
          ? typeof config.queryTransformVersion === 'function'
            ? config.queryTransformVersion(claims)
            : (config.queryTransformVersion ?? 0)
          : 0
        if (!Number.isSafeInteger(transformVersion) || transformVersion < 0) {
          throw new TypeError('queryTransformVersion must be a non-negative safe integer')
        }
        const releaseQueryPull =
          queryAware && config.resolveQuery
            ? await this.#acquireQueryPullLock(
                typeof body.clientGroupID === 'string' ? body.clientGroupID : ''
              )
            : null
        let response: Record<string, unknown>
        try {
          if (queryAware && body.queries) {
            const queries = body.queries as {
              version?: unknown
              patch?: unknown
            }
            if (Array.isArray(queries.patch)) {
              const patch = []
              for (const operation of queries.patch) {
                if (!operation || typeof operation !== 'object') {
                  patch.push(operation)
                  continue
                }
                const op = operation as Record<string, unknown>
                if (op.op === 'put') {
                  if (!config.resolveQuery || typeof op.name !== 'string') {
                    throw requestError('query put requires a server-resolved named query')
                  }
                  if (!Array.isArray(op.args)) {
                    throw requestError('named query args must be an array')
                  }
                  const args = op.args as JsonValue[]
                  let ast: JsonValue
                  try {
                    // resolveQuery may be async and needs `env` (a consumer can
                    // delegate the transform to its app's real synced-queries
                    // endpoint over an app service binding — authenticate runs in the
                    // worker isolate, but the query loop runs here in the DO, so the
                    // binding must come from the DO's own env, not a shared global).
                    ast = await config.resolveQuery(op.name, args, claims, this.env)
                  } catch (error) {
                    throw requestError(`unknown or unsupported named query: ${op.name}`)
                  }
                  patch.push({
                    op: 'put',
                    hash: op.hash,
                    ast,
                    transformVersion,
                  })
                } else patch.push(operation)
              }
              body = { ...body, queries: { ...queries, patch } }
            }
          }
          if (queryAware) {
            body = { ...body, _serverQueryTransformVersion: transformVersion }
          }
          const clientID = typeof body.clientID === 'string' ? body.clientID : ''
          this.#pulling.add(clientID)
          try {
            const txStarted = performance.now()
            const duringFault = this.#takeFault('pull_during_tx')
            response = this.ctx.storage.transactionSync(() => {
              const result = this.#wasm(() =>
                queryAware
                  ? engine_handle_query_pull(
                      this.#engineDb,
                      config.schema,
                      this.#retainChanges(),
                      body,
                      claims.userID
                    )
                  : engine_handle_pull(
                      this.#engineDb,
                      config.schema,
                      this.#visibility(claims),
                      caps,
                      this.#retainChanges(),
                      body,
                      claims.userID
                    )
              ) as Record<string, unknown>
              if (duringFault) throw this.#faultError(duringFault, 'pull_during_tx')
              return result
            })
            transactionMs = performance.now() - txStarted
          } finally {
            this.#pulling.delete(clientID)
          }
        } finally {
          releaseQueryPull?.()
        }
        const afterPullFault = this.#takeFault('pull_after_commit')
        if (afterPullFault) throw this.#faultError(afterPullFault, 'pull_after_commit')
        const patch = Array.isArray(response.rowsPatch) ? response.rowsPatch : []
        const queriesBody = body.queries as { patch?: unknown[] } | undefined
        const queryPuts =
          queryAware && Array.isArray(queriesBody?.patch)
            ? queriesBody.patch.filter(
                (entry) => (entry as { op?: unknown } | null)?.op === 'put'
              ).length
            : 0
        this.#counters.queryRecompilations += queryPuts
        const state = this.#engineStateBestEffort()
        this.#log({
          namespaceHash: namespace,
          requestKind: 'pull',
          resultClass: response.unchanged === true ? 'unchanged' : 'success',
          inputCookie: body.cookie ?? null,
          outputCookie: response.cookie ?? null,
          retainedFloor: state?.floor ?? null,
          currentWatermark: state?.watermark ?? null,
          changeRowsScanned: null,
          changeRowsIncluded: null,
          queriesRecomputed: queryPuts,
          rowPuts: patch.filter((entry) => entry?.op === 'put').length,
          rowDeletes: patch.filter((entry) => entry?.op === 'del').length,
          lmidAdvances: 0,
          transactionMs,
          totalMs: performance.now() - started,
          resetReason: null,
          wasmBoundaryCalls: this.#counters.wasmBoundaryCalls,
          sql: this.#engineDb.stats,
        })
        return json(response)
      } catch (error) {
        const status = statusOf(error)
        if (status === 409) this.#counters.resets++
        if (status === 500) this.#counters.invariantFailures++
        const state = this.#engineStateBestEffort()
        this.#log({
          namespaceHash: namespace,
          requestKind: 'pull',
          resultClass: status === 409 ? 'reset' : 'error',
          inputCookie: body?.cookie ?? null,
          outputCookie: null,
          retainedFloor: state?.floor ?? null,
          currentWatermark: state?.watermark ?? null,
          changeRowsScanned: null,
          changeRowsIncluded: 0,
          queriesRecomputed: 0,
          rowPuts: 0,
          rowDeletes: 0,
          lmidAdvances: 0,
          transactionMs,
          totalMs: performance.now() - started,
          resetReason: status === 409 ? errorMessage(error) : null,
        })
        return json({ error: errorMessage(error) }, status)
      }
    }

    async #push(
      request: Request,
      claims: NormalizedClaims,
      namespace: string,
      upstreamPath: string | null
    ): Promise<Response> {
      this.#counters.pushes++
      this.#engineDb.resetStats()
      const started = performance.now()
      let transactionMs = 0
      let lmidAdvances = 0
      let resultClass = 'success'
      if (!this.#writerEnabled()) {
        // Workerd requires the request stream to be consumed before the DO
        // returns a response. Discard it without parsing or logging payloads.
        await request.arrayBuffer()
        const state = this.#engineStateBestEffort()
        this.#log({
          namespaceHash: namespace,
          requestKind: 'push',
          resultClass: 'writer_disabled',
          inputCookie: null,
          outputCookie: null,
          retainedFloor: state?.floor ?? null,
          currentWatermark: state?.watermark ?? null,
          changeRowsScanned: 0,
          changeRowsIncluded: 0,
          queriesRecomputed: 0,
          rowPuts: 0,
          rowDeletes: 0,
          lmidAdvances: 0,
          transactionMs: 0,
          totalMs: performance.now() - started,
          resetReason: 'writer disabled by operator',
          wasmBoundaryCalls: this.#counters.wasmBoundaryCalls,
          sql: this.#engineDb.stats,
        })
        return json({ error: 'writer disabled by operator' }, 503)
      }
      if (config.mutateUrl) {
        try {
          const bytes = await request.arrayBuffer()
          const body = JSON.parse(new TextDecoder().decode(bytes)) as Record<
            string,
            unknown
          >
          const plan = this.#wasm(() => engine_push_validate(body)) as PushPlan
          if (plan.kind === 'respond') return json(plan.response)

          const endpoint = new URL(
            `${upstreamPath ?? ''}${config.mutateUrl}`,
            config.mutateOrigin ?? 'https://upstream.invalid'
          )
          const headers = new Headers(request.headers)
          headers.delete(NAMESPACE_HEADER)
          headers.delete(UPSTREAM_PATH_HEADER)
          headers.set('host', endpoint.host)
          const upstreamResponse = await this.#fetchDelegatedPush(
            endpoint,
            headers,
            bytes,
            this.#engineState().upstreamWatermark === '0'
          )
          if (!upstreamResponse.ok) {
            return new Response(upstreamResponse.body, upstreamResponse)
          }
          const upstreamBody = (await upstreamResponse.json()) as DelegatedPushBody
          const delegatedResponse = upstreamBody.pushResponse ?? upstreamBody
          if (isStructuredPushFailed(delegatedResponse)) {
            // PushFailed is a successful protocol response describing an
            // application-level failure. There are intentionally no mutation
            // acknowledgements to finalize in the host; preserve the body so
            // the Zero client can apply its retry/error policy.
            return json({ pushResponse: delegatedResponse })
          }
          const acknowledged =
            typeof upstreamBody.pushResponse === 'object' &&
            upstreamBody.pushResponse !== null &&
            'mutations' in upstreamBody.pushResponse
              ? upstreamBody.pushResponse.mutations
              : upstreamBody.mutations
          if (!Array.isArray(acknowledged)) {
            throw new Error('delegated push returned no mutation results')
          }
          for (const mutation of plan.mutations) {
            const ack = acknowledged.some(
              (result) =>
                result.id?.clientID === mutation.clientID &&
                String(result.id?.id) === mutation.id
            )
            if (!ack) {
              throw new Error(
                `delegated push did not acknowledge ${mutation.clientID}:${mutation.id}`
              )
            }
          }
          // the delegated app response is causally visible through DATA by
          // contract. start an ingest round after that response, even if an
          // older round is still in flight, then journal lmids. every capped
          // log prefix therefore preserves effects-before-ack.
          await this.#ingestAfterCurrent(upstreamPath)
          for (const mutation of plan.mutations) {
            this.ctx.storage.transactionSync(() => {
              const decision = this.#wasm(() =>
                engine_preflight(
                  this.#engineDb,
                  plan.clientGroupID,
                  mutation.clientID,
                  mutation.id,
                  claims.userID
                )
              ) as Preflight
              if (decision.kind === 'applied') {
                this.#wasm(() =>
                  engine_finalize(
                    this.#engineDb,
                    plan.clientGroupID,
                    mutation.clientID,
                    mutation.id
                  )
                )
                lmidAdvances++
              }
            })
          }
          if (lmidAdvances > 0) {
            this.ctx.storage.transactionSync(() =>
              this.#wasm(() => engine_prune(this.#engineDb, this.#retainChanges()))
            )
          }
          return json({ pushResponse: delegatedResponse })
        } catch (error) {
          const status = statusOf(error)
          return json(errorBody(error), status)
        }
      }
      try {
        const body = await requestObject(request)
        const beforeMutationFault = this.#takeFault('push_before_mutation')
        if (beforeMutationFault)
          throw this.#faultError(beforeMutationFault, 'push_before_mutation')
        if (!this.#executor) throw new Error('local sync executor is not configured')
        // Consume the fault outside the transaction it aborts so a rollback
        // cannot restore the one-shot control flag.
        this.#executorBeforeCommitFault = this.#takeFault(
          'push_after_write_before_commit'
        )
        const txStarted = performance.now()
        let result
        try {
          result = await this.#executor.push(body, claims)
        } finally {
          this.#executorBeforeCommitFault = null
        }
        transactionMs += performance.now() - txStarted

        const mutationResults =
          'mutations' in result.pushResponse ? result.pushResponse.mutations : []
        for (const mutation of mutationResults) {
          if (
            'error' in mutation.result &&
            mutation.result.error === 'alreadyProcessed'
          ) {
            continue
          }
          lmidAdvances++
          if ('error' in mutation.result && mutation.result.error === 'app') {
            this.#counters.applicationErrors++
            resultClass = 'application_error'
          }
          this.ctx.waitUntil(this.#enqueueWake(mutation.id.clientID))
        }

        if (mutationResults.length > 0) {
          const txStarted = performance.now()
          await this.ctx.storage.transaction(async () => {
            this.#wasm(() => engine_prune(this.#engineDb, this.#retainChanges()))
          })
          transactionMs += performance.now() - txStarted
          this.#counters.retentionRuns++
        }

        const afterCommitFault = this.#takeFault('push_after_commit_before_response')
        if (afterCommitFault)
          throw this.#faultError(afterCommitFault, 'push_after_commit_before_response')

        const state = this.#engineStateBestEffort()
        this.#log({
          namespaceHash: namespace,
          requestKind: 'push',
          resultClass,
          inputCookie: null,
          outputCookie: null,
          retainedFloor: state?.floor ?? null,
          currentWatermark: state?.watermark ?? null,
          changeRowsScanned: 0,
          changeRowsIncluded: 0,
          queriesRecomputed: 0,
          rowPuts: 0,
          rowDeletes: 0,
          lmidAdvances,
          transactionMs,
          totalMs: performance.now() - started,
          resetReason: null,
          wasmBoundaryCalls: this.#counters.wasmBoundaryCalls,
          sql: this.#engineDb.stats,
        })
        if (this.#dropNextPushResponse) {
          this.#dropNextPushResponse = false
          return json({ error: 'intentionally dropped push response' }, 503)
        }
        return json(result)
      } catch (error) {
        const status = statusOf(error)
        if (status === 500) this.#counters.invariantFailures++
        const state = this.#engineStateBestEffort()
        this.#log({
          namespaceHash: namespace,
          requestKind: 'push',
          resultClass: 'error',
          inputCookie: null,
          outputCookie: null,
          retainedFloor: state?.floor ?? null,
          currentWatermark: state?.watermark ?? null,
          changeRowsScanned: 0,
          changeRowsIncluded: 0,
          queriesRecomputed: 0,
          rowPuts: 0,
          rowDeletes: 0,
          lmidAdvances,
          transactionMs,
          totalMs: performance.now() - started,
          resetReason: null,
        })
        return json({ error: errorMessage(error) }, status)
      }
    }

    #wake(request: Request): Response {
      if (request.headers.get('upgrade')?.toLowerCase() !== 'websocket') {
        return json({ error: 'websocket upgrade required' }, 426)
      }
      const clientID = new URL(request.url).searchParams.get('clientID')
      if (!clientID) return json({ error: 'clientID is required' }, 400)
      const pair = new WebSocketPair()
      const [client, server] = Object.values(pair)
      server.serializeAttachment({ clientID } satisfies SocketAttachment)
      this.ctx.acceptWebSocket(server, [`client:${clientID}`])
      // The alarm is only a safety net for an actively connected consumer.
      // A namespace with no wake socket has nobody to notify; its next pull or
      // push ingests synchronously. Arming from construction made every
      // namespace poll upstream forever after its first request, even across
      // DO eviction, producing a permanent rows-written floor at zero traffic.
      this.#armUpstreamAlarm()
      return new Response(null, { status: 101, webSocket: client })
    }

    #admin(
      route: string,
      request: Request,
      upstreamPath: string | null
    ): Promise<Response> | Response {
      if (route === '/admin/health') return json({ ok: true })
      if (route === '/admin/upstream-write-budget' && request.method === 'GET')
        return this.#upstreamWriteBudgetStatus()
      if (route === '/admin/status') {
        const heap = (
          performance as Performance & {
            memory?: {
              usedJSHeapSize: number
              totalJSHeapSize: number
              jsHeapSizeLimit: number
            }
          }
        ).memory
        return this.ctx.storage.getAlarm().then((upstreamAlarmAt) =>
          json({
            bootID: this.#bootID,
            idleTeardownMs,
            hibernations: this.#hibernations,
            databaseSizeBytes: this.ctx.storage.sql.databaseSize,
            connectedWakeSockets: this.ctx.getWebSockets().length,
            upstreamAlarmAt,
            writerEnabled: this.#writerEnabled(),
            wasmMemoryBytes: engine_memory_bytes(),
            heapUsedBytes: heap?.usedJSHeapSize ?? null,
            heapTotalBytes: heap?.totalJSHeapSize ?? null,
            heapLimitBytes: heap?.jsHeapSizeLimit ?? null,
            engine: this.#engineStateBestEffort(),
            counters: this.#counters,
            ingestBreaker: this.#ingestBreaker.status(),
          })
        )
      }
      if (route === '/admin/sql') {
        return request.json().then((body) => {
          const { params, query } = body as { params?: unknown; query?: string }
          if (typeof query !== 'string') return json({ error: 'query is required' }, 400)
          try {
            return json({ rows: this.#directSql.query(query, decodeSqlParams(params)) })
          } catch (error) {
            if (error instanceof TypeError && error.message.startsWith('params')) {
              return json({ error: `invalid params: ${error.message}` }, 400)
            }
            if (
              error instanceof TypeError &&
              error.message === 'transaction SQL is host-owned and forbidden'
            ) {
              return json({ error: error.message }, 400)
            }
            throw error
          }
        })
      }
      if (route === '/admin/invalidate') {
        this.ctx.storage.transactionSync(() =>
          this.#wasm(() => engine_invalidate(this.#engineDb))
        )
        return json({ ok: true, engine: this.#engineState() })
      }
      if (route === '/admin/resnapshot') {
        if (request.method !== 'POST') {
          return json({ error: 'method not allowed' }, 405)
        }
        return (async () => {
          try {
            const beforeUpstreamWatermark = this.#engineState().upstreamWatermark
            const applied = await this.#ingest(upstreamPath, true)
            const engine = this.#engineState()
            return json({
              ok: true,
              applied,
              beforeUpstreamWatermark,
              afterUpstreamWatermark: engine.upstreamWatermark,
              engine,
            })
          } catch (error) {
            return json(errorBody(error), statusOf(error))
          }
        })()
      }
      if (route === '/admin/drop-next-push-response') {
        this.#dropNextPushResponse = true
        return json({ ok: true })
      }
      if (route === '/admin/restart') {
        this.ctx.abort('admin requested durable object restart')
        return json({ ok: true, bootID: this.#bootID })
      }
      if (route === '/admin/visibility') {
        return request.json().then((body) => {
          const enabled = Boolean((body as { enabled?: unknown }).enabled)
          this.#controlSet('visibilityEnabled', enabled ? '1' : '0')
          return json({ ok: true, enabled })
        })
      }
      if (route === '/admin/query-aware') {
        return request.json().then((body) => {
          const enabled = Boolean((body as { enabled?: unknown }).enabled)
          this.#controlSet('queryAwareOverride', enabled ? '1' : '0')
          return json({ ok: true, enabled })
        })
      }
      if (route === '/admin/retention') {
        return request
          .json()
          .catch(() => ({}))
          .then((body) => {
            const value = Number((body as { retainChanges?: unknown }).retainChanges)
            if (!Number.isSafeInteger(value) || value < 0)
              return json({ error: 'invalid retainChanges' }, 400)
            this.#controlSet('retainChanges', String(value))
            return json({ ok: true, retainChanges: value })
          })
      }
      if (route === '/admin/writer') {
        if (request.method === 'GET')
          return json({ writerEnabled: this.#writerEnabled() })
        return request
          .json()
          .catch(() => ({}))
          .then((body) => {
            const enabled = (body as { enabled?: unknown }).enabled
            if (typeof enabled !== 'boolean')
              return json({ error: 'enabled must be a boolean' }, 400)
            this.#controlSet('writerEnabled', enabled ? '1' : '0')
            return json({ ok: true, writerEnabled: enabled })
          })
      }
      if (route === '/admin/ingest-breaker') {
        if (request.method === 'GET') return json(this.#ingestBreaker.status())
        this.#ingestBreaker.reopen()
        this.#controlDelete(
          'ingestBreakerReason',
          'ingestBreakerRetryAt',
          'ingestBreakerTrips'
        )
        console.log(JSON.stringify({ event: 'sync_upstream_ingest_breaker_reopened' }))
        return json({ ok: true, ...this.#ingestBreaker.status() })
      }
      if (route === '/admin/fault') {
        return request
          .json()
          .catch(() => ({}))
          .then((body) => {
            const value = body as {
              clear?: unknown
              point?: unknown
              kind?: unknown
            }
            if (value.clear === true) {
              this.#directSql.exec(
                "DELETE FROM _zsync_host_control WHERE key IN ('faultPoint', 'faultKind')"
              )
              return json({ ok: true, armed: null })
            }
            const points: FaultPoint[] = [
              'push_before_mutation',
              'push_after_write_before_commit',
              'push_after_commit_before_response',
              'pull_during_tx',
              'pull_after_commit',
            ]
            if (!points.includes(value.point as FaultPoint))
              return json({ error: 'invalid fault point' }, 400)
            if (value.kind !== 'error' && value.kind !== 'quota')
              return json({ error: 'invalid fault kind' }, 400)
            this.#controlSet('faultPoint', value.point as string)
            this.#controlSet('faultKind', value.kind)
            return json({
              ok: true,
              armed: { point: value.point, kind: value.kind },
            })
          })
      }
      return json({ error: 'not found' }, 404)
    }

    async fetch(request: Request): Promise<Response> {
      this.#simulateIdleTeardown(Date.now())
      const route = routeAfterNamespace(new URL(request.url).pathname)
      const namespace = request.headers.get(NAMESPACE_HEADER) ?? 'unknown'
      const upstreamPath = this.#rememberUpstreamPath(request)
      if (route.startsWith('/admin/')) return this.#admin(route, request, upstreamPath)

      if (route === '/wake' && request.method === 'GET') return this.#wake(request)
      if (route === '/notify' && request.method === 'POST') {
        try {
          const applied = await this.#ingest(upstreamPath)
          return json({ ok: true, applied })
        } catch (error) {
          return json(errorBody(error), statusOf(error))
        }
      }

      if ((route === '/pull' || route === '/push') && request.method === 'POST') {
        let forwarded
        try {
          forwarded = await forwardedSyncRequest(request)
        } catch (error) {
          return json(errorBody(error), statusOf(error))
        }
        // pull and push both establish the upstream schema and snapshot barrier.
        try {
          await this.#ingest(upstreamPath)
        } catch (error) {
          return json(errorBody(error), statusOf(error))
        }
        if (route === '/pull') {
          return this.#pull(forwarded.request, forwarded.claims, namespace)
        }
        return this.#push(forwarded.request, forwarded.claims, namespace, upstreamPath)
      }
      return json({ error: 'not found' }, 404)
    }

    async alarm(): Promise<void> {
      if (this.ctx.getWebSockets().length === 0) return
      try {
        await this.#ingest()
      } catch (error) {
        console.error(
          JSON.stringify({
            event: 'sync_upstream_ingest_error',
            status: statusOf(error),
            error: errorMessage(error),
          })
        )
      } finally {
        if (this.ctx.getWebSockets().length > 0) {
          const retryAfterMs = this.#ingestBreaker.status().retryAfterMs
          await this.ctx.storage.setAlarm(
            Date.now() + Math.max(upstreamIntervalMs, retryAfterMs)
          )
        }
      }
    }

    webSocketMessage(socket: WebSocket, message: string | ArrayBuffer): void {
      if (message === 'ping') socket.send('pong')
    }

    webSocketClose(
      socket: WebSocket,
      code: number,
      reason: string,
      _wasClean: boolean
    ): void {
      // The peer already closed, so echo the close to release the socket — but
      // WebSocket.close() rejects reserved/absent codes (1005 "no status", 1006
      // abnormal, 1015) with InvalidAccessError, and a real browser routinely
      // closes with 1001/1005. An uncaught throw here aborts the DO, so only
      // echo an application-permitted code and otherwise close cleanly.
      const echoable = code === 1000 || (code >= 3000 && code <= 4999)
      socketCloseQuietly(socket, echoable ? code : 1000, echoable ? reason : '')
    }

    webSocketError(socket: WebSocket, _error: unknown): void {
      socketCloseQuietly(socket, 1011, 'wake socket error')
    }
  }
}
