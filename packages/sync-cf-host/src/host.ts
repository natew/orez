import { DurableObject } from 'cloudflare:workers'

import { validatePullCaps } from './config.js'
import {
  engine_assemble_push_response,
  engine_compile_query,
  engine_finalize,
  engine_handle_pull,
  engine_handle_query_pull,
  engine_init_query_schema,
  engine_init_schema,
  engine_invalidate,
  engine_memory_bytes,
  engine_preflight,
  engine_prune,
  engine_push_validate,
  engine_record_app_error,
  engine_state,
  engine_version,
  initSync,
} from './generated/sync_wasm.js'
import wasmModule from './generated/sync_wasm_bg.wasm'
import {
  SqlStorageDirect,
  SqlStorageMutatorTransaction,
  SqlStorageSyncDb,
} from './sql-storage-adapter.js'
import { MutationApplicationError } from './types.js'

import type {
  DeferredEffect,
  JsonValue,
  NormalizedClaims,
  PullCaps,
  SyncHostConfig,
  SyncHostEnv,
} from './types.js'

initSync({ module: wasmModule })

const CLAIMS_HEADER = 'x-orez-sync-claims'
const NAMESPACE_HEADER = 'x-orez-sync-namespace'
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

type EngineState = { watermark: string; floor: string }
type SocketAttachment = { clientID: string }
type FaultPoint =
  | 'push_before_mutation'
  | 'push_after_write_before_commit'
  | 'push_after_commit_before_response'
  | 'pull_during_tx'
  | 'pull_after_commit'
type FaultKind = 'error' | 'quota'

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

function claimsFromRequest(request: Request): NormalizedClaims | null {
  const encoded = request.headers.get(CLAIMS_HEADER)
  if (!encoded) return null
  try {
    const value = JSON.parse(decodeURIComponent(encoded)) as NormalizedClaims
    return value && typeof value.userID === 'string' && value.userID.length > 0
      ? value
      : null
  } catch {
    return null
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
 * Durable Object receives only normalized claims over a binding-private header.
 */
export function createSyncWorker<Env extends SyncHostEnv>(
  config: SyncHostConfig<Env>
): ExportedHandler<Env> {
  return {
    async fetch(request, env): Promise<Response> {
      const namespace = config.namespace(request)
      if (!namespace) return new Response('orez sync-cf-host', { status: 200 })

      const route = routeAfterNamespace(new URL(request.url).pathname)
      const isAdmin = route.startsWith('/admin/')
      if (isAdmin) {
        const authorized = config.authorizeAdmin
          ? await config.authorizeAdmin(request, env)
          : Boolean(env.ADMIN_KEY) && request.headers.get('x-admin-key') === env.ADMIN_KEY
        if (!authorized) return json({ error: 'forbidden' }, 403)
      }

      const headers = new Headers(request.headers)
      headers.delete(CLAIMS_HEADER)
      headers.delete(NAMESPACE_HEADER)
      // Wake sockets carry only a client ID and the literal "wake" hint; they
      // expose no rows, claims, or mutation state. Keep this advisory channel
      // compatible with the vendored transport, which cannot attach an HTTP
      // Authorization header to its native WebSocket constructor.
      if (!isAdmin && route !== '/wake') {
        const claims = await config.authenticate(request, env)
        if (!claims) return json({ error: 'missing authentication' }, 401)
        headers.set(CLAIMS_HEADER, encodeURIComponent(JSON.stringify(claims)))
      }
      headers.set(NAMESPACE_HEADER, await namespaceHash(namespace))

      const forwarded = new Request(request, { headers })
      const id = env.SYNC_DO.idFromName(namespace)
      return env.SYNC_DO.get(id).fetch(forwarded)
    },
  }
}

/** Create the namespace Durable Object class for one bundled consumer config. */
export function createSyncDurableObject<Env extends SyncHostEnv>(
  config: SyncHostConfig<Env>
) {
  const defaultRetainChanges = String(config.retainChanges ?? 4_096)
  const caps: PullCaps = validatePullCaps({ ...DEFAULT_CAPS, ...config.caps })
  const idleTeardownMs = config.idleTeardownMs ?? 5_000
  // A CF fan-out wakes every client into an HTTP pull. Give concurrent writer
  // requests a real batching window so a storm burst creates one pull wave.
  const wakeCoalesceMs = config.wakeCoalesceMs ?? 25

  return class SyncDurableObject extends DurableObject<Env> {
    readonly #engineDb: SqlStorageSyncDb
    readonly #directSql: SqlStorageDirect
    readonly #mutatorSql: SqlStorageMutatorTransaction
    #bootID = crypto.randomUUID()
    #lastRequestAt = 0
    #hibernations = 0
    #dropNextPushResponse = false
    #counters = freshCounters()
    #pulling = new Set<string>()
    #wakeOrigins = new Set<string>()
    #wakeRecipients = new Set<WebSocket>()
    #wakePromise: Promise<void> | null = null

    constructor(ctx: DurableObjectState, env: Env) {
      super(ctx, env)
      this.#engineDb = new SqlStorageSyncDb(ctx.storage.sql)
      this.#directSql = new SqlStorageDirect(ctx.storage.sql)
      this.#mutatorSql = new SqlStorageMutatorTransaction(this.#directSql, (ast) =>
        this.#wasm(() => engine_compile_query(config.schema, ast))
      )
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
          this.#wasm(() => engine_init_schema(this.#engineDb, config.schema))
          if (config.queryAware || config.resolveQuery)
            this.#wasm(() => engine_init_query_schema(this.#engineDb))
        })
      })
    }

    #wasm<T>(call: () => T): T {
      this.#counters.wasmBoundaryCalls++
      return call()
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
            ? [{ table, sql: filter.sql, params: [...(filter.params ?? [])] }]
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

    #engineState(): EngineState | null {
      try {
        return this.#wasm(() => engine_state(this.#engineDb)) as EngineState
      } catch {
        return null
      }
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

    async #runEffects(effects: DeferredEffect[]): Promise<void> {
      for (const effect of effects) {
        try {
          await effect()
        } catch (error) {
          this.#counters.externalEffectFailures++
          console.error(
            JSON.stringify({
              event: 'sync_external_effect_error',
              hostVersion: config.hostVersion,
              error: errorMessage(error),
            })
          )
        }
      }
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
                  ast = config.resolveQuery(op.name, args, claims)
                } catch (error) {
                  throw requestError(`unknown or unsupported named query: ${op.name}`)
                }
                const transformVersion =
                  typeof config.queryTransformVersion === 'function'
                    ? config.queryTransformVersion(claims)
                    : (config.queryTransformVersion ?? 0)
                if (!Number.isSafeInteger(transformVersion) || transformVersion < 0) {
                  throw new TypeError(
                    'queryTransformVersion must be a non-negative safe integer'
                  )
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
        const clientID = typeof body.clientID === 'string' ? body.clientID : ''
        this.#pulling.add(clientID)
        let response: Record<string, unknown>
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
        const state = this.#engineState()
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
        this.#log({
          namespaceHash: namespace,
          requestKind: 'pull',
          resultClass: status === 409 ? 'reset' : 'error',
          inputCookie: body?.cookie ?? null,
          outputCookie: null,
          retainedFloor: this.#engineState()?.floor ?? null,
          currentWatermark: this.#engineState()?.watermark ?? null,
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
      namespace: string
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
        const state = this.#engineState()
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
      try {
        const body = await requestObject(request)
        const beforeMutationFault = this.#takeFault('push_before_mutation')
        if (beforeMutationFault)
          throw this.#faultError(beforeMutationFault, 'push_before_mutation')
        const plan = this.#wasm(() => engine_push_validate(body)) as PushPlan
        if (plan.kind === 'respond') return json(plan.response)

        const results: MutationResult[] = []
        for (const mutation of plan.mutations) {
          const deferred: DeferredEffect[] = []
          try {
            const txStarted = performance.now()
            // consume the before-commit fault OUTSIDE the transaction it is
            // about to abort: taken inside, the control-table delete rolls
            // back with the abort and the fault re-fires on every retry
            // instead of being one-shot.
            const beforeCommitFault = this.#takeFault('push_after_write_before_commit')
            const preflight = await this.ctx.storage.transaction(async () => {
              // Storage transactions may retry their closure. Never carry a
              // deferred effect from an abandoned attempt into the commit.
              deferred.length = 0
              const decision = this.#wasm(() =>
                engine_preflight(
                  this.#engineDb,
                  plan.clientGroupID,
                  mutation.clientID,
                  mutation.id,
                  claims.userID
                )
              ) as Preflight
              if (decision.kind === 'replay') return decision
              const mutator = config.mutators[mutation.name]
              if (!mutator) throw new Error(`unknown mutator: ${mutation.name}`)
              await mutator(this.#mutatorSql, mutation.args[0] ?? null, {
                claims,
                clientID: mutation.clientID,
                mutationID: mutation.id,
                defer(effect) {
                  deferred.push(effect)
                },
              })
              if (beforeCommitFault)
                throw this.#faultError(
                  beforeCommitFault,
                  'push_after_write_before_commit'
                )
              this.#wasm(() =>
                engine_finalize(
                  this.#engineDb,
                  plan.clientGroupID,
                  mutation.clientID,
                  mutation.id
                )
              )
              return decision
            })
            transactionMs += performance.now() - txStarted

            if ((preflight as Preflight).kind === 'replay') {
              const expected = (preflight as Extract<Preflight, { kind: 'replay' }>)
                .expected
              results.push({
                clientID: mutation.clientID,
                id: mutation.id,
                result: {
                  error: 'alreadyProcessed',
                  details: `Ignoring mutation from ${mutation.clientID} with ID ${mutation.id} as it was already processed. Expected: ${expected}`,
                },
              })
              continue
            }

            lmidAdvances++
            results.push({
              clientID: mutation.clientID,
              id: mutation.id,
              result: {},
            })
            // Keep the advisory fan-out off the push response's critical path.
            // waitUntil anchors the coalescing timer across request completion,
            // while the next serialized client push can join the same batch.
            this.ctx.waitUntil(this.#enqueueWake(mutation.clientID))
            await this.#runEffects(deferred)
          } catch (error) {
            const isAppError = error instanceof MutationApplicationError
            if (!isAppError) throw error
            this.#counters.applicationErrors++
            resultClass = 'application_error'
            const appError = error as Error & { details?: string }
            const txStarted = performance.now()
            await this.ctx.storage.transaction(async () => {
              this.#wasm(() =>
                engine_record_app_error(
                  this.#engineDb,
                  plan.clientGroupID,
                  mutation.clientID,
                  mutation.id,
                  claims.userID
                )
              )
            })
            transactionMs += performance.now() - txStarted
            lmidAdvances++
            results.push({
              clientID: mutation.clientID,
              id: mutation.id,
              result: {
                error: 'app',
                message: appError.message,
                details: appError.details ?? appError.message,
              },
            })
            this.ctx.waitUntil(this.#enqueueWake(mutation.clientID))
          }
        }

        if (plan.mutations.length > 0) {
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

        const response = this.#wasm(() => engine_assemble_push_response(results))
        const state = this.#engineState()
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
        return json(response)
      } catch (error) {
        const status = statusOf(error)
        if (status === 500) this.#counters.invariantFailures++
        this.#log({
          namespaceHash: namespace,
          requestKind: 'push',
          resultClass: 'error',
          inputCookie: null,
          outputCookie: null,
          retainedFloor: this.#engineState()?.floor ?? null,
          currentWatermark: this.#engineState()?.watermark ?? null,
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
      return new Response(null, { status: 101, webSocket: client })
    }

    #admin(route: string, request: Request): Promise<Response> | Response {
      if (route === '/admin/health') return json({ ok: true })
      if (route === '/admin/status') {
        return json({
          bootID: this.#bootID,
          idleTeardownMs,
          hibernations: this.#hibernations,
          databaseSizeBytes: this.ctx.storage.sql.databaseSize,
          connectedWakeSockets: this.ctx.getWebSockets().length,
          writerEnabled: this.#writerEnabled(),
          wasmMemoryBytes: engine_memory_bytes(),
          engine: this.#engineState(),
          counters: this.#counters,
        })
      }
      if (route === '/admin/sql') {
        return request.json().then((body) => {
          const { query } = body as { query?: string }
          if (typeof query !== 'string') return json({ error: 'query is required' }, 400)
          return json({ rows: this.#directSql.query(query) })
        })
      }
      if (route === '/admin/invalidate') {
        this.ctx.storage.transactionSync(() =>
          this.#wasm(() => engine_invalidate(this.#engineDb))
        )
        return json({ ok: true, engine: this.#engineState() })
      }
      if (route === '/admin/drop-next-push-response') {
        this.#dropNextPushResponse = true
        return json({ ok: true })
      }
      if (route === '/admin/restart') {
        this.#bootID = crypto.randomUUID()
        this.#hibernations++
        this.#counters = freshCounters()
        this.#pulling.clear()
        this.#wakeOrigins.clear()
        this.#wakeRecipients.clear()
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
      if (route.startsWith('/admin/')) return this.#admin(route, request)

      if (route === '/wake' && request.method === 'GET') return this.#wake(request)

      const claims = claimsFromRequest(request)
      if (!claims) return json({ error: 'missing normalized claims' }, 401)
      if (route === '/pull' && request.method === 'POST') {
        return this.#pull(request, claims, namespace)
      }
      if (route === '/push' && request.method === 'POST') {
        return this.#push(request, claims, namespace)
      }
      return json({ error: 'not found' }, 404)
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
