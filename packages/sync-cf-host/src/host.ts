import { DurableObject } from 'cloudflare:workers'
import { createSyncExecutor, isMutationRetryError } from 'orez-sync-executor/core'
import { createSocketHost } from 'orez-sync-executor/realtime'

import { validateSyncHostConfig } from './config.js'
import { createQueryCompiler } from './query-compiler.js'
import { resolveQueryPatch } from './query-patch.js'
import { ServingLagTracker } from './serving-lag.js'
import {
  decodeSqlParams,
  SqlStorageDirect,
  SqlStorageMutatorTransaction,
  SqlStorageSyncDb,
} from './sql-storage-adapter.js'
import {
  DEFAULT_UPSTREAM_MAX_RESPONSE_BYTES,
  DEFAULT_UPSTREAM_MAX_REQUEST_BYTES,
  DEFAULT_UPSTREAM_REQUEST_TIMEOUT_MS,
  fetchBoundedUpstreamJson,
  readBoundedJsonResponse,
  readBoundedStream,
  UpstreamResponseLimitError,
} from './upstream-response.js'
import {
  engine_authorize_realtime_subscription,
  engine_apply_snapshot_changes,
  engine_apply_snapshot_page,
  engine_apply_upstream,
  engine_begin_snapshot_generation,
  engine_finalize,
  engine_finalize_snapshot_generation,
  engine_handle_query_pull,
  engine_init_query_schema,
  engine_init_schema,
  engine_invalidate,
  engine_memory_bytes,
  engine_preflight,
  engine_prune,
  engine_push_validate,
  engine_read_snapshot_progress,
  engine_schema_revision,
  engine_state,
  engine_version,
} from './wasm.js'
import {
  IngestBreakerError,
  IngestCircuitBreaker,
  retryDelayMs,
  shouldRetryDelegatedPush,
} from './write-safeguards.js'

import type { SyncHostConfig, SyncHostEnv } from './types.js'
import type { Schema } from '@rocicorp/zero'
import type {
  ApplicationDatabase,
  ApplicationTransaction,
  JsonValue,
  NormalizedClaims,
  SyncExecutor,
} from 'orez-sync-executor'
import type {
  HostConnection,
  RealtimeIdentity,
  RealtimeSocketHost,
  RealtimeTopic,
} from 'orez-sync-executor/realtime'

const NAMESPACE_HEADER = 'x-orez-sync-namespace'
const UPSTREAM_PATH_HEADER = 'x-orez-sync-upstream-path'
const NOTIFY_IF_SUBSCRIBED_HEADER = 'x-orez-notify-if-subscribed'
const WAKE_SUBSCRIBER_TAG = 'orez:wake-subscriber'
// A websocket upgrade is a GET, so the authenticated identity cannot ride the
// body the way /pull and /push carry their claims. It rides a private header
// the worker always deletes from the incoming request before setting its own,
// so a client cannot present one.
const IDENTITY_HEADER = 'x-orez-sync-identity'
const DEFAULT_SNAPSHOT_PAGE_ROWS = 2_000
const MIN_SNAPSHOT_PAGE_ROWS = 100
const WORKER_STAGE_TELEMETRY_SAMPLE_RATE = 0.01
const WORKER_STAGE_WAITING_MS = 5_000

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

type EngineState = {
  watermark: string
  floor: string
  upstreamWatermark: string
}
type UpstreamBatch = {
  watermark: number
  oldestCommitTimeMs?: number
  sourceTimeMs?: number
  // tables whose changes the upstream feed dropped because it does not publish
  // them. absent on an upstream that predates the field.
  unpublishedTables?: string[]
  changes: Array<{
    watermark: number
    tableName: string
    op: string
    rowData: Record<string, unknown> | null
    oldData: Record<string, unknown> | null
  }>
}
type WakeStatus = {
  at: number
  tables: string[]
  socketCount: number
  originCount: number
  sent: number
  eligibleRecipients: number
  coalesceMs: number
  fanoutMs: number
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

// a feed that STOPS publishing a table this replica already syncs is a
// publication misconfiguration, and it is invisible otherwise: the page applies
// nothing, the cursor still advances on the trailing syncCursor marker, and
// every query keeps answering from the last row that made it through, so
// clients see a table frozen at the moment of the bad deploy with no error
// anywhere.
//
// an app may also model tables it deliberately never publishes, capturing them
// for rollback only. in any single page those look identical to a demotion, so
// the test is not "modelled but dropped" but "dropped after this replica had
// already been receiving it": compare against the tables this replica has
// actually ingested and name only the ones that left.
function unpublishedRegressions(
  batch: UpstreamBatch,
  schema: { tables: Record<string, unknown> },
  publishedSeen: ReadonlySet<string>
): string[] {
  const reported = batch.unpublishedTables
  if (!Array.isArray(reported) || reported.length === 0) return []
  return reported.filter(
    (table) =>
      typeof table === 'string' &&
      Object.hasOwn(schema.tables, table) &&
      publishedSeen.has(table)
  )
}

function upstreamBatchTables(batch: UpstreamBatch): Set<string> {
  const tables = new Set<string>()
  for (const change of batch.changes) {
    if (typeof change?.tableName === 'string' && change.tableName.length > 0) {
      tables.add(change.tableName)
    }
  }
  return tables
}
type SnapshotPage = {
  watermark: number
  rows: Record<string, unknown>[]
  nextCursor: string | null
  unpublishedTables?: string[]
}
// A hibernatable socket outlives the object holding the hub, so anything the
// hub must not lose lives here. Identity is recorded at the authenticated
// upgrade and never read from a frame; topics are what `rehydrate` replays
// after an eviction, in the shape it accepts.
type SocketAttachment = {
  clientID: string
  identity?: RealtimeIdentity
  topics?: RealtimeTopic[]
  // A producer socket, which has no identity and no topics: it holds
  // generations, and generations deliberately do not survive an eviction.
  producerID?: string
}
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

function json(value: unknown, status = 200, headers?: Record<string, string>): Response {
  return Response.json(value, {
    status,
    headers: { 'cache-control': 'no-store', ...headers },
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
  if (isMutationRetryError(error)) {
    // the reason travels in `details` so a caller can pace on why it was
    // refused. it used to arrive as an acknowledged app error, which cost a
    // ledger write per refusal and consumed the mutation.
    return {
      error: errorMessage(error),
      ...(error.details === undefined ? {} : { details: error.details }),
      retryAfterMs: error.retryAfterMs,
    }
  }
  if (
    typeof error === 'object' &&
    error !== null &&
    'retryAfterMs' in error &&
    typeof error.retryAfterMs === 'number'
  ) {
    return { error: errorMessage(error), retryAfterMs: error.retryAfterMs }
  }
  return { error: errorMessage(error) }
}

// one response builder for every refusal, so a rejection that tells the caller
// when to come back always says so in the header an HTTP client already reads.
function errorResponse(error: unknown): Response {
  const body = errorBody(error)
  const retryAfterMs = body.retryAfterMs
  // whole seconds on the wire, rounded up: rounding down invites the caller
  // back while the window it has to wait out is still open.
  return json(
    body,
    statusOf(error),
    typeof retryAfterMs === 'number'
      ? { 'retry-after': String(Math.ceil(retryAfterMs / 1000)) }
      : undefined
  )
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

async function boundedRequestObject(
  request: Request,
  maxBytes: number,
  timeoutMs: number
): Promise<Record<string, unknown>> {
  try {
    const bytes = await readBoundedStream(
      request.body,
      maxBytes,
      AbortSignal.timeout(timeoutMs)
    )
    const value = JSON.parse(new TextDecoder().decode(bytes)) as unknown
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
      throw requestError('request body must be a JSON object')
    }
    return value as Record<string, unknown>
  } catch (error) {
    if (error instanceof UpstreamResponseLimitError) {
      throw requestError(`request body exceeded ${maxBytes} bytes`, 413)
    }
    if (error instanceof SyntaxError) throw requestError('invalid JSON request body')
    throw error
  }
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

async function forwardedSyncRequest(request: Request): Promise<{
  claims: NormalizedClaims
  body: Record<string, unknown>
  request: Request
}> {
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
    body: body as Record<string, unknown>,
    request: jsonBodyRequest(request, headers, body),
  }
}

async function namespaceHash(namespace: string): Promise<string> {
  const bytes = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(namespace))
  return Array.from(new Uint8Array(bytes).slice(0, 8), (byte) =>
    byte.toString(16).padStart(2, '0')
  ).join('')
}

type WorkerStage = 'authenticate' | 'sync_do_forward'

async function timeWorkerStage<Value>(
  fields: {
    hostVersion: string
    requestKind: string
    stage: WorkerStage
    namespaceHash: string | null
    sampled: boolean
  },
  work: () => Value | Promise<Value>
): Promise<Value> {
  const started = performance.now()
  let waited = false
  const emit = (outcome: 'success' | 'error' | 'waiting', value?: Value): void => {
    try {
      console.log(
        JSON.stringify({
          event: 'sync_worker_stage',
          hostVersion: fields.hostVersion,
          requestKind: fields.requestKind,
          stage: fields.stage,
          outcome,
          durationMs: Math.round((performance.now() - started) * 1_000) / 1_000,
          namespaceHash: fields.namespaceHash,
          status: value instanceof Response ? value.status : null,
          sampleRate:
            outcome === 'success' && !waited ? WORKER_STAGE_TELEMETRY_SAMPLE_RATE : 1,
        })
      )
    } catch {}
  }
  const waitingTimer = setTimeout(() => {
    waited = true
    emit('waiting')
  }, WORKER_STAGE_WAITING_MS)
  try {
    const value = await work()
    clearTimeout(waitingTimer)
    if (fields.sampled || waited) emit('success', value)
    return value
  } catch (error) {
    clearTimeout(waitingTimer)
    emit('error')
    throw error
  }
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
): ExportedHandler<Env> & {
  notify(env: Env, namespace: string): Promise<Response>
} {
  validateSyncHostConfig(config)
  const requestTimeoutMs =
    config.upstream?.requestTimeoutMs ?? DEFAULT_UPSTREAM_REQUEST_TIMEOUT_MS
  const maxRequestBytes =
    config.upstream?.maxRequestBytes ?? DEFAULT_UPSTREAM_MAX_REQUEST_BYTES
  const allowedOrigins = new Set(config.allowedOrigins ?? [])
  const upstreamPath = (namespace: string): string | null => {
    if (!config.upstream) return null
    const path =
      typeof config.upstream.namespacePath === 'function'
        ? config.upstream.namespacePath(namespace)
        : config.upstream.namespacePath
    if (!path.startsWith('/')) {
      throw new Error('upstream namespacePath must be an absolute path')
    }
    return path.replace(/\/$/, '')
  }
  const corsHeaders = (origin: string): Record<string, string> => ({
    'access-control-allow-origin': origin,
    vary: 'origin',
  })
  return {
    async fetch(request, env): Promise<Response> {
      const requestOrigin = request.headers.get('origin')
      const corsOrigin =
        requestOrigin && allowedOrigins.has(requestOrigin) ? requestOrigin : null

      // answer allowed preflights before any routing or authentication: a
      // preflight carries no Authorization header, so the auth wall below 401s
      // it and the browser never sends the real request.
      if (
        corsOrigin &&
        request.method === 'OPTIONS' &&
        request.headers.has('access-control-request-method')
      ) {
        return new Response(null, {
          status: 204,
          headers: {
            ...corsHeaders(corsOrigin),
            'access-control-allow-methods': 'GET, POST, OPTIONS',
            'access-control-allow-headers': 'authorization, content-type',
            'access-control-max-age': '86400',
          },
        })
      }

      const withCors = (response: Response): Response => {
        // a websocket upgrade has an immutable response; the socket handshake
        // is not CORS-gated anyway.
        if (!corsOrigin || response.webSocket) return response
        const wrapped = new Response(response.body, response)
        for (const [name, value] of Object.entries(corsHeaders(corsOrigin))) {
          wrapped.headers.set(name, value)
        }
        return wrapped
      }

      const handle = async (): Promise<Response> => {
        const namespace = config.namespace(request)
        if (!namespace) return new Response('orez sync-cf-host', { status: 200 })

        const route = routeAfterNamespace(new URL(request.url).pathname)
        const requestKind = route.startsWith('/') ? route.slice(1) : route
        const sampled = Math.random() < WORKER_STAGE_TELEMETRY_SAMPLE_RATE
        const isAdmin = route.startsWith('/admin/')
        let wakeUserID: string | null = null
        if (route === '/wake') {
          let wakeRequest: Request = request
          const protocol = request.headers.get('sec-websocket-protocol')?.trim()
          const encodedAuth =
            protocol?.startsWith('orez-auth.') === true
              ? protocol.slice('orez-auth.'.length)
              : null
          if (
            !request.headers.has('authorization') &&
            encodedAuth &&
            /^[A-Za-z0-9_-]+$/.test(encodedAuth)
          ) {
            try {
              const base64 = encodedAuth.replaceAll('-', '+').replaceAll('_', '/')
              const binary = globalThis.atob(
                base64.padEnd(Math.ceil(base64.length / 4) * 4, '=')
              )
              const authToken = new TextDecoder().decode(
                Uint8Array.from(binary, (character) => character.charCodeAt(0))
              )
              if (authToken) {
                const wakeHeaders = new Headers(request.headers)
                wakeHeaders.set('authorization', `Bearer ${authToken}`)
                wakeRequest = new Request(request, { headers: wakeHeaders })
              }
            } catch {
              // malformed subprotocol credentials remain unauthenticated
            }
          }
          const wake = await config.authorizeWake(wakeRequest, env)
          if (!wake) return json({ error: 'missing wake capability' }, 401)
          if (typeof wake === 'object') wakeUserID = wake.userID
          // A namespace that streams fields authorizes every subscription against
          // this userID, so a capability that does not carry one cannot open the
          // socket. Failing here names the cause; accepting it would produce a
          // wake-only socket whose subscriptions silently never deliver.
          if (config.streamingManifest && !wakeUserID) {
            return json({ error: 'wake capability must identify a user' }, 401)
          }
        } else if (route === '/notify') {
          if (!(await config.authorizeNotify(request, env))) {
            return json({ error: 'forbidden' }, 403)
          }
        } else if (isAdmin) {
          const authorized = config.authorizeAdmin
            ? await config.authorizeAdmin(request, env)
            : Boolean(env.ADMIN_KEY) &&
              request.headers.get('x-admin-key') === env.ADMIN_KEY
          if (!authorized) return json({ error: 'forbidden' }, 403)
        }

        const headers = new Headers(request.headers)
        headers.delete(NAMESPACE_HEADER)
        headers.delete(UPSTREAM_PATH_HEADER)
        headers.delete(IDENTITY_HEADER)
        headers.delete(NOTIFY_IF_SUBSCRIBED_HEADER)
        let forwardedBody: ForwardedSyncBody | null = null
        // /wake and /realtime/produce are both websocket upgrades and have no
        // body to put claims in. wake authentication is normalized from its
        // WebSocket subprotocol above; each route has its own authorization check
        // before either reaches the Durable Object.
        if (
          !isAdmin &&
          route !== '/wake' &&
          route !== '/notify' &&
          route !== '/realtime/produce'
        ) {
          // an authenticate throw is an availability answer, not an identity
          // verdict: the identity callback was reachable but told us to come
          // back (e.g. a just-created namespace whose row is still committing).
          // letting it propagate used to kill the whole request as an unhandled
          // exception, which reached clients as an opaque 500 with no
          // retry-after. map it through errorResponse so a status/retryAfterMs
          // the callback attached survives to the wire.
          let claims: Awaited<ReturnType<typeof config.authenticate>>
          try {
            claims = await timeWorkerStage(
              {
                hostVersion: config.hostVersion,
                requestKind,
                stage: 'authenticate',
                namespaceHash: null,
                sampled,
              },
              () => config.authenticate(request, env)
            )
          } catch (error) {
            return errorResponse(error)
          }
          if (
            !claims ||
            typeof claims.userID !== 'string' ||
            claims.userID.length === 0
          ) {
            return json({ error: 'missing authentication' }, 401)
          }
          if (!(await config.authorize(request, claims, namespace, env))) {
            return json({ error: 'forbidden' }, 403)
          }
          if ((route === '/pull' || route === '/push') && request.method === 'POST') {
            try {
              const body = await boundedRequestObject(
                request,
                maxRequestBytes,
                requestTimeoutMs
              )
              forwardedBody = { claims, body }
            } catch (error) {
              return errorResponse(error)
            }
          }
        }
        // Only the userID travels, because it is the only part the DO must be
        // unable to doubt. The socket also carries a clientID and clientGroupID,
        // but those are the client's own assertion and the engine checks the
        // group against this userID before it will read a single row, so there is
        // nothing gained by moving them here.
        if (wakeUserID) headers.set(IDENTITY_HEADER, encodeURIComponent(wakeUserID))
        const hashedNamespace = await namespaceHash(namespace)
        headers.set(NAMESPACE_HEADER, hashedNamespace)
        try {
          const path = upstreamPath(namespace)
          if (path !== null) headers.set(UPSTREAM_PATH_HEADER, path)
        } catch (error) {
          return errorResponse(error)
        }

        const forwarded = forwardedBody
          ? jsonBodyRequest(request, headers, forwardedBody)
          : new Request(request, { headers })
        const id = env.SYNC_DO.idFromName(namespace)
        return timeWorkerStage(
          {
            hostVersion: config.hostVersion,
            requestKind,
            stage: 'sync_do_forward',
            namespaceHash: hashedNamespace,
            sampled,
          },
          () => env.SYNC_DO.get(id).fetch(forwarded)
        )
      }
      return withCors(await handle())
    },
    async notify(env, namespace): Promise<Response> {
      if (!namespace) throw new TypeError('sync notification namespace is required')
      const path = upstreamPath(namespace)
      if (path === null) throw new Error('sync notifications require an upstream feed')
      const headers = new Headers({
        [NAMESPACE_HEADER]: await namespaceHash(namespace),
        [UPSTREAM_PATH_HEADER]: path,
        [NOTIFY_IF_SUBSCRIBED_HEADER]: '1',
      })
      const request = new Request('https://orez-sync.internal/namespace/notify', {
        method: 'POST',
        headers,
      })
      return env.SYNC_DO.get(env.SYNC_DO.idFromName(namespace)).fetch(request)
    },
  }
}

export interface SyncDurableObjectInstance<
  Env extends SyncHostEnv,
> extends DurableObject<Env> {
  fetch(request: Request): Promise<Response>
}

export interface SyncDurableObjectConstructor<Env extends SyncHostEnv> {
  new (ctx: DurableObjectState, env: Env): SyncDurableObjectInstance<Env>
}

/** Create the namespace Durable Object class for one bundled consumer config. */
export function createSyncDurableObject<
  Env extends SyncHostEnv,
  S extends Schema = Schema,
>(config: SyncHostConfig<Env, S>): SyncDurableObjectConstructor<Env> {
  validateSyncHostConfig(config)
  const compileQuery = createQueryCompiler(config.schema)
  const defaultRetainChanges = String(config.retainChanges ?? 4_096)
  const idleTeardownMs = config.idleTeardownMs ?? 5_000
  // A CF fan-out wakes every client into an HTTP pull. Give concurrent writer
  // requests a real batching window so a storm burst creates one pull wave.
  const wakeCoalesceMs = config.wakeCoalesceMs ?? 25
  const upstreamIntervalMs = config.upstream?.intervalMs ?? 15_000
  const upstreamLimit = config.upstream?.changeLimit ?? 1_000
  const upstreamRequestTimeoutMs =
    config.upstream?.requestTimeoutMs ?? DEFAULT_UPSTREAM_REQUEST_TIMEOUT_MS
  const upstreamMaxResponseBytes =
    config.upstream?.maxResponseBytes ?? DEFAULT_UPSTREAM_MAX_RESPONSE_BYTES
  const upstreamMaxRequestBytes =
    config.upstream?.maxRequestBytes ?? DEFAULT_UPSTREAM_MAX_REQUEST_BYTES
  const ingestBudgetRows = config.upstream?.ingestBudgetRows ?? 150_000
  const ingestBudgetWindowMs = config.upstream?.ingestBudgetWindowMs ?? 5 * 60_000
  const ingestBackoffMs = config.upstream?.ingestBackoffMs ?? 1_000
  const ingestMaxBackoffMs = config.upstream?.ingestMaxBackoffMs ?? 60_000
  const delegateMaxAttempts = config.delegatedPushRetry?.maxAttempts ?? 3
  const delegateInitialBackoffMs = config.delegatedPushRetry?.initialBackoffMs ?? 100
  const delegateMaxBackoffMs = config.delegatedPushRetry?.maxBackoffMs ?? 1_000
  // 30s, not a snappier number: delegate wall time under write-lane contention
  // is queueing, not compute (measured ~5s wall at ~90ms cpu), and an abort
  // cancels the app invocation mid-transaction. 5s canceled real pushes on any
  // seed heavier than trivial and each retry re-queued more contention
  // (production example apps, 2026-08-03).
  const delegateTimeoutMs = config.delegatedPushRetry?.timeoutMs ?? 30_000

  return class SyncDurableObject extends DurableObject<Env> {
    readonly #engineDb: SqlStorageSyncDb
    readonly #directSql: SqlStorageDirect
    readonly #mutatorSql: SqlStorageMutatorTransaction
    readonly #executor: SyncExecutor<S> | null
    #executorBeforeCommitFault: FaultKind | null = null
    #bootID = crypto.randomUUID()
    #initialized = false
    #initSkipped = false
    #lastRequestAt = 0
    #hibernations = 0
    #dropNextPushResponse = false
    #counters = freshCounters()
    #sqlBilling = { rowsRead: 0, rowsWritten: 0 }
    #pulling = new Set<string>()
    #activePullGroups = new Map<string, number>()
    #servingLag = new ServingLagTracker()
    #wakeOrigins = new Set<string>()
    #wakeRecipients = new Set<WebSocket>()
    #wakeTables = new Set<string>()
    #wakePromise: Promise<void> | null = null
    #lastWake: WakeStatus | null = null
    // Streaming fields. Null when the namespace configures no manifest, which
    // is every wake-only deployment: no hub is built and nothing below runs.
    #realtime: RealtimeSocketHost | null = null
    #realtimeConnections = new Map<WebSocket, HostConnection>()
    // Resolves once the sockets from a previous incarnation have been replayed
    // into this hub. Held as a promise rather than a flag so frames arriving
    // during the replay wait for it instead of racing past into an empty hub.
    #realtimeReady: Promise<void> | null = null
    #ingestPromise: Promise<number> | null = null
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
        this.#sqlBilling.rowsWritten += rows
        if (this.#recordingIngestBillable) this.#ingestBreaker.recordBillable(rows)
      }
      const recordRowsRead = (rows: number) => {
        this.#sqlBilling.rowsRead += rows
      }
      this.#engineDb = new SqlStorageSyncDb(
        ctx.storage.sql,
        recordRowsWritten,
        recordRowsRead,
        (failure) => {
          console.error(
            JSON.stringify({
              event: 'sync_sql_failure',
              hostVersion: config.hostVersion,
              ...failure,
            })
          )
        }
      )
      this.#directSql = new SqlStorageDirect(
        ctx.storage.sql,
        recordRowsWritten,
        recordRowsRead
      )
      this.#mutatorSql = new SqlStorageMutatorTransaction(
        this.#directSql,
        (ast, format) => this.#wasm(() => compileQuery(ast, format)),
        config.transactionQueryBudget
      )
      const database: ApplicationDatabase = {
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
              query: (sql, params) => {
                // exact helper statements use sqlite returning so the executor
                // can prove the physical primary keys that changed. Keep the
                // host's pre-commit fault boundary after those writes too.
                if (
                  /\b(?:INSERT|UPDATE|DELETE|REPLACE)\b/i.test(sql) &&
                  /\bRETURNING\b/i.test(sql)
                ) {
                  applicationWrite = true
                }
                return this.#mutatorSql.query(sql, params)
              },
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
      // Upstream-backed namespaces are initialized lazily so a notify for a
      // namespace with no wake subscribers remains a true zero-read/write
      // fast path. Local-execution namespaces have no such notify path, and
      // preserving eager initialization there matters for consumers that
      // subclass this Durable Object and handle an application route before
      // delegating to super.fetch().
      if (!config.upstream) {
        ctx.blockConcurrencyWhile(async () => this.#initialize())
      }
    }

    #initialize(): void {
      if (this.#initialized) return
      this.ctx.storage.transactionSync(() => {
        this.#directSql.exec(`CREATE TABLE IF NOT EXISTS _zsync_host_control (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        )`)
        // one durable fingerprint gates the whole schema pass. hibernation
        // reconstructs this object every few idle minutes, and re-running
        // consumer DDL plus both engine inits against an already-current
        // database costs ~1.5k billable reads per wake. everything the pass
        // depends on is in the fingerprint, so a build that changes any DDL
        // surface re-runs it exactly once per namespace.
        const initFingerprint = JSON.stringify([
          config.hostVersion,
          this.#wasm(() => engine_schema_revision()),
          config.schema,
          config.initialize.toString(),
        ])
        this.#initSkipped = this.#controlGet('initFingerprint') === initFingerprint
        if (!this.#initSkipped) {
          config.initialize(this.#directSql)
          this.#directSql.exec(
            "INSERT OR IGNORE INTO _zsync_host_control (key, value) VALUES ('writerEnabled', '1')"
          )
          this.#wasm(() => engine_init_schema(this.#engineDb, config.schema))
          this.#wasm(() => engine_init_query_schema(this.#engineDb))
          this.#controlSet('initFingerprint', initFingerprint)
        }
        const ingestBreakerReason = this.#controlGet('ingestBreakerReason')
        if (
          ingestBreakerReason === 'ingestBudgetExceeded' ||
          ingestBreakerReason === 'ingestCursorStalled' ||
          ingestBreakerReason === 'ingestTableUnpublished'
        ) {
          this.#ingestBreaker.restore(
            ingestBreakerReason,
            Number(this.#controlGet('ingestBreakerRetryAt')),
            Number(this.#controlGet('ingestBreakerTrips'))
          )
        }
      })
      this.#initialized = true
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

    #simulateIdleTeardown(now: number): void {
      if (this.#lastRequestAt > 0 && now - this.#lastRequestAt >= idleTeardownMs) {
        this.#bootID = crypto.randomUUID()
        this.#hibernations++
        this.#counters = freshCounters()
        this.#sqlBilling = { rowsRead: 0, rowsWritten: 0 }
        this.#pulling.clear()
        this.#wakeOrigins.clear()
        this.#wakeRecipients.clear()
        this.#wakeTables.clear()
        this.#wakePromise = null
        this.#lastWake = null
        // Real hibernation reconstructs the object, so the hub and every
        // connection handle are gone while the sockets stay open. Modelling
        // that here is what makes rehydration reachable from a test: leaving
        // the hub in place would make the simulation pass for a reason the
        // real runtime never gives it.
        this.#realtime = null
        this.#realtimeConnections.clear()
        this.#realtimeReady = null
      }
      this.#lastRequestAt = now
    }

    // the admin-set namespace knobs (writer, retention) live in
    // _zsync_host_control, NOT in instance fields: a real eviction recreates
    // this instance, and an in-memory override silently reverting to the
    // config default mid-run changes namespace behavior under the client.
    #controlGet(key: string): string | null {
      const row = this.#directSql.query<{ value: string }>(
        'SELECT value FROM _zsync_host_control WHERE key = ?',
        [key]
      )[0]
      return row?.value ?? null
    }

    #controlSet(key: string, value: string): void {
      // the WHERE guard makes a same-value set free: upstreamPath arrives on
      // every forwarded request, and an unguarded upsert billed one row
      // written per request for a value that almost never changes.
      this.#directSql.exec(
        'INSERT INTO _zsync_host_control (key, value) VALUES (?, ?) ' +
          'ON CONFLICT(key) DO UPDATE SET value = excluded.value ' +
          'WHERE value <> excluded.value',
        [key, value]
      )
    }

    // the set of tables this replica has actually received published changes
    // for. it is the baseline that separates "the app never publishes this
    // table" from "the feed stopped publishing a table we were syncing", and it
    // has to survive eviction, so it lives in _zsync_host_control rather than
    // an instance field. it only ever grows, and a write only happens the first
    // time a table shows up.
    #publishedSeenCache: Set<string> | null = null

    #publishedSeen(): Set<string> {
      if (!this.#publishedSeenCache) {
        const stored = this.#controlGet('publishedTablesSeen')
        let names: unknown = null
        if (stored !== null) {
          try {
            names = JSON.parse(stored)
          } catch {
            names = null
          }
        }
        this.#publishedSeenCache = new Set(
          Array.isArray(names)
            ? names.filter((name): name is string => typeof name === 'string')
            : []
        )
      }
      return this.#publishedSeenCache
    }

    #notePublishedTables(tables: Iterable<string>): void {
      const seen = this.#publishedSeen()
      let added = false
      for (const table of tables) {
        if (seen.has(table)) continue
        seen.add(table)
        added = true
      }
      if (added) {
        this.#controlSet('publishedTablesSeen', JSON.stringify([...seen].sort()))
      }
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

    #activeClientGroupIDs(): Set<string> {
      const groups = new Set(this.#activePullGroups.keys())
      for (const socket of this.ctx.getWebSockets(WAKE_SUBSCRIBER_TAG)) {
        const clientGroupID = socketAttachment(socket)?.identity?.clientGroupID
        if (clientGroupID) groups.add(clientGroupID)
      }
      return groups
    }

    #beginActivePull(clientGroupID: string): void {
      this.#activePullGroups.set(
        clientGroupID,
        (this.#activePullGroups.get(clientGroupID) ?? 0) + 1
      )
    }

    #endActivePull(clientGroupID: string): void {
      const count = this.#activePullGroups.get(clientGroupID) ?? 0
      if (count <= 1) this.#activePullGroups.delete(clientGroupID)
      else this.#activePullGroups.set(clientGroupID, count - 1)
    }

    async #fetchUpstreamJson(endpoint: URL) {
      const result = await fetchBoundedUpstreamJson(
        this.#serviceBinding(),
        endpoint.toString(),
        { headers: { host: endpoint.host } },
        {
          timeoutMs: upstreamRequestTimeoutMs,
          maxBytes: upstreamMaxResponseBytes,
        }
      )
      const sourceTimeMs =
        result.body &&
        typeof result.body === 'object' &&
        !Array.isArray(result.body) &&
        typeof (result.body as { sourceTimeMs?: unknown }).sourceTimeMs === 'number'
          ? (result.body as { sourceTimeMs: number }).sourceTimeMs
          : null
      if (sourceTimeMs !== null) {
        this.#servingLag.recordClockSkew(
          sourceTimeMs,
          result.sendTimeMs,
          result.receiveTimeMs
        )
      }
      return result
    }

    async #upstreamWriteBudgetStatus(): Promise<Response> {
      if (!config.upstream) return json({ error: 'upstream is not configured' }, 404)
      const path = this.#controlGet('upstreamPath')
      if (path === null) return json({ error: 'upstream path is not known yet' }, 409)
      const endpoint = new URL(`${path}/_orez/write-budget`, 'https://upstream.invalid')
      const { response, body } = await this.#fetchUpstreamJson(endpoint)
      if (!response.ok) {
        return json(
          {
            error: 'upstream write-budget status unavailable',
            upstreamStatus: response.status,
          },
          502
        )
      }
      return json(body)
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
      reason: 'ingestBudgetExceeded' | 'ingestCursorStalled' | 'ingestTableUnpublished',
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
      const { response, body } = await this.#fetchUpstreamJson(endpoint)
      if (!response.ok) {
        throw requestError(
          `upstream snapshot page returned ${response.status}`,
          response.status >= 500 ? 502 : response.status
        )
      }
      const page = body as Partial<SnapshotPage>
      if (
        !Number.isSafeInteger(page.watermark) ||
        Number(page.watermark) < 0 ||
        !Array.isArray(page.rows) ||
        (page.nextCursor !== null && typeof page.nextCursor !== 'string')
      ) {
        throw new Error('invalid upstream snapshot page response')
      }
      // a snapshot answer is upstream stating outright whether it publishes this
      // table, which makes it the one place that can build a complete baseline:
      // every modelled table is paged here, not just the ones that happen to be
      // written while this replica is watching.
      if (!page.unpublishedTables?.includes(table)) this.#notePublishedTables([table])
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
        let oldestLiveCommitTimeMs: number | undefined
        const changedTables = new Set<string>()
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
              changedTables.add(table)
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
            const { response, body } = await this.#fetchUpstreamJson(endpoint)
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
            const batch = body as UpstreamBatch
            if (!Number.isSafeInteger(batch.watermark) || !Array.isArray(batch.changes)) {
              throw new Error('invalid upstream changes response')
            }
            const catchupUnpublished = unpublishedRegressions(
              batch,
              config.schema,
              this.#publishedSeen()
            )
            if (catchupUnpublished.length > 0) {
              this.#tripIngest('ingestTableUnpublished', {
                phase: 'snapshot_catchup',
                generation: activeProgress.generation,
                cursor,
                batchWatermark: batch.watermark,
                changeRows: batch.changes.length,
                tables: catchupUnpublished,
              })
            }
            const catchupTables = upstreamBatchTables(batch)
            for (const table of catchupTables) changedTables.add(table)
            this.#notePublishedTables(catchupTables)
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
          const { response, body } = await this.#fetchUpstreamJson(endpoint)
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
          const batch = body as UpstreamBatch
          if (!Number.isSafeInteger(batch.watermark) || !Array.isArray(batch.changes)) {
            throw new Error('invalid upstream changes response')
          }
          const batchCommitTimeMs = batch.oldestCommitTimeMs
          if (
            typeof batchCommitTimeMs === 'number' &&
            Number.isFinite(batchCommitTimeMs)
          ) {
            oldestLiveCommitTimeMs =
              oldestLiveCommitTimeMs === undefined
                ? batchCommitTimeMs
                : Math.min(oldestLiveCommitTimeMs, batchCommitTimeMs)
          }
          const unpublished = unpublishedRegressions(
            batch,
            config.schema,
            this.#publishedSeen()
          )
          if (unpublished.length > 0) {
            this.#tripIngest('ingestTableUnpublished', {
              phase: 'changes',
              cursor,
              batchWatermark: batch.watermark,
              changeRows: batch.changes.length,
              tables: unpublished,
            })
          }
          const batchTables = upstreamBatchTables(batch)
          for (const table of batchTables) changedTables.add(table)
          this.#notePublishedTables(batchTables)
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
        if (oldestLiveCommitTimeMs !== undefined) {
          const activeGroups = this.#activeClientGroupIDs()
          if (endingWatermark !== startingWatermark || total > 0) {
            this.#servingLag.onVersionReady(
              endingWatermark,
              oldestLiveCommitTimeMs,
              activeGroups
            )
          } else {
            const servedAt = Date.now()
            for (const _clientGroupID of activeGroups) {
              this.#servingLag.recordNoChange(oldestLiveCommitTimeMs, servedAt)
            }
          }
        }
        if (snapshotCompleted || total > 0 || endingWatermark !== startingWatermark) {
          await this.#enqueueWake('__upstream__', changedTables)
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
        const attemptTimeoutMs = provisioning
          ? Math.max(delegateTimeoutMs, 25_000)
          : delegateTimeoutMs
        // ask the signal, not the rejection: this signal aborts for exactly one
        // reason, so `aborted` names a timeout without depending on how the
        // runtime shapes its DOMException.
        const timeout = AbortSignal.timeout(attemptTimeoutMs)
        try {
          response = await binding.fetch(endpoint.toString(), {
            method: 'POST',
            headers,
            body,
            signal: timeout,
          })
        } catch (error) {
          lastError = error
        }
        const timedOut = response === null && timeout.aborted
        if (
          !shouldRetryDelegatedPush(
            response?.status ?? null,
            attempt,
            delegateMaxAttempts,
            timedOut
          )
        ) {
          if (response) return response
          // the retry log below is the only place a delegated push failure was
          // ever named, and a terminal failure skips it. log here too, or the
          // host answers 500 and says nothing about why.
          console.warn(
            JSON.stringify({
              event: 'sync_delegated_push_failed',
              attempt,
              maxAttempts: delegateMaxAttempts,
              timedOut,
              timeoutMs: attemptTimeoutMs,
              error: errorMessage(lastError),
            })
          )
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

    #enqueueWake(originClientID: string, tables: Iterable<string> = []): Promise<void> {
      this.#wakeOrigins.add(originClientID)
      for (const table of tables) this.#wakeTables.add(table)
      for (const socket of this.ctx.getWebSockets(WAKE_SUBSCRIBER_TAG)) {
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
          const tables = [...this.#wakeTables].sort()
          this.#wakeTables = new Set()
          this.#counters.wakeBatches++
          let sent = 0
          const sockets = this.ctx.getWebSockets(WAKE_SUBSCRIBER_TAG)
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
          const wakeStatus: WakeStatus = {
            at: Date.now(),
            tables,
            socketCount: sockets.length,
            originCount: origins.size,
            sent,
            eligibleRecipients: recipients.size,
            coalesceMs: fanoutStarted - queuedAt,
            fanoutMs: performance.now() - fanoutStarted,
          }
          this.#lastWake = wakeStatus
          console.log(
            JSON.stringify({
              event: 'sync_wake',
              hostVersion: config.hostVersion,
              ...wakeStatus,
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
        const transformVersion =
          typeof config.queryTransformVersion === 'function'
            ? config.queryTransformVersion(claims)
            : (config.queryTransformVersion ?? 0)
        if (!Number.isSafeInteger(transformVersion) || transformVersion < 0) {
          throw new TypeError('queryTransformVersion must be a non-negative safe integer')
        }
        let response: Record<string, unknown>
        {
          const queries = body.queries as
            | { version?: unknown; patch?: unknown }
            | undefined
          if (queries && Array.isArray(queries.patch)) {
            // named queries resolve in-process against the app's ordinary
            // Zero registry, synchronously: patch application order is
            // arrival order by construction, with no cross-request await
            // between resolution and the engine transaction.
            const patch = resolveQueryPatch(
              queries.patch,
              config.queries,
              claims,
              transformVersion,
              requestError,
              config.tolerateUnknownQueries === true
            )
            body = { ...body, queries: { ...queries, patch } }
          }
          body = { ...body, _serverQueryTransformVersion: transformVersion }
          const clientID = typeof body.clientID === 'string' ? body.clientID : ''
          this.#pulling.add(clientID)
          try {
            const txStarted = performance.now()
            const duringFault = this.#takeFault('pull_during_tx')
            response = this.ctx.storage.transactionSync(() => {
              const result = this.#wasm(() =>
                engine_handle_query_pull(
                  this.#engineDb,
                  config.schema,
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
        }
        const afterPullFault = this.#takeFault('pull_after_commit')
        if (afterPullFault) throw this.#faultError(afterPullFault, 'pull_after_commit')
        const patch = Array.isArray(response.rowsPatch) ? response.rowsPatch : []
        const queriesBody = body.queries as { patch?: unknown[] } | undefined
        const queryPuts = Array.isArray(queriesBody?.patch)
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
        const result = json(response)
        const clientGroupID =
          typeof body.clientGroupID === 'string' ? body.clientGroupID : ''
        this.#servingLag.onVersionServed(clientGroupID, response.cookie)
        return result
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
        await readBoundedStream(
          request.body,
          upstreamMaxRequestBytes,
          AbortSignal.timeout(upstreamRequestTimeoutMs)
        )
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
          const boundedBytes = await readBoundedStream(
            request.body,
            upstreamMaxRequestBytes,
            AbortSignal.timeout(upstreamRequestTimeoutMs)
          )
          const bytes = boundedBytes.buffer.slice(
            boundedBytes.byteOffset,
            boundedBytes.byteOffset + boundedBytes.byteLength
          ) as ArrayBuffer
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
            const upstreamErrorBody = await readBoundedStream(
              upstreamResponse.body,
              upstreamMaxResponseBytes,
              AbortSignal.timeout(upstreamRequestTimeoutMs)
            )
            const errorBytes = upstreamErrorBody.buffer.slice(
              upstreamErrorBody.byteOffset,
              upstreamErrorBody.byteOffset + upstreamErrorBody.byteLength
            ) as ArrayBuffer
            return new Response(errorBytes, upstreamResponse)
          }
          const upstreamBody = (await readBoundedJsonResponse(
            upstreamResponse,
            upstreamMaxResponseBytes,
            AbortSignal.timeout(upstreamRequestTimeoutMs)
          )) as DelegatedPushBody
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
          return errorResponse(error)
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
        return errorResponse(error)
      }
    }

    // The hub for this incarnation, built on first use. Null unless the
    // namespace configures a manifest, so a wake-only deployment never pays for
    // one and every realtime path below is skipped.
    #realtimeHost(): RealtimeSocketHost | null {
      const manifest = config.streamingManifest
      if (!manifest) return null
      if (this.#realtime) return this.#realtime
      this.#realtime = createSocketHost({
        manifest,
        // Answered from this object's own SQLite, which is the same durable
        // membership the client's query pull reads. A field is streamed to a
        // client exactly when the row carrying it is already being synced to
        // that client, so streaming can never widen what somebody can see.
        authorizeSubscribe: (identity, topic) => {
          const result = this.#wasm(() =>
            engine_authorize_realtime_subscription(
              this.#engineDb,
              config.schema,
              identity.clientGroupID,
              identity.userID,
              topic.table,
              topic.key
            )
          ) as { ownsGroup: boolean; authorized: boolean }
          if (result.authorized) return { status: 'active' }
          // the group is this user's, but the row is not in its membership
          // yet. That is the optimistic-row race, not a denial: the client
          // holds a row from its own unacked mutation, and retries after the
          // pull that records it.
          if (result.ownsGroup) return { status: 'pending' }
          return {
            status: 'denied',
            reason: 'row is not in this client group',
          }
        },
      })
      return this.#realtime
    }

    // Replay the sockets that outlived the previous incarnation. Every open
    // socket is replayed, not just the one that woke us: a producer's frames
    // must reach every subscriber of a topic, so restoring only the socket that
    // happened to send first would silently drop the rest.
    #realtimeRehydrate(host: RealtimeSocketHost): Promise<void> {
      if (this.#realtimeReady) return this.#realtimeReady
      this.#realtimeReady = (async () => {
        const subscribers: {
          socket: WebSocket
          identity: RealtimeIdentity
          connectionID: string
          topics: RealtimeTopic[]
        }[] = []
        for (const socket of this.ctx.getWebSockets()) {
          if (this.#realtimeConnections.has(socket)) continue
          const attachment = socketAttachment(socket)
          if (!attachment) continue
          if (attachment.producerID) {
            // Generations are ephemeral by design, so nothing is restored here
            // beyond the channel itself; the producer opens a new generation
            // and the durable row covers the gap either way.
            this.#realtimeConnections.set(
              socket,
              host.acceptProducer(socket, attachment.producerID)
            )
            continue
          }
          if (!attachment.identity) continue
          subscribers.push({
            socket,
            identity: attachment.identity,
            connectionID: attachment.identity.clientID,
            topics: attachment.topics ?? [],
          })
        }
        const restored = await host.rehydrate(subscribers)
        subscribers.forEach((entry, index) => {
          this.#realtimeConnections.set(entry.socket, restored[index])
        })
      })()
      return this.#realtimeReady
    }

    // A socket's topics are the only realtime state that must survive an
    // eviction, so they are rewritten whenever they change rather than on a
    // timer: an eviction is not announced.
    #realtimePersist(socket: WebSocket, connection: HostConnection): void {
      const attachment = socketAttachment(socket)
      if (!attachment) return
      socket.serializeAttachment({
        ...attachment,
        topics: [...connection.topics()],
      } satisfies SocketAttachment)
    }

    #wake(request: Request): Response {
      if (request.headers.get('upgrade')?.toLowerCase() !== 'websocket') {
        return json({ error: 'websocket upgrade required' }, 426)
      }
      const params = new URL(request.url).searchParams
      const clientID = params.get('clientID')
      if (!clientID) return json({ error: 'clientID is required' }, 400)
      const pair = new WebSocketPair()
      const [client, server] = Object.values(pair)
      // The userID is the worker's, taken from the authenticated request. The
      // group is the client's own claim, and stays a claim: every subscription
      // is checked against this userID before a row is read, so asserting
      // someone else's group buys nothing.
      const encodedUserID = request.headers.get(IDENTITY_HEADER)
      const clientGroupID = params.get('clientGroupID')
      const identity =
        encodedUserID && clientGroupID
          ? {
              userID: decodeURIComponent(encodedUserID),
              clientID,
              clientGroupID,
            }
          : undefined
      server.serializeAttachment({
        clientID,
        identity,
      } satisfies SocketAttachment)
      this.ctx.acceptWebSocket(server, [WAKE_SUBSCRIBER_TAG, `client:${clientID}`])
      if (identity) {
        const host = this.#realtimeHost()
        if (host) {
          this.#realtimeConnections.set(
            server,
            host.acceptSubscriber(server, identity, clientID)
          )
        }
      }
      // The alarm is only a safety net for an actively connected consumer.
      // A namespace with no wake socket has nobody to notify; its next pull or
      // push ingests synchronously. Arming from construction made every
      // namespace poll upstream forever after its first request, even across
      // DO eviction, producing a permanent rows-written floor at zero traffic.
      this.#armUpstreamAlarm()
      const protocol = request.headers.get('sec-websocket-protocol')
      return new Response(null, {
        status: 101,
        headers: protocol ? { 'Sec-WebSocket-Protocol': protocol } : undefined,
        webSocket: client,
      } as ResponseInit & { webSocket: WebSocket })
    }

    #admin(
      route: string,
      request: Request,
      upstreamPath: string | null
    ): Promise<Response> | Response {
      if (route === '/admin/health') return json({ ok: true })
      if (route === '/admin/sql-billing') return json({ ...this.#sqlBilling })
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
            initSkipped: this.#initSkipped,
            idleTeardownMs,
            hibernations: this.#hibernations,
            databaseSizeBytes: this.ctx.storage.sql.databaseSize,
            connectedWakeSockets: this.ctx.getWebSockets(WAKE_SUBSCRIBER_TAG).length,
            upstreamAlarmAt,
            writerEnabled: this.#writerEnabled(),
            wasmMemoryBytes: engine_memory_bytes(),
            heapUsedBytes: heap?.usedJSHeapSize ?? null,
            heapTotalBytes: heap?.totalJSHeapSize ?? null,
            heapLimitBytes: heap?.jsHeapSizeLimit ?? null,
            engine: this.#engineStateBestEffort(),
            sqlBillingSinceBoot: this.#sqlBilling,
            counters: this.#counters,
            ingestBreaker: this.#ingestBreaker.status(),
            lastWake: this.#lastWake,
          })
        )
      }
      if (route === '/admin/sql') {
        return request.json().then((body) => {
          const { params, query } = body as {
            params?: unknown
            query?: string
          }
          if (typeof query !== 'string') return json({ error: 'query is required' }, 400)
          try {
            return json({
              rows: this.#directSql.query(query, decodeSqlParams(params)),
            })
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
            return errorResponse(error)
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
      const route = routeAfterNamespace(new URL(request.url).pathname)
      if (
        route === '/notify' &&
        request.method === 'POST' &&
        request.headers.get(NOTIFY_IF_SUBSCRIBED_HEADER) === '1' &&
        this.ctx.getWebSockets(WAKE_SUBSCRIBER_TAG).length === 0
      ) {
        return json({
          ok: true,
          applied: 0,
          skipped: 'no-wake-subscribers',
          rowsWritten: this.#sqlBilling.rowsWritten,
        })
      }
      this.#initialize()
      this.#simulateIdleTeardown(Date.now())
      const namespace = request.headers.get(NAMESPACE_HEADER) ?? 'unknown'
      const upstreamPath = this.#rememberUpstreamPath(request)
      if (route.startsWith('/admin/')) return this.#admin(route, request, upstreamPath)

      if (route === '/wake' && request.method === 'GET') return this.#wake(request)
      if (route === '/realtime/produce' && request.method === 'GET') {
        return this.#realtimeProduce(request, this.env)
      }
      if (route === '/notify' && request.method === 'POST') {
        try {
          const applied = await this.#ingestAfterCurrent(upstreamPath)
          return json({ ok: true, applied })
        } catch (error) {
          return errorResponse(error)
        }
      }

      if ((route === '/pull' || route === '/push') && request.method === 'POST') {
        let forwarded
        try {
          forwarded = await forwardedSyncRequest(request)
        } catch (error) {
          return errorResponse(error)
        }
        const clientGroupID =
          route === '/pull' && typeof forwarded.body.clientGroupID === 'string'
            ? forwarded.body.clientGroupID
            : null
        if (clientGroupID) this.#beginActivePull(clientGroupID)
        try {
          // pull and push both establish the upstream schema and snapshot barrier.
          try {
            await this.#ingest(upstreamPath)
          } catch (error) {
            return errorResponse(error)
          }
          if (route === '/pull') {
            return await this.#pull(forwarded.request, forwarded.claims, namespace)
          }
          return await this.#push(
            forwarded.request,
            forwarded.claims,
            namespace,
            upstreamPath
          )
        } finally {
          if (clientGroupID) this.#endActivePull(clientGroupID)
        }
      }
      return json({ error: 'not found' }, 404)
    }

    async alarm(): Promise<void> {
      if (this.ctx.getWebSockets(WAKE_SUBSCRIBER_TAG).length === 0) return
      this.#initialize()
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
        if (this.ctx.getWebSockets(WAKE_SUBSCRIBER_TAG).length > 0) {
          const retryAfterMs = this.#ingestBreaker.status().retryAfterMs
          await this.ctx.storage.setAlarm(
            Date.now() + Math.max(upstreamIntervalMs, retryAfterMs)
          )
        }
      }
    }

    // A producer channel. Publishing is a far stronger capability than waking,
    // so it needs its own authorization and gets no default: a namespace that
    // configures no authorizeProduce cannot be published to at all.
    async #realtimeProduce(request: Request, env: Env): Promise<Response> {
      if (request.headers.get('upgrade')?.toLowerCase() !== 'websocket') {
        return json({ error: 'websocket upgrade required' }, 426)
      }
      const host = this.#realtimeHost()
      if (!host) return json({ error: 'namespace streams no fields' }, 404)
      if (!config.authorizeProduce || !(await config.authorizeProduce(request, env))) {
        return json({ error: 'forbidden' }, 403)
      }
      const producerID =
        new URL(request.url).searchParams.get('producerID') ?? crypto.randomUUID()
      const pair = new WebSocketPair()
      const [client, server] = Object.values(pair)
      server.serializeAttachment({
        clientID: producerID,
        producerID,
      } satisfies SocketAttachment)
      this.ctx.acceptWebSocket(server, [`producer:${producerID}`])
      this.#realtimeConnections.set(server, host.acceptProducer(server, producerID))
      return new Response(null, { status: 101, webSocket: client })
    }

    async webSocketMessage(
      socket: WebSocket,
      message: string | ArrayBuffer
    ): Promise<void> {
      if (message === 'ping') {
        socket.send('pong')
        return
      }
      this.#initialize()
      const host = this.#realtimeHost()
      if (!host || typeof message !== 'string') return
      // A cold start left the hub empty while the socket stayed open, so the
      // subscriptions have to be replayed before this frame is applied.
      await this.#realtimeRehydrate(host)
      const connection = this.#realtimeConnections.get(socket)
      if (!connection) {
        // a wake-only socket (no clientGroupID on the upgrade) has no realtime
        // identity, so a frame here is a subscription that can never deliver.
        // Closing names the cause; ignoring it would look like streaming that
        // silently stopped.
        socket.close(1008, 'realtime frames require clientGroupID on the wake upgrade')
        return
      }
      connection.handleMessage(message)
      // handleMessage moves the owned-topic set synchronously before any async
      // delivery work starts, so the set is final when it returns.
      this.#realtimePersist(socket, connection)
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
      this.#realtimeDrop(socket)
      socketCloseQuietly(socket, echoable ? code : 1000, echoable ? reason : '')
    }

    webSocketError(socket: WebSocket, _error: unknown): void {
      this.#realtimeDrop(socket)
      socketCloseQuietly(socket, 1011, 'wake socket error')
    }

    // Release whatever the socket held: a subscriber's topics, or a producer's
    // generations. Dropping a producer reveals the durable row to everyone
    // watching it, so a crashed producer cannot strand a stale overlay.
    #realtimeDrop(socket: WebSocket): void {
      const connection = this.#realtimeConnections.get(socket)
      if (!connection) return
      this.#realtimeConnections.delete(socket)
      connection.close()
    }
  }
}
