/**
 * zero-cache embedded runner for cloudflare workers.
 *
 * runs zero-cache in-process with SINGLE_PROCESS=1, using bundler aliases
 * to swap Node.js dependencies for CF-compatible shims:
 *
 *   postgres           → orez/worker/shims/postgres-browser
 *                         (real postgres package over MessagePort)
 *   @rocicorp/zero-sqlite3 → orez/worker/shims/sqlite (DO SQLite)
 *   fastify            → orez/worker/shims/fastify   (route capture)
 *   ws                 → orez/worker/shims/ws        (CF WebSocket)
 *
 * the postgres MessagePort proxy is backed by DoBackend, so zero-cache still
 * uses its real PG wire protocol, but storage is Cloudflare DO SQLite instead
 * of PGlite.
 *
 * usage in a Durable Object:
 *
 *   import { startZeroCacheEmbedCF } from 'orez/worker'
 *
 *   // in ensureInitialized():
 *   const zc = await startZeroCacheEmbedCF({ ... })
 *
 *   // in DO fetch():
 *   return zc.handleRequest(request)
 *
 * the embed uses standard-accepted WebSockets. do not route these sockets
 * through Durable Object hibernation APIs; the in-process zero-cache proxy must
 * keep its live bridge for the connection lifetime.
 *
 * idle hibernation is achieved at a coarser grain instead: the DO watches
 * `connectionCount` and, once it hits 0 past a grace window, calls stop() to
 * tear the whole embed down. with no live bridge and no zero-cache timers left,
 * the DO is evicted and stops accruing GB-s; the next request cold-starts it
 * again from durable DO SQLite. see plans/cf-do-idle-hibernation.md and
 * `shouldHibernateIdleZeroCache` in ./zero-cache-do-idle.ts.
 */

import { EventEmitter } from 'node:events'

import { setLogLevel } from '../log.js'
import { createBrowserProxy, type BrowserProxy } from '../pg-proxy-browser.js'
import { DoBackend } from '../pg-proxy-do-backend.js'
import { resetReplicationState } from '../replication/handler.js'
import {
  DurableObjectWebSocketHandoff,
  type DurableObjectWebSocket,
  type DurableObjectWebSocketHandoffContext,
  type HandoffRequestMessage,
} from './durable-object-websocket-handoff.js'
import { sweepLeakedSqliteHandles } from './embed-generation.js'
import { createLocalSqlBackend } from './local-sql-backend.js'
import { resetFastifyRegistry } from './shims/fastify.js'
import { acquireZeroProcessEnv } from './shims/zero-process-env.js'
// static import so wrangler follows zero-cache's dependency tree and shim aliases.
import { runWorker as _runWorker } from './zero-cache-run-worker.js'

const runWorkerFn = _runWorker as (
  parent: unknown,
  env: Record<string, string>
) => Promise<void>

const WORKER_SHUTDOWN_TIMEOUT_MS = 5_000

type GenerationState = {
  cleanupDone: boolean
  cleanupFailed: boolean
  token: symbol
  workerDone: boolean
}

type OwnedMutation = {
  hadValue: boolean
  installedValue: unknown
  key: PropertyKey
  previousValue: unknown
  target: Record<PropertyKey, unknown>
}

type EmbedParent = EventEmitter & {
  send: (msg: unknown, sendHandle?: unknown) => boolean
  kill: (signal?: string) => void
  pid: number
}

// zero-cache's in-process worker graph and the CF shims still contain
// process-wide module state. keep one embed per isolate until those upstream
// globals are instance-routed; rejecting a second generation is safer than
// cross-routing one durable object's sql or process events into another.
let activeGeneration: GenerationState | null = null
const propertyOwners = new WeakMap<object, Map<PropertyKey, symbol>>()

function releaseGenerationWhenComplete(generation: GenerationState): void {
  if (
    activeGeneration === generation &&
    generation.workerDone &&
    generation.cleanupDone &&
    !generation.cleanupFailed
  ) {
    activeGeneration = null
  }
}

function setOwnedProperty(
  mutations: OwnedMutation[],
  generation: GenerationState,
  target: Record<PropertyKey, unknown>,
  key: PropertyKey,
  value: unknown
): void {
  const mutation = mutations.find(
    (candidate) => candidate.target === target && candidate.key === key
  )
  if (mutation) {
    mutation.installedValue = value
  } else {
    mutations.push({
      hadValue: Object.prototype.hasOwnProperty.call(target, key),
      installedValue: value,
      key,
      previousValue: target[key],
      target,
    })
  }

  let owners = propertyOwners.get(target)
  if (!owners) {
    owners = new Map()
    propertyOwners.set(target, owners)
  }
  owners.set(key, generation.token)
  target[key] = value
}

function updateOwnedProperty(
  mutations: OwnedMutation[],
  generation: GenerationState,
  target: Record<PropertyKey, unknown>,
  key: PropertyKey
): void {
  const mutation = mutations.find(
    (candidate) => candidate.target === target && candidate.key === key
  )
  if (!mutation) return
  const owners = propertyOwners.get(target)
  if (owners?.get(key) !== generation.token) return
  mutation.installedValue = target[key]
}

function restoreOwnedProperties(
  mutations: OwnedMutation[],
  generation: GenerationState
): void {
  for (let index = mutations.length - 1; index >= 0; index--) {
    const mutation = mutations[index]
    const owners = propertyOwners.get(mutation.target)
    if (owners?.get(mutation.key) !== generation.token) continue
    owners.delete(mutation.key)
    if (mutation.target[mutation.key] !== mutation.installedValue) continue
    if (mutation.hadValue) {
      mutation.target[mutation.key] = mutation.previousValue
    } else {
      delete mutation.target[mutation.key]
    }
  }
}

export interface ZeroCacheEmbedCFOptions {
  /** DO SQLite storage (also registered on globalThis.__orez_do_sqlite) */
  doSqlite: unknown

  /** base URL for the DO SQL execution endpoints (`/exec`, `/batch`). */
  backendUrl?: string

  /** custom fetch used by DoBackend; lets a DO route directly to another DO stub. */
  backendFetch?: typeof fetch

  /** namespace sent to the DO SQL endpoints. */
  backendNamespace?: string

  /** postgres username/password expected by the in-process proxy. */
  pgUser?: string
  pgPassword?: string

  /** zero app ID (default: 'zero') */
  appId?: string

  /** publication names */
  publications?: string[]

  /** additional env vars passed to zero-cache */
  env?: Record<string, string>

  /** fetch implementation for Worker-local mutate/query API URLs. */
  apiFetch?: typeof fetch

  /** timeout in ms waiting for zero-cache ready (default: 30000) */
  readyTimeout?: number
}

export interface ZeroCacheEmbedCF {
  /** whether zero-cache is ready */
  readonly ready: boolean

  /**
   * number of live sync WebSocket sessions. zero means no client is
   * connected, so the DO can be torn down + hibernated (see
   * plans/cf-do-idle-hibernation.md). HTTP push/pull requests are stateless
   * and not counted.
   */
  readonly connectionCount: number

  /**
   * handle an incoming request from the DO's fetch() handler.
   * routes HTTP to zero-cache's Fastify handlers, WebSocket
   * upgrades through the zero-cache handoff mechanism.
   */
  handleRequest(
    request: Request,
    ctx?: DurableObjectWebSocketHandoffContext
  ): Promise<Response>

  /** stop zero-cache */
  stop(): Promise<void>
}

// tx-journal owner id for every pg session this embed opens. recovery at
// embed boot targets exactly this owner, so it can never roll back another
// client's live transaction on the shared upstream db (e.g. the app worker's
// in-DO pg session).
const EMBED_TX_OWNER = 'orez-embed'

// roll back journaled transactions a dead embed generation left on the
// remote SQL DO (upstream db sessions killed mid-transaction).
async function recoverRemoteTransactions(
  url: string,
  backendFetch?: typeof fetch
): Promise<void> {
  const fetcher = backendFetch ?? fetch
  const resp = await fetcher(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ owner: EMBED_TX_OWNER }),
  })
  if (!resp.ok) {
    const text = await resp.text().catch(() => '')
    throw new Error(
      `zero-cache CF embed: tx recovery failed: HTTP ${resp.status} ${text.slice(0, 500)}`
    )
  }
}

type DoBackendProtocolSessionFactory = {
  createProtocolSession(): DoBackend
}

function addProtocolSessionFactory(
  backend: DoBackend,
  createProtocolSession: () => DoBackend
): DoBackend & DoBackendProtocolSessionFactory {
  return Object.assign(backend, { createProtocolSession })
}

/**
 * start zero-cache in embedded CF Workers mode.
 *
 * must be called with a DO SQLite handle for zero-cache's replica storage and
 * a DoBackend target for upstream/CVR/change Postgres connections.
 */
export async function startZeroCacheEmbedCF(
  opts: ZeroCacheEmbedCFOptions
): Promise<ZeroCacheEmbedCF> {
  if (activeGeneration) {
    throw new Error(
      'zero-cache CF embed: another generation is active or still tearing down'
    )
  }

  const generation: GenerationState = {
    cleanupDone: false,
    cleanupFailed: false,
    token: Symbol('zero-cache-cf-generation'),
    workerDone: true,
  }
  activeGeneration = generation

  const globalRecord = globalThis as Record<PropertyKey, unknown>
  let processRecord: Record<PropertyKey, unknown>
  let processEnv: Record<PropertyKey, unknown>
  let releaseProcessEnv: (() => void) | null = null
  const mutations: OwnedMutation[] = []
  const backendRoots: DoBackend[] = []
  let proxy: BrowserProxy | null = null
  let parent: EmbedParent | null = null
  let parentEmitter: EventEmitter | null = null
  let wrappedParent: unknown = null
  let isReady = false
  let stopping = false
  let runWorkerPromise: Promise<void> | null = null
  let workerSettledPromise: Promise<void> | null = null
  let workerError: unknown
  let workerFailed = false
  let startupFailure: unknown
  let shutdownPromise: Promise<void> | null = null
  let fastifyInstance: any = null
  let readyTimer: ReturnType<typeof setTimeout> | undefined
  let debugEmbed = false
  const webSocketHandoff = new DurableObjectWebSocketHandoff(() => fastifyInstance)

  const updateOwnedFastifyInstance = () => {
    const instancesMutation = mutations.find(
      (mutation) =>
        mutation.target === globalRecord && mutation.key === '__orez_fastify_instances'
    )
    const currentInstance = globalRecord.__orez_fastify_instance
    if (
      Array.isArray(instancesMutation?.installedValue) &&
      instancesMutation.installedValue.includes(currentInstance)
    ) {
      updateOwnedProperty(mutations, generation, globalRecord, '__orez_fastify_instance')
    }
  }

  const shutdown = (): Promise<void> => {
    if (shutdownPromise) return shutdownPromise
    stopping = true
    isReady = false
    if (readyTimer) clearTimeout(readyTimer)

    shutdownPromise = (async () => {
      const cleanupErrors: unknown[] = []
      let resourceCleanupFailed = false

      if (wrappedParent && runWorkerPromise && !generation.workerDone) {
        try {
          ;(wrappedParent as { kill(signal?: string): void }).kill('SIGTERM')
        } catch (err) {
          cleanupErrors.push(err)
        }
      }

      if (workerSettledPromise && !generation.workerDone) {
        let timeout: ReturnType<typeof setTimeout> | undefined
        const workerStopped = await Promise.race([
          workerSettledPromise.then(() => true),
          new Promise<false>((resolve) => {
            timeout = setTimeout(() => resolve(false), WORKER_SHUTDOWN_TIMEOUT_MS)
          }),
        ])
        if (timeout) clearTimeout(timeout)
        if (!workerStopped) {
          cleanupErrors.push(
            new Error(
              `zero-cache CF embed: worker did not terminate within ${WORKER_SHUTDOWN_TIMEOUT_MS}ms`
            )
          )
        }
      }
      if (workerFailed && workerError !== startupFailure) {
        cleanupErrors.push(workerError)
      }

      if (proxy) {
        try {
          await proxy.close()
        } catch (err) {
          resourceCleanupFailed = true
          cleanupErrors.push(err)
        }
      }

      const backendResults = await Promise.allSettled(
        backendRoots.map((backend) => Promise.resolve().then(() => backend.close()))
      )
      for (const result of backendResults) {
        if (result.status === 'rejected') {
          resourceCleanupFailed = true
          cleanupErrors.push(result.reason)
        }
      }

      updateOwnedFastifyInstance()

      restoreOwnedProperties(mutations, generation)
      releaseProcessEnv?.()
      parentEmitter?.removeAllListeners()
      if (generation.workerDone) parent?.removeAllListeners()

      generation.cleanupDone = true
      generation.cleanupFailed = resourceCleanupFailed
      releaseGenerationWhenComplete(generation)

      if (cleanupErrors.length > 0) {
        throw new AggregateError(cleanupErrors, 'zero-cache CF embed: teardown failed')
      }
    })()
    return shutdownPromise
  }

  const handleUnexpectedWorkerExit = () => {
    if (!isReady || stopping) return
    isReady = false
    if (!workerFailed) {
      workerFailed = true
      workerError = new Error(
        'zero-cache CF embed: runWorker exited after becoming ready'
      )
    }
    void shutdown().catch((error) => {
      console.error('[orez-zero-cache-cf] unexpected worker exit cleanup failed', error)
    })
  }

  try {
    releaseProcessEnv = acquireZeroProcessEnv()
    processRecord = globalRecord.process as Record<PropertyKey, unknown>
    processEnv = processRecord.env as Record<PropertyKey, unknown>

    // wire orez's own logger from env, mirroring the node path's setLogLevel.
    setLogLevel(
      (opts.env?.OREZ_LOG_LEVEL as 'debug' | 'info' | 'warn' | 'error') || 'warn'
    )

    resetReplicationState()
    const leakedHandles = sweepLeakedSqliteHandles()
    if (leakedHandles > 0) {
      console.warn(
        `[orez-zero-cache-cf] closed ${leakedHandles} sqlite handles leaked by the previous embed generation`
      )
    }

    setOwnedProperty(
      mutations,
      generation,
      globalRecord,
      '__orez_fastify_instance',
      undefined
    )
    delete globalRecord.__orez_fastify_instance
    setOwnedProperty(mutations, generation, globalRecord, '__orez_fastify_instances', [])
    resetFastifyRegistry()
    updateOwnedProperty(mutations, generation, globalRecord, '__orez_fastify_instances')

    const appId = opts.appId || 'zero'
    const publications = opts.publications?.join(',') || `orez_${appId}_public`
    const readyTimeout = opts.readyTimeout ?? 30000
    const pgUser = opts.pgUser || 'user'
    const pgPassword = opts.pgPassword || ''
    const backendUrl = opts.backendUrl || 'https://orez-do-backend.local'
    const backendNamespace = opts.backendNamespace || appId
    const localSql = createLocalSqlBackend(opts.doSqlite)

    const instantiateBackend = (dbName: string) =>
      new DoBackend(backendUrl, dbName, backendNamespace, {
        fetch: dbName === 'postgres' ? opts.backendFetch : localSql.fetch,
        txOwner: EMBED_TX_OWNER,
      })

    const createRootBackend = (dbName: string) => {
      const backend = instantiateBackend(dbName)
      backendRoots.push(backend)
      return backend
    }

    localSql.recoverOrphanedTransactions()
    await recoverRemoteTransactions(
      `${backendUrl.replace(/\/+$/, '')}/recover-txs?db=postgres&ns=${encodeURIComponent(backendNamespace)}`,
      opts.backendFetch
    )

    const backends = {
      postgres: createRootBackend('postgres'),
      cvr: createRootBackend('zero_cvr'),
      cdb: createRootBackend('zero_cdb'),
    }
    const proxyBackends = {
      postgres: addProtocolSessionFactory(backends.postgres, () =>
        instantiateBackend('postgres')
      ),
      cvr: addProtocolSessionFactory(backends.cvr, () => instantiateBackend('zero_cvr')),
      cdb: addProtocolSessionFactory(backends.cdb, () => instantiateBackend('zero_cdb')),
    }

    const backendReadyResults = await Promise.allSettled([
      backends.postgres.waitReady,
      backends.cvr.waitReady,
      backends.cdb.waitReady,
    ])
    const backendReadyErrors = backendReadyResults.flatMap((result) =>
      result.status === 'rejected' ? [result.reason] : []
    )
    if (backendReadyErrors.length === 1) throw backendReadyErrors[0]
    if (backendReadyErrors.length > 1) {
      throw new AggregateError(
        backendReadyErrors,
        'zero-cache CF embed: backend setup failed'
      )
    }

    proxy = await createBrowserProxy(
      {
        postgres: proxyBackends.postgres as any,
        cvr: proxyBackends.cvr as any,
        cdb: proxyBackends.cdb as any,
        postgresReplicas: [],
      } as any,
      {
        pgUser,
        pgPassword,
        singleDb: false,
        logLevel: opts.env?.ZERO_LOG_LEVEL || 'info',
      }
    )

    setOwnedProperty(
      mutations,
      generation,
      globalRecord,
      '__orez_do_sqlite',
      opts.doSqlite
    )
    setOwnedProperty(
      mutations,
      generation,
      globalRecord,
      '__orez_proxy_connect',
      (port: MessagePort) => proxy?.handleConnection(port)
    )
    setOwnedProperty(mutations, generation, globalRecord, '__orez_proxy_user', pgUser)
    setOwnedProperty(
      mutations,
      generation,
      globalRecord,
      '__orez_proxy_password',
      pgPassword
    )

    const createdParent = new EventEmitter() as EmbedParent
    parent = createdParent
    parentEmitter = new EventEmitter()
    createdParent.send = (message: unknown, sendHandle?: unknown) => {
      parentEmitter?.emit('message', message, sendHandle)
      return true
    }
    createdParent.kill = (signal = 'SIGTERM') => {
      createdParent.emit(signal, signal)
    }
    createdParent.pid = (processRecord.pid as number | undefined) ?? 1

    const originalFetch = globalRecord.fetch as typeof fetch
    setOwnedProperty(mutations, generation, processRecord, 'exit', (code?: number) =>
      parent?.emit('exit', code ?? 0)
    )
    if (opts.apiFetch) {
      setOwnedProperty(
        mutations,
        generation,
        globalRecord,
        'fetch',
        (input: RequestInfo | URL, init?: RequestInit) => {
          const request = new Request(input, init)
          const url = new URL(request.url)
          if (url.hostname === 'orez-zero-api.local') return opts.apiFetch!(request)
          return originalFetch(input as any, init as any)
        }
      )
    }

    const env: Record<string, string> = {
      ...(processEnv as Record<string, string>),
      SINGLE_PROCESS: '1',
      NODE_ENV: 'development',
      ZERO_UPSTREAM_DB: `postgres://${pgUser}:ignored@127.0.0.1/postgres`,
      ZERO_CVR_DB: `postgres://${pgUser}:ignored@127.0.0.1/zero_cvr`,
      ZERO_CHANGE_DB: `postgres://${pgUser}:ignored@127.0.0.1/zero_cdb`,
      ZERO_REPLICA_FILE: ':do-sqlite:',
      ZERO_PORT: '0',
      ZERO_APP_ID: appId,
      ZERO_APP_PUBLICATIONS: publications,
      ZERO_ADMIN_PASSWORD: opts.env?.ZERO_ADMIN_PASSWORD || crypto.randomUUID(),
      ZERO_NUM_SYNC_WORKERS: opts.env?.ZERO_NUM_SYNC_WORKERS || '1',
      ZERO_ENABLE_QUERY_PLANNER: 'false',
      ZERO_UPSTREAM_MAX_CONNS: opts.env?.ZERO_UPSTREAM_MAX_CONNS || '2',
      ZERO_CVR_MAX_CONNS: opts.env?.ZERO_CVR_MAX_CONNS || '2',
      ZERO_CHANGE_MAX_CONNS: opts.env?.ZERO_CHANGE_MAX_CONNS || '2',
      ZERO_LOG_LEVEL: opts.env?.ZERO_LOG_LEVEL || 'warn',
      ...opts.env,
      ZERO_SHADOW_SYNC_ENABLED: 'false',
    }
    for (const [key, value] of Object.entries(env)) {
      setOwnedProperty(mutations, generation, processEnv, key, value)
    }

    debugEmbed =
      env.OREZ_DEBUG_EMBED === '1' || globalRecord.__OREZ_DEBUG_EMBED__ === true

    wrappedParent = new Proxy(createdParent, {
      get(target, prop, receiver) {
        if (prop === 'onMessageType') {
          return (
            type: string,
            handler: (msg: unknown, sendHandle?: unknown) => void
          ) => {
            target.on('message', (data: unknown, sendHandle?: unknown) => {
              if (Array.isArray(data) && data.length === 2 && data[0] === type) {
                handler(data[1], sendHandle)
              }
            })
            return receiver
          }
        }
        if (prop === 'onceMessageType') {
          return (
            type: string,
            handler: (msg: unknown, sendHandle?: unknown) => void
          ) => {
            const listener = (data: unknown, sendHandle?: unknown) => {
              if (Array.isArray(data) && data.length === 2 && data[0] === type) {
                target.off('message', listener)
                handler(data[1], sendHandle)
              }
            }
            target.on('message', listener)
            return receiver
          }
        }
        return Reflect.get(target, prop, receiver)
      },
    })

    const readyPromise = new Promise<void>((resolve, reject) => {
      readyTimer = setTimeout(() => {
        reject(
          new Error(
            `zero-cache CF embed: timed out waiting for ready after ${readyTimeout}ms`
          )
        )
      }, readyTimeout)
      parentEmitter?.on('message', (msg: unknown) => {
        if (debugEmbed) console.debug('[orez-zero-cache-cf] parent message', msg)
        if (!stopping && Array.isArray(msg) && msg[0] === 'ready') {
          if (readyTimer) clearTimeout(readyTimer)
          isReady = true
          resolve()
        }
      })
    })

    generation.workerDone = false
    runWorkerPromise = Promise.resolve().then(() => runWorkerFn(wrappedParent, env))
    void runWorkerPromise.catch((err) => {
      if (debugEmbed) console.error('[orez-zero-cache-cf] runWorker error', err)
    })
    workerSettledPromise = runWorkerPromise.then(
      () => {
        generation.workerDone = true
        handleUnexpectedWorkerExit()
        if (generation.cleanupDone) parent?.removeAllListeners()
        releaseGenerationWhenComplete(generation)
      },
      (error) => {
        workerError = error
        workerFailed = true
        generation.workerDone = true
        handleUnexpectedWorkerExit()
        if (generation.cleanupDone) parent?.removeAllListeners()
        releaseGenerationWhenComplete(generation)
      }
    )
    const workerStartupPromise = runWorkerPromise.then(() => {
      if (!isReady) {
        throw new Error('zero-cache CF embed: runWorker exited before ready')
      }
    })
    await Promise.race([readyPromise, workerStartupPromise])

    fastifyInstance = globalRecord.__orez_fastify_instance
    updateOwnedFastifyInstance()
  } catch (startupError) {
    startupFailure = startupError
    try {
      await shutdown()
    } catch (cleanupError) {
      throw new AggregateError(
        [startupError, cleanupError],
        'zero-cache CF embed: startup failed and teardown also failed',
        { cause: startupError }
      )
    }
    throw startupError
  }

  return {
    get ready() {
      return isReady
    },

    get connectionCount() {
      return webSocketHandoff.activeConnections
    },

    async handleRequest(
      request: Request,
      ctx?: DurableObjectWebSocketHandoffContext
    ): Promise<Response> {
      if (!isReady) {
        return new Response('zero-cache not ready', { status: 503 })
      }

      const url = new URL(request.url)
      const isUpgrade =
        request.headers.get('upgrade')?.toLowerCase() === 'websocket' ||
        request.headers.get('x-soot-ws-upgrade') === 'true'

      if (isUpgrade) {
        return handleWebSocketUpgrade(request, url, webSocketHandoff, ctx)
      }

      return handleHttpRequest(request, url, fastifyInstance)
    },

    stop: shutdown,
  }
}

// -- HTTP request handling --
// routes through the Fastify shim's inject() method

async function handleHttpRequest(
  request: Request,
  url: URL,
  fastify: any
): Promise<Response> {
  if (!fastify?.inject) {
    return new Response('fastify not available', { status: 503 })
  }

  const headers: Record<string, string> = {}
  request.headers.forEach((value, key) => {
    headers[key] = value
  })

  let payload: string | null = null
  if (request.method !== 'GET' && request.method !== 'HEAD' && request.body) {
    payload = await request.text()
  }

  const result = await fastify.inject({
    method: request.method,
    url: url.pathname + url.search,
    headers,
    payload,
  })

  return new Response(result.body, {
    status: result.statusCode,
    headers: result.headers,
  })
}

// -- WebSocket upgrade handling --
// creates WebSocketPair, accepts the server socket with the standard CF
// WebSocket API, and feeds zero-cache a process-lived proxy socket.

function handleWebSocketUpgrade(
  request: Request,
  url: URL,
  webSocketHandoff: DurableObjectWebSocketHandoff,
  ctx: DurableObjectWebSocketHandoffContext | undefined
): Response {
  const WsPair = (globalThis as any).WebSocketPair
  if (!WsPair) {
    return new Response('WebSocketPair not available', { status: 500 })
  }

  const pair = new WsPair()
  const [client, server] = Object.values(pair) as [WebSocket, DurableObjectWebSocket]

  // build a serializable request object for the handoff
  const headers: Record<string, string> = {}
  request.headers.forEach((value, key) => {
    headers[key] = value
  })

  const message: HandoffRequestMessage = {
    url: url.pathname + url.search,
    headers,
    method: 'GET',
  }

  if (!webSocketHandoff.accept(server, message, ctx)) {
    server.close(1011, 'zero-cache websocket route unavailable')
    return new Response('zero-cache websocket route unavailable', { status: 404 })
  }

  // return 101 with client socket
  // must echo Sec-WebSocket-Protocol — browsers reject the upgrade without it
  const secProtocol = request.headers.get('sec-websocket-protocol')
  const upgradeHeaders: Record<string, string> = {}
  if (secProtocol) {
    upgradeHeaders['Sec-WebSocket-Protocol'] = secProtocol
  }
  try {
    return new Response(null, {
      status: 101,
      headers: upgradeHeaders,
      // @ts-expect-error CF Workers Response extension
      webSocket: client,
    })
  } catch {
    const resp = new Response(null, { status: 200 })
    ;(resp as any).__orez_websocket = client
    ;(resp as any).__orez_ws_upgrade = true
    return resp
  }
}
