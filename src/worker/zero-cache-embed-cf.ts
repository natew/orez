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
 */

import './shims/zero-process-env.js'

import { EventEmitter } from 'node:events'

// static import so wrangler can follow the dependency tree and bundle
// zero-cache with all its transitive deps + our shim aliases.
// @ts-expect-error — internal zero-cache module, no type declarations
import { runWorker as _runWorker } from '@rocicorp/zero/out/zero-cache/src/server/runner/run-worker.js'

import { createBrowserProxy, type BrowserProxy } from '../pg-proxy-browser.js'
import { DoBackend } from '../pg-proxy-do-backend.js'

const runWorkerFn = _runWorker as (
  parent: unknown,
  env: Record<string, string>
) => Promise<void>

export interface ZeroCacheEmbedCFOptions {
  /** DO SQLite storage (also registered on globalThis.__orez_do_sqlite) */
  doSqlite: unknown

  /**
   * base URL for the DO SQL execution endpoints (`/exec`, `/batch`).
   * ignored when `backends` is supplied.
   */
  backendUrl?: string

  /** custom fetch used by DoBackend; lets a DO route directly to another DO stub. */
  backendFetch?: typeof fetch

  /** namespace sent to the DO SQL endpoints. */
  backendNamespace?: string

  /** pre-created DoBackend instances. mainly useful for tests. */
  backends?: {
    postgres: DoBackend
    cvr: DoBackend
    cdb: DoBackend
  }

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
   * handle an incoming request from the DO's fetch() handler.
   * routes HTTP to zero-cache's Fastify handlers, WebSocket
   * upgrades through the zero-cache handoff mechanism.
   */
  handleRequest(request: Request): Promise<Response>

  /** stop zero-cache */
  stop(): Promise<void>
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
  const appId = opts.appId || 'zero'
  const publications = opts.publications?.join(',') || `orez_${appId}_public`
  const readyTimeout = opts.readyTimeout ?? 30000
  const pgUser = opts.pgUser || 'user'
  const pgPassword = opts.pgPassword || ''
  const backendUrl = opts.backendUrl || 'https://orez-do-backend.local'
  const backendNamespace = opts.backendNamespace || appId

  const createBackend = (dbName: string) =>
    new DoBackend(backendUrl, dbName, backendNamespace, {
      fetch: opts.backendFetch,
    })

  const backends =
    opts.backends ??
    ({
      postgres: createBackend('postgres'),
      cvr: createBackend('zero_cvr'),
      cdb: createBackend('zero_cdb'),
    } satisfies NonNullable<ZeroCacheEmbedCFOptions['backends']>)

  const proxyBackends = opts.backends
    ? backends
    : {
        postgres: addProtocolSessionFactory(backends.postgres, () =>
          createBackend('postgres')
        ),
        cvr: addProtocolSessionFactory(backends.cvr, () => createBackend('zero_cvr')),
        cdb: addProtocolSessionFactory(backends.cdb, () => createBackend('zero_cdb')),
      }

  await Promise.all([
    backends.postgres.waitReady,
    backends.cvr.waitReady,
    backends.cdb.waitReady,
  ])

  const proxy: BrowserProxy = await createBrowserProxy(
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

  // ensure globals are set for shims
  ;(globalThis as any).__orez_do_sqlite = opts.doSqlite
  ;(globalThis as any).__orez_proxy_connect = (port: MessagePort) => {
    proxy.handleConnection(port)
  }
  ;(globalThis as any).__orez_proxy_user = pgUser
  ;(globalThis as any).__orez_proxy_password = pgPassword

  // ensure process.env exists (CF Workers doesn't have it natively)
  ;(globalThis as any).process ??= {}
  ;(globalThis as any).process.env ??= {}
  ;(globalThis as any).process.pid ??= 1
  ;(globalThis as any).process.argv ??= []

  // CRITICAL: set SINGLE_PROCESS before importing zero-cache.
  // zero-cache's childWorker() checks process.env.SINGLE_PROCESS directly.
  ;(globalThis as any).process.env.SINGLE_PROCESS = '1'
  ;(globalThis as any).process.env.NODE_ENV = 'development'

  // shim process.kill (used by HeartbeatMonitor) to be a no-op
  ;(globalThis as any).process.kill ??= () => {}

  // create fake parent EventEmitter for zero-cache's runWorker()
  // must be declared before process.exit shim (which references it)
  const parent = new EventEmitter() as EventEmitter & {
    send: (msg: unknown) => boolean
    kill: (signal?: string) => void
    pid: number
  }

  const parentEmitter = new EventEmitter()

  parent.send = (message: unknown, sendHandle?: unknown) => {
    parentEmitter.emit('message', message, sendHandle)
    return true
  }
  parent.kill = (signal = 'SIGTERM') => {
    parent.emit(signal, signal)
  }
  parent.pid = (globalThis as any).process.pid ?? 1

  // shim process.exit to emit on parent instead of actually exiting
  const origExit = (globalThis as any).process.exit
  const origNodeEnv = (globalThis as any).process.env.NODE_ENV
  const origKill = (globalThis as any).process.kill
  const origFetch = (globalThis as any).fetch
  ;(globalThis as any).process.exit = (code?: number) => {
    parent.emit('exit', code ?? 0)
  }
  if (opts.apiFetch) {
    ;(globalThis as any).fetch = (input: RequestInfo | URL, init?: RequestInit) => {
      const request = new Request(input, init)
      const url = new URL(request.url)
      if (url.hostname === 'orez-zero-api.local') return opts.apiFetch!(request)
      return origFetch(input as any, init as any)
    }
  }

  // build env for zero-cache
  const env: Record<string, string> = {
    ...((globalThis as any).process.env as Record<string, string>),
    SINGLE_PROCESS: '1',
    NODE_ENV: 'development',
    // postgres-browser intercepts these URLs and routes PG wire over
    // MessagePort to the DoBackend-backed proxy above.
    ZERO_UPSTREAM_DB: `postgres://${pgUser}:ignored@127.0.0.1/postgres`,
    ZERO_CVR_DB: `postgres://${pgUser}:ignored@127.0.0.1/zero_cvr`,
    ZERO_CHANGE_DB: `postgres://${pgUser}:ignored@127.0.0.1/zero_cdb`,
    // this path is intercepted by the sqlite shim
    ZERO_REPLICA_FILE: ':do-sqlite:',
    // don't bind a port — we route via inject/handoff
    ZERO_PORT: '0',
    ZERO_APP_ID: appId,
    ZERO_APP_PUBLICATIONS: publications,
    ZERO_ADMIN_PASSWORD: opts.env?.ZERO_ADMIN_PASSWORD || crypto.randomUUID(),
    ZERO_LOG_LEVEL: opts.env?.ZERO_LOG_LEVEL || 'info',
    ZERO_NUM_SYNC_WORKERS: opts.env?.ZERO_NUM_SYNC_WORKERS || '1',
    ZERO_ENABLE_QUERY_PLANNER: 'false',
    ...opts.env,
    // shadow sync is an optional upstream canary that imports the initial-sync
    // copy path. keep it disabled in the CF embed to avoid bundling unused
    // worker code and storage paths into Durable Objects.
    ZERO_SHADOW_SYNC_ENABLED: 'false',
  }
  Object.assign((globalThis as any).process.env, env)

  const debugEmbed =
    env.OREZ_DEBUG_EMBED === '1' || (globalThis as any).__OREZ_DEBUG_EMBED__ === true

  // wrap parent with onMessageType/onceMessageType helpers
  // must forward sendHandle (second arg) for WebSocket handoff
  const wrappedParent = new Proxy(parent, {
    get(target, prop, receiver) {
      if (prop === 'onMessageType') {
        return (type: string, handler: (msg: unknown, sendHandle?: unknown) => void) => {
          target.on('message', (data: unknown, sendHandle?: unknown) => {
            if (Array.isArray(data) && data.length === 2 && data[0] === type) {
              handler(data[1], sendHandle)
            }
          })
          return receiver
        }
      }
      if (prop === 'onceMessageType') {
        return (type: string, handler: (msg: unknown, sendHandle?: unknown) => void) => {
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

  // track state
  let isReady = false
  let runWorkerPromise: Promise<void> | null = null

  // capture the Fastify shim instance from zero-cache's HttpService.
  // the fastify shim stores itself on globalThis when created.
  let fastifyInstance: any = null
  let readyTimer: ReturnType<typeof setTimeout> | undefined

  // wait for "ready" message
  const readyPromise = new Promise<void>((resolve, reject) => {
    readyTimer = setTimeout(() => {
      reject(
        new Error(
          `zero-cache CF embed: timed out waiting for ready after ${readyTimeout}ms`
        )
      )
    }, readyTimeout)

    parentEmitter.on('message', (msg: unknown) => {
      if (debugEmbed) console.debug('[orez-zero-cache-cf] parent message', msg)
      if (Array.isArray(msg) && msg[0] === 'ready') {
        if (readyTimer) clearTimeout(readyTimer)
        isReady = true
        resolve()
      }
    })
  })

  // start zero-cache
  runWorkerPromise = runWorkerFn(wrappedParent, env).catch((err) => {
    if (debugEmbed) console.error('[orez-zero-cache-cf] runWorker error', err)
    if (!isReady) {
      throw err
    }
    // after ready, errors during shutdown are expected
  })
  const workerStartupPromise = runWorkerPromise.then(() => {
    if (!isReady) {
      throw new Error('zero-cache CF embed: runWorker exited before ready')
    }
  })

  // wait for ready
  try {
    await Promise.race([readyPromise, workerStartupPromise])
  } catch (err) {
    if (readyTimer) clearTimeout(readyTimer)
    throw err
  }

  // get the fastify instance (set by our shim during init)
  fastifyInstance = (globalThis as any).__orez_fastify_instance

  return {
    get ready() {
      return isReady
    },

    async handleRequest(request: Request): Promise<Response> {
      if (!isReady) {
        return new Response('zero-cache not ready', { status: 503 })
      }

      const url = new URL(request.url)
      const isUpgrade =
        request.headers.get('upgrade')?.toLowerCase() === 'websocket' ||
        request.headers.get('x-soot-ws-upgrade') === 'true'

      if (isUpgrade) {
        return handleWebSocketUpgrade(request, url, fastifyInstance)
      }

      return handleHttpRequest(request, url, fastifyInstance)
    },

    async stop() {
      isReady = false
      wrappedParent.kill('SIGTERM')
      if (runWorkerPromise) {
        await Promise.race([runWorkerPromise, new Promise((r) => setTimeout(r, 5000))])
      }
      await new Promise((r) => setTimeout(r, 200))
      proxy.close()
      await Promise.all([
        backends.postgres.close(),
        backends.cvr.close(),
        backends.cdb.close(),
      ])
      // restore all modified globals
      if (origExit) {
        ;(globalThis as any).process.exit = origExit
      }
      if (origNodeEnv !== undefined) {
        ;(globalThis as any).process.env.NODE_ENV = origNodeEnv
      }
      if (origKill) {
        ;(globalThis as any).process.kill = origKill
      }
      if (opts.apiFetch) {
        ;(globalThis as any).fetch = origFetch
      }
      delete (globalThis as any).process.env.SINGLE_PROCESS
      delete (globalThis as any).__orez_proxy_connect
      delete (globalThis as any).__orez_proxy_user
      delete (globalThis as any).__orez_proxy_password
    },
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
// creates WebSocketPair and feeds the server socket into zero-cache's
// handoff mechanism via the Fastify shim's server EventEmitter.

function handleWebSocketUpgrade(request: Request, url: URL, fastify: any): Response {
  const WsPair = (globalThis as any).WebSocketPair
  if (!WsPair) {
    return new Response('WebSocketPair not available', { status: 500 })
  }

  const pair = new WsPair()
  const [client, server] = Object.values(pair) as [any, any]

  // accept the server side (CF Workers requirement)
  server.accept()

  // build a serializable request object for the handoff
  const headers: Record<string, string> = {}
  request.headers.forEach((value, key) => {
    headers[key] = value
  })

  const message = {
    url: url.pathname + url.search,
    headers,
    method: 'GET',
  }

  // emit handoff on the Fastify server's EventEmitter.
  // installWebSocketHandoff (non-Server branch) listens for this:
  //   source.onMessageType("handoff", (msg, socket) => { ... })
  if (fastify?.server) {
    fastify.server.emit(
      'message',
      ['handoff', { message, head: new Uint8Array(0) }],
      server // the CF WebSocket as sendHandle
    )
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
