/**
 * zero-cache embedded runner for browser Web Workers.
 *
 * same pattern as the CF embed but for browser environments:
 *
 *   postgres           → orez/worker/shims/postgres  (PGlite-backed)
 *   @rocicorp/zero-sqlite3 → orez/worker/shims/sqlite (sql.js or in-memory)
 *   fastify            → orez/worker/shims/fastify   (route capture)
 *   ws                 → orez/worker/shims/ws        (MessagePort/WebSocket)
 *
 * the consumer's bundler (esbuild/vite) must configure these aliases
 * plus Node.js polyfills. use getBrowserBuildConfig() for the alias map.
 *
 * usage:
 *
 *   import { startZeroCacheEmbedBrowser } from 'orez/worker/browser-embed'
 *
 *   const zc = await startZeroCacheEmbedBrowser({
 *     pglite: pg,
 *     sqlite: sqlJsDb, // optional — sql.js Database instance
 *   })
 *
 *   // connect a Zero client via WebSocket-like object
 *   zc.handleWebSocket(wsOrPort)
 *
 *   // or handle HTTP requests (push/pull)
 *   const result = await zc.handleHttp({ method: 'GET', url: '/' })
 */

import EventEmitter from 'node:events'

// static import so the bundler can follow the dependency tree.
// @ts-expect-error — internal zero-cache module, no type declarations
import { runWorker as _runWorker } from '@rocicorp/zero/out/zero-cache/src/server/runner/run-worker.js'

import type { PGlite } from '@electric-sql/pglite'

const runWorkerFn = _runWorker as (
  parent: unknown,
  env: Record<string, string>
) => Promise<void>

export interface ZeroCacheEmbedBrowserOptions {
  /** PGlite instance */
  pglite: PGlite

  /**
   * sql.js Database instance for SQLite replica storage.
   * if not provided, looks for globalThis.__orez_sqljs_db,
   * then falls back to an in-memory stub (limited functionality).
   */
  sqlite?: unknown

  /** zero app ID (default: 'zero') */
  appId?: string

  /** publication names */
  publications?: string[]

  /** additional env vars passed to zero-cache */
  env?: Record<string, string>

  /** timeout in ms waiting for zero-cache ready (default: 30000) */
  readyTimeout?: number
}

export interface HttpRequest {
  method: string
  url: string
  headers?: Record<string, string>
  body?: string | null
}

export interface HttpResponse {
  status: number
  headers: Record<string, string>
  body: string
}

/** WebSocket-like object — matches CF WebSocket, browser WebSocket, or MessagePort adapter */
interface WsLike {
  readyState: number
  send(data: string | ArrayBuffer | ArrayBufferView): void
  close(code?: number, reason?: string): void
  addEventListener(type: string, handler: (event: any) => void): void
  removeEventListener(type: string, handler: (event: any) => void): void
}

export interface ZeroCacheEmbedBrowser {
  /** whether zero-cache is ready */
  readonly ready: boolean

  /**
   * handle a WebSocket connection from a Zero client.
   * accepts any WebSocket-like object (browser WebSocket, MessagePort adapter, etc.)
   * feeds it into zero-cache's handoff mechanism.
   */
  handleWebSocket(ws: WsLike, url?: string): void

  /**
   * handle an HTTP request (push/pull/health).
   * for environments without the Fetch API Request/Response.
   */
  handleHttp(request: HttpRequest): Promise<HttpResponse>

  /** stop zero-cache */
  stop(): Promise<void>
}

export async function startZeroCacheEmbedBrowser(
  opts: ZeroCacheEmbedBrowserOptions
): Promise<ZeroCacheEmbedBrowser> {
  const appId = opts.appId || 'zero'
  const publications = opts.publications?.join(',') || `orez_${appId}_public`
  const readyTimeout = opts.readyTimeout ?? 30000

  // set up sqlite storage from sql.js or in-memory
  if (opts.sqlite) {
    // consumer provided a sql.js Database — create adapter
    const { createSqlJsStorage } = await import('./shims/sqlite-browser.js')
    ;(globalThis as any).__orez_do_sqlite = createSqlJsStorage(opts.sqlite as any)
  } else if (!(globalThis as any).__orez_do_sqlite) {
    // no sqlite provided — use in-memory stub
    const { createInMemoryStorage } = await import('./shims/sqlite-browser.js')
    ;(globalThis as any).__orez_do_sqlite = createInMemoryStorage()
  }

  // set up PGlite for postgres shim
  ;(globalThis as any).__orez_pglite = opts.pglite

  // ensure process globals exist (browser has no process)
  ;(globalThis as any).process ??= {}
  ;(globalThis as any).process.env ??= {}
  ;(globalThis as any).process.pid ??= 1
  ;(globalThis as any).process.argv ??= []
  ;(globalThis as any).process.kill ??= () => {}

  // CRITICAL: set SINGLE_PROCESS before zero-cache runs
  ;(globalThis as any).process.env.SINGLE_PROCESS = '1'
  ;(globalThis as any).process.env.NODE_ENV = 'development'

  // create fake parent EventEmitter for zero-cache's runWorker()
  const parent = new EventEmitter() as EventEmitter & {
    send: (msg: unknown) => boolean
    kill: (signal?: string) => void
    pid: number
  }

  const parentEmitter = new EventEmitter()

  parent.send = (message: unknown) => {
    parentEmitter.emit('message', message)
    return true
  }
  parent.kill = (signal = 'SIGTERM') => {
    parent.emit(signal, signal)
  }
  parent.pid = 1

  // shim process.exit
  const origExit = (globalThis as any).process.exit
  ;(globalThis as any).process.exit = (code?: number) => {
    parent.emit('exit', code ?? 0)
  }

  // build env for zero-cache
  const env: Record<string, string> = {
    ...((globalThis as any).process.env as Record<string, string>),
    SINGLE_PROCESS: '1',
    NODE_ENV: 'development',
    ZERO_UPSTREAM_DB: 'pglite://in-process',
    ZERO_CVR_DB: 'pglite://in-process',
    ZERO_CHANGE_DB: 'pglite://in-process',
    ZERO_REPLICA_FILE: ':browser-sqlite:',
    ZERO_PORT: '0',
    ZERO_APP_ID: appId,
    ZERO_APP_PUBLICATIONS: publications,
    ZERO_LOG_LEVEL: opts.env?.ZERO_LOG_LEVEL || 'info',
    ZERO_NUM_SYNC_WORKERS: opts.env?.ZERO_NUM_SYNC_WORKERS || '1',
    ZERO_ENABLE_QUERY_PLANNER: 'false',
    ...opts.env,
  }

  // wrap parent with onMessageType/onceMessageType helpers
  const wrappedParent = new Proxy(parent, {
    get(target, prop, receiver) {
      if (prop === 'onMessageType') {
        return (type: string, handler: (msg: unknown) => void) => {
          target.on('message', (data: unknown) => {
            if (Array.isArray(data) && data.length === 2 && data[0] === type) {
              handler(data[1])
            }
          })
          return receiver
        }
      }
      if (prop === 'onceMessageType') {
        return (type: string, handler: (msg: unknown) => void) => {
          const listener = (data: unknown) => {
            if (Array.isArray(data) && data.length === 2 && data[0] === type) {
              target.off('message', listener)
              handler(data[1])
            }
          }
          target.on('message', listener)
          return receiver
        }
      }
      return Reflect.get(target, prop, receiver)
    },
  })

  // state
  let isReady = false
  let runWorkerPromise: Promise<void> | null = null
  let fastifyInstance: any = null

  // wait for "ready" message
  const readyPromise = new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(
        new Error(
          `zero-cache browser embed: timed out waiting for ready after ${readyTimeout}ms`
        )
      )
    }, readyTimeout)

    parentEmitter.on('message', (msg: unknown) => {
      if (Array.isArray(msg) && msg[0] === 'ready') {
        clearTimeout(timeout)
        isReady = true
        resolve()
      }
    })
  })

  // start zero-cache
  runWorkerPromise = runWorkerFn(wrappedParent, env).catch((err) => {
    if (!isReady) {
      throw err
    }
  })

  await readyPromise

  fastifyInstance = (globalThis as any).__orez_fastify_instance

  return {
    get ready() {
      return isReady
    },

    handleWebSocket(ws: WsLike, url = '/') {
      if (!isReady || !fastifyInstance?.server) return

      const message = {
        url,
        headers: {},
        method: 'GET',
      }

      // feed the WebSocket into zero-cache's handoff mechanism.
      // the fastify shim's server is an EventEmitter with onMessageType.
      // installWebSocketHandoff (non-Server branch) listens for "handoff".
      fastifyInstance.server.emit(
        'message',
        ['handoff', { message, head: new Uint8Array(0) }],
        ws // the WebSocket-like object as sendHandle
      )
    },

    async handleHttp(request: HttpRequest): Promise<HttpResponse> {
      if (!isReady || !fastifyInstance?.inject) {
        return { status: 503, headers: {}, body: 'not ready' }
      }

      const result = await fastifyInstance.inject({
        method: request.method,
        url: request.url,
        headers: request.headers || {},
        payload: request.body,
      })

      return {
        status: result.statusCode,
        headers: result.headers,
        body: result.body,
      }
    },

    async stop() {
      isReady = false
      wrappedParent.kill('SIGTERM')
      if (runWorkerPromise) {
        await Promise.race([
          runWorkerPromise,
          new Promise((r) => setTimeout(r, 5000)),
        ])
      }
      if (origExit) {
        ;(globalThis as any).process.exit = origExit
      }
      delete (globalThis as any).process.env.SINGLE_PROCESS
    },
  }
}
