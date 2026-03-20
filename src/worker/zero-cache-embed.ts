/**
 * zero-cache embedded runner.
 *
 * runs zero-cache in-process with SINGLE_PROCESS=1 instead of spawning
 * a child process. uses the same TCP proxy approach as startZeroLite()
 * for database connectivity — zero-cache connects to PGlite via the proxy.
 *
 * two modes:
 *
 * 1. **development** (this file): zero-cache uses real postgres package
 *    to connect to PGlite via TCP proxy. no bundler aliases needed.
 *
 * 2. **CF Workers** (future): bundler aliases swap postgres/sqlite3 for
 *    our shims. no TCP proxy, no port binding, all in-process.
 *
 * env vars:
 *   SINGLE_PROCESS=1       — all workers in-process via EventEmitter
 *   ZERO_UPSTREAM_DB       — postgres connection string (to TCP proxy)
 *   ZERO_CVR_DB            — postgres connection string (to TCP proxy)
 *   ZERO_CHANGE_DB         — postgres connection string (to TCP proxy)
 *   ZERO_REPLICA_FILE      — sqlite replica file path
 *   ZERO_PORT              — HTTP port for zero-cache dispatcher
 */

import EventEmitter from 'node:events'
import { resolve } from 'node:path'

import type { PGlite } from '@electric-sql/pglite'

export interface ZeroCacheEmbedOptions {
  /** PGlite instance (not used directly — zero-cache connects via TCP proxy) */
  pglite: PGlite

  /** connection string for the upstream database (postgres://...) */
  upstreamDb: string

  /** connection string for the CVR database */
  cvrDb: string

  /** connection string for the change database */
  changeDb: string

  /** path to the SQLite replica file */
  replicaFile: string

  /** port for zero-cache HTTP server (0 = random) */
  port?: number

  /** zero app ID (default: 'zero') */
  appId?: string

  /** publication names */
  publications?: string[]

  /** additional env vars passed to zero-cache */
  env?: Record<string, string>

  /** timeout in ms waiting for zero-cache to be ready (default: 60000) */
  readyTimeout?: number
}

export interface ZeroCacheEmbed {
  /** the port zero-cache is listening on */
  readonly port: number

  /** whether zero-cache is ready to handle requests */
  readonly ready: boolean

  /** stop zero-cache and all in-process workers */
  stop(): Promise<void>
}

/**
 * start zero-cache in embedded (in-process) mode.
 *
 * instead of spawning a child process, imports and runs zero-cache's
 * runWorker() directly with SINGLE_PROCESS=1. all worker coordination
 * happens via EventEmitter IPC channels instead of process.fork().
 *
 * zero-cache still connects to PGlite via the TCP proxy (same as
 * the child process mode), so no bundler aliases are needed.
 */
export async function startZeroCacheEmbed(
  opts: ZeroCacheEmbedOptions
): Promise<ZeroCacheEmbed> {
  const appId = opts.appId || 'zero'
  const publications = opts.publications?.join(',') || `orez_${appId}_public`
  const readyTimeout = opts.readyTimeout ?? 60000

  // CRITICAL: set SINGLE_PROCESS on process.env BEFORE importing zero-cache.
  // zero-cache's childWorker() and ProcessManager check process.env directly,
  // not the env object passed to runWorker(). without this, zero-cache will
  // fork() child processes instead of using inProcChannel().
  process.env.SINGLE_PROCESS = '1'

  // also set NODE_ENV on process.env — zero-cache's config normalization
  // reads process.env.NODE_ENV to decide production vs development mode
  const origNodeEnv = process.env.NODE_ENV
  process.env.NODE_ENV = 'development'

  // build env for zero-cache. these are passed to runWorker() and also
  // propagated to in-process child workers via childWorker().
  const env: Record<string, string> = {
    // inherit process env for NODE_PATH, PATH, etc.
    ...(process.env as Record<string, string>),
    // zero-cache config (must come after spread to override)
    SINGLE_PROCESS: '1',
    NODE_ENV: 'development',
    ZERO_UPSTREAM_DB: opts.upstreamDb,
    ZERO_CVR_DB: opts.cvrDb,
    ZERO_CHANGE_DB: opts.changeDb,
    ZERO_REPLICA_FILE: opts.replicaFile,
    ZERO_PORT: String(opts.port ?? 0),
    ZERO_APP_ID: appId,
    ZERO_APP_PUBLICATIONS: publications,
    ZERO_LOG_LEVEL: opts.env?.ZERO_LOG_LEVEL || 'info',
    ZERO_NUM_SYNC_WORKERS: opts.env?.ZERO_NUM_SYNC_WORKERS || '1',
    ZERO_ENABLE_QUERY_PLANNER: 'false',
    ...opts.env,
  }

  // create a fake parent that zero-cache's runWorker() can communicate with.
  // in normal mode, this is the child process object. here it's an EventEmitter.
  const parent = new EventEmitter() as EventEmitter & {
    send: (msg: unknown) => boolean
    kill: (signal?: string) => void
    pid: number
  }

  // capture messages from zero-cache
  const parentMessages: unknown[] = []
  const parentEmitter = new EventEmitter()

  parent.send = (message: unknown) => {
    parentMessages.push(message)
    parentEmitter.emit('message', message)
    return true
  }
  parent.kill = (signal = 'SIGTERM') => {
    parent.emit(signal, signal)
  }
  parent.pid = process.pid

  // wrap parent with onMessageType/onceMessageType helpers that zero-cache expects
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

  // import and start zero-cache's runner
  let runWorkerFn: (parent: unknown, env: Record<string, string>) => Promise<void>
  try {
    // @rocicorp/zero's package.json exports don't expose internal modules,
    // so we resolve the full filesystem path and import directly.
    const { createRequire } = await import('node:module')
    const require = createRequire(import.meta.url)
    const zeroEntry = require.resolve('@rocicorp/zero')
    const runWorkerPath = zeroEntry.replace(
      /\/out\/.*$/,
      '/out/zero-cache/src/server/runner/run-worker.js'
    )
    const mod = await (import(runWorkerPath) as Promise<{ runWorker: typeof runWorkerFn }>)
    runWorkerFn = mod.runWorker
  } catch (err) {
    throw new Error(
      `failed to import zero-cache runWorker: ${err}. ` +
        'ensure @rocicorp/zero is installed.'
    )
  }

  // track state
  let isReady = false
  let resolvedPort = opts.port ?? 0
  let runWorkerPromise: Promise<void> | null = null

  // wait for "ready" message from zero-cache
  const readyPromise = new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error(`zero-cache embed: timed out waiting for ready after ${readyTimeout}ms`))
    }, readyTimeout)

    parentEmitter.on('message', (msg: unknown) => {
      if (Array.isArray(msg) && msg[0] === 'ready') {
        clearTimeout(timeout)
        isReady = true
        resolve()
      }
    })
  })

  // intercept process.exit() — zero-cache's ProcessManager may call it
  // during shutdown even in single-process mode (nested ProcessManagers).
  // we convert it to a no-op and handle cleanup ourselves.
  const origExit = process.exit
  process.exit = ((code?: number) => {
    // don't actually exit — just emit on parent to trigger cleanup
    parent.emit('exit', code ?? 0)
  }) as never

  // start runWorker (runs until killed)
  runWorkerPromise = runWorkerFn(wrappedParent, env).catch((err) => {
    if (!isReady) {
      throw err
    }
    // after ready, errors during shutdown are expected
  })

  // wait for zero-cache to be ready
  await readyPromise

  // if port was 0, we need to discover the actual port.
  // zero-cache logs the address but doesn't expose it programmatically.
  // for now, if port=0, the caller needs to discover it themselves.
  if (opts.port && opts.port > 0) {
    resolvedPort = opts.port
  }

  return {
    get port() {
      return resolvedPort
    },

    get ready() {
      return isReady
    },

    async stop() {
      isReady = false
      // send SIGTERM to trigger graceful shutdown
      wrappedParent.kill('SIGTERM')
      // wait for runWorker to finish (with timeout).
      // IMPORTANT: do NOT clean up process.env.SINGLE_PROCESS until after
      // runWorker completes — the ProcessManager's async exit handler
      // checks singleProcessMode() and must find it still set.
      if (runWorkerPromise) {
        await Promise.race([
          runWorkerPromise,
          new Promise((r) => setTimeout(r, 5000)),
        ])
      }
      // give async exit handlers time to complete
      await new Promise((r) => setTimeout(r, 200))
      // now safe to restore process.exit and process.env
      process.exit = origExit
      delete process.env.SINGLE_PROCESS
      if (origNodeEnv !== undefined) {
        process.env.NODE_ENV = origNodeEnv
      } else {
        delete process.env.NODE_ENV
      }
    },
  }
}
