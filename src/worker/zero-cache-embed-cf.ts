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

import { createBrowserProxy, type BrowserProxy } from '../pg-proxy-browser.js'
import { DoBackend, releaseDoBackendInstanceCaches } from '../pg-proxy-do-backend.js'
import {
  deleteReplicationState,
  resetReplicationState,
  signalReplicationChange,
} from '../replication/handler.js'
import {
  apiURLForCFInstance,
  dispatcherFastifyForCFInstance,
  logCFInstance,
  postgresURLForCFInstance,
  registerCFInstanceRuntime,
  releaseCFInstanceRuntime,
  setCFInstanceEnv,
  setCFInstanceProxy,
  setCFInstanceRuntimeAbandon,
  setCFInstanceRuntimeStop,
  sqliteDirectoryForCFInstance,
  sqlitePathForCFInstance,
  stopCFInstanceRuntimeForReplacement,
  type CFInstanceRuntime,
} from './cf-instance-runtime.js'
import {
  DurableObjectWebSocketHandoff,
  type DurableObjectWebSocket,
  type DurableObjectWebSocketHandoffContext,
  type HandoffRequestMessage,
} from './durable-object-websocket-handoff.js'
import { sweepCFInstanceSqliteHandles } from './embed-generation.js'
import { createLocalSqlBackend } from './local-sql-backend.js'
import { cleanupInactiveSnapshotTablesForCFInstance } from './shims/sqlite.js'
import { acquireZeroProcessEnv } from './shims/zero-process-env.js'
// static import so wrangler follows zero-cache's dependency tree and shim aliases.
import {
  abandonWorkerTree as _abandonWorkerTree,
  runWorker as _runWorker,
} from './zero-cache-run-worker.js'

const runWorkerFn = _runWorker as (
  parent: unknown,
  env: Record<string, string>
) => Promise<void>
const abandonWorkerTreeFn = _abandonWorkerTree as (taskId: string) => void

const WORKER_SHUTDOWN_TIMEOUT_MS = 5_000
const WORKER_FORCE_SHUTDOWN_TIMEOUT_MS = 5_000
const STARTUP_CLEANUP_TIMEOUT_MS = 15_000
const ZERO_STARTUP_STOPPED_ERROR = 'OrezZeroStartupStoppedError'

type GenerationState = {
  abandoned: boolean
  cleanupDone: boolean
  cleanupFailed: boolean
  runtime: CFInstanceRuntime
  workerDone: boolean
}

type EmbedParent = EventEmitter & {
  send: (msg: unknown, sendHandle?: unknown) => boolean
  kill: (signal?: string) => void
  pid: number
}

function releaseGenerationWhenComplete(generation: GenerationState): void {
  if (generation.abandoned) return
  if (generation.workerDone && generation.cleanupDone && !generation.cleanupFailed) {
    deleteReplicationState(generation.runtime.instanceId)
    releaseCFInstanceRuntime(generation.runtime)
  }
}

function abortReason(signal: AbortSignal): unknown {
  return signal.reason ?? new Error('zero-cache CF embed: operation aborted')
}

function isExpectedStartupStop(error: unknown): boolean {
  return error instanceof Error && error.name === ZERO_STARTUP_STOPPED_ERROR
}

function waitForAbortable<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) return Promise.reject(abortReason(signal))
  return new Promise<T>((resolve, reject) => {
    let settled = false
    const finish = (complete: () => void) => {
      if (settled) return
      settled = true
      signal.removeEventListener('abort', onAbort)
      complete()
    }
    const onAbort = () => finish(() => reject(abortReason(signal)))
    signal.addEventListener('abort', onAbort, { once: true })
    promise.then(
      (value) => finish(() => resolve(value)),
      (error) => finish(() => reject(error))
    )
  })
}

export interface ZeroCacheEmbedCFOptions {
  /** stable logical Durable Object identity. required for isolate-safe routing. */
  instanceId: string

  /** DO SQLite storage owned by this logical Durable Object. */
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

  /** optional instance-scoped diagnostic sink. */
  log?: (event: Record<string, unknown>) => void

  /** timeout in ms for the complete zero-cache startup (default: 30000) */
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
// this request is deliberately not abortable: a service-binding abort can
// reject locally while the target DO keeps running. teardown joins this
// promise before releasing the runtime claim, so a replacement cannot open
// transactions under the same owner until recovery has actually responded.
async function recoverRemoteTransactions(
  url: string,
  owner: string,
  backendFetch?: typeof fetch
): Promise<void> {
  const fetcher = backendFetch ?? fetch
  const resp = await fetcher(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ owner }),
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
  const startupStartedAt = Date.now()
  const appId = opts.appId || 'zero'
  const publications = opts.publications?.join(',') || `orez_${appId}_public`
  const readyTimeout = opts.readyTimeout ?? 30000
  const pgUser = opts.pgUser || 'user'
  const pgPassword = opts.pgPassword || ''
  const backendUrl = opts.backendUrl || 'https://orez-do-backend.local'
  const backendNamespace = opts.backendNamespace || appId
  if (!opts.apiFetch && (opts.env?.ZERO_MUTATE_URL || opts.env?.ZERO_QUERY_URL)) {
    throw new Error(
      'zero-cache CF embed: apiFetch is required with ZERO_MUTATE_URL or ZERO_QUERY_URL'
    )
  }
  if (!Number.isFinite(readyTimeout) || readyTimeout <= 0) {
    throw new Error('zero-cache CF embed: readyTimeout must be a positive number')
  }

  const startupDeadlineAt = startupStartedAt + readyTimeout
  const startupAbort = new AbortController()
  const startupOperations = new Set<Promise<unknown>>()
  let startupPhase = 'entry'
  const emitStartupEvent = (event: Record<string, unknown>) => {
    try {
      opts.log?.({
        component: 'embed',
        instanceId: opts.instanceId,
        ...event,
      })
    } catch {
      // diagnostics cannot break startup
    }
  }
  const assertWithinStartupDeadline = (phase: string) => {
    if (startupAbort.signal.aborted) throw abortReason(startupAbort.signal)
    const now = Date.now()
    if (now < startupDeadlineAt) return
    startupPhase = phase
    const error = new Error(
      `zero-cache CF embed: startup timed out after ${readyTimeout}ms in phase ${phase}`
    )
    emitStartupEvent({
      elapsedMs: now - startupStartedAt,
      event: 'startup-phase-timeout',
      phase,
      timeoutMs: readyTimeout,
    })
    startupAbort.abort(error)
    throw error
  }
  let startupTimer!: ReturnType<typeof setTimeout>
  const armStartupTimer = () => {
    startupTimer = setTimeout(
      () => {
        try {
          assertWithinStartupDeadline(startupPhase)
        } catch {
          return
        }
        armStartupTimer()
      },
      Math.max(0, startupDeadlineAt - Date.now())
    )
  }
  armStartupTimer()
  const trackStartupOperation = <T>(operation: Promise<T>): Promise<T> => {
    startupOperations.add(operation)
    void operation.then(
      () => startupOperations.delete(operation),
      () => startupOperations.delete(operation)
    )
    return operation
  }
  const runStartupPhase = async <T>(phase: string, run: () => Promise<T>) => {
    startupPhase = phase
    assertWithinStartupDeadline(phase)
    const phaseStartedAt = Date.now()
    emitStartupEvent({
      elapsedMs: phaseStartedAt - startupStartedAt,
      event: 'startup-phase-start',
      phase,
      remainingMs: Math.max(0, startupDeadlineAt - phaseStartedAt),
    })
    try {
      const operation = trackStartupOperation(Promise.resolve().then(run))
      const result = await waitForAbortable(operation, startupAbort.signal)
      assertWithinStartupDeadline(phase)
      emitStartupEvent({
        elapsedMs: Date.now() - startupStartedAt,
        event: 'startup-phase-complete',
        phase,
        phaseElapsedMs: Date.now() - phaseStartedAt,
        remainingMs: Math.max(0, startupDeadlineAt - Date.now()),
      })
      return result
    } catch (error) {
      emitStartupEvent({
        elapsedMs: Date.now() - startupStartedAt,
        error: error instanceof Error ? error.message : String(error),
        event: 'startup-phase-failed',
        phase,
        phaseElapsedMs: Date.now() - phaseStartedAt,
      })
      throw error
    }
  }

  let registeredRuntime: CFInstanceRuntime | undefined
  try {
    await runStartupPhase('replacement-stop', () =>
      stopCFInstanceRuntimeForReplacement(opts.instanceId, opts.doSqlite)
    )
    await runStartupPhase('runtime-registration', async () => {
      registeredRuntime = registerCFInstanceRuntime({
        apiFetch: opts.apiFetch,
        doSqlite: opts.doSqlite,
        env: opts.env ?? {},
        instanceId: opts.instanceId,
        log: opts.log,
        pgPassword,
        pgUser,
      })
    })
  } catch (error) {
    if (registeredRuntime) releaseCFInstanceRuntime(registeredRuntime)
    clearTimeout(startupTimer)
    throw error
  }
  if (!registeredRuntime) {
    clearTimeout(startupTimer)
    throw new Error('zero-cache CF embed: runtime registration did not complete')
  }
  const runtime = registeredRuntime

  const generation: GenerationState = {
    abandoned: false,
    cleanupDone: false,
    cleanupFailed: false,
    runtime,
    workerDone: true,
  }
  const instanceId = runtime.instanceId
  const zeroTaskId = `orez-cf-${runtime.encodedId}`

  const globalRecord = globalThis as Record<PropertyKey, unknown>
  let processRecord: Record<PropertyKey, unknown>
  let releaseProcessEnv: (() => void) | null = null
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
  let debugEmbed = false
  const webSocketHandoff = new DurableObjectWebSocketHandoff(() => fastifyInstance)
  const releaseProcessEnvWhenWorkerDone = () => {
    if (!generation.workerDone || !releaseProcessEnv) return
    releaseProcessEnv()
    releaseProcessEnv = null
  }

  const shutdown = (): Promise<void> => {
    if (generation.abandoned) return Promise.resolve()
    if (shutdownPromise) return shutdownPromise
    stopping = true
    isReady = false

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
        let workerStopped = await Promise.race([
          workerSettledPromise.then(() => true),
          new Promise<false>((resolve) => {
            timeout = setTimeout(() => resolve(false), WORKER_SHUTDOWN_TIMEOUT_MS)
          }),
        ])
        if (timeout) clearTimeout(timeout)
        if (!workerStopped) {
          logCFInstance(runtime, {
            component: 'embed',
            event: 'worker-force-stop',
            timeoutMs: WORKER_SHUTDOWN_TIMEOUT_MS,
          })
          try {
            ;(wrappedParent as { kill(signal?: string): void }).kill('SIGQUIT')
          } catch (err) {
            cleanupErrors.push(err)
          }
          timeout = undefined
          workerStopped = await Promise.race([
            workerSettledPromise.then(() => true),
            new Promise<false>((resolve) => {
              timeout = setTimeout(() => resolve(false), WORKER_FORCE_SHUTDOWN_TIMEOUT_MS)
            }),
          ])
          if (timeout) clearTimeout(timeout)
          if (!workerStopped) {
            cleanupErrors.push(
              new Error(
                `zero-cache CF embed: worker did not terminate after SIGTERM (${WORKER_SHUTDOWN_TIMEOUT_MS}ms) and SIGQUIT (${WORKER_FORCE_SHUTDOWN_TIMEOUT_MS}ms)`
              )
            )
          }
        }
      }
      if (generation.abandoned) return
      if (
        workerFailed &&
        workerError !== startupFailure &&
        !isExpectedStartupStop(workerError)
      ) {
        cleanupErrors.push(workerError)
      }

      await Promise.allSettled([...startupOperations])

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
      releaseDoBackendInstanceCaches(instanceId)

      try {
        webSocketHandoff.closeAll()
      } catch (err) {
        resourceCleanupFailed = true
        cleanupErrors.push(err)
      }
      try {
        sweepCFInstanceSqliteHandles(runtime)
      } catch (err) {
        resourceCleanupFailed = true
        cleanupErrors.push(err)
      }
      releaseProcessEnvWhenWorkerDone()
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
      logCFInstance(runtime, {
        component: 'embed',
        error,
        event: 'unexpected-worker-exit-cleanup-failed',
      })
      console.error(
        `[orez-zero-cache-cf:${runtime.instanceId}] unexpected worker exit cleanup failed`,
        error
      )
    })
  }

  setCFInstanceRuntimeStop(runtime, shutdown)
  setCFInstanceRuntimeAbandon(runtime, () => {
    generation.abandoned = true
    generation.cleanupDone = true
    stopping = true
    isReady = false
    abandonWorkerTreeFn(zeroTaskId)
    if (releaseProcessEnv) {
      releaseProcessEnv()
      releaseProcessEnv = null
    }
    deleteReplicationState(instanceId)
    releaseDoBackendInstanceCaches(instanceId)
    runtime.sqliteHandles.clear()
    parentEmitter?.removeAllListeners()
    parent?.removeAllListeners()
  })

  try {
    releaseProcessEnv = acquireZeroProcessEnv()
    processRecord = globalRecord.process as Record<PropertyKey, unknown>

    await runStartupPhase('stale-snapshot-cleanup', () =>
      cleanupInactiveSnapshotTablesForCFInstance(runtime)
    )

    const localSql = createLocalSqlBackend(opts.doSqlite)
    const txOwner = `${EMBED_TX_OWNER}:${runtime.encodedId}`

    const instantiateBackend = (dbName: string) =>
      new DoBackend(backendUrl, dbName, backendNamespace, {
        allowTransactionalDDL: true,
        fetch: dbName === 'postgres' ? opts.backendFetch : localSql.fetch,
        instanceId,
        log: opts.log,
        signal: startupAbort.signal,
        signalReplication: () => signalReplicationChange(instanceId),
        txOwner,
      })

    const createRootBackend = (dbName: string) => {
      const backend = instantiateBackend(dbName)
      backendRoots.push(backend)
      return backend
    }

    localSql.recoverOrphanedTransactions()
    await runStartupPhase('remote-transaction-recovery', () =>
      recoverRemoteTransactions(
        `${backendUrl.replace(/\/+$/, '')}/recover-txs?db=postgres&ns=${encodeURIComponent(backendNamespace)}`,
        txOwner,
        opts.backendFetch
      )
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

    const backendReadyResults = await runStartupPhase('backend-initialization', () =>
      Promise.allSettled(
        Object.entries(backends).map(async ([database, backend]) => {
          const startedAt = Date.now()
          emitStartupEvent({
            database,
            elapsedMs: startedAt - startupStartedAt,
            event: 'startup-backend-start',
            phase: 'backend-initialization',
          })
          try {
            await backend.waitReady
            assertWithinStartupDeadline('backend-initialization')
            emitStartupEvent({
              database,
              elapsedMs: Date.now() - startupStartedAt,
              event: 'startup-backend-complete',
              phase: 'backend-initialization',
              phaseElapsedMs: Date.now() - startedAt,
            })
          } catch (error) {
            emitStartupEvent({
              database,
              elapsedMs: Date.now() - startupStartedAt,
              error: error instanceof Error ? error.message : String(error),
              event: 'startup-backend-failed',
              phase: 'backend-initialization',
              phaseElapsedMs: Date.now() - startedAt,
            })
            throw error
          }
        })
      )
    )
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

    proxy = await runStartupPhase('proxy-creation', () =>
      createBrowserProxy(
        {
          postgres: proxyBackends.postgres as any,
          cvr: proxyBackends.cvr as any,
          cdb: proxyBackends.cdb as any,
          postgresReplicas: [],
        } as any,
        {
          debugWire: opts.env?.OREZ_DEBUG_WIRE === '1',
          instanceId,
          pgUser,
          pgPassword,
          singleDb: false,
          logLevel: opts.env?.ZERO_LOG_LEVEL || 'info',
        }
      )
    )
    setCFInstanceProxy(runtime, (port) => proxy?.handleConnection(port))

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

    const env: Record<string, string> = {
      SINGLE_PROCESS: '1',
      NODE_ENV: 'development',
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
      ZERO_UPSTREAM_DB: postgresURLForCFInstance(instanceId, 'postgres', pgUser),
      ZERO_CVR_DB: postgresURLForCFInstance(instanceId, 'zero_cvr', pgUser),
      ZERO_CHANGE_DB: postgresURLForCFInstance(instanceId, 'zero_cdb', pgUser),
      ZERO_REPLICA_FILE: sqlitePathForCFInstance(instanceId),
      ZERO_STORAGE_DB_TMP_DIR: sqliteDirectoryForCFInstance(instanceId),
      ZERO_PORT: String(runtime.basePort),
      ZERO_TASK_ID: zeroTaskId,
      ZERO_SHADOW_SYNC_ENABLED: 'false',
    }
    if (opts.apiFetch) {
      for (const key of ['ZERO_MUTATE_URL', 'ZERO_QUERY_URL'] as const) {
        if (env[key]) env[key] = apiURLForCFInstance(instanceId, env[key])
      }
    }
    setCFInstanceEnv(runtime, env)
    resetReplicationState(instanceId, env, opts.log)

    debugEmbed = env.OREZ_DEBUG_EMBED === '1'

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
        const value = Reflect.get(target, prop, target)
        if (typeof value !== 'function') return value
        return (...args: unknown[]) => {
          const result = value.apply(target, args)
          return result === target ? receiver : result
        }
      },
    })

    const readyPromise = new Promise<void>((resolve) => {
      parentEmitter?.on('message', (msg: unknown) => {
        if (debugEmbed) {
          logCFInstance(runtime, {
            component: 'embed',
            event: 'parent-message',
            message: msg,
          })
        }
        if (!stopping && Array.isArray(msg) && msg[0] === 'ready') {
          isReady = true
          resolve()
        }
      })
    })

    generation.workerDone = false
    runWorkerPromise = Promise.resolve().then(() => runWorkerFn(wrappedParent, env))
    void runWorkerPromise.catch((err) => {
      if (debugEmbed) {
        logCFInstance(runtime, {
          component: 'embed',
          error: err,
          event: 'worker-error',
        })
      }
    })
    workerSettledPromise = runWorkerPromise.then(
      () => {
        generation.workerDone = true
        releaseProcessEnvWhenWorkerDone()
        handleUnexpectedWorkerExit()
        if (generation.cleanupDone) parent?.removeAllListeners()
        releaseGenerationWhenComplete(generation)
      },
      (error) => {
        workerError = error
        workerFailed = true
        generation.workerDone = true
        releaseProcessEnvWhenWorkerDone()
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
    await runStartupPhase('worker-readiness', () =>
      Promise.race([readyPromise, workerStartupPromise])
    )

    fastifyInstance = dispatcherFastifyForCFInstance(instanceId)
    assertWithinStartupDeadline('startup-complete')
  } catch (startupError) {
    clearTimeout(startupTimer)
    if (!startupAbort.signal.aborted) startupAbort.abort(startupError)
    startupFailure = startupError
    const cleanupAbort = new AbortController()
    const cleanupStartedAt = Date.now()
    emitStartupEvent({
      elapsedMs: cleanupStartedAt - startupStartedAt,
      event: 'startup-cleanup-start',
      phase: startupPhase,
      timeoutMs: STARTUP_CLEANUP_TIMEOUT_MS,
    })
    const cleanupTimer = setTimeout(() => {
      const error = new Error(
        `zero-cache CF embed: startup cleanup timed out after ${STARTUP_CLEANUP_TIMEOUT_MS}ms`
      )
      emitStartupEvent({
        elapsedMs: Date.now() - startupStartedAt,
        event: 'startup-cleanup-timeout',
        phase: startupPhase,
        timeoutMs: STARTUP_CLEANUP_TIMEOUT_MS,
      })
      cleanupAbort.abort(error)
    }, STARTUP_CLEANUP_TIMEOUT_MS)
    try {
      await waitForAbortable(shutdown(), cleanupAbort.signal)
      emitStartupEvent({
        elapsedMs: Date.now() - startupStartedAt,
        event: 'startup-cleanup-complete',
        phase: startupPhase,
        phaseElapsedMs: Date.now() - cleanupStartedAt,
      })
    } catch (cleanupError) {
      clearTimeout(cleanupTimer)
      throw new AggregateError(
        [startupError, cleanupError],
        'zero-cache CF embed: startup failed and teardown also failed',
        { cause: startupError }
      )
    }
    clearTimeout(cleanupTimer)
    throw startupError
  }
  clearTimeout(startupTimer)

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
