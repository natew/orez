/**
 * zero-cache embedded runner.
 *
 * runs zero-cache's `runWorker()` in-process with SINGLE_PROCESS=1,
 * using shims that redirect postgres/sqlite/http to PGlite, DO SQLite,
 * and DO's fetch() handler respectively.
 *
 * this is the Phase 3 integration point where shims are wired together.
 * the actual shim registration happens via bundler aliases (esbuild/wrangler):
 *
 *   alias: {
 *     'postgres': './src/worker/shims/postgres.js',
 *     '@rocicorp/zero-sqlite3': './src/worker/shims/sqlite.js',
 *   }
 *
 * env vars configure zero-cache for in-process single-db mode:
 *   SINGLE_PROCESS=1       — all workers in-process via EventEmitter
 *   ZERO_UPSTREAM_DB       — intercepted by postgres shim
 *   ZERO_CVR_DB            — intercepted by postgres shim
 *   ZERO_CHANGE_DB         — intercepted by postgres shim
 *   ZERO_REPLICA_FILE      — intercepted by sqlite shim
 *   ZERO_PORT=0            — don't bind a port
 */

import type { PGlite } from '@electric-sql/pglite'

export interface ZeroCacheEmbedOptions {
  /** PGlite instance for upstream database */
  pglite: PGlite

  /**
   * DO SQLite storage. in CF Workers this is `this.ctx.storage.sql`.
   * used for zero-cache's replica (CVR + change db).
   */
  doSqlite?: unknown

  /** zero app ID (default: 'zero') */
  appId?: string

  /** publication names */
  publications?: string[]

  /** additional env vars passed to zero-cache */
  env?: Record<string, string>
}

export interface ZeroCacheEmbed {
  /**
   * handle a request from DO's fetch().
   * routes to zero-cache's HTTP/WebSocket handlers.
   */
  handleRequest(request: Request): Promise<Response>

  /** stop zero-cache workers */
  stop(): Promise<void>
}

/**
 * start zero-cache in embedded mode.
 *
 * prerequisites:
 * - PGlite must be initialized with change tracking installed
 * - bundler must alias 'postgres' and '@rocicorp/zero-sqlite3' to our shims
 * - shims must be initialized with the PGlite/DO SQLite instances
 *
 * Phase 3 TODO:
 * - import runWorker from @rocicorp/zero
 * - configure env for SINGLE_PROCESS=1
 * - register shim instances on globalThis
 * - call runWorker(null, env)
 * - capture fastify instance for request routing
 * - wire up InProcessWriter for change streaming
 */
export async function startZeroCacheEmbed(
  opts: ZeroCacheEmbedOptions
): Promise<ZeroCacheEmbed> {
  const appId = opts.appId || 'zero'
  const publications = opts.publications?.join(',') || `orez_${appId}_public`

  // register shim instances so the bundler-aliased shims can find them.
  // the postgres shim reads __orez_pglite, the sqlite shim reads __orez_do_sqlite.
  ;(globalThis as any).__orez_pglite = opts.pglite
  if (opts.doSqlite) {
    ;(globalThis as any).__orez_do_sqlite = opts.doSqlite
  }

  // env vars for zero-cache single-process mode
  const env: Record<string, string> = {
    SINGLE_PROCESS: '1',
    ZERO_UPSTREAM_DB: 'pglite://in-process',
    ZERO_CVR_DB: 'pglite://in-process',
    ZERO_CHANGE_DB: 'pglite://in-process',
    ZERO_REPLICA_FILE: ':do-sqlite:',
    ZERO_PORT: '0',
    ZERO_APP_ID: appId,
    ZERO_APP_PUBLICATIONS: publications,
    NODE_ENV: 'development',
    ZERO_LOG_LEVEL: 'info',
    ZERO_NUM_SYNC_WORKERS: '1',
    ZERO_ENABLE_QUERY_PLANNER: 'false',
    ...opts.env,
  }

  // Phase 3 TODO: import and call runWorker
  // const { runWorker } = await import('@rocicorp/zero/out/zero-cache/src/server/main.js')
  // await runWorker(null, env)

  // Phase 3 TODO: capture the fastify/http service for routing
  // const httpAdapter = ...

  return {
    async handleRequest(_request: Request): Promise<Response> {
      // Phase 3: route to zero-cache via httpAdapter
      return new Response('zero-cache embed not yet initialized', { status: 503 })
    },

    async stop(): Promise<void> {
      // Phase 3: stop zero-cache workers
      delete (globalThis as any).__orez_pglite
      delete (globalThis as any).__orez_do_sqlite
    },
  }
}
