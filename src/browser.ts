/**
 * orez browser entry — aligned to orez-node architecture.
 *
 * mirrors index.ts startup sequence:
 *   1. create PGlite Web Workers (3 instances)
 *   2. install change tracking
 *   3. start pg-proxy-browser (wire protocol proxy)
 *   4. start zero-cache (SINGLE_PROCESS=1, connects to proxy)
 *
 * intended to run inside a Web Worker. PGlite instances run in
 * separate Web Workers (like orez-node's worker threads).
 */

import { PGliteWebProxy } from './pglite-web-proxy.js'
import { installChangeTracking } from './replication/change-tracker.js'
import { resetReplicationState } from './replication/handler.js'

import type { PGlite } from '@electric-sql/pglite'

export interface OrezBrowserConfig {
  /** app ID for zero-cache (default: 'zero') */
  appId?: string

  /** publication names for replication */
  publications?: string[]

  /** URL for PGlite web worker script */
  pgliteWorkerUrl: string

  /** URL for bedrock-sqlite WASM */
  bedrockSqliteUrl?: string

  /** init SQL to run on postgres instance after creation */
  initSql?: string

  /** log level */
  logLevel?: string
}

export interface OrezBrowserInstance {
  /** PGlite proxies (for direct queries from project-server) */
  instances: {
    postgres: PGliteWebProxy
    cvr: PGliteWebProxy
    cdb: PGliteWebProxy
  }

  /** signal the replication handler that changes are available */
  signalReplication(): void

  /** handle a WebSocket connection from a Zero client */
  handleWebSocket(ws: any, url?: string, headers?: Record<string, string>): void

  /** handle an HTTP request (push/pull) */
  handleHttp(request: { method: string; url: string; headers?: Record<string, string>; body?: string | null }): Promise<{ status: number; headers: Record<string, string>; body: string }>

  /** stop everything */
  stop(): Promise<void>
}

export async function startOrezBrowser(config: OrezBrowserConfig): Promise<OrezBrowserInstance> {
  const appId = config.appId || 'zero'
  const publications = config.publications?.join(',') || `orez_${appId}_public`

  // step 1: create PGlite Web Workers (3 instances, like orez-node)
  const pgPostgresWorker = new Worker(config.pgliteWorkerUrl, { type: 'module', name: 'pglite-postgres' })
  const pgCvrWorker = new Worker(config.pgliteWorkerUrl, { type: 'module', name: 'pglite-cvr' })
  const pgCdbWorker = new Worker(config.pgliteWorkerUrl, { type: 'module', name: 'pglite-cdb' })

  // init each PGlite worker
  pgPostgresWorker.postMessage({ type: 'init', dataDir: 'idb://orez-postgres', name: 'postgres' })
  pgCvrWorker.postMessage({ type: 'init', dataDir: 'idb://orez-cvr', name: 'cvr' })
  pgCdbWorker.postMessage({ type: 'init', dataDir: 'idb://orez-cdb', name: 'cdb' })

  // create proxies (like orez-node's PGliteWorkerProxy)
  const pgPostgres = new PGliteWebProxy(pgPostgresWorker, 'postgres')
  const pgCvr = new PGliteWebProxy(pgCvrWorker, 'cvr')
  const pgCdb = new PGliteWebProxy(pgCdbWorker, 'cdb')

  await Promise.all([pgPostgres.waitReady, pgCvr.waitReady, pgCdb.waitReady])
  console.debug('[orez-browser] all 3 PGlite workers ready')

  // step 2: install change tracking (like orez-node)
  await installChangeTracking(pgPostgres as unknown as PGlite)
  console.debug('[orez-browser] change tracking installed')

  // run user init SQL if provided
  if (config.initSql) {
    await pgPostgres.exec(config.initSql)
    console.debug('[orez-browser] init SQL complete')
  }

  // create publication
  try {
    const pubs = await pgPostgres.query<{ count: string }>(
      `SELECT count(*) as count FROM pg_publication WHERE pubname = $1`,
      [publications]
    )
    if (Number(pubs.rows[0]?.count) === 0) {
      await pgPostgres.exec(`CREATE PUBLICATION "${publications}"`)
    }
  } catch {}

  // step 3: start pg-proxy-browser
  // the proxy handles wire protocol, replication, mutexes — like orez-node's pg-proxy.
  // zero-cache's postgres shim routes queries through this proxy.
  const { createBrowserProxy } = await import('./pg-proxy-browser.js')
  const proxy = await createBrowserProxy(
    { postgres: pgPostgres as unknown as PGlite, cvr: pgCvr as unknown as PGlite, cdb: pgCdb as unknown as PGlite },
    { pgPassword: '', pgUser: 'user' }
  )
  console.debug('[orez-browser] pg-proxy-browser started')

  // step 4: start zero-cache (SINGLE_PROCESS=1)
  // zero-cache uses the postgres shim which wraps PGlite via the proxies.
  // the postgres shim + handler.ts provide the same replication interception
  // as pg-proxy does in orez-node.
  //
  // install PGlite globals for the postgres shim
  ;(globalThis as any).__orez_pglite = pgPostgres
  ;(globalThis as any).__orez_pglite_instances = {
    postgres: pgPostgres,
    cvr: pgCvr,
    cdb: pgCdb,
  }

  // start zero-cache via browser-embed's runWorker
  const { startZeroCacheEmbedBrowser } = await import('./worker/browser-embed.js')
  const zc = await startZeroCacheEmbedBrowser({
    pglite: pgPostgres as unknown as PGlite,
    appId,
    publications: config.publications,
    env: {
      ZERO_LOG_LEVEL: config.logLevel || 'info',
    },
  })
  console.debug('[orez-browser] zero-cache started')

  // step 5: expose API
  const { signalReplicationChange } = await import('./replication/handler.js')

  return {
    instances: { postgres: pgPostgres, cvr: pgCvr, cdb: pgCdb },

    signalReplication() {
      signalReplicationChange()
    },

    handleWebSocket(ws: any, url = '/', headers?: Record<string, string>) {
      zc.handleWebSocket(ws, url, headers)
    },

    async handleHttp(request: { method: string; url: string; headers?: Record<string, string>; body?: string | null }) {
      return zc.handleHttp(request)
    },

    async stop() {
      await zc.stop()
      proxy.close()
      resetReplicationState()
      await Promise.all([
        pgPostgres.close(),
        pgCvr.close(),
        pgCdb.close(),
      ])
    },
  }
}
