/**
 * orez: pglite-powered zero-sync development backend.
 *
 * starts a pglite instance, tcp proxy, and zero-cache process.
 * replaces docker-based postgresql and zero-cache with a single
 * `bun run` command.
 */

import { spawn, type ChildProcess } from 'node:child_process'
import { existsSync, mkdirSync, readFileSync, unlinkSync, writeFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import { resolve } from 'node:path'

import { createLogStore, type LogStore } from './admin/log-store.js'
import { createHttpLogStore, startHttpProxy, type HttpLogStore } from './admin/http-proxy.js'
import { getConfig, getConnectionString } from './config.js'
import { log, port, setLogLevel, setLogStore } from './log.js'
import { startPgProxy } from './pg-proxy.js'
import { createPGliteInstances, runMigrations } from './pglite-manager.js'
import { findPort } from './port.js'
import { installChangeTracking } from './replication/change-tracker.js'

import type { ZeroLiteConfig } from './config.js'
import type { PGlite } from '@electric-sql/pglite'

export { getConfig, getConnectionString } from './config.js'
export type { Hook, LogLevel, ZeroLiteConfig } from './config.js'

// helper to run a hook (string command or callback function)
async function runHook(
  hook: string | (() => void | Promise<void>) | undefined,
  name: string,
  env: Record<string, string>
): Promise<void> {
  if (!hook) return

  if (typeof hook === 'function') {
    log.debug.orez(`running ${name} callback`)
    await hook()
    log.orez(`${name} done`)
    return
  }

  // string command
  log.debug.orez(`running ${name}: ${hook}`)
  await new Promise<void>((resolve, reject) => {
    const child = spawn(hook, {
      shell: true,
      stdio: 'inherit',
      env: { ...process.env, ...env },
    })
    child.on('exit', (code) => {
      if (code === 0) {
        log.orez(`${name} done`)
        resolve()
      } else {
        reject(new Error(`${name} exited with code ${code}`))
      }
    })
    child.on('error', reject)
  })
}

// resolve a package entry — import.meta.resolve doesn't work in vitest
function resolvePackage(pkg: string): string {
  try {
    const resolved = import.meta.resolve(pkg)
    if (resolved) return resolved.replace('file://', '')
  } catch {}
  try {
    const require = createRequire(import.meta.url)
    return require.resolve(pkg)
  } catch {}
  return ''
}

export async function startZeroLite(overrides: Partial<ZeroLiteConfig> = {}) {
  const config = getConfig(overrides)
  setLogLevel(config.logLevel)

  // find available ports
  const pgPort = await findPort(config.pgPort)
  const zeroPort = config.skipZeroCache
    ? config.zeroPort
    : await findPort(config.zeroPort)
  const adminPort = config.adminPort > 0 ? await findPort(config.adminPort) : 0
  if (pgPort !== config.pgPort)
    log.debug.orez(`port ${config.pgPort} in use, using ${pgPort}`)
  if (!config.skipZeroCache && zeroPort !== config.zeroPort)
    log.debug.orez(`port ${config.zeroPort} in use, using ${zeroPort}`)
  if (adminPort > 0 && adminPort !== config.adminPort)
    log.debug.orez(`port ${config.adminPort} in use, using ${adminPort}`)
  config.pgPort = pgPort
  config.zeroPort = zeroPort
  config.adminPort = adminPort

  // create log store for admin dashboard
  const logStore: LogStore | undefined =
    adminPort > 0 ? createLogStore(config.dataDir) : undefined

  // wire up logStore so all log.* calls flow to admin dashboard
  setLogStore(logStore)

  // create http log store for HTTP tab
  const httpLog: HttpLogStore | undefined =
    adminPort > 0 ? createHttpLogStore() : undefined

  log.debug.orez(`data dir: ${resolve(config.dataDir)}`)

  mkdirSync(config.dataDir, { recursive: true })

  // write pid file for IPC (pg_restore uses this to signal restart)
  const pidFile = resolve(config.dataDir, 'orez.pid')
  writeFileSync(pidFile, String(process.pid))

  // start pglite (separate instances for postgres, zero_cvr, zero_cdb)
  const instances = await createPGliteInstances(config)
  const db = instances.postgres

  // run migrations (on postgres instance only)
  const migrationsApplied = await runMigrations(db, config)

  // install change tracking (on postgres instance only)
  log.debug.orez('installing change tracking')
  await installChangeTracking(db)

  // start tcp proxy (routes connections to correct instance by database name)
  const pgServer = await startPgProxy(instances, config)

  if (migrationsApplied > 0)
    log.orez(
      `${migrationsApplied} migration${migrationsApplied === 1 ? '' : 's'} applied`
    )

  // seed data if needed
  await seedIfNeeded(db, config)

  // run on-db-ready hook (e.g. migrations) before zero-cache starts
  if (config.onDbReady) {
    const upstreamUrl = getConnectionString(config, 'postgres')
    const cvrUrl = getConnectionString(config, 'zero_cvr')
    const cdbUrl = getConnectionString(config, 'zero_cdb')
    await runHook(config.onDbReady, 'on-db-ready', {
      ZERO_UPSTREAM_DB: upstreamUrl,
      ZERO_CVR_DB: cvrUrl,
      ZERO_CHANGE_DB: cdbUrl,
      DATABASE_URL: upstreamUrl,
      OREZ_PG_PORT: String(config.pgPort),
    })

    // re-install change tracking on tables created by on-db-ready
    log.debug.orez('re-installing change tracking after on-db-ready')
    await installChangeTracking(db)
  }

  // clean up stale sqlite replica from previous runs
  cleanupStaleReplica(config)

  // when admin is enabled, zero-cache runs on internal port with http proxy in front
  let zeroInternalPort = config.zeroPort
  let httpProxyServer: import('node:http').Server | null = null
  if (httpLog && !config.skipZeroCache) {
    zeroInternalPort = await findPort(config.zeroPort + 1000)
    log.debug.orez(`http proxy: public ${config.zeroPort} → internal ${zeroInternalPort}`)
  }

  // start zero-cache
  let zeroCacheProcess: ChildProcess | null = null
  let zeroEnv: Record<string, string> = {}
  if (!config.skipZeroCache) {
    // use internal port when http proxy is enabled
    const zeroConfig = httpLog
      ? { ...config, zeroPort: zeroInternalPort }
      : config
    const result = await startZeroCache(zeroConfig, logStore)
    zeroCacheProcess = result.process
    zeroEnv = result.env
    await waitForZeroCache(zeroConfig)

    // start http proxy in front of zero-cache when admin is enabled
    if (httpLog) {
      httpProxyServer = await startHttpProxy({
        listenPort: config.zeroPort,
        targetPort: zeroInternalPort,
        httpLog,
      })
      log.debug.orez(`http proxy listening on ${config.zeroPort}`)
    }

    log.zero(`ready ${port(config.zeroPort, 'magenta')}`)
  } else {
    log.orez('skip zero-cache')
  }

  // run on-healthy hook after all services are ready
  if (config.onHealthy) {
    await runHook(config.onHealthy, 'on-healthy', {
      OREZ_PG_PORT: String(config.pgPort),
      OREZ_ZERO_PORT: String(config.zeroPort),
    })
  }

  const killZeroCache = async () => {
    if (zeroCacheProcess && !zeroCacheProcess.killed) {
      zeroCacheProcess.kill('SIGTERM')
      await new Promise<void>((r) => {
        const timeout = setTimeout(() => {
          if (zeroCacheProcess && !zeroCacheProcess.killed) {
            zeroCacheProcess.kill('SIGKILL')
          }
          r()
        }, 3000)
        zeroCacheProcess!.on('exit', () => {
          clearTimeout(timeout)
          r()
        })
      })
    }
  }

  // simple restart without any state cleanup
  const restartZeroCache = async () => {
    await killZeroCache()
    // use internal port when http proxy is enabled
    const zeroConfig = httpLog ? { ...config, zeroPort: zeroInternalPort } : config
    const result = await startZeroCache(zeroConfig, logStore)
    zeroCacheProcess = result.process
    zeroEnv = result.env
    await waitForZeroCache(zeroConfig)
  }

  // unified reset function for zero state
  // modes:
  //   'cache-only' - deletes replica file only (fast, for minor sync issues)
  //   'full' - deletes CVR/CDB + replica and recreates instances (for schema changes)
  let resetInProgress = false
  const resetZeroState = async (mode: 'cache-only' | 'full'): Promise<void> => {
    if (resetInProgress) {
      log.orez('reset already in progress, skipping')
      return
    }
    resetInProgress = true

    try {
      log.orez(`resetting zero state (${mode})...`)

      // stop zero-cache first
      log.orez('stopping zero-cache...')
      await killZeroCache()
      log.orez('zero-cache stopped')

      if (mode === 'full') {
        // give connections time to drain before closing instances
        await new Promise((r) => setTimeout(r, 500))

        // close CVR/CDB instances
        log.orez('closing CVR/CDB...')
        await instances.cvr.close().catch((e: any) => {
          log.debug.orez(`cvr close error (expected): ${e?.message || e}`)
        })
        await instances.cdb.close().catch((e: any) => {
          log.debug.orez(`cdb close error (expected): ${e?.message || e}`)
        })
        log.orez('CVR/CDB closed')

        // delete CVR/CDB data directories
        log.orez('deleting CVR/CDB data...')
        const { rmSync } = await import('node:fs')
        for (const dir of ['pgdata-cvr', 'pgdata-cdb']) {
          try {
            rmSync(resolve(config.dataDir, dir), { recursive: true, force: true })
          } catch {}
        }

        // recreate CVR/CDB instances
        log.orez('recreating CVR/CDB...')
        const { PGlite } = await import('@electric-sql/pglite')
        mkdirSync(resolve(config.dataDir, 'pgdata-cvr'), { recursive: true })
        mkdirSync(resolve(config.dataDir, 'pgdata-cdb'), { recursive: true })
        instances.cvr = new PGlite({
          dataDir: resolve(config.dataDir, 'pgdata-cvr'),
          relaxedDurability: true,
        })
        instances.cdb = new PGlite({
          dataDir: resolve(config.dataDir, 'pgdata-cdb'),
          relaxedDurability: true,
        })
        await instances.cvr.waitReady
        await instances.cdb.waitReady
        log.orez('CVR/CDB recreated')
      }

      // always clean up replica file
      cleanupStaleReplica(config)
      log.orez('replica cleaned up')

      // restart zero-cache
      log.orez('starting zero-cache...')
      // use internal port when http proxy is enabled
      const zeroConfig = httpLog ? { ...config, zeroPort: zeroInternalPort } : config
      const result = await startZeroCache(zeroConfig, logStore)
      zeroCacheProcess = result.process
      zeroEnv = result.env

      await waitForZeroCache(zeroConfig)
      log.orez(`zero state reset complete (${mode})`)
      log.zero(`ready ${port(config.zeroPort, 'magenta')}`)
    } catch (err: any) {
      log.orez(`reset failed: ${err?.message || err}`)
      throw err
    } finally {
      resetInProgress = false
    }
  }

  // handle SIGUSR1 to reset zero state (sent by pg_restore)
  if (!config.skipZeroCache) {
    process.on('SIGUSR1', () => {
      log.orez('received SIGUSR1')
      resetZeroState('full').catch((err) => {
        log.orez(`SIGUSR1 reset failed: ${err?.message || err}`)
      })
    })
  }

  const stop = async () => {
    log.debug.orez('shutting down')
    httpProxyServer?.close()
    await killZeroCache()
    pgServer.close()
    await Promise.all([
      instances.postgres.close(),
      instances.cvr.close(),
      instances.cdb.close(),
    ])
    try {
      unlinkSync(pidFile)
    } catch {}
    log.debug.orez('stopped')
  }

  return {
    config,
    stop,
    db,
    instances,
    pgPort: config.pgPort,
    zeroPort: config.zeroPort,
    logStore,
    httpLog,
    zeroEnv,
    restartZero: config.skipZeroCache ? undefined : restartZeroCache,
    // cache-only reset: just replica file (fast, for minor sync issues)
    resetZero: config.skipZeroCache ? undefined : () => resetZeroState('cache-only'),
    // full reset: CVR/CDB + replica (for schema changes, used by pg_restore via SIGUSR1)
    resetZeroFull: config.skipZeroCache ? undefined : () => resetZeroState('full'),
  }
}

function cleanupStaleReplica(config: ZeroLiteConfig): void {
  const replicaPath = resolve(config.dataDir, 'zero-replica.db')
  // delete replica + all lock/wal files so zero-cache does a fresh sync
  // the replica is just a cache of pglite data, safe to recreate
  for (const suffix of ['', '-wal', '-shm', '-wal2']) {
    const file = replicaPath + suffix
    try {
      if (existsSync(file)) {
        unlinkSync(file)
        if (suffix) log.debug.orez(`cleaned up stale ${suffix} file`)
        else log.debug.orez('cleaned up stale replica (will re-sync)')
      }
    } catch {
      // ignore
    }
  }
}

async function seedIfNeeded(db: PGlite, config: ZeroLiteConfig): Promise<void> {
  // check if we already have data
  try {
    const result = await db.query<{ count: string }>(
      'SELECT count(*) as count FROM public."user"'
    )
    if (Number(result.rows[0].count) > 0) {
      return
    }
  } catch {
    // table might not exist yet
  }

  log.debug.orez('seeding demo data')
  const seedFile = resolve(config.seedFile)
  if (!existsSync(seedFile)) {
    log.debug.orez('no seed file found, skipping')
    return
  }

  const sql = readFileSync(seedFile, 'utf-8')
  const statements = sql
    .split('--> statement-breakpoint')
    .map((s) => s.trim())
    .filter(Boolean)

  for (const stmt of statements) {
    await db.exec(stmt)
  }
  log.orez('seeded')
}

// create a fake @rocicorp/zero-sqlite3 package in tmpdir that redirects to
// bedrock-sqlite (wasm). uses NODE_PATH to make node resolve our shim first —
// no require hooks, no Module._resolveFilename monkey-patching, no .cjs files
// in the package (which all break vite).
function writeSqliteShim(): string {
  const tmp = process.env.TMPDIR || process.env.TEMP || '/tmp'
  const dir = resolve(tmp, 'orez-sqlite', 'node_modules', '@rocicorp', 'zero-sqlite3')
  mkdirSync(dir, { recursive: true })

  const bedrockEntry = resolvePackage('bedrock-sqlite')

  writeFileSync(
    resolve(dir, 'package.json'),
    '{"name":"@rocicorp/zero-sqlite3","main":"./index.js"}\n'
  )

  writeFileSync(
    resolve(dir, 'index.js'),
    `'use strict';
var mod = require('${bedrockEntry}');
var OrigDatabase = mod.Database;
var SqliteError = mod.SqliteError;
function Database() {
  var db = new OrigDatabase(...arguments);
  try {
    db.pragma('journal_mode = delete');
    db.pragma('busy_timeout = 30000');
    db.pragma('synchronous = normal');
  } catch(e) {}
  return db;
}
Database.prototype = OrigDatabase.prototype;
Database.prototype.constructor = Database;
Object.keys(OrigDatabase).forEach(function(k) { Database[k] = OrigDatabase[k]; });
Database.prototype.unsafeMode = function() { return this; };
if (!Database.prototype.defaultSafeIntegers) Database.prototype.defaultSafeIntegers = function() { return this; };
if (!Database.prototype.serialize) Database.prototype.serialize = function() { throw new Error('not supported in wasm'); };
if (!Database.prototype.backup) Database.prototype.backup = function() { throw new Error('not supported in wasm'); };
var tmpDb = new OrigDatabase(':memory:');
var tmpStmt = tmpDb.prepare('SELECT 1');
var SP = Object.getPrototypeOf(tmpStmt);
if (!SP.safeIntegers) SP.safeIntegers = function() { return this; };
SP.scanStatus = function() { return undefined; };
SP.scanStatusV2 = function() { return []; };
SP.scanStatusReset = function() {};
tmpDb.close();
Database.SQLITE_SCANSTAT_NLOOP = 0;
Database.SQLITE_SCANSTAT_NVISIT = 1;
Database.SQLITE_SCANSTAT_EST = 2;
Database.SQLITE_SCANSTAT_NAME = 3;
Database.SQLITE_SCANSTAT_EXPLAIN = 4;
Database.SQLITE_SCANSTAT_SELECTID = 5;
Database.SQLITE_SCANSTAT_PARENTID = 6;
Database.SQLITE_SCANSTAT_NCYCLE = 7;
Database.SQLITE_SCANSTAT_COMPLEX = 8;
module.exports = Database;
module.exports.SqliteError = SqliteError;
`
  )

  // return the node_modules root so it can be prepended to NODE_PATH
  return resolve(tmp, 'orez-sqlite', 'node_modules')
}

async function startZeroCache(
  config: ZeroLiteConfig,
  logStore?: LogStore
): Promise<{ process: ChildProcess; env: Record<string, string> }> {
  // resolve @rocicorp/zero entry for finding zero-cache modules
  const zeroEntry = resolvePackage('@rocicorp/zero')

  if (!zeroEntry) {
    throw new Error('zero-cache not found. install @rocicorp/zero')
  }

  if (config.disableWasmSqlite) {
    log.debug.orez('wasm sqlite disabled, using native @rocicorp/zero-sqlite3')
  }

  const upstreamUrl = getConnectionString(config, 'postgres')
  const cvrUrl = getConnectionString(config, 'zero_cvr')
  const cdbUrl = getConnectionString(config, 'zero_cdb')

  // defaults that can be overridden by user env
  // when admin is enabled and user hasn't set ZERO_LOG_LEVEL, capture debug
  // logs for the admin UI while still respecting --log-level for console output
  const zeroLogLevel =
    config.adminPort > 0 && !process.env.ZERO_LOG_LEVEL ? 'debug' : config.logLevel
  const defaults: Record<string, string> = {
    NODE_ENV: 'development',
    ZERO_LOG_LEVEL: zeroLogLevel,
    ZERO_NUM_SYNC_WORKERS: '1',
    // disable query planner — it relies on scanStatus which causes infinite
    // loops with wasm sqlite and has caused freezes with native too.
    // planner is an optimization, not required for correctness.
    ZERO_ENABLE_QUERY_PLANNER: 'false',
  }

  const env: Record<string, string> = {
    ...defaults,
    ...(process.env as Record<string, string>),
    // orez is a development tool — always run zero-cache in development mode
    // to avoid production requirements like --admin-password
    NODE_ENV: 'development',
    ZERO_UPSTREAM_DB: upstreamUrl,
    ZERO_CVR_DB: cvrUrl,
    ZERO_CHANGE_DB: cdbUrl,
    ZERO_REPLICA_FILE: resolve(config.dataDir, 'zero-replica.db'),
    ZERO_PORT: String(config.zeroPort),
  }

  const zeroCacheBin = resolve(zeroEntry, '..', 'cli.js')
  if (!existsSync(zeroCacheBin)) {
    throw new Error('zero-cache cli.js not found. install @rocicorp/zero')
  }

  // wasm sqlite: create a fake @rocicorp/zero-sqlite3 in tmpdir and prepend
  // to NODE_PATH so node resolves our shim first. no require hooks, no
  // Module._resolveFilename monkey-patching (which conflicts with vite).
  if (!config.disableWasmSqlite) {
    const shimNodeModules = writeSqliteShim()
    const existingNodePath = process.env.NODE_PATH || ''
    env.NODE_PATH = existingNodePath
      ? `${shimNodeModules}:${existingNodePath}`
      : shimNodeModules
  }

  const nodeOptions = !config.disableWasmSqlite
    ? `--max-old-space-size=16384 ${process.env.NODE_OPTIONS || ''}`
    : process.env.NODE_OPTIONS || ''
  if (nodeOptions.trim()) env.NODE_OPTIONS = nodeOptions.trim()

  const child = spawn(zeroCacheBin, [], {
    env,
    stdio: ['ignore', 'pipe', 'pipe'],
  })

  // detect log level from zero-cache output
  const detectLevel = (line: string, fallback: string): string => {
    const lower = line.toLowerCase()
    if (
      lower.includes('"level":"error"') ||
      lower.includes(' error ') ||
      lower.includes('error:')
    )
      return 'error'
    if (
      lower.includes('"level":"warn"') ||
      lower.includes(' warn ') ||
      lower.includes('warning:')
    )
      return 'warn'
    if (lower.includes('"level":"debug"') || lower.includes(' debug ')) return 'debug'
    return fallback
  }

  child.stdout?.on('data', (data: Buffer) => {
    const lines = data.toString().trim().split('\n')
    for (const line of lines) {
      log.debug.zero(line)
      logStore?.push('zero', detectLevel(line, 'info'), line)
    }
  })

  child.stderr?.on('data', (data: Buffer) => {
    const lines = data.toString().trim().split('\n')
    for (const line of lines) {
      log.debug.zero(line)
      logStore?.push('zero', detectLevel(line, 'error'), line)
    }
  })

  child.on('exit', (code) => {
    if (code !== 0 && code !== null) {
      log.zero(`exited with code ${code}`)
      logStore?.push('zero', 'error', `exited with code ${code}`)
    }
  })

  return { process: child, env }
}

async function waitForZeroCache(
  config: ZeroLiteConfig,
  timeoutMs = 60000
): Promise<void> {
  const start = Date.now()
  const url = `http://127.0.0.1:${config.zeroPort}/`

  while (Date.now() - start < timeoutMs) {
    try {
      const res = await fetch(url)
      if (res.ok) return
    } catch {
      // not ready yet
    }
    await new Promise((r) => setTimeout(r, 500))
  }

  log.zero('health check timed out, continuing anyway')
}
