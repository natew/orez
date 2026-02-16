/**
 * orez: pglite-powered zero-sync development backend.
 *
 * starts a pglite instance, tcp proxy, and zero-cache process.
 * replaces docker-based postgresql and zero-cache with a single
 * `bun run` command.
 */

import { spawn, type ChildProcess } from 'node:child_process'
import { existsSync, mkdirSync, readFileSync, unlinkSync, writeFileSync } from 'node:fs'
import { resolve } from 'node:path'

import {
  createHttpLogStore,
  startHttpProxy,
  type HttpLogStore,
} from './admin/http-proxy.js'
import { createLogStore, type LogStore } from './admin/log-store.js'
import { getConfig, getConnectionString } from './config.js'
import { log, port, setLogLevel, setLogStore } from './log.js'
import { startPgProxy } from './pg-proxy.js'
import { createPGliteInstances, runMigrations } from './pglite-manager.js'
import { findPort } from './port.js'
import {
  cleanCdcStateOnStartup,
  hasCdcCorruptionSignature,
  recoverFromCdcCorruption,
} from './recovery.js'
import { installChangeTracking } from './replication/change-tracker.js'
import {
  formatNativeBootstrapInstructions,
  hasMissingNativeBinarySignature,
  inspectNativeSqliteBinary,
  resolveSqliteMode,
  resolveSqliteModeConfig,
  type SqliteMode,
  type SqliteModeConfig,
} from './sqlite-mode/index.js'

import type { ZeroLiteConfig } from './config.js'
import type { PGlite } from '@electric-sql/pglite'

type ZeroChildProcess = ChildProcess & { __orezTail?: string[] }

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

function getManagedPublicationConfig(): { names: string[]; managedByOrez: boolean } {
  const existing = process.env.ZERO_APP_PUBLICATIONS?.trim()
  if (existing) {
    const names = existing
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
    return { names, managedByOrez: false }
  }

  const appId = (process.env.ZERO_APP_ID || 'zero').trim() || 'zero'
  const fallback = `orez_${appId}_public`
  process.env.ZERO_APP_PUBLICATIONS = fallback
  return { names: [fallback], managedByOrez: true }
}

async function syncManagedPublications(
  db: PGlite,
  names: string[],
  managedByOrez: boolean
): Promise<void> {
  if (!managedByOrez || names.length === 0) return

  const tables = await db.query<{ tablename: string }>(
    `SELECT tablename
     FROM pg_tables
     WHERE schemaname = 'public'
       AND tablename NOT LIKE '_zero_%'`
  )
  const publicTables = tables.rows
    .map((r) => r.tablename)
    .filter((t) => !t.startsWith('_'))

  for (const pub of names) {
    const quotedPub = '"' + pub.replace(/"/g, '""') + '"'
    await db.exec(`CREATE PUBLICATION ${quotedPub}`).catch(() => {})

    if (publicTables.length === 0) continue
    const inPub = await db.query<{ tablename: string }>(
      `SELECT tablename
       FROM pg_publication_tables
       WHERE pubname = $1
         AND schemaname = 'public'`,
      [pub]
    )
    const inPubSet = new Set(inPub.rows.map((r) => r.tablename))
    const toAdd = publicTables.filter((t) => !inPubSet.has(t))
    if (toAdd.length === 0) continue
    const tableList = toAdd.map((t) => `"public"."${t.replace(/"/g, '""')}"`).join(', ')
    await db.exec(`ALTER PUBLICATION ${quotedPub} ADD TABLE ${tableList}`)
    log.debug.orez(`added ${toAdd.length} table(s) to publication "${pub}"`)
  }
}

// resolvePackage moved to sqlite-mode/resolve-mode.ts
import { resolvePackage } from './sqlite-mode/resolve-mode.js'

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

  // resolve sqlite mode config early (used for shim application and cleanup)
  // auto-detects native if available, falls back to wasm
  let sqliteMode = resolveSqliteMode(config.disableWasmSqlite, config.forceWasmSqlite)
  let sqliteModeConfig = resolveSqliteModeConfig(
    config.disableWasmSqlite,
    config.forceWasmSqlite
  )
  if (sqliteMode === 'wasm' && !sqliteModeConfig) {
    log.orez(
      'warning: wasm sqlite requested but dependencies are missing, falling back to native'
    )
    sqliteMode = 'native'
    config.disableWasmSqlite = true
    sqliteModeConfig = resolveSqliteModeConfig(true, false)
  }

  mkdirSync(config.dataDir, { recursive: true })

  // write pid file for IPC (pg_restore uses this to signal restart)
  const pidFile = resolve(config.dataDir, 'orez.pid')
  writeFileSync(pidFile, String(process.pid))

  // write admin port file so pg_restore can find it
  const adminFile = resolve(config.dataDir, 'orez.admin')
  if (adminPort > 0) {
    writeFileSync(adminFile, String(adminPort))
  }

  // start pglite (separate instances for postgres, zero_cvr, zero_cdb)
  const instances = await createPGliteInstances(config)
  const db = instances.postgres
  const managedPub = getManagedPublicationConfig()
  if (managedPub.managedByOrez) {
    log.debug.orez(`using managed publication: ${managedPub.names.join(', ')}`)
  }

  // run migrations (on postgres instance only)
  const migrationsApplied = await runMigrations(db, config)
  await syncManagedPublications(db, managedPub.names, managedPub.managedByOrez)

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
    await syncManagedPublications(db, managedPub.names, managedPub.managedByOrez)
    log.debug.orez('re-installing change tracking after on-db-ready')
    await installChangeTracking(db)
  }

  // clean up stale sqlite replica from previous runs
  cleanupStaleReplica(config)

  // proactively clean CDC state to prevent duplicate key errors on restart.
  // this handles cases where orez was killed (SIGKILL) mid-transaction,
  // leaving stale watermarks in the changeLog table.
  await cleanCdcStateOnStartup(instances.cdb)

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
    const zeroConfig = httpLog ? { ...config, zeroPort: zeroInternalPort } : config

    // helper to start zero-cache and wait for it (including stability check)
    const tryStartZeroCache = async () => {
      const result = await startZeroCache(
        zeroConfig,
        logStore,
        sqliteMode,
        sqliteModeConfig
      )
      zeroCacheProcess = result.process
      zeroEnv = result.env
      await waitForZeroCache(zeroConfig, zeroCacheProcess, 60000, sqliteMode)

      // stability check: wait a bit to catch early crashes (e.g. change-streamer)
      // zero-cache can pass health check but crash shortly after when workers start
      await new Promise((r) => setTimeout(r, 2000))
      if (zeroCacheProcess.exitCode !== null) {
        const tail = (zeroCacheProcess as ZeroChildProcess).__orezTail
        const details = tail?.length ? tail.slice(-20).join('\n') : ''
        throw new Error(`zero-cache crashed during startup stability check\n${details}`)
      }
    }

    try {
      await tryStartZeroCache()
    } catch (err: any) {
      const errMsg = err?.message || String(err)
      // check for CDC corruption (duplicate key in changeLog)
      if (hasCdcCorruptionSignature(errMsg)) {
        await recoverFromCdcCorruption({
          config,
          instances,
          zeroCacheProcess,
        })
        log.orez('retrying zero-cache startup...')
        await tryStartZeroCache()
        log.orez('CDC corruption auto-recovery successful')
      } else if (
        // native sqlite failed to load at runtime - fallback to wasm
        sqliteMode === 'native' &&
        !config.disableWasmSqlite &&
        hasMissingNativeBinarySignature(errMsg)
      ) {
        log.orez('native sqlite failed to load, falling back to wasm...')
        sqliteMode = 'wasm'
        sqliteModeConfig = resolveSqliteModeConfig(false, true) // force wasm
        await tryStartZeroCache()
        log.orez('wasm fallback successful')
      } else {
        // unrecoverable error, rethrow
        throw err
      }
    }

    // start http proxy in front of zero-cache when admin is enabled
    if (httpLog) {
      httpProxyServer = await startHttpProxy({
        listenPort: config.zeroPort,
        targetPort: zeroInternalPort,
        httpLog,
      })
      log.debug.orez(`http proxy listening on ${config.zeroPort}`)
    }

    log.zero(`ready ${port(config.zeroPort, 'magenta')} (sqlite: ${sqliteMode})`)
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
    const result = await startZeroCache(
      zeroConfig,
      logStore,
      sqliteMode,
      sqliteModeConfig
    )
    zeroCacheProcess = result.process
    zeroEnv = result.env
    await waitForZeroCache(zeroConfig, zeroCacheProcess, 60000, sqliteMode)
  }

  // unified reset function for zero state
  // modes:
  //   'cache-only' - deletes replica file only (fast, for minor sync issues)
  //   'full' - deletes CVR/CDB + replica and recreates instances (for schema changes)
  let resetInProgress = false
  const resetFile = resolve(config.dataDir, 'orez.resetting')
  const resetZeroState = async (mode: 'cache-only' | 'full'): Promise<void> => {
    if (resetInProgress) {
      log.orez('reset already in progress, skipping')
      return
    }
    resetInProgress = true
    // write marker file so pg_restore can wait for reset to complete
    writeFileSync(resetFile, String(Date.now()))

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

        // remove stale zero shard schemas from upstream; these can outlive CVR/CDB
        // and cause dispatcher errors after full reset.
        const shardSchemas = await db.query<{ schemaname: string }>(
          `SELECT DISTINCT schemaname
           FROM pg_tables
           WHERE tablename IN ('clients', 'replicas', 'mutations')
             AND schemaname NOT IN (
               'pg_catalog',
               'information_schema',
               'pg_toast',
               'public',
               '_orez'
             )
             AND schemaname NOT LIKE 'pg_%'`
        )
        for (const { schemaname } of shardSchemas.rows) {
          const quoted = '"' + schemaname.replace(/"/g, '""') + '"'
          await db.exec(`DROP SCHEMA IF EXISTS ${quoted} CASCADE`)
        }
        if (shardSchemas.rows.length > 0) {
          log.orez(`dropped ${shardSchemas.rows.length} stale shard schema(s)`)
        }

        // clear upstream replication tracking so zero-cache starts from a
        // clean change stream baseline after full reset.
        await db.exec(`TRUNCATE _orez._zero_changes`).catch(() => {})
        await db.exec(`TRUNCATE _orez._zero_replication_slots`).catch(() => {})
        await db
          .exec(`ALTER SEQUENCE _orez._zero_watermark RESTART WITH 1`)
          .catch(() => {})
        log.orez('cleared upstream replication tracking state')
      }

      // always clean up replica file
      cleanupStaleReplica(config)
      log.orez('replica cleaned up')

      // re-run on-db-ready hook after full reset (re-runs migrations, syncs publication)
      if (mode === 'full' && config.onDbReady) {
        log.orez('re-running on-db-ready...')
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
      }

      // always re-install change tracking after a full reset so public table
      // triggers reflect any schema changes introduced by restore.
      await syncManagedPublications(db, managedPub.names, managedPub.managedByOrez)
      log.debug.orez('re-installing change tracking after full reset')
      await installChangeTracking(db)

      // restart zero-cache
      log.orez('starting zero-cache...')
      // use internal port when http proxy is enabled
      const zeroConfig = httpLog ? { ...config, zeroPort: zeroInternalPort } : config
      const result = await startZeroCache(
        zeroConfig,
        logStore,
        sqliteMode,
        sqliteModeConfig
      )
      zeroCacheProcess = result.process
      zeroEnv = result.env

      await waitForZeroCache(zeroConfig, zeroCacheProcess, 60000, sqliteMode)
      log.orez(`zero state reset complete (${mode})`)
      log.zero(`ready ${port(config.zeroPort, 'magenta')}`)
    } catch (err: any) {
      log.orez(`reset failed: ${err?.message || err}`)
      throw err
    } finally {
      resetInProgress = false
      // remove marker file so pg_restore knows we're done
      try {
        unlinkSync(resetFile)
      } catch {}
    }
  }

  // handle SIGUSR1 to reset zero state (sent by pg_restore after restore completes)
  if (!config.skipZeroCache) {
    process.on('SIGUSR1', () => {
      log.orez('received SIGUSR1 - full reset')
      resetZeroState('full').catch((err) => {
        log.orez(`SIGUSR1 reset failed: ${err?.message || err}`)
      })
    })

    // handle SIGUSR2 to quiesce zero-cache (sent by pg_restore before restore starts)
    process.on('SIGUSR2', () => {
      log.orez('received SIGUSR2 - stopping zero-cache for restore')
      killZeroCache().catch((err) => {
        log.orez(`SIGUSR2 stop failed: ${err?.message || err}`)
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
    try {
      unlinkSync(adminFile)
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
    // stop zero-cache without restart (for pg_restore to safely modify schema)
    stopZero: config.skipZeroCache ? undefined : killZeroCache,
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

// install wasm shim into node_modules/@rocicorp/zero-sqlite3
// backs up original and writes a conditional loader that switches based on env
function installWasmShim(bedrockPath: string, zeroEntry: string): void {
  // find node_modules where @rocicorp/zero is installed
  const nodeModulesMatch = zeroEntry.match(/^(.+\/node_modules)\/@rocicorp\/zero\//)
  if (!nodeModulesMatch) {
    throw new Error(`cannot determine node_modules path from ${zeroEntry}`)
  }
  const nodeModules = nodeModulesMatch[1]
  const shimDir = resolve(nodeModules, '@rocicorp', 'zero-sqlite3')
  const indexPath = resolve(shimDir, 'index.js')
  const originalPath = resolve(shimDir, 'index.original.js')
  const wasmPath = resolve(shimDir, 'index.wasm.js')

  // if directory doesn't exist, create it with just the wasm shim
  if (!existsSync(shimDir)) {
    mkdirSync(shimDir, { recursive: true })
  }

  // backup original if it exists and hasn't been backed up
  if (existsSync(indexPath) && !existsSync(originalPath)) {
    const content = readFileSync(indexPath, 'utf-8')
    // only backup if it's not already our shim
    if (!content.includes('orez-sqlite-shim')) {
      writeFileSync(originalPath, content)
      log.debug.orez('backed up original zero-sqlite3/index.js')
    }
  }

  // write wasm implementation
  writeFileSync(
    wasmPath,
    `// orez wasm sqlite implementation
import bedrockSqlite from '${bedrockPath}';
const { Database: OrigDatabase, SqliteError } = bedrockSqlite;

function Database(...args) {
  const db = new OrigDatabase(...args);
  try {
    db.pragma('journal_mode = wal2');
    db.pragma('busy_timeout = 30000');
    db.pragma('synchronous = normal');
  } catch(e) {}
  return db;
}
Database.prototype = OrigDatabase.prototype;
Database.prototype.constructor = Database;
Object.keys(OrigDatabase).forEach(k => { Database[k] = OrigDatabase[k]; });
Database.prototype.unsafeMode = function() { return this; };
if (!Database.prototype.defaultSafeIntegers) Database.prototype.defaultSafeIntegers = function() { return this; };
if (!Database.prototype.serialize) Database.prototype.serialize = function() { throw new Error('not supported in wasm'); };
if (!Database.prototype.backup) Database.prototype.backup = function() { throw new Error('not supported in wasm'); };

const tmpDb = new OrigDatabase(':memory:');
const tmpStmt = tmpDb.prepare('SELECT 1');
const SP = Object.getPrototypeOf(tmpStmt);
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

export default Database;
export { SqliteError };
`
  )

  // write esm loader - always uses wasm since native requires compiled binary
  // users who want native should use --disable-wasm-sqlite flag which skips this
  writeFileSync(
    indexPath,
    `// orez-sqlite-shim: wasm sqlite for zero-cache
// this file was generated by orez - original backed up to index.original.js
export * from './index.wasm.js';
export { default } from './index.wasm.js';
`
  )

  // ensure package.json exists with correct config
  const pkgPath = resolve(shimDir, 'package.json')
  if (!existsSync(pkgPath)) {
    writeFileSync(
      pkgPath,
      JSON.stringify(
        {
          name: '@rocicorp/zero-sqlite3',
          version: '0.0.0-orez-shim',
          main: './index.js',
        },
        null,
        2
      ) + '\n'
    )
  }

  log.debug.orez(`installed wasm shim to ${shimDir}`)
}

async function startZeroCache(
  config: ZeroLiteConfig,
  logStore?: LogStore,
  sqliteMode: SqliteMode = resolveSqliteMode(config.disableWasmSqlite),
  sqliteModeConfig?: SqliteModeConfig | null
): Promise<{ process: ChildProcess; env: Record<string, string> }> {
  // resolve @rocicorp/zero entry for finding zero-cache modules
  const zeroEntry = resolvePackage('@rocicorp/zero')

  if (!zeroEntry) {
    throw new Error('zero-cache not found. install @rocicorp/zero')
  }

  if (sqliteMode === 'native') {
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

  // wasm mode: install shim into node_modules/@rocicorp/zero-sqlite3
  // backs up original and replaces with wasm wrapper
  if (sqliteMode === 'wasm' && sqliteModeConfig?.bedrockPath) {
    installWasmShim(sqliteModeConfig.bedrockPath, zeroEntry)
  }

  const nodeOptions =
    sqliteMode === 'wasm'
      ? `--max-old-space-size=16384 ${process.env.NODE_OPTIONS || ''}`
      : process.env.NODE_OPTIONS || ''
  if (nodeOptions.trim()) env.NODE_OPTIONS = nodeOptions.trim()

  const child = spawn(zeroCacheBin, [], {
    env,
    stdio: ['ignore', 'pipe', 'pipe'],
  }) as ZeroChildProcess
  child.__orezTail = []

  const pushTail = (line: string) => {
    const tail = child.__orezTail!
    tail.push(line)
    if (tail.length > 80) tail.splice(0, tail.length - 80)
  }

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
      pushTail(`stdout: ${line}`)
      const level = detectLevel(line, 'info')
      if (level === 'warn' || level === 'error') log.zero(line)
      else log.debug.zero(line)
      logStore?.push('zero', level, line)
    }
  })

  child.stderr?.on('data', (data: Buffer) => {
    const lines = data.toString().trim().split('\n')
    for (const line of lines) {
      pushTail(`stderr: ${line}`)
      const level = detectLevel(line, 'error')
      if (level === 'warn' || level === 'error') log.zero(line)
      else log.debug.zero(line)
      logStore?.push('zero', level, line)
    }
  })

  child.on('exit', (code) => {
    if (code !== 0 && code !== null) {
      pushTail(`exit: code ${code}`)
      log.zero(`exited with code ${code}`)
      logStore?.push('zero', 'error', `exited with code ${code}`)
    }
  })

  return { process: child, env }
}

async function waitForZeroCache(
  config: ZeroLiteConfig,
  zeroProcess?: ChildProcess | null,
  timeoutMs = 60000,
  sqliteMode: SqliteMode = resolveSqliteMode(config.disableWasmSqlite)
): Promise<void> {
  const start = Date.now()
  const url = `http://127.0.0.1:${config.zeroPort}/`

  while (Date.now() - start < timeoutMs) {
    if (zeroProcess && zeroProcess.exitCode !== null) {
      const tail = (zeroProcess as ZeroChildProcess).__orezTail
      const details = tail?.length ? `\n${tail.slice(-20).join('\n')}` : ''
      throw new Error(
        `zero-cache exited with code ${zeroProcess.exitCode}${details}${nativeStartupDiagnostics(details, sqliteMode)}`
      )
    }

    try {
      const controller = new AbortController()
      const timer = setTimeout(() => controller.abort(), 1000)
      const res = await fetch(url, { signal: controller.signal })
      clearTimeout(timer)
      // zero may return 404 on "/" while still being healthy.
      if (res.ok || res.status === 404) return
    } catch {
      // not ready yet
    }
    await new Promise((r) => setTimeout(r, 500))
  }

  const tail = (zeroProcess as ZeroChildProcess | null | undefined)?.__orezTail
  const details = tail?.length ? `\n${tail.slice(-20).join('\n')}` : ''
  throw new Error(
    `zero-cache health check timed out after ${timeoutMs}ms${details}${nativeStartupDiagnostics(details, sqliteMode)}`
  )
}

function nativeStartupDiagnostics(details: string, sqliteMode: SqliteMode): string {
  if (sqliteMode !== 'native') return ''
  if (!details) return ''
  if (!hasMissingNativeBinarySignature(details)) return ''

  const check = inspectNativeSqliteBinary()
  const instructions = formatNativeBootstrapInstructions(check)
  return `\n\nnative sqlite startup diagnostics:\n${instructions}`
}
