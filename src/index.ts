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
import { basename, dirname, resolve } from 'node:path'

import { getConfig, getConnectionString } from './config.js'
import { log, setLogLevel } from './log.js'
import { startPgProxy } from './pg-proxy.js'
import { createPGliteInstances, runMigrations } from './pglite-manager.js'
import { findPort } from './port.js'
import { installChangeTracking } from './replication/change-tracker.js'

import type { ZeroLiteConfig } from './config.js'
import type { PGlite } from '@electric-sql/pglite'

export { getConfig, getConnectionString } from './config.js'
export type { LogLevel, ZeroLiteConfig } from './config.js'

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
  if (pgPort !== config.pgPort) log.orez(`port ${config.pgPort} in use, using ${pgPort}`)
  if (!config.skipZeroCache && zeroPort !== config.zeroPort)
    log.orez(`port ${config.zeroPort} in use, using ${zeroPort}`)
  config.pgPort = pgPort
  config.zeroPort = zeroPort

  log.orez('starting...')
  log.debug.orez(`data dir: ${resolve(config.dataDir)}`)

  mkdirSync(config.dataDir, { recursive: true })

  // start pglite (separate instances for postgres, zero_cvr, zero_cdb)
  const instances = await createPGliteInstances(config)
  const db = instances.postgres

  // run migrations (on postgres instance only)
  await runMigrations(db, config)

  // install change tracking (on postgres instance only)
  log.debug.orez('installing change tracking')
  await installChangeTracking(db)

  // start tcp proxy (routes connections to correct instance by database name)
  const pgServer = await startPgProxy(instances, config)

  // seed data if needed
  await seedIfNeeded(db, config)

  // write .env.local
  writeEnvLocal(config)

  // run on-db-ready command (e.g. migrations) before zero-cache starts
  if (config.onDbReady) {
    log.orez(`running on-db-ready: ${config.onDbReady}`)
    const upstreamUrl = getConnectionString(config, 'postgres')
    const cvrUrl = getConnectionString(config, 'zero_cvr')
    const cdbUrl = getConnectionString(config, 'zero_cdb')
    await new Promise<void>((resolve, reject) => {
      const child = spawn(config.onDbReady, {
        shell: true,
        stdio: 'inherit',
        env: {
          ...process.env,
          ZERO_UPSTREAM_DB: upstreamUrl,
          ZERO_CVR_DB: cvrUrl,
          ZERO_CHANGE_DB: cdbUrl,
          DATABASE_URL: upstreamUrl,
          OREZ_PG_PORT: String(config.pgPort),
        },
      })
      child.on('exit', (code) => {
        if (code === 0) {
          log.orez('on-db-ready complete')
          resolve()
        } else {
          reject(new Error(`on-db-ready exited with code ${code}`))
        }
      })
      child.on('error', reject)
    })

    // re-install change tracking on tables created by on-db-ready
    log.debug.orez('re-installing change tracking after on-db-ready')
    await installChangeTracking(db)
  }

  // clean up stale sqlite replica from previous runs
  cleanupStaleReplica(config)

  // start zero-cache
  let zeroCacheProcess: ChildProcess | null = null
  if (!config.skipZeroCache) {
    log.orez('starting zero-cache...')
    zeroCacheProcess = await startZeroCache(config)
    await waitForZeroCache(config)
  } else {
    log.orez('skipping zero-cache (skipZeroCache=true)')
  }

  const stop = async () => {
    log.orez('shutting down...')
    if (zeroCacheProcess && !zeroCacheProcess.killed) {
      zeroCacheProcess.kill('SIGTERM')
      // wait up to 3s for graceful exit, then force kill
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
    pgServer.close()
    await Promise.all([
      instances.postgres.close(),
      instances.cvr.close(),
      instances.cdb.close(),
    ])
    cleanupEnvLocal()
    log.orez('stopped')
  }

  return { config, stop, db, instances, pgPort: config.pgPort, zeroPort: config.zeroPort }
}

// use .env.development.local so it overrides .env.development in vite's load order:
// .env < .env.local < .env.development < .env.development.local
const ENV_LOCAL_PATH = resolve('.env.development.local')
const ENV_LOCAL_MARKER = '# auto-generated by orez'

function writeEnvLocal(config: ZeroLiteConfig): void {
  const upstreamUrl = getConnectionString(config, 'postgres')
  const cvrUrl = getConnectionString(config, 'zero_cvr')
  const cdbUrl = getConnectionString(config, 'zero_cdb')

  const content = `${ENV_LOCAL_MARKER}
VITE_PORT_POSTGRES=${config.pgPort}
VITE_PORT_ZERO=${config.zeroPort}
VITE_PUBLIC_ZERO_SERVER="http://localhost:${config.zeroPort}"
ZERO_UPSTREAM_DB="${upstreamUrl}"
ZERO_CVR_DB="${cvrUrl}"
ZERO_CHANGE_DB="${cdbUrl}"
DATABASE_URL="${upstreamUrl}"
`
  writeFileSync(ENV_LOCAL_PATH, content)
  log.debug.orez('wrote .env.development.local')
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

function cleanupEnvLocal(): void {
  try {
    if (existsSync(ENV_LOCAL_PATH)) {
      const content = readFileSync(ENV_LOCAL_PATH, 'utf-8')
      if (content.startsWith(ENV_LOCAL_MARKER)) {
        unlinkSync(ENV_LOCAL_PATH)
        log.debug.orez('cleaned up .env.local')
      }
    }
  } catch {
    // ignore cleanup errors
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

  log.orez('seeding demo data...')
  const seedFile = resolve(config.seedFile)
  if (!existsSync(seedFile)) {
    log.orez('no seed file found, skipping')
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
  log.orez('seeded demo data')
}

function patchSqliteForWasm(): void {
  // replace @rocicorp/zero-sqlite3 with bedrock-sqlite (wasm) so
  // zero-cache uses wasm instead of native bindings
  try {
    // find the package — import.meta.resolve may fail if native build
    // didn't produce lib/index.js, so fall back to manual lookup
    let libDir: string
    const sqliteEntry = resolvePackage('@rocicorp/zero-sqlite3')
    if (sqliteEntry) {
      libDir = resolve(sqliteEntry, '..')
    } else {
      // native build likely failed, find package via @rocicorp/zero
      const zeroEntry = resolvePackage('@rocicorp/zero')
      // walk up to find the actual node_modules dir (entry depth varies)
      let nodeModules = dirname(zeroEntry)
      while (nodeModules !== '/' && basename(nodeModules) !== 'node_modules') {
        nodeModules = dirname(nodeModules)
      }
      const pkgDir = resolve(nodeModules, '@rocicorp', 'zero-sqlite3')
      if (!existsSync(resolve(pkgDir, 'package.json'))) {
        log.orez('@rocicorp/zero-sqlite3 not found, skipping wasm patch')
        return
      }
      libDir = resolve(pkgDir, 'lib')
      mkdirSync(libDir, { recursive: true })
    }
    const indexPath = resolve(libDir, 'index.js')

    // check if already patched
    if (existsSync(indexPath)) {
      const current = readFileSync(indexPath, 'utf-8')
      if (current.includes('OrigDatabase')) return // already patched
    }

    // back up originals before patching
    if (existsSync(indexPath)) {
      writeFileSync(indexPath + '.original', readFileSync(indexPath))
    }
    const dbBackupPath = resolve(libDir, 'database.js')
    if (existsSync(dbBackupPath)) {
      writeFileSync(dbBackupPath + '.original', readFileSync(dbBackupPath))
    }

    // resolve bedrock-sqlite's dist entry point
    const bedrockEntry = resolvePackage('bedrock-sqlite')
    if (!existsSync(bedrockEntry)) {
      log.orez('bedrock-sqlite not found, skipping wasm patch')
      return
    }

    // inline the full shim into index.js - no external file dependency
    const shim = `'use strict';
// patched by orez: bedrock-sqlite (wasm) replaces native bindings
var mod = require('${bedrockEntry}');
var OrigDatabase = mod.Database;
var SqliteError = mod.SqliteError;

// wrap constructor to set busy_timeout on every connection immediately.
function Database() {
  var db = new OrigDatabase(...arguments);
  try {
    db.pragma('busy_timeout = 30000');
    db.pragma('synchronous = normal');
    // reduce page cache from compile-time 64MB to 8MB.
    // wasm keeps page cache inside V8 heap (unlike native),
    // so a smaller cache prevents OOM during initial sync.
    db.pragma('cache_size = -8000');
  } catch(e) {}
  return db;
}
Database.prototype = OrigDatabase.prototype;
Database.prototype.constructor = Database;
Object.keys(OrigDatabase).forEach(function(k) { Database[k] = OrigDatabase[k]; });

// unsafeMode enables resetting sibling statements before commit/rollback
// (native better-sqlite3 does this in C++, we do it in JS in api.js)
Database.prototype.unsafeMode = function(enabled) {
  if (enabled === undefined) enabled = true;
  this._unsafe = !!enabled;
  return this;
};
if (!Database.prototype.defaultSafeIntegers) Database.prototype.defaultSafeIntegers = function() { return this; };
if (!Database.prototype.serialize) Database.prototype.serialize = function() { throw new Error('not supported in wasm'); };
if (!Database.prototype.backup) Database.prototype.backup = function() { throw new Error('not supported in wasm'); };

// patch Statement prototype
var tmpDb = new OrigDatabase(':memory:');
var tmpStmt = tmpDb.prepare('SELECT 1');
var SP = Object.getPrototypeOf(tmpStmt);
if (!SP.safeIntegers) SP.safeIntegers = function() { return this; };
// unconditionally override scanStatus — bedrock-sqlite may have a broken
// native impl that returns non-undefined garbage, causing infinite loops
// in zero-cache's getScanstatusLoops
SP.scanStatus = function() { return undefined; };
SP.scanStatusV2 = function() { return []; };
SP.scanStatusReset = function() {};
tmpDb.close();

// scanstat constants
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
    writeFileSync(indexPath, shim)

    // also patch database.js since it has top-level native addon loading
    const dbPath = resolve(libDir, 'database.js')
    if (existsSync(dbPath)) {
      writeFileSync(dbPath, `'use strict';\nmodule.exports = require('./index');\n`)
    }

    log.debug.orez('patched @rocicorp/zero-sqlite3 -> bedrock-sqlite (wasm)')
  } catch (e) {
    log.orez(`sqlite wasm patch failed: ${e}`)
  }
}

function unpatchSqliteWasm(): void {
  // restore native @rocicorp/zero-sqlite3 if it was previously patched
  try {
    const sqliteEntry = resolvePackage('@rocicorp/zero-sqlite3')
    if (!sqliteEntry) return
    const libDir = resolve(sqliteEntry, '..')
    const indexPath = resolve(libDir, 'index.js')
    if (!existsSync(indexPath)) return
    const current = readFileSync(indexPath, 'utf-8')
    if (!current.includes('OrigDatabase')) return // not patched

    // restore from backups if available
    for (const file of ['index.js', 'database.js']) {
      const filePath = resolve(libDir, file)
      const backupPath = filePath + '.original'
      if (existsSync(backupPath)) {
        writeFileSync(filePath, readFileSync(backupPath))
      }
    }
    log.orez('restored native @rocicorp/zero-sqlite3 (removed wasm patch)')
  } catch (e) {
    log.orez(`failed to restore native sqlite: ${e}`)
  }
}

async function startZeroCache(config: ZeroLiteConfig): Promise<ChildProcess> {
  // resolve @rocicorp/zero entry for finding zero-cache modules
  const zeroEntry = resolvePackage('@rocicorp/zero')

  if (!zeroEntry) {
    throw new Error('zero-cache binary not found. install @rocicorp/zero')
  }

  // patch sqlite to use wasm before starting zero-cache (unless disabled)
  if (!config.disableWasmSqlite) {
    patchSqliteForWasm()
  } else {
    unpatchSqliteWasm()
    log.debug.orez('wasm sqlite disabled, using native @rocicorp/zero-sqlite3')
  }

  const upstreamUrl = getConnectionString(config, 'postgres')
  const cvrUrl = getConnectionString(config, 'zero_cvr')
  const cdbUrl = getConnectionString(config, 'zero_cdb')

  // defaults that can be overridden by user env
  const defaults: Record<string, string> = {
    NODE_ENV: 'development',
    ZERO_LOG_LEVEL: config.logLevel,
    ZERO_NUM_SYNC_WORKERS: '1',
    // wasm sqlite's scanStatus returns non-undefined garbage that causes
    // infinite loops in getScanstatusLoops. disable the query planner to
    // avoid the code path entirely (planner is an optimization, not required).
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

  // wasm sqlite keeps page cache + WAL buffers inside V8 heap.
  // increase heap limit for spawned zero-cache processes.
  const nodeOptions = !config.disableWasmSqlite
    ? `--max-old-space-size=16384 ${process.env.NODE_OPTIONS || ''}`
    : process.env.NODE_OPTIONS || ''
  if (nodeOptions.trim()) env.NODE_OPTIONS = nodeOptions.trim()

  const child = spawn(zeroCacheBin, [], {
    env,
    stdio: ['ignore', 'pipe', 'pipe'],
  })

  child.stdout?.on('data', (data: Buffer) => {
    const lines = data.toString().trim().split('\n')
    for (const line of lines) {
      log.zero(line)
    }
  })

  child.stderr?.on('data', (data: Buffer) => {
    const lines = data.toString().trim().split('\n')
    for (const line of lines) {
      log.zero(line)
    }
  })

  child.on('exit', (code) => {
    if (code !== 0 && code !== null) {
      log.zero(`exited with code ${code}`)
    }
  })

  return child
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
      if (res.ok) {
        log.zero('ready')
        return
      }
    } catch {
      // not ready yet
    }
    await new Promise((r) => setTimeout(r, 500))
  }

  log.zero('health check timed out, continuing anyway')
}
