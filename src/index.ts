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
import { dirname, resolve } from 'node:path'

import { getConfig, getConnectionString } from './config.js'
import { log, port, setLogLevel } from './log.js'
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
  if (pgPort !== config.pgPort)
    log.debug.orez(`port ${config.pgPort} in use, using ${pgPort}`)
  if (!config.skipZeroCache && zeroPort !== config.zeroPort)
    log.debug.orez(`port ${config.zeroPort} in use, using ${zeroPort}`)
  config.pgPort = pgPort
  config.zeroPort = zeroPort

  log.debug.orez(`data dir: ${resolve(config.dataDir)}`)

  mkdirSync(config.dataDir, { recursive: true })

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

  log.orez(`db up ${port(pgPort, 'green')}`)
  if (migrationsApplied > 0)
    log.orez(
      `${migrationsApplied} migration${migrationsApplied === 1 ? '' : 's'} applied`
    )

  // seed data if needed
  await seedIfNeeded(db, config)

  // run on-db-ready command (e.g. migrations) before zero-cache starts
  if (config.onDbReady) {
    log.debug.orez(`running on-db-ready: ${config.onDbReady}`)
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
          log.orez('on-db-ready done')
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
    zeroCacheProcess = await startZeroCache(config)
    await waitForZeroCache(config)
    log.zero(`ready ${port(config.zeroPort, 'magenta')}`)
  } else {
    log.orez('skip zero-cache')
  }

  const stop = async () => {
    log.debug.orez('shutting down')
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
    log.debug.orez('stopped')
  }

  return { config, stop, db, instances, pgPort: config.pgPort, zeroPort: config.zeroPort }
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

async function startZeroCache(config: ZeroLiteConfig): Promise<ChildProcess> {
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
  const defaults: Record<string, string> = {
    NODE_ENV: 'development',
    ZERO_LOG_LEVEL: config.logLevel,
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

  child.stdout?.on('data', (data: Buffer) => {
    const lines = data.toString().trim().split('\n')
    for (const line of lines) {
      log.debug.zero(line)
    }
  })

  child.stderr?.on('data', (data: Buffer) => {
    const lines = data.toString().trim().split('\n')
    for (const line of lines) {
      log.debug.zero(line)
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
      if (res.ok) return
    } catch {
      // not ready yet
    }
    await new Promise((r) => setTimeout(r, 500))
  }

  log.zero('health check timed out, continuing anyway')
}
