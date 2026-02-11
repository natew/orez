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
import { fileURLToPath } from 'node:url'

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

  // write .env.local
  writeEnvLocal(config)

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
    cleanupEnvLocal()
    log.debug.orez('stopped')
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

// resolve path to the --require hook for sqlite.
// the loader tries native @rocicorp/zero-sqlite3 first, falling back to
// bedrock-sqlite (wasm) if native bindings aren't available.
// loaded via NODE_OPTIONS — no files in node_modules are modified.
const __dirname_esm = dirname(fileURLToPath(import.meta.url))
const sqliteLoaderPath = resolve(__dirname_esm, 'sqlite-loader.cjs')

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

  // sqlite loader: tries native first, falls back to wasm. when user forces
  // native via --disable-wasm-sqlite, skip the loader entirely.
  const nodeOptions = !config.disableWasmSqlite
    ? `--require ${sqliteLoaderPath} --max-old-space-size=16384 ${process.env.NODE_OPTIONS || ''}`
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
