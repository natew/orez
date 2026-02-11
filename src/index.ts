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
import { totalmem } from 'node:os'
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

  // run beforeZero callback (e.g. create tables before zero-cache starts)
  if (config.beforeZero) {
    log.debug.orez('running beforeZero callback')
    await config.beforeZero(db)
    // re-install change tracking on tables created by the callback
    await installChangeTracking(db)
  }

  // clean up stale lock files from previous crash (keep replica for fast restart)
  cleanupStaleLockFiles(config)

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

function cleanupStaleLockFiles(config: ZeroLiteConfig): void {
  const replicaPath = resolve(config.dataDir, 'zero-replica.db')
  // only delete lock/wal files that prevent zero-cache from starting after a crash.
  // keep the replica db itself — zero-cache catches up via replication, which is
  // nearly instant vs a full initial sync (COPY of all tables). if the replica is
  // too stale, ZERO_AUTO_RESET=true makes zero-cache wipe and resync automatically.
  for (const suffix of ['-wal', '-shm', '-wal2']) {
    const file = replicaPath + suffix
    try {
      if (existsSync(file)) {
        unlinkSync(file)
        log.debug.orez(`cleaned up stale ${suffix} file`)
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

// write esm loader hooks to tmpdir that intercept @rocicorp/zero-sqlite3
// and redirect to bedrock-sqlite wasm. templates live in src/shim/.
// returns the path to register.mjs (passed via --import in NODE_OPTIONS).
function writeSqliteShim(): string {
  const tmp = process.env.TMPDIR || process.env.TEMP || '/tmp'
  const dir = resolve(tmp, 'orez-sqlite')
  mkdirSync(dir, { recursive: true })

  const bedrockEntry = resolvePackage('bedrock-sqlite')
  const shimDir = resolve(dirname(fileURLToPath(import.meta.url)), '..', 'src', 'shim')

  const hooksPath = resolve(dir, 'hooks.mjs')
  const hooksTemplate = readFileSync(resolve(shimDir, 'hooks.mjs'), 'utf-8')
  writeFileSync(hooksPath, hooksTemplate.replace(/__BEDROCK_PATH__/g, bedrockEntry))

  const registerPath = resolve(dir, 'register.mjs')
  const registerTemplate = readFileSync(resolve(shimDir, 'register.mjs'), 'utf-8')
  writeFileSync(
    registerPath,
    registerTemplate.replace(/__HOOKS_URL__/g, `file://${hooksPath}`)
  )

  return registerPath
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
    // work around postgres.js bug: concurrent COPY TO STDOUT on a reused
    // connection causes .readable() to hang indefinitely. setting workers
    // high ensures each table gets its own connection (1 COPY per conn).
    // zero-cache already applies this workaround on windows (initial-sync.js).
    ZERO_INITIAL_SYNC_TABLE_COPY_WORKERS: '999',
    // auto-reset on replication errors (e.g. after pg_restore) instead of
    // crashing — zero-cache wipes its replica and resyncs from scratch.
    ZERO_AUTO_RESET: 'true',
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

  // calculate heap size: ~25% of system memory, min 4gb
  const memMB = Math.round(totalmem() / 1024 / 1024)
  const heapMB = Math.max(4096, Math.round(memMB * 0.25))
  const existing = process.env.NODE_OPTIONS || ''

  // wasm sqlite: write shim + ESM loader to tmpdir, pass --import to intercept
  // @rocicorp/zero-sqlite3 resolution with our bedrock-sqlite wasm build
  if (!config.disableWasmSqlite) {
    const registerPath = writeSqliteShim()
    const registerUrl = `file://${registerPath}`
    env.NODE_OPTIONS =
      `--import ${registerUrl} --max-old-space-size=${heapMB} ${existing}`.trim()
  } else {
    env.NODE_OPTIONS = `--max-old-space-size=${heapMB} ${existing}`.trim()
  }

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

  let stderrBuf = ''
  child.stderr?.on('data', (data: Buffer) => {
    const chunk = data.toString()
    stderrBuf += chunk
    const lines = chunk.trim().split('\n')
    for (const line of lines) {
      log.debug.zero(line)
    }
  })

  child.on('exit', (code) => {
    if (code !== 0 && code !== null) {
      if (stderrBuf.includes('Could not locate the bindings file')) {
        log.zero(
          'native @rocicorp/zero-sqlite3 not found — native deps were not compiled.\n' +
            'either:\n' +
            '  • remove --disable-wasm-sqlite to use the built-in wasm sqlite\n' +
            '  • install with native deps: bun install --trust @rocicorp/zero-sqlite3\n' +
            '    or add "trustedDependencies": ["@rocicorp/zero-sqlite3"] to package.json'
        )
      } else {
        const lastLines = stderrBuf.trim().split('\n').slice(-5).join('\n')
        if (lastLines) {
          log.zero(`exited with code ${code}:\n${lastLines}`)
        } else {
          log.zero(`exited with code ${code}`)
        }
      }
    }
  })

  return child
}

async function waitForZeroCache(
  config: ZeroLiteConfig,
  timeoutMs = 120000
): Promise<void> {
  const start = Date.now()
  const url = `http://127.0.0.1:${config.zeroPort}/`

  while (Date.now() - start < timeoutMs) {
    try {
      const res = await fetch(url)
      if (res.ok || res.status === 404) return
    } catch {
      // not ready yet
    }
    await new Promise((r) => setTimeout(r, 42))
  }

  log.zero('health check timed out, continuing anyway')
}
