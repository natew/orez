import {
  readFileSync,
  readdirSync,
  existsSync,
  mkdirSync,
  renameSync,
  unlinkSync,
} from 'node:fs'
import { join, resolve } from 'node:path'

import { PGlite } from '@electric-sql/pglite'
import { btree_gin } from '@electric-sql/pglite/contrib/btree_gin'
import { btree_gist } from '@electric-sql/pglite/contrib/btree_gist'
import { citext } from '@electric-sql/pglite/contrib/citext'
import { cube } from '@electric-sql/pglite/contrib/cube'
import { earthdistance } from '@electric-sql/pglite/contrib/earthdistance'
import { fuzzystrmatch } from '@electric-sql/pglite/contrib/fuzzystrmatch'
import { hstore } from '@electric-sql/pglite/contrib/hstore'
import { ltree } from '@electric-sql/pglite/contrib/ltree'
import { pg_trgm } from '@electric-sql/pglite/contrib/pg_trgm'
import { pgcrypto } from '@electric-sql/pglite/contrib/pgcrypto'
import { uuid_ossp } from '@electric-sql/pglite/contrib/uuid_ossp'
import { vector } from '@electric-sql/pglite/vector'

import { log } from './log.js'
import { PGliteWorkerProxy } from './pglite-ipc.js'

import type { ZeroLiteConfig } from './config.js'

// check if a process is running (works on unix systems)
function isProcessRunning(pid: number): boolean {
  try {
    process.kill(pid, 0) // signal 0 = check existence, don't actually kill
    return true
  } catch {
    return false
  }
}

// clean stale lock files left behind by crashes
// returns true if locks were cleaned
function cleanStaleLocks(dataPath: string): boolean {
  const pidFile = join(dataPath, 'postmaster.pid')
  if (!existsSync(pidFile)) return false

  try {
    const content = readFileSync(pidFile, 'utf-8')
    const pid = parseInt(content.split('\n')[0], 10)

    if (pid && isProcessRunning(pid)) {
      // process is still running, don't touch it
      return false
    }

    // process is gone, clean up stale locks
    const lockFiles = [
      pidFile,
      ...readdirSync(dataPath)
        .filter((f) => f.startsWith('.s.PGSQL.'))
        .map((f) => join(dataPath, f)),
    ]

    for (const file of lockFiles) {
      try {
        unlinkSync(file)
      } catch {}
    }

    return lockFiles.length > 0
  } catch {
    return false
  }
}

export interface PGliteInstances {
  postgres: PGlite
  cvr: PGlite
  cdb: PGlite
  /** read replicas of the postgres instance (empty if disabled) */
  postgresReplicas: PGlite[]
}

// shared setup extracted from the 4 factory functions below

/** migrate old single-instance pgdata dir to the new pgdata-postgres layout */
function migrateDataDir(config: ZeroLiteConfig): void {
  const pgliteDataDir = (config.pgliteOptions as Record<string, any>)?.dataDir
  if (!pgliteDataDir || !String(pgliteDataDir).startsWith('memory://')) {
    const oldDataPath = resolve(config.dataDir, 'pgdata')
    const newDataPath = resolve(config.dataDir, 'pgdata-postgres')
    if (existsSync(oldDataPath) && !existsSync(newDataPath)) {
      renameSync(oldDataPath, newDataPath)
      log.debug.pglite('migrated pgdata → pgdata-postgres')
    }
  }
}

/** create publication if ZERO_APP_PUBLICATIONS is set and publication doesn't exist */
async function ensurePublication(db: {
  exec(sql: string): Promise<any>
  query<T>(sql: string, params?: any[]): Promise<{ rows: T[] }>
}): Promise<void> {
  await db.exec('CREATE EXTENSION IF NOT EXISTS plpgsql')

  const pubName = process.env.ZERO_APP_PUBLICATIONS?.trim()
  if (pubName) {
    const pubs = await db.query<{ count: string }>(
      `SELECT count(*) as count FROM pg_publication WHERE pubname = $1`,
      [pubName]
    )
    if (Number(pubs.rows[0].count) === 0) {
      const quoted = '"' + pubName.replace(/"/g, '""') + '"'
      await db.exec(`CREATE PUBLICATION ${quoted}`)
    }
  }
}

// pglite startParams replaces defaults, so always include required flags
const PGLITE_BASE_FLAGS = [
  '--single',
  '-F',
  '-O',
  '-j',
  '-c',
  'search_path=public',
  '-c',
  'exit_on_error=false',
  '-c',
  'log_checkpoints=false',
]

// cvr/cdb are just zero-cache bookkeeping — minimal fixed memory
const ZERO_START_PARAMS = [
  ...PGLITE_BASE_FLAGS,
  '-c',
  'shared_buffers=128kB',
  '-c',
  'wal_buffers=64kB',
  '-c',
  'work_mem=64kB',
  '-c',
  'maintenance_work_mem=1MB',
  '-c',
  'temp_buffers=800kB',
]

// create a single pglite instance with given dataDir suffix
async function createInstance(
  config: ZeroLiteConfig,
  name: string,
  withExtensions: boolean
): Promise<PGlite> {
  const {
    dataDir: userDataDir,
    debug: _dbg,
    ...userOpts
  } = config.pgliteOptions as Record<string, any>

  const useMemory = typeof userDataDir === 'string' && userDataDir.startsWith('memory://')
  const dataPath = useMemory ? 'memory://' : resolve(config.dataDir, `pgdata-${name}`)

  if (!useMemory) {
    mkdirSync(dataPath, { recursive: true })
    // clean stale locks from previous crashes before trying to open
    if (cleanStaleLocks(dataPath)) {
      log.debug.pglite(`cleaned stale locks in ${name}`)
    }
  }

  log.debug.pglite(`creating ${name} instance at ${dataPath}`)

  const isMain = withExtensions

  try {
    const db = new PGlite({
      dataDir: dataPath,
      debug: config.logLevel === 'debug' ? 1 : 0,
      relaxedDurability: true,
      initialMemory: isMain ? 32 * 1024 * 1024 : 16 * 1024 * 1024,
      ...(isMain ? {} : { startParams: ZERO_START_PARAMS }),
      // main instance: user overrides via pgliteOptions, zero instances: fixed
      ...(isMain
        ? {
            startParams: [
              ...PGLITE_BASE_FLAGS,
              '-c',
              'shared_buffers=4MB',
              '-c',
              'wal_buffers=1MB',
            ],
            ...userOpts,
            extensions: userOpts.extensions || {
              vector,
              pg_trgm,
              pgcrypto,
              uuid_ossp,
              citext,
              hstore,
              ltree,
              fuzzystrmatch,
              btree_gin,
              btree_gist,
              cube,
              earthdistance,
            },
          }
        : { extensions: {} }),
    })

    await db.waitReady

    if (isMain) {
      await db.exec(`
        SET work_mem = '4MB';
        SET maintenance_work_mem = '16MB';
        SET effective_cache_size = '64MB';
        SET random_page_cost = 1.1;
        SET jit = off;
      `)
    } else {
      await db.exec(`SET jit = off;`)
    }

    log.debug.pglite(`${name} ready`)
    return db
  } catch (err) {
    const msg = String(err)
    if (msg.includes('Aborted()') || msg.includes('_pg_initdb')) {
      log.pglite(`failed to start ${name} database`)
      log.pglite(``)
      log.pglite(`the data directory may be corrupted or locked.`)
      log.pglite(`to fix, try one of:`)
      log.pglite(``)
      log.pglite(`  1. remove lock files:`)
      log.pglite(`     rm -f ${dataPath}/postmaster.pid ${dataPath}/.s.PGSQL.*`)
      log.pglite(``)
      log.pglite(`  2. start fresh (loses data):`)
      log.pglite(
        `     rm -rf ${config.dataDir}/pgdata-* ${config.dataDir}/zero-replica.db*`
      )
      log.pglite(``)
    }
    throw err
  }
}

/**
 * create separate pglite instances for each "database".
 *
 * this mirrors real postgresql where postgres, zero_cvr, and zero_cdb are
 * independent databases with separate transaction contexts. each instance
 * has its own session state, so transactions on one database can't be
 * corrupted by queries on another.
 */
export async function createPGliteInstances(
  config: ZeroLiteConfig
): Promise<PGliteInstances> {
  migrateDataDir(config)

  const [postgres, cvr, cdb] = await Promise.all([
    createInstance(config, 'postgres', true),
    createInstance(config, 'cvr', false),
    createInstance(config, 'cdb', false),
  ])

  await ensurePublication(postgres)
  return { postgres, cvr, cdb, postgresReplicas: [] }
}

/**
 * create worker-backed pglite instances.
 *
 * each instance runs in its own worker thread with a separate event loop,
 * so PGlite WASM execution doesn't block the proxy or replication handler.
 * ArrayBuffers are transferred (not copied) for wire protocol data.
 */
export async function createPGliteWorkerInstances(
  config: ZeroLiteConfig
): Promise<PGliteInstances> {
  migrateDataDir(config)

  const pgliteDataDir = (config.pgliteOptions as Record<string, any>)?.dataDir
  const useMemory =
    typeof pgliteDataDir === 'string' && pgliteDataDir.startsWith('memory://')
  const {
    dataDir: _ud,
    debug: _dbg,
    ...userOpts
  } = config.pgliteOptions as Record<string, any>

  function makeWorkerConfig(name: string, withExtensions: boolean) {
    const dataPath = useMemory ? 'memory://' : resolve(config.dataDir, `pgdata-${name}`)
    if (!useMemory) {
      mkdirSync(dataPath, { recursive: true })
      if (cleanStaleLocks(dataPath)) {
        log.debug.pglite(`cleaned stale locks in ${name}`)
      }
    }
    return {
      dataDir: dataPath,
      name,
      withExtensions,
      debug: config.logLevel === 'debug' ? 1 : 0,
      pgliteOptions: userOpts,
    }
  }

  log.pglite('starting worker threads for postgres, cvr, cdb')

  const pgProxy = new PGliteWorkerProxy(makeWorkerConfig('postgres', true))
  const cvrProxy = new PGliteWorkerProxy(makeWorkerConfig('cvr', false))
  const cdbProxy = new PGliteWorkerProxy(makeWorkerConfig('cdb', false))

  await Promise.all([pgProxy.waitReady, cvrProxy.waitReady, cdbProxy.waitReady])
  log.pglite('all worker threads ready')

  await ensurePublication(pgProxy)

  return {
    postgres: pgProxy as unknown as PGlite,
    cvr: cvrProxy as unknown as PGlite,
    cdb: cdbProxy as unknown as PGlite,
    postgresReplicas: [],
  }
}

/**
 * create a single pglite instance shared across all databases.
 *
 * uses one instance for postgres, cvr, and cdb — much lighter than three
 * separate instances. intended for constrained environments like cloudflare
 * workers where running 3 pglite instances is too expensive.
 */
export async function createSinglePGliteInstance(
  config: ZeroLiteConfig
): Promise<PGliteInstances> {
  migrateDataDir(config)
  log.pglite('starting single shared pglite instance')

  const db = await createInstance(config, 'postgres', true)
  await ensurePublication(db)

  // same instance for all three — pg-proxy detects this and shares a mutex
  return { postgres: db, cvr: db, cdb: db, postgresReplicas: [] }
}

/**
 * create a single worker-backed pglite instance shared across all databases.
 */
export async function createSinglePGliteWorkerInstance(
  config: ZeroLiteConfig
): Promise<PGliteInstances> {
  migrateDataDir(config)

  const pgliteDataDir = (config.pgliteOptions as Record<string, any>)?.dataDir
  const useMemory =
    typeof pgliteDataDir === 'string' && pgliteDataDir.startsWith('memory://')
  const {
    dataDir: _ud,
    debug: _dbg,
    ...userOpts
  } = config.pgliteOptions as Record<string, any>

  const dataPath = useMemory ? 'memory://' : resolve(config.dataDir, 'pgdata-postgres')
  if (!useMemory) {
    mkdirSync(dataPath, { recursive: true })
    if (cleanStaleLocks(dataPath)) {
      log.debug.pglite('cleaned stale locks in postgres')
    }
  }

  log.pglite('starting single shared pglite worker thread')

  const proxy = new PGliteWorkerProxy({
    dataDir: dataPath,
    name: 'postgres',
    withExtensions: true,
    debug: config.logLevel === 'debug' ? 1 : 0,
    pgliteOptions: userOpts,
  })

  await proxy.waitReady
  log.pglite('single worker thread ready')

  await ensurePublication(proxy)

  const db = proxy as unknown as PGlite
  return { postgres: db, cvr: db, cdb: db, postgresReplicas: [] }
}

/** create a single worker-backed PGlite instance (for CVR/CDB recreation during reset) */
export function createPGliteWorker(dataDir: string, name: string): PGliteWorkerProxy {
  return new PGliteWorkerProxy({
    dataDir,
    name,
    withExtensions: false,
    debug: 0,
    pgliteOptions: {},
  })
}

/**
 * create read replicas of the postgres instance.
 *
 * dumps the primary's data directory and initializes N new worker threads
 * from the dump. each replica is an independent PGlite instance on its own
 * core, handling read queries concurrently.
 *
 * call this AFTER migrations, seed, on-db-ready — the dump captures the
 * full database state at the time of cloning.
 */
export async function createReadReplicas(
  primary: PGlite,
  count: number,
  config: ZeroLiteConfig
): Promise<PGlite[]> {
  if (count <= 0) return []

  const proxy = primary as unknown as PGliteWorkerProxy
  if (typeof proxy.dumpDataDir !== 'function') {
    log.pglite('read replicas require worker threads (dumpDataDir not available)')
    return []
  }

  log.pglite(`creating ${count} read replica(s)...`)
  const t0 = performance.now()

  const dump = await proxy.dumpDataDir()
  log.debug.pglite(`primary dump: ${(dump.byteLength / 1024 / 1024).toFixed(1)}MB`)

  const {
    dataDir: _ud,
    debug: _dbg,
    ...userOpts
  } = config.pgliteOptions as Record<string, any>

  const replicas: PGliteWorkerProxy[] = []
  for (let i = 0; i < count; i++) {
    const replica = new PGliteWorkerProxy({
      dataDir: 'memory://',
      name: `postgres-replica-${i}`,
      withExtensions: true,
      debug: config.logLevel === 'debug' ? 1 : 0,
      pgliteOptions: userOpts,
      loadDataDir: dump,
    })
    replicas.push(replica)
  }

  await Promise.all(replicas.map((r) => r.waitReady))
  log.pglite(`${count} read replica(s) ready in ${(performance.now() - t0).toFixed(0)}ms`)

  return replicas as unknown as PGlite[]
}

/** run pending migrations, returns count of newly applied migrations */
export async function runMigrations(db: PGlite, config: ZeroLiteConfig): Promise<number> {
  if (!config.migrationsDir) {
    log.debug.orez('no migrations directory configured, skipping')
    return 0
  }

  const migrationsDir = resolve(config.migrationsDir)
  if (!existsSync(migrationsDir)) {
    log.debug.orez('no migrations directory found, skipping')
    return 0
  }

  // create migrations tracking table
  await db.exec(`
    CREATE TABLE IF NOT EXISTS public.migrations (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL UNIQUE,
      applied_at TIMESTAMPTZ DEFAULT NOW()
    )
  `)

  // read drizzle journal for correct migration order
  const journalPath = join(migrationsDir, 'meta', '_journal.json')
  let files: string[]
  if (existsSync(journalPath)) {
    const journal = JSON.parse(readFileSync(journalPath, 'utf-8'))
    files = journal.entries.map((e: { tag: string }) => `${e.tag}.sql`)
  } else {
    files = readdirSync(migrationsDir)
      .filter((f) => f.endsWith('.sql'))
      .sort()
  }

  let applied = 0
  for (const file of files) {
    const name = file.replace(/\.sql$/, '')

    // check if already applied
    const result = await db.query<{ count: string }>(
      'SELECT count(*) as count FROM public.migrations WHERE name = $1',
      [name]
    )
    if (Number(result.rows[0].count) > 0) {
      continue
    }

    log.debug.orez(`applying migration: ${name}`)
    const sql = readFileSync(join(migrationsDir, file), 'utf-8')

    // split by drizzle's statement-breakpoint marker
    const statements = sql
      .split('--> statement-breakpoint')
      .map((s) => s.trim())
      .filter(Boolean)

    for (const stmt of statements) {
      await db.exec(stmt)
    }

    await db.query('INSERT INTO public.migrations (name) VALUES ($1)', [name])
    log.debug.orez(`applied migration: ${name}`)
    applied++
  }

  log.debug.orez('migrations complete')
  return applied
}
