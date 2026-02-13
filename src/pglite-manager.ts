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
import { pg_trgm } from '@electric-sql/pglite/contrib/pg_trgm'
import { vector } from '@electric-sql/pglite/vector'

import { log } from './log.js'

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
}

// create a single pglite instance with given dataDir suffix
async function createInstance(
  config: ZeroLiteConfig,
  name: string,
  withExtensions: boolean
): Promise<PGlite> {
  const dataPath = resolve(config.dataDir, `pgdata-${name}`)
  mkdirSync(dataPath, { recursive: true })

  // clean stale locks from previous crashes before trying to open
  if (cleanStaleLocks(dataPath)) {
    log.debug.pglite(`cleaned stale locks in ${name}`)
  }

  log.debug.pglite(`creating ${name} instance at ${dataPath}`)
  const {
    dataDir: _d,
    debug: _dbg,
    ...userOpts
  } = config.pgliteOptions as Record<string, any>

  try {
    const db = new PGlite({
      dataDir: dataPath,
      debug: config.logLevel === 'debug' ? 1 : 0,
      relaxedDurability: true,
      ...userOpts,
      extensions: withExtensions ? userOpts.extensions || { vector, pg_trgm } : {},
    })

    await db.waitReady
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
  // migrate from old single-instance layout (pgdata → pgdata-postgres)
  const oldDataPath = resolve(config.dataDir, 'pgdata')
  const newDataPath = resolve(config.dataDir, 'pgdata-postgres')
  if (existsSync(oldDataPath) && !existsSync(newDataPath)) {
    renameSync(oldDataPath, newDataPath)
    log.debug.pglite('migrated pgdata → pgdata-postgres')
  }

  // create all 3 instances in parallel (only postgres needs app extensions)
  const [postgres, cvr, cdb] = await Promise.all([
    createInstance(config, 'postgres', true),
    createInstance(config, 'cvr', false),
    createInstance(config, 'cdb', false),
  ])

  // postgres-specific setup
  await postgres.exec('CREATE EXTENSION IF NOT EXISTS plpgsql')

  // create empty publication for zero-cache on postgres instance
  const pubName = process.env.ZERO_APP_PUBLICATIONS || 'zero_pub'
  const pubs = await postgres.query<{ count: string }>(
    `SELECT count(*) as count FROM pg_publication WHERE pubname = $1`,
    [pubName]
  )
  if (Number(pubs.rows[0].count) === 0) {
    const quoted = '"' + pubName.replace(/"/g, '""') + '"'
    await postgres.exec(`CREATE PUBLICATION ${quoted}`)
  }

  return { postgres, cvr, cdb }
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
