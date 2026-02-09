import { readFileSync, readdirSync, existsSync, mkdirSync } from 'node:fs'
import { join, resolve } from 'node:path'

import { PGlite } from '@electric-sql/pglite'
import { pg_trgm } from '@electric-sql/pglite/contrib/pg_trgm'
import { vector } from '@electric-sql/pglite/vector'

import { log } from './log.js'

import type { ZeroLiteConfig } from './config.js'

export async function createPGliteInstance(config: ZeroLiteConfig): Promise<PGlite> {
  const dataPath = resolve(config.dataDir, 'pgdata')
  mkdirSync(dataPath, { recursive: true })

  log.debug.pglite(`creating instance at ${dataPath}`)
  const {
    dataDir: _d,
    debug: _dbg,
    ...userOpts
  } = config.pgliteOptions as Record<string, any>
  const db = new PGlite({
    dataDir: dataPath,
    debug: config.logLevel === 'debug' ? 1 : 0,
    relaxedDurability: true,
    ...userOpts,
    extensions: userOpts.extensions || { vector, pg_trgm },
  })

  await db.waitReady
  log.debug.pglite('ready')

  // ensure plpgsql is available (needed by migrations and trigger functions)
  await db.exec('CREATE EXTENSION IF NOT EXISTS plpgsql')

  // create schemas for multi-db simulation
  await db.exec('CREATE SCHEMA IF NOT EXISTS zero_cvr')
  await db.exec('CREATE SCHEMA IF NOT EXISTS zero_cdb')

  // create empty publication for zero-cache
  // the app's migrations (via --on-db-ready) add specific tables to it,
  // excluding private tables like user/session/account.
  // DO NOT use FOR ALL TABLES - it auto-includes every future table.
  const pubName = process.env.ZERO_APP_PUBLICATIONS || 'zero_pub'
  const pubs = await db.query<{ count: string }>(
    `SELECT count(*) as count FROM pg_publication WHERE pubname = $1`,
    [pubName]
  )
  if (Number(pubs.rows[0].count) === 0) {
    const quoted = '"' + pubName.replace(/"/g, '""') + '"'
    await db.exec(`CREATE PUBLICATION ${quoted}`)
  }

  return db
}

export async function runMigrations(db: PGlite, config: ZeroLiteConfig): Promise<void> {
  if (!config.migrationsDir) {
    log.debug.orez('no migrations directory configured, skipping')
    return
  }

  const migrationsDir = resolve(config.migrationsDir)
  if (!existsSync(migrationsDir)) {
    log.debug.orez('no migrations directory found, skipping')
    return
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
  }

  log.debug.orez('migrations complete')
}
