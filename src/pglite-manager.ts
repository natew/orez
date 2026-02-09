import { readFileSync, readdirSync, existsSync, mkdirSync } from 'node:fs'
import { join, resolve } from 'node:path'

import { PGlite } from '@electric-sql/pglite'

import { log } from './log.js'

import type { ZeroLiteConfig } from './config.js'

export async function createPGliteInstance(config: ZeroLiteConfig): Promise<PGlite> {
  const dataPath = resolve(config.dataDir, 'pgdata')
  mkdirSync(dataPath, { recursive: true })

  log.pglite(`creating instance at ${dataPath}`)
  const db = new PGlite(dataPath)

  await db.waitReady
  log.pglite('ready')

  // create schemas for multi-db simulation
  await db.exec('CREATE SCHEMA IF NOT EXISTS zero_cvr')
  await db.exec('CREATE SCHEMA IF NOT EXISTS zero_cdb')

  // create publication for zero-cache
  const pubName = process.env.ZERO_APP_PUBLICATIONS || 'zero_pub'
  const pubs = await db.query<{ count: string }>(
    `SELECT count(*) as count FROM pg_publication WHERE pubname = $1`,
    [pubName]
  )
  if (Number(pubs.rows[0].count) === 0) {
    const quoted = '"' + pubName.replace(/"/g, '""') + '"'
    await db.exec(`CREATE PUBLICATION ${quoted} FOR ALL TABLES`)
  }

  return db
}

export async function runMigrations(db: PGlite, config: ZeroLiteConfig): Promise<void> {
  const migrationsDir = resolve(config.migrationsDir)
  if (!existsSync(migrationsDir)) {
    log.orez('no migrations directory found, skipping')
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

    log.orez(`applying migration: ${name}`)
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
    log.orez(`applied migration: ${name}`)
  }

  log.orez('migrations complete')
}
