/**
 * recovery helpers for zero state corruption and other startup issues.
 * centralizes error detection and recovery logic to avoid scattering it throughout the codebase.
 */

import { mkdirSync, rmSync } from 'node:fs'
import { resolve } from 'node:path'

import { log } from './log.js'

import type { PGlite } from '@electric-sql/pglite'
import type { ChildProcess } from 'node:child_process'

export interface RecoveryContext {
  config: { dataDir: string }
  instances: {
    postgres: PGlite
    cvr: PGlite
    cdb: PGlite
  }
  zeroCacheProcess: ChildProcess | null
}

/**
 * detect CDC corruption errors from zero-cache output.
 * these occur when zero-cache crashes mid-transaction, leaving duplicate
 * watermark entries in the changeLog table.
 */
export function hasCdcCorruptionSignature(details: string): boolean {
  if (!details) return false
  // duplicate key in changeLog table (CDC state corruption)
  if (details.includes('changeLog_pkey') && details.includes('duplicate key')) {
    return true
  }
  // duplicate key with watermark pattern
  if (details.includes('23505') && details.includes('watermark')) {
    return true
  }
  return false
}

/**
 * recover from CDC corruption by resetting CVR/CDB state.
 * this is called when zero-cache fails to start due to duplicate changeLog entries.
 */
export async function recoverFromCdcCorruption(ctx: RecoveryContext): Promise<void> {
  const { config, instances, zeroCacheProcess } = ctx

  log.orez('detected CDC state corruption, auto-recovering...')

  // kill the failed zero-cache process
  if (zeroCacheProcess && !zeroCacheProcess.killed) {
    zeroCacheProcess.kill('SIGKILL')
    await new Promise((r) => setTimeout(r, 500))
  }

  // close and delete CVR/CDB instances
  await instances.cvr.close().catch(() => {})
  await instances.cdb.close().catch(() => {})

  for (const dir of ['pgdata-cvr', 'pgdata-cdb']) {
    try {
      rmSync(resolve(config.dataDir, dir), { recursive: true, force: true })
    } catch {}
  }
  log.orez('deleted corrupted CVR/CDB data')

  // delete replica file
  const replicaPath = resolve(config.dataDir, 'zero-replica.db')
  for (const suffix of ['', '-shm', '-wal', '-wal2']) {
    try {
      rmSync(replicaPath + suffix, { force: true })
    } catch {}
  }

  // recreate CVR/CDB instances
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
  log.orez('recreated CVR/CDB instances')

  // clear upstream replication tracking
  const db = instances.postgres
  await db.exec(`TRUNCATE _orez._zero_changes`).catch(() => {})
  await db.exec(`TRUNCATE _orez._zero_replication_slots`).catch(() => {})
  await db.exec(`ALTER SEQUENCE _orez._zero_watermark RESTART WITH 1`).catch(() => {})

  // drop stale shard schemas
  const shardSchemas = await db.query<{ schemaname: string }>(
    `SELECT DISTINCT schemaname FROM pg_tables
     WHERE tablename IN ('clients', 'replicas', 'mutations')
       AND schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'public', '_orez')
       AND schemaname NOT LIKE 'pg_%'`
  )
  for (const { schemaname } of shardSchemas.rows) {
    await db.exec(`DROP SCHEMA IF EXISTS "${schemaname.replace(/"/g, '""')}" CASCADE`)
  }

  log.orez('CDC corruption recovery complete')
}

/**
 * proactively clean CDC state on startup to prevent duplicate key errors.
 * this handles cases where orez was killed (SIGKILL) mid-transaction,
 * leaving stale watermarks in the changeLog table.
 *
 * in dev mode, it's safe to drop all CDC schemas - zero-cache will recreate them.
 */
export async function cleanCdcStateOnStartup(cdb: PGlite): Promise<void> {
  try {
    // find all CDC schemas (e.g. chat_0/cdc, startchat_0/cdc)
    const result = await cdb.query<{ nspname: string }>(
      `SELECT nspname FROM pg_namespace WHERE nspname LIKE '%/cdc'`
    )

    if (result.rows.length === 0) {
      return // no CDC schemas to clean
    }

    for (const { nspname } of result.rows) {
      const quoted = '"' + nspname.replace(/"/g, '""') + '"'
      await cdb.exec(`DROP SCHEMA IF EXISTS ${quoted} CASCADE`)
    }

    log.debug.orez(`cleaned ${result.rows.length} CDC schema(s) on startup`)
  } catch (err: any) {
    // non-fatal - zero-cache might still work
    log.debug.orez(`CDC cleanup warning: ${err?.message || err}`)
  }
}
