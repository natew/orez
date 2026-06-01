/**
 * recovery helpers for zero state corruption and other startup issues.
 * centralizes error detection and recovery logic to avoid scattering it throughout the codebase.
 */

import { existsSync, mkdirSync, rmSync, statSync } from 'node:fs'
import { resolve } from 'node:path'

import { isChildProcessRunning, terminateChildProcessTree } from './child-process.js'
import { log } from './log.js'
import { createPGliteWorker } from './pglite-manager.js'

import type { PGlite } from '@electric-sql/pglite'
import type { ChildProcess } from 'node:child_process'

export interface RecoveryContext {
  config: { dataDir: string; useWorkerThreads?: boolean }
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
 * zero's replica monitor starts before the serving replica necessarily has a
 * completed initial sync. until that sync creates the replica metadata tables,
 * the monitor can log this while zero-cache is otherwise healthy.
 */
export function hasZeroReplicaMonitorWarmupSignature(details: string): boolean {
  return (
    details.includes('Unable to read watermark from replica') &&
    details.includes('_zero.replicationState')
  )
}

/**
 * detect zero-cache state mismatches between the replica, CVR DB, and change DB.
 * these are not application-level Zero errors; they mean orez's local dev cache
 * state must be rebuilt as one consistency domain.
 */
export function hasZeroStateInconsistencySignature(details: string): boolean {
  if (!details) return false

  if (details.includes('RowsVersionBehindError')) return true
  if (details.includes('max attempts exceeded waiting for CVR')) return true
  if (details.includes('replica db must be in wal2 mode')) return true
  if (
    details.includes('SqliteError: unable to open database file') ||
    details.includes('SQLITE_CANTOPEN')
  ) {
    return true
  }

  return false
}

export function hasRecoverableZeroStateSignature(details: string): boolean {
  return hasCdcCorruptionSignature(details) || hasZeroStateInconsistencySignature(details)
}

/**
 * detect replica files that cannot possibly be a valid already-synced Zero
 * replica. zero-cache can create the file itself during initial sync; an empty
 * file here is a leftover local-state artifact, not useful cache state.
 */
export function getZeroReplicaStartupResetReason(dataDir: string): string | null {
  const replicaPath = resolve(dataDir, 'zero-replica.db')
  if (!existsSync(replicaPath)) return null

  try {
    const stat = statSync(replicaPath)
    if (stat.size === 0) return `empty replica file at ${replicaPath}`
  } catch {
    return null
  }

  return null
}

/**
 * recover from zero-cache state corruption by resetting the CVR DB, change DB,
 * local replica, and upstream zero bookkeeping together.
 */
export async function recoverZeroState(ctx: RecoveryContext): Promise<void> {
  const { config, instances, zeroCacheProcess } = ctx

  log.orez('detected zero-cache state corruption, auto-recovering...')

  // kill the failed zero-cache process
  if (isChildProcessRunning(zeroCacheProcess)) {
    await terminateChildProcessTree(zeroCacheProcess, {
      gracefulSignal: 'SIGKILL',
      forceSignal: 'SIGKILL',
      graceMs: 1000,
      forceGraceMs: 1000,
    })
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
  if (config.useWorkerThreads) {
    const cvrProxy = createPGliteWorker(resolve(config.dataDir, 'pgdata-cvr'), 'cvr')
    const cdbProxy = createPGliteWorker(resolve(config.dataDir, 'pgdata-cdb'), 'cdb')
    await Promise.all([cvrProxy.waitReady, cdbProxy.waitReady])
    instances.cvr = cvrProxy as unknown as PGlite
    instances.cdb = cdbProxy as unknown as PGlite
  } else {
    const { PGlite: PGliteCtor } = await import('@electric-sql/pglite')
    mkdirSync(resolve(config.dataDir, 'pgdata-cvr'), { recursive: true })
    mkdirSync(resolve(config.dataDir, 'pgdata-cdb'), { recursive: true })
    instances.cvr = new PGliteCtor({
      dataDir: resolve(config.dataDir, 'pgdata-cvr'),
      relaxedDurability: true,
    })
    instances.cdb = new PGliteCtor({
      dataDir: resolve(config.dataDir, 'pgdata-cdb'),
      relaxedDurability: true,
    })
    await instances.cvr.waitReady
    await instances.cdb.waitReady
  }
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

  log.orez('zero-cache state recovery complete')
}
