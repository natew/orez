/**
 * orez/worker: embeddable PGlite + change tracking.
 *
 * runs without Node.js dependencies — works in CF Workers, browsers,
 * vitest, bun, deno. provides the PGlite database layer with change
 * tracking and replication encoding that zero-cache needs.
 *
 * usage:
 *   import { createOrezWorker } from 'orez/worker'
 *
 *   const orez = await createOrezWorker({
 *     pgliteOptions: { dataDir: 'memory://' },
 *   })
 *   await orez.exec('CREATE TABLE foo (id TEXT PRIMARY KEY, name TEXT)')
 *   await orez.installChangeTracking()
 *   await orez.query('INSERT INTO foo VALUES ($1, $2)', ['1', 'bar'])
 *   const changes = await orez.getChangesSince(0)
 */

import { Mutex } from '../mutex.js'
import {
  installChangeTracking,
  getChangesSince,
  getCurrentWatermark,
  purgeConsumedChanges,
} from '../replication/change-tracker.js'
import { handleStartReplication } from '../replication/handler.js'

import type { PGlite, Results } from '@electric-sql/pglite'
import type { ChangeRecord } from '../replication/change-tracker.js'
import type { ReplicationWriter } from '../replication/handler.js'
import type { OrezWorkerOptions, OrezWorker } from './types.js'

export type { OrezWorkerOptions, OrezWorker } from './types.js'
export type { ChangeRecord } from '../replication/change-tracker.js'
export type { ReplicationWriter } from '../replication/handler.js'

/**
 * create an orez worker instance.
 *
 * accepts either a pre-created PGlite instance or PGliteOptions to
 * create one. installs the _orez schema and change tracking infrastructure.
 */
export async function createOrezWorker(opts: OrezWorkerOptions): Promise<OrezWorker> {
  let db: PGlite
  let ownsInstance: boolean

  if (opts.pglite) {
    db = opts.pglite
    ownsInstance = false
  } else if (opts.pgliteOptions) {
    // dynamic import so PGlite isn't required at module load time.
    // this lets the worker module be imported in environments where
    // PGlite is provided externally (CF Workers with custom WASM).
    const { PGlite: PGliteCtor } = await import('@electric-sql/pglite')
    db = new PGliteCtor(opts.pgliteOptions)
    await db.waitReady
    ownsInstance = true
  } else {
    throw new Error('orez/worker: provide either pglite or pgliteOptions')
  }

  const mutex = new Mutex()

  // set up publication env if provided (change-tracker reads this)
  if (opts.publications?.length) {
    // change-tracker reads ZERO_APP_PUBLICATIONS to decide which tables to track.
    // in non-Node environments globalThis may not have process.env, so we
    // set it defensively.
    if (typeof globalThis !== 'undefined') {
      ;(globalThis as any).process ??= {}
      ;(globalThis as any).process.env ??= {}
      ;(globalThis as any).process.env.ZERO_APP_PUBLICATIONS = opts.publications.join(',')
    }
  }

  // install core schema (plpgsql, _orez schema)
  await db.exec('CREATE EXTENSION IF NOT EXISTS plpgsql')

  // install change tracking (creates _orez schema, tables, trigger function)
  await installChangeTracking(db)

  const worker: OrezWorker = {
    get db() {
      return db
    },

    get mutex() {
      return mutex
    },

    get ownsInstance() {
      return ownsInstance
    },

    async query<T extends Record<string, unknown> = Record<string, unknown>>(
      sql: string,
      params?: unknown[]
    ): Promise<Results<T>> {
      return db.query<T>(sql, params)
    },

    async exec(sql: string): Promise<void> {
      await db.exec(sql)
    },

    async installChangeTracking(): Promise<void> {
      await installChangeTracking(db)
    },

    async getChangesSince(watermark: number, limit?: number): Promise<ChangeRecord[]> {
      return getChangesSince(db, watermark, limit)
    },

    async getCurrentWatermark(): Promise<number> {
      return getCurrentWatermark(db)
    },

    async purgeChanges(watermark: number): Promise<number> {
      return purgeConsumedChanges(db, watermark)
    },

    async startReplication(writer: ReplicationWriter): Promise<void> {
      await handleStartReplication('START_REPLICATION', writer, db, mutex)
    },

    async close(): Promise<void> {
      if (ownsInstance && !db.closed) {
        await db.close()
      }
    },
  }

  return worker
}
