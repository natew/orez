/**
 * embed restart contract — generation-begin resource reclamation.
 *
 * zero-cache is designed for process-per-worker lifecycles: workers never
 * close every sqlite handle on drain because process death reclaims them
 * (e.g. the syncer's MutagenService keeps the replica open, DatabaseStorage
 * tmp dbs are never closed, and `runUntilKilled` fires service `stop()`s
 * without awaiting them). an embed runs all workers in one isolate, and on
 * Cloudflare the isolate (module state included) survives an idle-hibernation
 * teardown — so a second embed generation inherits every handle the dead
 * generation leaked, and boot wedges (observed: SQLITE_BUSY on the replica's
 * `journal_mode` switch in node; silent replicator→change-streamer handoff
 * to a dead generation on CF).
 *
 * the contract: starting a new embed generation proves the previous one is
 * dead, so the embed reclaims at START exactly what process death would have
 * reclaimed — no more, no less. every sqlite handle zero-cache opens is
 * registered at the platform's native seam. node uses
 * `globalThis.__orez_open_sqlite_dbs` through the pure-tracking zqlite patch
 * (see zero-sqlite-handle-patch.ts). CF stores handles on the explicit Durable
 * Object runtime. doing this at start (not stop) also covers generations that
 * crashed without running stop(), exactly like a process supervisor restarting
 * a dead worker.
 */

import type { CFInstanceRuntime } from './cf-instance-runtime.js'

interface CloseableDb {
  close(): unknown
}

/** the cross-module registry of open zero-cache sqlite handles. */
export function openSqliteHandleRegistry(): Set<CloseableDb> {
  return ((globalThis as any).__orez_open_sqlite_dbs ??= new Set<CloseableDb>())
}

/**
 * close every sqlite handle a previous embed generation left open.
 * returns the number of handles closed.
 */
export function sweepLeakedSqliteHandles(): number {
  const dbs = openSqliteHandleRegistry()
  let closed = 0
  // close() deregisters itself from the set — iterate over a copy
  for (const db of [...dbs]) {
    try {
      db.close()
    } catch {
      // a handle that errors on close is as dead as process exit leaves it
    }
    closed++
  }
  dbs.clear()
  return closed
}

/** close only the sqlite handles owned by one CF Durable Object runtime. */
export function sweepCFInstanceSqliteHandles(runtime: CFInstanceRuntime): number {
  let closed = 0
  const errors: unknown[] = []
  for (const db of [...runtime.sqliteHandles]) {
    try {
      db.close()
      runtime.sqliteHandles.delete(db)
    } catch (error) {
      errors.push(error)
    }
    closed++
  }
  if (errors.length === 1) throw errors[0]
  if (errors.length > 1) {
    throw new AggregateError(errors, 'zero-cache CF embed: sqlite teardown failed')
  }
  return closed
}
