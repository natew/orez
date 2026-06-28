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
  config: {
    dataDir: string
    useWorkerThreads?: boolean
    ephemeral?: boolean
    ephemeralDir?: string
  }
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
  // zero-cache's ChangeProcessor only throws this when its replica writer is
  // still inside one pgoutput transaction and then receives a second BEGIN.
  // restarting into the same local replica/cvr/cdb state just replays the bad
  // stream; reset the zero state as one consistency domain.
  //
  // `details` is the child's captured log tail, where zero-cache's logger has
  // JSON.stringify'd the Error — so the inner `{"tag":"begin"}` arrives ESCAPED
  // as `\"tag\":\"begin\"`. matching the unescaped token would silently miss in
  // production (the crash log is escaped) while passing a unit test fed the
  // unescaped form — so normalize backslashes before matching the begin token.
  if (
    details.includes('Already in a transaction') &&
    details.replace(/\\/g, '').includes('"tag":"begin"')
  ) {
    return true
  }
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
 * Choose the reset MODE for a recoverable zero-state inconsistency from the crash
 * / log tail.
 *
 *  - 'full' deletes the CVR DB + change DB + replica together. Required when the
 *    change DB itself is corrupt (CDC duplicate-key), because the replica is
 *    rebuilt FROM the change DB — keeping a corrupt CDB would just replay the bad
 *    stream. But it also wipes the CVR, so the recreated CVR starts at the empty
 *    "00" version and EVERY connected client is evicted with ClientNotFound (its
 *    persisted baseCookie is now ahead of the CVR). It is the heavy hammer.
 *  - 'cache-only' deletes ONLY the replica and preserves the CVR/CDB. This fixes
 *    the common replica-vs-CVR desync — RowsVersionBehindError, "max attempts
 *    waiting for CVR", a bad replica change stream, wal2-mode/cantopen — because
 *    the replica re-syncs from upstream to a version >= the preserved CVR. The
 *    CVR survives, so clients reconnect against their existing baseCookie with no
 *    ClientNotFound, and the rebuild also reclaims a bloated replica wal2.
 *
 * The desync class is by far the most common cause of resets in practice (a
 * transient change-streamer timeout crashes zero-cache, and on restart the
 * replica comes back behind the preserved CVR). Sending those through 'cache-only'
 * instead of 'full' is what stops the mass client eviction.
 *
 * `cacheResetExhausted` escalates a replica-only inconsistency to a full reset
 * when a cache-only reset was already tried for it recently and didn't stick — so
 * we never loop on cache-only, but we also never reach for the client-evicting
 * full reset until the gentle path has had its one shot.
 */
export function zeroInconsistencyResetMode(
  details: string,
  opts: { cacheResetExhausted: boolean }
): 'cache-only' | 'full' {
  if (hasCdcCorruptionSignature(details)) return 'full'
  return opts.cacheResetExhausted ? 'full' : 'cache-only'
}

/**
 * detect a transient zero-cache crash that does NOT imply the local zero state
 * (replica / CVR DB / change DB) is inconsistent or corrupt. these are runtime
 * faults — a query timeout, an unhandled rejection in a worker, an OOM kill —
 * where the on-disk state is still valid and a plain process restart recovers.
 *
 * this matters because a `full` reset deletes the CVR DB, which holds the
 * per-client registry and the CVR version every connected client presents as
 * its baseCookie. wiping it mid-session makes the recreated CVR start at the
 * empty "00" version, so every still-connected client's baseCookie is now
 * ahead of the CVR and the view-syncer rejects it with `ClientNotFound`
 * (see checkClientAndCVRVersions). a restart-only recovery keeps the CVR
 * intact, so clients reconnect against their existing baseCookie with no error.
 */
export function hasTransientCrashSignature(details: string): boolean {
  if (!details) return false
  // a corruption / state-drift crash is NOT transient — let the caller take
  // the heavier full-reset path for those.
  if (hasRecoverableZeroStateSignature(details)) return false

  // change-streamer / transaction-pool query timeout against pglite under load.
  if (details.includes('response for statement timed out')) return true
  // generic statement / query timeout phrasing.
  if (details.includes('statement timed out') || details.includes('query timed out')) {
    return true
  }
  // node process pressure that kills the worker without touching the db files.
  if (details.includes('JavaScript heap out of memory')) return true
  if (details.includes('FATAL ERROR') && details.includes('heap')) return true

  return false
}

/**
 * the recovery action the crash watcher should take after zero-cache exits
 * unexpectedly, derived purely from the crash output tail.
 *
 *  - 'full-reset' deletes + recreates the CVR/CDB and replica. only correct when
 *    local zero state is actually corrupt/desynced, because it evicts every
 *    connected client (the recreated CVR starts at "00", so each client's
 *    persisted baseCookie is ahead of it → ClientNotFound on reconnect).
 *  - 'restart' relaunches the zero-cache process against the existing, valid
 *    on-disk state. clients reconnect transparently against their baseCookie.
 *
 * a plain unexpected exit with no recognizable tail defaults to 'restart' — the
 * conservative, non-destructive choice. only an explicit corruption/inconsistency
 * signature escalates to 'full-reset'.
 */
export function classifyZeroCrashRecovery(details: string): {
  action: 'full-reset' | 'restart'
  reason: string
} {
  if (hasCdcCorruptionSignature(details)) {
    return { action: 'full-reset', reason: 'CDC corruption' }
  }
  if (hasZeroStateInconsistencySignature(details)) {
    return { action: 'full-reset', reason: 'state inconsistency' }
  }
  if (hasTransientCrashSignature(details)) {
    return { action: 'restart', reason: 'transient crash' }
  }
  return { action: 'restart', reason: 'unexpected exit' }
}

/**
 * the action orez should take after zero-cache fails its INITIAL startup
 * (the first `startZeroCache` + health/stability wait), derived from the crash
 * tail plus how much recovery has already been attempted this startup.
 *
 * the common case is a transient crash: the change-streamer worker dies mid
 * initial-sync (a dropped proxy connection or a query timeout under load),
 * exits 255, and cascades zero-cache to a graceful "exited with code 0". the
 * on-disk state is NOT corrupt, so re-spawning re-runs the sync and almost
 * always succeeds — the same thing a user does by hand ("it works if I restart
 * it once") and the same call the post-startup crash watcher makes
 * (see {@link classifyZeroCrashRecovery}). this used to be fatal only because
 * the initial start wasn't wired into that restart path.
 *
 *  - 'recover-state' — a genuine corruption/inconsistency signature: rebuild
 *    local zero state, then retry. tried at most once; if it survives the
 *    reset the state is beyond automatic repair.
 *  - 'wasm-fallback' — native sqlite couldn't load: switch to wasm, then retry.
 *  - 'restart' — transient/unexpected crash: plain relaunch, within budget.
 *  - 'cache-reset' — plain restarts exhausted: rebuild the local replica as a
 *    last resort, preserving CVR/CDB so returning clients are not evicted with
 *    ClientNotFound.
 *  - 'give-up' — nothing left to try: surface the original error.
 */
export type ZeroStartupRecoveryAction =
  | 'recover-state'
  | 'wasm-fallback'
  | 'restart'
  | 'cache-reset'
  | 'give-up'

export interface ZeroStartupRetryState {
  /** plain relaunches already attempted (not counting the first start). */
  plainRestarts: number
  /** budget for plain relaunches before rebuilding the local replica. */
  maxRestarts: number
  /** a corruption/inconsistency reset has already been tried this startup. */
  didRecoverState: boolean
  /** the last-resort replica rebuild has already been tried this startup. */
  didCacheReset: boolean
  /** native sqlite is in use and a wasm fallback is permitted. */
  canWasmFallback: boolean
  /** the wasm fallback has already been applied this startup. */
  didWasmFallback: boolean
  /** the crash tail matches a missing-native-binary signature. */
  nativeBinaryMissing: boolean
}

export function classifyZeroStartupRecovery(
  details: string,
  state: ZeroStartupRetryState
): { action: ZeroStartupRecoveryAction; reason: string } {
  // a genuine persisted-state corruption/inconsistency must be reset, not just
  // restarted into the same crash — but only once. if it survives the reset the
  // state is beyond what orez can repair automatically.
  if (hasCdcCorruptionSignature(details) || hasZeroStateInconsistencySignature(details)) {
    return state.didRecoverState
      ? { action: 'give-up', reason: 'state corruption persists after reset' }
      : { action: 'recover-state', reason: 'state corruption' }
  }

  // a missing native sqlite binary is DETERMINISTIC, never transient: either
  // fall back to wasm (once, when permitted) or fail fast. it must NOT enter
  // the restart/cache-reset path below — retrying would just loop into the same
  // missing-binary error, and the user explicitly wants `bun dev` to exit fast
  // when native zero sqlite isn't there rather than churn.
  if (state.nativeBinaryMissing) {
    if (state.canWasmFallback && !state.didWasmFallback) {
      return { action: 'wasm-fallback', reason: 'native sqlite unavailable' }
    }
    return {
      action: 'give-up',
      reason: 'native sqlite unavailable and wasm fallback unavailable',
    }
  }

  // transient / unexpected crash (the common case). plain relaunch within budget.
  if (state.plainRestarts < state.maxRestarts) {
    return { action: 'restart', reason: 'transient startup crash' }
  }

  // budget spent — rebuild the replica as a last resort, but preserve CVR/CDB
  // so persisted clients can reconnect without ClientNotFound.
  if (!state.didCacheReset) {
    return { action: 'cache-reset', reason: 'still crashing after restarts' }
  }

  return { action: 'give-up', reason: 'unrecoverable startup crash' }
}

/**
 * detect replica files that cannot possibly be a valid already-synced Zero
 * replica. zero-cache can create the file itself during initial sync; an empty
 * file here is a leftover local-state artifact, not useful cache state.
 */
function pgliteDataDirFor(
  config: RecoveryContext['config'],
  name: 'cvr' | 'cdb'
): string {
  return config.ephemeral ? 'memory://' : resolve(config.dataDir, `pgdata-${name}`)
}

function zeroReplicaPath(config: RecoveryContext['config']): string {
  return resolve(config.ephemeralDir ?? config.dataDir, 'zero-replica.db')
}

export function getZeroReplicaStartupResetReason(replicaDir: string): string | null {
  const replicaPath = resolve(replicaDir, 'zero-replica.db')
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
  const replicaPath = zeroReplicaPath(config)
  for (const suffix of ['', '-shm', '-wal', '-wal2']) {
    try {
      rmSync(replicaPath + suffix, { force: true })
    } catch {}
  }

  // recreate CVR/CDB instances
  const cvrDataDir = pgliteDataDirFor(config, 'cvr')
  const cdbDataDir = pgliteDataDirFor(config, 'cdb')
  if (config.useWorkerThreads) {
    const cvrProxy = createPGliteWorker(cvrDataDir, 'cvr')
    const cdbProxy = createPGliteWorker(cdbDataDir, 'cdb')
    await Promise.all([cvrProxy.waitReady, cdbProxy.waitReady])
    instances.cvr = cvrProxy as unknown as PGlite
    instances.cdb = cdbProxy as unknown as PGlite
  } else {
    const { PGlite: PGliteCtor } = await import('@electric-sql/pglite')
    if (!config.ephemeral) {
      mkdirSync(cvrDataDir, { recursive: true })
      mkdirSync(cdbDataDir, { recursive: true })
    }
    instances.cvr = new PGliteCtor({
      dataDir: cvrDataDir,
      relaxedDurability: true,
    })
    instances.cdb = new PGliteCtor({
      dataDir: cdbDataDir,
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
