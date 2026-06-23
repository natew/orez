/**
 * periodically checkpoint zero-cache's native replica WAL.
 *
 * orez disables litestream (see zero-litestream-patch.ts) because it owns the
 * replica on disk with no backup. But in stock zero-cache, litestream is also
 * what reclaims the replica WAL. With it gone, nothing checkpoints the wal2:
 * zero-cache runs no checkpoint loop in non-backup mode, and SQLite's PASSIVE
 * autocheckpoint cannot advance past the view-syncer's continuously-held read
 * snapshots. So the wal2 file grows without bound and every read scans it —
 * slow queries, plus reconnect/re-sync churn on clients syncing over the network.
 *
 * The sqlite shim (shim-template.ts) handles this for WASM mode, but native mode
 * uses the raw @rocicorp/zero-sqlite3 with no shim, so the checkpoint has to be
 * injected into zero-cache itself. The write worker's `init` opens the WRITABLE
 * replica (`db = new Database(lc, dbPath); applyPragmas(db, pragmas)`) — exactly
 * the connection that should checkpoint. We patch in a periodic
 * `wal_checkpoint(TRUNCATE)` right after the pragmas are applied.
 *
 * Validated (native @rocicorp/zero-sqlite3 probe): a held reader blocks all
 * checkpointing, but with cycling readers + periodic TRUNCATE the WAL stays
 * bounded (~44MB) vs growing unbounded (176MB+) without. Interval via
 * OREZ_REPLICA_CHECKPOINT_MS (default 10000ms, 0 disables); timer is unref'd so
 * it never keeps the worker alive.
 *
 * Mirrors orez's existing in-place zero-cache patching (zero-litestream-patch.ts):
 * idempotent, fails loudly if the upstream shape changed.
 */

import { existsSync, readFileSync, writeFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'

import { resolvePackage } from './sqlite-mode/package-resolve.js'

const OREZ_MARKER = '/* orez: periodic replica WAL checkpoint (litestream replacement) */'

// the writer's pragma-application line is the anchor; we inject the periodic
// checkpoint immediately after it, on the same `db` (the writable replica).
const ANCHOR = 'applyPragmas(db, pragmas);'

const INJECT = `${ANCHOR}
${OREZ_MARKER}
try {
  var __orezCpMs = parseInt((typeof process !== 'undefined' && process.env && process.env.OREZ_REPLICA_CHECKPOINT_MS) || '', 10);
  if (!(__orezCpMs > 0)) __orezCpMs = 10000;
  if (!(typeof process !== 'undefined' && process.env && process.env.OREZ_REPLICA_CHECKPOINT_MS === '0')) {
    var __orezCpTimer = setInterval(function () {
      try { db.pragma('wal_checkpoint(TRUNCATE)'); } catch (e) {}
    }, __orezCpMs);
    if (__orezCpTimer && __orezCpTimer.unref) __orezCpTimer.unref();
  }
} catch (e) {}`

/** locate the compiled write-worker.js inside the resolved @rocicorp/zero */
function findWriteWorker(): string | null {
  const zeroEntry = resolvePackage('@rocicorp/zero')
  if (!zeroEntry) return null

  let dir = dirname(zeroEntry)
  while (dir && !existsSync(resolve(dir, 'package.json'))) {
    const parent = resolve(dir, '..')
    if (parent === dir) break
    dir = parent
  }

  const wwPath = resolve(
    dir,
    'out',
    'zero-cache',
    'src',
    'services',
    'replicator',
    'write-worker.js'
  )
  return existsSync(wwPath) ? wwPath : null
}

/** apply the checkpoint injection to a specific write-worker.js. idempotent. */
export function applyCheckpointPatch(writeWorkerPath: string): void {
  const content = readFileSync(writeWorkerPath, 'utf-8')
  if (content.includes(OREZ_MARKER)) return // already patched

  if (!content.includes(ANCHOR)) {
    throw new Error(
      `orez: could not patch zero-cache replica checkpoint — '${ANCHOR}' not found in ${writeWorkerPath}. ` +
        `@rocicorp/zero may have changed; update zero-checkpoint-patch.ts.`
    )
  }

  // replace only the first occurrence (the anchor is unique in write-worker.js)
  const patched = content.replace(ANCHOR, INJECT)
  writeFileSync(writeWorkerPath, patched)
}

/**
 * patch zero-cache's write worker to periodically TRUNCATE-checkpoint the native
 * replica WAL. idempotent and safe to call on every startup.
 */
export function enableZeroReplicaCheckpoint(): void {
  const wwPath = findWriteWorker()
  if (!wwPath) return
  applyCheckpointPatch(wwPath)
}
