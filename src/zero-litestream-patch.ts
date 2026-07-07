/**
 * neutralize zero-cache's litestream replica restore for orez.
 *
 * zero's dedicated change-streamer worker unconditionally calls
 * `restoreReplica()` on startup whenever the change-log already has rows.
 * it assumes a litestream backup exists to restore the SQLite replica from.
 * orez has no litestream backup — it owns the replica on disk directly — so
 * without a guard the restore throws "Missing --litestream-executable" (or
 * "Missing required value" for the backup URL) after a retry, which zero
 * logs at error level on every warm restart.
 *
 * the correct config gate doesn't exist for the dedicated change-streamer:
 *   - leaving litestream unconfigured -> the noisy double-attempt error above.
 *   - setting ZERO_LITESTREAM_BACKUP_URL to satisfy the restore -> switches the
 *     replicator into "serving-copy" mode (VACUUM INTO a copy on every start)
 *     and spawns a backup worker + litestream `replicate` subprocess. wrong for
 *     an ephemeral dev backend.
 *
 * so we patch the compiled `restoreReplica` for the no-backup case. the guard
 * must preserve upstream's LOCK-RELEASE semantics, not just silence the error:
 * the change-streamer acquires a changeLog purge lock before restoring — an
 * open `SELECT ... FOR SHARE` transaction, which holds a real xid — and its
 * caller only releases that lock when restoreReplica THROWS. a plain
 * `return` here (the original v1 guard) kept the lock held into
 * `initializePostgresChangeSource`; whenever the local replica was missing
 * (unclean shutdown, cache-only reset), initial sync then ran with the lock
 * still open and `CREATE_REPLICATION_SLOT` waited on our own transaction's
 * xid — a deterministic self-deadlock, crash-looping the change-streamer on
 * `ensureSchemaMigrated`/55P03 lock timeouts until the change db was dropped.
 *
 * throwing `BackupNotFoundException` mirrors what unpatched zero does when
 * litestream config is absent (its `must(backupURL)` throws and the caller
 * releases the lock, logs at warn, and proceeds): a valid local replica is
 * resumed exactly as before, an invalid/missing one initial-syncs lock-free.
 * the purge lock only exists to fence multi-node purge races during a real
 * backup restore; single-node orez has neither.
 *
 * this mirrors orez's existing in-place patching of @rocicorp/zero-sqlite3
 * (see sqlite-mode/apply-mode.ts). it is idempotent and only touches orez's
 * own resolved @rocicorp/zero, which is what every orez launch path (node
 * spawn, node in-process, and the browser bundle built from these files) loads.
 */

import { existsSync, readFileSync, writeFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'

import { resolvePackage } from './sqlite-mode/package-resolve.js'

const OREZ_MARKER = '/* orez: litestream restore disabled (no backup configured) — v2 */'

// the previous guard generation: silenced the error but swallowed the throw
// the caller relies on to release the changeLog purge lock (see header).
// stripped on sight so upgrades replace it instead of stacking guards.
const OREZ_MARKER_V1 = '/* orez: litestream restore disabled (no backup configured) */'
const GUARD_V1 = `${OREZ_MARKER_V1}\n\tif (!config.litestream?.backupURL) return;`

// the guard injected at the top of restoreReplica's body. when no backup URL
// is configured there is nothing to restore; surfacing not-found makes the
// caller release the purge lock before any initial sync (the deadlock fix),
// while callers without a lock (replicaConstraints == null) just proceed.
const GUARD = `${OREZ_MARKER}
	if (!config.litestream?.backupURL) {
		if (replicaConstraints) throw new BackupNotFoundException("(litestream disabled)");
		return;
	}`

/** locate the compiled litestream commands.js inside the resolved @rocicorp/zero */
function findLitestreamCommands(): string | null {
  const zeroEntry = resolvePackage('@rocicorp/zero')
  if (!zeroEntry) return null

  // zeroEntry resolves to .../@rocicorp/zero/out/zero/src/zero.js (the "main").
  // walk up to the package root, then into the known compiled location.
  let dir = dirname(zeroEntry)
  while (dir && !existsSync(resolve(dir, 'package.json'))) {
    const parent = resolve(dir, '..')
    if (parent === dir) break
    dir = parent
  }

  const commandsPath = resolve(
    dir,
    'out',
    'zero-cache',
    'src',
    'services',
    'litestream',
    'commands.js'
  )
  return existsSync(commandsPath) ? commandsPath : null
}

/**
 * apply the restore-guard to a specific compiled litestream commands.js —
 * shared by the in-place node path and the CF overlay (cf-patches.ts).
 * idempotent; upgrades a v1 guard in place.
 */
export function applyLitestreamRestoreGuard(commandsPath: string): void {
  let content = readFileSync(commandsPath, 'utf-8')

  // strip the v1 guard BEFORE the already-patched check: a v1-era orez run
  // against an already-v2-patched file re-injects v1 above the v2 guard
  // (its marker check doesn't recognize v2), and v1's early `return` then
  // shadows the fix. checking the v2 marker first would return early here
  // and leave that stacked v1 in place. observed live on 2026-07-07.
  const hadV1 = content.includes(GUARD_V1)
  if (hadV1) {
    content = content.replace(`\n\t${GUARD_V1}`, '')
  }

  if (content.includes(OREZ_MARKER)) {
    if (hadV1) writeFileSync(commandsPath, content)
    return // already patched
  }

  const anchor = 'async function restoreReplica(lc, config, replicaConstraints) {'
  if (!content.includes(anchor)) {
    // upstream shape changed — fail loudly rather than silently leaving the
    // restore enabled, so the patch can be updated to match.
    throw new Error(
      `orez: could not patch zero-cache litestream restore — restoreReplica signature not found in ${commandsPath}. ` +
        `@rocicorp/zero may have changed; update zero-litestream-patch.ts.`
    )
  }

  const patched = content.replace(anchor, `${anchor}\n\t${GUARD}`)
  writeFileSync(commandsPath, patched)
}

/**
 * patch zero-cache so the change-streamer's replica restore is a no-op when
 * no litestream backup is configured. idempotent and safe to call on every
 * startup.
 */
export function disableZeroLitestreamRestore(): void {
  const commandsPath = findLitestreamCommands()
  if (!commandsPath) return
  applyLitestreamRestoreGuard(commandsPath)
}
