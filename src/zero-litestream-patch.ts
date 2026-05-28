/**
 * neutralize zero-cache's litestream replica restore for orez.
 *
 * zero 1.5's dedicated change-streamer worker unconditionally calls
 * `restoreReplica()` on startup whenever the change-log already has rows
 * (i.e. on every restart after the first replication). it assumes a litestream
 * backup exists to restore the SQLite replica from. orez has no litestream
 * backup — it owns the replica on disk directly — so the restore throws
 * "Missing --litestream-executable" (or "Missing required value" for the backup
 * URL), which zero logs as an error and "recovers" from by wastefully
 * re-syncing the entire replica from scratch.
 *
 * the correct config gate doesn't exist for the dedicated change-streamer:
 *   - leaving litestream unconfigured -> the error above + resync.
 *   - setting ZERO_LITESTREAM_BACKUP_URL to satisfy the restore -> switches the
 *     replicator into "serving-copy" mode (VACUUM INTO a copy on every start)
 *     and spawns a backup worker + litestream `replicate` subprocess. wrong for
 *     an ephemeral dev backend.
 *
 * so we patch the compiled `restoreReplica` to be a no-op when no litestream
 * backup URL is configured. zero's own dispatcher (`main.js`) already gates all
 * backup/restore work behind `litestream.backupURL`; this brings the dedicated
 * change-streamer in line. a no-op restore leaves orez's existing local replica
 * in place with the purge-lock held — identical to the path where a real
 * restore found a backup matching the current replica — so the change-streamer
 * resumes from the local replica and releases the lock on ownership assumption.
 *
 * this mirrors orez's existing in-place patching of @rocicorp/zero-sqlite3
 * (see sqlite-mode/apply-mode.ts). it is idempotent and only touches orez's
 * own resolved @rocicorp/zero, which is what every orez launch path (node
 * spawn, node in-process, and the browser bundle built from these files) loads.
 */

import { existsSync, readFileSync, writeFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'

import { resolvePackage } from './sqlite-mode/package-resolve.js'

const OREZ_MARKER = '/* orez: litestream restore disabled (no backup configured) */'

// the early-return guard injected at the top of restoreReplica's body. when
// no backup URL is configured there is nothing to restore, so the local
// replica is used as-is and the change-streamer resumes from it.
const GUARD = `${OREZ_MARKER}\n\tif (!config.litestream?.backupURL) return;`

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
 * patch zero-cache so the change-streamer's replica restore is a no-op when
 * no litestream backup is configured. idempotent and safe to call on every
 * startup.
 */
export function disableZeroLitestreamRestore(): void {
  const commandsPath = findLitestreamCommands()
  if (!commandsPath) return

  const content = readFileSync(commandsPath, 'utf-8')
  if (content.includes(OREZ_MARKER)) return // already patched

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
