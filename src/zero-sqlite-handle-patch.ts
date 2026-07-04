/**
 * register zero-cache's sqlite handles for the embed restart contract (node).
 *
 * zero-cache opens every sqlite connection through zqlite's `Database`
 * wrapper. this patch adds pure tracking to the compiled wrapper: each
 * instance registers itself in `globalThis.__orez_open_sqlite_dbs` on open
 * and deregisters on close. when the registry global is absent (the normal
 * child-process zero-cache, or the write-worker thread's own isolate) the
 * optional-chained calls are no-ops, so behavior is unchanged everywhere.
 *
 * the registry exists so a second embed generation in the same process can
 * reclaim handles the dead generation leaked — see
 * worker/embed-generation.ts for the contract. on CF the sqlite shim
 * registers its own instances instead, so the overlay does not need this
 * patch.
 *
 * same in-place patching pattern as zero-litestream-patch.ts: idempotent,
 * only touches orez's own resolved @rocicorp/zero.
 */

import { existsSync, readFileSync, writeFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'

import { resolvePackage } from './sqlite-mode/package-resolve.js'

const OREZ_MARKER = '__orez_open_sqlite_dbs'

// the constructor's default-import binding for @rocicorp/zero-sqlite3 is chosen
// by zero's bundler and has flipped between `SQLite3Database` and
// `Sqlite3Database` across releases from identical source (rolldown re-picks the
// canonical name for the shared default import whenever a co-bundled module's
// imports change). match either spelling so the anchor survives that reshuffle.
const OPEN_ANCHOR = /this\.#db = new S[qQ][lL]ite3Database\(path, options\);/
const OPEN_HOOK_SUFFIX = `
			globalThis.__orez_open_sqlite_dbs?.add(this); /* orez: embed handle registry */`

const CLOSE_ANCHOR = '\tclose() {'
const CLOSE_HOOK = `\tclose() {
		globalThis.__orez_open_sqlite_dbs?.delete(this); /* orez: embed handle registry */`

/** locate the compiled zqlite db.js inside the resolved @rocicorp/zero */
function findZqliteDb(): string | null {
  const zeroEntry = resolvePackage('@rocicorp/zero')
  if (!zeroEntry) return null

  let dir = dirname(zeroEntry)
  while (dir && !existsSync(resolve(dir, 'package.json'))) {
    const parent = resolve(dir, '..')
    if (parent === dir) break
    dir = parent
  }

  const dbPath = resolve(dir, 'out', 'zqlite', 'src', 'db.js')
  return existsSync(dbPath) ? dbPath : null
}

/**
 * apply the handle-registry hooks to a specific compiled zqlite db.js.
 * idempotent. shared entrypoint so it can be tested against real bundled
 * output without resolving the installed package.
 */
export function applyZqliteHandleRegistry(dbPath: string): void {
  const content = readFileSync(dbPath, 'utf-8')
  if (content.includes(OREZ_MARKER)) return // already patched

  if (!OPEN_ANCHOR.test(content) || !content.includes(CLOSE_ANCHOR)) {
    // upstream shape changed — fail loudly so the embed restart contract
    // never silently loses handle tracking.
    throw new Error(
      `orez: could not patch zqlite Database handle registry — anchors not found in ${dbPath}. ` +
        `@rocicorp/zero may have changed; update zero-sqlite-handle-patch.ts.`
    )
  }

  const patched = content
    .replace(OPEN_ANCHOR, (match) => `${match}${OPEN_HOOK_SUFFIX}`)
    .replace(CLOSE_ANCHOR, CLOSE_HOOK)
  writeFileSync(dbPath, patched)
}

/**
 * patch zero-cache's sqlite Database wrapper to register open handles in
 * `globalThis.__orez_open_sqlite_dbs`. idempotent and safe to call on
 * every startup.
 */
export function installZeroSqliteHandleRegistry(): void {
  const dbPath = findZqliteDb()
  if (!dbPath) return
  applyZqliteHandleRegistry(dbPath)
}
