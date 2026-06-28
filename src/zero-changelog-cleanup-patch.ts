/**
 * keep zero-cache's changeLog cleanup alive after no-subscriber windows.
 *
 * zero-cache schedules changeLog cleanup from the local replica/backup watermark
 * and then gates the actual purge on connected subscriber ACKs. That safety rule
 * is correct, but the no-subscriber branch returns before the method's reschedule
 * finally block. In Orez's local/ephemeral mode it is common to start zero-cache
 * before any app client is connected, so the first cleanup can permanently strand
 * the pending watermark set and the CDB changeLog grows until catchup scans get
 * expensive enough to hit PGlite statement timeouts.
 *
 * This patch keeps Zero's ACK gate unchanged. It only moves the no-subscriber
 * return inside the existing try/finally so cleanup retries until a subscriber is
 * present and acknowledged past the pending watermark.
 */

import { existsSync, readFileSync, writeFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'

import { resolvePackage } from './sqlite-mode/package-resolve.js'

const OREZ_MARKER = '/* orez: retry changeLog cleanup when subscribers are absent */'

const ANCHOR = `\t\tconst current = [...this.#forwarder.getAcks()];
\t\tif (current.length === 0) {
\t\t\tthis.#lc.warn?.("No subscribers to confirm cleanup");
\t\t\treturn;
\t\t}
\t\ttry {`

const REPLACEMENT = `\t\tconst current = [...this.#forwarder.getAcks()];
\t\ttry {
\t\t\t${OREZ_MARKER}
\t\t\tif (current.length === 0) {
\t\t\t\tthis.#lc.warn?.("No subscribers to confirm cleanup");
\t\t\t\treturn;
\t\t\t}`

function findChangeStreamerService(): string | null {
  const zeroEntry = resolvePackage('@rocicorp/zero')
  if (!zeroEntry) return null

  let dir = dirname(zeroEntry)
  while (dir && !existsSync(resolve(dir, 'package.json'))) {
    const parent = resolve(dir, '..')
    if (parent === dir) break
    dir = parent
  }

  const servicePath = resolve(
    dir,
    'out',
    'zero-cache',
    'src',
    'services',
    'change-streamer',
    'change-streamer-service.js'
  )
  return existsSync(servicePath) ? servicePath : null
}

export function applyChangeLogCleanupRetryPatch(servicePath: string): void {
  const content = readFileSync(servicePath, 'utf-8')
  if (content.includes(OREZ_MARKER)) return

  if (!content.includes(ANCHOR)) {
    throw new Error(
      `orez: could not patch zero-cache changeLog cleanup retry — no-subscriber cleanup branch not found in ${servicePath}. ` +
        `@rocicorp/zero may have changed; update zero-changelog-cleanup-patch.ts.`
    )
  }

  writeFileSync(servicePath, content.replace(ANCHOR, REPLACEMENT))
}

export function enableZeroChangeLogCleanupRetry(): void {
  const servicePath = findChangeStreamerService()
  if (!servicePath) return
  applyChangeLogCleanupRetryPatch(servicePath)
}
