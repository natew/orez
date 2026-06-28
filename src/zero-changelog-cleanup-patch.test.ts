import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'

import { afterEach, describe, expect, it } from 'vitest'

import { applyChangeLogCleanupRetryPatch } from './zero-changelog-cleanup-patch.js'

let tmpDirs: string[] = []

afterEach(() => {
  for (const dir of tmpDirs) {
    rmSync(dir, { recursive: true, force: true })
  }
  tmpDirs = []
})

describe('applyChangeLogCleanupRetryPatch', () => {
  it('keeps no-subscriber cleanup inside the rescheduling finally block', () => {
    const file = writeService(`
async #purgeOldChanges() {
\t\tconst initial = [...this.#initialWatermarks];
\t\tif (initial.length === 0) {
\t\t\tthis.#lc.warn?.("No initial watermarks to check for cleanup");
\t\t\treturn;
\t\t}
\t\tconst current = [...this.#forwarder.getAcks()];
\t\tif (current.length === 0) {
\t\t\tthis.#lc.warn?.("No subscribers to confirm cleanup");
\t\t\treturn;
\t\t}
\t\ttry {
\t\t\tconst earliestInitial = min(...initial);
\t\t} catch (e) {
\t\t\tthis.#lc.warn?.(\`error purging change log\`, e);
\t\t} finally {
\t\t\tif (this.#initialWatermarks.size) this.#state.setTimeout(() => this.#purgeOldChanges(), CLEANUP_DELAY_MS);
\t\t}
}
`)

    applyChangeLogCleanupRetryPatch(file)

    const patched = readFileSync(file, 'utf-8')
    const tryIndex = patched.indexOf('\t\ttry {')
    const noSubscribersIndex = patched.indexOf('No subscribers to confirm cleanup')
    const finallyIndex = patched.indexOf('finally')

    expect(patched).toContain(
      '/* orez: retry changeLog cleanup when subscribers are absent */'
    )
    expect(tryIndex).toBeGreaterThan(-1)
    expect(noSubscribersIndex).toBeGreaterThan(tryIndex)
    expect(noSubscribersIndex).toBeLessThan(finallyIndex)
  })

  it('is idempotent', () => {
    const file = writeService(`
\t\tconst current = [...this.#forwarder.getAcks()];
\t\tif (current.length === 0) {
\t\t\tthis.#lc.warn?.("No subscribers to confirm cleanup");
\t\t\treturn;
\t\t}
\t\ttry {
`)

    applyChangeLogCleanupRetryPatch(file)
    applyChangeLogCleanupRetryPatch(file)

    const patched = readFileSync(file, 'utf-8')
    expect(
      patched.match(/retry changeLog cleanup when subscribers are absent/g)
    ).toHaveLength(1)
  })

  it('fails loudly when zero-cache changes the cleanup branch shape', () => {
    const file = writeService('async function changedShape() {}')

    expect(() => applyChangeLogCleanupRetryPatch(file)).toThrow(
      'could not patch zero-cache changeLog cleanup retry'
    )
  })
})

function writeService(content: string): string {
  const dir = mkdtempSync(join(tmpdir(), 'orez-changelog-cleanup-patch-'))
  tmpDirs.push(dir)
  const file = resolve(dir, 'change-streamer-service.js')
  writeFileSync(file, content)
  return file
}
