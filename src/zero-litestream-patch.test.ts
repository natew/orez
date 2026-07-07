import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { describe, expect, it } from 'vitest'

import { applyLitestreamRestoreGuard } from './zero-litestream-patch.js'

const ANCHOR = 'async function restoreReplica(lc, config, replicaConstraints) {'

const UNPATCHED = `${ANCHOR}
	for (let i = 0; i < MAX_RETRIES; i++) {
		// ...
	}
}
`

// exactly what the v1 guard left behind in installed copies of @rocicorp/zero
const V1_GUARD = `/* orez: litestream restore disabled (no backup configured) */\n\tif (!config.litestream?.backupURL) return;`
const V1_PATCHED = UNPATCHED.replace(ANCHOR, `${ANCHOR}\n\t${V1_GUARD}`)

function writeTemp(content: string): string {
  const file = join(mkdtempSync(join(tmpdir(), 'orez-ls-patch-')), 'commands.js')
  writeFileSync(file, content)
  return file
}

describe('applyLitestreamRestoreGuard', () => {
  it('injects a guard that surfaces backup-not-found so the purge lock is released', () => {
    const file = writeTemp(UNPATCHED)
    applyLitestreamRestoreGuard(file)
    const patched = readFileSync(file, 'utf-8')
    // the guard must throw for lock-holding callers: a plain return keeps the
    // PurgeLock's xid open and initial sync's CREATE_REPLICATION_SLOT
    // deadlocks on it (native-postgres backend, 55P03 crash loop)
    expect(patched).toContain('throw new BackupNotFoundException')
    expect(patched.indexOf('BackupNotFoundException')).toBeGreaterThan(
      patched.indexOf(ANCHOR)
    )
  })

  it('upgrades the v1 guard (which kept the purge lock held) in place', () => {
    const file = writeTemp(V1_PATCHED)
    applyLitestreamRestoreGuard(file)
    const patched = readFileSync(file, 'utf-8')
    expect(patched).toContain('throw new BackupNotFoundException')
    expect(patched).not.toContain('backupURL) return;')
  })

  it('strips a v1 guard re-injected above an already-upgraded v2 guard', () => {
    const file = writeTemp(UNPATCHED)
    applyLitestreamRestoreGuard(file) // v2
    // simulate a v1-era orez re-injecting its guard above the v2 one
    const v2Content = readFileSync(file, 'utf-8')
    writeFileSync(file, v2Content.replace(ANCHOR, `${ANCHOR}\n\t${V1_GUARD}`))
    applyLitestreamRestoreGuard(file)
    expect(readFileSync(file, 'utf-8')).toBe(v2Content)
  })

  it('is idempotent', () => {
    const file = writeTemp(UNPATCHED)
    applyLitestreamRestoreGuard(file)
    const once = readFileSync(file, 'utf-8')
    applyLitestreamRestoreGuard(file)
    expect(readFileSync(file, 'utf-8')).toBe(once)
  })

  it('fails loudly when the restoreReplica shape changed', () => {
    const file = writeTemp('function somethingElse() {}')
    expect(() => applyLitestreamRestoreGuard(file)).toThrow(/restoreReplica/)
  })
})
