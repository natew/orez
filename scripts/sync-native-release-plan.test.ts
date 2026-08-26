import { describe, expect, it } from 'bun:test'
import { execFileSync } from 'node:child_process'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { resolve } from 'node:path'

import {
  decideSyncNativeRelease,
  nextSyncNativeVersion,
  parseNpmMetadata,
  syncNativeContractCheckMode,
  syncNativeRevision,
  syncNativeSourceRevision,
} from './sync-native-release-plan.js'

describe('sync-native release planning', () => {
  it('allocates the next immutable patch without editing source manifests', () => {
    expect(nextSyncNativeVersion('0.1.6', '0.1.6')).toBe('0.1.7')
    expect(nextSyncNativeVersion('0.1.9', '0.1.6')).toBe('0.1.10')
  })

  it('honors an intentionally newer source baseline', () => {
    expect(nextSyncNativeVersion('0.1.9', '0.2.0')).toBe('0.2.0')
  })

  it('extracts the durable contract independently of the package version', () => {
    expect(syncNativeRevision('sync-native 0.1.7 core0.1.6:s2:q4:t3:l2\n')).toBe(
      'core0.1.6:s2:q4:t3:l2'
    )
  })

  it('normalizes npm 11 and npm 12 metadata output', () => {
    const metadata = {
      version: '0.1.6',
      orezSourceCommit: 'abc123',
      orezNativeSourceRevision: 'sha256:def456',
    }
    expect(parseNpmMetadata(JSON.stringify(metadata))).toEqual(metadata)
    expect(parseNpmMetadata(JSON.stringify([metadata]))).toEqual(metadata)
  })

  it('publishes a behavior-only Rust change without changing the durable contract', () => {
    const localSourceRevision = syncNativeSourceRevision()
    expect(
      decideSyncNativeRelease({
        latestVersion: '0.1.8',
        sourceVersion: '0.1.6',
        completeRelease: true,
        localRevision: 'core0.1.6:s3:q5:t3:l2',
        publishedRevision: 'core0.1.6:s3:q5:t3:l2',
        localSourceRevision,
        publishedSourceRevision: 'sha256:stale',
      })
    ).toMatchObject({
      publish: true,
      version: '0.1.9',
      reason: 'npm 0.1.8 was built from native source sha256:stale',
    })
  })

  it('reuses a complete native package built from the exact same source', () => {
    const sourceRevision = syncNativeSourceRevision()
    expect(
      decideSyncNativeRelease({
        latestVersion: '0.1.9',
        sourceVersion: '0.1.6',
        completeRelease: true,
        localRevision: 'core0.1.6:s3:q5:t3:l2',
        publishedRevision: 'core0.1.6:s3:q5:t3:l2',
        localSourceRevision: sourceRevision,
        publishedSourceRevision: sourceRevision,
      })
    ).toMatchObject({
      publish: false,
      version: '0.1.9',
    })
  })

  it('hashes the native Rust inputs deterministically', () => {
    const revision = syncNativeSourceRevision()
    expect(revision).toMatch(/^sha256:[a-f0-9]{64}$/)
    expect(syncNativeSourceRevision()).toBe(revision)

    const differentWorkingDirectory = mkdtempSync(
      resolve(tmpdir(), 'orez-native-source-revision-')
    )
    try {
      const moduleUrl = new URL('./sync-native-release-plan.ts', import.meta.url).href
      const fromAnotherDirectory = execFileSync(
        process.execPath,
        [
          '-e',
          `import { syncNativeSourceRevision } from ${JSON.stringify(moduleUrl)}; console.log(syncNativeSourceRevision())`,
        ],
        { cwd: differentWorkingDirectory, encoding: 'utf8' }
      ).trim()
      expect(fromAnotherDirectory).toBe(revision)
    } finally {
      rmSync(differentWorkingDirectory, { recursive: true })
    }
  })

  it('selects and verifies the native contract for every trusted package release', () => {
    expect(
      syncNativeContractCheckMode({
        dryRun: false,
        packOnly: false,
        rePublish: false,
        trustedPublishing: true,
      })
    ).toBe('select-and-verify')
    expect(
      syncNativeContractCheckMode({
        dryRun: true,
        packOnly: false,
        rePublish: false,
        trustedPublishing: true,
      })
    ).toBe('verify')
    expect(
      syncNativeContractCheckMode({
        dryRun: false,
        packOnly: true,
        rePublish: false,
        trustedPublishing: true,
      })
    ).toBe('skip')
  })

  it('rejects malformed versions and binary output', () => {
    expect(() => nextSyncNativeVersion('0.1.6-beta.1', '0.1.6')).toThrow(
      'major.minor.patch'
    )
    expect(() => syncNativeRevision('sync-native 0.1.7')).toThrow('invalid sync-native')
  })
})
