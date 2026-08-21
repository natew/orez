import { describe, expect, it } from 'bun:test'

import { nextSyncNativeVersion, syncNativeRevision } from './sync-native-release-plan.js'

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

  it('rejects malformed versions and binary output', () => {
    expect(() => nextSyncNativeVersion('0.1.6-beta.1', '0.1.6')).toThrow(
      'major.minor.patch'
    )
    expect(() => syncNativeRevision('sync-native 0.1.7')).toThrow('invalid sync-native')
  })
})
