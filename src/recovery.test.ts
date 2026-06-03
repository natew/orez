import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { describe, expect, it } from 'vitest'

import {
  classifyZeroCrashRecovery,
  getZeroReplicaStartupResetReason,
  hasCdcCorruptionSignature,
  hasRecoverableZeroStateSignature,
  hasTransientCrashSignature,
  hasZeroReplicaMonitorWarmupSignature,
  hasZeroStateInconsistencySignature,
} from './recovery.js'

describe('zero recovery signatures', () => {
  it('detects cdc duplicate watermark corruption', () => {
    expect(
      hasCdcCorruptionSignature(
        'duplicate key value violates unique constraint "changeLog_pkey"'
      )
    ).toBe(true)
    expect(hasCdcCorruptionSignature('23505 duplicate key on watermark')).toBe(true)
    expect(hasCdcCorruptionSignature('connection reset by peer')).toBe(false)
  })

  it('detects zero replica and cvr state drift', () => {
    expect(
      hasZeroStateInconsistencySignature(
        'RowsVersionBehindError: rowsVersion (a1) is behind CVR a2'
      )
    ).toBe(true)
    expect(
      hasZeroStateInconsistencySignature(
        'ClientNotFound: max attempts exceeded waiting for CVR@a2 to catch up'
      )
    ).toBe(true)
    expect(
      hasZeroStateInconsistencySignature(
        'Error: replica db must be in wal2 mode (current: delete)'
      )
    ).toBe(true)
    expect(
      hasZeroStateInconsistencySignature('SqliteError: unable to open database file')
    ).toBe(true)
  })

  it('treats replica monitor missing metadata as warmup noise', () => {
    const details =
      'Unable to read watermark from replica: SqliteError: no such table: _zero.replicationState'

    expect(hasZeroReplicaMonitorWarmupSignature(details)).toBe(true)
    expect(hasZeroStateInconsistencySignature(details)).toBe(false)
  })

  it('does not classify unrelated transient connection output as state drift', () => {
    expect(hasZeroStateInconsistencySignature('WebSocket connection closed')).toBe(false)
    expect(
      hasZeroStateInconsistencySignature(
        'Unable to read watermark from upstream: transient timeout'
      )
    ).toBe(false)
  })

  it('combines recoverable zero state signatures', () => {
    expect(
      hasRecoverableZeroStateSignature(
        'RowsVersionBehindError: rowsVersion (a1) is behind CVR a2'
      )
    ).toBe(true)
    expect(
      hasRecoverableZeroStateSignature(
        'duplicate key value violates unique constraint "changeLog_pkey"'
      )
    ).toBe(true)
    expect(hasRecoverableZeroStateSignature('WebSocket connection closed')).toBe(false)
  })

  it('classifies a change-streamer statement timeout as a transient crash, not state drift', () => {
    // this is the exact tail observed in the wave-28 contamination incident:
    // the change-streamer worker's pglite query timed out and the worker exited.
    // local zero state was valid; a full reset would have wiped the CVR and
    // evicted every connected client with ClientNotFound.
    const details = [
      'pid=16593,worker=change-streamer,workerIndex=0 unhandledRejection',
      '{"name":"Error","errorMsg":"response for statement timed out after 24827 ms",',
      '"stack":"Error: response for statement timed out after 24827 ms\\n',
      '    at abortWith (.../db/transaction-pool.js:239:17)"}',
    ].join('')

    expect(hasTransientCrashSignature(details)).toBe(true)
    // and it must NOT be misread as a state-inconsistency that warrants full reset.
    expect(hasZeroStateInconsistencySignature(details)).toBe(false)
    expect(hasCdcCorruptionSignature(details)).toBe(false)
    expect(hasRecoverableZeroStateSignature(details)).toBe(false)
  })

  it('classifies an out-of-memory worker kill as a transient crash', () => {
    expect(
      hasTransientCrashSignature(
        'FATAL ERROR: Reached heap limit Allocation failed - JavaScript heap out of memory'
      )
    ).toBe(true)
    expect(hasTransientCrashSignature('worker query timed out after 30000 ms')).toBe(true)
  })

  it('does not treat a state-inconsistency crash as transient', () => {
    // these need the heavier full reset; the transient classifier must defer.
    expect(
      hasTransientCrashSignature(
        'RowsVersionBehindError: rowsVersion (a1) is behind CVR a2'
      )
    ).toBe(false)
    expect(
      hasTransientCrashSignature('max attempts exceeded waiting for CVR@a2 to catch up')
    ).toBe(false)
    expect(
      hasTransientCrashSignature(
        'duplicate key value violates unique constraint "changeLog_pkey"'
      )
    ).toBe(false)
  })

  it('does not classify an opaque exit (no recognizable tail) as transient', () => {
    // a plain unexpected exit with no useful tail is neither transient nor
    // state-drift; the crash watcher restarts (CVR-preserving) for both, but
    // the classifier itself must stay conservative.
    expect(hasTransientCrashSignature('')).toBe(false)
    expect(hasTransientCrashSignature('zero-cache exited with code 1')).toBe(false)
  })

  describe('crash recovery policy (the wave-28 contamination fix)', () => {
    // the wave-28 incident: a still-connected browser zero client (baseCookie
    // at version "01") survived while orez did a FULL reset on a transient
    // change-streamer statement timeout. the full reset wiped the CVR back to
    // "00", so on reconnect checkClientAndCVRVersions threw ClientNotFound and
    // the client could not recover without a page reload — a wedge. the fix:
    // a transient crash must take the CVR-preserving 'restart' path.
    const incidentTail = [
      'pid=16593,worker=change-streamer,workerIndex=0 unhandledRejection',
      '{"errorMsg":"response for statement timed out after 24827 ms"}',
    ].join(' ')

    it('restarts (preserves CVR) on the exact incident crash tail', () => {
      const { action, reason } = classifyZeroCrashRecovery(incidentTail)
      expect(action).toBe('restart')
      expect(reason).toBe('transient crash')
    })

    it('restarts (preserves CVR) on an opaque unexpected exit', () => {
      // a crash with no recognizable tail must NOT escalate to a CVR wipe —
      // the conservative, non-destructive default is to restart.
      expect(classifyZeroCrashRecovery('')).toEqual({
        action: 'restart',
        reason: 'unexpected exit',
      })
      expect(classifyZeroCrashRecovery('zero-cache exited with code 1')).toEqual({
        action: 'restart',
        reason: 'unexpected exit',
      })
    })

    it('restarts (preserves CVR) on an out-of-memory worker kill', () => {
      expect(
        classifyZeroCrashRecovery(
          'FATAL ERROR: Reached heap limit Allocation failed - JavaScript heap out of memory'
        ).action
      ).toBe('restart')
    })

    it('still full-resets on genuine CDC corruption', () => {
      expect(
        classifyZeroCrashRecovery(
          'duplicate key value violates unique constraint "changeLog_pkey"'
        )
      ).toEqual({ action: 'full-reset', reason: 'CDC corruption' })
    })

    it('still full-resets on CVR/replica state inconsistency', () => {
      expect(
        classifyZeroCrashRecovery(
          'RowsVersionBehindError: rowsVersion (a1) is behind CVR a2'
        )
      ).toEqual({ action: 'full-reset', reason: 'state inconsistency' })
      expect(
        classifyZeroCrashRecovery('max attempts exceeded waiting for CVR@a2 to catch up')
          .action
      ).toBe('full-reset')
    })
  })

  it('requires startup reset for an empty replica file', () => {
    const dir = mkdtempSync(join(tmpdir(), 'orez-recovery-'))
    try {
      expect(getZeroReplicaStartupResetReason(dir)).toBe(null)
      writeFileSync(join(dir, 'zero-replica.db'), '')
      expect(getZeroReplicaStartupResetReason(dir)).toContain('empty replica file')
      writeFileSync(join(dir, 'zero-replica.db'), 'not empty')
      expect(getZeroReplicaStartupResetReason(dir)).toBe(null)
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  })
})
