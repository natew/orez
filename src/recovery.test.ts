import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { describe, expect, it } from 'vitest'

import {
  classifyZeroCrashRecovery,
  classifyZeroStartupRecovery,
  getZeroReplicaStartupResetReason,
  hasCdcCorruptionSignature,
  hasRecoverableZeroStateSignature,
  hasTransientCrashSignature,
  hasZeroReplicaMonitorWarmupSignature,
  hasZeroStateInconsistencySignature,
  zeroInconsistencyResetMode,
  type ZeroStartupRetryState,
} from './recovery.js'

// default startup-retry state: first failure, native mode with wasm allowed,
// nothing tried yet. tests override only the fields they exercise.
function startupState(
  overrides: Partial<ZeroStartupRetryState> = {}
): ZeroStartupRetryState {
  return {
    plainRestarts: 0,
    maxRestarts: 3,
    didRecoverState: false,
    didFullReset: false,
    canWasmFallback: true,
    didWasmFallback: false,
    nativeBinaryMissing: false,
    ...overrides,
  }
}

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
    expect(
      hasZeroStateInconsistencySignature(
        'Message Processing failed: Error: Already in a transaction {"tag":"begin","commitLsn":"001E44E7/FFBA2006","xid":1}'
      )
    ).toBe(true)
    // the REAL captured form: orez's tail holds zero-cache's logger output,
    // where the Error was JSON.stringify'd so the inner object is ESCAPED.
    // this is the exact shape that previously slipped past detection and cost
    // 3×60s of doomed restarts before the give-up reset finally fired.
    expect(
      hasZeroStateInconsistencySignature(
        '[orez:zero] pid=94245,worker=write-worker Message Processing failed: {"name":"Error","errorMsg":"Already in a transaction {\\"tag\\":\\"begin\\",\\"commitLsn\\":\\"001E44E7/FFBA2006\\",\\"xid\\":1}"}'
      )
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
    expect(
      hasZeroStateInconsistencySignature(
        'SQLSTATE 25001: there is already a transaction in progress'
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
      expect(
        classifyZeroCrashRecovery(
          'Message Processing failed: Error: Already in a transaction {"tag":"begin","commitLsn":"001E44E7/FFBA2006","xid":1}'
        )
      ).toEqual({ action: 'full-reset', reason: 'state inconsistency' })
    })
  })

  describe('classifyZeroStartupRecovery (initial-start retry policy)', () => {
    // the reported bug: zero-cache's change-streamer worker dies mid initial
    // sync, exits 255, and cascades the runner to a graceful exit. orez's
    // waitForZeroCache then throws "zero-cache exited with code 0". the on-disk
    // state isn't corrupt — a plain relaunch re-runs the sync — but the initial
    // start used to rethrow this instead of restarting (post-startup crashes
    // were already auto-restarted; the initial start wasn't).
    const userCrashTail = [
      'zero-cache exited with code 0',
      'change-streamer.js (59865) exited with code (255)',
      'all user-facing workers exited',
    ].join('\n')

    it('restarts the change-streamer cold-sync crash that triggered this bug', () => {
      expect(classifyZeroStartupRecovery(userCrashTail, startupState())).toEqual({
        action: 'restart',
        reason: 'transient startup crash',
      })
    })

    it('restarts an opaque startup crash with no recognizable tail', () => {
      expect(classifyZeroStartupRecovery('', startupState())).toEqual({
        action: 'restart',
        reason: 'transient startup crash',
      })
      expect(
        classifyZeroStartupRecovery(
          'zero-cache crashed during startup stability check',
          startupState()
        ).action
      ).toBe('restart')
    })

    it('keeps restarting until the plain-restart budget is spent', () => {
      expect(
        classifyZeroStartupRecovery('boom', startupState({ plainRestarts: 2 })).action
      ).toBe('restart')
      // budget spent → escalate to one full reset
      expect(
        classifyZeroStartupRecovery('boom', startupState({ plainRestarts: 3 }))
      ).toEqual({
        action: 'full-reset',
        reason: 'still crashing after restarts',
      })
      // full reset already tried → give up and surface the error
      expect(
        classifyZeroStartupRecovery(
          'boom',
          startupState({ plainRestarts: 3, didFullReset: true })
        )
      ).toEqual({ action: 'give-up', reason: 'unrecoverable startup crash' })
    })

    it('resets local state once on genuine corruption, then gives up if it persists', () => {
      const corruption = 'duplicate key value violates unique constraint "changeLog_pkey"'
      expect(classifyZeroStartupRecovery(corruption, startupState())).toEqual({
        action: 'recover-state',
        reason: 'state corruption',
      })
      // corruption survived the reset → don't loop on it
      expect(
        classifyZeroStartupRecovery(corruption, startupState({ didRecoverState: true }))
      ).toEqual({ action: 'give-up', reason: 'state corruption persists after reset' })
    })

    it('resets local state on CVR/replica inconsistency (not a plain restart)', () => {
      expect(
        classifyZeroStartupRecovery(
          'RowsVersionBehindError: rowsVersion (a1) is behind CVR a2',
          startupState()
        ).action
      ).toBe('recover-state')
      expect(
        classifyZeroStartupRecovery(
          'Message Processing failed: Error: Already in a transaction {"tag":"begin","commitLsn":"001E44E7/FFBA2006","xid":1}',
          startupState()
        ).action
      ).toBe('recover-state')
    })

    it('falls back to wasm once when native sqlite is missing and wasm is allowed', () => {
      expect(
        classifyZeroStartupRecovery(
          'Could not locate the bindings file',
          startupState({
            nativeBinaryMissing: true,
            canWasmFallback: true,
          })
        )
      ).toEqual({ action: 'wasm-fallback', reason: 'native sqlite unavailable' })
    })

    it('FAILS FAST (does not restart) when native sqlite is missing and wasm is disabled', () => {
      // this is the "exit 1 instantly if zero-native isn't there" behavior:
      // a missing native binary is deterministic, so when wasm fallback is off
      // (interactive `bun dev` sets disableWasmSqlite) it must give up, NOT
      // churn through restarts + a full state reset into the same error.
      const result = classifyZeroStartupRecovery(
        'Could not locate the bindings file',
        startupState({ nativeBinaryMissing: true, canWasmFallback: false })
      )
      expect(result).toEqual({
        action: 'give-up',
        reason: 'native sqlite unavailable and wasm fallback unavailable',
      })
    })

    it('does not loop the wasm fallback once it has already been applied', () => {
      expect(
        classifyZeroStartupRecovery(
          'Could not locate the bindings file',
          startupState({
            nativeBinaryMissing: true,
            canWasmFallback: false,
            didWasmFallback: true,
          })
        ).action
      ).toBe('give-up')
    })

    it('prioritizes corruption over the native and restart paths', () => {
      // a tail that is both corruption AND somehow native-flagged must take the
      // state-reset path, never a plain restart into the same corrupt state.
      expect(
        classifyZeroStartupRecovery(
          'duplicate key value violates unique constraint "changeLog_pkey"',
          startupState({ nativeBinaryMissing: true })
        ).action
      ).toBe('recover-state')
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

  describe('zeroInconsistencyResetMode (cache-only vs full)', () => {
    const rowsBehind = 'RowsVersionBehindError: rowsVersion (a1) is behind CVR a2'
    const cvrCatchup =
      'ProtocolError: max attempts exceeded waiting for CVR@a2 to catch up from a1'
    const cdcCorrupt = 'duplicate key value violates unique constraint "changeLog_pkey"'

    it('rebuilds only the replica (cache-only) for a replica-vs-CVR desync', () => {
      // the common case: gentle path first so connected clients are NOT evicted.
      expect(zeroInconsistencyResetMode(rowsBehind, { cacheResetExhausted: false })).toBe(
        'cache-only'
      )
      expect(zeroInconsistencyResetMode(cvrCatchup, { cacheResetExhausted: false })).toBe(
        'cache-only'
      )
    })

    it('escalates a desync to full once a cache-only reset has been tried', () => {
      // never loop on cache-only: a repeat within the window takes the heavy hammer.
      expect(zeroInconsistencyResetMode(rowsBehind, { cacheResetExhausted: true })).toBe(
        'full'
      )
    })

    it('always full-resets CDC corruption — the change DB itself is bad', () => {
      // the replica is rebuilt FROM the change DB, so a corrupt CDB needs the full
      // reset regardless of how many cache-only attempts came before.
      expect(zeroInconsistencyResetMode(cdcCorrupt, { cacheResetExhausted: false })).toBe(
        'full'
      )
      expect(zeroInconsistencyResetMode(cdcCorrupt, { cacheResetExhausted: true })).toBe(
        'full'
      )
    })
  })
})
