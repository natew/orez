import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { describe, expect, it } from 'vitest'

import {
  getZeroReplicaStartupResetReason,
  hasCdcCorruptionSignature,
  hasRecoverableZeroStateSignature,
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
      hasZeroStateInconsistencySignature(
        'Unable to read watermark from replica: SqliteError: no such table: _zero.replicationState'
      )
    ).toBe(true)
    expect(
      hasZeroStateInconsistencySignature('SqliteError: unable to open database file')
    ).toBe(true)
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
        'Unable to read watermark from replica: SqliteError: no such table: _zero.replicationState'
      )
    ).toBe(true)
    expect(
      hasRecoverableZeroStateSignature(
        'duplicate key value violates unique constraint "changeLog_pkey"'
      )
    ).toBe(true)
    expect(hasRecoverableZeroStateSignature('WebSocket connection closed')).toBe(false)
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
