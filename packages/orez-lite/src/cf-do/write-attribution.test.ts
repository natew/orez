import { describe, expect, it } from 'vitest'

import {
  assertNoForbiddenAttributionFields,
  assertWriteAttributionReconciles,
  classifyPhysicalStatement,
  namespaceClassFromObjectName,
  physicalBreakdownTotal,
  WORKERS_LOG_SAMPLING,
  WriteAttributionCollector,
  type WriteAttributionBreakdown,
} from './write-attribution.js'

const META = {
  workerVersion: 'local',
  namespaceClass: 'control' as const,
  processStartedAt: 1_000,
  sampleRate: 1,
  observedAt: 2_000,
}

function sixRowCapturedUpdate(indexes = 0): WriteAttributionCollector {
  const collector = new WriteAttributionCollector()
  collector.recordPhysical('UPDATE file SET body = ? WHERE id = ?', 2 + indexes)
  collector.noteTriggerCaptures(1)
  collector.recordLogicalCapture({
    table: 'file',
    op: 'UPDATE',
    visibility: 'synced',
    publish: true,
  })
  collector.recordPhysical('DELETE FROM "_orez_cdc_buffer" WHERE seq = ?', 1)
  collector.recordPhysical(
    'INSERT INTO _zero_pending_changes (transaction_id, table_name, op) VALUES (?, ?, ?)',
    1
  )
  collector.recordPhysical(
    'INSERT INTO _zero_changes (table_name, op, row_data, old_data) SELECT table_name, op, row_data, old_data FROM _zero_pending_changes WHERE transaction_id = ?',
    1
  )
  collector.recordPhysical(
    'DELETE FROM _zero_pending_changes WHERE transaction_id = ?',
    1
  )
  collector.recordPhysical(
    'INSERT INTO "_orez_tx_schema" (tx_id, owner, type, name, tbl_name, sql) SELECT ?, ?, ?, ?, ?, ?',
    1
  )
  collector.recordPhysical('DELETE FROM "_orez_tx_schema" WHERE tx_id = ?', 1)
  collector.recordPhysical(
    'UPDATE "_zero_change_state" SET last_value = ? WHERE id = 1',
    1
  )
  return collector
}

describe('write attribution classification', () => {
  it('attributes capture and journal tables to internal buckets, not application', () => {
    expect(classifyPhysicalStatement('UPDATE file SET body = ? WHERE id = ?')).toEqual({
      source: 'application',
      table: 'file',
      op: 'UPDATE',
    })
    expect(
      classifyPhysicalStatement('DELETE FROM "_orez_cdc_buffer" WHERE seq = 1')
    ).toEqual({
      source: 'cdc_buffer',
      table: '_orez_cdc_buffer',
      op: 'DELETE',
    })
    expect(
      classifyPhysicalStatement(
        'INSERT INTO _zero_pending_changes (transaction_id) VALUES (?)'
      )
    ).toEqual({
      source: 'pending_changes',
      table: '_zero_pending_changes',
      op: 'INSERT',
    })
    expect(
      classifyPhysicalStatement(
        'INSERT INTO _zero_changes (table_name, op) SELECT table_name, op FROM _zero_pending_changes'
      )
    ).toEqual({
      source: 'zero_changes',
      table: '_zero_changes',
      op: 'INSERT',
    })
    expect(
      classifyPhysicalStatement('DELETE FROM "_orez_tx_schema" WHERE tx_id = ?')
    ).toEqual({
      source: 'bookkeeping',
      table: '_orez_tx_schema',
      op: 'DELETE',
    })
  })

  it('classifies namespace object names without emitting the raw name', () => {
    expect(namespaceClassFromObjectName('singleton')).toBe('control')
    expect(namespaceClassFromObjectName('soot')).toBe('control')
    expect(namespaceClassFromObjectName('ns:proj-abc')).toBe('project')
    expect(namespaceClassFromObjectName('proj-abc')).toBe('project')
    expect(namespaceClassFromObjectName('test-write-attribution')).toBe('test')
    expect(namespaceClassFromObjectName(null)).toBe('test')
  })
})

describe('write attribution reconciliation', () => {
  it('fails an incomplete physical breakdown before any complete fixture is trusted', () => {
    const incomplete: WriteAttributionBreakdown = {
      application: [
        {
          table: 'file',
          op: 'UPDATE',
          visibility: 'synced',
          logicalRows: 1,
          physicalRows: 1,
          indexRows: 0,
        },
      ],
      cdcBuffer: 2,
      pendingChanges: 2,
      zeroChanges: 1,
      bookkeeping: 3,
      unclassified: 0,
    }
    expect(physicalBreakdownTotal(incomplete)).toBe(9)
    expect(() =>
      assertWriteAttributionReconciles({
        physicalTotal: 10,
        logicalTotal: 1,
        complete: true,
        breakdown: incomplete,
      })
    ).toThrow(/physical breakdown 9 does not equal physicalTotal 10/)
  })

  it('fails a misclassified complete event that hides unclassified rows', () => {
    expect(() =>
      assertWriteAttributionReconciles({
        physicalTotal: 4,
        logicalTotal: 0,
        complete: true,
        breakdown: {
          application: [],
          cdcBuffer: 0,
          pendingChanges: 0,
          zeroChanges: 0,
          bookkeeping: 0,
          unclassified: 4,
        },
      })
    ).toThrow(/complete write attribution cannot include unclassified rows/)
  })

  it('reconciles the six-row captured-update collector to physicalTotal', () => {
    const summary = sixRowCapturedUpdate().summarize(META)
    assertWriteAttributionReconciles(summary)
    assertNoForbiddenAttributionFields(summary as unknown as Record<string, unknown>)
    expect(summary.physicalTotal).toBe(9)
    expect(summary.logicalTotal).toBe(1)
    expect(summary.rustVisibleRows).toBe(1)
    expect(summary.complete).toBe(true)
    expect(summary.logSampling).toBe(WORKERS_LOG_SAMPLING)
    expect(summary.breakdown).toEqual({
      application: [
        {
          table: 'file',
          op: 'UPDATE',
          visibility: 'synced',
          logicalRows: 1,
          physicalRows: 1,
          indexRows: 0,
        },
      ],
      cdcBuffer: 2,
      pendingChanges: 2,
      zeroChanges: 1,
      bookkeeping: 3,
      unclassified: 0,
    })
  })

  it('counts measurable index rows on the application statement after trigger capture', () => {
    const summary = sixRowCapturedUpdate(2).summarize(META)
    assertWriteAttributionReconciles(summary)
    expect(summary.physicalTotal).toBe(11)
    expect(summary.breakdown.application[0]).toMatchObject({
      table: 'file',
      logicalRows: 1,
      physicalRows: 3,
      indexRows: 2,
    })
  })

  it('attributes an uncaptured private update without inventing a synced row', () => {
    const collector = new WriteAttributionCollector()
    collector.recordPhysical(
      'UPDATE factoryExecutionLease SET expiresAt = ? WHERE id = ?',
      4
    )
    collector.recordUncapturedLogical(1)
    collector.recordPhysical('INSERT INTO "_orez_tx_schema" (tx_id) VALUES (?)', 1)
    collector.recordPhysical('DELETE FROM "_orez_tx_schema" WHERE tx_id = ?', 1)
    const summary = collector.summarize({ ...META, namespaceClass: 'control' })
    assertWriteAttributionReconciles(summary)
    expect(summary.rustVisibleRows).toBe(0)
    expect(summary.breakdown.application).toEqual([
      {
        table: 'factoryExecutionLease',
        op: 'UPDATE',
        visibility: 'private',
        logicalRows: 1,
        physicalRows: 4,
        indexRows: 3,
      },
    ])
  })

  it('omits sql, params, and production identifiers from the summary object', () => {
    const summary = sixRowCapturedUpdate().summarize(META)
    expect(Object.keys(summary)).not.toEqual(
      expect.arrayContaining([...FORBIDDEN_FROM_KEYS])
    )
    assertNoForbiddenAttributionFields(summary as unknown as Record<string, unknown>)
  })
})

const FORBIDDEN_FROM_KEYS = [
  'sql',
  'params',
  'namespace',
  'objectId',
  'objectName',
] as const
