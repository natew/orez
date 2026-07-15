import { describe, expect, test } from 'bun:test'

import {
  HISTORY_SCHEMA_VERSION,
  checkSnapshotMonotonicity,
  projectElleListAppend,
  validateHistory,
  type HistoryEvent,
} from './history.js'

function event(overrides: Partial<HistoryEvent>): HistoryEvent {
  return {
    schemaVersion: HISTORY_SCHEMA_VERSION,
    index: 0,
    relativeMicros: 0,
    opId: 'op-1',
    process: 'client-1',
    phase: 'invoke',
    kind: 'transaction',
    ...overrides,
  }
}

describe('history structure', () => {
  test('accepts paired single-threaded operations', () => {
    const history = [
      event({ transaction: [{ type: 'append', key: 'x', value: 1 }] }),
      event({
        index: 1,
        relativeMicros: 10,
        phase: 'ok',
        transaction: [{ type: 'append', key: 'x', value: 1 }],
      }),
    ]
    expect(validateHistory(history)).toEqual({ valid: true, violations: [] })
  })

  test('rejects an overlapping process and an unpaired completion', () => {
    const history = [
      event({}),
      event({ index: 1, relativeMicros: 1, opId: 'op-2' }),
      event({ index: 2, relativeMicros: 2, opId: 'op-3', phase: 'info' }),
    ]
    const checked = validateHistory(history)
    expect(checked.valid).toBe(false)
    expect(checked.violations).toContain('process client-1 overlaps op-1 and op-2')
    expect(checked.violations).toContain('operation op-3 completes without an invocation')
  })

  test('rejects a terminal transaction that differs from its invocation', () => {
    const history = [
      event({
        transaction: [
          { type: 'append', key: 'x', value: 1 },
          { type: 'read', key: 'y', value: null },
        ],
      }),
      event({
        index: 1,
        relativeMicros: 1,
        phase: 'ok',
        transaction: [
          { type: 'append', key: 'x', value: 2 },
          { type: 'read', key: 'z', value: [3] },
        ],
      }),
    ]
    expect(validateHistory(history).violations).toContain(
      'operation op-1 changes transaction at completion'
    )
  })

  test('rejects a negative first relative timestamp', () => {
    const history = [event({ relativeMicros: -1 })]
    expect(validateHistory(history).violations).toContain(
      'event 0 has non-monotonic time -1'
    )
  })
})

describe('snapshot monotonicity', () => {
  test('detects rollback within a generation', () => {
    const history = [
      event({
        phase: 'ok',
        kind: 'read',
        clientId: 'c1',
        snapshot: { generation: 'g1', watermark: '9007199254740993' },
      }),
      event({
        index: 1,
        relativeMicros: 1,
        opId: 'op-2',
        phase: 'ok',
        kind: 'read',
        clientId: 'c1',
        snapshot: { generation: 'g1', watermark: '9007199254740992' },
      }),
    ]
    expect(checkSnapshotMonotonicity(history).violations).toEqual([
      'client c1 regresses from 9007199254740993 to 9007199254740992',
    ])
  })

  test('allows a lower watermark after an explicit generation reset', () => {
    const history = [
      event({
        phase: 'ok',
        kind: 'read',
        clientId: 'c1',
        snapshot: { generation: 'g1', watermark: '9007199254740993' },
      }),
      event({
        index: 1,
        relativeMicros: 1,
        opId: 'op-2',
        phase: 'ok',
        kind: 'read',
        clientId: 'c1',
        snapshot: {
          generation: 'g2',
          watermark: '1',
          resetReason: 'retention fallback',
        },
      }),
    ]
    expect(checkSnapshotMonotonicity(history)).toEqual({ valid: true, violations: [] })
  })

  test('rejects an unrecorded generation change and a non-canonical watermark', () => {
    const history = [
      event({
        phase: 'ok',
        kind: 'read',
        clientId: 'c1',
        snapshot: { generation: 'g1', watermark: '10' },
      }),
      event({
        index: 1,
        relativeMicros: 1,
        opId: 'op-2',
        phase: 'ok',
        kind: 'read',
        clientId: 'c1',
        snapshot: { generation: 'g2', watermark: '09' },
      }),
    ]
    expect(checkSnapshotMonotonicity(history).violations).toEqual([
      'client c1 has non-canonical watermark 09',
    ])

    history[1]!.snapshot = { generation: 'g2', watermark: '9' }
    expect(checkSnapshotMonotonicity(history).violations).toEqual([
      'client c1 changes generation without a recorded reset reason',
    ])
  })

  test('rejects a watermark outside the signed i64 wire domain', () => {
    const history = [
      event({
        phase: 'ok',
        kind: 'read',
        clientId: 'c1',
        snapshot: { generation: 'g1', watermark: '9223372036854775808' },
      }),
    ]
    expect(checkSnapshotMonotonicity(history).violations).toEqual([
      'client c1 has non-canonical watermark 9223372036854775808',
    ])
  })
})

describe('elle projection', () => {
  test('projects only dedicated transactional operations', () => {
    const history = [
      event({
        transaction: [
          { type: 'append', key: 'x', value: 1 },
          { type: 'read', key: 'y', value: null },
        ],
      }),
      event({
        index: 1,
        relativeMicros: 10,
        phase: 'ok',
        transaction: [
          { type: 'append', key: 'x', value: 1 },
          { type: 'read', key: 'y', value: [2] },
        ],
      }),
    ]
    expect(projectElleListAppend(history)).toEqual([
      {
        index: 0,
        time: 0,
        process: 0,
        type: 'invoke',
        f: 'txn',
        value: [
          ['append', 'x', 1],
          ['r', 'y', null],
        ],
      },
      {
        index: 1,
        time: 10,
        process: 0,
        type: 'ok',
        f: 'txn',
        value: [
          ['append', 'x', 1],
          ['r', 'y', [2]],
        ],
      },
    ])
  })

  test('rejects duplicate append identities', () => {
    const history = [
      event({ transaction: [{ type: 'append', key: 'x', value: 1 }] }),
      event({
        index: 1,
        relativeMicros: 1,
        phase: 'ok',
        transaction: [{ type: 'append', key: 'x', value: 1 }],
      }),
      event({
        index: 2,
        relativeMicros: 2,
        opId: 'op-2',
        transaction: [{ type: 'append', key: 'x', value: 1 }],
      }),
      event({
        index: 3,
        relativeMicros: 3,
        opId: 'op-2',
        phase: 'fail',
        transaction: [{ type: 'append', key: 'x', value: 1 }],
      }),
    ]
    expect(() => projectElleListAppend(history)).toThrow(
      'append value 1 is not unique for key x'
    )
  })
})
