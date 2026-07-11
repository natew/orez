import { describe, expect, test } from 'bun:test'

import { validateHistory, type HistoryEvent } from './history.js'
import { HistoryRecorder, type HistoryEventInput } from './recorder.js'

function clock(...values: number[]): () => number {
  let index = 0
  return () => values[index++]!
}

function invocation(overrides: Partial<HistoryEventInput> = {}): HistoryEventInput {
  return {
    opId: 'op-1',
    process: 'client-1',
    phase: 'invoke',
    kind: 'transaction',
    transaction: [{ type: 'append', key: 'x', value: 1 }],
    ...overrides,
  }
}

function terminal(overrides: Partial<HistoryEventInput> = {}): HistoryEventInput {
  return {
    ...invocation(),
    phase: 'ok',
    ...overrides,
  }
}

describe('HistoryRecorder', () => {
  test('assigns contiguous indices and run-relative microseconds', () => {
    const recorder = new HistoryRecorder(clock(1_000_000, 1_000_007))
    const invoked = recorder.record(invocation())
    const completed = recorder.record(terminal())
    expect(invoked.index).toBe(0)
    expect(invoked.relativeMicros).toBe(0)
    expect(completed.index).toBe(1)
    expect(completed.relativeMicros).toBe(7)
    const history = recorder.finalize()
    expect(validateHistory(history)).toEqual({ valid: true, violations: [] })
  })

  test('accepts a completed read list for an invoked null read', () => {
    const recorder = new HistoryRecorder(clock(20, 21))
    recorder.record(
      invocation({ transaction: [{ type: 'read', key: 'x', value: null }] })
    )
    recorder.record(
      terminal({ transaction: [{ type: 'read', key: 'x', value: [1, 2] }] })
    )
    expect(validateHistory(recorder.finalize())).toEqual({ valid: true, violations: [] })
  })

  test('snapshots and returned events cannot mutate recorder state', () => {
    const recorder = new HistoryRecorder(clock(10, 11))
    const returned = recorder.record(invocation())
    returned.index = 99
    const snapshot = recorder.snapshot()
    snapshot[0]!.opId = 'mutated'
    recorder.record(terminal())
    expect(recorder.finalize().map(({ index, opId }) => ({ index, opId }))).toEqual([
      { index: 0, opId: 'op-1' },
      { index: 1, opId: 'op-1' },
    ])
  })

  test('rejects overlapping processes and duplicate invocations', () => {
    const overlapping = new HistoryRecorder(clock(0, 1))
    overlapping.record(invocation())
    expect(() => overlapping.record(invocation({ opId: 'op-2' }))).toThrow(
      'process client-1 overlaps op-1 and op-2'
    )

    const duplicate = new HistoryRecorder(clock(0, 1))
    duplicate.record(invocation())
    expect(() => duplicate.record(invocation())).toThrow(
      'operation op-1 is invoked more than once'
    )
  })

  test('rejects unknown, duplicate, and mismatched terminals', () => {
    const unknown = new HistoryRecorder(clock(0))
    expect(() => unknown.record(terminal())).toThrow(
      'operation op-1 completes without an invocation'
    )

    const duplicate = new HistoryRecorder(clock(0, 1, 2))
    duplicate.record(invocation())
    duplicate.record(terminal())
    expect(() => duplicate.record(terminal())).toThrow(
      'operation op-1 completes more than once'
    )

    const changed = new HistoryRecorder(clock(0, 1))
    changed.record(invocation())
    expect(() =>
      changed.record(terminal({ transaction: [{ type: 'append', key: 'x', value: 2 }] }))
    ).toThrow('operation op-1 changes transaction at completion')

    const changedProcess = new HistoryRecorder(clock(0, 1))
    changedProcess.record(invocation())
    expect(() => changedProcess.record(terminal({ process: 'client-2' }))).toThrow(
      'operation op-1 changes process or kind at completion'
    )

    const changedKind = new HistoryRecorder(clock(0, 1))
    changedKind.record(invocation())
    expect(() => changedKind.record(terminal({ kind: 'mutation' }))).toThrow(
      'operation op-1 changes process or kind at completion'
    )
  })

  test('refuses incomplete finalize and records after finalize', () => {
    expect(() => new HistoryRecorder(clock()).finalize()).toThrow(
      'cannot finalize empty history'
    )

    const incomplete = new HistoryRecorder(clock(0))
    incomplete.record(invocation())
    expect(() => incomplete.finalize()).toThrow(
      'cannot finalize history with pending operations: op-1'
    )

    const finalized = new HistoryRecorder(clock(0, 1, 2))
    finalized.record(invocation())
    finalized.record(terminal())
    finalized.finalize()
    expect(() => finalized.record(invocation({ opId: 'op-2' }))).toThrow(
      'history recorder is finalized'
    )
    expect(() => finalized.finalize()).toThrow('history recorder is finalized')
  })

  test('rejects regressing and unsafe clocks', () => {
    const regressing = new HistoryRecorder(clock(10, 9))
    regressing.record(invocation())
    expect(() => regressing.record(terminal())).toThrow(
      'clock regressed to run-relative microseconds -1'
    )

    const unsafe = new HistoryRecorder(clock(Number.MAX_SAFE_INTEGER + 1))
    expect(() => unsafe.record(invocation())).toThrow(
      `clock returned unsafe microseconds ${Number.MAX_SAFE_INTEGER + 1}`
    )
  })

  test('round-trip mutation is rejected by the existing validator', () => {
    const recorder = new HistoryRecorder(clock(0, 1))
    recorder.record(invocation())
    recorder.record(terminal())
    const history: HistoryEvent[] = recorder.finalize()
    history[1]!.index = 4
    expect(validateHistory(history).violations).toContain('event 1 has index 4')
  })
})
