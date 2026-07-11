import { describe, expect, test } from 'bun:test'

import { executeMutator } from '../fixture-data.js'
import {
  projectAtomicRead,
  validateAtomicAppendArgs,
  validateAtomicProfileEvidence,
} from './atomic-visibility-workload.js'
import { ATOMIC_VISIBILITY_WORKLOAD_PROFILE } from './atomic-visibility.js'

import type { SyncDb } from '../../../src/sync-server/sync-server.js'

const effects = [
  { id: 'run-a', projectId: 'p0', rank: 101 },
  { id: 'run-b', projectId: 'p1', rank: 102 },
]

describe('atomic visibility workload contract', () => {
  test('accepts one validated multi-effect transaction', () => {
    expect(validateAtomicAppendArgs({ effects })).toEqual({ effects })
  })

  test('authoritative executor validates the entire group before any write', () => {
    const calls: unknown[][] = []
    const tx = {
      exec: (...args: unknown[]) => calls.push(args),
    } as unknown as SyncDb
    expect(() =>
      executeMutator(
        tx,
        'atomicVisibility.appendGroup',
        { effects: [effects[0]] },
        { userID: 'u0' }
      )
    ).toThrow('at least two effects')
    expect(calls).toHaveLength(0)

    executeMutator(tx, 'atomicVisibility.appendGroup', { effects }, { userID: 'u0' })
    expect(calls).toHaveLength(2)
    expect(calls.map(([, values]) => values)).toEqual([
      ['run-a', 'p0', 'atomic-visibility:run-a', 101, 0, null, null],
      ['run-b', 'p1', 'atomic-visibility:run-b', 102, 0, null, null],
    ])
  })

  test.each([
    [{ effects: [effects[0]] }, 'at least two effects'],
    [{ effects: [effects[0], { ...effects[1], id: effects[0]!.id }] }, 'id run-a'],
    [
      { effects: [effects[0], { ...effects[1], projectId: 'p0', rank: 101 }] },
      'identity p0=101',
    ],
    [{ effects: [effects[0], { ...effects[1], rank: 1.5 }] }, 'safe integer'],
    [{ effects: [effects[0], { ...effects[1], projectId: '' }] }, 'nonempty string'],
  ])('rejects invalid append args %#', (args, message) => {
    expect(() => validateAtomicAppendArgs(args)).toThrow(message as string)
  })

  test('projects complete explicit scope including empty lists deterministically', () => {
    expect(
      projectAtomicRead(
        ['p0', 'p1'],
        [
          { id: 'b', projectId: 'p0', rank: 9 },
          { id: 'a', projectId: 'p0', rank: 3 },
        ]
      )
    ).toEqual([
      { type: 'read', key: 'p0', value: [3, 9] },
      { type: 'read', key: 'p1', value: [] },
    ])
  })

  test('rejects projection outside scope and lossy observed values', () => {
    expect(() =>
      projectAtomicRead(['p0'], [{ id: 'x', projectId: 'p1', rank: 1 }])
    ).toThrow('outside requested project scope')
    expect(() =>
      projectAtomicRead(['p0'], [{ id: 'x', projectId: 'p0', rank: Number.NaN }])
    ).toThrow('lossless finite number')
  })

  test('requires exact profile, explicit scope, prefix, and empty authority preflight', () => {
    const evidence = {
      profile: ATOMIC_VISIBILITY_WORKLOAD_PROFILE,
      projectIds: ['p0', 'p1'],
      idPrefix: 'atomic-run-',
      authorityPreflightRows: [],
    }
    expect(() => validateAtomicProfileEvidence(evidence)).not.toThrow()
    expect(() =>
      validateAtomicProfileEvidence({ ...evidence, authorityPreflightRows: effects })
    ).toThrow('not absent from initial authority')
    expect(() =>
      validateAtomicProfileEvidence({ ...evidence, projectIds: ['p0', 'p0'] })
    ).toThrow('is not unique')
    expect(() => validateAtomicProfileEvidence({ ...evidence, idPrefix: '' })).toThrow(
      'nonempty string'
    )
    expect(() =>
      validateAtomicProfileEvidence({
        ...evidence,
        profile: { ...ATOMIC_VISIBILITY_WORKLOAD_PROFILE, version: 2 as 1 },
      })
    ).toThrow('does not match checker')
  })
})
