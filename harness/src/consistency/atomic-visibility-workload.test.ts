import { describe, expect, test } from 'bun:test'

import { executeMutator } from '../fixture-data.js'
import {
  AtomicObservationCollector,
  atomicReplayCommand,
  assertAtomicAuthorityRows,
  assertAtomicInitialClientAbsence,
  classifyAtomicObservation,
  projectAtomicRead,
  validateAtomicAppendArgs,
  validateAtomicProfileEvidence,
} from './atomic-visibility-workload.js'
import {
  ATOMIC_VISIBILITY_WORKLOAD_PROFILE,
  checkAtomicVisibility,
} from './atomic-visibility.js'
import { HistoryRecorder } from './recorder.js'

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
    [{ effects: [effects[0], { ...effects[1], rank: -0 }] }, 'safe integer'],
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
        profile: { ...ATOMIC_VISIBILITY_WORKLOAD_PROFILE, version: 1 as 2 },
      })
    ).toThrow('does not match checker')
  })

  test('corroborates the exact authoritative effect set', () => {
    expect(() => assertAtomicAuthorityRows(effects, effects)).not.toThrow()
    expect(() => assertAtomicAuthorityRows(effects, effects.slice(0, 1))).toThrow(
      '1 rows for 2 effects'
    )
    expect(() =>
      assertAtomicAuthorityRows(effects, [effects[0]!, { ...effects[1]!, id: 'extra' }])
    ).toThrow('unexpected id extra')
    expect(() =>
      assertAtomicAuthorityRows(effects, [effects[0]!, { ...effects[1]!, rank: 999 }])
    ).toThrow('does not match p1=102')
    expect(() => assertAtomicAuthorityRows(effects, [effects[0]!, effects[0]!])).toThrow(
      'duplicate id run-a'
    )
  })

  test('classifies every observer snapshot without skipping a strict subset', () => {
    expect(classifyAtomicObservation(effects, [])).toBe('none')
    expect(classifyAtomicObservation(effects, [effects[0]!])).toBe('partial')
    expect(classifyAtomicObservation(effects, effects)).toBe('all')
  })

  test('requires group identities to be absent from initial client state', () => {
    expect(() => assertAtomicInitialClientAbsence(effects, [])).not.toThrow()
    expect(() => assertAtomicInitialClientAbsence(effects, [effects[0]!])).toThrow(
      'initial client state (partial)'
    )
    expect(() => assertAtomicInitialClientAbsence(effects, effects)).toThrow(
      'initial client state (all)'
    )
  })

  test('records a strict subset as terminal and freezes the decisive history', () => {
    let now = 0
    const recorder = new HistoryRecorder(() => now++)
    const recordPair = (opId: string, rows: typeof effects) => {
      recorder.record({
        opId,
        process: 'reader',
        phase: 'invoke',
        kind: 'read',
        transaction: [
          { type: 'read', key: 'p0', value: null },
          { type: 'read', key: 'p1', value: null },
        ],
      })
      recorder.record({
        opId,
        process: 'reader',
        phase: 'ok',
        kind: 'read',
        transaction: projectAtomicRead(['p0', 'p1'], rows),
      })
    }
    recordPair('before', [])
    const mutation = effects.map(({ projectId: key, rank: value }) => ({
      type: 'append' as const,
      key,
      value,
    }))
    recorder.record({
      opId: 'group',
      process: 'writer',
      phase: 'invoke',
      kind: 'mutation',
      transaction: mutation,
    })
    recorder.record({
      opId: 'group',
      process: 'writer',
      phase: 'ok',
      kind: 'mutation',
      transaction: mutation,
    })
    const seen: string[] = []
    let index = 0
    const collector = new AtomicObservationCollector(effects, (rows, state) => {
      seen.push(state)
      recordPair(`after-${index++}`, [...rows])
    })
    collector.initialize([])
    collector.arm()
    collector.observe([])
    collector.observe([effects[0]!])
    const frozenLength = recorder.snapshot().length
    expect(() => collector.observe(effects)).toThrow(
      'atomic observer is terminal after partial'
    )

    expect(seen).toEqual(['none', 'partial'])
    expect(recorder.snapshot()).toHaveLength(frozenLength)
    const outcome = checkAtomicVisibility(recorder.snapshot())
    expect(outcome.valid).toBe(false)
    expect(outcome.violations).toContain(
      'atomic group group is partially visible in read after-1; missing effects: p1=102'
    )
  })

  test('replay command preserves a safe leading-dash seed as one option value', () => {
    expect(atomicReplayCommand('orez-local', '-case')).toBe(
      'bun src/atomic-visibility-lane.ts --target orez-local --seed=-case --replay'
    )
  })
})
