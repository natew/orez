import { describe, expect, test } from 'bun:test'

import {
  checkAtomicVisibility,
  ATOMIC_VISIBILITY_WORKLOAD_PROFILE,
} from './atomic-visibility.js'
import { HISTORY_SCHEMA_VERSION, type HistoryEvent, type MicroOp } from './history.js'

type Terminal = 'ok' | 'info' | 'fail'

function mutation(opId: string, operations: MicroOp[], phase: Terminal = 'ok') {
  return [
    { opId, process: `process-${opId}`, phase: 'invoke' as const, operations },
    { opId, process: `process-${opId}`, phase, operations },
  ]
}

function read(opId: string, values: Record<string, number[] | null>) {
  const invoked = Object.keys(values).map((key) => ({
    type: 'read' as const,
    key,
    value: null,
  }))
  const completed = Object.entries(values).map(([key, value]) => ({
    type: 'read' as const,
    key,
    value,
  }))
  return [
    { opId, process: `process-${opId}`, phase: 'invoke' as const, operations: invoked },
    { opId, process: `process-${opId}`, phase: 'ok' as const, operations: completed },
  ]
}

function history(
  ...operations: {
    opId: string
    process: string
    phase: 'invoke' | Terminal
    operations: MicroOp[]
  }[]
): HistoryEvent[] {
  return operations.map(({ operations: transaction, ...operation }, index) => ({
    schemaVersion: HISTORY_SCHEMA_VERSION,
    index,
    relativeMicros: index,
    kind: operation.opId.startsWith('read') ? 'read' : 'mutation',
    transaction,
    ...operation,
  }))
}

const group = [
  { type: 'append' as const, key: 'x', value: 1 },
  { type: 'append' as const, key: 'y', value: 2 },
]

function validEvidence(phase: Terminal = 'ok'): HistoryEvent[] {
  return history(
    ...mutation('group-1', group, phase),
    ...read('read-before', { x: [], y: [] }),
    ...read('read-after', { x: [1], y: [2] })
  )
}

function violations(events: HistoryEvent[]): string[] {
  return checkAtomicVisibility(events).violations
}

describe(`atomic visibility (${ATOMIC_VISIBILITY_WORKLOAD_PROFILE.name}@${ATOMIC_VISIBILITY_WORKLOAD_PROFILE.version})`, () => {
  test('pins the dedicated adapter evidence boundary', () => {
    expect(ATOMIC_VISIBILITY_WORKLOAD_PROFILE.adapterRequirements).toEqual({
      mutation: 'authoritative-atomic-append-transaction',
      read: 'complete-full-scope-list-observation',
      appendIdentity: 'run-fresh-and-absent-from-initial-state',
    })
  })

  test('accepts none-before and all-after observations', () => {
    expect(checkAtomicVisibility(validEvidence())).toEqual({
      valid: true,
      violations: [],
    })
  })

  test('rejects a strict subset across two keys', () => {
    const events = history(
      ...mutation('group-1', group),
      ...read('read-subset', { x: [1], y: [] }),
      ...read('read-complete', { x: [1], y: [2] })
    )
    expect(violations(events)).toEqual([
      'atomic group group-1 is partially visible in read read-subset; missing effects: y=2',
    ])
  })

  test('rejects a strict subset of two effects on the same key', () => {
    const sameKey = [
      { type: 'append' as const, key: 'x', value: 1 },
      { type: 'append' as const, key: 'x', value: 2 },
    ]
    const events = history(
      ...mutation('group-same-key', sameKey),
      ...read('read-subset', { x: [1] }),
      ...read('read-complete', { x: [1, 2] })
    )
    expect(violations(events)).toEqual([
      'atomic group group-same-key is partially visible in read read-subset; missing effects: x=2',
    ])
  })

  test('allows info groups to be wholly absent or wholly present', () => {
    expect(checkAtomicVisibility(validEvidence('info'))).toEqual({
      valid: true,
      violations: [],
    })
    const all = history(
      ...mutation('group-info', group, 'info'),
      ...read('read-all', { x: [1], y: [2] })
    )
    expect(checkAtomicVisibility(all)).toEqual({ valid: true, violations: [] })
  })

  test('rejects a partially visible info group', () => {
    const events = history(
      ...mutation('group-info', group, 'info'),
      ...read('read-subset', { x: [1], y: [] }),
      ...read('read-complete', { x: [1], y: [2] })
    )
    expect(violations(events)).toEqual([
      'atomic group group-info is partially visible in read read-subset; missing effects: y=2',
    ])
  })

  test('does not require every ok group to become visible', () => {
    const events = history(
      ...mutation('group-visible', group),
      ...mutation('group-absent', [
        { type: 'append', key: 'a', value: 3 },
        { type: 'append', key: 'b', value: 4 },
      ]),
      ...read('read-evidence', { x: [1], y: [2], a: [], b: [] })
    )
    expect(checkAtomicVisibility(events)).toEqual({ valid: true, violations: [] })
  })

  test('rejects histories with no multi-effect group', () => {
    const events = history(
      ...mutation('single', [{ type: 'append', key: 'x', value: 1 }]),
      ...read('read-one', { x: [1] })
    )
    expect(violations(events)).toEqual([
      'atomic visibility requires at least one multi-effect mutation group',
      'atomic visibility requires at least one eligible group/read pair',
      'atomic visibility requires at least one eligible pair observing a complete group',
    ])
  })

  test('rejects histories with no successful read', () => {
    expect(violations(history(...mutation('group-1', group)))).toEqual([
      'atomic visibility requires at least one successful read',
      'atomic visibility requires at least one eligible group/read pair',
      'atomic visibility requires at least one eligible pair observing a complete group',
    ])
  })

  test('ignores partial-scope reads but rejects partial-scope-only evidence', () => {
    const events = history(
      ...mutation('group-1', group),
      ...read('read-partial-scope', { x: [1] })
    )
    expect(violations(events)).toEqual([
      'atomic visibility requires at least one eligible group/read pair',
      'atomic visibility requires at least one eligible pair observing a complete group',
    ])
  })

  test('rejects eligible evidence that observes only none', () => {
    const events = history(
      ...mutation('group-1', group),
      ...read('read-none', { x: [], y: [] })
    )
    expect(violations(events)).toEqual([
      'atomic visibility requires at least one eligible pair observing a complete group',
    ])
  })

  test('rejects duplicate append identities across groups', () => {
    const events = history(
      ...mutation('group-1', group),
      ...mutation('group-2', [
        { type: 'append', key: 'x', value: 1 },
        { type: 'append', key: 'z', value: 3 },
      ]),
      ...read('read-complete', { x: [1], y: [2], z: [3] })
    )
    expect(violations(events)).toEqual([
      'append identity x=1 is used more than once (group-1, group-2)',
    ])
  })

  test('rejects duplicate keys in one read observation', () => {
    const events = validEvidence()
    const terminal = events.find(
      (event) => event.opId === 'read-after' && event.phase === 'ok'
    )!
    terminal.transaction!.push({ type: 'read', key: 'x', value: [1] })
    events
      .find((event) => event.opId === 'read-after' && event.phase === 'invoke')!
      .transaction!.push({ type: 'read', key: 'x', value: null })
    expect(violations(events)).toEqual(['read read-after contains duplicate key x'])
  })

  test('rejects reads inside mutations and appends inside reads', () => {
    const mutationWithRead = history(
      ...mutation('bad-mutation', [
        { type: 'append', key: 'x', value: 9 },
        { type: 'read', key: 'y', value: null },
      ]),
      ...mutation('group-1', group),
      ...read('read-complete', { x: [1], y: [2] })
    )
    expect(violations(mutationWithRead)).toEqual([
      'mutation bad-mutation terminal ok contains read micro-operation at index 1',
    ])

    const readWithAppend = validEvidence()
    for (const event of readWithAppend.filter(({ opId }) => opId === 'read-before')) {
      event.transaction!.push({ type: 'append', key: 'z', value: 3 })
    }
    expect(violations(readWithAppend)).toEqual([
      'read read-before contains append micro-operation at index 2',
    ])
  })

  test('rejects terminal mutations and reads without transaction arrays', () => {
    const missingMutation = validEvidence()
    for (const event of missingMutation.filter(({ opId }) => opId === 'group-1')) {
      delete event.transaction
    }
    expect(violations(missingMutation)).toEqual([
      'mutation group-1 terminal ok has no transaction array',
      'atomic visibility requires at least one multi-effect mutation group',
      'atomic visibility requires at least one eligible group/read pair',
      'atomic visibility requires at least one eligible pair observing a complete group',
    ])

    const missingRead = validEvidence()
    for (const event of missingRead.filter(({ opId }) => opId === 'read-before')) {
      delete event.transaction
    }
    expect(violations(missingRead)).toEqual(['read read-before has no transaction array'])
  })

  test('treats a completed null read as an empty list', () => {
    const events = history(
      ...mutation('group-1', group),
      ...read('read-null', { x: null, y: null }),
      ...read('read-complete', { x: [1], y: [2] })
    )
    expect(checkAtomicVisibility(events)).toEqual({ valid: true, violations: [] })
  })

  test('excludes failed mutations from atomic groups', () => {
    const events = history(
      ...mutation('failed-group', group, 'fail'),
      ...read('read-effects', { x: [1], y: [] })
    )
    expect(violations(events)).toContain(
      'atomic visibility requires at least one multi-effect mutation group'
    )
    expect(
      violations(events).some((violation) => violation.includes('partially visible'))
    ).toBe(false)
  })

  test('propagates validateHistory failures without semantic diagnostics', () => {
    const malformed = history(...mutation('group-1', group))
    malformed.shift()
    malformed.forEach((event, index) => (event.index = index))
    expect(violations(malformed)).toEqual([
      'operation group-1 completes without an invocation',
    ])
  })
})
