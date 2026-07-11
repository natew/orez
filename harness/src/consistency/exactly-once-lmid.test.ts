import { describe, expect, test } from 'bun:test'

import { checkExactlyOnceLmid, EXACTLY_ONCE_LMID_PROFILE } from './exactly-once-lmid.js'
import { FAULT_SCHEDULE_SCHEMA_VERSION, type FaultSchedule } from './fault-schedule.js'
import { validateHistory } from './history.js'
import { HistoryRecorder } from './recorder.js'

import type {
  ExactlyOnceEvidence,
  ExactlyOnceIdentity,
  HistoryEvent,
  TerminalPhase,
} from './history.js'

const identity: ExactlyOnceIdentity = {
  clientGroupId: 'group-1',
  clientId: 'client-1',
  mutationId: 1,
}
const effect = { type: 'increment-probe' as const, probeId: 'probe-1' }

function history(phase: TerminalPhase = 'ok'): {
  events: HistoryEvent[]
  schedule: FaultSchedule
} {
  let now = 0
  const recorder = new HistoryRecorder(() => now++)
  const pair = (
    opId: string,
    process: string,
    kind: HistoryEvent['kind'],
    invoke: ExactlyOnceEvidence,
    terminal: ExactlyOnceEvidence,
    terminalPhase: TerminalPhase = 'ok'
  ) => {
    recorder.record({
      opId,
      process,
      phase: 'invoke',
      kind,
      clientId: identity.clientId,
      clientGroupId: identity.clientGroupId,
      exactlyOnce: invoke,
    })
    return recorder.record({
      opId,
      process,
      phase: terminalPhase,
      kind,
      clientId: identity.clientId,
      clientGroupId: identity.clientGroupId,
      exactlyOnce: terminal,
    })
  }
  const authority = (
    observation: 'before' | 'after',
    observed: Extract<ExactlyOnceEvidence, { type: 'authority' }>['observed']
  ) => {
    const stable = {
      type: 'authority' as const,
      profileVersion: 1 as const,
      observation,
      identity,
      effect,
    }
    return pair(
      `authority-${observation}`,
      `authority-${observation}`,
      'read',
      { ...stable, observed: null },
      { ...stable, observed }
    )
  }
  authority('before', {
    probeRowCount: 1,
    applicationCount: '0',
    clientRowCount: 0,
    lastMutationId: null,
  })
  const clientProbe = {
    type: 'client-probe' as const,
    profileVersion: 1 as const,
    identity,
    effect,
  }
  pair(
    'client-probe',
    'client-probe',
    'read',
    { ...clientProbe, observed: null },
    {
      ...clientProbe,
      observed: { resultType: 'complete', applicationCount: '0' },
    }
  )
  const faultEvents = new Map<string, HistoryEvent>()
  const fault = (stage: 'arm' | 'fire' | 'heal', hook: string) => {
    const stable = {
      type: 'fault' as const,
      profileVersion: 1 as const,
      identity,
      planId: 'drop-1',
      operationId: 'mutation-1',
      stage,
      hook,
    }
    faultEvents.set(
      stage,
      pair(
        `fault-${stage}`,
        `fault-${stage}`,
        'fault',
        { ...stable, observed: null },
        { ...stable, observed: { acknowledged: true } }
      )
    )
  }
  fault('arm', 'before-push')
  const mutationEvidence = {
    type: 'mutation' as const,
    profileVersion: 1 as const,
    identity,
    effect,
  }
  recorder.record({
    opId: 'mutation-1',
    process: 'writer',
    phase: 'invoke',
    kind: 'mutation',
    clientId: identity.clientId,
    clientGroupId: identity.clientGroupId,
    exactlyOnce: mutationEvidence,
  })
  const pushStable = (attempt: number) => ({
    type: 'push' as const,
    profileVersion: 1 as const,
    identity,
    attempt,
    source: attempt === 1 ? ('stock-client' as const) : ('harness-replay' as const),
    bodyDigest: 'a'.repeat(64),
    rawBodySha256: 'b'.repeat(64),
  })
  recorder.record({
    opId: 'push-1',
    process: 'push-1',
    phase: 'invoke',
    kind: 'push',
    clientId: identity.clientId,
    clientGroupId: identity.clientGroupId,
    exactlyOnce: { ...pushStable(1), observed: null },
  })
  const pullStable = {
    type: 'pull' as const,
    profileVersion: 1 as const,
    identity: {
      clientId: identity.clientId,
      clientGroupId: identity.clientGroupId,
    },
    attempt: 1,
  }
  recorder.record({
    opId: 'pull-1',
    process: 'pull-1',
    phase: 'invoke',
    kind: 'pull',
    clientId: identity.clientId,
    clientGroupId: identity.clientGroupId,
    exactlyOnce: { ...pullStable, observed: null },
  })
  fault('fire', 'after-commit-before-response')
  fault('heal', 'response-drop-consumed')
  recorder.record({
    opId: 'push-1',
    process: 'push-1',
    phase: 'info',
    kind: 'push',
    clientId: identity.clientId,
    clientGroupId: identity.clientGroupId,
    exactlyOnce: { ...pushStable(1), observed: { outcome: 'response-lost' } },
  })
  recorder.record({
    opId: 'pull-1',
    process: 'pull-1',
    phase: 'ok',
    kind: 'pull',
    clientId: identity.clientId,
    clientGroupId: identity.clientGroupId,
    exactlyOnce: {
      ...pullStable,
      observed: { outcome: 'pull-lmid-observed', lastMutationId: '1' },
    },
  })
  recorder.record({
    opId: 'mutation-1',
    process: 'writer',
    phase,
    kind: 'mutation',
    clientId: identity.clientId,
    clientGroupId: identity.clientGroupId,
    exactlyOnce: mutationEvidence,
  })
  if (phase !== 'fail') {
    pair(
      'push-2',
      'push-2',
      'push',
      { ...pushStable(2), observed: null },
      {
        ...pushStable(2),
        observed: {
          outcome: 'response',
          status: 200,
          bodySha256: 'c'.repeat(64),
          responseClientId: identity.clientId,
          responseMutationCount: 1,
          mutationId: 1,
          error: 'alreadyProcessed',
          details: 'Ignoring mutation. Expected: 2',
        },
      }
    )
  }
  authority('after', {
    probeRowCount: 1,
    applicationCount: '1',
    clientRowCount: 1,
    lastMutationId: '1',
  })

  const point = {
    arm: { logicalStep: 1, hook: 'before-push' },
    fire: { logicalStep: 2, hook: 'after-commit-before-response' },
    heal: { logicalStep: 3, hook: 'response-drop-consumed' },
  }
  const schedule: FaultSchedule = {
    schemaVersion: FAULT_SCHEDULE_SCHEMA_VERSION,
    faultsRequired: true,
    plans: [
      {
        id: 'drop-1',
        kind: 'drop-push-response',
        ...point,
        operationId: 'mutation-1',
        identity,
      },
    ],
    receipts: (['arm', 'fire', 'heal'] as const).map((stage) => ({
      planId: 'drop-1',
      phase: stage,
      ...point[stage],
      operationId: 'mutation-1',
      identity,
      anchor: {
        historyIndex: faultEvents.get(stage)!.index,
        historyOpId: faultEvents.get(stage)!.opId,
      },
    })),
  }
  return { events: recorder.snapshot(), schedule }
}

describe(`${EXACTLY_ONCE_LMID_PROFILE.name}@${EXACTLY_ONCE_LMID_PROFILE.version}`, () => {
  test('accepts the exact lost-response, pull-LMID, and replay path', () => {
    const fixture = history()
    expect(validateHistory(fixture.events)).toEqual({ valid: true, violations: [] })
    expect(checkExactlyOnceLmid(fixture.events, fixture.schedule)).toEqual({
      status: 'pass',
      violations: [],
      reports: [
        'stock push response lost; pull observed LMID 1; harness replay was already processed',
        'no automatic stock-client retry is claimed',
      ],
    })
  })

  test('duplicate application is a safety failure even with terminal info', () => {
    const fixture = history('info')
    const after = fixture.events.find(
      (event) =>
        event.exactlyOnce?.type === 'authority' &&
        event.exactlyOnce.observation === 'after' &&
        event.phase === 'ok'
    )!
    if (after.exactlyOnce?.type === 'authority' && after.exactlyOnce.observed) {
      after.exactlyOnce.observed.applicationCount = '2'
    }
    const result = checkExactlyOnceLmid(fixture.events, fixture.schedule)
    expect(result.status).toBe('fail')
    expect(result.violations).toContain(
      'after authority does not show one application and LMID 1'
    )
  })

  test('terminal info alone is inconclusive', () => {
    const fixture = history('info')
    expect(checkExactlyOnceLmid(fixture.events, fixture.schedule)).toEqual({
      status: 'inconclusive',
      violations: [],
      reports: ['mutation terminal outcome is unknown after complete recovery evidence'],
    })
  })

  test('rejects replay body and receipt anchor mutants', () => {
    const digest = history()
    const push2 = digest.events.filter(
      (event) => event.exactlyOnce?.type === 'push' && event.exactlyOnce.attempt === 2
    )
    for (const event of push2) {
      if (event.exactlyOnce?.type === 'push') event.exactlyOnce.bodyDigest = 'different'
    }
    expect(checkExactlyOnceLmid(digest.events, digest.schedule).violations).toContain(
      'push replay body digest does not match attempt 1'
    )

    const anchor = history()
    anchor.schedule.receipts[1]!.anchor!.historyIndex++
    expect(checkExactlyOnceLmid(anchor.events, anchor.schedule).violations).toContain(
      'fire receipt does not anchor its fault history event'
    )
  })

  test('rejects malformed canonical domains', () => {
    for (const value of ['01', '-1', '9223372036854775808']) {
      const fixture = history()
      const after = fixture.events.find(
        (event) =>
          event.exactlyOnce?.type === 'authority' &&
          event.exactlyOnce.observation === 'after' &&
          event.phase === 'ok'
      )!
      if (after.exactlyOnce?.type === 'authority' && after.exactlyOnce.observed) {
        after.exactlyOnce.observed.lastMutationId = value
      }
      expect(checkExactlyOnceLmid(fixture.events, fixture.schedule).status).toBe('fail')
    }
  })

  test('returns violations for malformed raw JSON instead of throwing', () => {
    const malformedHistory = history()
    malformedHistory.events[0]!.exactlyOnce = {
      type: 'authority',
      profileVersion: 1,
      identity: null,
    } as never
    expect(() =>
      checkExactlyOnceLmid(malformedHistory.events, malformedHistory.schedule)
    ).not.toThrow()
    expect(
      checkExactlyOnceLmid(malformedHistory.events, malformedHistory.schedule).status
    ).toBe('fail')

    const malformedSchedule = history()
    malformedSchedule.schedule.receipts[0]!.operationId = null as never
    malformedSchedule.schedule.receipts[0]!.anchor!.historyOpId = 7 as never
    expect(() =>
      checkExactlyOnceLmid(malformedSchedule.events, malformedSchedule.schedule)
    ).not.toThrow()
    expect(
      checkExactlyOnceLmid(malformedSchedule.events, malformedSchedule.schedule).status
    ).toBe('fail')
  })

  test('rejects source, raw digest, rank, pull, attempt, and outcome mutants', () => {
    const cases: Array<[string, (fixture: ReturnType<typeof history>) => void]> = [
      [
        'source swap',
        ({ events }) => {
          for (const event of events) {
            if (event.exactlyOnce?.type === 'push' && event.exactlyOnce.attempt === 1)
              event.exactlyOnce.source = 'harness-replay'
          }
        },
      ],
      [
        'unequal raw digest',
        ({ events }) => {
          for (const event of events) {
            if (event.exactlyOnce?.type === 'push' && event.exactlyOnce.attempt === 2)
              event.exactlyOnce.rawBodySha256 = 'c'.repeat(64)
          }
        },
      ],
      [
        'rank one preflight',
        ({ events }) => {
          const event = events.find(
            (candidate) =>
              candidate.exactlyOnce?.type === 'client-probe' && candidate.phase === 'ok'
          )!
          if (event.exactlyOnce?.type === 'client-probe' && event.exactlyOnce.observed)
            event.exactlyOnce.observed.applicationCount = '1'
        },
      ],
      [
        'pull wrong lmid',
        ({ events }) => {
          const event = events.find(
            (candidate) =>
              candidate.exactlyOnce?.type === 'pull' && candidate.phase === 'ok'
          )!
          if (event.exactlyOnce?.type === 'pull' && event.exactlyOnce.observed)
            event.exactlyOnce.observed.lastMutationId = '2'
        },
      ],
      [
        'duplicate attempt',
        ({ events }) => {
          for (const event of events) {
            if (event.exactlyOnce?.type === 'push' && event.exactlyOnce.attempt === 2)
              event.exactlyOnce.attempt = 1
          }
        },
      ],
      [
        'wrong expected id',
        ({ events }) => {
          const event = events.find(
            (candidate) =>
              candidate.exactlyOnce?.type === 'push' &&
              candidate.exactlyOnce.attempt === 2 &&
              candidate.phase === 'ok'
          )!
          if (
            event.exactlyOnce?.type === 'push' &&
            event.exactlyOnce.observed?.outcome === 'response'
          )
            event.exactlyOnce.observed.details = 'Expected: 3'
        },
      ],
      [
        'extra replay response mutation',
        ({ events }) => {
          const event = events.find(
            (candidate) =>
              candidate.exactlyOnce?.type === 'push' &&
              candidate.exactlyOnce.attempt === 2 &&
              candidate.phase === 'ok'
          )!
          if (
            event.exactlyOnce?.type === 'push' &&
            event.exactlyOnce.observed?.outcome === 'response'
          )
            event.exactlyOnce.observed.responseMutationCount = 2
        },
      ],
      [
        'authority row missing',
        ({ events }) => {
          const event = events.find(
            (candidate) =>
              candidate.exactlyOnce?.type === 'authority' &&
              candidate.exactlyOnce.observation === 'after' &&
              candidate.phase === 'ok'
          )!
          if (event.exactlyOnce?.type === 'authority' && event.exactlyOnce.observed)
            event.exactlyOnce.observed.probeRowCount = 0
        },
      ],
    ]
    for (const [label, mutate] of cases) {
      const fixture = history()
      mutate(fixture)
      const result = checkExactlyOnceLmid(fixture.events, fixture.schedule)
      expect(result.status, label).toBe('fail')
      expect(result.violations.length, label).toBeGreaterThan(0)
    }
  })

  test('rank-one plus info remains fail and mutation fail is invalid', () => {
    const rank = history('info')
    const probe = rank.events.find(
      (event) => event.exactlyOnce?.type === 'client-probe' && event.phase === 'ok'
    )!
    if (probe.exactlyOnce?.type === 'client-probe' && probe.exactlyOnce.observed)
      probe.exactlyOnce.observed.applicationCount = '1'
    expect(checkExactlyOnceLmid(rank.events, rank.schedule).status).toBe('fail')
    expect(
      checkExactlyOnceLmid(history('fail').events, history('fail').schedule).status
    ).toBe('fail')
  })

  test('closes nested raw evidence objects', () => {
    const mutants: Array<(events: HistoryEvent[]) => void> = [
      (events) => Object.assign(events[0]!.exactlyOnce!.identity, { extra: true }),
      (events) =>
        Object.assign(
          (
            events.find((event) => event.exactlyOnce?.type === 'mutation')!
              .exactlyOnce as Extract<ExactlyOnceEvidence, { type: 'mutation' }>
          ).effect,
          { extra: true }
        ),
      (events) => {
        const event = events.find(
          (candidate) =>
            candidate.exactlyOnce?.type === 'authority' && candidate.phase === 'ok'
        )!
        Object.assign(
          (event.exactlyOnce as Extract<ExactlyOnceEvidence, { type: 'authority' }>)
            .observed,
          { extra: true }
        )
      },
      (events) => {
        const event = events.find(
          (candidate) =>
            candidate.exactlyOnce?.type === 'client-probe' && candidate.phase === 'ok'
        )!
        Object.assign(
          (event.exactlyOnce as Extract<ExactlyOnceEvidence, { type: 'client-probe' }>)
            .observed,
          { extra: true }
        )
      },
      (events) => {
        const event = events.find(
          (candidate) =>
            candidate.exactlyOnce?.type === 'pull' && candidate.phase === 'ok'
        )!
        Object.assign(
          (event.exactlyOnce as Extract<ExactlyOnceEvidence, { type: 'pull' }>).observed,
          { extra: true }
        )
      },
      (events) => {
        const event = events.find(
          (candidate) =>
            candidate.exactlyOnce?.type === 'fault' && candidate.phase === 'ok'
        )!
        Object.assign(
          (event.exactlyOnce as Extract<ExactlyOnceEvidence, { type: 'fault' }>).observed,
          { extra: true }
        )
      },
    ]
    for (const mutate of mutants) {
      const events = history().events
      mutate(events)
      expect(validateHistory(events).valid).toBe(false)
    }
  })
})
