import { describe, expect, test } from 'bun:test'

import { checkExactlyOnceLmid, EXACTLY_ONCE_LMID_PROFILE } from './exactly-once-lmid.js'
import { FAULT_SCHEDULE_SCHEMA_VERSION, type FaultSchedule } from './fault-schedule.js'
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
    bodyDigest: 'a'.repeat(64),
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
  pair(
    'push-2',
    'push-2',
    'push',
    { ...pushStable(2), observed: null },
    {
      ...pushStable(2),
      observed: { outcome: 'already-processed', mutationId: 1 },
    }
  )
  pair(
    'pull-2',
    'pull-2',
    'pull',
    { ...pullStable, attempt: 2, observed: null },
    {
      ...pullStable,
      attempt: 2,
      observed: { outcome: 'pull-lmid-observed', lastMutationId: '1' },
    }
  )
  recorder.record({
    opId: 'mutation-1',
    process: 'writer',
    phase,
    kind: 'mutation',
    clientId: identity.clientId,
    clientGroupId: identity.clientGroupId,
    exactlyOnce: mutationEvidence,
  })
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
    expect(checkExactlyOnceLmid(fixture.events, fixture.schedule)).toEqual({
      status: 'pass',
      violations: [],
      reports: ['recovered via pull LMID 1 and identical already-processed push replay'],
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
})
