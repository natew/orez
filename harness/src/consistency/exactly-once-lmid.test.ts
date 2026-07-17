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
const observer = {
  clientGroupId: 'observer-group-1',
  clientId: 'observer-client-1',
}
const effect = { type: 'increment-probe' as const, probeId: 'probe-1' }
const rejectedIdentity: ExactlyOnceIdentity = { ...identity, mutationId: 2 }
const rejectedEffect = { type: 'rejected-increment' as const, probeId: 'probe-1' }

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
    const eventIdentity =
      invoke.type === 'client-probe' ? invoke.observer : invoke.identity
    recorder.record({
      opId,
      process,
      phase: 'invoke',
      kind,
      clientId: eventIdentity.clientId,
      clientGroupId: eventIdentity.clientGroupId,
      exactlyOnce: invoke,
    })
    return recorder.record({
      opId,
      process,
      phase: terminalPhase,
      kind,
      clientId: eventIdentity.clientId,
      clientGroupId: eventIdentity.clientGroupId,
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
    observer,
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
    operationDigest: 'a'.repeat(64),
    mutationTimestamp: 123456,
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
  fault('fire', 'after-commit-before-client-delivery')
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
    const quiesce = {
      type: 'client-quiesce' as const,
      profileVersion: 1 as const,
      identity: {
        clientId: identity.clientId,
        clientGroupId: identity.clientGroupId,
      },
    }
    pair(
      'client-quiesce',
      'client-quiesce',
      'barrier',
      { ...quiesce, observed: null },
      {
        ...quiesce,
        observed: {
          closed: true,
          controllerPullAbortsRequested: 0,
          pendingAfterQuiesce: 0,
          pendingPushAtClose: 0,
          pendingPullAtClose: 0,
        },
      }
    )
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
  const rejectedMutation = {
    type: 'mutation' as const,
    profileVersion: 1 as const,
    identity: rejectedIdentity,
    effect: rejectedEffect,
  }
  pair(
    'mutation-2-app-error',
    'app-error-writer',
    'mutation',
    rejectedMutation,
    rejectedMutation
  )
  authority('after', {
    probeRowCount: 1,
    applicationCount: '1',
    clientRowCount: 1,
    lastMutationId: '2',
  })

  const point = {
    arm: { logicalStep: 1, hook: 'before-push' },
    fire: { logicalStep: 2, hook: 'after-commit-before-client-delivery' },
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

function reindex(events: HistoryEvent[], schedule?: FaultSchedule): void {
  events.forEach((event, index) => {
    event.index = index
    event.relativeMicros = index
  })
  for (const receipt of schedule?.receipts ?? []) {
    const event = events.find(
      (candidate) =>
        candidate.opId === receipt.anchor?.historyOpId && candidate.phase !== 'invoke'
    )
    if (event && receipt.anchor) receipt.anchor.historyIndex = event.index
  }
}

function addStockRetry(fixture: ReturnType<typeof history>): void {
  const firstInvoke = fixture.events.find(
    (event) => event.exactlyOnce?.type === 'push' && event.phase === 'invoke'
  )!
  const harness = fixture.events.filter(
    (event) => event.exactlyOnce?.type === 'push' && event.exactlyOnce.attempt === 2
  )
  for (const event of harness) {
    event.opId = 'push-3'
    event.process = 'push-3'
    if (event.exactlyOnce?.type === 'push') event.exactlyOnce.attempt = 3
  }
  const retryInvoke = structuredClone(firstInvoke)
  retryInvoke.opId = 'push-2'
  retryInvoke.process = 'push-2'
  if (retryInvoke.exactlyOnce?.type === 'push') {
    retryInvoke.exactlyOnce.attempt = 2
    retryInvoke.exactlyOnce.rawBodySha256 = 'd'.repeat(64)
    retryInvoke.exactlyOnce.mutationTimestamp++
  }
  const retryTerminal = structuredClone(harness.find((event) => event.phase === 'ok')!)
  retryTerminal.opId = 'push-2'
  retryTerminal.process = 'push-2'
  if (retryTerminal.exactlyOnce?.type === 'push') {
    retryTerminal.exactlyOnce.attempt = 2
    retryTerminal.exactlyOnce.source = 'stock-client'
    retryTerminal.exactlyOnce.rawBodySha256 = 'd'.repeat(64)
    retryTerminal.exactlyOnce.mutationTimestamp++
  }
  const mutationTerminal = fixture.events.findIndex(
    (event) => event.exactlyOnce?.type === 'mutation' && event.phase !== 'invoke'
  )
  fixture.events.splice(mutationTerminal, 0, retryInvoke, retryTerminal)
  reindex(fixture.events, fixture.schedule)
}

function addAnotherStockRetry(fixture: ReturnType<typeof history>): void {
  const harness = fixture.events.filter(
    (event) =>
      event.exactlyOnce?.type === 'push' && event.exactlyOnce.source === 'harness-replay'
  )
  const attempt = (
    harness[0]!.exactlyOnce as Extract<ExactlyOnceEvidence, { type: 'push' }>
  ).attempt
  const retry = structuredClone(harness)
  for (const event of harness) {
    event.opId = `push-${attempt + 1}`
    event.process = `push-${attempt + 1}`
    if (event.exactlyOnce?.type === 'push') event.exactlyOnce.attempt++
  }
  for (const event of retry) {
    event.opId = `push-${attempt}`
    event.process = `push-${attempt}`
    if (event.exactlyOnce?.type === 'push') {
      event.exactlyOnce.source = 'stock-client'
      event.exactlyOnce.rawBodySha256 = String(attempt).repeat(64).slice(0, 64)
      event.exactlyOnce.mutationTimestamp += attempt
    }
  }
  const mutationTerminal = fixture.events.findIndex(
    (event) => event.exactlyOnce?.type === 'mutation' && event.phase !== 'invoke'
  )
  fixture.events.splice(mutationTerminal, 0, ...retry)
  reindex(fixture.events, fixture.schedule)
}

function addPendingPullAbort(fixture: ReturnType<typeof history>): void {
  const firstPull = fixture.events.find(
    (event) => event.exactlyOnce?.type === 'pull' && event.phase === 'invoke'
  )!
  const invoke = structuredClone(firstPull)
  invoke.opId = 'pull-2'
  invoke.process = 'pull-2'
  if (invoke.exactlyOnce?.type === 'pull') invoke.exactlyOnce.attempt = 2
  const mutationTerminal = fixture.events.findIndex(
    (event) => event.exactlyOnce?.type === 'mutation' && event.phase !== 'invoke'
  )
  fixture.events.splice(mutationTerminal, 0, invoke)
  const quiesceInvoke = fixture.events.findIndex(
    (event) => event.exactlyOnce?.type === 'client-quiesce' && event.phase === 'invoke'
  )
  const terminal = structuredClone(invoke)
  terminal.phase = 'info'
  if (terminal.exactlyOnce?.type === 'pull')
    terminal.exactlyOnce.observed = { outcome: 'aborted-by-quiesce-controller' }
  fixture.events.splice(quiesceInvoke + 1, 0, terminal)
  const quiesceTerminal = fixture.events.find(
    (event) => event.exactlyOnce?.type === 'client-quiesce' && event.phase === 'ok'
  )!
  if (
    quiesceTerminal.exactlyOnce?.type === 'client-quiesce' &&
    quiesceTerminal.exactlyOnce.observed
  ) {
    quiesceTerminal.exactlyOnce.observed.pendingPullAtClose = 1
    quiesceTerminal.exactlyOnce.observed.controllerPullAbortsRequested = 1
  }
  reindex(fixture.events, fixture.schedule)
}

describe(`${EXACTLY_ONCE_LMID_PROFILE.name}@${EXACTLY_ONCE_LMID_PROFILE.version}`, () => {
  test('accepts the exact lost-response, pull-LMID, and replay path', () => {
    const fixture = history()
    expect(validateHistory(fixture.events)).toEqual({ valid: true, violations: [] })
    expect(checkExactlyOnceLmid(fixture.events, fixture.schedule)).toEqual({
      status: 'pass',
      violations: [],
      reports: [
        'pullLmidObserved=true stockRetryCount=0',
        'stockRetryTimestamps=none',
        'stockRetryTimestampDriftCount=0',
        'final harness replay was already processed',
        'appErrorMutationCount=1',
        'app-error mutation advanced LMID with no row effects',
        'neither stock retry nor pull recovery is universally required',
      ],
    })
  })

  test('accepts retry-only, both recovery branches, and pending pull quiescence', () => {
    const retryOnly = history()
    addStockRetry(retryOnly)
    retryOnly.events = retryOnly.events.filter(
      (event) => event.exactlyOnce?.type !== 'pull'
    )
    reindex(retryOnly.events, retryOnly.schedule)
    expect(checkExactlyOnceLmid(retryOnly.events, retryOnly.schedule)).toMatchObject({
      status: 'pass',
      reports: expect.arrayContaining(['pullLmidObserved=false stockRetryCount=1']),
    })

    const both = history()
    addStockRetry(both)
    expect(checkExactlyOnceLmid(both.events, both.schedule).status).toBe('pass')

    const retryBeforePull = history()
    addStockRetry(retryBeforePull)
    const retryEvents = retryBeforePull.events.filter(
      (event) =>
        event.exactlyOnce?.type === 'push' &&
        event.exactlyOnce.source === 'stock-client' &&
        event.exactlyOnce.attempt === 2
    )
    retryBeforePull.events = retryBeforePull.events.filter(
      (event) => !retryEvents.includes(event)
    )
    const pullTerminal = retryBeforePull.events.findIndex(
      (event) => event.exactlyOnce?.type === 'pull' && event.phase === 'ok'
    )
    retryBeforePull.events.splice(pullTerminal, 0, ...retryEvents)
    reindex(retryBeforePull.events, retryBeforePull.schedule)
    expect(
      checkExactlyOnceLmid(retryBeforePull.events, retryBeforePull.schedule).status
    ).toBe('pass')

    const nullThenRetry = history()
    addStockRetry(nullThenRetry)
    const nullPull = nullThenRetry.events.find(
      (event) => event.exactlyOnce?.type === 'pull' && event.phase === 'ok'
    )!
    if (nullPull.exactlyOnce?.type === 'pull' && nullPull.exactlyOnce.observed)
      nullPull.exactlyOnce.observed.lastMutationId = null
    expect(
      checkExactlyOnceLmid(nullThenRetry.events, nullThenRetry.schedule).status
    ).toBe('pass')

    const lifecycle = history()
    addPendingPullAbort(lifecycle)
    expect(checkExactlyOnceLmid(lifecycle.events, lifecycle.schedule).status).toBe('pass')

    const completedDuringClose = history()
    addPendingPullAbort(completedDuringClose)
    const terminal = completedDuringClose.events.find(
      (event) =>
        event.exactlyOnce?.type === 'pull' &&
        event.exactlyOnce.observed?.outcome === 'aborted-by-quiesce-controller'
    )!
    terminal.phase = 'ok'
    if (terminal.exactlyOnce?.type === 'pull') {
      terminal.exactlyOnce.observed = {
        outcome: 'pull-lmid-observed',
        lastMutationId: null,
      }
    }
    expect(
      checkExactlyOnceLmid(completedDuringClose.events, completedDuringClose.schedule)
        .status
    ).toBe('pass')
  })

  test('rejects malformed quiescence counts, identities, phases, and late traffic', () => {
    const cases: Array<[string, (fixture: ReturnType<typeof history>) => void]> = [
      [
        'quiesce info',
        ({ events }) => {
          events.find(
            (event) =>
              event.exactlyOnce?.type === 'client-quiesce' && event.phase === 'ok'
          )!.phase = 'info'
        },
      ],
      [
        'negative zero count',
        ({ events }) => {
          const event = events.find(
            (candidate) =>
              candidate.exactlyOnce?.type === 'client-quiesce' && candidate.phase === 'ok'
          )!
          if (event.exactlyOnce?.type === 'client-quiesce' && event.exactlyOnce.observed)
            event.exactlyOnce.observed.pendingPushAtClose = -0
        },
      ],
      [
        'quiesce mutation identity',
        ({ events }) => {
          const event = events.find(
            (candidate) => candidate.exactlyOnce?.type === 'client-quiesce'
          )!
          ;(
            event.exactlyOnce!.identity as unknown as Record<string, unknown>
          ).mutationId = 1
        },
      ],
      [
        'wrong aborted phase',
        (fixture) => {
          addPendingPullAbort(fixture)
          fixture.events.find(
            (event) =>
              event.exactlyOnce?.type === 'pull' &&
              event.exactlyOnce.observed?.outcome === 'aborted-by-quiesce-controller'
          )!.phase = 'ok'
        },
      ],
      [
        'wrong LMID phase',
        ({ events }) => {
          events.find(
            (event) => event.exactlyOnce?.type === 'pull' && event.phase === 'ok'
          )!.phase = 'info'
        },
      ],
      [
        'pending push at close',
        ({ events }) => {
          const event = events.find(
            (candidate) =>
              candidate.exactlyOnce?.type === 'client-quiesce' && candidate.phase === 'ok'
          )!
          if (event.exactlyOnce?.type === 'client-quiesce' && event.exactlyOnce.observed)
            event.exactlyOnce.observed.pendingPushAtClose = 1
        },
      ],
      [
        'nonzero pending after quiesce',
        ({ events }) => {
          const event = events.find(
            (candidate) =>
              candidate.exactlyOnce?.type === 'client-quiesce' && candidate.phase === 'ok'
          )!
          if (event.exactlyOnce?.type === 'client-quiesce' && event.exactlyOnce.observed)
            event.exactlyOnce.observed.pendingAfterQuiesce = 1
        },
      ],
      [
        'quiesce invoke before mutation terminal',
        (fixture) => {
          const index = fixture.events.findIndex(
            (event) =>
              event.exactlyOnce?.type === 'client-quiesce' && event.phase === 'invoke'
          )
          const [invoke] = fixture.events.splice(index, 1)
          const mutationTerminal = fixture.events.findIndex(
            (event) => event.exactlyOnce?.type === 'mutation' && event.phase !== 'invoke'
          )
          fixture.events.splice(mutationTerminal, 0, invoke!)
          reindex(fixture.events, fixture.schedule)
        },
      ],
      [
        'after authority invoked before replay',
        (fixture) => {
          const index = fixture.events.findIndex(
            (event) =>
              event.exactlyOnce?.type === 'authority' &&
              event.exactlyOnce.observation === 'after' &&
              event.phase === 'invoke'
          )
          const [invoke] = fixture.events.splice(index, 1)
          const replay = fixture.events.findIndex(
            (event) =>
              event.exactlyOnce?.type === 'push' &&
              event.exactlyOnce.source === 'harness-replay'
          )
          fixture.events.splice(replay, 0, invoke!)
          reindex(fixture.events, fixture.schedule)
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

  test('rejects negative zero in every quiescence counter', () => {
    for (const field of [
      'pendingPushAtClose',
      'pendingPullAtClose',
      'controllerPullAbortsRequested',
      'pendingAfterQuiesce',
    ] as const) {
      const fixture = history()
      const event = fixture.events.find(
        (candidate) =>
          candidate.exactlyOnce?.type === 'client-quiesce' && candidate.phase === 'ok'
      )!
      if (event.exactlyOnce?.type === 'client-quiesce' && event.exactlyOnce.observed)
        event.exactlyOnce.observed[field] = -0
      expect(validateHistory(fixture.events).valid, field).toBe(false)
    }
  })

  test('rejects bounded-recovery and final-replay cardinality mutants', () => {
    const fourRetries = history()
    addStockRetry(fourRetries)
    addAnotherStockRetry(fourRetries)
    addAnotherStockRetry(fourRetries)
    addAnotherStockRetry(fourRetries)
    expect(checkExactlyOnceLmid(fourRetries.events, fourRetries.schedule).status).toBe(
      'fail'
    )

    for (const mode of ['missing', 'duplicate'] as const) {
      const fixture = history()
      const harness = fixture.events.filter(
        (event) =>
          event.exactlyOnce?.type === 'push' &&
          event.exactlyOnce.source === 'harness-replay'
      )
      if (mode === 'missing')
        fixture.events = fixture.events.filter((event) => !harness.includes(event))
      else fixture.events.splice(-2, 0, ...structuredClone(harness))
      reindex(fixture.events, fixture.schedule)
      expect(checkExactlyOnceLmid(fixture.events, fixture.schedule).status, mode).toBe(
        'fail'
      )
    }
  })

  test('rejects stock push and pull traffic after quiescence', () => {
    const latePull = history()
    addStockRetry(latePull)
    const pulls = latePull.events.filter((event) => event.exactlyOnce?.type === 'pull')
    latePull.events = latePull.events.filter((event) => !pulls.includes(event))
    const afterQuiesce = latePull.events.findIndex(
      (event) => event.exactlyOnce?.type === 'client-quiesce' && event.phase === 'ok'
    )
    latePull.events.splice(afterQuiesce + 1, 0, ...pulls)
    reindex(latePull.events, latePull.schedule)
    expect(checkExactlyOnceLmid(latePull.events, latePull.schedule).violations).toContain(
      'stock protocol traffic occurred after client quiescence'
    )

    const latePush = history()
    const stock = latePush.events
      .filter(
        (event) =>
          event.exactlyOnce?.type === 'push' &&
          event.exactlyOnce.source === 'stock-client'
      )
      .map((event) => structuredClone(event))
    for (const event of stock) {
      event.opId = 'late-stock-push'
      event.process = 'late-stock-push'
      if (event.exactlyOnce?.type === 'push') event.exactlyOnce.attempt = 3
    }
    const quiesced = latePush.events.findIndex(
      (event) => event.exactlyOnce?.type === 'client-quiesce' && event.phase === 'ok'
    )
    latePush.events.splice(quiesced + 1, 0, ...stock)
    reindex(latePush.events, latePush.schedule)
    expect(checkExactlyOnceLmid(latePush.events, latePush.schedule).violations).toContain(
      'stock protocol traffic occurred after client quiescence'
    )
  })

  test('rejects stock retry operation drift and invoke before response loss', () => {
    const drift = history()
    addStockRetry(drift)
    for (const event of drift.events) {
      if (
        event.exactlyOnce?.type === 'push' &&
        event.exactlyOnce.source === 'stock-client' &&
        event.exactlyOnce.attempt === 2
      )
        event.exactlyOnce.operationDigest = 'e'.repeat(64)
    }
    expect(checkExactlyOnceLmid(drift.events, drift.schedule).violations).toContain(
      'stock retry 2 changed semantic body'
    )

    const early = history()
    addStockRetry(early)
    const retryInvokeIndex = early.events.findIndex(
      (event) =>
        event.exactlyOnce?.type === 'push' &&
        event.exactlyOnce.source === 'stock-client' &&
        event.exactlyOnce.attempt === 2 &&
        event.phase === 'invoke'
    )
    const [retryInvoke] = early.events.splice(retryInvokeIndex, 1)
    const lossIndex = early.events.findIndex(
      (event) =>
        event.exactlyOnce?.type === 'push' &&
        event.exactlyOnce.attempt === 1 &&
        event.phase === 'info'
    )
    early.events.splice(lossIndex, 0, retryInvoke!)
    reindex(early.events, early.schedule)
    expect(checkExactlyOnceLmid(early.events, early.schedule).violations).toContain(
      'stock retry invokes must follow loss and precede mutation terminal'
    )
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
      'after authority does not show one application and LMID 2'
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
      if (event.exactlyOnce?.type === 'push')
        event.exactlyOnce.operationDigest = 'different'
    }
    expect(checkExactlyOnceLmid(digest.events, digest.schedule).violations).toContain(
      'final harness replay does not match captured push 1'
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

  test('rejects a client probe made by the mutation writer', () => {
    const fixture = history()
    for (const event of fixture.events) {
      if (event.exactlyOnce?.type !== 'client-probe') continue
      event.exactlyOnce.observer = {
        clientId: identity.clientId,
        clientGroupId: identity.clientGroupId,
      }
      event.clientId = identity.clientId
      event.clientGroupId = identity.clientGroupId
    }
    expect(checkExactlyOnceLmid(fixture.events, fixture.schedule)).toMatchObject({
      status: 'fail',
      violations: ['missing complete non-writing client rank-0 probe precondition'],
    })
  })

  test('requires one rejected mutation, rollback, and its LMID advance', () => {
    const missing = history()
    missing.events = missing.events.filter(
      (event) =>
        event.exactlyOnce?.type !== 'mutation' ||
        event.exactlyOnce.effect.type !== 'rejected-increment'
    )
    reindex(missing.events, missing.schedule)
    expect(checkExactlyOnceLmid(missing.events, missing.schedule).violations).toContain(
      'expected exactly one app-error mutation, got 0'
    )

    const failedResponse = history()
    failedResponse.events.find(
      (event) =>
        event.exactlyOnce?.type === 'mutation' &&
        event.exactlyOnce.effect.type === 'rejected-increment' &&
        event.phase === 'ok'
    )!.phase = 'fail'
    expect(
      checkExactlyOnceLmid(failedResponse.events, failedResponse.schedule).violations
    ).toContain('app-error mutation did not receive the expected rejection')

    for (const [field, value] of [
      ['applicationCount', '2'],
      ['lastMutationId', '1'],
    ] as const) {
      const fixture = history()
      const after = fixture.events.find(
        (event) =>
          event.exactlyOnce?.type === 'authority' &&
          event.exactlyOnce.observation === 'after' &&
          event.phase === 'ok'
      )!
      if (after.exactlyOnce?.type === 'authority' && after.exactlyOnce.observed) {
        after.exactlyOnce.observed[field] = value
      }
      expect(checkExactlyOnceLmid(fixture.events, fixture.schedule).violations).toContain(
        'after authority does not show one application and LMID 2'
      )
    }
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
