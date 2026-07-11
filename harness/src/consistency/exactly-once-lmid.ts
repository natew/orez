import { validateFaultSchedule, type FaultSchedule } from './fault-schedule.js'
import {
  validateHistory,
  type ExactlyOnceEvidence,
  type HistoryEvent,
} from './history.js'

import type { ConsistencyCheck } from './artifacts.js'

export const EXACTLY_ONCE_LMID_PROFILE = {
  name: 'exactly-once-lost-push-lmid-plus-server-replay',
  version: 1,
} as const

export type ExactlyOnceLmidResult = Pick<
  ConsistencyCheck,
  'status' | 'violations' | 'reports'
>

const MAX_I64 = 9223372036854775807n

function canonicalI64(value: string): bigint | undefined {
  if (!/^(0|[1-9][0-9]*)$/.test(value)) return undefined
  const parsed = BigInt(value)
  return parsed <= MAX_I64 ? parsed : undefined
}

function evidenceEvents<T extends ExactlyOnceEvidence['type']>(
  events: readonly HistoryEvent[],
  type: T
): (HistoryEvent & { exactlyOnce: Extract<ExactlyOnceEvidence, { type: T }> })[] {
  return events.filter(
    (
      event
    ): event is HistoryEvent & {
      exactlyOnce: Extract<ExactlyOnceEvidence, { type: T }>
    } => event.exactlyOnce?.type === type
  )
}

function terminalPairs<T extends ExactlyOnceEvidence['type']>(
  events: readonly HistoryEvent[],
  type: T
) {
  const typed = evidenceEvents(events, type)
  return typed
    .filter((event) => event.phase === 'invoke')
    .map((invoke) => ({
      invoke,
      terminal: typed.find(
        (event) => event.opId === invoke.opId && event.phase !== 'invoke'
      ),
    }))
}

function sameIdentity(a: ExactlyOnceEvidence, b: ExactlyOnceEvidence): boolean {
  return (
    a.identity.clientGroupId === b.identity.clientGroupId &&
    a.identity.clientId === b.identity.clientId &&
    (!('mutationId' in a.identity) ||
      !('mutationId' in b.identity) ||
      a.identity.mutationId === b.identity.mutationId)
  )
}

export function checkExactlyOnceLmid(
  events: readonly HistoryEvent[],
  schedule: FaultSchedule
): ExactlyOnceLmidResult {
  const history = validateHistory(events)
  const fault = validateFaultSchedule(schedule)
  const structural = [
    ...history.violations.map((value) => `history: ${value}`),
    ...fault.violations.map((value) => `schedule: ${value}`),
  ]
  if (structural.length > 0) return { status: 'fail', violations: structural }

  const violations: string[] = []
  const reports: string[] = []
  const mutations = terminalPairs(events, 'mutation')
  if (mutations.length !== 1) {
    violations.push(`expected exactly one mutation, got ${mutations.length}`)
  }
  const mutation = mutations[0]
  const identity = mutation?.invoke.exactlyOnce.identity
  if (identity !== undefined) {
    if (!Number.isSafeInteger(identity.mutationId) || identity.mutationId !== 1) {
      violations.push(`mutation id must be the fresh-client value 1`)
    }
  }

  for (const event of events) {
    const evidence = event.exactlyOnce
    if (evidence === undefined || identity === undefined) continue
    if (!sameIdentity(evidence, mutation!.invoke.exactlyOnce)) {
      violations.push(`event ${event.opId} has a conflicting mutation identity`)
    }
    if (
      'attempt' in evidence &&
      (!Number.isSafeInteger(evidence.attempt) || evidence.attempt <= 0)
    ) {
      violations.push(`event ${event.opId} has invalid attempt ${evidence.attempt}`)
    }
  }

  const authorities = terminalPairs(events, 'authority')
  const authority = (observation: 'before' | 'after') =>
    authorities.find((pair) => pair.invoke.exactlyOnce.observation === observation)
  const before = authority('before')
  const after = authority('after')
  if (
    authorities.length !== 2 ||
    authorities.filter((pair) => pair.invoke.exactlyOnce.observation === 'before')
      .length !== 1 ||
    authorities.filter((pair) => pair.invoke.exactlyOnce.observation === 'after')
      .length !== 1
  ) {
    violations.push('authority observations must be exactly one before and one after')
  }
  if (!before?.terminal) violations.push('missing before authority observation')
  if (!after?.terminal) violations.push('missing after authority observation')

  const validateAuthority = (
    label: 'before' | 'after',
    pair: (typeof authorities)[number] | undefined
  ) => {
    const observed = pair?.terminal?.exactlyOnce.observed
    if (observed === null || observed === undefined) return
    if (
      !Number.isSafeInteger(observed.probeRowCount) ||
      observed.probeRowCount < 0 ||
      !Number.isSafeInteger(observed.clientRowCount) ||
      observed.clientRowCount < 0
    ) {
      violations.push(`${label} authority has invalid row counts`)
    }
    const application = canonicalI64(observed.applicationCount)
    if (application === undefined) {
      violations.push(`${label} authority has noncanonical application count`)
    }
    const lmid =
      observed.lastMutationId === null ? null : canonicalI64(observed.lastMutationId)
    if (observed.lastMutationId !== null && lmid === undefined) {
      violations.push(`${label} authority has noncanonical LMID`)
    }
    if (label === 'before') {
      if (
        observed.probeRowCount !== 1 ||
        application !== 0n ||
        observed.clientRowCount !== 0 ||
        observed.lastMutationId !== null
      ) {
        violations.push('before authority is not a fresh zero-application state')
      }
    } else if (
      observed.probeRowCount !== 1 ||
      application !== 1n ||
      observed.clientRowCount !== 1 ||
      lmid !== 1n
    ) {
      violations.push('after authority does not show one application and LMID 1')
    }
  }
  validateAuthority('before', before)
  validateAuthority('after', after)

  const pushes = terminalPairs(events, 'push').sort(
    (a, b) => a.invoke.exactlyOnce.attempt - b.invoke.exactlyOnce.attempt
  )
  const mutationPhase = mutation?.terminal?.phase
  const expectedPushes = mutationPhase === 'fail' ? 1 : 2
  if (
    pushes.length !== expectedPushes ||
    pushes.some((pair, index) => pair.invoke.exactlyOnce.attempt !== index + 1)
  ) {
    violations.push('push attempts must be exactly sequential attempts 1 and 2')
  }
  if (
    pushes[0]?.invoke.exactlyOnce.source !== 'stock-client' ||
    (expectedPushes === 2 && pushes[1]?.invoke.exactlyOnce.source !== 'harness-replay')
  ) {
    violations.push('requires one stock-client push then one harness replay')
  }
  if (
    pushes.some(
      (pair) =>
        !/^[0-9a-f]{64}$/.test(pair.invoke.exactlyOnce.bodyDigest) ||
        !/^[0-9a-f]{64}$/.test(pair.invoke.exactlyOnce.rawBodySha256)
    )
  ) {
    violations.push('push body digest is not canonical sha256 hex')
  }
  if (
    expectedPushes === 2 &&
    pushes[0] &&
    pushes[1] &&
    (pushes[0].invoke.exactlyOnce.bodyDigest !==
      pushes[1].invoke.exactlyOnce.bodyDigest ||
      pushes[0].invoke.exactlyOnce.rawBodySha256 !==
        pushes[1].invoke.exactlyOnce.rawBodySha256)
  ) {
    violations.push('push replay body digest does not match attempt 1')
  }
  if (pushes[0]?.terminal?.exactlyOnce.observed?.outcome !== 'response-lost') {
    violations.push('push attempt 1 did not lose its response')
  }
  const second = pushes[1]?.terminal?.exactlyOnce.observed
  if (
    expectedPushes === 2 &&
    (second?.outcome !== 'response' ||
      second.status !== 200 ||
      second.responseMutationCount !== 1 ||
      second.responseClientId !== identity?.clientId ||
      second.mutationId !== identity?.mutationId ||
      second.error !== 'alreadyProcessed' ||
      second.details?.match(/Expected:\s*(\d+)$/)?.[1] !== '2')
  ) {
    violations.push('push attempt 2 is not the matching already-processed replay')
  }

  const pulls = terminalPairs(events, 'pull').sort(
    (a, b) => a.invoke.exactlyOnce.attempt - b.invoke.exactlyOnce.attempt
  )
  if (pulls.length !== 1 || pulls[0]?.invoke.exactlyOnce.attempt !== 1) {
    violations.push('requires exactly one completed pull attempt 1')
  }
  for (const pair of pulls) {
    const observed = pair.terminal?.exactlyOnce.observed
    if (observed?.outcome !== 'pull-lmid-observed') {
      violations.push(`pull attempt ${pair.invoke.exactlyOnce.attempt} has wrong outcome`)
    } else if (
      observed.lastMutationId !== null &&
      canonicalI64(observed.lastMutationId) === undefined
    ) {
      violations.push(
        `pull attempt ${pair.invoke.exactlyOnce.attempt} has noncanonical LMID`
      )
    }
  }
  const recovered = pulls.find(
    (pair) => pair.terminal?.exactlyOnce.observed?.lastMutationId === '1'
  )
  if (!recovered?.terminal) violations.push('missing post-loss pull LMID 1 observation')

  const dropPlans = schedule.plans.filter(
    (candidate) => candidate.kind === 'drop-push-response'
  )
  const plan = dropPlans[0]
  if (
    schedule.faultsRequired !== true ||
    schedule.plans.length !== 1 ||
    dropPlans.length !== 1
  ) {
    violations.push('requires exactly one required drop-push-response fault plan')
  }
  if (
    plan &&
    (plan.arm.hook !== 'before-push' ||
      plan.fire.hook !== 'after-commit-before-response' ||
      plan.heal?.hook !== 'response-drop-consumed' ||
      plan.operationId !== mutation?.invoke.opId ||
      !plan.identity ||
      plan.identity.clientId !== identity?.clientId ||
      plan.identity.clientGroupId !== identity?.clientGroupId ||
      plan.identity.mutationId !== identity?.mutationId)
  ) {
    violations.push('fault plan does not match the versioned operation contract')
  }
  const faultTerminals = evidenceEvents(events, 'fault').filter(
    (event) => event.phase !== 'invoke'
  )
  const stageEvent = (stage: 'arm' | 'fire' | 'heal') =>
    faultTerminals.find((event) => event.exactlyOnce.stage === stage)
  if (
    faultTerminals.length !== 3 ||
    (['arm', 'fire', 'heal'] as const).some(
      (stage) =>
        faultTerminals.filter((event) => event.exactlyOnce.stage === stage).length !== 1
    )
  ) {
    violations.push('fault history must contain exactly one terminal per stage')
  }
  for (const stage of ['arm', 'fire', 'heal'] as const) {
    const receipt = schedule.receipts.find(
      (candidate) => candidate.planId === plan?.id && candidate.phase === stage
    )
    const event = stageEvent(stage)
    if (
      !receipt?.anchor ||
      !event ||
      receipt.anchor.historyIndex !== event.index ||
      receipt.anchor.historyOpId !== event.opId ||
      receipt.operationId !== event.exactlyOnce.operationId
    ) {
      violations.push(`${stage} receipt does not anchor its fault history event`)
    }
    if (
      event &&
      (event.exactlyOnce.planId !== plan?.id ||
        event.exactlyOnce.hook !== plan?.[stage]?.hook ||
        event.exactlyOnce.operationId !== mutation?.invoke.opId)
    ) {
      violations.push(`${stage} fault history does not match its plan`)
    }
  }

  const ordered = [
    before?.terminal?.index,
    terminalPairs(events, 'client-probe')[0]?.terminal?.index,
    stageEvent('arm')?.index,
    mutation?.invoke.index,
    pushes[0]?.invoke.index,
    stageEvent('fire')?.index,
    stageEvent('heal')?.index,
    pushes[0]?.terminal?.index,
    recovered?.terminal.index,
    mutation?.terminal?.index,
    ...(expectedPushes === 2
      ? [pushes[1]?.invoke.index, pushes[1]?.terminal?.index]
      : []),
    after?.terminal?.index,
  ]
  if (
    ordered.some((value) => value === undefined) ||
    ordered.some((value, index) => index > 0 && value! <= ordered[index - 1]!)
  ) {
    violations.push('exactly-once evidence is not in the required history order')
  }

  if (mutationPhase === 'fail') violations.push('success-only mutation terminated fail')
  const clientProbes = terminalPairs(events, 'client-probe')
  const clientObserved = clientProbes[0]?.terminal?.exactlyOnce.observed
  if (
    clientProbes.length !== 1 ||
    clientObserved?.resultType !== 'complete' ||
    canonicalI64(clientObserved.applicationCount) !== 0n
  ) {
    violations.push('missing complete client rank-0 probe precondition')
  }
  if (violations.length > 0) return { status: 'fail', violations }
  if (mutationPhase === 'info') {
    reports.push('mutation terminal outcome is unknown after complete recovery evidence')
    return { status: 'inconclusive', violations: [], reports }
  }
  if (mutationPhase !== 'ok') {
    return { status: 'fail', violations: ['mutation has no known successful terminal'] }
  }
  return {
    status: 'pass',
    violations: [],
    reports: [
      'stock push response lost; pull observed LMID 1; harness replay was already processed',
      'no automatic stock-client retry is claimed',
    ],
  }
}
