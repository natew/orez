import { isDeepStrictEqual } from 'node:util'

export const HISTORY_SCHEMA_VERSION = 1 as const
const MAX_I64 = 9223372036854775807n

export type TerminalPhase = 'ok' | 'fail' | 'info'
export type HistoryPhase = 'invoke' | TerminalPhase
export type HistoryKind =
  | 'transaction'
  | 'read'
  | 'mutation'
  | 'barrier'
  | 'fault'
  | 'push'
  | 'pull'

export type ExactlyOnceClientIdentity = {
  clientGroupId: string
  clientId: string
}

export type ExactlyOnceIdentity = ExactlyOnceClientIdentity & {
  mutationId: number
}

export type ExactlyOnceEffect = {
  type: 'increment-probe'
  probeId: string
}

export type ExactlyOnceEvidence =
  | {
      type: 'mutation'
      profileVersion: 1
      identity: ExactlyOnceIdentity
      effect: ExactlyOnceEffect
    }
  | {
      type: 'authority'
      profileVersion: 1
      observation: 'before' | 'after'
      identity: ExactlyOnceIdentity
      effect: ExactlyOnceEffect
      observed: null | {
        probeRowCount: number
        applicationCount: string
        clientRowCount: number
        lastMutationId: string | null
      }
    }
  | {
      type: 'client-probe'
      profileVersion: 1
      identity: ExactlyOnceIdentity
      effect: ExactlyOnceEffect
      observed: null | { resultType: 'complete'; applicationCount: string }
    }
  | {
      type: 'client-quiesce'
      profileVersion: 1
      identity: ExactlyOnceClientIdentity
      observed: null | {
        closed: true
        pendingPushAtClose: number
        pendingPullAtClose: number
        controllerPullAbortsRequested: number
        pendingAfterQuiesce: number
      }
    }
  | {
      type: 'push'
      profileVersion: 1
      identity: ExactlyOnceIdentity
      attempt: number
      source: 'stock-client' | 'harness-replay'
      bodyDigest: string
      rawBodySha256: string
      observed:
        | null
        | { outcome: 'response-lost' }
        | {
            outcome: 'response'
            status: number
            bodySha256: string
            responseClientId: string | null
            responseMutationCount: number
            mutationId: number | null
            error: string | null
            details: string | null
          }
    }
  | {
      type: 'pull'
      profileVersion: 1
      identity: ExactlyOnceClientIdentity
      attempt: number
      observed:
        | null
        | { outcome: 'pull-lmid-observed'; lastMutationId: string | null }
        | { outcome: 'aborted-by-quiesce-controller' }
    }
  | {
      type: 'fault'
      profileVersion: 1
      identity: ExactlyOnceIdentity
      planId: string
      operationId: string
      stage: 'arm' | 'fire' | 'heal'
      hook: string
      observed: null | { acknowledged: true }
    }

export type AppendMicroOp = {
  type: 'append'
  key: string
  value: number
}

export type ReadMicroOp = {
  type: 'read'
  key: string
  value: number[] | null
}

export type MicroOp = AppendMicroOp | ReadMicroOp

export type Snapshot = {
  // generation changes only on an explicitly recorded reset.
  generation: string
  watermark: string
  resetReason?: string
}

export type HistoryEvent = {
  schemaVersion: typeof HISTORY_SCHEMA_VERSION
  index: number
  // run-relative microseconds remain exact for far longer than any harness run.
  relativeMicros: number
  opId: string
  process: string
  phase: HistoryPhase
  kind: HistoryKind
  clientId?: string
  clientGroupId?: string
  transaction?: MicroOp[]
  snapshot?: Snapshot
  error?: string
  metadata?: Record<string, unknown>
  exactlyOnce?: ExactlyOnceEvidence
}

export type RunManifest = {
  schemaVersion: typeof HISTORY_SCHEMA_VERSION
  kind: 'orez-consistency-history'
  runId: string
  seed: {
    value: string
    source: 'fixed' | 'random' | 'replay'
  }
  workload: {
    name: string
    version: number
  }
  target: {
    name: string
    build: string
  }
  replay: {
    command: string
    env: Record<string, string>
  }
}

export type CheckResult = {
  valid: boolean
  violations: string[]
}

export type ElleListAppendEvent = {
  index: number
  time: number
  process: number
  type: HistoryPhase
  f: 'txn'
  value: (['append', string, number] | ['r', string, number[] | null])[]
}

function result(violations: string[]): CheckResult {
  return { valid: violations.length === 0, violations }
}

function canonicalWatermark(value: string): bigint | undefined {
  if (!/^(0|[1-9][0-9]*)$/.test(value)) return undefined
  const parsed = BigInt(value)
  return parsed <= MAX_I64 ? parsed : undefined
}

function transactionsCorrespond(
  invocation: HistoryEvent,
  completion: HistoryEvent
): boolean {
  if (invocation.transaction === undefined || completion.transaction === undefined) {
    return invocation.transaction === completion.transaction
  }
  if (invocation.transaction.length !== completion.transaction.length) return false
  return invocation.transaction.every((operation, index) => {
    const completed = completion.transaction![index]!
    if (operation.type !== completed.type || operation.key !== completed.key) return false
    if (operation.type === 'append') {
      return completed.type === 'append' && operation.value === completed.value
    }
    return operation.value === null && completed.type === 'read'
  })
}

function stableExactlyOnce(value: ExactlyOnceEvidence): unknown {
  if (value.type === 'mutation') return value
  const { observed: _observed, ...stable } = value
  return stable
}

export function exactlyOnceEvidenceCorresponds(
  invocation: ExactlyOnceEvidence | undefined,
  terminal: ExactlyOnceEvidence | undefined
): boolean {
  try {
    if (invocation === undefined || terminal === undefined) return invocation === terminal
    if (!isDeepStrictEqual(stableExactlyOnce(invocation), stableExactlyOnce(terminal)))
      return false
    if (invocation.type === 'mutation') return terminal.type === 'mutation'
    return (
      invocation.observed === null &&
      terminal.type === invocation.type &&
      terminal.observed !== null
    )
  } catch {
    return false
  }
}

function nonemptyString(value: unknown): value is string {
  return typeof value === 'string' && value.trim() !== ''
}

function exactKeys(value: Record<string, unknown>, expected: string[]): boolean {
  const actual = Object.keys(value).sort()
  const sorted = [...expected].sort()
  return (
    actual.length === sorted.length && actual.every((key, index) => key === sorted[index])
  )
}

function validateExactlyOnceEvent(event: HistoryEvent, violations: string[]): boolean {
  const evidence = event.exactlyOnce
  if (evidence === undefined) return true
  if (typeof evidence !== 'object' || evidence === null) {
    violations.push(`event ${event.index} has malformed exactly-once evidence`)
    return false
  }
  const raw = evidence as unknown as Record<string, unknown>
  const allowedEvidenceKeys: Record<string, string[]> = {
    mutation: ['effect', 'identity', 'profileVersion', 'type'],
    authority: [
      'effect',
      'identity',
      'observation',
      'observed',
      'profileVersion',
      'type',
    ],
    'client-probe': ['effect', 'identity', 'observed', 'profileVersion', 'type'],
    'client-quiesce': ['identity', 'observed', 'profileVersion', 'type'],
    push: [
      'attempt',
      'bodyDigest',
      'identity',
      'observed',
      'profileVersion',
      'rawBodySha256',
      'source',
      'type',
    ],
    pull: ['attempt', 'identity', 'observed', 'profileVersion', 'type'],
    fault: [
      'hook',
      'identity',
      'observed',
      'operationId',
      'planId',
      'profileVersion',
      'stage',
      'type',
    ],
  }
  const allowed = allowedEvidenceKeys[String(raw.type)]
  if (
    ![
      'mutation',
      'authority',
      'client-probe',
      'client-quiesce',
      'push',
      'pull',
      'fault',
    ].includes(String(raw.type)) ||
    raw.profileVersion !== 1 ||
    typeof raw.identity !== 'object' ||
    raw.identity === null
  ) {
    violations.push(`event ${event.index} has malformed exactly-once evidence`)
    return false
  }
  if (
    !allowed ||
    Object.keys(raw)
      .sort()
      .some((key, index) => key !== allowed[index]) ||
    Object.keys(raw).length !== allowed.length
  ) {
    violations.push(`event ${event.index} has forbidden exactly-once fields`)
    return false
  }
  const rawIdentity = raw.identity as Record<string, unknown>
  if (
    !exactKeys(
      rawIdentity,
      raw.type === 'pull' || raw.type === 'client-quiesce'
        ? ['clientId', 'clientGroupId']
        : ['clientId', 'clientGroupId', 'mutationId']
    ) ||
    !nonemptyString(rawIdentity.clientId) ||
    !nonemptyString(rawIdentity.clientGroupId) ||
    (raw.type !== 'pull' &&
      raw.type !== 'client-quiesce' &&
      (!Number.isSafeInteger(rawIdentity.mutationId) ||
        Number(rawIdentity.mutationId) <= 0))
  ) {
    violations.push(`event ${event.index} has malformed exactly-once identity`)
    return false
  }
  if (
    (raw.type === 'pull' || raw.type === 'client-quiesce') &&
    'mutationId' in rawIdentity
  ) {
    violations.push(`event ${event.index} client-only evidence claims a mutation id`)
    return false
  }
  if (
    (raw.type === 'authority' ||
      raw.type === 'mutation' ||
      raw.type === 'client-probe') &&
    (typeof raw.effect !== 'object' ||
      raw.effect === null ||
      !exactKeys(raw.effect as Record<string, unknown>, ['type', 'probeId']) ||
      (raw.effect as Record<string, unknown>).type !== 'increment-probe' ||
      !nonemptyString((raw.effect as Record<string, unknown>).probeId))
  ) {
    violations.push(`event ${event.index} has malformed exactly-once effect`)
    return false
  }
  if (
    (raw.type === 'push' || raw.type === 'pull') &&
    (!Number.isSafeInteger(raw.attempt) || Number(raw.attempt) <= 0)
  ) {
    violations.push(`event ${event.index} has malformed exactly-once attempt`)
    return false
  }
  if (
    raw.type === 'push' &&
    (!nonemptyString(raw.bodyDigest) || !nonemptyString(raw.rawBodySha256))
  ) {
    violations.push(`event ${event.index} has malformed push body digest`)
    return false
  }
  if (
    raw.type === 'push' &&
    raw.source !== 'stock-client' &&
    raw.source !== 'harness-replay'
  ) {
    violations.push(`event ${event.index} has malformed push source`)
    return false
  }
  if (
    raw.type === 'fault' &&
    (!nonemptyString(raw.planId) ||
      !nonemptyString(raw.operationId) ||
      !['arm', 'fire', 'heal'].includes(String(raw.stage)) ||
      !nonemptyString(raw.hook))
  ) {
    violations.push(`event ${event.index} has malformed fault evidence`)
    return false
  }
  if (event.clientId !== evidence.identity.clientId) {
    violations.push(`event ${event.index} exactly-once clientId disagrees with top level`)
  }
  if (event.clientGroupId !== evidence.identity.clientGroupId) {
    violations.push(
      `event ${event.index} exactly-once clientGroupId disagrees with top level`
    )
  }
  const expectedKind =
    evidence.type === 'authority' || evidence.type === 'client-probe'
      ? 'read'
      : evidence.type === 'client-quiesce'
        ? 'barrier'
        : evidence.type
  if (event.kind !== expectedKind) {
    violations.push(
      `event ${event.index} exactly-once ${evidence.type} uses kind ${event.kind}`
    )
  }
  if (event.phase === 'invoke') {
    if (evidence.type !== 'mutation' && evidence.observed !== null) {
      violations.push(
        `event ${event.index} exactly-once invoke already has an observation`
      )
    }
  } else if (evidence.type !== 'mutation' && evidence.observed === null) {
    violations.push(`event ${event.index} exactly-once terminal has no observation`)
  } else if (evidence.type !== 'mutation') {
    const observed = evidence.observed as unknown
    if (typeof observed !== 'object' || observed === null) {
      violations.push(`event ${event.index} has malformed terminal observation`)
      return false
    }
    const value = observed as Record<string, unknown>
    if (
      evidence.type === 'authority' &&
      (!exactKeys(value, [
        'probeRowCount',
        'applicationCount',
        'clientRowCount',
        'lastMutationId',
      ]) ||
        !Number.isSafeInteger(value.probeRowCount) ||
        !Number.isSafeInteger(value.clientRowCount) ||
        typeof value.applicationCount !== 'string' ||
        (value.lastMutationId !== null && typeof value.lastMutationId !== 'string'))
    ) {
      violations.push(`event ${event.index} has malformed authority observation`)
      return false
    }
    if (
      evidence.type === 'client-probe' &&
      (!exactKeys(value, ['resultType', 'applicationCount']) ||
        value.resultType !== 'complete' ||
        typeof value.applicationCount !== 'string')
    ) {
      violations.push(`event ${event.index} has malformed client probe observation`)
      return false
    }
    if (
      evidence.type === 'client-quiesce' &&
      (!exactKeys(value, [
        'closed',
        'controllerPullAbortsRequested',
        'pendingAfterQuiesce',
        'pendingPushAtClose',
        'pendingPullAtClose',
      ]) ||
        value.closed !== true ||
        !Number.isSafeInteger(value.pendingPushAtClose) ||
        Object.is(value.pendingPushAtClose, -0) ||
        Number(value.pendingPushAtClose) < 0 ||
        !Number.isSafeInteger(value.pendingPullAtClose) ||
        Object.is(value.pendingPullAtClose, -0) ||
        Number(value.pendingPullAtClose) < 0 ||
        !Number.isSafeInteger(value.controllerPullAbortsRequested) ||
        Object.is(value.controllerPullAbortsRequested, -0) ||
        Number(value.controllerPullAbortsRequested) < 0 ||
        !Number.isSafeInteger(value.pendingAfterQuiesce) ||
        Object.is(value.pendingAfterQuiesce, -0) ||
        Number(value.pendingAfterQuiesce) < 0)
    ) {
      violations.push(`event ${event.index} has malformed client quiesce observation`)
      return false
    }
    if (
      evidence.type === 'push' &&
      !['response-lost', 'response'].includes(String(value.outcome))
    ) {
      violations.push(`event ${event.index} has malformed push observation`)
      return false
    }
    if (evidence.type === 'push') {
      const keys = Object.keys(value).sort()
      if (
        (value.outcome === 'response-lost' &&
          (keys.length !== 1 || keys[0] !== 'outcome')) ||
        (value.outcome === 'response' &&
          (!exactKeys(value, [
            'bodySha256',
            'details',
            'error',
            'mutationId',
            'outcome',
            'responseClientId',
            'responseMutationCount',
            'status',
          ]) ||
            !Number.isSafeInteger(value.status) ||
            Number(value.status) < 0 ||
            !/^[0-9a-f]{64}$/.test(String(value.bodySha256)) ||
            (value.responseClientId !== null &&
              typeof value.responseClientId !== 'string') ||
            !Number.isSafeInteger(value.responseMutationCount) ||
            Number(value.responseMutationCount) < 0 ||
            (value.mutationId !== null &&
              (!Number.isSafeInteger(value.mutationId) ||
                Number(value.mutationId) <= 0)) ||
            (value.error !== null && typeof value.error !== 'string') ||
            (value.details !== null && typeof value.details !== 'string')))
      ) {
        violations.push(`event ${event.index} has malformed push observation fields`)
        return false
      }
    }
    if (
      evidence.type === 'pull' &&
      !(
        (value.outcome === 'pull-lmid-observed' &&
          exactKeys(value, ['outcome', 'lastMutationId']) &&
          (value.lastMutationId === null || typeof value.lastMutationId === 'string')) ||
        (value.outcome === 'aborted-by-quiesce-controller' &&
          exactKeys(value, ['outcome']))
      )
    ) {
      violations.push(`event ${event.index} has malformed pull observation`)
      return false
    }
    if (
      evidence.type === 'fault' &&
      (!exactKeys(value, ['acknowledged']) || value.acknowledged !== true)
    ) {
      violations.push(`event ${event.index} has malformed fault observation`)
      return false
    }
  }
  return true
}

export function validateHistory(events: readonly HistoryEvent[]): CheckResult {
  const violations: string[] = []
  if (!Array.isArray(events)) return result(['history is not an array'])
  const inFlightByProcess = new Map<string, string>()
  const invoked = new Map<string, HistoryEvent>()
  const completed = new Set<string>()
  let previousTime = -1

  for (let position = 0; position < events.length; position++) {
    const event = events[position]!
    if (typeof event !== 'object' || event === null) {
      violations.push(`event ${position} is not an object`)
      continue
    }
    if (event.schemaVersion !== HISTORY_SCHEMA_VERSION) {
      violations.push(`event ${position} has schema version ${event.schemaVersion}`)
    }
    if (event.index !== position) {
      violations.push(`event ${position} has index ${event.index}`)
    }
    if (
      !Number.isSafeInteger(event.relativeMicros) ||
      event.relativeMicros < 0 ||
      event.relativeMicros < previousTime
    ) {
      violations.push(`event ${position} has non-monotonic time ${event.relativeMicros}`)
    }
    previousTime = event.relativeMicros
    const evidenceValid = validateExactlyOnceEvent(event, violations)
    if (!nonemptyString(event.opId) || !nonemptyString(event.process)) {
      violations.push(`event ${position} has invalid operation or process identity`)
      continue
    }
    if (!['invoke', 'ok', 'fail', 'info'].includes(event.phase)) {
      violations.push(`event ${position} has invalid phase ${String(event.phase)}`)
      continue
    }
    if (
      !['transaction', 'read', 'mutation', 'barrier', 'fault', 'push', 'pull'].includes(
        event.kind
      )
    ) {
      violations.push(`event ${position} has invalid kind ${String(event.kind)}`)
      continue
    }

    if (event.phase === 'invoke') {
      if (invoked.has(event.opId) || completed.has(event.opId)) {
        violations.push(`operation ${event.opId} is invoked more than once`)
      }
      const active = inFlightByProcess.get(event.process)
      if (active !== undefined) {
        violations.push(`process ${event.process} overlaps ${active} and ${event.opId}`)
      }
      invoked.set(event.opId, event)
      inFlightByProcess.set(event.process, event.opId)
      continue
    }

    const invocation = invoked.get(event.opId)
    if (invocation === undefined) {
      violations.push(`operation ${event.opId} completes without an invocation`)
      continue
    }
    if (completed.has(event.opId)) {
      violations.push(`operation ${event.opId} completes more than once`)
      continue
    }
    if (invocation.process !== event.process || invocation.kind !== event.kind) {
      violations.push(`operation ${event.opId} changes process or kind at completion`)
    }
    if (!transactionsCorrespond(invocation, event)) {
      violations.push(`operation ${event.opId} changes transaction at completion`)
    }
    if (
      evidenceValid &&
      !exactlyOnceEvidenceCorresponds(invocation.exactlyOnce, event.exactlyOnce)
    ) {
      violations.push(
        `operation ${event.opId} changes exactly-once evidence at completion`
      )
    }
    completed.add(event.opId)
    inFlightByProcess.delete(event.process)
  }

  for (const [opId] of invoked) {
    if (!completed.has(opId)) violations.push(`operation ${opId} has no terminal event`)
  }

  return result(violations)
}

export function checkSnapshotMonotonicity(events: readonly HistoryEvent[]): CheckResult {
  const violations: string[] = []
  const previousByClient = new Map<string, Snapshot>()

  for (const event of events) {
    if (event.phase !== 'ok' || event.snapshot === undefined) continue
    if (event.clientId === undefined) {
      violations.push(`event ${event.index} records a snapshot without a clientId`)
      continue
    }

    const previous = previousByClient.get(event.clientId)
    const current = event.snapshot
    const currentWatermark = canonicalWatermark(current.watermark)
    if (currentWatermark === undefined) {
      violations.push(
        `client ${event.clientId} has non-canonical watermark ${current.watermark}`
      )
      continue
    }
    if (previous !== undefined) {
      if (current.generation === previous.generation) {
        if (current.resetReason !== undefined) {
          violations.push(`client ${event.clientId} resets without changing generation`)
        }
        const previousWatermark = canonicalWatermark(previous.watermark)!
        if (currentWatermark < previousWatermark) {
          violations.push(
            `client ${event.clientId} regresses from ${previous.watermark} to ${current.watermark}`
          )
        }
      } else if (current.resetReason === undefined) {
        violations.push(
          `client ${event.clientId} changes generation without a recorded reset reason`
        )
      }
    }
    previousByClient.set(event.clientId, current)
  }

  return result(violations)
}

export function projectElleListAppend(
  events: readonly HistoryEvent[]
): ElleListAppendEvent[] {
  const structural = validateHistory(events)
  if (!structural.valid) {
    throw new Error(`invalid history:\n${structural.violations.join('\n')}`)
  }

  const projected: ElleListAppendEvent[] = []
  const appendValues = new Map<string, Set<number>>()
  const processIds = new Map<string, number>()
  for (const event of events) {
    if (event.kind !== 'transaction') continue
    if (event.transaction === undefined || event.transaction.length === 0) {
      throw new Error(`transaction ${event.opId} has no micro-operations`)
    }

    const value: ElleListAppendEvent['value'] = event.transaction.map((operation) => {
      if (operation.type === 'read') return ['r', operation.key, operation.value]
      if (event.phase === 'invoke') {
        const values = appendValues.get(operation.key) ?? new Set<number>()
        if (values.has(operation.value)) {
          throw new Error(
            `append value ${operation.value} is not unique for key ${operation.key}`
          )
        }
        values.add(operation.value)
        appendValues.set(operation.key, values)
      }
      return ['append', operation.key, operation.value]
    })

    projected.push({
      index: event.index,
      time: event.relativeMicros,
      process:
        processIds.get(event.process) ??
        (() => {
          const id = processIds.size
          processIds.set(event.process, id)
          return id
        })(),
      type: event.phase,
      f: 'txn',
      value,
    })
  }
  return projected
}
