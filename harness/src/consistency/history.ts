export const HISTORY_SCHEMA_VERSION = 1 as const

export type TerminalPhase = 'ok' | 'fail' | 'info'
export type HistoryPhase = 'invoke' | TerminalPhase
export type HistoryKind = 'transaction' | 'read' | 'mutation' | 'barrier' | 'fault'

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
  return BigInt(value)
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

export function validateHistory(events: readonly HistoryEvent[]): CheckResult {
  const violations: string[] = []
  const inFlightByProcess = new Map<string, string>()
  const invoked = new Map<string, HistoryEvent>()
  const completed = new Set<string>()
  let previousTime = -1

  for (let position = 0; position < events.length; position++) {
    const event = events[position]!
    if (event.schemaVersion !== HISTORY_SCHEMA_VERSION) {
      violations.push(`event ${position} has schema version ${event.schemaVersion}`)
    }
    if (event.index !== position) {
      violations.push(`event ${position} has index ${event.index}`)
    }
    if (
      !Number.isSafeInteger(event.relativeMicros) ||
      event.relativeMicros < previousTime
    ) {
      violations.push(`event ${position} has non-monotonic time ${event.relativeMicros}`)
    }
    previousTime = event.relativeMicros

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
