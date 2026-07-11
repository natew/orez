import {
  HISTORY_SCHEMA_VERSION,
  validateHistory,
  type HistoryEvent,
  type MicroOp,
} from './history.js'

export type HistoryEventInput = Omit<
  HistoryEvent,
  'schemaVersion' | 'index' | 'relativeMicros'
>

export type MicrosecondClock = () => number

function transactionsCorrespond(invoked?: MicroOp[], completed?: MicroOp[]): boolean {
  if (invoked === undefined || completed === undefined) return invoked === completed
  if (invoked.length !== completed.length) return false
  return invoked.every((operation, index) => {
    const terminal = completed[index]!
    if (operation.type !== terminal.type || operation.key !== terminal.key) return false
    if (operation.type === 'append') {
      return terminal.type === 'append' && operation.value === terminal.value
    }
    return operation.value === null && terminal.type === 'read'
  })
}

export class HistoryRecorder {
  readonly #clock: MicrosecondClock
  readonly #events: HistoryEvent[] = []
  readonly #invoked = new Map<string, HistoryEvent>()
  readonly #completed = new Set<string>()
  readonly #inFlightByProcess = new Map<string, string>()
  #startedAt: number | undefined
  #previousRelativeMicros = -1
  #finalized = false

  constructor(clock: MicrosecondClock) {
    this.#clock = clock
  }

  record(input: HistoryEventInput): HistoryEvent {
    if (this.#finalized) throw new Error('history recorder is finalized')

    const now = this.#clock()
    if (!Number.isSafeInteger(now))
      throw new Error(`clock returned unsafe microseconds ${now}`)
    this.#startedAt ??= now
    const relativeMicros = now - this.#startedAt
    if (
      !Number.isSafeInteger(relativeMicros) ||
      relativeMicros < this.#previousRelativeMicros
    ) {
      throw new Error(`clock regressed to run-relative microseconds ${relativeMicros}`)
    }

    const event: HistoryEvent = structuredClone({
      ...input,
      schemaVersion: HISTORY_SCHEMA_VERSION,
      index: this.#events.length,
      relativeMicros,
    })

    if (event.phase === 'invoke') this.#recordInvocation(event)
    else this.#recordTerminal(event)

    this.#events.push(event)
    this.#previousRelativeMicros = relativeMicros
    return structuredClone(event)
  }

  snapshot(): HistoryEvent[] {
    return structuredClone(this.#events)
  }

  finalize(): HistoryEvent[] {
    if (this.#finalized) throw new Error('history recorder is finalized')
    if (this.#inFlightByProcess.size > 0) {
      const pending = [...this.#invoked.keys()].filter(
        (opId) => !this.#completed.has(opId)
      )
      throw new Error(
        `cannot finalize history with pending operations: ${pending.join(', ')}`
      )
    }
    const checked = validateHistory(this.#events)
    if (!checked.valid) {
      throw new Error(
        `cannot finalize invalid history:\n${checked.violations.join('\n')}`
      )
    }
    this.#finalized = true
    return structuredClone(this.#events)
  }

  #recordInvocation(event: HistoryEvent): void {
    if (this.#invoked.has(event.opId) || this.#completed.has(event.opId)) {
      throw new Error(`operation ${event.opId} is invoked more than once`)
    }
    const active = this.#inFlightByProcess.get(event.process)
    if (active !== undefined) {
      throw new Error(`process ${event.process} overlaps ${active} and ${event.opId}`)
    }
    this.#invoked.set(event.opId, event)
    this.#inFlightByProcess.set(event.process, event.opId)
  }

  #recordTerminal(event: HistoryEvent): void {
    if (this.#completed.has(event.opId)) {
      throw new Error(`operation ${event.opId} completes more than once`)
    }
    const invocation = this.#invoked.get(event.opId)
    if (invocation === undefined) {
      throw new Error(`operation ${event.opId} completes without an invocation`)
    }
    if (invocation.process !== event.process || invocation.kind !== event.kind) {
      throw new Error(`operation ${event.opId} changes process or kind at completion`)
    }
    if (!transactionsCorrespond(invocation.transaction, event.transaction)) {
      throw new Error(`operation ${event.opId} changes transaction at completion`)
    }
    this.#completed.add(event.opId)
    this.#inFlightByProcess.delete(event.process)
  }
}
