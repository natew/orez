import {
  validateHistory,
  type AppendMicroOp,
  type CheckResult,
  type HistoryEvent,
} from './history.js'

export const ATOMIC_VISIBILITY_WORKLOAD_PROFILE = {
  name: 'dedicated-append-only-atomic-visibility',
  version: 2,
  adapterRequirements: {
    mutation: 'authoritative-atomic-append-transaction',
    read: 'complete-full-scope-list-observation',
    appendIdentity: 'run-fresh-and-absent-from-initial-state',
  },
} as const

type AtomicGroup = {
  opId: string
  clientId: string | undefined
  effects: AppendMicroOp[]
  keys: Set<string>
}

type ReadObservation = {
  opId: string
  clientId: string | undefined
  values: Map<string, number[]>
}

function result(violations: string[]): CheckResult {
  return { valid: violations.length === 0, violations }
}

/**
 * Checks none-or-all visibility only for histories emitted by
 * ATOMIC_VISIBILITY_WORKLOAD_PROFILE's dedicated versioned adapter.
 *
 * Schema v1 preserves transaction grouping and observed list membership, but
 * does not prove that `kind: mutation` is an authoritative atomic transaction
 * or that `kind: read` is a complete full-scope client observation. The adapter
 * must enforce those meanings and allocate every `(key, value)` append identity
 * freshly for this run, absent from both initial authority and client state.
 * Without that seed-state guarantee, a legitimate pre-mutation read could look
 * like a partially visible group because this checker intentionally applies no
 * realtime ordering. It does not infer plane or scope from metadata/process
 * names, require eventual visibility, or support mixed/general histories.
 */
export function checkAtomicVisibility(events: readonly HistoryEvent[]): CheckResult {
  const structural = validateHistory(events)
  if (!structural.valid) return structural

  const violations: string[] = []
  const groups: AtomicGroup[] = []
  const reads: ReadObservation[] = []
  const appendOwners = new Map<string, string>()
  let successfulReads = 0

  for (const event of events) {
    if (event.kind === 'mutation' && event.phase === 'ok') {
      if (!Array.isArray(event.transaction)) {
        violations.push(
          `mutation ${event.opId} terminal ${event.phase} has no transaction array`
        )
        continue
      }
      const transaction = event.transaction
      const effects: AppendMicroOp[] = []
      transaction.forEach((operation, index) => {
        if (operation.type === 'read') {
          violations.push(
            `mutation ${event.opId} terminal ${event.phase} contains read micro-operation at index ${index}`
          )
          return
        }
        effects.push(operation)
        const identity = JSON.stringify([operation.key, operation.value])
        const owner = appendOwners.get(identity)
        if (owner !== undefined) {
          violations.push(
            `append identity ${operation.key}=${operation.value} is used more than once (${owner}, ${event.opId})`
          )
        } else {
          appendOwners.set(identity, event.opId)
        }
      })
      if (transaction.length >= 2 && effects.length === transaction.length) {
        groups.push({
          opId: event.opId,
          clientId: event.clientId,
          effects,
          keys: new Set(effects.map(({ key }) => key)),
        })
      }
      continue
    }

    if (event.kind === 'read' && event.phase === 'ok') {
      successfulReads++
      if (!Array.isArray(event.transaction)) {
        violations.push(`read ${event.opId} has no transaction array`)
        continue
      }
      const values = new Map<string, number[]>()
      for (const [index, operation] of event.transaction.entries()) {
        if (operation.type === 'append') {
          violations.push(
            `read ${event.opId} contains append micro-operation at index ${index}`
          )
          continue
        }
        if (values.has(operation.key)) {
          violations.push(`read ${event.opId} contains duplicate key ${operation.key}`)
          continue
        }
        values.set(operation.key, operation.value ?? [])
      }
      reads.push({ opId: event.opId, clientId: event.clientId, values })
    }
  }

  let eligiblePairs = 0
  let completePairs = 0
  let nonWritingCompletePairs = 0
  for (const group of groups) {
    for (const read of reads) {
      if (![...group.keys].every((key) => read.values.has(key))) continue
      eligiblePairs++
      const nonWriting =
        group.clientId !== undefined &&
        read.clientId !== undefined &&
        read.clientId !== group.clientId
      const missing = group.effects.filter(
        ({ key, value }) => !read.values.get(key)!.includes(value)
      )
      const present = group.effects.length - missing.length
      if (present === group.effects.length) {
        completePairs++
        if (nonWriting) nonWritingCompletePairs++
      } else if (present > 0) {
        violations.push(
          `atomic group ${group.opId} is partially visible in read ${read.opId}; missing effects: ${missing.map(({ key, value }) => `${key}=${value}`).join(', ')}`
        )
      }
    }
  }

  if (groups.length === 0) {
    violations.push('atomic visibility requires at least one multi-effect mutation group')
  }
  if (successfulReads === 0) {
    violations.push('atomic visibility requires at least one successful read')
  }
  if (nonWritingCompletePairs === 0) {
    violations.push(
      'atomic visibility requires at least one non-writing client observation of a complete group'
    )
  }
  if (eligiblePairs === 0) {
    violations.push('atomic visibility requires at least one eligible group/read pair')
  }
  if (completePairs === 0) {
    violations.push(
      'atomic visibility requires at least one eligible pair observing a complete group'
    )
  }

  return result(violations)
}
