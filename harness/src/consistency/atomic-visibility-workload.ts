import { ATOMIC_VISIBILITY_WORKLOAD_PROFILE } from './atomic-visibility.js'

export type AtomicAppendEffect = {
  id: string
  projectId: string
  rank: number
}

export type AtomicAppendArgs = {
  effects: AtomicAppendEffect[]
}

export type AtomicTaskRow = AtomicAppendEffect

export type AtomicProfileEvidence = {
  profile: typeof ATOMIC_VISIBILITY_WORKLOAD_PROFILE
  projectIds: string[]
  idPrefix: string
  authorityPreflightRows: AtomicTaskRow[]
}

function nonempty(value: unknown, label: string): asserts value is string {
  if (typeof value !== 'string' || value.trim() === '') {
    throw new Error(`${label} must be a nonempty string`)
  }
}

export function validateAtomicAppendArgs(value: unknown): AtomicAppendArgs {
  if (typeof value !== 'object' || value === null || !('effects' in value)) {
    throw new Error('atomic append args must contain effects')
  }
  const effects = value.effects
  if (!Array.isArray(effects) || effects.length < 2) {
    throw new Error('atomic append requires at least two effects')
  }

  const ids = new Set<string>()
  const identities = new Set<string>()
  const validated = effects.map((effect, index): AtomicAppendEffect => {
    if (typeof effect !== 'object' || effect === null) {
      throw new Error(`atomic append effect ${index} must be an object`)
    }
    const candidate = effect as Record<string, unknown>
    nonempty(candidate.id, `atomic append effect ${index} id`)
    nonempty(candidate.projectId, `atomic append effect ${index} projectId`)
    const rank = candidate.rank
    if (!Number.isSafeInteger(rank) || Object.is(rank, -0)) {
      throw new Error(`atomic append effect ${index} rank must be a safe integer`)
    }
    if (ids.has(candidate.id)) {
      throw new Error(`atomic append id ${candidate.id} is not unique`)
    }
    ids.add(candidate.id)
    const identity = JSON.stringify([candidate.projectId, rank])
    if (identities.has(identity)) {
      throw new Error(
        `atomic append identity ${candidate.projectId}=${rank} is not unique`
      )
    }
    identities.add(identity)
    return {
      id: candidate.id,
      projectId: candidate.projectId,
      rank: rank as number,
    }
  })
  return { effects: validated }
}

export function validateAtomicProfileEvidence(evidence: AtomicProfileEvidence): void {
  if (
    evidence.profile.name !== ATOMIC_VISIBILITY_WORKLOAD_PROFILE.name ||
    evidence.profile.version !== ATOMIC_VISIBILITY_WORKLOAD_PROFILE.version
  ) {
    throw new Error('atomic visibility workload profile does not match checker')
  }
  if (!Array.isArray(evidence.projectIds) || evidence.projectIds.length === 0) {
    throw new Error('atomic visibility requires an explicit nonempty project scope')
  }
  const scope = new Set<string>()
  for (const [index, projectId] of evidence.projectIds.entries()) {
    nonempty(projectId, `atomic visibility projectId ${index}`)
    if (scope.has(projectId)) {
      throw new Error(`atomic visibility projectId ${projectId} is not unique`)
    }
    scope.add(projectId)
  }
  nonempty(evidence.idPrefix, 'atomic visibility id prefix')
  if (evidence.authorityPreflightRows.length !== 0) {
    throw new Error('atomic visibility identities are not absent from initial authority')
  }
}

export function projectAtomicRead(
  projectIds: readonly string[],
  rows: readonly AtomicTaskRow[]
): { type: 'read'; key: string; value: number[] }[] {
  const scope = new Set(projectIds)
  if (scope.size !== projectIds.length || scope.size === 0) {
    throw new Error('atomic read projection requires a unique nonempty project scope')
  }
  const values = new Map(projectIds.map((projectId) => [projectId, [] as number[]]))
  for (const [index, row] of rows.entries()) {
    if (!scope.has(row.projectId)) {
      throw new Error(`atomic read row ${index} is outside requested project scope`)
    }
    if (!Number.isFinite(row.rank) || Object.is(row.rank, -0)) {
      throw new Error(`atomic read row ${index} rank must be a lossless finite number`)
    }
    values.get(row.projectId)!.push(row.rank)
  }
  return projectIds.map((projectId) => ({
    type: 'read' as const,
    key: projectId,
    value: values.get(projectId)!.sort((a, b) => a - b),
  }))
}

export function assertAtomicAuthorityRows(
  effects: readonly AtomicAppendEffect[],
  rows: readonly AtomicTaskRow[]
): void {
  if (rows.length !== effects.length) {
    throw new Error(
      `atomic authority returned ${rows.length} rows for ${effects.length} effects`
    )
  }
  const expected = new Map(effects.map((effect) => [effect.id, effect]))
  const seen = new Set<string>()
  for (const row of rows) {
    const effect = expected.get(row.id)
    if (effect === undefined) {
      throw new Error(`atomic authority returned unexpected id ${row.id}`)
    }
    if (seen.has(row.id)) {
      throw new Error(`atomic authority returned duplicate id ${row.id}`)
    }
    seen.add(row.id)
    if (row.projectId !== effect.projectId || row.rank !== effect.rank) {
      throw new Error(
        `atomic authority row ${row.id} does not match ${effect.projectId}=${effect.rank}`
      )
    }
  }
}

export function classifyAtomicObservation(
  effects: readonly AtomicAppendEffect[],
  rows: readonly AtomicTaskRow[]
): 'none' | 'partial' | 'all' {
  const present = effects.filter((effect) =>
    rows.some((row) => row.projectId === effect.projectId && row.rank === effect.rank)
  ).length
  if (present === 0) return 'none'
  if (present === effects.length) return 'all'
  return 'partial'
}

export function assertAtomicInitialClientAbsence(
  effects: readonly AtomicAppendEffect[],
  rows: readonly AtomicTaskRow[]
): void {
  const observation = classifyAtomicObservation(effects, rows)
  if (observation !== 'none') {
    throw new Error(
      `atomic visibility identities are present in initial client state (${observation})`
    )
  }
}

export class AtomicObservationCollector {
  readonly #effects: readonly AtomicAppendEffect[]
  readonly #record: (
    rows: readonly AtomicTaskRow[],
    classification: 'none' | 'partial' | 'all'
  ) => void
  #initialized = false
  #armed = false

  constructor(
    effects: readonly AtomicAppendEffect[],
    record: (
      rows: readonly AtomicTaskRow[],
      classification: 'none' | 'partial' | 'all'
    ) => void
  ) {
    this.#effects = effects
    this.#record = record
  }

  initialize(rows: readonly AtomicTaskRow[]): void {
    if (this.#initialized) throw new Error('atomic observer is already initialized')
    assertAtomicInitialClientAbsence(this.#effects, rows)
    this.#initialized = true
  }

  arm(): void {
    if (!this.#initialized) throw new Error('atomic observer is not initialized')
    if (this.#armed) throw new Error('atomic observer is already armed')
    this.#armed = true
  }

  observe(rows: readonly AtomicTaskRow[]): 'none' | 'partial' | 'all' {
    if (!this.#armed) throw new Error('atomic observer is not armed')
    const classification = classifyAtomicObservation(this.#effects, rows)
    this.#record(rows, classification)
    return classification
  }
}

export function atomicReplayCommand(target: string, seed: string): string {
  return `bun src/atomic-visibility-lane.ts --target ${target} --seed=${seed} --replay`
}
