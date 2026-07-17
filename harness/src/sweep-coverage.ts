// Pairwise accounting for the sweep grammar. Unlike a raw Cartesian product,
// the denominator excludes combinations the generator cannot produce (for
// example start!=none on a non-task table, or limit=set with cardinality=one).
import type { GenSpec, GenWhere } from './fixture.js'

export const SWEEP_COVERAGE_AXES = [
  { name: 'table', values: ['user', 'project', 'member', 'task'] },
  { name: 'filter', values: ['none', 'cmp', 'boolean'] },
  { name: 'exists', values: ['none', 'plain', 'filtered'] },
  { name: 'order', values: ['id', 'multi', 'cursor'] },
  { name: 'limit', values: ['none', 'set'] },
  { name: 'start', values: ['none', 'exclusive', 'inclusive'] },
  { name: 'cursorValue', values: ['none', 'number', 'null'] },
  { name: 'related', values: ['none', 'plain', 'decorated', 'nested'] },
  { name: 'cardinality', values: ['many', 'one'] },
] as const

type Axis = (typeof SWEEP_COVERAGE_AXES)[number]
export type SweepCoverageAxisName = Axis['name']
type AxisAssignment = Record<SweepCoverageAxisName, string>

export type AxisPairCoverage = {
  axes: [SweepCoverageAxisName, SweepCoverageAxisName]
  hit: number
  total: number
  percent: number
}

export type PairwiseCoverageReport = {
  specs: number
  hit: number
  total: number
  percent: number
  byAxisPair: AxisPairCoverage[]
  missing: string[]
}

function percent(hit: number, total: number): number {
  return total === 0 ? 100 : Math.round((hit / total) * 1_000) / 10
}

function combinations(n: number): [number, number][] {
  const result: [number, number][] = []
  for (let left = 0; left < n; left++) {
    for (let right = left + 1; right < n; right++) result.push([left, right])
  }
  return result
}

function assignments(): AxisAssignment[] {
  const result: AxisAssignment[] = []
  const visit = (axisIndex: number, partial: Partial<AxisAssignment>) => {
    if (axisIndex === SWEEP_COVERAGE_AXES.length) {
      const assignment = partial as AxisAssignment
      if (isReachable(assignment)) result.push(assignment)
      return
    }
    const axis = SWEEP_COVERAGE_AXES[axisIndex]!
    for (const value of axis.values) {
      visit(axisIndex + 1, { ...partial, [axis.name]: value })
    }
  }
  visit(0, {})
  return result
}

// Coarse reachability constraints mirror genSpec()/genSub(). They describe
// which axis combinations are possible, not the generator probabilities.
function isReachable(assignment: AxisAssignment): boolean {
  const { table, exists, order, limit, start, cursorValue, related, cardinality } =
    assignment
  if (table === 'user' && (exists !== 'none' || related !== 'none')) return false
  if (table !== 'task' && start !== 'none') return false
  if ((start === 'none') !== (order !== 'cursor')) return false
  if ((start === 'none') !== (cursorValue === 'none')) return false
  if (table !== 'project' && related === 'plain') return false
  if (cardinality === 'one' && limit === 'set') return false
  return true
}

function filterValue(where: GenWhere | undefined): string {
  if (!where) return 'none'
  return where.op === 'cmp' ? 'cmp' : 'boolean'
}

function relatedValue(spec: GenSpec): string {
  if (!spec.related?.length) return 'none'
  if (spec.related.some((relation) => relation.sub?.related?.length)) return 'nested'
  if (spec.related.some((relation) => relation.sub !== undefined)) return 'decorated'
  return 'plain'
}

export function sweepAxisAssignment(spec: GenSpec): AxisAssignment {
  const start = spec.start ? (spec.start.inclusive ? 'inclusive' : 'exclusive') : 'none'
  const cursorValue = !spec.start
    ? 'none'
    : spec.start.row.dueAt === null
      ? 'null'
      : 'number'
  const exists = !spec.exists?.length
    ? 'none'
    : spec.exists.some((entry) => entry.where)
      ? 'filtered'
      : 'plain'
  const order = spec.start
    ? 'cursor'
    : spec.orderBy?.some(([column]) => column !== 'id')
      ? 'multi'
      : 'id'
  return {
    table: spec.table,
    filter: filterValue(spec.where),
    exists,
    order,
    limit: spec.limit === undefined ? 'none' : 'set',
    start,
    cursorValue,
    related: relatedValue(spec),
    cardinality: spec.one ? 'one' : 'many',
  }
}

function pairKey(
  leftAxis: SweepCoverageAxisName,
  leftValue: string,
  rightAxis: SweepCoverageAxisName,
  rightValue: string
): string {
  return `${leftAxis}=${leftValue} × ${rightAxis}=${rightValue}`
}

export function sweepPairwiseCoverage(specs: readonly GenSpec[]): PairwiseCoverageReport {
  const axisPairs = combinations(SWEEP_COVERAGE_AXES.length)
  const coverable = new Set<string>()
  const coverableByPair = new Map<string, Set<string>>()

  for (const assignment of assignments()) {
    for (const [leftIndex, rightIndex] of axisPairs) {
      const left = SWEEP_COVERAGE_AXES[leftIndex]!
      const right = SWEEP_COVERAGE_AXES[rightIndex]!
      const key = pairKey(
        left.name,
        assignment[left.name],
        right.name,
        assignment[right.name]
      )
      coverable.add(key)
      const axisPair = `${left.name}×${right.name}`
      const values = coverableByPair.get(axisPair) ?? new Set<string>()
      values.add(key)
      coverableByPair.set(axisPair, values)
    }
  }

  const hit = new Set<string>()
  for (const spec of specs) {
    const assignment = sweepAxisAssignment(spec)
    if (!isReachable(assignment)) {
      throw new Error(
        `sweep coverage classifier produced unreachable axes: ${JSON.stringify(assignment)}`
      )
    }
    for (const [leftIndex, rightIndex] of axisPairs) {
      const left = SWEEP_COVERAGE_AXES[leftIndex]!
      const right = SWEEP_COVERAGE_AXES[rightIndex]!
      hit.add(
        pairKey(left.name, assignment[left.name], right.name, assignment[right.name])
      )
    }
  }

  const byAxisPair = axisPairs.map(([leftIndex, rightIndex]) => {
    const left = SWEEP_COVERAGE_AXES[leftIndex]!
    const right = SWEEP_COVERAGE_AXES[rightIndex]!
    const keys = coverableByPair.get(`${left.name}×${right.name}`)!
    const pairHit = [...keys].filter((key) => hit.has(key)).length
    return {
      axes: [left.name, right.name] as [SweepCoverageAxisName, SweepCoverageAxisName],
      hit: pairHit,
      total: keys.size,
      percent: percent(pairHit, keys.size),
    }
  })

  const hitCount = [...coverable].filter((key) => hit.has(key)).length
  return {
    specs: specs.length,
    hit: hitCount,
    total: coverable.size,
    percent: percent(hitCount, coverable.size),
    byAxisPair,
    missing: [...coverable].filter((key) => !hit.has(key)).sort(),
  }
}
