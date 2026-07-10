import { describe, expect, test } from 'bun:test'

import {
  SWEEP_COVERAGE_AXES,
  sweepAxisAssignment,
  sweepPairwiseCoverage,
} from './sweep-coverage.js'

import type { GenSpec } from './fixture.js'

describe('sweep pairwise coverage', () => {
  test('classifies every grammar axis', () => {
    const spec: GenSpec = {
      table: 'task',
      where: {
        op: 'and',
        children: [
          { op: 'cmp', col: 'done', cmp: '=', value: true },
          { op: 'cmp', col: 'rank', cmp: '>', value: 1 },
        ],
      },
      exists: [
        { rel: 'project', where: { op: 'cmp', col: 'id', cmp: '=', value: 'p0' } },
      ],
      orderBy: [
        ['rank', 'desc'],
        ['id', 'asc'],
      ],
      start: { row: { rank: 1, id: 't0' }, inclusive: true },
      limit: 3,
      related: [
        {
          rel: 'project',
          sub: { related: [{ rel: 'members' }] },
        },
      ],
    }

    expect(sweepAxisAssignment(spec)).toEqual({
      table: 'task',
      filter: 'boolean',
      exists: 'filtered',
      order: 'cursor',
      limit: 'set',
      start: 'inclusive',
      related: 'nested',
      cardinality: 'many',
    })
  })

  test('reports a constrained denominator and deterministic missing tuples', () => {
    const bare: GenSpec = { table: 'user', orderBy: [['id', 'asc']] }
    const report = sweepPairwiseCoverage([bare])

    expect(SWEEP_COVERAGE_AXES).toHaveLength(8)
    expect(report.hit).toBe(28) // C(8, 2): one tuple hit per axis pair
    expect(report.total).toBe(225)
    expect(report.percent).toBeGreaterThan(0)
    expect(report.percent).toBeLessThan(100)
    expect(report.missing).toEqual([...report.missing].sort())
    expect(report.byAxisPair).toHaveLength(28)
  })

  test('rejects a shape outside the declared generator grammar', () => {
    const impossible: GenSpec = {
      table: 'user',
      orderBy: [['id', 'asc']],
      exists: [{ rel: 'not-generated' }],
    }
    expect(() => sweepPairwiseCoverage([impossible])).toThrow(
      'classifier produced unreachable axes'
    )
  })
})
