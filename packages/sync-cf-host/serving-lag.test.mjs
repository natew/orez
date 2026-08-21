import { describe, expect, test } from 'bun:test'

import { ServingLagTracker } from './src/serving-lag.ts'

function harness() {
  const values = { lag: [], clamps: [], skew: [] }
  const tracker = new ServingLagTracker({
    servingLag: { record: (...args) => values.lag.push(args) },
    servingLagClamps: { add: (...args) => values.clamps.push(args) },
    upstreamClockSkew: { record: (...args) => values.skew.push(args) },
  })
  return { tracker, values }
}

describe('end-to-end serving lag', () => {
  test('coalesces to the oldest commit and clears on an empty advancement', () => {
    const { tracker, values } = harness()
    tracker.onVersionReady(4, 2_000, ['group'])
    tracker.onVersionReady(7, 2_100, ['group'])
    tracker.onVersionServed('group', 6, 3_000)
    expect(values.lag).toEqual([])
    tracker.onVersionServed('group', 7, 3_000)
    expect(values.lag).toEqual([[1, { outcome: 'advanced' }]])
  })

  test('tracks groups independently and clamps negative observations', () => {
    const { tracker, values } = harness()
    tracker.onVersionReady('9', 5_000, ['a', 'b'])
    tracker.onVersionServed('a', '9', 4_900)
    tracker.onVersionServed('b', '9', 5_200)
    expect(values.lag).toEqual([
      [0, { outcome: 'advanced' }],
      [0.2, { outcome: 'advanced' }],
    ])
    expect(values.clamps).toEqual([[1, { outcome: 'advanced' }]])
  })

  test('records no-change completion and raw midpoint clock skew', () => {
    const { tracker, values } = harness()
    tracker.recordNoChange(1_000, 1_250)
    tracker.recordClockSkew(1_120, 1_000, 1_100)
    expect(values.lag).toEqual([[0.25, { outcome: 'no_change' }]])
    expect(values.skew).toEqual([[0.07]])
  })
})
