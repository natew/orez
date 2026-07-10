import { describe, expect, test } from 'bun:test'

import { validatePullCaps } from './src/config.ts'

describe('sync host config', () => {
  test('rejects a zero row cap that would stall cursor progress', () => {
    expect(() =>
      validatePullCaps({ maxChangeRows: 0, maxChangeBytes: 2_000_000 })
    ).toThrow('positive safe integer')
  })
})
