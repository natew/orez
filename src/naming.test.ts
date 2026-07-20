import { describe, expect, test } from 'vitest'

import { internalSchema, publicationName } from './naming.js'

describe('zero naming conventions', () => {
  test('derives the internal schema and publication from the app id', () => {
    expect(internalSchema('soot')).toBe('soot_0')
    expect(publicationName('soot')).toBe('zero_soot')
  })
})
