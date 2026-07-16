import { describe, expect, test } from 'bun:test'

import {
  MutationApplicationError,
  isMutationApplicationError,
} from './src/mutation-error.ts'

describe('mutation application error contract', () => {
  test('classifies the shared class', () => {
    expect(isMutationApplicationError(new MutationApplicationError('nope'))).toBe(true)
  })

  test('classifies a foreign contract-compatible class structurally', () => {
    // an authoring layer (on-zero adapter) that must not depend on orez throws
    // its own class; the contract is the shape, never instanceof.
    class ForeignRejection extends Error {
      constructor(details) {
        super(details)
        this.name = 'MutationApplicationError'
        this.details = details
      }
    }
    expect(isMutationApplicationError(new ForeignRejection('denied'))).toBe(true)
  })

  test('rejects errors outside the contract', () => {
    expect(isMutationApplicationError(new Error('boom'))).toBe(false)
    const named = new Error('boom')
    named.name = 'MutationApplicationError'
    // name alone is not the contract: details must be a string
    expect(isMutationApplicationError(named)).toBe(false)
    expect(isMutationApplicationError(null)).toBe(false)
    expect(isMutationApplicationError('MutationApplicationError')).toBe(false)
  })
})
