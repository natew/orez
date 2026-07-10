import { describe, expect, test } from 'bun:test'

import { validatePullCaps, validateSyncHostConfig } from './src/config.ts'

describe('sync host config', () => {
  test('rejects a zero row cap that would stall cursor progress', () => {
    expect(() =>
      validatePullCaps({ maxChangeRows: 0, maxChangeBytes: 2_000_000 })
    ).toThrow('positive safe integer')
  })
})

const base = {
  hostVersion: 'test',
  schema: { tables: {} },
  initialize() {},
  authenticate() {
    return { userID: 'u' }
  },
  namespace() {
    return 'n'
  },
}

describe('mutation mode', () => {
  test('requires exactly one local or delegated path', () => {
    expect(() => validateSyncHostConfig(base)).toThrow('exactly one')
    expect(() =>
      validateSyncHostConfig({ ...base, mutators: {}, mutateUrl: '/push' })
    ).toThrow('exactly one')
  })

  test('delegation requires a valid upstream binding', () => {
    expect(() => validateSyncHostConfig({ ...base, mutateUrl: '/push' })).toThrow(
      'requires upstream'
    )
    expect(() =>
      validateSyncHostConfig({
        ...base,
        mutateUrl: '/push',
        upstream: { binding: 'DATA', namespacePath: (namespace) => `/${namespace}` },
      })
    ).not.toThrow()
  })

  test('mutateBinding is static and only valid for delegated pushes', () => {
    expect(() =>
      validateSyncHostConfig({ ...base, mutators: {}, mutateBinding: 'APP' })
    ).toThrow('requires mutateUrl')
    expect(() =>
      validateSyncHostConfig({
        ...base,
        mutateUrl: '/push',
        mutateBinding: 'APP',
        upstream: { binding: 'DATA', namespacePath: '/data' },
      })
    ).not.toThrow()
  })

  test('forbids local mutators plus upstream ingest', () => {
    expect(() =>
      validateSyncHostConfig({
        ...base,
        mutators: {},
        upstream: { binding: 'DATA', namespacePath: '/data' },
      })
    ).toThrow('cannot combine')
  })
})
