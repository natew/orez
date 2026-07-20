import { describe, expect, test } from 'bun:test'

import { validatePullCaps, validateSyncHostConfig } from './src/config.ts'

describe('sync host config', () => {
  test('rejects a zero row cap that would stall cursor progress', () => {
    expect(() =>
      validatePullCaps({ maxChangeRows: 0, maxChangeBytes: 2_000_000 })
    ).toThrow('positive safe integer')
  })

  test('validates transaction query execution budgets', () => {
    expect(() =>
      validateSyncHostConfig({
        ...base,
        mutators: {},
        transactionQueryBudget: { maxSelects: 0 },
      })
    ).toThrow('transactionQueryBudget.maxSelects')
    expect(() =>
      validateSyncHostConfig({
        ...base,
        mutators: {},
        transactionQueryBudget: { maxRows: 500 },
      })
    ).not.toThrow()
  })
})

const base = {
  hostVersion: 'test',
  schema: { tables: {}, relationships: {} },
  initialize() {},
  authenticate() {
    return { userID: 'u' }
  },
  authorize() {
    return true
  },
  authorizeWake() {
    return true
  },
  authorizeNotify() {
    return true
  },
  namespace() {
    return 'n'
  },
}

describe('mutation mode', () => {
  test('requires explicit wake and notify capabilities', () => {
    expect(() =>
      validateSyncHostConfig({ ...base, authorize: undefined, mutators: {} })
    ).toThrow('authorize is required')
    expect(() =>
      validateSyncHostConfig({ ...base, authorizeWake: undefined, mutators: {} })
    ).toThrow('authorizeWake is required')
    expect(() =>
      validateSyncHostConfig({ ...base, authorizeNotify: undefined, mutators: {} })
    ).toThrow('authorizeNotify is required')
  })

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

  test('mutateOrigin is an http(s) origin only for delegated pushes', () => {
    expect(() =>
      validateSyncHostConfig({
        ...base,
        mutators: {},
        mutateOrigin: 'https://app.example.com',
      })
    ).toThrow('requires mutateUrl')
    for (const mutateOrigin of [
      'app.example.com',
      'ftp://app.example.com',
      'https://app.example.com/path',
      'https://app.example.com?query=1',
      'https://app.example.com#hash',
      'https://app.example.com/',
    ]) {
      expect(() =>
        validateSyncHostConfig({
          ...base,
          mutateUrl: '/push',
          mutateOrigin,
          upstream: { binding: 'DATA', namespacePath: '/data' },
        })
      ).toThrow('absolute http(s) origin')
    }
    expect(() =>
      validateSyncHostConfig({
        ...base,
        mutateUrl: '/push',
        mutateOrigin: 'https://app.example.com',
        upstream: { binding: 'DATA', namespacePath: '/data' },
      })
    ).not.toThrow()
  })

  test('validates ingest budgets and delegated retry bounds', () => {
    expect(() =>
      validateSyncHostConfig({
        ...base,
        mutateUrl: '/push',
        upstream: { binding: 'DATA', namespacePath: '/', ingestBudgetRows: 0 },
      })
    ).toThrow('upstream.ingestBudgetRows')
    expect(() =>
      validateSyncHostConfig({
        ...base,
        mutateUrl: '/push',
        upstream: { binding: 'DATA', namespacePath: '/' },
        delegatedPushRetry: { maxAttempts: 0 },
      })
    ).toThrow('delegatedPushRetry.maxAttempts')
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
