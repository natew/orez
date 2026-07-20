import { describe, expect, it } from 'bun:test'

import {
  orderReleasePackages,
  selectLocalReleasePackages,
} from './release-package-order.js'

const pkg = (name: string, dependencies?: Record<string, string>) => ({
  pkg: { name, dependencies },
})

const optionalPkg = (name: string, optionalDependencies?: Record<string, string>) => ({
  pkg: { name, optionalDependencies },
})

describe('orderReleasePackages', () => {
  it('publishes exact workspace dependencies before their consumers', () => {
    const packages = [
      pkg('orez', {
        'bedrock-sqlite': 'workspace:*',
        'orez-sync-cf-host': 'workspace:*',
      }),
      pkg('bedrock-sqlite'),
      pkg('pg-to-sqlite'),
      pkg('orez-sync-cf-host'),
    ]

    expect(orderReleasePackages(packages).map((item) => item.pkg.name)).toEqual([
      'bedrock-sqlite',
      'orez-sync-cf-host',
      'orez',
      'pg-to-sqlite',
    ])
  })

  it('fails closed on a package dependency cycle', () => {
    expect(() =>
      orderReleasePackages([
        pkg('a', { b: 'workspace:*' }),
        pkg('b', { a: 'workspace:*' }),
      ])
    ).toThrow('release package dependency cycle')
  })

  it('adds a missing local dependency required by an installed package', () => {
    const packages = [
      pkg('orez', { 'orez-sync-cf-host': 'workspace:*' }),
      pkg('orez-sync-cf-host'),
      pkg('unrelated'),
    ]

    expect(
      selectLocalReleasePackages(packages, new Set(['orez'])).map((item) => item.pkg.name)
    ).toEqual(['orez-sync-cf-host', 'orez'])
  })

  it('orders optional platform packages before their launcher', () => {
    const packages = [
      optionalPkg('orez-sync-native', {
        'orez-sync-native-darwin-arm64': 'workspace:*',
      }),
      optionalPkg('orez-sync-native-darwin-arm64'),
    ]

    expect(orderReleasePackages(packages).map((item) => item.pkg.name)).toEqual([
      'orez-sync-native-darwin-arm64',
      'orez-sync-native',
    ])
  })
})
