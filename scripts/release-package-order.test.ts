import { describe, expect, it } from 'bun:test'

import {
  assertLocalReleaseVersions,
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
      pkg('orez-sync-cf-host'),
    ]

    expect(orderReleasePackages(packages).map((item) => item.pkg.name)).toEqual([
      'bedrock-sqlite',
      'orez-sync-cf-host',
      'orez',
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

describe('assertLocalReleaseVersions', () => {
  it('refuses to replace an installed package with a different local version', () => {
    expect(() =>
      assertLocalReleaseVersions([
        {
          pkg: { name: 'on-zero', version: '0.12.5' },
          copies: [{ dir: '/app/node_modules/on-zero', version: '0.12.6' }],
        },
      ])
    ).toThrow('on-zero: --into would replace installed 0.12.6 with local 0.12.5')
  })

  it('allows local source to replace the same installed version', () => {
    expect(() =>
      assertLocalReleaseVersions([
        {
          pkg: { name: 'on-zero', version: '0.12.6' },
          copies: [{ dir: '/app/node_modules/on-zero', version: '0.12.6' }],
        },
      ])
    ).not.toThrow()
  })
})
