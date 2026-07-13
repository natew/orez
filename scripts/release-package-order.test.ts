import { describe, expect, it } from 'bun:test'

import { orderReleasePackages } from './release-package-order.js'

const pkg = (name: string, dependencies?: Record<string, string>) => ({
  pkg: { name, dependencies },
})

describe('orderReleasePackages', () => {
  it('publishes exact workspace dependencies before their consumers', () => {
    const packages = [
      pkg('orez', { 'bedrock-sqlite': 'workspace:*' }),
      pkg('bedrock-sqlite'),
      pkg('pg-to-sqlite'),
      pkg('orez-sync-cf-host'),
    ]

    expect(orderReleasePackages(packages).map((item) => item.pkg.name)).toEqual([
      'bedrock-sqlite',
      'orez',
      'pg-to-sqlite',
      'orez-sync-cf-host',
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
})
