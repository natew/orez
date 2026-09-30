import { expect, test } from 'bun:test'
import { createHash } from 'node:crypto'

import { verifyNpmRelease } from './verify-npm-release.js'

type State =
  | 'queued'
  | 'missing-tarball'
  | 'corrupt'
  | 'wrong-version'
  | 'wrong-tag'
  | 'ready'

function registryFixture(stateOf: (name: string, request: number) => State) {
  const requests = new Map<string, number>()
  const tarballs = new Map<string, number>()
  const server = Bun.serve({
    port: 0,
    fetch(request) {
      const url = new URL(request.url)
      const name = decodeURIComponent(url.pathname.slice(1))
      if (url.pathname.startsWith('/tarball/')) {
        const pkg = name.slice('tarball/'.length)
        tarballs.set(pkg, (tarballs.get(pkg) ?? 0) + 1)
        const state = stateOf(pkg, requests.get(pkg)!)
        if (state === 'missing-tarball') return new Response('not found', { status: 404 })
        return new Response(state === 'corrupt' ? 'damaged bytes' : `tarball for ${pkg}`)
      }
      expect(request.headers.get('cache-control')).toBe('no-cache')
      expect(request.headers.get('accept')).toBe('application/vnd.npm.install-v1+json')
      expect(url.searchParams.has('release-check')).toBe(true)
      const count = (requests.get(name) ?? 0) + 1
      requests.set(name, count)
      const state = stateOf(name, count)
      if (state === 'queued') return new Response('not found', { status: 404 })
      return Response.json({
        'dist-tags': { canary: state === 'wrong-tag' ? '0.0.0' : '1.0.0-canary.1' },
        versions: {
          '1.0.0-canary.1': {
            name,
            version: state === 'wrong-version' ? '0.0.0' : '1.0.0-canary.1',
            dist: {
              tarball: `${url.origin}/tarball/${encodeURIComponent(name)}`,
              shasum: createHash('sha1').update(`tarball for ${name}`).digest('hex'),
            },
          },
        },
      })
    },
  })
  return { server, registry: server.url.origin, requests, tarballs }
}

test('waits for both queued versions and delayed tarballs, checking ready packages once', async () => {
  const fixture = registryFixture((name, request) => {
    if (name === '@orez/queued' && request === 1) return 'queued'
    if (name === '@orez/queued' && request === 2) return 'missing-tarball'
    return 'ready'
  })
  try {
    await verifyNpmRelease(
      ['ready', '@orez/queued'].map((name) => ({ name, version: '1.0.0-canary.1' })),
      {
        registry: fixture.registry,
        tag: 'canary',
        timeoutMs: 1000,
        retryDelayMs: 1,
      }
    )
    expect(fixture.requests.get('ready')).toBe(1)
    expect(fixture.tarballs.get('ready')).toBe(1)
    expect(fixture.requests.get('@orez/queued')).toBe(3)
    expect(fixture.tarballs.get('@orez/queued')).toBe(2)
  } finally {
    fixture.server.stop(true)
  }
})

for (const [state, reason] of [
  ['queued', 'package index returned HTTP 404'],
  ['missing-tarball', 'tarball returned HTTP 404'],
  ['corrupt', 'tarball checksum mismatch'],
  ['wrong-version', 'missing from the package index'],
  ['wrong-tag', 'canary points to 0.0.0'],
] as const) {
  test(`fails a release when ${state} never becomes available`, async () => {
    const fixture = registryFixture(() => state)
    try {
      await expect(
        verifyNpmRelease([{ name: 'unavailable', version: '1.0.0-canary.1' }], {
          registry: fixture.registry,
          tag: 'canary',
          timeoutMs: 50,
          retryDelayMs: 5,
        })
      ).rejects.toThrow(`unavailable@1.0.0-canary.1: ${reason}`)
      expect(fixture.requests.get('unavailable')).toBeGreaterThan(0)
    } finally {
      fixture.server.stop(true)
    }
  })
}

test('a stalled registry request cannot outlive the release deadline', async () => {
  let requested = false
  const server = Bun.serve({
    port: 0,
    fetch() {
      requested = true
      return new Promise<Response>(() => {})
    },
  })
  try {
    await expect(
      verifyNpmRelease([{ name: 'stalled', version: '1.0.0' }], {
        registry: server.url.origin,
        timeoutMs: 50,
        retryDelayMs: 5,
      })
    ).rejects.toThrow('stalled@1.0.0:')
    expect(requested).toBe(true)
  } finally {
    server.stop(true)
  }
})
