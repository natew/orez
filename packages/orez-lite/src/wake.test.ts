import { afterEach, describe, expect, test, vi } from 'vitest'

import {
  mintWakeCapability,
  timingSafeEqual,
  verifySharedSecretHeader,
  verifyWakeCapability,
} from './wake.js'

const secret = '0123456789abcdef0123456789abcdef'

afterEach(() => vi.useRealTimers())

describe('wake capabilities', () => {
  test('mints and verifies a versioned namespace-scoped capability', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-07-20T00:00:00Z'))
    const minted = await mintWakeCapability(secret, {
      namespace: 'proj-one',
      identity: 'user-1',
      ttlMs: 60_000,
    })

    expect(minted.token.split('.')).toHaveLength(3)
    expect(minted.token.startsWith('v1.')).toBe(true)
    await expect(
      verifyWakeCapability(secret, minted.token, {
        namespace: 'proj-one',
        ttlMs: 60_000,
      })
    ).resolves.toEqual({
      namespace: 'proj-one',
      identity: 'user-1',
      expiresAt: Date.now() + 60_000,
    })
    await expect(
      verifyWakeCapability(secret, minted.token, {
        namespace: 'proj-two',
        ttlMs: 60_000,
      })
    ).resolves.toBeNull()
  })

  test('rejects expired, overlong-ttl, and tampered capabilities', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-07-20T00:00:00Z'))
    const { token } = await mintWakeCapability(secret, {
      namespace: 'soot',
      identity: 'anon-1',
      ttlMs: 60_000,
    })

    await expect(
      verifyWakeCapability(secret, token, { namespace: 'soot', ttlMs: 30_000 })
    ).resolves.toBeNull()
    const parts = token.split('.')
    const tampered = `${parts[0]}.${parts[1]}x.${parts[2]}`
    await expect(
      verifyWakeCapability(secret, tampered, { namespace: 'soot', ttlMs: 60_000 })
    ).resolves.toBeNull()

    vi.advanceTimersByTime(60_001)
    await expect(
      verifyWakeCapability(secret, token, { namespace: 'soot', ttlMs: 60_000 })
    ).resolves.toBeNull()
  })

  test('checks shared-secret headers without node crypto', () => {
    const authorized = new Request('https://example.test/callback', {
      headers: { 'x-callback-secret': 'expected-secret' },
    })
    expect(
      verifySharedSecretHeader(authorized, 'expected-secret', 'x-callback-secret')
    ).toBe(true)
    expect(
      verifySharedSecretHeader(authorized, 'different-secret', 'x-callback-secret')
    ).toBe(false)
    expect(timingSafeEqual('same', 'same')).toBe(true)
    expect(timingSafeEqual('same', 'short')).toBe(false)
  })
})
