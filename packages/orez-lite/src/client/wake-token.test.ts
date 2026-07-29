import { describe, expect, test, vi } from 'vitest'

import { createWakeTokenFetcher } from './transport.js'

describe('wake token fetcher', () => {
  test('supports cookie and JSON-body authentication', async () => {
    const fetch = vi.fn(async () => Response.json({ token: 'cookie-token' }))
    const getToken = createWakeTokenFetcher(
      'https://example.test/api/zero/wake-token',
      {
        credentials: 'include',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ namespace: 'soot' }),
      },
      fetch
    )

    await expect(getToken()).resolves.toBe('cookie-token')
    expect(fetch).toHaveBeenCalledWith('https://example.test/api/zero/wake-token', {
      method: 'POST',
      credentials: 'include',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ namespace: 'soot' }),
    })
  })

  test('resolves fresh bearer headers for each token request', async () => {
    let session = 'session-1'
    const fetch = vi.fn(async () => Response.json({ token: `${session}-wake` }))
    const getToken = createWakeTokenFetcher(
      'https://example.test/api/zero/proj-one/wake-token',
      () => ({
        credentials: 'include',
        headers: { authorization: `Bearer ${session}` },
      }),
      fetch
    )

    await expect(getToken()).resolves.toBe('session-1-wake')
    session = 'session-2'
    await expect(getToken()).resolves.toBe('session-2-wake')
    expect(fetch.mock.calls[1]?.[1]?.headers).toEqual({
      authorization: 'Bearer session-2',
    })
  })

  test('rejects failed and malformed responses', async () => {
    const failed = createWakeTokenFetcher(
      'https://example.test/token',
      {},
      vi.fn(async () => new Response(null, { status: 401 }))
    )
    await expect(failed()).rejects.toThrow('wake token request failed: 401')

    const malformed = createWakeTokenFetcher(
      'https://example.test/token',
      {},
      vi.fn(async () => Response.json({ token: null }))
    )
    await expect(malformed()).rejects.toThrow('wake token response is invalid')
  })
})
