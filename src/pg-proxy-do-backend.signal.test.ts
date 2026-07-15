import { describe, expect, test } from 'vitest'

import { DoBackend } from './pg-proxy-do-backend.js'

describe('DoBackend cancellation', () => {
  test('threads one signal through every durable object request', async () => {
    const controller = new AbortController()
    const signals: Array<AbortSignal | null | undefined> = []
    const backendFetch = (async (_input, init) => {
      signals.push(init?.signal)
      if (init?.signal?.aborted) throw init.signal.reason
      return Response.json({ rows: [], columns: [] })
    }) as typeof fetch
    const backend = new DoBackend(
      'https://orez-do-backend.local',
      'zero_cdb',
      'abort-signal-test',
      { fetch: backendFetch, signal: controller.signal }
    )

    await backend.waitReady
    expect(signals.length).toBeGreaterThan(0)
    expect(signals.every((signal) => signal === controller.signal)).toBe(true)

    controller.abort(new Error('startup deadline reached'))
    await expect(backend.query('SELECT 1')).rejects.toThrow('startup deadline reached')
  })
})
