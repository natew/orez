import { describe, expect, test, vi } from 'vitest'

import { DoBackend } from './pg-proxy-do-backend.js'

describe('DoBackend cancellation', () => {
  test('reports the exact initialization phase around an abort-ignoring request', async () => {
    const controller = new AbortController()
    const events: Array<Record<string, unknown>> = []
    let releaseMetadata!: () => void
    const metadataGate = new Promise<void>((resolve) => {
      releaseMetadata = resolve
    })
    let requestCount = 0
    const backendFetch = (async () => {
      requestCount++
      if (requestCount === 2) await metadataGate
      return Response.json({ rows: [], columns: [] })
    }) as typeof fetch
    const backend = new DoBackend(
      'https://orez-do-backend.local',
      'zero_cdb',
      'init-phase-test',
      {
        fetch: backendFetch,
        log: (event) => events.push(event),
        signal: controller.signal,
      }
    )
    const initializing = backend.waitReady.catch((error) => error)

    await vi.waitFor(() =>
      expect(events.at(-1)).toMatchObject({
        database: 'zero_cdb',
        event: 'do-backend-init-phase-start',
        phase: 'durable-metadata-table',
      })
    )

    controller.abort(new Error('startup deadline reached'))
    releaseMetadata()
    await expect(initializing).resolves.toBeInstanceOf(Error)
    await backend.close()
  })

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
    expect(signals[0]).not.toBe(controller.signal)
    expect(signals.every((signal) => signal === signals[0])).toBe(true)

    controller.abort(new Error('startup deadline reached'))
    await expect(backend.query('SELECT 1')).rejects.toThrow('startup deadline reached')
  })
})
