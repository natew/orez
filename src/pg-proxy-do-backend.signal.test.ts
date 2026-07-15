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

  test('close aborts and joins initialization before it resolves', async () => {
    let releaseFetch!: () => void
    const fetchGate = new Promise<void>((resolve) => {
      releaseFetch = resolve
    })
    let fetchCalls = 0
    const backendFetch = (async () => {
      fetchCalls++
      await fetchGate
      return Response.json({ rows: [], columns: [] })
    }) as typeof fetch
    const backend = new DoBackend(
      'https://orez-do-backend.local',
      'zero_cdb',
      'close-init-test',
      { fetch: backendFetch }
    )
    const initializing = backend.waitReady.catch((error) => error)
    await vi.waitFor(() => expect(fetchCalls).toBe(1))

    let closeSettled = false
    const closing = backend.close().then(() => {
      closeSettled = true
    })
    await Promise.resolve()
    expect(closeSettled).toBe(false)

    releaseFetch()
    const initError = await initializing
    await closing

    expect(initError).toBeInstanceOf(Error)
    expect(String(initError)).toContain('closed')
    expect(fetchCalls).toBe(1)
    expect(backend.ready).toBe(false)
  })

  test('close joins remote rollback through a non-aborted cleanup request', async () => {
    let releaseRollback!: () => void
    const rollbackGate = new Promise<void>((resolve) => {
      releaseRollback = resolve
    })
    let rollbackSignal: AbortSignal | null | undefined
    const paths: string[] = []
    const backendFetch = (async (input, init) => {
      const url = new URL(
        typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      )
      paths.push(url.pathname)
      if (url.pathname === '/rollback-tx') {
        rollbackSignal = init?.signal
        await rollbackGate
      }
      return Response.json({ rows: [], columns: [], affectedRows: 1 })
    }) as typeof fetch
    const backend = new DoBackend(
      'https://orez-do-backend.local',
      'zero_cdb',
      'close-rollback-test',
      { fetch: backendFetch }
    )
    await backend.waitReady
    await backend.exec('BEGIN')
    await backend.exec('INSERT INTO message (id) VALUES (1)')

    let closeSettled = false
    const closing = backend.close().then(() => {
      closeSettled = true
    })
    await vi.waitFor(() => expect(paths).toContain('/rollback-tx'))

    expect(rollbackSignal).toBeUndefined()
    expect(closeSettled).toBe(false)

    releaseRollback()
    await closing
    expect(closeSettled).toBe(true)
  })

  test('close rejects when remote rollback cleanup rejects', async () => {
    const rollbackError = new Error('rollback request failed')
    const paths: string[] = []
    const backendFetch = (async (input) => {
      const url = new URL(
        typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      )
      paths.push(url.pathname)
      if (url.pathname === '/rollback-tx') throw rollbackError
      return Response.json({ rows: [], columns: [], affectedRows: 1 })
    }) as typeof fetch
    const backend = new DoBackend(
      'https://orez-do-backend.local',
      'zero_cdb',
      'close-rollback-rejection-test',
      { fetch: backendFetch }
    )
    await backend.waitReady
    await backend.exec('BEGIN')
    await backend.exec('INSERT INTO message (id) VALUES (1)')

    await expect(backend.close()).rejects.toBe(rollbackError)
    expect(paths).toContain('/rollback-tx')
    expect(backend.closed).toBe(true)
  })
})
