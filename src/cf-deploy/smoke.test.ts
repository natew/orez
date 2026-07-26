import { afterEach, describe, expect, test, vi } from 'vitest'

import {
  EMBED_READY_TIMEOUT_MS,
  EMBED_WARM_INTERVAL_MS,
  EMBED_WARM_TIMEOUT_MS,
} from './leaves.js'
import { pollWorkerReady, warmZeroCacheEmbed } from './smoke'

describe('warmZeroCacheEmbed', () => {
  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  test('shares a fifteen-minute deployment budget with the native embed', () => {
    expect(EMBED_READY_TIMEOUT_MS).toBe(600_000)
    expect(EMBED_WARM_TIMEOUT_MS).toBe(900_000)
  })

  test('fails the deployment when the native embed misses its readiness budget', async () => {
    vi.spyOn(Date, 'now')
      .mockReturnValueOnce(0)
      .mockReturnValue(EMBED_WARM_TIMEOUT_MS + 1)
    const fetchMock = vi.spyOn(globalThis, 'fetch')

    await expect(warmZeroCacheEmbed('https://example.test', vi.fn())).rejects.toThrow(
      `did not become ready within ${EMBED_WARM_TIMEOUT_MS / 1000}s`
    )
    expect(fetchMock).not.toHaveBeenCalled()
  })

  test('keeps polling after the inner readiness budget while setup headroom remains', async () => {
    vi.useFakeTimers()
    vi.spyOn(Date, 'now')
      .mockReturnValueOnce(0)
      .mockReturnValueOnce(EMBED_READY_TIMEOUT_MS)
      .mockReturnValueOnce(EMBED_READY_TIMEOUT_MS + EMBED_WARM_INTERVAL_MS)
    const fetchMock = vi
      .spyOn(globalThis, 'fetch')
      .mockResolvedValueOnce(new Response(null, { status: 202 }))
      .mockResolvedValueOnce(new Response(null, { status: 200 }))

    const readiness = warmZeroCacheEmbed('https://example.test', vi.fn())
    await vi.runAllTimersAsync()
    await expect(readiness).resolves.toBeUndefined()
    expect(fetchMock).toHaveBeenCalledTimes(2)
    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      'https://example.test/keepalive?deploy=1',
      expect.any(Object)
    )
  })

  test('accepts a ready deploy probe without retrying', async () => {
    const fetchMock = vi
      .spyOn(globalThis, 'fetch')
      .mockResolvedValue(new Response('ready', { status: 200 }))

    await expect(
      warmZeroCacheEmbed('https://example.test', vi.fn())
    ).resolves.toBeUndefined()
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  test('starts exactly one fresh boot for a data-worker replacement', async () => {
    const fetchMock = vi
      .spyOn(globalThis, 'fetch')
      .mockResolvedValueOnce(new Response('booting', { status: 202 }))
      .mockResolvedValueOnce(new Response('ready', { status: 200 }))

    await expect(
      warmZeroCacheEmbed('https://example.test', vi.fn(), { startFresh: true })
    ).resolves.toBeUndefined()
    expect(fetchMock).toHaveBeenCalledTimes(2)
    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      'https://example.test/keepalive',
      expect.any(Object)
    )
    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      'https://example.test/keepalive?deploy=1',
      expect.any(Object)
    )
  })

  test('aborts immediately after the embed reports a persisted boot failure', async () => {
    const fetchMock = vi
      .spyOn(globalThis, 'fetch')
      .mockResolvedValue(
        Response.json(
          { status: 'boot-failed', failures: 1, reason: 'replica rank mismatch' },
          { status: 409 }
        )
      )

    await expect(warmZeroCacheEmbed('https://example.test', vi.fn())).rejects.toThrow(
      'zero-cache embed boot failed after 1 attempt: replica rank mismatch; deploy aborted to avoid restarting initial sync'
    )
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  test('bounds malformed terminal signals and transient responses', async () => {
    vi.useFakeTimers()
    vi.spyOn(Date, 'now')
      .mockReturnValueOnce(0)
      .mockReturnValueOnce(0)
      .mockReturnValueOnce(1)
      .mockReturnValue(EMBED_WARM_TIMEOUT_MS + 1)
    const fetchMock = vi
      .spyOn(globalThis, 'fetch')
      .mockResolvedValueOnce(
        Response.json({ status: 'boot-failed', failures: 'not-a-count' }, { status: 409 })
      )
      .mockResolvedValueOnce(new Response('unavailable', { status: 503 }))

    const readiness = expect(
      warmZeroCacheEmbed('https://example.test', vi.fn())
    ).rejects.toThrow(
      `zero-cache embed did not become ready within ${EMBED_WARM_TIMEOUT_MS / 1000}s`
    )
    await vi.runAllTimersAsync()
    await readiness
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  test('does not retry worker health after the native embed times out', async () => {
    vi.useFakeTimers()
    let now = -(EMBED_WARM_TIMEOUT_MS + 1)
    vi.spyOn(Date, 'now').mockImplementation(() => (now += EMBED_WARM_TIMEOUT_MS + 1))
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockImplementation(async (input) => {
      if (String(input).endsWith('/version.json')) {
        return Response.json({ version: 'expected' })
      }
      return new Response(null, { status: 200 })
    })

    const readiness = expect(
      pollWorkerReady({
        url: 'https://example.test',
        expectedVersion: 'expected',
        workerName: 'example',
        log: vi.fn(),
      })
    ).rejects.toThrow(
      `zero-cache embed did not become ready within ${EMBED_WARM_TIMEOUT_MS / 1000}s`
    )
    await vi.runAllTimersAsync()
    await readiness
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  test('does not probe the removed embed for a Rust-host app', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockImplementation(async (input) => {
      if (String(input).endsWith('/version.json')) {
        return Response.json({ version: 'expected' })
      }
      return new Response(null, { status: 200 })
    })

    await pollWorkerReady({
      url: 'https://example.test',
      expectedVersion: 'expected',
      workerName: 'example',
      warmZeroCache: false,
      log: vi.fn(),
    })

    expect(fetchMock).toHaveBeenCalledTimes(2)
    expect(fetchMock).not.toHaveBeenCalledWith(
      'https://example.test/keepalive?deploy=1',
      expect.anything()
    )
  })
})
