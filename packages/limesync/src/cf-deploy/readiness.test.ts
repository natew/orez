import { afterEach, expect, test, vi } from 'vitest'

import { waitForWorkerReady } from './readiness.js'

afterEach(() => {
  vi.restoreAllMocks()
})

test('waits for the deployed worker and matching asset version', async () => {
  const fetchMock = vi.spyOn(globalThis, 'fetch').mockImplementation(async (input) => {
    if (String(input).endsWith('/version.json')) {
      return Response.json({ version: 'expected' })
    }
    return new Response(null, { status: 200 })
  })

  await waitForWorkerReady({
    url: 'https://example.test',
    expectedVersion: 'expected',
    workerName: 'example',
    log: vi.fn(),
  })

  expect(fetchMock).toHaveBeenCalledTimes(6)
})

test('restarts the stability count when an old edge appears', async () => {
  const versions = ['expected', 'old', 'expected', 'expected', 'expected']
  const fetchMock = vi.spyOn(globalThis, 'fetch').mockImplementation(async (input) => {
    if (String(input).endsWith('/version.json')) {
      return Response.json({ version: versions.shift() })
    }
    return new Response(null, { status: 200 })
  })

  await waitForWorkerReady({
    url: 'https://example.test',
    expectedVersion: 'expected',
    workerName: 'example',
    log: vi.fn(),
    intervalMs: 0,
  })

  expect(fetchMock).toHaveBeenCalledTimes(10)
})

test('fails after the configured readiness attempts', async () => {
  vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response(null, { status: 503 }))

  await expect(
    waitForWorkerReady({
      url: 'https://example.test',
      expectedVersion: 'expected',
      workerName: 'example',
      log: vi.fn(),
      intervalMs: 0,
      maxAttempts: 2,
    })
  ).rejects.toThrow(Error)
})
