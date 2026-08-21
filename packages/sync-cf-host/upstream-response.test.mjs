import { describe, expect, test } from 'bun:test'

import {
  fetchBoundedUpstreamJson,
  readBoundedJsonResponse,
  UpstreamResponseLimitError,
} from './src/upstream-response.ts'

describe('bounded upstream responses', () => {
  test('rejects a declared oversized body before reading it', async () => {
    const response = new Response('{"ok":true}', {
      headers: { 'content-length': '999' },
    })
    await expect(
      readBoundedJsonResponse(response, 10, new AbortController().signal)
    ).rejects.toBeInstanceOf(UpstreamResponseLimitError)
  })

  test('rejects a chunked body as soon as it crosses the byte limit', async () => {
    const response = new Response(
      new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode('{"value":"'))
          controller.enqueue(new TextEncoder().encode('too-wide"}'))
          controller.close()
        },
      })
    )
    await expect(
      readBoundedJsonResponse(response, 12, new AbortController().signal)
    ).rejects.toThrow('exceeded 12 bytes')
  })

  test('the deadline covers a stalled successful response body', async () => {
    const binding = {
      async fetch() {
        return new Response(new ReadableStream({ start() {} }))
      },
    }
    await expect(
      fetchBoundedUpstreamJson(
        binding,
        'https://upstream.invalid/changes',
        {},
        {
          timeoutMs: 10,
          maxBytes: 1024,
        }
      )
    ).rejects.toThrow('missed 10ms deadline')
  })

  test('returns raw timing stamps around a valid body', async () => {
    const times = [100, 140]
    const result = await fetchBoundedUpstreamJson(
      { fetch: async () => Response.json({ watermark: 0, changes: [] }) },
      'https://upstream.invalid/changes',
      {},
      { timeoutMs: 100, maxBytes: 1024, now: () => times.shift() }
    )
    expect(result.body).toEqual({ watermark: 0, changes: [] })
    expect(result).toMatchObject({ sendTimeMs: 100, receiveTimeMs: 140 })
  })

  test('cancels an error response body without waiting for it to stream', async () => {
    let canceled = false
    const result = await fetchBoundedUpstreamJson(
      {
        fetch: async () =>
          new Response(
            new ReadableStream({
              cancel() {
                canceled = true
              },
            }),
            { status: 503 }
          ),
      },
      'https://upstream.invalid/changes',
      {},
      { timeoutMs: 100, maxBytes: 1024 }
    )
    expect(result.response.status).toBe(503)
    expect(result.body).toBeNull()
    expect(canceled).toBe(true)
  })
})
