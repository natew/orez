import { describe, expect, test, vi } from 'vitest'

import { reportPushDiagnostics, summarizePushRequest } from './diagnostics.js'

describe('push diagnostics', () => {
  test('summarizes only allowlisted scalar mutation arguments', () => {
    const request = new Request('https://example.test/push?appID=chat&schema=chat_0')
    const summary = summarizePushRequest(
      request,
      JSON.stringify({
        clientGroupID: 'group-1',
        requestID: 'request-1',
        pushVersion: 1,
        mutations: [
          {
            id: 4,
            clientID: 'client-1',
            name: 'message.create',
            type: 'custom',
            args: [{ threadId: 'thread-1', secret: 'do-not-log', nested: {} }],
          },
        ],
      }),
      ['threadId', 'secretObject']
    )

    expect(summary).toEqual({
      url: request.url,
      appID: 'chat',
      schema: 'chat_0',
      clientGroupID: 'group-1',
      requestID: 'request-1',
      pushVersion: 1,
      mutationCount: 1,
      mutations: [
        {
          id: 4,
          clientID: 'client-1',
          name: 'message.create',
          type: 'custom',
          argSummary: 'threadId=thread-1',
        },
      ],
    })
  })

  test('reports response failures and per-mutation application errors together', async () => {
    const callback = vi.fn()
    const request = new Request('https://example.test/push')
    const bodyText = JSON.stringify({
      mutations: [
        {
          id: 2,
          clientID: 'client-1',
          name: 'message.create',
          args: [{ threadId: 'thread-1' }],
        },
      ],
    })

    await reportPushDiagnostics(
      { argAllowlist: ['threadId'], callback },
      {
        request,
        bodyText,
        response: {
          mutations: [
            {
              id: { id: 2, clientID: 'client-1' },
              result: {
                error: 'app',
                message: 'denied',
                details: { name: 'AccessError' },
              },
            },
          ],
        },
      }
    )

    expect(callback).toHaveBeenCalledWith({
      request: expect.objectContaining({
        mutationCount: 1,
        mutations: [expect.objectContaining({ argSummary: 'threadId=thread-1' })],
      }),
      failure: null,
      mutationErrors: [
        {
          id: 2,
          clientID: 'client-1',
          error: 'app',
          message: 'denied',
          detailsName: 'AccessError',
        },
      ],
    })
  })
})
