import { SyncExecutorRequestError } from 'orez-sync-executor'
import { describe, expect, test, vi } from 'vitest'

import { createZeroHttpMount, ZeroHttpRequestError } from './mount.js'

describe('zero-http request mount', () => {
  test('matches glued database prefixes', () => {
    const mount = createZeroHttpMount({
      pathPrefix: '/p-',
      authenticate: () => ({ userID: 'user-1' }),
      server: () => ({
        handlePull: vi.fn(),
        handlePush: vi.fn(),
      }),
    })

    expect(mount.match('/p-project_1/pull')).toEqual({
      databaseID: 'project_1',
      operation: 'pull',
    })
    expect(mount.match('/p-project_1/push')).toEqual({
      databaseID: 'project_1',
      operation: 'push',
    })
    expect(mount.match('/p-/pull')).toBeNull()
  })

  test('runs authentication and push policy before resolving the server', async () => {
    const order: string[] = []
    const handlePush = vi.fn(async () => ({ pushResponse: { mutations: [] } }))
    const mount = createZeroHttpMount({
      pathPrefix: '/p-',
      async authenticate(_request, route) {
        order.push(`authenticate:${route.databaseID}`)
        order.push('access')
        order.push('provision')
        return { userID: 'user-1' }
      },
      async beforePush(_request, bodyText) {
        order.push(`beforePush:${JSON.parse(bodyText).pushVersion}`)
        return null
      },
      server(databaseID) {
        order.push(`server:${databaseID}`)
        return { handlePull: vi.fn(), handlePush }
      },
    })

    const response = await mount.handleRequest(
      new Request('https://example.test/p-project-1/push', {
        method: 'POST',
        body: JSON.stringify({ pushVersion: 1 }),
      })
    )

    expect(response?.status).toBe(200)
    await expect(response?.json()).resolves.toEqual({
      pushResponse: { mutations: [] },
    })
    expect(order).toEqual([
      'authenticate:project-1',
      'access',
      'provision',
      'beforePush:1',
      'server:project-1',
    ])
    expect(handlePush).toHaveBeenCalledWith({ pushVersion: 1 }, { userID: 'user-1' })
  })

  test('supports fixed mounts and short-circuits denied authentication', async () => {
    const server = vi.fn()
    const mount = createZeroHttpMount({
      pathPrefix: '/zero-http/',
      databaseID: 'control',
      authenticate: () => Response.json({ error: 'denied' }, { status: 401 }),
      server,
    })

    const response = await mount.handleRequest(
      new Request('https://example.test/zero-http/pull', {
        method: 'POST',
        body: '{}',
      })
    )

    expect(response?.status).toBe(401)
    expect(server).not.toHaveBeenCalled()
    await expect(
      mount.handleRequest(
        new Request('https://example.test/elsewhere/pull', {
          method: 'POST',
          body: '{}',
        })
      )
    ).resolves.toBeNull()
  })

  test('maps request errors and disables pull caching', async () => {
    const handlePull = vi
      .fn()
      .mockRejectedValueOnce(new ZeroHttpRequestError(409, 'future cookie'))
      .mockRejectedValueOnce(new SyncExecutorRequestError(403, 'wrong user'))
      .mockResolvedValueOnce({ cookie: 1, rowsPatch: [] })
    const mount = createZeroHttpMount({
      pathPrefix: '/sync/',
      authenticate: () => ({ userID: 'user-1' }),
      server: () => ({ handlePull, handlePush: vi.fn() }),
    })
    const request = () =>
      new Request('https://example.test/sync/app/pull', {
        method: 'POST',
        body: '{}',
      })

    const future = await mount.handleRequest(request())
    expect(future?.status).toBe(409)
    await expect(future?.json()).resolves.toEqual({ error: 'future cookie' })

    const wrongUser = await mount.handleRequest(request())
    expect(wrongUser?.status).toBe(403)
    await expect(wrongUser?.json()).resolves.toEqual({ error: 'wrong user' })

    const success = await mount.handleRequest(request())
    expect(success?.headers.get('cache-control')).toBe('no-store')

    const invalid = await mount.handleRequest(
      new Request('https://example.test/sync/app/pull', {
        method: 'POST',
        body: '{',
      })
    )
    expect(invalid?.status).toBe(400)
    await expect(invalid?.json()).resolves.toEqual({ error: 'invalid pull body' })
  })
})
