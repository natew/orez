import { createSchema, string, table } from '@rocicorp/zero'
import { createZeroClientTransport } from 'orez-lite/client'
import { expect, test, vi } from 'vitest'

import { createZeroClient } from './createZeroClient'

const noteTable = table('note').columns({ id: string() }).primaryKey('id')
const schema = createSchema({ tables: [noteTable] })

function response(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  })
}

async function eventually(assertion: () => void, timeoutMs = 2_000) {
  const deadline = Date.now() + timeoutMs
  let lastError: unknown
  while (Date.now() < deadline) {
    try {
      assertion()
      return
    } catch (error) {
      lastError = error
      await new Promise((resolve) => setTimeout(resolve, 5))
    }
  }
  throw lastError
}

test('unchanged unauthorized auth parks after one real HTTP transport request', async () => {
  const authorizations: Array<string | null> = []
  const fetch = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
    authorizations.push(new Headers(init?.headers).get('authorization'))
    return response({ error: 'missing authentication' }, 401)
  })
  const refreshAuth = vi.fn(async () => 'expired-token')
  const client = createZeroClient({
    schema,
    models: {},
    groupedQueries: {},
    instanceName: 'http-auth-unchanged',
  })
  const connection = client.connectHeadless({
    userID: 'unauthorized-user',
    auth: 'expired-token',
    cacheURL: 'https://http-auth-unchanged.orez.test/zero',
    kvStore: 'mem',
    transport: createZeroClientTransport({ fetch }),
    refreshAuth,
  })

  try {
    await eventually(() =>
      expect(connection.zero.connection.state.current.name).toBe('needs-auth')
    )
    await new Promise((resolve) => setTimeout(resolve, 100))

    expect(authorizations).toEqual(['Bearer expired-token'])
    expect(refreshAuth).toHaveBeenCalledTimes(1)
    expect(connection.zero.connection.state.current.name).toBe('needs-auth')
  } finally {
    await connection.close()
  }
})

test('warm headless client refreshes an expired bearer and reconnects', async () => {
  const authorizations: Array<string | null> = []
  const fetch = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
    const authorization = new Headers(init?.headers).get('authorization')
    authorizations.push(authorization)
    if (authorization === 'Bearer expired-token') {
      return response({ error: 'expired bearer' }, 401)
    }
    return response({ cookie: 1, lastMutationIDChanges: {}, rowsPatch: [] })
  })
  const refreshAuth = vi.fn(async () => 'fresh-token')
  const client = createZeroClient({
    schema,
    models: {},
    groupedQueries: {},
    instanceName: 'http-auth-refresh',
  })
  const connection = client.connectHeadless({
    userID: 'authorized-user',
    auth: 'expired-token',
    cacheURL: 'https://http-auth-refresh.orez.test/zero',
    kvStore: 'mem',
    transport: createZeroClientTransport({ fetch }),
    refreshAuth,
  })

  try {
    await eventually(() =>
      expect(connection.zero.connection.state.current.name).toBe('connected')
    )

    expect(refreshAuth).toHaveBeenCalledTimes(1)
    expect(authorizations).toEqual(['Bearer expired-token', 'Bearer fresh-token'])
  } finally {
    await connection.close()
  }
})

test('three rejected replacement bearers leave a real HTTP client parked', async () => {
  const authorizations: Array<string | null> = []
  let replacement = 0
  const fetch = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
    authorizations.push(new Headers(init?.headers).get('authorization'))
    return response({ error: 'project access required' }, 403)
  })
  const refreshAuth = vi.fn(async () => `replacement-token-${++replacement}`)
  const client = createZeroClient({
    schema,
    models: {},
    groupedQueries: {},
    instanceName: 'http-auth-bounded',
  })
  const connection = client.connectHeadless({
    userID: 'unauthorized-user',
    auth: 'expired-token',
    cacheURL: 'https://http-auth-bounded.orez.test/zero',
    kvStore: 'mem',
    transport: createZeroClientTransport({ fetch }),
    refreshAuth,
  })

  try {
    await eventually(() => expect(authorizations).toHaveLength(4))
    await new Promise((resolve) => setTimeout(resolve, 100))

    expect(authorizations).toEqual([
      'Bearer expired-token',
      'Bearer replacement-token-1',
      'Bearer replacement-token-2',
      'Bearer replacement-token-3',
    ])
    expect(refreshAuth).toHaveBeenCalledTimes(3)
    expect(connection.zero.connection.state.current.name).toBe('needs-auth')
  } finally {
    await connection.close()
  }
})
