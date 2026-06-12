import { Zero } from '@rocicorp/zero'
import { afterEach, describe, expect, test, vi } from 'vitest'

import { zeroHttpFixtureMutators, zeroHttpFixtureSchema } from './fixture-schema.js'
import { installZeroHttpTransport } from './transport.js'

const ORIGIN = 'https://zero-http.local'

type RequestRecord = {
  url: string
  path: string
  headers: Record<string, string>
  body: any
}

const zeros: Zero<any, any>[] = []
const transports: Array<{ uninstall(): void }> = []
let storageID = 0

afterEach(async () => {
  while (zeros.length) await zeros.pop()?.close()
  while (transports.length) transports.pop()?.uninstall()
  vi.useRealTimers()
})

describe('zero-http transport', () => {
  test('connect + complete hydrates a stock Zero materialized query', async () => {
    const requests: RequestRecord[] = []
    let cookie = 0
    const fetch = vi.fn(async (input: string | URL, init?: RequestInit) => {
      const request = recordRequest(input, init)
      requests.push(request)
      expect(request.path).toBe('/pull')
      expect(request.headers.authorization).toBe('Bearer token-u1')
      return jsonResponse({
        cookie: ++cookie,
        lastMutationIDChanges: {},
        rowsPatch: [
          { op: 'clear' },
          { op: 'put', tableName: 'user', value: { id: 'u1', name: 'ada' } },
          {
            op: 'put',
            tableName: 'project',
            value: { id: 'p1', ownerId: 'u1', name: 'control' },
          },
          {
            op: 'put',
            tableName: 'member',
            value: { id: 'm1', projectId: 'p1', userId: 'u1' },
          },
        ],
      })
    })
    const transport = install(fetch)
    const zero = createZero()

    const view = zero.query.project.related('members').materialize()
    const data = await waitForComplete(view)
    view.destroy()

    expect(transport.connections).toBe(1)
    expect(data).toEqual([
      {
        id: 'p1',
        ownerId: 'u1',
        name: 'control',
        members: [{ id: 'm1', projectId: 'p1', userId: 'u1' }],
      },
    ])
    expect(requests[0].body.cookie).toBeNull()
    expect(requests[0].body.clientID).toEqual(expect.any(String))
    expect(requests[0].body.clientGroupID).toEqual(expect.any(String))
  })

  test('push frames POST to /push, resolve server promises, and schedule a follow-up pull', async () => {
    const requests: RequestRecord[] = []
    const fetch = vi.fn(async (input: string | URL, init?: RequestInit) => {
      const request = recordRequest(input, init)
      requests.push(request)

      if (request.path === '/pull') {
        return jsonResponse({ cookie: request.body.cookie, unchanged: true })
      }

      expect(request.path).toBe('/push')
      const mutation = request.body.mutations[0]
      return jsonResponse({
        pushResponse: {
          mutations: [
            {
              id: { clientID: mutation.clientID, id: mutation.id },
              result: {},
            },
          ],
        },
      })
    })
    install(fetch)
    const zero = createZero()

    await eventually(() =>
      expect(requests.filter((request) => request.path === '/pull').length).toBe(1)
    )
    const pullsBeforePush = requests.filter((request) => request.path === '/pull').length

    const mutation = zero.mutate.project.create({
      id: 'p1',
      ownerId: 'u1',
      name: 'created',
    })
    await mutation.client
    await mutation.server

    const push = requests.find((request) => request.path === '/push')
    expect(push?.headers.authorization).toBe('Bearer token-u1')
    expect(push?.body).toMatchObject({
      clientGroupID: expect.any(String),
      pushVersion: 1,
      requestID: expect.any(String),
      mutations: [
        {
          type: 'custom',
          name: 'project|create',
          id: 1,
          clientID: expect.any(String),
          args: [{ id: 'p1', ownerId: 'u1', name: 'created' }],
        },
      ],
    })
    await eventually(() =>
      expect(requests.filter((request) => request.path === '/pull').length).toBe(
        pullsBeforePush + 1
      )
    )
  })

  test('cookie discipline skips unchanged pokes, chains changed pokes, and coalesces concurrent pulls', async () => {
    const requests: RequestRecord[] = []
    const responses: Array<any | Promise<any>> = [
      {
        cookie: 1,
        lastMutationIDChanges: {},
        rowsPatch: [{ op: 'clear' }],
      },
      { cookie: 1, unchanged: true },
      {
        cookie: 2,
        lastMutationIDChanges: {},
        rowsPatch: [
          { op: 'clear' },
          {
            op: 'put',
            tableName: 'project',
            value: { id: 'p2', ownerId: 'u1', name: 'second' },
          },
        ],
      },
      {
        cookie: 3,
        lastMutationIDChanges: {},
        rowsPatch: [
          { op: 'clear' },
          {
            op: 'put',
            tableName: 'project',
            value: { id: 'p3', ownerId: 'u1', name: 'third' },
          },
        ],
      },
    ]
    const deferred = defer<any>()
    responses.push(deferred.promise)

    let inFlight = 0
    let maxInFlight = 0
    const fetch = vi.fn(async (input: string | URL, init?: RequestInit) => {
      inFlight++
      maxInFlight = Math.max(maxInFlight, inFlight)
      try {
        const request = recordRequest(input, init)
        requests.push(request)
        const response = responses.shift()
        if (!response) throw new Error('missing canned response')
        return jsonResponse(await response)
      } finally {
        inFlight--
      }
    })
    const transport = install(fetch)
    const messages = openRawSocket()

    await eventually(() =>
      expect(messages.some((message) => message[0] === 'pokeEnd')).toBe(true)
    )
    expect(requests[0].body.cookie).toBeNull()
    expect(findMessage(messages, 'pokeStart')[1].baseCookie).toBeNull()
    expect(findMessage(messages, 'pokeEnd')[1].cookie).toBe('00000000000000000001')
    messages.length = 0

    await transport.pull()
    expect(messages.filter((message) => message[0].startsWith('poke'))).toEqual([])
    expect(requests[1].body.cookie).toBe(1)

    await transport.pull()
    expect(findMessage(messages, 'pokeStart')[1].baseCookie).toBe('00000000000000000001')
    expect(findMessage(messages, 'pokeEnd')[1].cookie).toBe('00000000000000000002')
    expect(requests[2].body.cookie).toBe(1)
    messages.length = 0

    await transport.pull()
    expect(findMessage(messages, 'pokeStart')[1].baseCookie).toBe('00000000000000000002')
    expect(findMessage(messages, 'pokeEnd')[1].cookie).toBe('00000000000000000003')
    expect(requests[3].body.cookie).toBe(2)
    messages.length = 0

    const concurrentA = transport.pull()
    const concurrentB = transport.pull()
    await eventually(() => expect(requests.length).toBe(5))
    deferred.resolve({
      cookie: 4,
      lastMutationIDChanges: {},
      rowsPatch: [{ op: 'clear' }],
    })
    await Promise.all([concurrentA, concurrentB])

    expect(requests.length).toBe(5)
    expect(maxInFlight).toBe(1)
    expect(findMessage(messages, 'pokeStart')[1].baseCookie).toBe('00000000000000000003')
    expect(findMessage(messages, 'pokeEnd')[1].cookie).toBe('00000000000000000004')
  })

  test('ping is answered locally and the stock Zero connection survives idle ping', async () => {
    const fetch = vi.fn(async (input: string | URL, init?: RequestInit) => {
      const request = recordRequest(input, init)
      expect(request.path).toBe('/pull')
      return jsonResponse({ cookie: request.body.cookie, unchanged: true })
    })
    const transport = install(fetch)
    const zero = createZero({ pingTimeoutMs: 10 })

    await eventually(() => expect(zero.connection.state.current.name).toBe('connected'))
    await sleep(40)

    expect(zero.connection.state.current.name).toBe('connected')
    expect(transport.connections).toBe(1)
  })

  test('non-origin WebSockets pass through to the native implementation', () => {
    const previous = globalThis.WebSocket
    class NativeWebSocket {
      static CONNECTING = 0
      static OPEN = 1
      static CLOSING = 2
      static CLOSED = 3
      constructor(
        readonly url: string | URL,
        readonly protocols?: string | string[]
      ) {}
    }
    globalThis.WebSocket = NativeWebSocket as unknown as typeof WebSocket

    const transport = installZeroHttpTransport({ origin: ORIGIN, fetch: vi.fn() })
    const socket = new WebSocket('wss://elsewhere.local/socket', 'native')

    expect(socket).toBeInstanceOf(NativeWebSocket)
    expect((socket as unknown as NativeWebSocket).url).toBe(
      'wss://elsewhere.local/socket'
    )

    transport.uninstall()
    expect(globalThis.WebSocket).toBe(NativeWebSocket)
    globalThis.WebSocket = previous
  })
})

function install(fetch: typeof globalThis.fetch) {
  const transport = installZeroHttpTransport({ origin: ORIGIN, fetch })
  transports.push(transport)
  return transport
}

function createZero(options: { pingTimeoutMs?: number } = {}) {
  const zero = new Zero({
    server: ORIGIN,
    userID: 'u1',
    auth: 'token-u1',
    schema: zeroHttpFixtureSchema,
    kvStore: 'mem',
    storageKey: `zero-http-test-${++storageID}`,
    mutators: zeroHttpFixtureMutators,
    pingTimeoutMs: options.pingTimeoutMs,
  })
  zeros.push(zero)
  return zero
}

function recordRequest(input: string | URL, init?: RequestInit): RequestRecord {
  const url = new URL(String(input))
  const headers = Object.fromEntries(
    Object.entries((init?.headers ?? {}) as Record<string, string>).map(
      ([key, value]) => [key.toLowerCase(), value]
    )
  )
  return {
    url: url.toString(),
    path: url.pathname,
    headers,
    body: init?.body ? JSON.parse(String(init.body)) : undefined,
  }
}

function jsonResponse(body: unknown) {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  })
}

async function waitForComplete<T>(view: {
  addListener(listener: (data: T, resultType: string) => void): () => void
}): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timeout = setTimeout(
      () => reject(new Error('timed out waiting for complete query')),
      5_000
    )
    let cleanup = () => {}
    cleanup = view.addListener((data, resultType) => {
      if (resultType !== 'complete') return
      clearTimeout(timeout)
      cleanup()
      resolve(JSON.parse(JSON.stringify(data)) as T)
    })
  })
}

async function eventually(assertion: () => void | Promise<void>, timeout = 1_000) {
  const started = Date.now()
  let lastError: unknown
  while (Date.now() - started < timeout) {
    try {
      await assertion()
      return
    } catch (error) {
      lastError = error
      await sleep(10)
    }
  }
  throw lastError
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function openRawSocket() {
  const url = new URL(`${ORIGIN}/sync/v51/connect`)
  url.protocol = 'wss:'
  url.searchParams.set('clientID', 'c1')
  url.searchParams.set('clientGroupID', 'cg1')
  url.searchParams.set('userID', 'u1')
  url.searchParams.set('baseCookie', '')
  url.searchParams.set('lmid', '0')
  url.searchParams.set('wsid', 'ws-test')
  const messages: Array<[string, any]> = []
  const socket = new WebSocket(
    url,
    encodeSecProtocol(['initConnection', { desiredQueriesPatch: [] }], 'token-u1')
  )
  socket.addEventListener('message', (event) => {
    messages.push(JSON.parse(String(event.data)))
  })
  return messages
}

function findMessage(messages: Array<[string, any]>, type: string) {
  const message = messages.find((item) => item[0] === type)
  expect(message).toBeDefined()
  return message as [string, any]
}

function encodeSecProtocol(
  initConnectionMessage: [string, Record<string, unknown>],
  authToken: string
) {
  return encodeURIComponent(
    Buffer.from(JSON.stringify({ initConnectionMessage, authToken })).toString('base64')
  )
}

function defer<T>() {
  let resolve!: (value: T) => void
  let reject!: (error: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}
