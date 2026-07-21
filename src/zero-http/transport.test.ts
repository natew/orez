import { Zero, defineMutator, defineMutators } from '@rocicorp/zero'
import { afterEach, describe, expect, test, vi } from 'vitest'

import { createEncryptedColumnCodec } from './encrypted-column-codec.js'
import { zeroHttpFixtureMutators, zeroHttpFixtureSchema } from './fixture-schema.js'
import {
  createZeroClientTransport,
  ensureHttpPullTransport,
  installHttpPullTransport,
} from './transport.js'

import type {
  EncryptedColumnManifest,
  EncryptedRowBatch,
  HttpPullLifecycleEvent,
  PayloadCodec,
  PullResponse,
  PushRequest,
} from './transport.js'

const ORIGIN = 'https://zero-http.local'
const transportEncryptionKey = new Uint8Array(32).fill(17)
const transportEncryptionManifest = {
  version: 1,
  networkID: 'transport-network',
  schemaID: 'transport-schema',
  rowMutations: {
    'cloud.applyBatch': {
      argumentIndex: 0,
      format: 'orez-row-batch-v1',
    },
  },
  tables: {
    project: {
      serverName: 'project_record',
      primaryKey: ['id'],
      primaryKeyServerNames: { id: 'project_id' },
      columns: { name: { serverName: 'project_name' } },
    },
  },
} as const satisfies EncryptedColumnManifest
const encryptionFixtureMutators = defineMutators({
  cloud: {
    applyBatch: defineMutator<EncryptedRowBatch, typeof zeroHttpFixtureSchema>(
      async () => {}
    ),
  },
})

type RequestRecord = {
  url: string
  path: string
  headers: Record<string, string>
  body: any
}

const zeros: Zero<any, any>[] = []
const transports: Array<{ uninstall(): void }> = []
let storageID = 0
let restoreNativeWebSocket: (() => void) | undefined

afterEach(async () => {
  while (zeros.length) await zeros.pop()?.close()
  while (transports.length) transports.pop()?.uninstall()
  restoreNativeWebSocket?.()
  restoreNativeWebSocket = undefined
  vi.useRealTimers()
})

describe('zero-http transport', () => {
  test('connect + complete maps physical rows into a stock Zero materialized query', async () => {
    const requests: RequestRecord[] = []
    let cookie = 0
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = recordRequest(input, init)
      requests.push(request)
      expect(request.path).toBe('/pull')
      expect(request.headers.authorization).toBe('Bearer token-u1')
      // sync engines emit physical downstream names; Zero maps them back to
      // the logical fixture schema while ingesting the poke.
      return jsonResponse({
        cookie: ++cookie,
        lastMutationIDChanges: {},
        rowsPatch: [
          { op: 'clear' },
          {
            op: 'put',
            tableName: 'user_record',
            value: { user_id: 'u1', display_name: 'ada' },
          },
          {
            op: 'put',
            tableName: 'project_record',
            value: { project_id: 'p1', owner_id: 'u1', project_name: 'control' },
          },
          {
            op: 'put',
            tableName: 'project_member',
            value: { member_id: 'm1', project_id: 'p1', user_id: 'u1' },
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

  test('decrypts a stock Zero pull whose primary key has a physical name', async () => {
    const payloadCodec = createTransportEncryptionCodec()
    const encoded = await payloadCodec.encodePush({
      mutations: [
        {
          type: 'custom',
          name: 'cloud.applyBatch',
          clientID: 'transport-client',
          id: 1,
          args: [
            {
              sourceID: 'transport-source',
              fromSeq: 1,
              throughSeq: 1,
              rows: [
                {
                  seq: 1,
                  op: 'put',
                  table: 'project',
                  value: { id: 'p1', ownerId: 'u1', name: 'decrypted project' },
                },
              ],
            },
          ],
        },
      ],
    })
    const batch = encoded.mutations[0].args?.[0] as {
      rows: Array<{ value: { name: string } }>
    }
    const encryptedName = batch.rows[0].value.name
    const fetch = vi.fn(async () =>
      jsonResponse({
        cookie: 1,
        lastMutationIDChanges: {},
        rowsPatch: [
          { op: 'clear' },
          {
            op: 'put',
            tableName: 'user_record',
            value: { user_id: 'u1', display_name: 'ada' },
          },
          {
            op: 'put',
            tableName: 'project_record',
            value: {
              project_id: 'p1',
              owner_id: 'u1',
              project_name: encryptedName,
            },
          },
        ],
      })
    )
    const transport = installHttpPullTransport({
      origin: ORIGIN,
      fetch,
      payloadCodec,
    })
    transports.push(transport)
    const zero = createZero()

    const view = zero.query.project.materialize()
    const data = await waitForComplete(view)
    view.destroy()

    expect(data).toEqual([{ id: 'p1', ownerId: 'u1', name: 'decrypted project' }])
  })

  test('stock Zero encrypts a plaintext row mutation and decrypts the applied row into a query', async () => {
    const plaintext = 'stock Zero plaintext round trip'
    let storedCiphertext: string | undefined
    let returnedStoredRow = false
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = recordRequest(input, init)
      if (request.path.endsWith('/pull')) {
        if (storedCiphertext && !returnedStoredRow) {
          returnedStoredRow = true
          return jsonResponse({
            cookie: 1,
            lastMutationIDChanges: {},
            rowsPatch: [
              { op: 'clear' },
              {
                op: 'put',
                tableName: 'project_record',
                value: {
                  project_id: 'p-stock',
                  owner_id: 'u1',
                  project_name: storedCiphertext,
                },
              },
            ],
          })
        }
        return jsonResponse({ cookie: request.body.cookie, unchanged: true })
      }

      expect(request.path).toBe('/push')
      const mutation = request.body.mutations[0]
      expect(mutation.name).toBe('cloud.applyBatch')
      const appliedBatch = mutation.args[0]
      const appliedValue = appliedBatch.rows[0].value
      expect(appliedValue.id).toBe('p-stock')
      expect(appliedValue.ownerId).toBe('u1')
      expect(appliedValue.name).toMatch(/^orez-e1\.4\./)
      expect(JSON.stringify(request.body)).not.toContain(plaintext)
      storedCiphertext = appliedValue.name
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
    const transport = installHttpPullTransport({
      origin: ORIGIN,
      fetch,
      payloadCodec: createTransportEncryptionCodec(),
    })
    transports.push(transport)
    const zero = createEncryptedZero()
    const view = zero.query.project.materialize()
    await eventually(() =>
      expect(
        fetch.mock.calls.some((call) => recordRequest(call[0], call[1]).path === '/pull')
      ).toBe(true)
    )

    const mutation = zero.mutate(
      encryptionFixtureMutators.cloud.applyBatch({
        sourceID: 'stock-source',
        fromSeq: 1,
        throughSeq: 1,
        rows: [
          {
            seq: 1,
            op: 'put',
            table: 'project',
            value: { id: 'p-stock', ownerId: 'u1', name: plaintext },
          },
        ],
      })
    )
    await mutation.client
    await mutation.server
    await eventually(() =>
      expect(view.data).toEqual([
        expect.objectContaining({ id: 'p-stock', ownerId: 'u1', name: plaintext }),
      ])
    )

    expect(storedCiphertext).toMatch(/^orez-e1\.4\./)
    expect(returnedStoredRow).toBe(true)
    view.destroy()
  })

  test('rejects plaintext injected into an encrypted pull before any poke', async () => {
    const fetch = vi.fn(async () =>
      jsonResponse({
        cookie: 1,
        lastMutationIDChanges: {},
        rowsPatch: [
          {
            op: 'put',
            tableName: 'project_record',
            value: {
              project_id: 'p1',
              owner_id: 'u1',
              project_name: 'edge plaintext injection',
            },
          },
        ],
      })
    )
    const transport = installHttpPullTransport({
      origin: ORIGIN,
      fetch,
      payloadCodec: createTransportEncryptionCodec(),
    })
    transports.push(transport)
    const { messages } = openRawSocketWithMessages()

    await eventually(() => expect(fetch).toHaveBeenCalledTimes(1))
    await eventually(() => expect(transport.connections).toBe(0))
    expect(messages.some((message) => message[0].startsWith('poke'))).toBe(false)
  })

  test('got queries survive a snapshot-reset poke after the ack', async () => {
    // hold the FIRST /pull response until the client has sent its desired
    // queries, so the got ack rides the first poke. every response is a full
    // snapshot reset (leading op:'clear'), so the immediate follow-up pull
    // emits a second clear-bearing poke AFTER the ack. a rows clear resets the
    // client's entire replicache space including got-query marks — without the
    // transport re-asserting its acked got set, the query regresses to
    // 'unknown' forever and this times out (the load-dependent flake this
    // pins deterministically).
    let cookie = 0
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      recordRequest(input, init)
      if (cookie === 0) await sleep(150)
      return jsonResponse({
        cookie: ++cookie,
        lastMutationIDChanges: {},
        rowsPatch: [
          { op: 'clear' },
          {
            op: 'put',
            tableName: 'user_record',
            value: { user_id: 'u1', display_name: 'ada' },
          },
          {
            op: 'put',
            tableName: 'project_record',
            value: { project_id: 'p1', owner_id: 'u1', project_name: 'control' },
          },
        ],
      })
    })
    install(fetch)
    const zero = createZero()

    const view = zero.query.project.materialize()
    const data = await waitForComplete(view)
    view.destroy()

    expect(fetch.mock.calls.length).toBeGreaterThanOrEqual(2)
    expect(data).toEqual([{ id: 'p1', ownerId: 'u1', name: 'control' }])
  })

  test('push frames POST to /push, resolve server promises, and schedule a follow-up pull', async () => {
    const requests: RequestRecord[] = []
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
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

  test('pullOrigin and pushOrigin route sync through the authoritative application server', async () => {
    const requests: RequestRecord[] = []
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = recordRequest(input, init)
      requests.push(request)
      if (request.path.endsWith('/pull')) {
        return jsonResponse({ cookie: request.body.cookie, unchanged: true })
      }
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
    const transport = installHttpPullTransport({
      origin: ORIGIN,
      pullOrigin: 'https://app.local/zero-http',
      pushOrigin: 'https://app.local/zero-http',
      fetch,
    })
    transports.push(transport)
    const zero = createZero()

    await eventually(() =>
      expect(requests.some((request) => request.path.endsWith('/pull'))).toBe(true)
    )
    const mutation = zero.mutate.project.create({
      id: 'p1',
      ownerId: 'u1',
      name: 'created',
    })
    await mutation.client
    await mutation.server

    const push = requests.find((request) => request.path.endsWith('/push'))
    // push always carries the schema-shard routing params (native hosts route
    // by them; other servers ignore unknown query params)
    expect(push?.url).toBe('https://app.local/zero-http/push?schema=zero_0&appID=zero')
    expect(requests.find((request) => request.path.endsWith('/pull'))?.url).toBe(
      'https://app.local/zero-http/pull'
    )
  })

  test('updateAuth frame updates bearer headers for later requests', async () => {
    const requests: RequestRecord[] = []
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = recordRequest(input, init)
      requests.push(request)
      expect(request.path).toBe('/pull')
      return jsonResponse({ cookie: request.body.cookie, unchanged: true })
    })
    const transport = install(fetch)
    const { socket } = openRawSocketWithMessages({ authToken: 'token-old' })

    await eventually(() => expect(requests.length).toBe(1))
    expect(requests[0].headers.authorization).toBe('Bearer token-old')

    socket.send(JSON.stringify(['updateAuth', { auth: 'token-new' }]))
    await transport.pull()

    expect(requests.at(-1)?.headers.authorization).toBe('Bearer token-new')
  })

  test('push frames are serialized and queued bursts are batched per socket', async () => {
    const firstPushStarted = defer<void>()
    const releaseFirstPush = defer<void>()
    const pushRequests: number[][] = []
    const lifecycle: HttpPullLifecycleEvent[] = []
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = recordRequest(input, init)
      if (request.path === '/pull') {
        return jsonResponse({ cookie: request.body.cookie, unchanged: true })
      }

      expect(request.path).toBe('/push')
      const mutationIDs = request.body.mutations.map((mutation: any) => mutation.id)
      pushRequests.push(mutationIDs)
      if (mutationIDs[0] === 1) {
        firstPushStarted.resolve()
        await releaseFirstPush.promise
      }
      return jsonResponse({
        pushResponse: {
          mutations: request.body.mutations.map((mutation: any) => ({
            id: { clientID: mutation.clientID, id: mutation.id },
            result: {},
          })),
        },
      })
    })
    const transport = installHttpPullTransport({
      origin: ORIGIN,
      fetch,
      lifecycle: (event) => lifecycle.push(event),
    })
    transports.push(transport)
    const { messages, socket } = openRawSocketWithMessages()

    await eventually(() =>
      expect(messages.some((message) => message[0] === 'connected')).toBe(true)
    )
    socket.send(JSON.stringify(['push', pushBody(1)]))
    await firstPushStarted.promise
    socket.send(JSON.stringify(['push', pushBody(2)]))
    socket.send(JSON.stringify(['push', pushBody(3)]))
    socket.send(JSON.stringify(['push', pushBody(4)]))
    await sleep(25)

    expect(pushRequests).toEqual([[1]])
    releaseFirstPush.resolve()
    await eventually(() => expect(pushRequests).toEqual([[1], [2, 3, 4]]))
    await eventually(() =>
      expect(messages.filter((message) => message[0] === 'pushResponse')).toHaveLength(2)
    )
    expect(
      messages
        .filter((message) => message[0] === 'pushResponse')
        .flatMap((message) => message[1].mutations.map((mutation: any) => mutation.id.id))
    ).toEqual([1, 2, 3, 4])
    expect(
      lifecycle
        .filter((event) => event.type === 'push')
        .map(({ pushFrameCount, mutationCount }) => ({
          pushFrameCount,
          mutationCount,
        }))
    ).toEqual([
      { pushFrameCount: 1, mutationCount: 1 },
      { pushFrameCount: 3, mutationCount: 3 },
    ])
  })

  test('encodes each push attempt inside the existing FIFO push chain', async () => {
    const firstEncodeStarted = defer<void>()
    const releaseFirstEncode = defer<void>()
    const encodedIDs: number[] = []
    const postedIDs: number[] = []
    const payloadCodec: PayloadCodec = {
      id: 'test-push-codec',
      async encodePush(body) {
        const id = body.mutations[0].id
        encodedIDs.push(id)
        if (id === 1) {
          firstEncodeStarted.resolve()
          await releaseFirstEncode.promise
        }
        return {
          ...body,
          mutations: body.mutations.map((mutation) => ({
            ...mutation,
            args: [{ encodedByCodec: true, original: mutation.args?.[0] ?? null }],
          })),
        } as PushRequest
      },
      async decodePull(response) {
        return response
      },
    }
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = recordRequest(input, init)
      if (request.path === '/pull') {
        return jsonResponse({ cookie: request.body.cookie, unchanged: true })
      }
      const mutation = request.body.mutations[0]
      postedIDs.push(mutation.id)
      expect(mutation.args[0].encodedByCodec).toBe(true)
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
    const transport = installHttpPullTransport({
      origin: ORIGIN,
      fetch,
      payloadCodec,
    })
    transports.push(transport)
    const { messages, socket } = openRawSocketWithMessages()
    await eventually(() =>
      expect(messages.some((message) => message[0] === 'connected')).toBe(true)
    )

    socket.send(JSON.stringify(['push', pushBody(1)]))
    await firstEncodeStarted.promise
    socket.send(JSON.stringify(['push', pushBody(2)]))
    await sleep(25)

    expect(encodedIDs).toEqual([1])
    expect(postedIDs).toEqual([])
    releaseFirstEncode.resolve()
    await eventually(() => expect(encodedIDs).toEqual([1, 2]))
    await eventually(() => expect(postedIDs).toEqual([1, 2]))
  })

  test('does not POST a push when its payload codec fails closed', async () => {
    const paths: string[] = []
    const payloadCodec: PayloadCodec = {
      id: 'test-fail-closed-codec',
      async encodePush() {
        throw new Error('no current encryption key is available')
      },
      async decodePull(response) {
        return response
      },
    }
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = recordRequest(input, init)
      paths.push(request.path)
      return jsonResponse({ cookie: request.body.cookie, unchanged: true })
    })
    const transport = installHttpPullTransport({
      origin: ORIGIN,
      fetch,
      payloadCodec,
    })
    transports.push(transport)
    const { messages, socket } = openRawSocketWithMessages()
    await eventually(() =>
      expect(messages.some((message) => message[0] === 'connected')).toBe(true)
    )

    socket.send(JSON.stringify(['push', pushBody(1)]))

    await eventually(() => expect(transport.connections).toBe(0))
    expect(paths).toEqual(['/pull'])
  })

  test('decodes initial, direct, wake, and recovery pulls at the fetch boundary', async () => {
    const wakeSockets = useFakeNativeWebSocket()
    const decodePull = vi.fn(async (response: PullResponse) => {
      if (response.unchanged) return response
      return {
        ...response,
        rowsPatch: response.rowsPatch.map((patch) => {
          if (
            !patch ||
            typeof patch !== 'object' ||
            Array.isArray(patch) ||
            patch.op !== 'put' ||
            !patch.value ||
            typeof patch.value !== 'object' ||
            Array.isArray(patch.value)
          ) {
            return patch
          }
          return {
            ...patch,
            value: {
              ...patch.value,
              project_name: `plain-${patch.value.project_name}`,
            },
          }
        }),
      } as PullResponse
    })
    const payloadCodec: PayloadCodec = {
      id: 'test-pull-codec',
      async encodePush(body) {
        return body
      },
      decodePull,
    }
    let cookie = 0
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = recordRequest(input, init)
      expect(request.path).toBe('/pull')
      cookie++
      return jsonResponse({
        cookie,
        lastMutationIDChanges: {},
        rowsPatch: [
          {
            op: 'put',
            tableName: 'project_record',
            value: {
              project_id: `p${cookie}`,
              owner_id: 'u1',
              project_name: `cipher-${cookie}`,
            },
          },
        ],
      })
    })
    const transport = installHttpPullTransport({
      origin: ORIGIN,
      fetch,
      wake: true,
      payloadCodec,
    })
    transports.push(transport)
    const { messages, socket } = openRawSocketWithMessages()

    await eventually(() => expect(decodePull).toHaveBeenCalledTimes(1))
    await eventually(() => expect(wakeSockets).toHaveLength(1))
    await transport.pull()
    expect(decodePull).toHaveBeenCalledTimes(2)

    wakeSockets[0].onmessage?.()
    await eventually(() => expect(decodePull).toHaveBeenCalledTimes(3))

    socket.send(
      JSON.stringify([
        'pull',
        {
          clientGroupID: 'cg1',
          cookie: null,
          requestID: 'mutation-recovery',
        },
      ])
    )
    await eventually(() => expect(decodePull).toHaveBeenCalledTimes(4))
    await eventually(() =>
      expect(
        messages.some(
          (message) =>
            message[0] === 'pull' && message[1].requestID === 'mutation-recovery'
        )
      ).toBe(true)
    )

    const emittedNames = messages
      .filter((message) => message[0] === 'pokePart')
      .flatMap((message) => message[1].rowsPatch ?? [])
      .filter((patch) => patch.op === 'put')
      .map((patch) => patch.value.project_name)
    expect(emittedNames).toEqual(['plain-cipher-1', 'plain-cipher-2', 'plain-cipher-3'])
  })

  test('a pull codec authentication failure emits no poke', async () => {
    const payloadCodec: PayloadCodec = {
      id: 'test-auth-failure-codec',
      async encodePush(body) {
        return body
      },
      async decodePull() {
        throw new Error('orez-e1 authentication failed')
      },
    }
    const fetch = vi.fn(async () =>
      jsonResponse({
        cookie: 1,
        lastMutationIDChanges: {},
        rowsPatch: [{ op: 'put', tableName: 'project_record', value: {} }],
      })
    )
    const transport = installHttpPullTransport({
      origin: ORIGIN,
      fetch,
      payloadCodec,
    })
    transports.push(transport)
    const { messages } = openRawSocketWithMessages()

    await eventually(() => expect(fetch).toHaveBeenCalledTimes(1))
    await eventually(() => expect(transport.connections).toBe(0))
    expect(messages.some((message) => message[0].startsWith('poke'))).toBe(false)
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
            tableName: 'project_record',
            value: { project_id: 'p2', owner_id: 'u1', project_name: 'second' },
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
            tableName: 'project_record',
            value: { project_id: 'p3', owner_id: 'u1', project_name: 'third' },
          },
        ],
      },
    ]
    const deferred = defer<any>()
    responses.push(deferred.promise)

    let inFlight = 0
    let maxInFlight = 0
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
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

  test('unchanged pull flushes late query registration to complete', async () => {
    const requests: RequestRecord[] = []
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = recordRequest(input, init)
      requests.push(request)
      expect(request.path).toBe('/pull')

      if (request.body.cookie === null) {
        return jsonResponse({
          cookie: 1,
          lastMutationIDChanges: {},
          rowsPatch: [
            { op: 'clear' },
            {
              op: 'put',
              tableName: 'user_record',
              value: { user_id: 'u1', display_name: 'ada' },
            },
            {
              op: 'put',
              tableName: 'project_record',
              value: { project_id: 'p1', owner_id: 'u1', project_name: 'first' },
            },
          ],
        })
      }

      return jsonResponse({ cookie: request.body.cookie, unchanged: true })
    })
    install(fetch)
    const zero = createZero()

    const projectView = zero.query.project.materialize()
    await waitForComplete(projectView)

    const userView = zero.query.user.materialize()
    const users = await waitForComplete<any[]>(userView)

    expect(users).toEqual([{ id: 'u1', name: 'ada' }])
    expect(requests.at(-1)?.body.cookie).toBe(1)
    projectView.destroy()
    userView.destroy()
  })

  test('ping is answered locally and the stock Zero connection survives idle ping', async () => {
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
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

  test('401 pull failure closes the fake socket without materializing data', async () => {
    const requests: RequestRecord[] = []
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = recordRequest(input, init)
      requests.push(request)
      expect(request.path).toBe('/pull')
      return jsonResponse({ error: 'unauthorized' }, { status: 401 })
    })
    const transport = install(fetch)
    const zero = createZero()
    const view = zero.query.project.materialize()
    const emissions: Array<{ data: any[]; resultType: string }> = []
    const cleanup = view.addListener((data: any, resultType) => {
      emissions.push({ data: JSON.parse(JSON.stringify(data)), resultType })
    })

    await eventually(() => expect(requests.length).toBeGreaterThan(0))
    await eventually(() => expect(zero.connection.state.current.name).toBe('needs-auth'))
    await sleep(25)

    expect(transport.connections).toBe(0)
    expect(emissions.flatMap((emission) => emission.data)).toEqual([])
    expect(view.data).toEqual([])
    cleanup()
    view.destroy()
  })

  test('400 pull failure is terminal and does not reconnect a stock Zero client', async () => {
    const requests: RequestRecord[] = []
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = recordRequest(input, init)
      requests.push(request)
      expect(request.path).toBe('/pull')
      return jsonResponse(
        { error: 'query not registered: expense.expensesInRange' },
        { status: 400 }
      )
    })
    const transport = install(fetch)
    const zero = createZero()

    await eventually(() => expect(zero.connection.state.current.name).toBe('error'))
    await sleep(100)

    expect(requests).toHaveLength(1)
    expect(transport.connections).toBe(0)
    expect(zero.connection.state.current).toMatchObject({
      name: 'error',
      reason: expect.stringContaining('query not registered: expense.expensesInRange'),
    })
  })

  test('pull and push 500 reconnects keep socket opens on their current Zero attempt', async () => {
    const lifecycle: HttpPullLifecycleEvent[] = []
    const zeroLogs: Array<{ context: Record<string, unknown>; args: unknown[] }> = []
    let failPull = false
    let failPush = false
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = recordRequest(input, init)
      if (request.path === '/pull') {
        if (failPull) {
          failPull = false
          return jsonResponse({ error: 'injected pull outage' }, { status: 500 })
        }
        return jsonResponse({ cookie: request.body.cookie, unchanged: true })
      }
      if (failPush) {
        failPush = false
        return jsonResponse({ error: 'injected push outage' }, { status: 500 })
      }
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
    const transport = installHttpPullTransport({
      origin: ORIGIN,
      fetch,
      lifecycle: (event) => lifecycle.push(event),
    })
    transports.push(transport)
    const zero = createZero({
      logSink: {
        log: (_level, context, ...args) => {
          zeroLogs.push({ context: context ?? {}, args })
        },
      },
    })

    await eventually(() => expect(zero.connection.state.current.name).toBe('connected'))
    failPull = true
    await transport.pull().catch(() => {})
    // a 500 is transient, so the reconnect waits out Zero's run-loop backoff
    // (5s) rather than retrying immediately — see the storm tests below.
    await eventually(
      () => expect(lifecycle.filter((event) => event.type === 'open')).toHaveLength(2),
      8_000
    )

    failPush = true
    const mutation = zero.mutate.project.create({
      id: 'p1',
      ownerId: 'u1',
      name: 'after reconnect',
    })
    await mutation.client
    await mutation.server.catch(() => {})
    await eventually(
      () => expect(lifecycle.filter((event) => event.type === 'open')).toHaveLength(3),
      8_000
    )
    await eventually(
      () => expect(zero.connection.state.current.name).toBe('connected'),
      8_000
    )

    const opens = lifecycle.filter((event) => event.type === 'open')
    expect(
      opens.map(({ generation, activeGeneration }) => [generation, activeGeneration])
    ).toEqual([
      [1, 1],
      [2, 2],
      [3, 3],
    ])
    expect(
      zeroLogs.some(({ args }) =>
        args.some((arg) => String(arg).includes('connect start time is undefined'))
      )
    ).toBe(false)
  }, 30_000)

  test('a pull 500 cannot let a timed-out socket construction open on the abandoned attempt', async () => {
    vi.useFakeTimers()
    const lifecycle: HttpPullLifecycleEvent[] = []
    let pullCount = 0
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = recordRequest(input, init)
      pullCount++
      if (pullCount === 1) {
        return jsonResponse({ error: 'injected pull outage' }, { status: 500 })
      }
      return jsonResponse({ cookie: request.body.cookie, unchanged: true })
    })
    const transport = installHttpPullTransport({
      origin: ORIGIN,
      fetch,
      lifecycle: (event) => lifecycle.push(event),
    })
    transports.push(transport)
    const first = openRawSocketWithMessages({ wsid: 'before-pull-500' })
    await vi.advanceTimersByTimeAsync(1)
    expect(first.socket.readyState).toBe(first.socket.CLOSED)

    let delayedOpened = false
    let delayedClosed = 0
    const delayed = openRawSocketWithMessages({
      wsid: 'delayed-reconnect',
      attemptStartedAt: performance.now() - 10_001,
    })
    delayed.socket.addEventListener('open', () => {
      delayedOpened = true
    })
    delayed.socket.addEventListener('close', () => {
      delayedClosed++
    })
    await vi.advanceTimersByTimeAsync(1)

    expect(delayedOpened).toBe(false)
    expect(delayedClosed).toBe(1)
    expect(lifecycle.filter((event) => event.type === 'open')).toHaveLength(1)
    const aborted = lifecycle.find((event) => event.type === 'aborted')
    expect(aborted).toMatchObject({
      pageID: transport.pageID,
      transportID: transport.transportID,
      zeroInstanceID: `${transport.pageID}:zero:c1`,
      clientID: 'c1',
      clientGroupID: 'cg1',
      connectionAttemptID: `${transport.pageID}:zero:c1:attempt:delayed-reconnect`,
      socketID: `${transport.transportID}:socket:2`,
      wsid: 'delayed-reconnect',
      generation: 2,
      activeGeneration: 2,
      code: 1000,
    })
    expect(aborted?.attemptAgeMs).toBeGreaterThanOrEqual(10_000)
    expect(
      lifecycle.filter(
        (event) =>
          event.generation === 2 &&
          (event.type === 'aborted' ||
            event.type === 'close' ||
            event.type === 'superseded')
      )
    ).toHaveLength(1)
  })

  test('a replacement socket supersedes stale open and connected events for the same client', async () => {
    vi.useFakeTimers()
    const lifecycle: HttpPullLifecycleEvent[] = []
    const transport = installHttpPullTransport({
      origin: ORIGIN,
      fetch: vi.fn(async () => jsonResponse({ cookie: null, unchanged: true })),
      lifecycle: (event) => lifecycle.push(event),
    })
    transports.push(transport)

    let connectStart: number | undefined = Date.now()
    const errors: string[] = []
    const attachStockLifecycle = (socket: WebSocket) => {
      socket.addEventListener('open', () => {
        if (connectStart === undefined) {
          errors.push('Got open event but connect start time is undefined.')
        }
      })
      socket.addEventListener('message', (event) => {
        const message = JSON.parse(String(event.data)) as [string, unknown]
        if (message[0] === 'connected') connectStart = undefined
      })
    }

    const stale = openRawSocketWithMessages({ wsid: 'stale' }).socket
    attachStockLifecycle(stale)
    const current = openRawSocketWithMessages({ wsid: 'current' }).socket
    attachStockLifecycle(current)
    await vi.advanceTimersByTimeAsync(1)

    expect(
      lifecycle
        .filter((event) => event.type === 'open')
        .map(({ generation, activeGeneration, wsid }) => [
          generation,
          activeGeneration,
          wsid,
        ])
    ).toEqual([[2, 2, 'current']])
    expect(lifecycle).toContainEqual(
      expect.objectContaining({
        type: 'superseded',
        generation: 1,
        activeGeneration: 2,
        wsid: 'stale',
      })
    )
    expect(
      lifecycle.filter(
        (event) =>
          event.generation === 1 &&
          (event.type === 'aborted' ||
            event.type === 'close' ||
            event.type === 'superseded')
      )
    ).toHaveLength(1)
    expect(errors).toEqual([])
  })

  test('authenticated wake appends a freshly minted token when the socket opens', async () => {
    const wakeSockets = useFakeNativeWebSocket()
    const getToken = vi.fn(async () => 'signed token&scope=one')
    const fetch = unchangedPullFetch()
    const transport = installHttpPullTransport({
      origin: ORIGIN,
      fetch,
      wake: { getToken },
    })
    transports.push(transport)

    openRawSocketWithMessages()

    await eventually(() => expect(wakeSockets).toHaveLength(1))
    expect(getToken).toHaveBeenCalledTimes(1)
    expect(wakeSockets[0].url).toBe(
      'wss://zero-http.local/wake?clientID=c1&wakeToken=signed%20token%26scope%3Done'
    )
  })

  test('authenticated wake gets a fresh token for every reconnect attempt', async () => {
    const wakeSockets = useFakeNativeWebSocket()
    const getToken = vi
      .fn<() => Promise<string>>()
      .mockResolvedValueOnce('wake-token-1')
      .mockResolvedValueOnce('wake-token-2')
    const transport = installHttpPullTransport({
      origin: ORIGIN,
      fetch: unchangedPullFetch(),
      wake: { getToken },
    })
    transports.push(transport)

    openRawSocketWithMessages()
    await eventually(() => expect(wakeSockets).toHaveLength(1))
    wakeSockets[0].onerror?.()

    await eventually(() => expect(wakeSockets).toHaveLength(2), 1_000)
    expect(getToken).toHaveBeenCalledTimes(2)
    expect(wakeSockets.map((socket) => socket.url)).toEqual([
      'wss://zero-http.local/wake?clientID=c1&wakeToken=wake-token-1',
      'wss://zero-http.local/wake?clientID=c1&wakeToken=wake-token-2',
    ])
  })

  test('wake token rejection leaves pulls healthy and retries the advisory channel', async () => {
    const wakeSockets = useFakeNativeWebSocket()
    const getToken = vi.fn(async () => {
      throw new Error('mint route unavailable')
    })
    const fetch = unchangedPullFetch()
    const transport = installHttpPullTransport({
      origin: ORIGIN,
      fetch,
      wake: { getToken },
    })
    transports.push(transport)

    openRawSocketWithMessages()
    await eventually(() => expect(fetch).toHaveBeenCalled())
    const pullsBeforeManualPull = fetch.mock.calls.length

    await expect(transport.pull()).resolves.toBeUndefined()
    expect(fetch.mock.calls.length).toBeGreaterThan(pullsBeforeManualPull)
    await eventually(() => expect(getToken).toHaveBeenCalledTimes(2), 1_000)
    expect(wakeSockets).toHaveLength(0)
    expect(transport.connections).toBe(1)
  })

  test('wake true preserves the bare unauthenticated socket URL', async () => {
    const wakeSockets = useFakeNativeWebSocket()
    const transport = installHttpPullTransport({
      origin: ORIGIN,
      fetch: unchangedPullFetch(),
      wake: true,
    })
    transports.push(transport)

    openRawSocketWithMessages()

    await eventually(() => expect(wakeSockets).toHaveLength(1))
    expect(wakeSockets[0].url).toBe('wss://zero-http.local/wake?clientID=c1')
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

    const transport = installHttpPullTransport({ origin: ORIGIN, fetch: vi.fn() })
    const socket = new WebSocket('wss://elsewhere.local/socket', 'native')

    expect(socket).toBeInstanceOf(NativeWebSocket)
    expect((socket as unknown as NativeWebSocket).url).toBe(
      'wss://elsewhere.local/socket'
    )

    transport.uninstall()
    expect(globalThis.WebSocket).toBe(NativeWebSocket)
    globalThis.WebSocket = previous
  })

  test('ensureHttpPullTransport installs once per origin across module reloads', async () => {
    // unique origin: the ensure registry is module-global and page-lifetime
    const origin = 'http://127.0.0.1:65501'
    const fetch = vi.fn()
    const first = ensureHttpPullTransport({ origin, fetch })
    vi.resetModules()
    const reloaded = await import('./transport.js')
    const second = reloaded.ensureHttpPullTransport({ origin, fetch })
    expect(second).toBe(first)
    expect(() =>
      reloaded.ensureHttpPullTransport({ origin, fetch, pullIntervalMs: 999 })
    ).toThrow('already installed with different pullIntervalMs')
    first.uninstall()
  })

  test('createZeroClientTransport installs the shared transport for a Zero server', () => {
    const origin = 'http://127.0.0.1:65503'
    const fetch = vi.fn()
    const plugin = createZeroClientTransport({ fetch, pullIntervalMs: 1_000 })

    expect(plugin.type).toBe('orez-client')
    const first = plugin.install(origin)
    expect(plugin.install(origin)).toBe(first)
    first.uninstall()
  })

  test('ensureHttpPullTransport rejects a conflicting codec for one origin', () => {
    const origin = 'http://127.0.0.1:65502'
    const fetch = vi.fn()
    const codec = (id: string): PayloadCodec => ({
      id,
      async encodePush(body) {
        return body
      },
      async decodePull(response) {
        return response
      },
    })
    const first = ensureHttpPullTransport({
      origin,
      fetch,
      payloadCodec: codec('codec-a'),
    })

    expect(
      ensureHttpPullTransport({
        origin,
        fetch,
        payloadCodec: codec('codec-a'),
      })
    ).toBe(first)
    expect(() =>
      ensureHttpPullTransport({
        origin,
        fetch,
        payloadCodec: codec('codec-b'),
      })
    ).toThrow('already installed with different payloadCodecID')
    first.uninstall()
  })

  test('push responses are filtered to this client — foreign (recovery) results are dropped', async () => {
    // a mutation-RECOVERY push carries a previous client's pending mutations
    // and the server echoes that old clientID in the response. zero-cache only
    // delivers a client its own results; forwarding the raw response trips
    // zero's "received mutation for the wrong client" assert and kills the
    // connection (reproduced against soot's ultimate e2e, 2026-06-12).
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = recordRequest(input, init)
      if (request.path === '/pull') {
        return jsonResponse({ cookie: request.body.cookie, unchanged: true })
      }
      const mutation = request.body.mutations[0]
      return jsonResponse({
        pushResponse: {
          mutations: [
            {
              id: { clientID: 'previous-session-client', id: 7 },
              result: {
                error: 'alreadyProcessed',
                details: 'recovered mutation already applied',
              },
            },
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

    const mutation = zero.mutate.project.create({
      id: 'p-recovery',
      ownerId: 'u1',
      name: 'recovered',
    })
    // the client's own result must resolve; the foreign result must be
    // dropped instead of asserting the connection dead.
    await mutation.client
    await mutation.server
  })

  test('pull 409 (client cookie ahead of server) resets client state instead of reconnect-looping', async () => {
    // a 409 means the server's change-tracking watermark is BEHIND the
    // client's cookie — the server lost/reset its state (replica reset). the
    // transport must surface zero-cache's InvalidConnectionRequestBaseCookie
    // error so the stock client drops local state and calls
    // onClientStateNotFound, instead of failing the socket and retrying the
    // same future cookie forever.
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = recordRequest(input, init)
      expect(request.path).toBe('/pull')
      return jsonResponse(
        { error: 'future cookie 299 is ahead of server cookie 66' },
        { status: 409 }
      )
    })
    install(fetch)
    const onClientStateNotFound = vi.fn()
    createZero({ onClientStateNotFound })
    await eventually(() => expect(onClientStateNotFound).toHaveBeenCalled(), 5_000)
  })

  test('a rate-limited push waits out the server Retry-After instead of storming', async () => {
    // without a backoff frame this loop measured 605 push attempts per second:
    // a 429 closed the fake socket 1011, stock Zero swallows AbruptClose without
    // sleeping, and every reconnect re-pushed the same pending mutation — which
    // is what kept the client permanently rate limited.
    const pushes: number[] = []
    const pulls: number[] = []
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = recordRequest(input, init)
      if (request.path === '/pull') {
        pulls.push(Date.now())
        return jsonResponse({ cookie: request.body.cookie, unchanged: true })
      }
      pushes.push(Date.now())
      return jsonResponse(
        {
          kind: 'MutationRateLimited',
          message: 'rate limit exceeded',
          retryAfterMs: 8_000,
          rule: 'zero.light.minute',
        },
        { status: 429, headers: { 'retry-after': '8' } }
      )
    })
    install(fetch)
    const zero = createZero()
    void zero.mutate.project
      .create({ id: 'p-429', ownerId: 'u1', name: 'rate limited' })
      .server.catch(() => {})
    await eventually(() => expect(pushes.length).toBeGreaterThan(0), 5_000)
    await sleep(6_500)

    // 8s Retry-After beats Zero's 5s floor, so the retry is still pending.
    expect(pushes).toHaveLength(1)
    expect(pulls).toHaveLength(0)
    // and a rate limit is transient: the client must still be trying, not
    // parked in the terminal error/needs-auth states.
    expect(['connecting', 'disconnected', 'connected']).toContain(
      zero.connection.state.current.name
    )
  }, 20_000)

  test('a failing pull backs off instead of reconnecting flat out', async () => {
    // same storm shape as the 429, measured at 374 pulls per second before the
    // backoff frame. a deployment whose /pull 500s must not be DDoSed by its
    // own clients.
    const pulls: number[] = []
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      recordRequest(input, init)
      pulls.push(Date.now())
      return jsonResponse({ error: 'injected outage' }, { status: 500 })
    })
    install(fetch)
    const zero = createZero()
    await eventually(() => expect(pulls.length).toBeGreaterThan(0), 5_000)
    await sleep(6_000)

    expect(pulls.length).toBeLessThanOrEqual(2)
    expect(['connecting', 'disconnected', 'connected']).toContain(
      zero.connection.state.current.name
    )
  }, 20_000)
})

function install(fetch: typeof globalThis.fetch) {
  const transport = installHttpPullTransport({ origin: ORIGIN, fetch })
  transports.push(transport)
  return transport
}

function createTransportEncryptionCodec() {
  return createEncryptedColumnCodec({
    manifest: transportEncryptionManifest,
    keyring: {
      current: async () => ({ epoch: 4, key: transportEncryptionKey }),
      get: async (epoch) => (epoch === 4 ? transportEncryptionKey : undefined),
    },
  })
}

function createZero(
  options: {
    pingTimeoutMs?: number
    onClientStateNotFound?: () => void
    logSink?: {
      log(
        level: string,
        context: Record<string, unknown> | undefined,
        ...args: unknown[]
      ): void
    }
  } = {}
) {
  const zero = new Zero({
    server: ORIGIN,
    userID: 'u1',
    auth: 'token-u1',
    schema: zeroHttpFixtureSchema,
    kvStore: 'mem',
    storageKey: `zero-http-test-${++storageID}`,
    mutators: zeroHttpFixtureMutators,
    pingTimeoutMs: options.pingTimeoutMs,
    onClientStateNotFound: options.onClientStateNotFound,
    logLevel: options.logSink ? 'debug' : undefined,
    logSink: options.logSink,
  })
  zeros.push(zero)
  return zero
}

function createEncryptedZero() {
  const zero = new Zero({
    server: ORIGIN,
    userID: 'u1',
    auth: 'token-u1',
    schema: zeroHttpFixtureSchema,
    kvStore: 'mem',
    storageKey: `zero-http-encryption-test-${++storageID}`,
    mutators: encryptionFixtureMutators,
  })
  zeros.push(zero)
  return zero
}

function recordRequest(input: RequestInfo | URL, init?: RequestInit): RequestRecord {
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

function jsonResponse(body: unknown, init?: ResponseInit) {
  return new Response(JSON.stringify(body), {
    status: init?.status ?? 200,
    statusText: init?.statusText,
    headers: {
      'content-type': 'application/json',
      ...(init?.headers as Record<string, string> | undefined),
    },
  })
}

function unchangedPullFetch() {
  return vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const request = recordRequest(input, init)
    expect(request.path).toBe('/pull')
    return jsonResponse({ cookie: request.body.cookie, unchanged: true })
  })
}

function useFakeNativeWebSocket() {
  const previous = globalThis.WebSocket
  const sockets: Array<{
    url: string
    onmessage: (() => void) | null
    onclose: (() => void) | null
    onerror: (() => void) | null
  }> = []

  class FakeNativeWebSocket {
    static CONNECTING = 0
    static OPEN = 1
    static CLOSING = 2
    static CLOSED = 3

    readonly url: string
    onmessage: (() => void) | null = null
    onclose: (() => void) | null = null
    onerror: (() => void) | null = null

    constructor(url: string | URL) {
      this.url = String(url)
      sockets.push(this)
    }

    close() {
      this.onclose?.()
    }
  }

  globalThis.WebSocket = FakeNativeWebSocket as unknown as typeof WebSocket
  restoreNativeWebSocket = () => {
    globalThis.WebSocket = previous
  }
  return sockets
}

async function waitForComplete<T>(view: {
  addListener(listener: (data: any, resultType: string) => void): () => void
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
  return openRawSocketWithMessages().messages
}

function openRawSocketWithMessages(opts?: {
  authToken?: string
  desiredQueriesPatch?: unknown[]
  wsid?: string
  attemptStartedAt?: number
}) {
  const url = new URL(`${ORIGIN}/sync/v51/connect`)
  url.protocol = 'wss:'
  url.searchParams.set('clientID', 'c1')
  url.searchParams.set('clientGroupID', 'cg1')
  url.searchParams.set('userID', 'u1')
  url.searchParams.set('baseCookie', '')
  url.searchParams.set('lmid', '0')
  url.searchParams.set('wsid', opts?.wsid ?? 'ws-test')
  if (opts?.attemptStartedAt !== undefined) {
    url.searchParams.set('ts', String(opts.attemptStartedAt))
  }
  const messages: Array<[string, any]> = []
  const socket = new WebSocket(
    url,
    encodeSecProtocol(
      ['initConnection', { desiredQueriesPatch: opts?.desiredQueriesPatch ?? [] }],
      opts?.authToken ?? 'token-u1'
    )
  )
  socket.addEventListener('message', (event) => {
    messages.push(JSON.parse(String(event.data)))
  })
  return { messages, socket }
}

function pushBody(id: number) {
  return {
    clientGroupID: 'cg1',
    pushVersion: 1,
    requestID: `push-${id}`,
    timestamp: Date.now(),
    mutations: [
      {
        type: 'custom',
        name: 'project|create',
        id,
        clientID: 'c1',
        args: [{ id: `p${id}`, ownerId: 'u1', name: `project ${id}` }],
      },
    ],
  }
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

// installs the transport with the query-aware extension on (a queryTransform
// resolver), pushing it for afterEach cleanup.
function installWithQueries(
  fetch: typeof globalThis.fetch,
  queryTransform: (name: string, args: readonly unknown[]) => unknown
) {
  const transport = installHttpPullTransport({ origin: ORIGIN, fetch, queryTransform })
  transports.push(transport)
  return transport
}

describe('zero-http query-aware extension', () => {
  test('ships desired queries with pull state (transformed AST) and emits server got-query ack', async () => {
    const requests: RequestRecord[] = []
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = recordRequest(input, init)
      requests.push(request)
      return jsonResponse({
        cookie: 1,
        lastMutationIDChanges: {},
        rowsPatch: [{ op: 'clear' }],
        gotQueries: { version: 1, patch: [{ op: 'put', hash: 'h1' }] },
      })
    })
    installWithQueries(fetch, (name, args) => ({ resolved: name, args }))

    const { messages } = openRawSocketWithMessages({
      desiredQueriesPatch: [
        { op: 'put', hash: 'h1', name: 'byOwner', args: [{ ownerId: 'u1' }] },
      ],
    })

    await eventually(() => expect(requests.length).toBeGreaterThan(0))
    // the pull body ships {queries:{version, patch:[{op:'put',hash,ast}]}} with
    // name+args resolved to the AST by the transform
    expect(requests[0].body.queries).toEqual({
      version: 1,
      patch: [
        {
          op: 'put',
          hash: 'h1',
          ast: { resolved: 'byOwner', args: [{ ownerId: 'u1' }] },
        },
      ],
    })
    // the server's got-query ack is emitted to the client (not synthesized)
    await eventually(() => {
      const poke = messages.find(
        (m) => m[0] === 'pokePart' && Array.isArray(m[1].gotQueriesPatch)
      )
      expect(poke?.[1].gotQueriesPatch).toEqual([{ op: 'put', hash: 'h1' }])
    })
  })

  test('an ad-hoc put with an inline ast ships that ast unchanged', async () => {
    const requests: RequestRecord[] = []
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      requests.push(recordRequest(input, init))
      return jsonResponse({ cookie: 1, unchanged: true })
    })
    // a transform that would throw if called — an inline ast must bypass it
    installWithQueries(fetch, () => {
      throw new Error('transform should not run for an inline ast')
    })

    openRawSocketWithMessages({
      desiredQueriesPatch: [{ op: 'put', hash: 'h2', ast: { inline: true } }],
    })

    await eventually(() => expect(requests.length).toBeGreaterThan(0))
    expect(requests[0].body.queries.patch).toEqual([
      { op: 'put', hash: 'h2', ast: { inline: true } },
    ])
  })

  test('reconnect re-ships the desired set', async () => {
    const requests: RequestRecord[] = []
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      requests.push(recordRequest(input, init))
      // never ack the query version, so the desire stays un-acked
      return jsonResponse({ cookie: 1, unchanged: true })
    })
    installWithQueries(fetch, (name) => ({ resolved: name }))

    const desired = [{ op: 'put', hash: 'h1', name: 'byOwner', args: [] }]
    const first = openRawSocketWithMessages({ desiredQueriesPatch: desired })
    await eventually(() => expect(requests.length).toBeGreaterThan(0))
    first.socket.close()

    const before = requests.length
    openRawSocketWithMessages({ desiredQueriesPatch: desired })
    await eventually(() => expect(requests.length).toBeGreaterThan(before))
    // the reconnected socket re-ships the (still un-acked) desired query
    const replay = requests[requests.length - 1].body.queries
    expect(replay.patch).toEqual([
      { op: 'put', hash: 'h1', ast: { resolved: 'byOwner' } },
    ])
  })

  test('queryForward ships name+args for the server to resolve (no client transform)', async () => {
    const requests: RequestRecord[] = []
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      requests.push(recordRequest(input, init))
      return jsonResponse({ cookie: 1, unchanged: true })
    })
    // queryForward on, no transform: the permission transform stays server-side
    const transport = installHttpPullTransport({
      origin: ORIGIN,
      fetch,
      queryForward: true,
    })
    transports.push(transport)

    openRawSocketWithMessages({
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: 'h3',
          name: 'byOwner',
          args: [{ ownerId: 'u1' }],
          // Zero may include a locally evaluable AST on a named query put.
          // queryForward must discard it so the server remains authoritative.
          ast: { table: 'secret' },
        },
      ],
    })

    await eventually(() => expect(requests.length).toBeGreaterThan(0))
    expect(requests[0].body.queries.patch).toEqual([
      { op: 'put', hash: 'h3', name: 'byOwner', args: [{ ownerId: 'u1' }] },
    ])
  })
})
