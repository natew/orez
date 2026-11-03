import { afterEach, describe, expect, test } from 'vitest'

import { installHttpPullTransport } from './transport.js'

const ORIGIN = 'https://chat-zero-flush.test'
const transports: Array<{ uninstall(): void }> = []
const sockets: WebSocket[] = []

afterEach(() => {
  while (sockets.length) sockets.pop()?.close()
  while (transports.length) transports.pop()?.uninstall()
})

describe('http pull transport flush', () => {
  test('waits for a queued push before reporting the transport flushed', async () => {
    const pushStarted = deferred<void>()
    const releasePush = deferred<void>()
    const fetchImpl = async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(String(input))
      const body = JSON.parse(String(init?.body))
      if (url.pathname === '/pull') {
        return jsonResponse({ cookie: body.cookie, unchanged: true })
      }

      pushStarted.resolve()
      await releasePush.promise
      const mutation = body.mutations[0]
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
    }
    const transport = installHttpPullTransport({
      origin: ORIGIN,
      fetch: fetchImpl as unknown as typeof globalThis.fetch,
    })
    transports.push(transport)
    const socket = openSocket()
    sockets.push(socket)
    await new Promise<void>((resolve) => socket.addEventListener('open', () => resolve()))

    socket.send(JSON.stringify(['push', pushBody()]))
    let flushed = false
    const flush = transport.flush().then(() => {
      flushed = true
    })
    await pushStarted.promise
    expect(flushed).toBe(false)

    releasePush.resolve()
    await flush
    expect(flushed).toBe(true)
  })

  test('rejects the flush when its queued push fails', async () => {
    const fetchImpl = async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(String(input))
      const body = JSON.parse(String(init?.body))
      if (url.pathname === '/pull') {
        return jsonResponse({ cookie: body.cookie, unchanged: true })
      }
      throw new TypeError('offline')
    }
    const transport = installHttpPullTransport({
      origin: ORIGIN,
      fetch: fetchImpl as unknown as typeof globalThis.fetch,
    })
    transports.push(transport)
    const socket = openSocket()
    sockets.push(socket)
    await new Promise<void>((resolve) => socket.addEventListener('open', () => resolve()))

    socket.send(JSON.stringify(['push', pushBody()]))
    await expect(transport.flush()).rejects.toThrow('offline')
  })

  test('rejects stale flushes across two socket replacements', async () => {
    const pushes = new Map(
      ['c1', 'c2', 'c3'].map((clientID) => [
        clientID,
        { started: deferred<void>(), release: deferred<void>() },
      ])
    )
    const fetchImpl = async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(String(input))
      const body = JSON.parse(String(init?.body))
      if (url.pathname === '/pull') {
        return jsonResponse({ cookie: body.cookie, unchanged: true })
      }
      const clientID = body.mutations[0].clientID as string
      const push = pushes.get(clientID)
      if (!push) throw new Error(`unexpected client ${clientID}`)
      push.started.resolve()
      await push.release.promise
      return jsonResponse({
        pushResponse: {
          mutations: [
            {
              id: { clientID, id: body.mutations[0].id },
              result: {},
            },
          ],
        },
      })
    }
    const transport = installHttpPullTransport({
      origin: ORIGIN,
      fetch: fetchImpl as unknown as typeof globalThis.fetch,
    })
    transports.push(transport)

    const first = await openTrackedSocket('c1')
    first.send(JSON.stringify(['push', pushBody('c1')]))
    const firstFlush = transport.flush()
    await pushes.get('c1')?.started.promise

    first.close()
    const second = await openTrackedSocket('c2')
    second.send(JSON.stringify(['push', pushBody('c2')]))
    pushes.get('c1')?.release.resolve()
    await expect(firstFlush).rejects.toThrow('transport changed during flush')

    const secondFlush = transport.flush()
    await pushes.get('c2')?.started.promise
    second.close()
    const third = await openTrackedSocket('c3')
    third.send(JSON.stringify(['push', pushBody('c3')]))
    pushes.get('c2')?.release.resolve()
    await expect(secondFlush).rejects.toThrow('transport changed during flush')

    let finalFlushed = false
    const finalFlush = transport.flush().then(() => {
      finalFlushed = true
    })
    await pushes.get('c3')?.started.promise
    expect(finalFlushed).toBe(false)
    pushes.get('c3')?.release.resolve()
    await finalFlush
    expect(finalFlushed).toBe(true)
  })

  test('waits for a recovery push queued during the final flush pull', async () => {
    const finalPullStarted = deferred<void>()
    const releaseFinalPull = deferred<void>()
    const pushStarted = deferred<void>()
    const releasePush = deferred<void>()
    let flushing = false
    let flushPulls = 0
    const fetchImpl = async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(String(input))
      const body = JSON.parse(String(init?.body))
      if (url.pathname === '/pull') {
        if (flushing && ++flushPulls === 2) {
          finalPullStarted.resolve()
          await releaseFinalPull.promise
        }
        return jsonResponse({ cookie: body.cookie, unchanged: true })
      }

      pushStarted.resolve()
      await releasePush.promise
      const mutation = body.mutations[0]
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
    }
    const transport = installHttpPullTransport({
      origin: ORIGIN,
      fetch: fetchImpl as unknown as typeof globalThis.fetch,
    })
    transports.push(transport)
    const socket = await openTrackedSocket('late-push')
    await transport.pull()

    flushing = true
    let flushed = false
    const flush = transport.flush().then(() => {
      flushed = true
    })
    await finalPullStarted.promise

    socket.send(JSON.stringify(['push', pushBody('late-push')]))
    await pushStarted.promise
    releaseFinalPull.resolve()
    await new Promise<void>((resolve) => setTimeout(resolve, 0))
    expect(flushed).toBe(false)

    releasePush.resolve()
    await flush
    expect(flushed).toBe(true)
  })
})

async function openTrackedSocket(clientID: string) {
  const socket = openSocket(clientID)
  sockets.push(socket)
  await new Promise<void>((resolve) => socket.addEventListener('open', () => resolve()))
  return socket
}

function openSocket(clientID = 'c1') {
  const url = new URL(`${ORIGIN}/sync/v51/connect`)
  url.protocol = 'wss:'
  url.searchParams.set('clientID', clientID)
  url.searchParams.set('clientGroupID', 'cg1')
  url.searchParams.set('userID', 'u1')
  url.searchParams.set('baseCookie', '')
  url.searchParams.set('lmid', '0')
  url.searchParams.set('wsid', 'ws-test')
  return new WebSocket(
    url,
    encodeURIComponent(
      Buffer.from(
        JSON.stringify({
          initConnectionMessage: ['initConnection', { desiredQueriesPatch: [] }],
          authToken: 'token-u1',
        })
      ).toString('base64')
    )
  )
}

function pushBody(clientID = 'c1') {
  return {
    clientGroupID: 'cg1',
    pushVersion: 1,
    requestID: 'push-1',
    timestamp: Date.now(),
    mutations: [
      {
        type: 'custom',
        name: 'message|insert',
        id: 1,
        clientID,
        args: [{ id: 'm1' }],
      },
    ],
  }
}

function jsonResponse(body: unknown) {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  })
}

function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}
