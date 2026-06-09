import { afterEach, describe, expect, it, vi } from 'vitest'

import {
  DurableObjectWebSocketHandoff,
  type HandoffRequestMessage,
} from './durable-object-websocket-handoff.js'

function createMockSocket() {
  const listeners = new Map<string, Array<(event: any) => void>>()
  const socket: any = {
    readyState: 1,
    accept: vi.fn(),
    send: vi.fn(),
    close: undefined,
    closedWith: undefined as { code?: number; reason?: string } | undefined,
    addEventListener: vi.fn((type: string, handler: (event: any) => void) => {
      if (!listeners.has(type)) listeners.set(type, [])
      listeners.get(type)!.push(handler)
    }),
    removeEventListener: vi.fn((type: string, handler: (event: any) => void) => {
      const handlers = listeners.get(type)
      if (!handlers) return
      const index = handlers.indexOf(handler)
      if (index >= 0) handlers.splice(index, 1)
    }),
    fire(type: string, event: any) {
      for (const handler of [...(listeners.get(type) ?? [])]) handler(event)
    },
  }
  socket.close = vi.fn((code?: number, reason?: string) => {
    socket.readyState = 3
    socket.closedWith = { code, reason }
    socket.fire('close', { code, reason, wasClean: true })
  })
  return socket
}

const requestMessage: HandoffRequestMessage = {
  url: '/sync/v51/connect?clientID=a',
  headers: { upgrade: 'websocket' },
  method: 'GET',
}

describe('DurableObjectWebSocketHandoff', () => {
  afterEach(() => {
    vi.useRealTimers()
  })

  it('accepts the CF socket through the standard WebSocket API', () => {
    const cfSocket = createMockSocket()
    const localSockets: any[] = []
    const handoff = new DurableObjectWebSocketHandoff(() => ({
      tryHandoff: vi.fn((_msg, socket) => {
        localSockets.push(socket)
        return true
      }),
    }))

    expect(handoff.accept(cfSocket, requestMessage)).toBe(true)

    expect(cfSocket.accept).toHaveBeenCalledTimes(1)
    expect(cfSocket.addEventListener).toHaveBeenCalledWith(
      'message',
      expect.any(Function)
    )
    expect(cfSocket.addEventListener).toHaveBeenCalledWith('close', expect.any(Function))
    expect(cfSocket.addEventListener).toHaveBeenCalledWith('error', expect.any(Function))
    expect((cfSocket as any).serializeAttachment).toBeUndefined()

    localSockets[0].send('first poke')
    expect(cfSocket.send).toHaveBeenCalledWith('first poke')
  })

  it('keeps the request context alive until the first server frame', async () => {
    const cfSocket = createMockSocket()
    const localSockets: any[] = []
    const waitUntilPromises: Promise<unknown>[] = []
    const ctx = {
      waitUntil: vi.fn((promise: Promise<unknown>) => waitUntilPromises.push(promise)),
    }
    const handoff = new DurableObjectWebSocketHandoff(() => ({
      tryHandoff: vi.fn((_msg, socket) => {
        localSockets.push(socket)
        return true
      }),
    }))

    expect(handoff.accept(cfSocket, requestMessage, ctx)).toBe(true)
    expect(ctx.waitUntil).toHaveBeenCalledTimes(1)

    localSockets[0].send('connected')
    await expect(waitUntilPromises[0]).resolves.toBeUndefined()
  })

  it('releases the request context if zero-cache does not send promptly', async () => {
    vi.useFakeTimers()
    const cfSocket = createMockSocket()
    const waitUntilPromises: Promise<unknown>[] = []
    const ctx = {
      waitUntil: vi.fn((promise: Promise<unknown>) => waitUntilPromises.push(promise)),
    }
    const handoff = new DurableObjectWebSocketHandoff(() => ({
      tryHandoff: vi.fn(() => true),
    }))

    expect(handoff.accept(cfSocket, requestMessage, ctx)).toBe(true)

    let resolved = false
    waitUntilPromises[0].then(() => {
      resolved = true
    })
    vi.advanceTimersByTime(9_999)
    await Promise.resolve()
    expect(resolved).toBe(false)
    vi.advanceTimersByTime(1)
    await Promise.resolve()
    expect(resolved).toBe(true)
  })

  it('routes the /sync handoff via server.emit when no {websocket:true} route matches', () => {
    // regression: the /sync/v*/connect path is served by the ZeroDispatcher's
    // server.onMessageType('handoff') listener, NOT a fastify {websocket:true}
    // route — so tryHandoff returns false for it. without the server.emit
    // fallback the socket is accepted but never reaches zero-cache.
    delete (globalThis as any).__orez_fastify_instances
    const cfSocket = createMockSocket()
    const dispatcher = {
      tryHandoff: vi.fn(() => false),
      server: { emit: vi.fn(() => true) },
    }
    const handoff = new DurableObjectWebSocketHandoff(() => dispatcher)

    expect(handoff.accept(cfSocket, requestMessage)).toBe(true)

    expect(dispatcher.server.emit).toHaveBeenCalledWith(
      'message',
      ['handoff', { message: requestMessage, head: expect.any(Uint8Array) }],
      expect.any(Object)
    )
    expect(cfSocket.closedWith).toBeUndefined()
  })

  it('does not claim /sync handoff success before the dispatcher server exists', () => {
    // Zero's runner can expose a stale Fastify server before ZeroDispatcher
    // attaches its handoff listener. that server has only the shim's route
    // listener, so emitting there would accept the browser socket without ever
    // delivering it to zero-cache.
    const previousInstances = (globalThis as any).__orez_fastify_instances
    const cfSocket = createMockSocket()
    const staleServer = {
      emit: vi.fn(() => false),
      listenerCount: vi.fn(() => 1),
    }
    const stale = {
      tryHandoff: vi.fn(() => false),
      server: staleServer,
    }
    ;(globalThis as any).__orez_fastify_instances = [stale]
    const handoff = new DurableObjectWebSocketHandoff(() => stale)

    try {
      expect(handoff.accept(cfSocket, requestMessage)).toBe(false)
      expect(staleServer.emit).not.toHaveBeenCalled()
      expect(cfSocket.closedWith).toBeUndefined()
    } finally {
      ;(globalThis as any).__orez_fastify_instances = previousInstances
    }
  })

  it('routes /sync through the dispatcher server when the captured fallback is stale', () => {
    // Zero's runner sends "ready" before constructing ZeroDispatcher, so the
    // embed can capture a Fastify instance that is not the dispatcher. The
    // dispatcher server is distinguishable because installWebSocketHandoff adds
    // a second handoff message listener beyond the shim's route listener.
    const previousInstances = (globalThis as any).__orez_fastify_instances
    const cfSocket = createMockSocket()
    const staleServer = {
      emit: vi.fn(),
      listenerCount: vi.fn(() => 1),
    }
    const dispatcherServer = {
      emit: vi.fn(() => true),
      listenerCount: vi.fn(() => 2),
    }
    const stale = {
      tryHandoff: vi.fn(() => false),
      server: staleServer,
    }
    const dispatcher = {
      tryHandoff: vi.fn(() => false),
      server: dispatcherServer,
    }
    ;(globalThis as any).__orez_fastify_instances = [stale, dispatcher]
    const handoff = new DurableObjectWebSocketHandoff(() => stale)

    try {
      expect(handoff.accept(cfSocket, requestMessage)).toBe(true)

      expect(staleServer.emit).not.toHaveBeenCalled()
      expect(dispatcherServer.emit).toHaveBeenCalledWith(
        'message',
        ['handoff', { message: requestMessage, head: expect.any(Uint8Array) }],
        expect.any(Object)
      )
      expect(cfSocket.closedWith).toBeUndefined()
    } finally {
      ;(globalThis as any).__orez_fastify_instances = previousInstances
    }
  })

  it('routes peer messages into the local zero-cache socket', () => {
    const cfSocket = createMockSocket()
    let localSocket: any
    const handoff = new DurableObjectWebSocketHandoff(() => ({
      tryHandoff: vi.fn((_msg, socket) => {
        localSocket = socket
        return true
      }),
    }))
    handoff.accept(cfSocket, requestMessage)

    const onMessage = vi.fn()
    localSocket.addEventListener('message', onMessage)
    cfSocket.fire('message', { data: 'from browser' })

    expect(onMessage).toHaveBeenCalledWith({ data: 'from browser' })
    localSocket.close(1000, 'test complete')
  })

  it('tracks activeConnections across accept and close', () => {
    const handoff = new DurableObjectWebSocketHandoff(() => ({
      tryHandoff: vi.fn(() => true),
    }))
    expect(handoff.activeConnections).toBe(0)

    const a = createMockSocket()
    const b = createMockSocket()
    handoff.accept(a, requestMessage)
    expect(handoff.activeConnections).toBe(1)
    handoff.accept(b, requestMessage)
    expect(handoff.activeConnections).toBe(2)

    a.fire('close', { code: 1001, reason: 'gone', wasClean: true })
    expect(handoff.activeConnections).toBe(1)
    b.fire('close', { code: 1001, reason: 'gone', wasClean: true })
    expect(handoff.activeConnections).toBe(0)
  })

  it('does not retain a connection when the handoff is not consumed', () => {
    // a socket that no fastify instance claims is closed immediately, so it must
    // not leave a phantom live connection that would block idle hibernation.
    delete (globalThis as any).__orez_fastify_instances
    const handoff = new DurableObjectWebSocketHandoff(() => ({
      tryHandoff: vi.fn(() => false),
    }))
    const cfSocket = createMockSocket()
    expect(handoff.accept(cfSocket, requestMessage)).toBe(false)
    expect(handoff.activeConnections).toBe(0)
  })

  it('routes peer close and zero-cache close through the bridge', () => {
    const cfSocket = createMockSocket()
    let localSocket: any
    const handoff = new DurableObjectWebSocketHandoff(() => ({
      tryHandoff: vi.fn((_msg, socket) => {
        localSocket = socket
        return true
      }),
    }))
    handoff.accept(cfSocket, requestMessage)

    const onClose = vi.fn()
    localSocket.addEventListener('close', onClose)
    cfSocket.fire('close', { code: 1001, reason: 'browser closed', wasClean: true })
    expect(onClose).toHaveBeenCalledWith({
      code: 1001,
      reason: 'browser closed',
      wasClean: true,
    })

    const secondSocket = createMockSocket()
    handoff.accept(secondSocket, requestMessage)
    localSocket.close(1000, 'done')
    expect(secondSocket.close).toHaveBeenCalledWith(1000, 'done')
  })
})
