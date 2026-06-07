import { describe, expect, it, vi } from 'vitest'

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
  url: '/sync/v50/connect?clientID=a',
  headers: { upgrade: 'websocket' },
  method: 'GET',
}

describe('DurableObjectWebSocketHandoff', () => {
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
    expect(cfSocket.addEventListener).toHaveBeenCalledWith('message', expect.any(Function))
    expect(cfSocket.addEventListener).toHaveBeenCalledWith('close', expect.any(Function))
    expect(cfSocket.addEventListener).toHaveBeenCalledWith('error', expect.any(Function))
    expect((cfSocket as any).serializeAttachment).toBeUndefined()

    localSockets[0].send('first poke')
    expect(cfSocket.send).toHaveBeenCalledWith('first poke')
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
      server: { emit: vi.fn() },
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
