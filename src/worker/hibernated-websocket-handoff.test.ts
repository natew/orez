import { describe, expect, it, vi } from 'vitest'

import {
  HibernatedWebSocketHandoff,
  type HandoffAttachment,
  type HandoffRequestMessage,
} from './hibernated-websocket-handoff.js'

function createMockSocket() {
  let attachment: HandoffAttachment | undefined
  const socket: any = {
    readyState: 1,
    accept: vi.fn(),
    send: vi.fn(),
    close: undefined,
    closedWith: undefined as { code?: number; reason?: string } | undefined,
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    serializeAttachment: vi.fn((value: HandoffAttachment) => {
      attachment = value
    }),
    deserializeAttachment: vi.fn(() => attachment),
    get attachment() {
      return attachment
    },
  }
  socket.close = vi.fn((code?: number, reason?: string) => {
    socket.readyState = 3
    socket.closedWith = { code, reason }
  })
  return socket
}

const requestMessage: HandoffRequestMessage = {
  url: '/sync/v50/connect?clientID=a',
  headers: { upgrade: 'websocket' },
  method: 'GET',
}

describe('HibernatedWebSocketHandoff', () => {
  it('accepts the CF socket through Durable Object hibernation', () => {
    const cfSocket = createMockSocket()
    const durableObjectState = { acceptWebSocket: vi.fn() }
    const localSockets: any[] = []
    const handoff = new HibernatedWebSocketHandoff(() => ({
      tryHandoff: vi.fn((_msg, socket) => {
        localSockets.push(socket)
        return true
      }),
    }))

    expect(handoff.accept(durableObjectState, cfSocket, requestMessage)).toBe(true)

    expect(durableObjectState.acceptWebSocket).toHaveBeenCalledWith(cfSocket)
    expect(cfSocket.accept).not.toHaveBeenCalled()
    expect(cfSocket.serializeAttachment).toHaveBeenCalledWith(
      expect.objectContaining({
        type: 'orez-zero-cache-handoff-v1',
        message: requestMessage,
      })
    )

    localSockets[0].send('first poke')
    expect(cfSocket.send).toHaveBeenCalledWith('first poke')
  })

  it('routes hibernated messages into the local zero-cache socket', () => {
    const cfSocket = createMockSocket()
    const durableObjectState = { acceptWebSocket: vi.fn() }
    let localSocket: any
    const handoff = new HibernatedWebSocketHandoff(() => ({
      tryHandoff: vi.fn((_msg, socket) => {
        localSocket = socket
        return true
      }),
    }))
    handoff.accept(durableObjectState, cfSocket, requestMessage)

    const onMessage = vi.fn()
    localSocket.addEventListener('message', onMessage)
    handoff.handleMessage(cfSocket, 'from browser')

    expect(onMessage).toHaveBeenCalledWith({ data: 'from browser' })
  })

  it('recreates the local socket from serialized attachment after hibernation', () => {
    const cfSocket = createMockSocket()
    const durableObjectState = { acceptWebSocket: vi.fn() }
    const initial = new HibernatedWebSocketHandoff(() => ({
      tryHandoff: vi.fn(() => true),
    }))
    initial.accept(durableObjectState, cfSocket, requestMessage)

    const onMessage = vi.fn()
    const tryHandoff = vi.fn((_msg, socket: any) => {
      socket.addEventListener('message', onMessage)
      return true
    })
    const restored = new HibernatedWebSocketHandoff(() => ({ tryHandoff }))

    restored.handleMessage(cfSocket, 'wake message')

    expect(tryHandoff).toHaveBeenCalledWith(
      { message: requestMessage, head: expect.any(Uint8Array) },
      expect.any(Object)
    )
    expect(onMessage).toHaveBeenCalledWith({ data: 'wake message' })
  })

  it('routes peer close and zero-cache close through the bridge', () => {
    const cfSocket = createMockSocket()
    const durableObjectState = { acceptWebSocket: vi.fn() }
    let localSocket: any
    const handoff = new HibernatedWebSocketHandoff(() => ({
      tryHandoff: vi.fn((_msg, socket) => {
        localSocket = socket
        return true
      }),
    }))
    handoff.accept(durableObjectState, cfSocket, requestMessage)

    const onClose = vi.fn()
    localSocket.addEventListener('close', onClose)
    handoff.handleClose(cfSocket, 1001, 'browser closed', true)
    expect(onClose).toHaveBeenCalledWith({
      code: 1001,
      reason: 'browser closed',
      wasClean: true,
    })

    const secondSocket = createMockSocket()
    handoff.accept(durableObjectState, secondSocket, requestMessage)
    localSocket.close(1000, 'done')
    expect(secondSocket.close).toHaveBeenCalledWith(1000, 'done')
  })
})
