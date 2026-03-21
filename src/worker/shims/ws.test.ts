import { describe, it, expect, beforeEach, vi } from 'vitest'

import WebSocket, { WebSocketServer, createWebSocketStream } from './ws.js'

/** mock CF WebSocket — mimics the server side of a WebSocketPair */
function createMockCFWebSocket() {
  const listeners = new Map<string, Array<(event: any) => void>>()

  const ws = {
    readyState: 1, // OPEN
    send: vi.fn(),
    close: vi.fn(() => {
      ws.readyState = 3 // CLOSED
    }),
    accept: vi.fn(),
    addEventListener: vi.fn((type: string, handler: (event: any) => void) => {
      if (!listeners.has(type)) listeners.set(type, [])
      listeners.get(type)!.push(handler)
    }),
    removeEventListener: vi.fn((type: string, handler: (event: any) => void) => {
      const arr = listeners.get(type)
      if (arr) {
        const idx = arr.indexOf(handler)
        if (idx >= 0) arr.splice(idx, 1)
      }
    }),

    // helper to fire events in tests
    _fire(type: string, event: any) {
      for (const h of listeners.get(type) || []) h(event)
    },
  }

  return ws
}

describe('WebSocket shim', () => {
  let cfWs: ReturnType<typeof createMockCFWebSocket>
  let ws: InstanceType<typeof WebSocket>

  beforeEach(() => {
    cfWs = createMockCFWebSocket()
    ws = new WebSocket(cfWs as any)
  })

  describe('constructor', () => {
    it('wraps a CF WebSocket', () => {
      expect(ws).toBeInstanceOf(WebSocket)
    })

    it('handles string URL for localhost (in-process)', () => {
      // no longer throws — localhost URLs use the in-process path
      // without a fastify instance it emits close instead of throwing
      const ws = new WebSocket('ws://localhost')
      expect(ws).toBeInstanceOf(WebSocket)
    })

    it('sets up event listeners on CF WebSocket', () => {
      expect(cfWs.addEventListener).toHaveBeenCalledWith('message', expect.any(Function))
      expect(cfWs.addEventListener).toHaveBeenCalledWith('close', expect.any(Function))
      expect(cfWs.addEventListener).toHaveBeenCalledWith('error', expect.any(Function))
      expect(cfWs.addEventListener).toHaveBeenCalledWith('open', expect.any(Function))
    })
  })

  describe('static constants', () => {
    it('has readyState constants', () => {
      expect(WebSocket.CONNECTING).toBe(0)
      expect(WebSocket.OPEN).toBe(1)
      expect(WebSocket.CLOSING).toBe(2)
      expect(WebSocket.CLOSED).toBe(3)
    })

    it('has instance readyState constants', () => {
      expect(ws.CONNECTING).toBe(0)
      expect(ws.OPEN).toBe(1)
      expect(ws.CLOSING).toBe(2)
      expect(ws.CLOSED).toBe(3)
    })
  })

  describe('readyState', () => {
    it('reflects CF WebSocket readyState', () => {
      cfWs.readyState = 1
      expect(ws.readyState).toBe(1)
      cfWs.readyState = 3
      expect(ws.readyState).toBe(3)
    })
  })

  describe('send()', () => {
    it('sends string data', () => {
      ws.send('hello')
      expect(cfWs.send).toHaveBeenCalledWith('hello')
    })

    it('sends ArrayBuffer data', () => {
      const buf = new ArrayBuffer(4)
      ws.send(buf)
      expect(cfWs.send).toHaveBeenCalledWith(buf)
    })

    it('sends Uint8Array data', () => {
      const arr = new Uint8Array([1, 2, 3])
      ws.send(arr)
      expect(cfWs.send).toHaveBeenCalledWith(expect.any(Uint8Array))
    })

    it('sends Buffer data as Uint8Array', () => {
      const buf = Buffer.from('hello')
      ws.send(buf)
      expect(cfWs.send).toHaveBeenCalledWith(expect.any(Uint8Array))
    })

    it('calls callback on success', () => {
      const cb = vi.fn()
      ws.send('test', cb)
      expect(cb).toHaveBeenCalledWith()
    })

    it('calls callback with error on failure', () => {
      cfWs.send.mockImplementation(() => {
        throw new Error('send failed')
      })
      const cb = vi.fn()
      ws.send('test', cb)
      expect(cb).toHaveBeenCalledWith(expect.any(Error))
    })
  })

  describe('close()', () => {
    it('closes the CF WebSocket', () => {
      ws.close(1000, 'normal')
      expect(cfWs.close).toHaveBeenCalledWith(1000, 'normal')
    })

    it('does not throw if already closed', () => {
      cfWs.close.mockImplementation(() => {
        throw new Error('already closed')
      })
      expect(() => ws.close()).not.toThrow()
    })
  })

  describe('terminate()', () => {
    it('closes with code 1000', () => {
      ws.terminate()
      expect(cfWs.close).toHaveBeenCalled()
      expect(cfWs.close.mock.calls[0][0]).toBe(1000)
    })
  })

  describe('ping()', () => {
    it('emits pong (CF handles ping at platform level)', () => {
      const pongHandler = vi.fn()
      ws.on('pong', pongHandler)
      ws.ping()
      expect(pongHandler).toHaveBeenCalled()
    })
  })

  describe('event forwarding', () => {
    it('emits message events from CF WebSocket', () => {
      const handler = vi.fn()
      ws.on('message', handler)
      cfWs._fire('message', { data: 'hello' })
      expect(handler).toHaveBeenCalledWith({ data: 'hello' })
    })

    it('emits close events from CF WebSocket', () => {
      const handler = vi.fn()
      ws.on('close', handler)
      cfWs._fire('close', { code: 1000, reason: 'normal', wasClean: true })
      expect(handler).toHaveBeenCalledWith({
        code: 1000,
        reason: 'normal',
        wasClean: true,
      })
    })

    it('emits error events from CF WebSocket', () => {
      const handler = vi.fn()
      ws.on('error', handler)
      cfWs._fire('error', { message: 'oops', error: new Error('oops') })
      expect(handler).toHaveBeenCalledWith(expect.objectContaining({ message: 'oops' }))
    })

    it('supports addEventListener/removeEventListener', () => {
      const handler = vi.fn()
      ws.addEventListener('message', handler)
      cfWs._fire('message', { data: 'test' })
      expect(handler).toHaveBeenCalledTimes(1)

      ws.removeEventListener('message', handler)
      cfWs._fire('message', { data: 'test2' })
      expect(handler).toHaveBeenCalledTimes(1)
    })
  })
})

describe('WebSocketServer shim', () => {
  it('creates with noServer option', () => {
    const wss = new WebSocketServer({ noServer: true })
    expect(wss).toBeInstanceOf(WebSocketServer)
  })

  describe('handleUpgrade()', () => {
    it('wraps CF WebSocket and calls callback', () => {
      const wss = new WebSocketServer({ noServer: true })
      const cfWs = createMockCFWebSocket()
      const callback = vi.fn()

      wss.handleUpgrade({ url: '/test', headers: {} }, cfWs, Buffer.alloc(0), callback)

      expect(callback).toHaveBeenCalledTimes(1)
      const ws = callback.mock.calls[0][0]
      expect(ws).toBeInstanceOf(WebSocket)
    })

    it('wrapped WebSocket delegates send to CF WebSocket', () => {
      const wss = new WebSocketServer({ noServer: true })
      const cfWs = createMockCFWebSocket()
      let wrappedWs: InstanceType<typeof WebSocket>

      wss.handleUpgrade({}, cfWs, null, (ws) => {
        wrappedWs = ws
      })

      wrappedWs!.send('hello from server')
      expect(cfWs.send).toHaveBeenCalledWith('hello from server')
    })
  })
})

describe('createWebSocketStream', () => {
  it('creates a duplex stream from WebSocket', () => {
    const cfWs = createMockCFWebSocket()
    const ws = new WebSocket(cfWs as any)
    const stream = createWebSocketStream(ws)

    expect(stream).toBeDefined()
    expect(stream.readable).toBe(true)
    expect(stream.writable).toBe(true)
  })

  it('writes to WebSocket via stream', async () => {
    const cfWs = createMockCFWebSocket()
    const ws = new WebSocket(cfWs as any)
    const stream = createWebSocketStream(ws)

    await new Promise<void>((resolve) => {
      stream.write('hello', () => {
        expect(cfWs.send).toHaveBeenCalledWith('hello')
        stream.destroy()
        resolve()
      })
    })
  })

  it('reads from WebSocket messages', async () => {
    const cfWs = createMockCFWebSocket()
    const ws = new WebSocket(cfWs as any)
    const stream = createWebSocketStream(ws)

    const received = new Promise<string>((resolve) => {
      stream.on('data', (chunk) => {
        resolve(chunk.toString())
      })
    })

    cfWs._fire('message', { data: 'world' })

    const data = await received
    expect(data).toBe('world')
    stream.destroy()
  })

  it('ends stream on WebSocket close', async () => {
    const cfWs = createMockCFWebSocket()
    const ws = new WebSocket(cfWs as any)
    const stream = createWebSocketStream(ws)

    const closed = new Promise<void>((resolve) => {
      stream.on('close', () => resolve())
    })

    cfWs._fire('close', { code: 1000 })
    await closed
  })
})
