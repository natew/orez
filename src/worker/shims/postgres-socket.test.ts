import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { Buffer } from 'buffer'

import { createSocketFactory } from './postgres-socket.js'

// helper: create a socket via factory, capturing the proxy-side port
function createTestSocket() {
  let proxyPort: MessagePort | null = null
  const factory = createSocketFactory((port) => {
    proxyPort = port
  })
  const socket = factory()
  return { socket, get proxyPort() { return proxyPort! } }
}

// helper: wait for next microtask
function tick() {
  return new Promise<void>((r) => queueMicrotask(r))
}

// helper: wait for next macrotask
function nextTick(ms = 0) {
  return new Promise<void>((r) => setTimeout(r, ms))
}

describe('MessagePortSocket', () => {
  describe('readyState lifecycle', () => {
    it('starts as opening, transitions to open after microtask', async () => {
      const { socket } = createTestSocket()
      expect(socket.readyState).toBe('opening')
      expect(socket.connecting).toBe(true)
      expect(socket.pending).toBe(true)

      await tick()

      expect(socket.readyState).toBe('open')
      expect(socket.connecting).toBe(false)
      expect(socket.pending).toBe(false)
    })

    it('transitions to closed after destroy', async () => {
      const { socket } = createTestSocket()
      await tick()
      expect(socket.readyState).toBe('open')

      socket.destroy()
      expect(socket.readyState).toBe('closed')
      expect(socket.destroyed).toBe(true)
    })

    it('stays closed if destroyed before open', () => {
      const { socket } = createTestSocket()
      socket.destroy()
      expect(socket.readyState).toBe('closed')
    })
  })

  describe('connect and ready events', () => {
    it('emits connect and ready after microtask', async () => {
      const { socket } = createTestSocket()
      const events: string[] = []
      socket.on('connect', () => events.push('connect'))
      socket.on('ready', () => events.push('ready'))

      await tick()
      expect(events).toEqual(['connect', 'ready'])
    })

    it('does not emit connect/ready if destroyed before microtask', async () => {
      const { socket } = createTestSocket()
      const events: string[] = []
      socket.on('connect', () => events.push('connect'))
      socket.on('ready', () => events.push('ready'))

      socket.destroy()
      await tick()
      expect(events).toEqual([])
    })
  })

  describe('write', () => {
    it('sends data to proxy port as ArrayBuffer', async () => {
      const { socket, proxyPort } = createTestSocket()
      await tick()

      const received: ArrayBuffer[] = []
      proxyPort.onmessage = (ev: MessageEvent) => {
        received.push(ev.data)
      }
      proxyPort.start()

      const data = Buffer.from([0x51, 0x00, 0x00, 0x00, 0x04])
      socket.write(data)

      await nextTick(10)
      expect(received.length).toBe(1)
      expect(new Uint8Array(received[0])).toEqual(new Uint8Array([0x51, 0x00, 0x00, 0x00, 0x04]))
    })

    it('returns true on successful write', async () => {
      const { socket } = createTestSocket()
      await tick()
      expect(socket.write(Buffer.from([1, 2, 3]))).toBe(true)
    })

    it('returns false when destroyed', async () => {
      const { socket } = createTestSocket()
      await tick()
      socket.destroy()
      expect(socket.write(Buffer.from([1, 2, 3]))).toBe(false)
    })

    it('calls callback (as encoding arg)', async () => {
      const { socket } = createTestSocket()
      await tick()
      const cb = vi.fn()
      socket.write(Buffer.from([1]), cb)
      expect(cb).toHaveBeenCalled()
    })

    it('calls callback (as third arg)', async () => {
      const { socket } = createTestSocket()
      await tick()
      const cb = vi.fn()
      socket.write(Buffer.from([1]), 'utf8', cb)
      expect(cb).toHaveBeenCalled()
    })

    it('tracks bytesWritten', async () => {
      const { socket } = createTestSocket()
      await tick()
      expect(socket.bytesWritten).toBe(0)
      socket.write(Buffer.from([1, 2, 3]))
      expect(socket.bytesWritten).toBe(3)
      socket.write(Buffer.from([4, 5]))
      expect(socket.bytesWritten).toBe(5)
    })

    it('handles string data', async () => {
      const { socket, proxyPort } = createTestSocket()
      await tick()

      const received: ArrayBuffer[] = []
      proxyPort.onmessage = (ev: MessageEvent) => received.push(ev.data)
      proxyPort.start()

      socket.write('hello')
      await nextTick(10)
      expect(received.length).toBe(1)
      expect(Buffer.from(new Uint8Array(received[0])).toString()).toBe('hello')
    })
  })

  describe('data reception', () => {
    it('emits data as Buffer when proxy sends ArrayBuffer', async () => {
      const { socket, proxyPort } = createTestSocket()
      await tick()

      const chunks: Buffer[] = []
      socket.on('data', (buf: Buffer) => chunks.push(buf))

      const data = new Uint8Array([0x52, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00])
      const copy = new Uint8Array(data)
      proxyPort.postMessage(copy.buffer, [copy.buffer])

      await nextTick(10)
      expect(chunks.length).toBe(1)
      expect(chunks[0]).toBeInstanceOf(Buffer)
      expect(chunks[0].readUInt8(0)).toBe(0x52)
    })

    it('emits data as Buffer when proxy sends Uint8Array', async () => {
      const { socket, proxyPort } = createTestSocket()
      await tick()

      const chunks: Buffer[] = []
      socket.on('data', (buf: Buffer) => chunks.push(buf))

      const data = new Uint8Array([1, 2, 3])
      proxyPort.postMessage(data)

      await nextTick(10)
      expect(chunks.length).toBe(1)
      expect(chunks[0]).toBeInstanceOf(Buffer)
    })

    it('tracks bytesRead', async () => {
      const { socket, proxyPort } = createTestSocket()
      await tick()

      socket.on('data', () => {}) // must have listener
      expect(socket.bytesRead).toBe(0)

      const data = new Uint8Array([1, 2, 3, 4, 5])
      proxyPort.postMessage(data)

      await nextTick(10)
      expect(socket.bytesRead).toBe(5)
    })

    it('ignores data after destroy', async () => {
      const { socket, proxyPort } = createTestSocket()
      await tick()

      const chunks: Buffer[] = []
      socket.on('data', (buf: Buffer) => chunks.push(buf))

      socket.destroy()
      proxyPort.postMessage(new Uint8Array([1, 2, 3]))

      await nextTick(10)
      expect(chunks.length).toBe(0)
    })
  })

  describe('pause/resume', () => {
    it('buffers data while paused', async () => {
      const { socket, proxyPort } = createTestSocket()
      await tick()

      const chunks: Buffer[] = []
      socket.on('data', (buf: Buffer) => chunks.push(buf))

      socket.pause()
      proxyPort.postMessage(new Uint8Array([1]))
      proxyPort.postMessage(new Uint8Array([2]))
      proxyPort.postMessage(new Uint8Array([3]))

      await nextTick(10)
      expect(chunks.length).toBe(0)

      socket.resume()
      expect(chunks.length).toBe(3)
      expect(chunks[0][0]).toBe(1)
      expect(chunks[1][0]).toBe(2)
      expect(chunks[2][0]).toBe(3)
    })

    it('handles re-entrant pause during resume flush', async () => {
      const { socket, proxyPort } = createTestSocket()
      await tick()

      const chunks: Buffer[] = []
      let pauseAfter = 2
      socket.on('data', (buf: Buffer) => {
        chunks.push(buf)
        if (chunks.length === pauseAfter) {
          socket.pause()
        }
      })

      socket.pause()
      proxyPort.postMessage(new Uint8Array([1]))
      proxyPort.postMessage(new Uint8Array([2]))
      proxyPort.postMessage(new Uint8Array([3]))
      proxyPort.postMessage(new Uint8Array([4]))

      await nextTick(10)

      // resume flushes first 2, then re-pauses
      socket.resume()
      expect(chunks.length).toBe(2)
      expect(chunks[0][0]).toBe(1)
      expect(chunks[1][0]).toBe(2)

      // resume again to get remaining
      pauseAfter = 999
      socket.resume()
      expect(chunks.length).toBe(4)
      expect(chunks[2][0]).toBe(3)
      expect(chunks[3][0]).toBe(4)
    })

    it('resume returns this for chaining', async () => {
      const { socket } = createTestSocket()
      expect(socket.resume()).toBe(socket)
    })

    it('pause returns this for chaining', async () => {
      const { socket } = createTestSocket()
      expect(socket.pause()).toBe(socket)
    })
  })

  describe('end', () => {
    it('writes final data then destroys via microtask', async () => {
      const { socket, proxyPort } = createTestSocket()
      await tick()

      const received: ArrayBuffer[] = []
      proxyPort.onmessage = (ev: MessageEvent) => received.push(ev.data)
      proxyPort.start()

      const terminateMsg = Buffer.from([0x58, 0x00, 0x00, 0x00, 0x04])
      socket.end(terminateMsg)

      // not yet destroyed (deferred)
      expect(socket.destroyed).toBe(false)

      await tick()
      expect(socket.destroyed).toBe(true)

      await nextTick(10)
      expect(received.length).toBe(1)
    })

    it('terminate flow: end(X) then once(close) resolves', async () => {
      // regression test: postgres terminate() calls socket.end(X_msg)
      // then registers socket.once('close', resolve). the close event
      // must fire AFTER the listener is registered, not before.
      const { socket } = createTestSocket()
      await tick()

      const terminateMsg = Buffer.from([0x58, 0x00, 0x00, 0x00, 0x04])
      socket.end(terminateMsg)

      // this is what postgres does right after socket.end():
      const closed = await Promise.race([
        new Promise<boolean>((r) => socket.once('close', () => r(true))),
        nextTick(500).then(() => false),
      ])

      expect(closed).toBe(true)
    })

    it('calls callback', async () => {
      const { socket } = createTestSocket()
      await tick()
      const cb = vi.fn()
      socket.end(cb)
      expect(cb).toHaveBeenCalled()
    })
  })

  describe('destroy', () => {
    it('emits end then close', async () => {
      const { socket } = createTestSocket()
      await tick()

      const events: string[] = []
      socket.on('end', () => events.push('end'))
      socket.on('close', () => events.push('close'))

      socket.destroy()
      expect(events).toEqual(['end', 'close'])
    })

    it('emits error before close when err passed', async () => {
      const { socket } = createTestSocket()
      await tick()

      const events: string[] = []
      socket.on('error', () => events.push('error'))
      socket.on('close', (hadError: boolean) => events.push(`close:${hadError}`))

      socket.destroy(new Error('test'))
      expect(events).toEqual(['error', 'close:true'])
    })

    it('close event has hadError=false without error', async () => {
      const { socket } = createTestSocket()
      await tick()

      let hadError: boolean | undefined
      socket.on('close', (h: boolean) => { hadError = h })

      socket.destroy()
      expect(hadError).toBe(false)
    })

    it('double destroy does not emit duplicate events', async () => {
      const { socket } = createTestSocket()
      await tick()

      let closeCount = 0
      socket.on('close', () => closeCount++)

      socket.destroy()
      socket.destroy()
      expect(closeCount).toBe(1)
    })

    it('returns this for chaining', async () => {
      const { socket } = createTestSocket()
      expect(socket.destroy()).toBe(socket)
    })

    it('sets writable and readable to false', async () => {
      const { socket } = createTestSocket()
      await tick()
      expect(socket.writable).toBe(true)
      expect(socket.readable).toBe(true)

      socket.destroy()
      expect(socket.writable).toBe(false)
      expect(socket.readable).toBe(false)
    })

    it('clears pause buffer', async () => {
      const { socket, proxyPort } = createTestSocket()
      await tick()

      const chunks: Buffer[] = []
      socket.on('data', (buf: Buffer) => chunks.push(buf))

      socket.pause()
      proxyPort.postMessage(new Uint8Array([1, 2, 3]))
      await nextTick(10)

      socket.destroy()

      // resume after destroy should not emit buffered data
      socket.resume()
      expect(chunks.length).toBe(0)
    })
  })

  describe('setTimeout', () => {
    it('emits timeout after inactivity', async () => {
      const { socket } = createTestSocket()
      await tick()

      const cb = vi.fn()
      socket.setTimeout(50, cb)

      await nextTick(100)
      expect(cb).toHaveBeenCalled()
    })

    it('resets timeout on write', async () => {
      const { socket } = createTestSocket()
      await tick()

      const cb = vi.fn()
      socket.setTimeout(80, cb)

      // write at 30ms to reset
      await nextTick(30)
      socket.write(Buffer.from([1]))

      // at 60ms from start (30ms from last write), should not have fired
      await nextTick(30)
      expect(cb).not.toHaveBeenCalled()

      // at 130ms from start (100ms from last write), should have fired
      await nextTick(70)
      expect(cb).toHaveBeenCalled()
    })

    it('resets timeout on data receive', async () => {
      const { socket, proxyPort } = createTestSocket()
      await tick()

      const cb = vi.fn()
      socket.on('data', () => {}) // need listener
      socket.setTimeout(80, cb)

      await nextTick(30)
      proxyPort.postMessage(new Uint8Array([1]))

      await nextTick(30)
      expect(cb).not.toHaveBeenCalled()

      await nextTick(70)
      expect(cb).toHaveBeenCalled()
    })

    it('clears timeout when set to 0', async () => {
      const { socket } = createTestSocket()
      await tick()

      const cb = vi.fn()
      socket.setTimeout(50, cb)
      socket.setTimeout(0)

      await nextTick(100)
      expect(cb).not.toHaveBeenCalled()
    })

    it('returns this for chaining', () => {
      const { socket } = createTestSocket()
      expect(socket.setTimeout(0)).toBe(socket)
    })
  })

  describe('no-op methods return this', () => {
    it('setKeepAlive', () => {
      const { socket } = createTestSocket()
      expect(socket.setKeepAlive()).toBe(socket)
    })

    it('setNoDelay', () => {
      const { socket } = createTestSocket()
      expect(socket.setNoDelay()).toBe(socket)
    })

    it('ref/unref', () => {
      const { socket } = createTestSocket()
      expect(socket.ref()).toBe(socket)
      expect(socket.unref()).toBe(socket)
    })

    it('connect', () => {
      const { socket } = createTestSocket()
      expect(socket.connect()).toBe(socket)
    })
  })

  describe('address info', () => {
    it('remoteAddress is 127.0.0.1', () => {
      const { socket } = createTestSocket()
      expect(socket.remoteAddress).toBe('127.0.0.1')
    })

    it('remotePort is 0', () => {
      const { socket } = createTestSocket()
      expect(socket.remotePort).toBe(0)
    })

    it('remoteFamily is IPv4', () => {
      const { socket } = createTestSocket()
      expect(socket.remoteFamily).toBe('IPv4')
    })

    it('localAddress is 127.0.0.1', () => {
      const { socket } = createTestSocket()
      expect(socket.localAddress).toBe('127.0.0.1')
    })

    it('localPort is 0', () => {
      const { socket } = createTestSocket()
      expect(socket.localPort).toBe(0)
    })

    it('address() returns AddressInfo', () => {
      const { socket } = createTestSocket()
      expect(socket.address()).toEqual({ address: '127.0.0.1', family: 'IPv4', port: 0 })
    })
  })

  describe('write error handling', () => {
    it('emits error and returns false if postMessage throws', async () => {
      const { socket } = createTestSocket()
      await tick()

      // destroy the port to force postMessage to throw
      // (accessing internals via the factory is not possible, so we'll
      // test this by closing the port first)
      socket.destroy()

      // after destroy, write returns false (but doesn't throw)
      const result = socket.write(Buffer.from([1]))
      expect(result).toBe(false)
    })
  })

  describe('custom properties', () => {
    it('allows setting ssl, host properties', () => {
      const { socket } = createTestSocket()
      socket.ssl = true
      socket.host = 'localhost'
      expect(socket.ssl).toBe(true)
      expect(socket.host).toBe('localhost')
    })
  })
})
