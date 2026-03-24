// NOTE THIS IS NOT OREZ NODE THIS IS NOT A GOOD REFERENCE BECAUSE ITS OUR EARLY GUESS AT WHAT COULD WORK
// DO NOT STUDY THIS, THE OTHER STUFF IN SRC IS WHERE YOU EANT TO LOOK

/**
 * ws (websocket) shim for cloudflare workers.
 *
 * wraps CF Workers WebSocket (from WebSocketPair) to implement the
 * ws npm package API that zero-cache uses. enables bundler aliasing
 * so zero-cache's WebSocket handling works with CF durable WebSockets.
 *
 * usage with bundler alias:
 *   alias: { 'ws': './src/worker/shims/ws.js' }
 */

import EventEmitter from 'node:events'
import { Duplex } from 'node:stream'

// -- readyState constants --
const CONNECTING = 0
const OPEN = 1
const CLOSING = 2
const CLOSED = 3

// -- CF WebSocket interface (minimal) --
interface CFWebSocket {
  send(data: string | ArrayBuffer | ArrayBufferView): void
  close(code?: number, reason?: string): void
  addEventListener(type: string, handler: (event: any) => void): void
  removeEventListener(type: string, handler: (event: any) => void): void
  readyState: number
  accept?(): void
}

// -- WebSocket shim --
// wraps a CF WebSocket to match the ws package WebSocket API

class WebSocket extends EventEmitter {
  static readonly CONNECTING = CONNECTING
  static readonly OPEN = OPEN
  static readonly CLOSING = CLOSING
  static readonly CLOSED = CLOSED

  readonly CONNECTING = CONNECTING
  readonly OPEN = OPEN
  readonly CLOSING = CLOSING
  readonly CLOSED = CLOSED

  #ws!: CFWebSocket
  #url: string
  #listeners = new Map<string, (event: any) => void>()

  constructor(urlOrSocket: string | CFWebSocket, _protocols?: unknown, _opts?: unknown) {
    super()

    if (typeof urlOrSocket === 'string') {
      this.#url = urlOrSocket
      // check for in-process connections (fastify shim uses port 0 or 1)
      const parsedUrl = new URL(urlOrSocket, 'http://localhost')
      const isInProcess =
        parsedUrl.port === '0' ||
        parsedUrl.port === '1' ||
        parsedUrl.hostname === 'localhost'

      if (isInProcess) {
        // in-process: connect via fastify server's handoff mechanism
        const fastifyInstance = (globalThis as any).__orez_fastify_instance
        if (fastifyInstance?.server) {
          // create paired message channels for bidirectional communication
          // the client-side WS (this) and serverWs are cross-linked so
          // ping/pong, messages, and close propagate between them
          const clientSide = this
          const serverWs: any = {
            readyState: 1,
            _listeners: {} as Record<string, Function[]>,
            send: (data: string | ArrayBuffer) => {
              // deliver to client side
              queueMicrotask(() => clientSide.emit('message', data))
            },
            close: (code?: number, reason?: string) => {
              serverWs.readyState = 3
              queueMicrotask(() => clientSide.emit('close', code || 1000, reason || ''))
            },
            ping: () => {
              // ping from server → deliver 'ping' event to client
              // (expectPingsForLiveness listens for 'ping', not 'pong')
              queueMicrotask(() => clientSide.emit('ping'))
            },
            addEventListener: (type: string, handler: Function) => {
              if (!serverWs._listeners[type]) serverWs._listeners[type] = []
              serverWs._listeners[type].push(handler)
            },
            removeEventListener: (type: string, handler: Function) => {
              const arr = serverWs._listeners[type]
              if (arr) {
                const idx = arr.indexOf(handler)
                if (idx >= 0) arr.splice(idx, 1)
              }
            },
          }

          this.#ws = {
            accept: () => {},
            send: (data: string | ArrayBuffer) => {
              // deliver to server side
              const handlers = serverWs._listeners['message'] || []
              for (const h of handlers) h({ data })
            },
            close: (code?: number, reason?: string) => {
              const handlers = serverWs._listeners['close'] || []
              for (const h of handlers) h({ code, reason })
            },
            addEventListener: () => {},
            removeEventListener: () => {},
            get readyState() {
              return 1
            },
          } as CFWebSocket

          // emit handoff to fastify server
          const path = parsedUrl.pathname + parsedUrl.search
          queueMicrotask(() => {
            fastifyInstance.server.emit(
              'message',
              [
                'handoff',
                {
                  message: { url: path, headers: {}, method: 'GET' },
                  head: new Uint8Array(0),
                },
              ],
              serverWs
            )
            this.emit('open')
          })
        } else {
          // no fastify instance — emit close immediately
          queueMicrotask(() => this.emit('close', 1006, 'no fastify server'))
        }
      } else if (typeof globalThis.WebSocket === 'function') {
        // real outbound WebSocket for external connections
        const nativeWs = new globalThis.WebSocket(urlOrSocket) as any
        this.#ws = {
          accept: () => {},
          send: (data: string | ArrayBuffer) => nativeWs.send(data),
          close: (code?: number, reason?: string) => nativeWs.close(code, reason),
          addEventListener: (type: string, handler: (event: any) => void) =>
            nativeWs.addEventListener(type, handler),
          removeEventListener: (type: string, handler: (event: any) => void) =>
            nativeWs.removeEventListener(type, handler),
          get readyState() {
            return nativeWs.readyState
          },
        } as CFWebSocket
        nativeWs.addEventListener('open', () => this.emit('open'))
        nativeWs.addEventListener('message', (ev: MessageEvent) =>
          this.emit('message', ev.data)
        )
        nativeWs.addEventListener('close', (ev: CloseEvent) =>
          this.emit('close', ev.code, ev.reason)
        )
        nativeWs.addEventListener('error', (ev: Event) =>
          this.emit('error', new Error('WebSocket error'))
        )
      } else {
        throw new Error(
          'ws shim: outbound WebSocket connections not yet supported. ' +
            'use the CF Workers fetch API for outbound WebSocket.'
        )
      }
    } else {
      this.#ws = urlOrSocket
      this.#url = ''
      this.#setupListeners()
    }
  }

  get url(): string {
    return this.#url
  }

  get readyState(): number {
    return this.#ws.readyState
  }

  send(
    data: string | Buffer | ArrayBuffer | ArrayBufferView,
    cb?: (err?: Error) => void
  ): void {
    try {
      if (typeof data === 'string') {
        this.#ws.send(data)
      } else if (Buffer.isBuffer(data)) {
        this.#ws.send(new Uint8Array(data.buffer, data.byteOffset, data.byteLength))
      } else if (data instanceof ArrayBuffer) {
        this.#ws.send(data)
      } else if (ArrayBuffer.isView(data)) {
        this.#ws.send(new Uint8Array(data.buffer, data.byteOffset, data.byteLength))
      } else {
        this.#ws.send(String(data))
      }
      cb?.()
    } catch (err) {
      cb?.(err as Error)
    }
  }

  close(code?: number, reason?: string): void {
    try {
      this.#ws.close(code, reason)
    } catch {
      // socket may already be closed
    }
  }

  terminate(): void {
    // real ws.terminate() destroys the socket without a close frame.
    // CF Workers don't expose raw socket destroy, so close with 1000.
    this.close(1000)
  }

  ping(_data?: unknown, _mask?: boolean, _cb?: () => void): void {
    // forward ping to underlying socket if it supports it (in-process pairs)
    if (typeof (this.#ws as any).ping === 'function') {
      ;(this.#ws as any).ping()
    }
    // also emit pong locally (CF WebSockets handle ping/pong at platform level)
    this.emit('pong')
  }

  // standard EventTarget-style addEventListener (used by Connection)
  addEventListener(type: string, handler: (event: any) => void): void {
    // wrap to emit EventEmitter-style
    this.on(type, handler)
  }

  removeEventListener(type: string, handler: (event: any) => void): void {
    this.off(type, handler)
  }

  #setupListeners(): void {
    // match ws npm package event signatures:
    //   message: (data: Buffer|string, isBinary: boolean)
    //   close: (code: number, reason: string)
    //   error: (err: Error)
    const onMessage = (event: any) => {
      const data = event.data
      this.emit('message', data, typeof data !== 'string')
    }
    const onClose = (event: any) => {
      this.emit('close', event.code ?? 1000, event.reason ?? '')
    }
    const onError = (event: any) => {
      this.emit('error', event.error ?? new Error(event.message ?? 'WebSocket error'))
    }
    const onOpen = () => {
      this.emit('open')
    }

    this.#ws.addEventListener('message', onMessage)
    this.#ws.addEventListener('close', onClose)
    this.#ws.addEventListener('error', onError)
    this.#ws.addEventListener('open', onOpen)

    this.#listeners.set('message', onMessage)
    this.#listeners.set('close', onClose)
    this.#listeners.set('error', onError)
    this.#listeners.set('open', onOpen)
  }
}

// -- WebSocketServer shim --
// zero-cache uses WebSocketServer with { noServer: true } for handleUpgrade

class WebSocketServer extends EventEmitter {
  constructor(_opts?: { noServer?: boolean }) {
    super()
  }

  close(cb?: (err?: Error) => void): void {
    // no-op — browser embed has no real server to close
    cb?.()
  }

  /**
   * handle a WebSocket upgrade. on CF Workers the upgrade is already done
   * (WebSocketPair), so this just wraps the CF WebSocket in our shim.
   *
   * @param message - the HTTP request (IncomingMessage-like object)
   * @param socket - the underlying socket (CF WebSocket on CF Workers)
   * @param head - upgrade head buffer
   * @param callback - receives the wrapped WebSocket
   */
  handleUpgrade(
    _message: unknown,
    socket: CFWebSocket | unknown,
    _head: unknown,
    callback: (ws: WebSocket) => void
  ): void {
    // wrap the CF WebSocket in our shim
    const ws = new WebSocket(socket as CFWebSocket)
    callback(ws)
  }
}

// -- createWebSocketStream --
// creates a Node.js Duplex stream from a WebSocket.
// used by zero-cache's Connection class for streaming messages.

function createWebSocketStream(
  ws: WebSocket,
  _opts?: { decodeStrings?: boolean }
): Duplex {
  const duplex = new Duplex({
    objectMode: false,
    decodeStrings: false,

    read() {
      // data is pushed from ws message events
    },

    write(
      chunk: Buffer | string,
      _encoding: string,
      callback: (err?: Error | null) => void
    ) {
      try {
        ws.send(typeof chunk === 'string' ? chunk : chunk.toString(), callback)
      } catch (err) {
        callback(err as Error)
      }
    },

    destroy(err: Error | null, callback: (err?: Error | null) => void) {
      ws.close()
      callback(err)
    },
  })

  // pipe ws messages into the readable side
  ws.on('message', (event: any) => {
    const data = event?.data ?? event
    if (typeof data === 'string') {
      duplex.push(data)
    } else if (data instanceof ArrayBuffer) {
      duplex.push(Buffer.from(data))
    } else if (ArrayBuffer.isView(data)) {
      duplex.push(Buffer.from(data.buffer, data.byteOffset, data.byteLength))
    } else {
      duplex.push(String(data))
    }
  })

  ws.on('close', () => {
    duplex.push(null) // signal end of readable
    duplex.destroy()
  })

  ws.on('error', (event: any) => {
    const err = event?.error ?? new Error(event?.message ?? 'WebSocket error')
    duplex.destroy(err)
  })

  return duplex
}

export default WebSocket
export { WebSocket, WebSocketServer, createWebSocketStream }
