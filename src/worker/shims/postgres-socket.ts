/**
 * MessagePort-backed socket for the postgres npm package.
 *
 * the postgres package (porsager/postgres) accepts a custom socket factory
 * via options.socket. this provides a net.Socket-compatible object backed
 * by a MessagePort that connects to pg-proxy-browser.
 *
 * usage:
 *   import postgres from 'postgres'
 *   import { createSocketFactory } from 'orez/worker/shims/postgres-socket'
 *
 *   const sql = postgres({
 *     socket: createSocketFactory(proxyPort),
 *     // ... other options
 *   })
 *
 * this replaces the postgres.ts shim entirely — the real postgres package
 * speaks wire protocol to pg-proxy-browser, just like orez-node speaks
 * wire protocol to pg-proxy over TCP.
 */

import { Buffer } from 'buffer'
import { EventEmitter } from 'events'

/**
 * create a socket factory for the postgres npm package.
 * each call to the factory creates a new MessageChannel,
 * gives one port to the proxy via connectFn, and returns
 * a Socket-like object backed by the other port.
 */
export function createSocketFactory(
  connectFn: (port: MessagePort) => void
) {
  return () => new MessagePortSocket(connectFn)
}

/**
 * net.Socket-compatible object backed by MessagePort.
 * implements the full interface that the postgres package uses,
 * plus reasonable net.Socket spec compliance for other consumers.
 */
class MessagePortSocket extends EventEmitter {
  private port: MessagePort | null = null
  private channel: MessageChannel | null = null
  private _destroyed = false
  private _ended = false
  private _readyState: 'opening' | 'open' | 'closed' = 'opening'

  // pause/resume buffering for COPY protocol backpressure
  private _paused = false
  private _pauseBuffer: Buffer[] = []

  // timeout tracking
  private _timeoutMs = 0
  private _timeoutTimer: ReturnType<typeof setTimeout> | null = null

  // net.Socket compat properties
  writable = true
  readable = true
  bytesRead = 0
  bytesWritten = 0

  // postgres may write these on native sockets (skipped for custom, but allow assignment)
  ssl?: boolean
  host?: string
  // port is already used by MessagePort field, use _pgPort for postgres assignment
  // actually postgres only writes these for non-custom sockets, so we just need
  // the property to be settable without error

  constructor(private connectFn: (port: MessagePort) => void) {
    super()
    this.channel = new MessageChannel()
    this.port = this.channel.port1

    // give server port to pg-proxy-browser
    this.connectFn(this.channel.port2)

    // forward incoming data from proxy — wrap as Buffer (postgres needs readUInt32BE etc.)
    this.port.onmessage = (ev: MessageEvent) => {
      if (this._destroyed) return

      let buf: Buffer | null = null
      if (ev.data instanceof ArrayBuffer) {
        buf = Buffer.from(new Uint8Array(ev.data))
      } else if (ev.data instanceof Uint8Array) {
        buf = Buffer.from(ev.data)
      }

      if (!buf) return

      this.bytesRead += buf.length
      this._resetTimeout()

      if (this._paused) {
        this._pauseBuffer.push(buf)
        return
      }

      this.emit('data', buf)
    }

    this.port.start()

    // transition to open and fire connect event async
    // for custom sockets postgres calls connected() directly (skips socket.on('connect')),
    // but we emit for generic socket compat
    queueMicrotask(() => {
      if (!this._destroyed) {
        this._readyState = 'open'
        this.emit('connect')
        this.emit('ready')
      }
    })
  }

  get destroyed() {
    return this._destroyed
  }

  get readyState(): string {
    return this._readyState
  }

  get connecting() {
    return this._readyState === 'opening'
  }

  get pending() {
    return this._readyState === 'opening'
  }

  // postgres calls socket.write(chunk, fn) — returns boolean for backpressure
  write(data: Uint8Array | Buffer | string, encoding?: any, callback?: Function): boolean {
    if (this._destroyed || !this.port) {
      if (typeof encoding === 'function') encoding()
      else if (typeof callback === 'function') callback()
      return false
    }

    const bytes: Uint8Array = typeof data === 'string'
      ? Buffer.from(data)
      : data instanceof Uint8Array ? data : Buffer.from(data)

    // copy before transfer — postgres may reference the buffer after write
    const copy = new Uint8Array(bytes.length)
    copy.set(bytes)

    try {
      this.port.postMessage(copy.buffer, [copy.buffer])
    } catch (err) {
      queueMicrotask(() => this.emit('error', err))
      if (typeof encoding === 'function') encoding()
      else if (typeof callback === 'function') callback()
      return false
    }

    this.bytesWritten += bytes.length
    this._resetTimeout()

    if (typeof encoding === 'function') encoding()
    else if (typeof callback === 'function') callback()

    return true
  }

  // postgres calls socket.end(terminateMsg) in terminate() then
  // registers socket.once('close', resolve). defer destroy so the
  // 'close' listener is registered before the event fires.
  end(data?: any, encoding?: any, callback?: Function) {
    if (typeof data === 'function') {
      callback = data
      data = undefined
      encoding = undefined
    } else if (typeof encoding === 'function') {
      callback = encoding
      encoding = undefined
    }

    if (data != null) this.write(data, encoding)
    this._ended = true

    // defer destroy to next microtask — terminate() calls socket.end(X_msg)
    // then line 408 of connection.js does socket.once('close', resolve).
    // synchronous destroy would fire 'close' before that listener exists.
    queueMicrotask(() => this.destroy())

    if (typeof callback === 'function') callback()
  }

  destroy(err?: Error) {
    if (this._destroyed) return this
    this._destroyed = true
    this._readyState = 'closed'
    this.writable = false
    this.readable = false

    // clear timeout
    if (this._timeoutTimer) {
      clearTimeout(this._timeoutTimer)
      this._timeoutTimer = null
    }

    // clear pause buffer
    this._pauseBuffer.length = 0

    if (this.port) {
      // delay port.close() to allow pending messages (like Terminate/X)
      // to be delivered. closing immediately after postMessage loses
      // the message, preventing the proxy from releasing its mutex.
      const p = this.port
      this.port = null
      setTimeout(() => p.close(), 50)
    }

    if (err) {
      this.emit('error', err)
    }
    this.emit('end')
    this.emit('close', !!err)
    return this
  }

  // flow control — MessagePort doesn't natively support pause/resume,
  // so we buffer incoming messages when paused and flush on resume.
  // the postgres COPY protocol relies on this (CopyData calls socket.pause()
  // when stream.push() returns false).
  pause() {
    this._paused = true
    return this
  }

  resume() {
    this._paused = false
    // flush buffered messages — exit if data handler re-pauses
    while (this._pauseBuffer.length && !this._paused) {
      this.emit('data', this._pauseBuffer.shift()!)
    }
    return this
  }

  // timeout — emit 'timeout' after ms of inactivity (no reads or writes).
  // postgres calls socket.setKeepAlive conditionally but doesn't use
  // setTimeout on custom sockets. still useful for detecting hung connections.
  setTimeout(ms: number, cb?: Function) {
    this._timeoutMs = ms
    if (this._timeoutTimer) {
      clearTimeout(this._timeoutTimer)
      this._timeoutTimer = null
    }
    if (cb) this.once('timeout', cb as (...args: any[]) => void)
    if (ms > 0) this._resetTimeout()
    return this
  }

  private _resetTimeout() {
    if (this._timeoutTimer) clearTimeout(this._timeoutTimer)
    if (this._timeoutMs > 0 && !this._destroyed) {
      this._timeoutTimer = globalThis.setTimeout(
        () => this.emit('timeout'),
        this._timeoutMs
      )
    }
  }

  // no-ops — these configure TCP-level behavior that doesn't apply to MessagePort
  setKeepAlive() { return this }
  setNoDelay() { return this }
  ref() { return this }
  unref() { return this }
  cork() {}
  uncork() {}

  // postgres skips connect() for custom sockets, but defensive for generic use
  connect() { return this }

  // net.Socket address info stubs
  address() { return { address: '127.0.0.1', family: 'IPv4', port: 0 } }
  get remoteAddress() { return '127.0.0.1' }
  get remotePort() { return 0 }
  get remoteFamily() { return 'IPv4' }
  get localAddress() { return '127.0.0.1' }
  get localPort() { return 0 }
}
