/**
 * MessagePort-backed socket for the postgres npm package.
 *
 * the postgres package (porsager/postgres) accepts a custom socket factory
 * via options.socket. this provides a Socket-like object backed by a
 * MessagePort that connects to pg-proxy-browser.
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

let _globalRecvCount = 0

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
 * Socket-like object backed by MessagePort.
 * implements the subset of net.Socket that the postgres package uses.
 */
class MessagePortSocket extends EventEmitter {
  private port: MessagePort | null = null
  private channel: MessageChannel | null = null
  private _destroyed = false
  private _ended = false

  // net.Socket compat properties
  writable = true
  readable = true

  constructor(private connectFn: (port: MessagePort) => void) {
    super()
    // create channel and connect
    this.channel = new MessageChannel()
    this.port = this.channel.port1

    // give server port to pg-proxy-browser
    this.connectFn(this.channel.port2)

    // forward incoming data from proxy — wrap as Buffer (postgres package needs readUInt32BE etc.)
    let recvCount = 0
    this.port.onmessage = (ev: MessageEvent) => {
      if (this._destroyed) return
      recvCount++
      let buf: Buffer | null = null
      if (ev.data instanceof ArrayBuffer) {
        buf = Buffer.from(new Uint8Array(ev.data))
      } else if (ev.data instanceof Uint8Array) {
        buf = Buffer.from(ev.data)
      }
      if (buf) {
        _globalRecvCount++
        if (_globalRecvCount % 100 === 0 || _globalRecvCount <= 5) {
          console.debug(`[pg-socket-global] recv#${_globalRecvCount} len=${buf.length}`)
        }
        this.emit('data', buf)
      } else {
        console.warn(`[pg-socket] unexpected data type at recv#${recvCount}:`, typeof ev.data)
      }
    }

    this.port.start()

    // fire connect event async (postgres expects this)
    queueMicrotask(() => {
      if (!this._destroyed) {
        this.emit('connect')
        this.emit('ready')
      }
    })
  }

  get destroyed() {
    return this._destroyed
  }

  // postgres package calls socket.write(data)
  write(data: Uint8Array | Buffer | string, encoding?: any, callback?: Function): boolean {
    if (this._destroyed || !this.port) {
      if (typeof encoding === 'function') encoding()
      else if (typeof callback === 'function') callback()
      return false
    }

    const bytes: Uint8Array = typeof data === 'string'
      ? Buffer.from(data)
      : data instanceof Uint8Array ? data : Buffer.from(data)

    // copy (not transfer) — the postgres package may reference the buffer after write
    const copy = new Uint8Array(bytes.length)
    copy.set(bytes)
    this.port.postMessage(copy.buffer, [copy.buffer])

    if (typeof encoding === 'function') encoding()
    else if (typeof callback === 'function') callback()

    return true
  }

  end(data?: any, encoding?: any, callback?: Function) {
    if (data) this.write(data, encoding)
    this._ended = true
    this.destroy()
    if (typeof callback === 'function') callback()
    else if (typeof encoding === 'function') encoding()
  }

  destroy(err?: Error) {
    if (this._destroyed) return this
    this._destroyed = true
    this.writable = false
    this.readable = false
    if (this.port) {
      this.port.close()
      this.port = null
    }
    if (err) {
      this.emit('error', err)
    }
    this.emit('close', !!err)
    return this
  }

  // no-ops for compatibility
  setKeepAlive() { return this }
  setTimeout() { return this }
  setNoDelay() { return this }
  ref() { return this }
  unref() { return this }
  cork() {}
  uncork() {}

  get remoteAddress() { return '127.0.0.1' }
  get remotePort() { return 0 }
}
