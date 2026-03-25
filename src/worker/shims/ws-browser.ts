/**
 * browser WebSocket/MessagePort adapter for the ws shim.
 *
 * wraps a browser WebSocket or MessagePort to match the interface
 * that the existing ws shim (orez/worker/shims/ws) expects. this lets
 * zero-cache handle WebSocket connections in browser Web Workers.
 *
 * usage:
 *   import { messagePortToWs, browserWsToWs } from 'orez/worker/shims/ws-browser'
 *
 *   // from a MessageChannel (worker ↔ main thread)
 *   const channel = new MessageChannel()
 *   const serverWs = messagePortToWs(channel.port2)
 *
 *   // from a browser WebSocket
 *   const ws = new WebSocket('ws://...')
 *   const shimWs = browserWsToWs(ws)
 */

// the interface that the ws shim expects (same as CF WebSocket)
// on/off are needed because createWebSocketStream uses ws.on('message')
interface WsCompatible {
  // ws constants — zero-cache uses ws.OPEN / ws.CONNECTING in switch statements
  CONNECTING: number
  OPEN: number
  CLOSING: number
  CLOSED: number
  readyState: number
  send(data: string | ArrayBuffer | ArrayBufferView): void
  close(code?: number, reason?: string): void
  addEventListener(type: string, handler: (event: any) => void): void
  removeEventListener(type: string, handler: (event: any) => void): void
  on(type: string, handler: (event: any) => void): void
  off(type: string, handler: (event: any) => void): void
}

/**
 * wrap a MessagePort to look like a WebSocket.
 *
 * MessagePort uses postMessage/onmessage, while the ws shim expects
 * send/addEventListener('message'). this bridges the two.
 *
 * messages that arrive before any listener is registered are buffered
 * and replayed when the first 'message' listener is added. this prevents
 * a race where the port starts receiving data before the connection
 * handler (e.g. createWebSocketStream / proxyInbound) has set up its
 * listener — which would silently drop messages.
 */
export function messagePortToWs(port: MessagePort): WsCompatible {
  const listeners = new Map<string, Set<(event: any) => void>>()
  let closed = false

  // buffer messages until a 'message' listener is registered
  const pendingMessages: any[] = []

  function addListener(type: string, handler: (event: any) => void) {
    if (!listeners.has(type)) listeners.set(type, new Set())
    listeners.get(type)!.add(handler)

    // flush buffered messages when first 'message' listener is added
    if (type === 'message' && pendingMessages.length > 0) {
      const queued = pendingMessages.splice(0)
      for (const event of queued) handler(event)
    }
  }

  function removeListener(type: string, handler: (event: any) => void) {
    listeners.get(type)?.delete(handler)
  }

  function emit(type: string, event: any) {
    const handlers = listeners.get(type)
    if (!handlers || handlers.size === 0) {
      // no listeners yet — buffer message events
      if (type === 'message') {
        pendingMessages.push(event)
      }
      return
    }
    for (const h of handlers) h(event)
  }

  // forward port messages → ws 'message' events
  port.onmessage = (event: MessageEvent) => {
    emit('message', { data: event.data })
  }

  port.onmessageerror = (event: MessageEvent) => {
    emit('error', { message: 'MessagePort error', error: event })
  }

  return {
    // ws constants — zero-cache uses ws.OPEN / ws.CONNECTING in switch statements
    CONNECTING: 0,
    OPEN: 1,
    CLOSING: 2,
    CLOSED: 3,

    get readyState() {
      return closed ? 3 : 1 // CLOSED or OPEN
    },

    send(data: string | ArrayBuffer | ArrayBufferView) {
      if (closed) return
      // MessagePort uses postMessage (structured clone)
      port.postMessage(data)
    },

    close(code?: number, _reason?: string) {
      if (closed) return
      closed = true
      port.close()
      emit('close', { code: code ?? 1000, reason: '', wasClean: true })
    },

    addEventListener: addListener,
    removeEventListener: removeListener,
    on: addListener,
    off: removeListener,
  }
}

/**
 * wrap a browser WebSocket to match the ws shim's expected interface.
 *
 * browser WebSocket and the ws shim's CFWebSocket interface are very
 * similar but have subtle differences. this normalizes them.
 */
export function browserWsToWs(ws: WebSocket): WsCompatible {
  return {
    CONNECTING: 0,
    OPEN: 1,
    CLOSING: 2,
    CLOSED: 3,

    get readyState() {
      return ws.readyState
    },

    send(data: string | ArrayBuffer | ArrayBufferView) {
      ws.send(data)
    },

    close(code?: number, reason?: string) {
      ws.close(code, reason)
    },

    addEventListener(type: string, handler: (event: any) => void) {
      ws.addEventListener(type, handler)
    },

    removeEventListener(type: string, handler: (event: any) => void) {
      ws.removeEventListener(type, handler)
    },

    on(type: string, handler: (event: any) => void) {
      ws.addEventListener(type, handler)
    },

    off(type: string, handler: (event: any) => void) {
      ws.removeEventListener(type, handler)
    },
  }
}

/**
 * create a connected pair of WebSocket-like objects for in-process
 * communication (e.g., when zero-cache and Zero client are in the
 * same browser context). uses MessageChannel internally.
 *
 * returns [client, server] — client goes to Zero client, server
 * goes to the browser embed's handleWebSocket().
 */
export function createInProcessPair(): [WsCompatible, WsCompatible] {
  const channel = new MessageChannel()
  return [messagePortToWs(channel.port1), messagePortToWs(channel.port2)]
}
