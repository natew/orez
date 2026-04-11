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
  // separate listener sets for on() vs addEventListener() to match real ws behavior.
  // in the ws package, on() is EventEmitter-style and addEventListener() is DOM-style.
  // createWebSocketStream uses ws.on('message') exclusively for inbound data.
  // streamOut uses ws.addEventListener('message') for ack handling.
  // if they share the same set, ack messages reach both handlers and
  // handleMessage tries to parse acks as protocol messages, closing the connection.
  const onListeners = new Map<string, Set<(event: any) => void>>()
  const domListeners = new Map<string, Set<(event: any) => void>>()
  let closed = false

  // buffer messages until a 'message' listener is registered (either kind)
  const pendingMessages: any[] = []

  function addOnListener(type: string, handler: (event: any) => void) {
    if (!onListeners.has(type)) onListeners.set(type, new Set())
    onListeners.get(type)!.add(handler)
    if (type === 'message' && pendingMessages.length > 0) {
      const queued = pendingMessages.splice(0)
      for (const event of queued) handler(event)
    }
  }

  function removeOnListener(type: string, handler: (event: any) => void) {
    onListeners.get(type)?.delete(handler)
  }

  function addDomListener(type: string, handler: (event: any) => void) {
    if (!domListeners.has(type)) domListeners.set(type, new Set())
    domListeners.get(type)!.add(handler)
    if (type === 'message' && pendingMessages.length > 0) {
      const queued = pendingMessages.splice(0)
      for (const event of queued) handler(event)
    }
  }

  function removeDomListener(type: string, handler: (event: any) => void) {
    domListeners.get(type)?.delete(handler)
  }

  function emit(type: string, event: any) {
    const onHandlers = onListeners.get(type)
    const domHandlers = domListeners.get(type)
    const hasAny =
      (onHandlers && onHandlers.size > 0) || (domHandlers && domHandlers.size > 0)
    if (!hasAny) {
      if (type === 'message') pendingMessages.push(event)
      return
    }
    if (onHandlers) for (const h of onHandlers) h(event)
    if (domHandlers) for (const h of domHandlers) h(event)
  }

  // forward port messages → ws 'message' events
  // filter out control messages from sync-ws-patch.js (__close, __open)
  port.onmessage = (event: MessageEvent) => {
    const data = event.data
    // control messages from sync-ws-patch.js — handle as close/open events
    if (data && typeof data === 'object' && !ArrayBuffer.isView(data) && !(data instanceof ArrayBuffer)) {
      if (data.__close) {
        closed = true
        port.close()
        emit('close', { code: data.code ?? 1000, reason: data.reason ?? '', wasClean: true })
        return
      }
      if (data.__open) {
        // already open, ignore
        return
      }
    }
    emit('message', { data })
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
      port.postMessage(data)
    },

    close(code?: number, _reason?: string) {
      if (closed) return
      closed = true
      port.close()
      emit('close', { code: code ?? 1000, reason: '', wasClean: true })
    },

    addEventListener: addDomListener,
    removeEventListener: removeDomListener,
    on: addOnListener,
    off: removeOnListener,
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
