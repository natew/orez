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
interface WsCompatible {
  readyState: number
  send(data: string | ArrayBuffer | ArrayBufferView): void
  close(code?: number, reason?: string): void
  addEventListener(type: string, handler: (event: any) => void): void
  removeEventListener(type: string, handler: (event: any) => void): void
}

/**
 * wrap a MessagePort to look like a WebSocket.
 *
 * MessagePort uses postMessage/onmessage, while the ws shim expects
 * send/addEventListener('message'). this bridges the two.
 */
export function messagePortToWs(port: MessagePort): WsCompatible {
  const listeners = new Map<string, Set<(event: any) => void>>()
  let closed = false

  function addListener(type: string, handler: (event: any) => void) {
    if (!listeners.has(type)) listeners.set(type, new Set())
    listeners.get(type)!.add(handler)
  }

  function removeListener(type: string, handler: (event: any) => void) {
    listeners.get(type)?.delete(handler)
  }

  function emit(type: string, event: any) {
    for (const h of listeners.get(type) || []) h(event)
  }

  // forward port messages → ws 'message' events
  port.onmessage = (event: MessageEvent) => {
    emit('message', { data: event.data })
  }

  port.onmessageerror = (event: MessageEvent) => {
    emit('error', { message: 'MessagePort error', error: event })
  }

  return {
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
