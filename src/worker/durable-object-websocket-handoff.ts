const OPEN = 1
const CLOSING = 2
const CLOSED = 3

export interface HandoffRequestMessage {
  url: string
  headers: Record<string, string>
  method: string
}

export interface DurableObjectWebSocket {
  accept(): void
  send(data: string | ArrayBuffer | ArrayBufferView): void
  close(code?: number, reason?: string): void
  addEventListener(type: string, handler: (event: any) => void): void
  removeEventListener(type: string, handler: (event: any) => void): void
  readyState: number
}

interface LocalWebSocket {
  send(data: string | ArrayBuffer | ArrayBufferView): void
  close(code?: number, reason?: string): void
  addEventListener(type: string, handler: (event: any) => void): void
  removeEventListener(type: string, handler: (event: any) => void): void
  readyState: number
  accept(): void
}

interface Bridge {
  id: string
  message: HandoffRequestMessage
  socket: LocalWebSocket
  receive(data: string | ArrayBuffer): void
  close(code: number, reason: string, wasClean: boolean): void
  error(error: unknown): void
}

interface FastifyHandoffTarget {
  tryHandoff(
    msg: { message: HandoffRequestMessage; head: Uint8Array },
    socket: LocalWebSocket
  ): boolean
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function websocketEventData(event: any): string | ArrayBuffer {
  const data = event?.data ?? event
  if (typeof data === 'string' || data instanceof ArrayBuffer) return data
  if (ArrayBuffer.isView(data)) {
    return Uint8Array.from(
      new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
    ).buffer
  }
  return String(data)
}

function createLocalSocketBridge(
  cfSocket: DurableObjectWebSocket,
  onClose: () => void
): Bridge['socket'] & {
  receive(data: string | ArrayBuffer): void
  closeFromPeer(code: number, reason: string, wasClean: boolean): void
  errorFromPeer(error: unknown): void
} {
  const listeners = new Map<string, Set<(event: any) => void>>()
  let readyState = OPEN
  let removePeerListeners = () => {}

  function addEventListener(type: string, handler: (event: any) => void) {
    let handlers = listeners.get(type)
    if (!handlers) {
      handlers = new Set()
      listeners.set(type, handlers)
    }
    handlers.add(handler)
  }

  function removeEventListener(type: string, handler: (event: any) => void) {
    listeners.get(type)?.delete(handler)
  }

  function emit(type: string, event: any) {
    const handlers = listeners.get(type)
    if (!handlers) return
    for (const handler of [...handlers]) handler(event)
  }

  function markClosed(code: number, reason: string, wasClean: boolean) {
    if (readyState === CLOSED) return
    readyState = CLOSED
    removePeerListeners()
    emit('close', { code, reason, wasClean })
    listeners.clear()
    onClose()
  }

  const localSocket = {
    accept() {},

    get readyState() {
      return readyState
    },

    send(data: string | ArrayBuffer | ArrayBufferView) {
      if (readyState !== OPEN || cfSocket.readyState !== OPEN) {
        throw new Error('WebSocket is closed')
      }
      cfSocket.send(data)
    },

    close(code?: number, reason?: string) {
      if (readyState === CLOSED) return
      readyState = CLOSING
      try {
        cfSocket.close(code, reason)
      } finally {
        markClosed(code ?? 1000, reason ?? '', true)
      }
    },

    addEventListener,
    removeEventListener,

    receive(data: string | ArrayBuffer) {
      if (readyState === CLOSED) return
      readyState = OPEN
      emit('message', { data })
    },

    closeFromPeer(code: number, reason: string, wasClean: boolean) {
      markClosed(code, reason, wasClean)
    },

    errorFromPeer(error: unknown) {
      emit('error', { error, message: errorMessage(error) })
      markClosed(1011, errorMessage(error), false)
    },
  }

  const onPeerMessage = (event: any) => {
    localSocket.receive(websocketEventData(event))
  }
  const onPeerClose = (event: any) => {
    localSocket.closeFromPeer(event?.code ?? 1005, event?.reason ?? '', event?.wasClean ?? false)
  }
  const onPeerError = (event: any) => {
    localSocket.errorFromPeer(event?.error ?? event)
  }

  cfSocket.addEventListener('message', onPeerMessage)
  cfSocket.addEventListener('close', onPeerClose)
  cfSocket.addEventListener('error', onPeerError)
  removePeerListeners = () => {
    cfSocket.removeEventListener('message', onPeerMessage)
    cfSocket.removeEventListener('close', onPeerClose)
    cfSocket.removeEventListener('error', onPeerError)
    removePeerListeners = () => {}
  }

  return localSocket
}

export class DurableObjectWebSocketHandoff {
  #bridges = new Map<string, Bridge>()

  constructor(private getFastify: () => FastifyHandoffTarget | null | undefined) {}

  accept(server: DurableObjectWebSocket, message: HandoffRequestMessage): boolean {
    const id = crypto.randomUUID()
    server.accept()

    const bridge = this.#createBridge(id, server, message)
    const handed = this.#handoff(bridge)
    // durable diagnostic: a `handoff=false` here means no fastify instance
    // consumed the upgrade (the CF-DO sync-dead class — see the /sync emit
    // fallback in #handoff). one cheap line per connection, greppable in
    // `wrangler tail`.
    console.log(`[orez-ws] accept handoff=${handed} url=${message.url}`)
    if (!handed) {
      bridge.close(1011, 'zero-cache websocket route unavailable', false)
    }

    return true
  }

  #createBridge(
    id: string,
    cfSocket: DurableObjectWebSocket,
    message: HandoffRequestMessage
  ): Bridge {
    const localSocket = createLocalSocketBridge(cfSocket, () => {
      this.#bridges.delete(id)
    })

    const bridge: Bridge = {
      id,
      message,
      socket: localSocket,
      receive: localSocket.receive,
      close: localSocket.closeFromPeer,
      error: localSocket.errorFromPeer,
    }
    this.#bridges.set(id, bridge)
    return bridge
  }

  #handoff(bridge: Bridge): boolean {
    const handoffMsg = { message: bridge.message, head: new Uint8Array(0) }
    const g = globalThis as unknown as {
      __orez_fastify_instances?: Array<
        FastifyHandoffTarget & { server?: { emit?: Function } }
      >
      __orez_fastify_instance?: FastifyHandoffTarget & { server?: { emit?: Function } }
    }
    const instances = g.__orez_fastify_instances ?? []

    // 1. {websocket:true} routes (e.g. /replication/*/changes) are matched by
    //    the fastify shim's tryHandoff. iterate every instance, stop at first.
    for (const inst of instances) {
      if (inst?.tryHandoff?.(handoffMsg, bridge.socket)) return true
    }
    const fallback = (this.getFastify() ?? g.__orez_fastify_instance) as
      | (FastifyHandoffTarget & { server?: { emit?: Function } })
      | null
      | undefined
    if (!instances.length && fallback?.tryHandoff?.(handoffMsg, bridge.socket)) {
      return true
    }

    // 2. the /sync/v*/connect path is served by the ZeroDispatcher's
    //    server.onMessageType('handoff') listener, NOT a {websocket:true} route,
    //    so tryHandoff never matches it. emit the handoff on the dispatcher's
    //    server — mirroring the in-process ws shim (shims/ws.ts). without this,
    //    the sync upgrade is accepted but never delivered to zero-cache.
    const server = fallback?.server
    if (server?.emit) {
      server.emit('message', ['handoff', handoffMsg], bridge.socket)
      return true
    }
    return false
  }
}
