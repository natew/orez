const OPEN = 1
const CLOSING = 2
const CLOSED = 3

const ZERO_CACHE_SOCKET_ATTACHMENT_TYPE = 'orez-zero-cache-handoff-v1'

export interface DurableObjectStateLike {
  acceptWebSocket(socket: HibernatableWebSocket, tags?: string[]): void
}

export interface HandoffRequestMessage {
  url: string
  headers: Record<string, string>
  method: string
}

export interface HandoffAttachment {
  type: typeof ZERO_CACHE_SOCKET_ATTACHMENT_TYPE
  id: string
  message: HandoffRequestMessage
}

export interface HibernatableWebSocket {
  send(data: string | ArrayBuffer | ArrayBufferView): void
  close(code?: number, reason?: string): void
  addEventListener(type: string, handler: (event: any) => void): void
  removeEventListener(type: string, handler: (event: any) => void): void
  readyState: number
  serializeAttachment(value: HandoffAttachment): void
  deserializeAttachment(): HandoffAttachment | undefined
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

function isHandoffAttachment(value: unknown): value is HandoffAttachment {
  if (!value || typeof value !== 'object') return false
  const record = value as Record<string, unknown>
  return (
    record.type === ZERO_CACHE_SOCKET_ATTACHMENT_TYPE &&
    typeof record.id === 'string' &&
    Boolean(record.message) &&
    typeof record.message === 'object'
  )
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function createLocalSocketBridge(
  cfSocket: HibernatableWebSocket,
  onClose: () => void
): Bridge['socket'] & {
  receive(data: string | ArrayBuffer): void
  closeFromPeer(code: number, reason: string, wasClean: boolean): void
  errorFromPeer(error: unknown): void
} {
  const listeners = new Map<string, Set<(event: any) => void>>()
  let readyState = OPEN

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
      if (readyState !== OPEN || cfSocket.readyState === CLOSED) {
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

  return localSocket
}

export class HibernatedWebSocketHandoff {
  #bridges = new Map<string, Bridge>()
  #socketIds = new WeakMap<object, string>()

  constructor(private getFastify: () => FastifyHandoffTarget | null | undefined) {}

  accept(
    durableObjectState: DurableObjectStateLike,
    server: HibernatableWebSocket,
    message: HandoffRequestMessage
  ): boolean {
    const id = crypto.randomUUID()
    const attachment: HandoffAttachment = {
      type: ZERO_CACHE_SOCKET_ATTACHMENT_TYPE,
      id,
      message,
    }

    durableObjectState.acceptWebSocket(server)
    server.serializeAttachment(attachment)
    this.#socketIds.set(server, id)

    const bridge = this.#createBridge(id, server, message)
    const handed = this.#handoff(bridge)
    // durable diagnostic: a `handoff=false` here means no fastify instance
    // consumed the upgrade (the CF-DO sync-dead class — see the /sync emit
    // fallback in #handoff). one cheap line per connection, greppable in
    // `wrangler tail`.
    console.log(`[orez-ws] accept handoff=${handed} url=${message.url}`)
    if (!handed) {
      bridge.close(1011, 'zero-cache websocket route unavailable', false)
      return false
    }
    return true
  }

  handleMessage(socket: HibernatableWebSocket, messageData: string | ArrayBuffer): void {
    const bridge = this.#bridgeForSocket(socket)
    if (!bridge) {
      socket.close(1011, 'missing zero-cache websocket attachment')
      return
    }
    bridge.receive(messageData)
  }

  handleClose(
    socket: HibernatableWebSocket,
    code: number,
    reason: string,
    wasClean: boolean
  ): void {
    const bridge = this.#bridgeForSocket(socket)
    if (!bridge) return
    bridge.close(code, reason, wasClean)
  }

  handleError(socket: HibernatableWebSocket, error: unknown): void {
    const bridge = this.#bridgeForSocket(socket)
    if (!bridge) return
    bridge.error(error)
  }

  #bridgeForSocket(socket: HibernatableWebSocket): Bridge | undefined {
    const attachment = this.#attachmentForSocket(socket)
    if (!attachment) return undefined

    const existing = this.#bridges.get(attachment.id)
    if (existing) return existing

    const bridge = this.#createBridge(attachment.id, socket, attachment.message)
    if (!this.#handoff(bridge)) {
      bridge.close(1011, 'zero-cache websocket route unavailable', false)
      return undefined
    }
    return bridge
  }

  #attachmentForSocket(socket: HibernatableWebSocket): HandoffAttachment | undefined {
    const attachment = socket.deserializeAttachment()
    if (isHandoffAttachment(attachment)) return attachment

    const id = this.#socketIds.get(socket)
    if (!id) return undefined
    const bridge = this.#bridges.get(id)
    if (!bridge) return undefined
    return {
      type: ZERO_CACHE_SOCKET_ATTACHMENT_TYPE,
      id,
      message: bridge.message,
    }
  }

  #createBridge(
    id: string,
    cfSocket: HibernatableWebSocket,
    message: HandoffRequestMessage
  ): Bridge {
    const localSocket = createLocalSocketBridge(cfSocket, () => {
      this.#bridges.delete(id)
      this.#socketIds.delete(cfSocket)
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
    this.#socketIds.set(cfSocket, id)
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
    //    server — mirroring the in-process ws shim (shims/ws.ts:187-189). WITHOUT
    //    this, the DO acceptWebSocket()s the server half, tryHandoff returns
    //    false, the upgrade returns a 404 with no paired client socket, and the
    //    browser sees an abnormal close (1006) → deployed-app sync is dead.
    const server = fallback?.server
    if (server?.emit) {
      server.emit('message', ['handoff', handoffMsg], bridge.socket)
      return true
    }
    return false
  }
}
