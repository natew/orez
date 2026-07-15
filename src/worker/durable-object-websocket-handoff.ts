const OPEN = 1
const CLOSING = 2
const CLOSED = 3
const FIRST_SEND_CONTEXT_TIMEOUT_MS = 10_000

export interface HandoffRequestMessage {
  url: string
  headers: Record<string, string>
  method: string
}

export interface DurableObjectWebSocketHandoffContext {
  waitUntil(promise: Promise<unknown>): void
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
  server?: {
    emit?: Function
    listenerCount?: (event: string) => number
  }
}

type HandoffServer = NonNullable<FastifyHandoffTarget['server']> & {
  emit: Function
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function websocketEventData(event: any): string | ArrayBuffer {
  const data = event?.data ?? event
  if (typeof data === 'string' || data instanceof ArrayBuffer) return data
  if (ArrayBuffer.isView(data)) {
    return Uint8Array.from(new Uint8Array(data.buffer, data.byteOffset, data.byteLength))
      .buffer
  }
  return String(data)
}

function createLocalSocketBridge(
  cfSocket: DurableObjectWebSocket,
  onClose: () => void,
  onFirstSend: () => void
): Bridge['socket'] & {
  receive(data: string | ArrayBuffer): void
  closeFromPeer(code: number, reason: string, wasClean: boolean): void
  errorFromPeer(error: unknown): void
} {
  const listeners = new Map<string, Set<(event: any) => void>>()
  let readyState = OPEN
  let removePeerListeners = () => {}
  let firstSendObserved = false

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
      try {
        cfSocket.send(data)
      } catch (err) {
        console.log(`[orez-ws] send error ${errorMessage(err)}`)
        throw err
      }
      if (!firstSendObserved) {
        firstSendObserved = true
        console.log(
          `[orez-ws] first send localReady=${readyState} cfReady=${cfSocket.readyState}`
        )
        onFirstSend()
      }
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
    localSocket.closeFromPeer(
      event?.code ?? 1005,
      event?.reason ?? '',
      event?.wasClean ?? false
    )
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

  /** number of live zero-cache sync bridges (open WebSocket sync sessions). */
  get activeConnections(): number {
    return this.#bridges.size
  }

  /** close every bridge owned by this logical Durable Object runtime. */
  closeAll(code = 1001, reason = 'zero-cache embed stopped'): void {
    for (const bridge of [...this.#bridges.values()]) {
      bridge.socket.close(code, reason)
    }
  }

  accept(
    server: DurableObjectWebSocket,
    message: HandoffRequestMessage,
    ctx?: DurableObjectWebSocketHandoffContext
  ): boolean {
    const id = crypto.randomUUID()
    server.accept()

    const releaseContext = this.#keepContextUntilFirstSend(ctx)
    const bridge = this.#createBridge(id, server, message, releaseContext)
    const handed = this.#handoff(bridge)
    // durable diagnostic: a `handoff=false` here means no fastify instance
    // consumed the upgrade (the CF-DO sync-dead class — see the /sync emit
    // fallback in #handoff). one cheap line per connection, greppable in
    // `wrangler tail`.
    console.log(`[orez-ws] accept handoff=${handed} url=${message.url}`)
    if (!handed) {
      releaseContext('no-handoff')
      bridge.close(1011, 'zero-cache websocket route unavailable', false)
    }

    return handed
  }

  #createBridge(
    id: string,
    cfSocket: DurableObjectWebSocket,
    message: HandoffRequestMessage,
    onFirstSend: () => void
  ): Bridge {
    const localSocket = createLocalSocketBridge(
      cfSocket,
      () => {
        this.#bridges.delete(id)
      },
      onFirstSend
    )

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

  #keepContextUntilFirstSend(
    ctx: DurableObjectWebSocketHandoffContext | undefined
  ): (reason?: string) => void {
    if (!ctx?.waitUntil) {
      console.log(`[orez-ws] waitUntil missing`)
      return () => {}
    }

    let timeout: ReturnType<typeof setTimeout> | undefined
    let finished = false
    let finish: (reason?: string) => void = () => {}
    const promise = new Promise<void>((resolve) => {
      finish = (reason = 'first-send') => {
        if (finished) return
        finished = true
        if (timeout) clearTimeout(timeout)
        console.log(`[orez-ws] waitUntil release reason=${reason}`)
        resolve()
      }
      timeout = setTimeout(() => finish('timeout'), FIRST_SEND_CONTEXT_TIMEOUT_MS)
    })
    ctx.waitUntil(promise)
    console.log(
      `[orez-ws] waitUntil registered timeoutMs=${FIRST_SEND_CONTEXT_TIMEOUT_MS}`
    )
    return finish
  }

  #handoff(bridge: Bridge): boolean {
    const handoffMsg = { message: bridge.message, head: new Uint8Array(0) }
    const instance = this.getFastify()
    if (!instance) return false

    // 1. {websocket:true} routes (e.g. /replication/*/changes) are matched by
    //    the exact fastify dispatcher owned by this Durable Object runtime.
    if (instance.tryHandoff?.(handoffMsg, bridge.socket)) return true

    // 2. the /sync/v*/connect path is served by the ZeroDispatcher's
    //    server.onMessageType('handoff') listener, NOT a {websocket:true} route,
    //    so tryHandoff never matches it. emit the handoff on the dispatcher's
    //    server — mirroring the in-process ws shim (shims/ws.ts).
    const server = instance.server
    if (typeof server?.emit !== 'function') return false
    const listeners = server.listenerCount?.('message')
    if (listeners !== undefined && listeners <= 1) return false
    const emitted = Boolean(
      (server as HandoffServer).emit('message', ['handoff', handoffMsg], bridge.socket)
    )
    console.log(
      `[orez-ws] dispatcher emit emitted=${emitted} listeners=${listeners ?? -1}`
    )
    return emitted
  }
}
