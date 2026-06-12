type WebSocketProtocols = string | string[] | undefined
type SocketEventType = 'open' | 'message' | 'close' | 'error'
type SocketListener = ((event: any) => void) | { handleEvent(event: any): void }

type WebSocketConstructor = {
  new (url: string | URL, protocols?: WebSocketProtocols): any
  CONNECTING?: number
  OPEN?: number
  CLOSING?: number
  CLOSED?: number
}

type DesiredQueryPatchOp =
  | { op: 'clear' }
  | { op: 'put' | 'del'; hash: string; [key: string]: unknown }

type GotQueryPatchOp = { op: 'clear' } | { op: 'put' | 'del'; hash: string }

type PullResponse =
  | {
      cookie: number
      lastMutationIDChanges: Record<string, number>
      rowsPatch: unknown[]
      unchanged?: false
    }
  | {
      cookie: number | null
      unchanged: true
    }

type TransportState = {
  readonly origin: URL
  readonly originString: string
  readonly fetch: typeof fetch
  readonly nativeWebSocket: WebSocketConstructor | undefined
  readonly sockets: Set<ZeroHttpSocket>
  nextPokeID: number
}

const COOKIE_WIDTH = 20

export function installZeroHttpTransport(opts: {
  origin: string
  fetch?: typeof fetch
}): {
  pull(): Promise<void>
  readonly connections: number
  uninstall(): void
} {
  const previousWebSocket = globalThis.WebSocket as WebSocketConstructor | undefined
  const fetchImpl = opts.fetch ?? globalThis.fetch
  if (!fetchImpl) {
    throw new Error('installZeroHttpTransport requires a fetch implementation')
  }

  const state: TransportState = {
    origin: new URL(opts.origin),
    originString: trimTrailingSlash(new URL(opts.origin).toString()),
    fetch: fetchImpl,
    nativeWebSocket: previousWebSocket,
    sockets: new Set(),
    nextPokeID: 0,
  }

  const Shim = class {
    static CONNECTING = 0
    static OPEN = 1
    static CLOSING = 2
    static CLOSED = 3

    constructor(url: string | URL, protocols?: WebSocketProtocols) {
      if (shouldIntercept(state.origin, url)) {
        return new ZeroHttpSocket(state, url, protocols)
      }
      if (!state.nativeWebSocket) {
        throw new Error(`No native WebSocket available for ${String(url)}`)
      }
      return new state.nativeWebSocket(url, protocols)
    }
  }

  globalThis.WebSocket = Shim as unknown as typeof WebSocket

  return {
    pull: async () => {
      await Promise.all([...state.sockets].map((socket) => socket.pull()))
    },
    get connections() {
      return state.sockets.size
    },
    uninstall: () => {
      if (globalThis.WebSocket === (Shim as unknown as typeof WebSocket)) {
        globalThis.WebSocket = previousWebSocket as typeof WebSocket
      }
    },
  }
}

class ZeroHttpSocket {
  readonly CONNECTING = 0
  readonly OPEN = 1
  readonly CLOSING = 2
  readonly CLOSED = 3

  readonly url: string
  readyState = this.CONNECTING

  private readonly connectURL: URL
  private readonly authToken: string | undefined
  private readonly listeners: Record<SocketEventType, Set<SocketListener>> = {
    open: new Set(),
    message: new Set(),
    close: new Set(),
    error: new Set(),
  }
  private readonly clientID: string
  private readonly clientGroupID: string
  private readonly wsid: string
  private cookie: string | null
  private pendingGotQueriesPatch: GotQueryPatchOp[] = []
  private pullInFlight: Promise<void> | undefined
  private pullAfterCurrent = false
  private openTimer: ReturnType<typeof setTimeout> | undefined

  constructor(
    private readonly state: TransportState,
    url: string | URL,
    protocols?: WebSocketProtocols
  ) {
    this.connectURL = toHttpURL(url)
    this.url = String(url)
    this.clientID = this.connectURL.searchParams.get('clientID') ?? ''
    this.clientGroupID = this.connectURL.searchParams.get('clientGroupID') ?? ''
    this.wsid = this.connectURL.searchParams.get('wsid') ?? `zero-http-${Date.now()}`
    const baseCookie = this.connectURL.searchParams.get('baseCookie')
    this.cookie = baseCookie ? baseCookie : null

    const decoded = decodeSecProtocol(protocols)
    this.authToken = decoded.authToken
    this.queueDesiredQueries(decoded.initConnectionMessage?.[1])

    this.state.sockets.add(this)
    this.openTimer = setTimeout(() => this.open(), 0)
  }

  addEventListener(type: SocketEventType, listener: SocketListener | null) {
    if (listener) this.listeners[type]?.add(listener)
  }

  removeEventListener(type: SocketEventType, listener: SocketListener | null) {
    if (listener) this.listeners[type]?.delete(listener)
  }

  dispatchEvent(event: { type: SocketEventType }) {
    this.emit(event.type, event)
    return true
  }

  send(data: string) {
    if (this.readyState !== this.OPEN) {
      throw new Error('cannot send on a socket that is not open')
    }
    const message = JSON.parse(data) as [string, any]
    switch (message[0]) {
      case 'initConnection':
      case 'changeDesiredQueries':
        this.queueDesiredQueries(message[1])
        this.requestPullAfterCurrent()
        return
      case 'push':
        void this.push(message[1])
        return
      case 'ping':
        this.emitMessage(['pong', {}])
        return
      case 'pull':
        void this.answerMutationRecoveryPull(message[1])
        return
      case 'deleteClients':
      case 'ackMutationResponses':
        return
      default:
        throw new Error(`unsupported zero-http upstream message ${message[0]}`)
    }
  }

  close(code = 1000, reason = '') {
    if (this.readyState === this.CLOSED) return
    if (this.openTimer) clearTimeout(this.openTimer)
    this.readyState = this.CLOSED
    this.state.sockets.delete(this)
    this.emit('close', { code, reason, wasClean: code <= 1001 })
  }

  pull(): Promise<void> {
    if (this.readyState === this.CLOSED) return Promise.resolve()
    if (this.pullInFlight) return this.pullInFlight
    this.pullInFlight = this.fetchPull(this.clientGroupID, this.cookie)
      .then((response) => {
        if (response.unchanged) return
        this.emitPoke(response)
      })
      .finally(async () => {
        const pullAgain = this.pullAfterCurrent
        this.pullAfterCurrent = false
        this.pullInFlight = undefined
        if (pullAgain && this.readyState !== this.CLOSED) await this.pull()
      })
    return this.pullInFlight
  }

  private open() {
    if (this.readyState !== this.CONNECTING) return
    this.readyState = this.OPEN
    this.emit('open', {})
    this.emitMessage(['connected', { wsid: this.wsid, timestamp: Date.now() }])
    setTimeout(() => void this.pull(), 0)
  }

  private queueDesiredQueries(body: unknown) {
    const desiredQueriesPatch = (body as { desiredQueriesPatch?: unknown })
      ?.desiredQueriesPatch
    if (!Array.isArray(desiredQueriesPatch)) return
    this.pendingGotQueriesPatch.push(...gotQueriesPatch(desiredQueriesPatch))
  }

  private async push(body: unknown) {
    const response = (await this.postJSON('/push', body)) as {
      pushResponse?: unknown
    }
    this.emitMessage(['pushResponse', response.pushResponse])
    this.requestPullAfterCurrent()
  }

  private requestPullAfterCurrent() {
    if (this.pullInFlight) {
      this.pullAfterCurrent = true
      return
    }
    void this.pull()
  }

  private async answerMutationRecoveryPull(body: {
    clientGroupID: string
    cookie: string | null
    requestID: string
  }) {
    const response = await this.fetchPull(body.clientGroupID, body.cookie)
    const cookie = toWebSocketCookie(response.cookie)
    this.emitMessage([
      'pull',
      {
        requestID: body.requestID,
        cookie: cookie ?? this.cookie ?? '0',
        lastMutationIDChanges: response.unchanged ? {} : response.lastMutationIDChanges,
      },
    ])
  }

  private async fetchPull(clientGroupID: string, cookie: string | null) {
    return (await this.postJSON('/pull', {
      clientID: this.clientID,
      clientGroupID,
      cookie: toHttpCookie(cookie),
    })) as PullResponse
  }

  private async postJSON(path: '/pull' | '/push', body: unknown) {
    const response = await this.state.fetch(`${this.state.originString}${path}`, {
      method: 'POST',
      headers: {
        authorization: this.authToken ? `Bearer ${this.authToken}` : '',
        'content-type': 'application/json',
      },
      body: JSON.stringify(body),
    })
    if (!response.ok) {
      throw new Error(`zero-http ${path} failed with ${response.status}`)
    }
    return response.json()
  }

  private emitPoke(response: Exclude<PullResponse, { unchanged: true }>) {
    const nextCookie = toWebSocketCookie(response.cookie)
    if (isStaleCookie(this.cookie, response.cookie)) {
      throw new Error(
        `zero-http pull returned stale cookie ${response.cookie} for ${this.cookie}`
      )
    }

    const pokeID = `zero-http-${++this.state.nextPokeID}`
    const gotQueries = this.pendingGotQueriesPatch
    this.pendingGotQueriesPatch = []

    this.emitMessage([
      'pokeStart',
      {
        pokeID,
        baseCookie: this.cookie,
        schemaVersions: {
          minSupportedVersion: 1,
          maxSupportedVersion: 1,
        },
        timestamp: Date.now(),
      },
    ])
    this.emitMessage([
      'pokePart',
      {
        pokeID,
        lastMutationIDChanges: response.lastMutationIDChanges,
        rowsPatch: response.rowsPatch,
      },
    ])
    if (gotQueries.length > 0) {
      this.emitMessage([
        'pokePart',
        {
          pokeID,
          gotQueriesPatch: gotQueries,
        },
      ])
    }
    this.emitMessage(['pokeEnd', { pokeID, cookie: nextCookie }])
    this.cookie = nextCookie
  }

  private emitMessage(message: unknown) {
    if (this.readyState !== this.OPEN) return
    this.emit('message', { data: JSON.stringify(message) })
  }

  private emit(type: SocketEventType, event: any) {
    const handler = (this as unknown as Record<string, unknown>)[`on${type}`]
    if (typeof handler === 'function') handler.call(this, event)
    for (const listener of this.listeners[type]) {
      if (typeof listener === 'function') listener(event)
      else listener.handleEvent(event)
    }
  }
}

function shouldIntercept(origin: URL, url: string | URL) {
  const candidate = toHttpURL(url)
  if (candidate.origin !== origin.origin) return false
  return candidate.pathname === `${trimTrailingSlash(origin.pathname)}/sync/v51/connect`
}

function toHttpURL(url: string | URL) {
  const parsed = new URL(url)
  if (parsed.protocol === 'ws:') parsed.protocol = 'http:'
  if (parsed.protocol === 'wss:') parsed.protocol = 'https:'
  return parsed
}

function trimTrailingSlash(value: string) {
  return value.endsWith('/') ? value.slice(0, -1) : value
}

function decodeSecProtocol(protocols: WebSocketProtocols):
  | {
      authToken?: string
      initConnectionMessage?: [string, Record<string, unknown>]
    }
  | Record<string, never> {
  const protocol = Array.isArray(protocols) ? protocols[0] : protocols
  if (!protocol) return {}
  try {
    const decoded = decodeURIComponent(protocol)
    const json = new TextDecoder().decode(
      Uint8Array.from(globalThis.atob(decoded), (char) => char.charCodeAt(0))
    )
    const parsed = JSON.parse(json) as {
      authToken?: string
      initConnectionMessage?: unknown
    }
    return {
      authToken: parsed.authToken,
      initConnectionMessage: Array.isArray(parsed.initConnectionMessage)
        ? (parsed.initConnectionMessage as [string, Record<string, unknown>])
        : undefined,
    }
  } catch {
    return {}
  }
}

function gotQueriesPatch(patch: DesiredQueryPatchOp[]) {
  const got: GotQueryPatchOp[] = []
  for (const op of patch) {
    if (op.op === 'clear') got.push({ op: 'clear' })
    else if (op.hash) got.push({ op: op.op, hash: op.hash })
  }
  return got
}

function toHttpCookie(cookie: string | null): number | null {
  if (cookie === null || cookie === '') return null
  const parsed = Number(cookie)
  if (!Number.isFinite(parsed)) {
    throw new Error(`zero-http cookie is not numeric: ${cookie}`)
  }
  return parsed
}

function toWebSocketCookie(cookie: number | null): string | null {
  return cookie === null ? null : String(cookie).padStart(COOKIE_WIDTH, '0')
}

function isStaleCookie(current: string | null, next: number) {
  const currentNumber = toHttpCookie(current)
  return currentNumber !== null && next <= currentNumber
}
