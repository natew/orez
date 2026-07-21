// http-pull transport: runs a stock @rocicorp/zero client over stateless HTTP
// by intercepting its /sync/v51/connect WebSocket with a shim that translates
// pull responses into v51 pokes. this browser-only module and the server mount
// are the two halves of Orez's zero-http protocol. the
// wire contract (lexicographic string cookies, gotQueriesPatch poke-part
// ordering, bounded FIFO push batching, updateAuth, 401→Unauthorized frame,
// teardown drain) is pinned by the tests in this directory. do not "simplify"
// any of it without re-running those tests against a stock zero client.

import { identityPayloadCodec } from './payload-codec.js'

import type { PayloadCodec, PullResponse, PushRequest } from './payload-codec.js'

export type {
  EncryptedColumnManifest,
  EncryptedRowBatch,
  EncryptionKeyring,
  JSONObject,
  JSONPrimitive,
  JSONValue,
  PayloadCodec,
  PullResponse,
  PushMutation,
  PushRequest,
} from './payload-codec.js'

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

// a shipped desired-query put carries an AST (client-resolved by a
// queryTransform, or an ad-hoc query's inline ast) OR name+args for the server
// to resolve. name+args is the auth-sensitive path: the permission transform
// stays server-side, so a client cannot forge the AST.
type QueryPatchOp =
  | { op: 'put'; hash: string; ast: unknown }
  | { op: 'put'; hash: string; name: string; args: readonly unknown[] }
  | { op: 'del'; hash: string }
  | { op: 'clear' }

// transforms a named query's (name, args) into its Zero v51 AST. providing one
// turns the query-aware extension on and resolves desired queries CLIENT-side
// (ship the AST). use for a native host with no query registry, or a trusted
// harness. omit it (with queryForward) to ship name+args and resolve SERVER-side.
export type QueryTransform = (name: string, args: readonly unknown[]) => unknown

type PushBody = Record<string, unknown> & {
  mutations: unknown[]
}

type PushBatch = {
  body: unknown
  frameCount: number
  mutationCount: number
}

type TransportState = {
  readonly appID: string
  readonly pageID: string
  readonly transportID: string
  readonly origin: URL
  readonly originString: string
  readonly pullOriginString: string
  readonly pushOriginString: string
  readonly fetch: typeof fetch
  readonly nativeWebSocket: WebSocketConstructor | undefined
  readonly sockets: Set<ZeroHttpSocket>
  readonly pullIntervalMs: number | undefined
  readonly wake: HttpPullTransportOptions['wake']
  readonly queryTransform: QueryTransform | undefined
  readonly queryForward: boolean
  readonly queryAware: boolean
  readonly payloadCodec: PayloadCodec
  readonly shardNum: number
  nextPokeID: number
  nextSocketGeneration: number
  readonly activeSocketGenerationByClient: Map<string, number>
  readonly activeSocketByClient: Map<string, ZeroHttpSocket>
  readonly lifecycle: ((event: HttpPullLifecycleEvent) => void) | undefined
}

const COOKIE_WIDTH = 20
// the stock zero-cache pusher drains queued frames into a single request once
// its current request completes. keep the same backpressure relief here, but
// cap the number of mutations so one response cannot monopolize the HTTP
// transport indefinitely under a sustained producer.
const MAX_PUSH_BATCH_MUTATIONS = 64
// @rocicorp/zero 1.6 starts this deadline before its async createSocket work.
// the connect URL's `ts` is captured at the same attempt boundary.
const ZERO_CONNECT_TIMEOUT_MS = 10_000
// ceiling on a server-supplied Retry-After. a daily quota can report a reset
// hours out; honoring that verbatim would leave the client dark until then.
const MAX_RETRY_AFTER_BACKOFF_MS = 60_000

export type HttpPullTransport = {
  pull(): Promise<void>
  flush(): Promise<void>
  readonly connections: number
  readonly pageID: string
  readonly transportID: string
  uninstall(): void
}

export type WakeTokenFetchInit = RequestInit | (() => RequestInit | Promise<RequestInit>)

export function createWakeTokenFetcher(
  tokenURL: string | URL,
  init: WakeTokenFetchInit = {},
  fetchImplementation: typeof fetch = globalThis.fetch
): () => Promise<string> {
  if (!fetchImplementation) {
    throw new Error('createWakeTokenFetcher requires a fetch implementation')
  }
  const request = fetchImplementation.bind(globalThis)
  return async () => {
    const fetchInit = typeof init === 'function' ? await init() : init
    const response = await request(tokenURL, { ...fetchInit, method: 'POST' })
    if (!response.ok) {
      throw new Error(`wake token request failed: ${response.status}`)
    }
    const body: unknown = await response.json().catch(() => null)
    if (
      !body ||
      typeof body !== 'object' ||
      Array.isArray(body) ||
      !('token' in body) ||
      typeof body.token !== 'string' ||
      !body.token
    ) {
      throw new Error('wake token response is invalid')
    }
    return body.token
  }
}

export type HttpPullTransportOptions = {
  // zero app id; encoded into /push routing params as schema=<appID>_<shardNum>
  // and appID=<appID> so a native host can address the right schema shard.
  appID?: string
  shardNum?: number
  origin: string
  // optional authoritative sync endpoint bases. websocket interception and
  // wake stay on origin, while pull/push POST to their configured bases.
  pullOrigin?: string
  pushOrigin?: string
  fetch?: typeof fetch
  // when set, every open connection also pulls on this interval so
  // server-initiated changes arrive without a client-side trigger
  pullIntervalMs?: number
  /**
   * opens a notification-only socket to <origin>/wake and pulls immediately on
   * any wake, demoting interval polling to a safety net. true preserves the
   * bare unauthenticated URL. for an authenticated host, pass getToken: each
   * socket attempt calls it afresh and appends the result as wakeToken because
   * browser WebSockets cannot send headers. consumers should implement
   * getToken by calling an authenticated edge route that mints a short-lived,
   * namespace-scoped signed token; the consumer's authorizeWake callback must
   * verify that token. mint failures leave this advisory channel down and retry
   * with backoff; pulls remain the source of correctness.
   */
  wake?: boolean | { getToken(): Promise<string> }
  // when provided, the query-aware extension is on and desired queries are
  // resolved client-side to an AST before shipping (native host / trusted
  // harness). omit for the baseline dialect (client-local got-query synthesis).
  queryTransform?: QueryTransform
  // turns the query-aware extension on WITHOUT a client-side transform: desired
  // queries ship as name+args for the SERVER (consumer worker) to resolve with
  // auth. the production path for permission-transformed queries.
  queryForward?: boolean
  // transforms selected row payloads at the final serialized transport
  // boundary. omitted options use the module-level identity codec.
  payloadCodec?: PayloadCodec
  // receives the structured connection lifecycle. when omitted, Orez logs
  // failures so production errors retain their owners without logging routine
  // connection and mutation traffic.
  lifecycle?: (event: HttpPullLifecycleEvent) => void
}

export type HttpPullLifecycleEvent = {
  type:
    | 'created'
    | 'listener'
    | 'open'
    | 'close'
    | 'failure'
    | 'superseded'
    | 'aborted'
    | 'push'
  pageID: string
  transportID: string
  zeroInstanceID: string
  clientGroupID: string
  connectionAttemptID: string
  socketID: string
  generation: number
  activeGeneration: number
  clientID: string
  wsid: string
  timestamp: number
  attemptStartedAt: number | undefined
  attemptAgeMs: number | undefined
  listener?: SocketEventType
  code?: number
  reason?: string
  pushFrameCount?: number
  mutationCount?: number
}

type HttpPullPageRegistry = {
  readonly pageID: string
  nextTransportGeneration: number
  readonly transportsByOrigin: Map<string, HttpPullTransportRegistration>
}

type HttpPullTransportRegistration = {
  readonly transport: HttpPullTransport
  readonly options: NormalizedHttpPullTransportOptions
}

type NormalizedHttpPullTransportOptions = {
  readonly appID: string
  readonly shardNum: number
  readonly origin: string
  readonly pullOrigin: string
  readonly pushOrigin: string
  readonly fetch: typeof fetch | undefined
  readonly pullIntervalMs: number | undefined
  readonly wake: false | true | (() => Promise<string>)
  readonly queryTransform: QueryTransform | undefined
  readonly queryForward: boolean
  readonly payloadCodecID: string
  readonly lifecycle: ((event: HttpPullLifecycleEvent) => void) | undefined
}

const HTTP_PULL_OPTION_FIELDS = [
  'appID',
  'shardNum',
  'origin',
  'pullOrigin',
  'pushOrigin',
  'fetch',
  'pullIntervalMs',
  'wake',
  'queryTransform',
  'queryForward',
  'payloadCodecID',
  'lifecycle',
] satisfies readonly (keyof NormalizedHttpPullTransportOptions)[]

const HTTP_PULL_PAGE_REGISTRY = Symbol.for('on-zero.http-pull.page-registry')

function isHttpPullPageRegistry(value: unknown): value is HttpPullPageRegistry {
  return (
    typeof value === 'object' &&
    value !== null &&
    'pageID' in value &&
    typeof value.pageID === 'string' &&
    'nextTransportGeneration' in value &&
    typeof value.nextTransportGeneration === 'number' &&
    'transportsByOrigin' in value &&
    value.transportsByOrigin instanceof Map
  )
}

function getHttpPullPageRegistry(): HttpPullPageRegistry {
  const existing = Reflect.get(globalThis, HTTP_PULL_PAGE_REGISTRY)
  if (isHttpPullPageRegistry(existing)) return existing
  const registry: HttpPullPageRegistry = {
    pageID: `page-${Date.now().toString(36)}`,
    nextTransportGeneration: 0,
    transportsByOrigin: new Map(),
  }
  Reflect.set(globalThis, HTTP_PULL_PAGE_REGISTRY, registry)
  return registry
}

function normalizePayloadCodec(codec: PayloadCodec | undefined): PayloadCodec {
  const resolved = codec ?? identityPayloadCodec
  if (
    typeof resolved.id !== 'string' ||
    !resolved.id ||
    typeof resolved.encodePush !== 'function' ||
    typeof resolved.decodePull !== 'function'
  ) {
    throw new Error('zero-http payload codec must have an id, encodePush, and decodePull')
  }
  return resolved
}

function logHttpPullLifecycle(event: HttpPullLifecycleEvent) {
  const processValue = Reflect.get(globalThis, 'process') as
    | { env?: { NODE_ENV?: string } }
    | undefined
  if (processValue?.env?.NODE_ENV === 'test' || event.type !== 'failure') return
  console.error(`[orez:client] ${JSON.stringify(event)}`)
}

export function installHttpPullTransport(
  opts: HttpPullTransportOptions
): HttpPullTransport {
  const pageRegistry = getHttpPullPageRegistry()
  const transportID = `${pageRegistry.pageID}:transport:${++pageRegistry.nextTransportGeneration}`
  const previousWebSocket = globalThis.WebSocket as WebSocketConstructor | undefined
  const fetchImpl = opts.fetch ?? globalThis.fetch
  if (!fetchImpl) {
    throw new Error('installHttpPullTransport requires a fetch implementation')
  }
  const payloadCodec = normalizePayloadCodec(opts.payloadCodec)

  const state: TransportState = {
    appID: opts.appID ?? 'zero',
    pageID: pageRegistry.pageID,
    transportID,
    origin: new URL(opts.origin),
    originString: trimTrailingSlash(new URL(opts.origin).toString()),
    pullOriginString: trimTrailingSlash(
      new URL(opts.pullOrigin ?? opts.origin).toString()
    ),
    pushOriginString: trimTrailingSlash(
      new URL(opts.pushOrigin ?? opts.origin).toString()
    ),
    // the transport invokes this as `state.fetch(...)` — without binding,
    // window.fetch sees `state` as its receiver and browsers throw
    // "Illegal invocation" (node's fetch doesn't care, so tests can't catch it)
    fetch: fetchImpl.bind(globalThis),
    nativeWebSocket: previousWebSocket,
    sockets: new Set(),
    pullIntervalMs: opts.pullIntervalMs,
    wake: opts.wake ?? false,
    queryTransform: opts.queryTransform,
    queryForward: opts.queryForward === true,
    queryAware: opts.queryTransform !== undefined || opts.queryForward === true,
    payloadCodec,
    shardNum: opts.shardNum ?? 0,
    nextPokeID: 0,
    nextSocketGeneration: 0,
    activeSocketGenerationByClient: new Map(),
    activeSocketByClient: new Map(),
    lifecycle: opts.lifecycle ?? logHttpPullLifecycle,
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

  const transport: HttpPullTransport = {
    pull: async () => {
      await Promise.all([...state.sockets].map((socket) => socket.pull()))
    },
    flush: async () => {
      await Promise.resolve()
      const generation = state.nextSocketGeneration
      const sockets = [...state.sockets]
      await Promise.all(sockets.map((socket) => socket.flush()))
      if (
        state.nextSocketGeneration !== generation ||
        sockets.some((socket) => !state.sockets.has(socket))
      ) {
        throw new Error('transport changed during flush')
      }
    },
    get connections() {
      return state.sockets.size
    },
    pageID: state.pageID,
    transportID: state.transportID,
    uninstall: () => {
      if (globalThis.WebSocket === (Shim as unknown as typeof WebSocket)) {
        globalThis.WebSocket = previousWebSocket as typeof WebSocket
      }
      const registered = pageRegistry.transportsByOrigin.get(state.originString)
      if (registered?.transport === transport) {
        pageRegistry.transportsByOrigin.delete(state.originString)
      }
    },
  }
  return transport
}

// per-origin idempotent install for app usage (ProvideZero rotates zero
// instances against the same server; installing per rotation would chain
// shims unboundedly). installed transports live for the page lifetime.
export function ensureHttpPullTransport(
  opts: HttpPullTransportOptions
): HttpPullTransport {
  const pageRegistry = getHttpPullPageRegistry()
  const key = trimTrailingSlash(new URL(opts.origin).toString())
  const payloadCodec = normalizePayloadCodec(opts.payloadCodec)
  const options: NormalizedHttpPullTransportOptions = {
    appID: opts.appID ?? 'zero',
    shardNum: opts.shardNum ?? 0,
    origin: key,
    pullOrigin: trimTrailingSlash(new URL(opts.pullOrigin ?? opts.origin).toString()),
    pushOrigin: trimTrailingSlash(new URL(opts.pushOrigin ?? opts.origin).toString()),
    fetch: opts.fetch ?? globalThis.fetch,
    pullIntervalMs: opts.pullIntervalMs,
    wake: typeof opts.wake === 'object' ? opts.wake.getToken : (opts.wake ?? false),
    queryTransform: opts.queryTransform,
    queryForward: opts.queryForward === true,
    payloadCodecID: payloadCodec.id,
    lifecycle: opts.lifecycle,
  }
  const existing = pageRegistry.transportsByOrigin.get(key)
  if (existing) {
    const conflicts = HTTP_PULL_OPTION_FIELDS.filter(
      (field) => !Object.is(existing.options[field], options[field])
    )
    if (conflicts.length > 0) {
      throw new Error(
        `HTTP pull transport for ${key} is already installed with different ${conflicts.join(', ')}`
      )
    }
    return existing.transport
  }
  const transport = installHttpPullTransport({ ...opts, payloadCodec })
  pageRegistry.transportsByOrigin.set(key, { transport, options })
  return transport
}

export type ZeroClientTransportPlugin = {
  readonly type: 'orez-client'
  install(origin: string): HttpPullTransport
}

export function createZeroClientTransport(
  options: Omit<HttpPullTransportOptions, 'origin'> = {}
): ZeroClientTransportPlugin {
  return Object.freeze({
    type: 'orez-client' as const,
    install(origin: string) {
      return ensureHttpPullTransport({ ...options, origin })
    },
  })
}

export async function flushHttpPullTransports() {
  const pageRegistry = getHttpPullPageRegistry()
  await Promise.all(
    [...pageRegistry.transportsByOrigin.values()].map(({ transport }) =>
      transport.flush()
    )
  )
}

// a data-changed nudge for hosts that know the server advanced (e.g. a local
// mutation applied elsewhere in the page): pull every installed transport now
// instead of waiting out its pullIntervalMs.
export async function pullHttpPullTransports() {
  const pageRegistry = getHttpPullPageRegistry()
  await Promise.all(
    [...pageRegistry.transportsByOrigin.values()].map(({ transport }) =>
      transport.pull()
    )
  )
}

class ZeroHttpSocket {
  readonly CONNECTING = 0
  readonly OPEN = 1
  readonly CLOSING = 2
  readonly CLOSED = 3

  readonly url: string
  readyState = this.CONNECTING

  private readonly connectURL: URL
  private authToken: string | undefined
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
  // a snapshot clear removes both rows and got-query marks from Replicache.
  // remember every delivered mark so a clear-bearing poke can restore them.
  private ackedGotHashes = new Set<string>()
  // query-aware extension state: the accumulated un-acked desired-query delta
  // to ship, a client-side query-state version that bumps on each change, and
  // the version/length of the delta the in-flight pull sent (to clear the
  // acked prefix on the server's ack).
  private desiredQueryPatch: QueryPatchOp[] = []
  private queryVersion = 0
  private sentQueryVersion: number | undefined
  private sentQueryPatchLen = 0
  private pullInFlight: Promise<void> | undefined
  private pullAfterCurrent = false
  private pendingPushes: unknown[] = []
  private pushInFlight = false
  private pushCompletion: Promise<void> = Promise.resolve()
  private flushGeneration = 0
  private readonly pendingUpstream = new Set<Promise<void>>()
  private nextLocalCookieID: number
  private openTimer: ReturnType<typeof setTimeout> | undefined
  private pullTimer: ReturnType<typeof setInterval> | undefined
  private wakeSocket: { close(): void } | undefined
  private wakeConnecting = false
  private wakeReconnectTimer: ReturnType<typeof setTimeout> | undefined
  private readonly generation: number
  private readonly zeroInstanceID: string
  private readonly connectionAttemptID: string
  private readonly socketID: string
  private readonly attemptStartedAt: number | undefined
  private settled = false

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
    this.generation = ++this.state.nextSocketGeneration
    this.zeroInstanceID = `${this.state.pageID}:zero:${this.clientID}`
    this.connectionAttemptID = `${this.zeroInstanceID}:attempt:${this.wsid}`
    this.socketID = `${this.state.transportID}:socket:${this.generation}`
    const attemptStartedAtParam = this.connectURL.searchParams.get('ts')
    const attemptStartedAt = Number(attemptStartedAtParam)
    this.attemptStartedAt =
      attemptStartedAtParam !== null && Number.isFinite(attemptStartedAt)
        ? attemptStartedAt
        : undefined
    const previousSocket = this.state.activeSocketByClient.get(this.clientID)
    this.state.activeSocketGenerationByClient.set(this.clientID, this.generation)
    this.state.activeSocketByClient.set(this.clientID, this)
    previousSocket?.supersede()
    this.emitLifecycle('created')
    const baseCookie = this.connectURL.searchParams.get('baseCookie')
    this.cookie = baseCookie ? baseCookie : null
    // the local cookie ID is encoded into the suffix of any cookie we
    // already advanced past (toLocalWebSocketCookie). reconnects rehydrate
    // this.cookie from the URL baseCookie param replicache persisted, so we
    // must resume the counter from its suffix — otherwise the first poke on
    // the new socket re-emits an identical suffix and replicache trips
    // "cookie did not change, but patch is not empty" and drops the patch.
    const suffixMatch = this.cookie?.match(/#(\d+)$/)
    this.nextLocalCookieID = suffixMatch ? Number(suffixMatch[1]) : 0

    const decoded = decodeSecProtocol(protocols)
    this.authToken = decoded.authToken
    this.queueDesiredQueries(decoded.initConnectionMessage?.[1])

    this.state.sockets.add(this)
    // open on the next task so Zero can attach its listeners. open() checks the
    // attempt timestamp first, so async socket construction that outlived Zero's
    // deadline closes without delivering an event to abandoned state.
    this.openTimer = setTimeout(() => this.open(), 0)
  }

  addEventListener(type: SocketEventType, listener: SocketListener | null) {
    if (listener) {
      this.listeners[type]?.add(listener)
      this.emitLifecycle('listener', { listener: type })
    }
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
        this.flushGeneration++
        this.queueDesiredQueries(message[1])
        this.requestPullAfterCurrent()
        return
      case 'updateAuth':
        this.flushGeneration++
        this.authToken = (message[1] as { auth?: string }).auth
        return
      case 'push':
        this.enqueuePush(message[1])
        return
      case 'ping':
        this.emitMessage(['pong', {}])
        return
      case 'pull': {
        this.flushGeneration++
        const recoveryPull = this.answerMutationRecoveryPull(message[1])
        this.trackUpstream(recoveryPull)
        this.run(recoveryPull)
        return
      }
      case 'deleteClients':
      case 'ackMutationResponses':
        return
      default:
        throw new Error(`unsupported zero-http upstream message ${message[0]}`)
    }
  }

  close(code = 1000, reason = '') {
    if (!this.settle('close', { code, reason })) return
    this.emit('close', { code, reason, wasClean: code <= 1001 })
  }

  pull(): Promise<void> {
    if (this.readyState !== this.OPEN) return Promise.resolve()
    if (this.pullInFlight) return this.pullInFlight
    this.pullInFlight = this.fetchPull(this.clientGroupID, this.cookie, true)
      .then((response) => {
        if (this.state.queryAware) this.applyServerGotQueries(response)
        if (response.unchanged) {
          this.emitGotQueriesPatch(response.cookie)
          return
        }
        this.emitPoke(response)
      })
      .catch((error) => {
        this.fail(error)
        throw error
      })
      .finally(async () => {
        const pullAgain = this.pullAfterCurrent
        this.pullAfterCurrent = false
        this.pullInFlight = undefined
        if (pullAgain && this.readyState !== this.CLOSED) await this.pull()
      })
    return this.pullInFlight
  }

  // settle every tracked upstream effect, then prove quiescence with two pull
  // round trips. repeat if work arrived during that pass.
  async flush(): Promise<void> {
    for (;;) {
      const generation = this.flushGeneration
      await Promise.all([...this.pendingUpstream])
      await this.pushCompletion
      await this.pull()
      await Promise.all([...this.pendingUpstream])
      await this.pushCompletion
      await this.pull()
      await Promise.resolve()
      if (this.flushGeneration === generation && this.pendingUpstream.size === 0) {
        return
      }
    }
  }

  private open() {
    if (this.readyState !== this.CONNECTING) return
    const attemptAgeMs = this.getAttemptAgeMs()
    if (attemptAgeMs !== undefined && attemptAgeMs >= ZERO_CONNECT_TIMEOUT_MS) {
      const reason = `zero-http connection attempt ${this.wsid} expired after ${Math.round(
        attemptAgeMs
      )}ms before socket construction completed`
      if (!this.settle('aborted', { code: 1000, reason })) return
      this.emit('close', { code: 1000, reason, wasClean: true })
      return
    }
    this.readyState = this.OPEN
    this.emitLifecycle('open')
    this.emit('open', {})
    this.emitMessage(['connected', { wsid: this.wsid, timestamp: Date.now() }])
    setTimeout(() => this.run(this.pull()), 0)
    if (this.state.pullIntervalMs) {
      this.pullTimer = setInterval(() => {
        this.run(this.pull())
      }, this.state.pullIntervalMs)
    }
    if (this.state.wake) this.openWakeChannel()
  }

  // notification-only wake channel: a real WebSocket to <origin>/wake that
  // carries no data. any frame means "pull now", so a wake triggers an
  // immediate (coalesced) pull — push-shaped propagation without waiting on
  // the poll interval. advisory only: if it drops we reconnect, and the
  // interval poll remains the safety net that guarantees convergence.
  private openWakeChannel() {
    const Native = this.state.nativeWebSocket
    if (
      !Native ||
      this.wakeSocket ||
      this.wakeConnecting ||
      this.readyState !== this.OPEN
    ) {
      return
    }
    this.wakeConnecting = true
    const wsBase = this.state.originString.replace(/^http/, 'ws')
    void (async () => {
      let wakeToken: string | undefined
      let socket: {
        onmessage: (() => void) | null
        onclose: (() => void) | null
        onerror: (() => void) | null
        close(): void
      }
      try {
        if (typeof this.state.wake === 'object') {
          wakeToken = await this.state.wake.getToken()
        }
        if (this.readyState !== this.OPEN || this.wakeSocket) return
        const url =
          `${wsBase}/wake?clientID=${encodeURIComponent(this.clientID)}` +
          (wakeToken === undefined ? '' : `&wakeToken=${encodeURIComponent(wakeToken)}`)
        socket = new Native(url) as unknown as typeof socket
      } catch {
        this.scheduleWakeReconnect()
        return
      } finally {
        this.wakeConnecting = false
      }
      this.wakeSocket = socket
      const reconnect = () => {
        if (this.wakeSocket !== socket) return
        this.wakeSocket = undefined
        this.scheduleWakeReconnect()
      }
      // route through requestPullAfterCurrent, NOT pull() directly: a wake that
      // lands while a pull is already in flight must set pullAfterCurrent so the
      // in-flight pull re-runs and picks up the woken change. calling pull()
      // directly would return the existing promise and silently drop the wake,
      // leaving convergence to the safety poll (a burst-storm latency bug).
      socket.onmessage = () => this.requestPullAfterCurrent()
      socket.onclose = reconnect
      socket.onerror = reconnect
    })()
  }

  private scheduleWakeReconnect() {
    if (this.readyState !== this.OPEN || this.wakeReconnectTimer) return
    this.wakeReconnectTimer = setTimeout(() => {
      this.wakeReconnectTimer = undefined
      this.openWakeChannel()
    }, 500)
  }

  private closeWakeChannel() {
    if (this.wakeReconnectTimer) {
      clearTimeout(this.wakeReconnectTimer)
      this.wakeReconnectTimer = undefined
    }
    const socket = this.wakeSocket
    this.wakeSocket = undefined
    if (socket) {
      try {
        socket.close()
      } catch {
        // best effort: an already-closing wake socket is harmless
      }
    }
  }

  private queueDesiredQueries(body: unknown) {
    const desiredQueriesPatch = (body as { desiredQueriesPatch?: unknown })
      ?.desiredQueriesPatch
    if (!Array.isArray(desiredQueriesPatch)) return
    if (!this.state.queryAware) {
      // baseline dialect: synthesize the got-query ack locally
      this.pendingGotQueriesPatch.push(...gotQueriesPatch(desiredQueriesPatch))
      return
    }
    // query-aware: accumulate the desired-query delta to ship to the server. a
    // put ships its inline ast, or a client-resolved ast (queryTransform), or
    // name+args for the server to resolve (queryForward). the server owns the
    // got-query ack.
    const transform = this.state.queryTransform
    for (const op of desiredQueriesPatch as DesiredQueryPatchOp[]) {
      if (op.op === 'clear') {
        this.desiredQueryPatch.push({ op: 'clear' })
      } else if (op.op === 'del') {
        this.desiredQueryPatch.push({ op: 'del', hash: op.hash })
      } else if (op.op === 'put') {
        const inline = (op as { ast?: unknown }).ast
        const name = (op as { name?: string }).name ?? ''
        const args = ((op as { args?: readonly unknown[] }).args ??
          []) as readonly unknown[]
        if (this.state.queryForward) {
          this.desiredQueryPatch.push({ op: 'put', hash: op.hash, name, args })
        } else if (inline !== undefined) {
          this.desiredQueryPatch.push({ op: 'put', hash: op.hash, ast: inline })
        } else if (transform) {
          this.desiredQueryPatch.push({
            op: 'put',
            hash: op.hash,
            ast: transform(name, args),
          })
        }
      }
    }
    this.queryVersion++
  }

  private async push({ body, frameCount, mutationCount }: PushBatch) {
    const encodedBody = await this.state.payloadCodec.encodePush(body as PushRequest)
    const response = (await this.postJSON('/push', encodedBody)) as {
      pushResponse?: unknown
    }
    this.emitLifecycle('push', {
      pushFrameCount: frameCount,
      mutationCount,
    })
    // mutation RECOVERY pushes carry a PREVIOUS client's pending mutations,
    // and the server response echoes that old clientID. zero-cache's pusher
    // groups results by clientID and only delivers a client its OWN results
    // (recovered mutations settle via lastMutationIDChanges in pokes); the
    // raw response would trip zero's "received mutation for the wrong
    // client" assert and kill the connection. mirror the same filtering.
    this.emitMessage([
      'pushResponse',
      filterMutationResultsToClient(
        'pushResponse' in response ? response.pushResponse : response,
        this.clientID
      ),
    ])
    this.requestPullAfterCurrent()
  }

  private enqueuePush(body: unknown) {
    this.flushGeneration++
    this.pendingPushes.push(body)
    if (this.pushInFlight) return
    const drain = this.drainPushes()
    this.pushCompletion = drain
    this.trackUpstream(drain)
    this.run(drain)
  }

  private async drainPushes() {
    this.pushInFlight = true
    try {
      while (this.readyState !== this.CLOSED && this.pendingPushes.length > 0) {
        await this.push(this.takePushBatch())
      }
    } finally {
      this.pushInFlight = false
    }
  }

  private takePushBatch(): PushBatch {
    const first = this.pendingPushes.shift()
    const firstBody = parsePushBody(first)
    if (!firstBody) {
      return { body: first, frameCount: 1, mutationCount: 0 }
    }

    const mutations = [...firstBody.mutations]
    let frameCount = 1
    while (this.pendingPushes.length > 0) {
      const nextBody = parsePushBody(this.pendingPushes[0])
      if (
        !nextBody ||
        !areCompatiblePushBodies(firstBody, nextBody) ||
        mutations.length + nextBody.mutations.length > MAX_PUSH_BATCH_MUTATIONS
      ) {
        break
      }
      this.pendingPushes.shift()
      mutations.push(...nextBody.mutations)
      frameCount++
    }

    return {
      body: frameCount === 1 ? firstBody : { ...firstBody, mutations },
      frameCount,
      mutationCount: mutations.length,
    }
  }

  private trackUpstream(promise: Promise<void>) {
    this.pendingUpstream.add(promise)
    void promise.then(
      () => this.pendingUpstream.delete(promise),
      () => this.pendingUpstream.delete(promise)
    )
  }

  private requestPullAfterCurrent() {
    if (this.pullInFlight) {
      this.pullAfterCurrent = true
      return
    }
    this.run(this.pull())
  }

  // in query-aware mode the got-query ack is authoritative from the server:
  // take the server's gotQueries.patch as the got patch to emit (replacing
  // local synthesis), and clear the acked prefix of the shipped desired delta
  // once the server acks that version (the ack never leads its row effects —
  // invariant 13 — so the client marks a query got only after its rows land).
  private applyServerGotQueries(response: PullResponse) {
    const got = response.gotQueries
    this.pendingGotQueriesPatch = got ? [...got.patch] : []
    if (
      got &&
      this.sentQueryVersion !== undefined &&
      got.version >= this.sentQueryVersion
    ) {
      this.desiredQueryPatch.splice(0, this.sentQueryPatchLen)
      this.sentQueryVersion = undefined
    }
  }

  private async answerMutationRecoveryPull(body: {
    clientGroupID: string
    cookie: string | null
    requestID: string
  }) {
    const response = await this.fetchPull(body.clientGroupID, body.cookie, false)
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

  private async fetchPull(
    clientGroupID: string,
    cookie: string | null,
    includeQueries: boolean
  ) {
    const body: Record<string, unknown> = {
      clientID: this.clientID,
      clientGroupID,
      cookie: toHttpCookie(cookie),
    }
    // ship the un-acked desired-query delta with the pull; remember what we
    // sent so the server ack can clear exactly that prefix. a recovery pull
    // (includeQueries=false) never carries desires.
    if (includeQueries && this.state.queryAware && this.desiredQueryPatch.length > 0) {
      this.sentQueryVersion = this.queryVersion
      this.sentQueryPatchLen = this.desiredQueryPatch.length
      body.queries = { version: this.queryVersion, patch: [...this.desiredQueryPatch] }
    } else {
      this.sentQueryVersion = undefined
    }
    const response = (await this.postJSON('/pull', body)) as PullResponse
    return this.state.payloadCodec.decodePull(response)
  }

  private async postJSON(path: '/pull' | '/push', body: unknown) {
    const base =
      path === '/push' ? this.state.pushOriginString : this.state.pullOriginString
    const url = new URL(`${base}${path}`)
    if (path === '/push') {
      url.searchParams.set('schema', `${this.state.appID}_${this.state.shardNum}`)
      url.searchParams.set('appID', this.state.appID)
    }
    const response = await this.state.fetch(url, {
      method: 'POST',
      headers: {
        authorization: this.authToken ? `Bearer ${this.authToken}` : '',
        'content-type': 'application/json',
      },
      body: JSON.stringify(body),
    })
    if (!response.ok) {
      let body = ''
      try {
        body = await response.text()
      } catch {
        // the status still owns the failure when the response body is unreadable
      }
      throw new ZeroHttpResponseError(
        path,
        response.status,
        body.length > 512 ? `${body.slice(0, 512)}...` : body || undefined,
        retryAfterMsFromResponse(response, body)
      )
    }
    return response.json()
  }

  private run(promise: Promise<void>) {
    void promise.catch((error) => this.fail(error))
  }

  private fail(error: unknown) {
    if (this.readyState === this.CLOSED) return
    this.emitLifecycle('failure', { reason: errorMessage(error) })
    if (isAuthHTTPError(error)) {
      this.emitMessage([
        'error',
        {
          kind: 'Unauthorized',
          message: error.message,
          origin: 'server',
        },
      ])
      if (this.readyState !== this.CLOSED) this.close(1000, error.message)
      return
    }
    if (isStaleClientCookieError(error)) {
      // the client's cookie is AHEAD of the server watermark — the server
      // lost or reset its change-tracking state (replica reset / restore).
      // mirror zero-cache's InvalidConnectionRequestBaseCookie error frame so
      // the stock client drops its local db and rebuilds from scratch instead
      // of reconnect-looping on 409 forever.
      this.emitMessage([
        'error',
        {
          kind: 'InvalidConnectionRequestBaseCookie',
          message: error.message,
          origin: 'server',
        },
      ])
      if (this.readyState !== this.CLOSED) this.close(1000, error.message)
      return
    }
    if (
      error instanceof ZeroHttpResponseError &&
      error.status >= 400 &&
      error.status < 500 &&
      error.status !== 408 &&
      error.status !== 425 &&
      error.status !== 429
    ) {
      this.emitMessage([
        'error',
        {
          kind: error.path === '/pull' ? 'InvalidConnectionRequest' : 'InvalidPush',
          message: error.message,
          origin: 'server',
        },
      ])
      if (this.readyState !== this.CLOSED) this.close(1000, error.message)
      return
    }
    // everything left is transient: a rate limit (429), a throttled or failing
    // server (5xx), or a network error. stock Zero skips its run-loop sleep for
    // a non-clean socket close — AbruptClose is one of the four close reasons
    // throwIfConnectionError deliberately swallows — so closing 1011 here
    // reconnects with NO delay, and each reconnect re-pushes the same pending
    // mutations. measured: 605 pushes/s against a 429 and 374 pulls/s against a
    // 500, which turns one transient failure into a self-sustaining storm that
    // rate-limits the client permanently. ServerOverloaded is the protocol's
    // only backoff-bearing frame (MutationRateLimited is swallowed with no
    // delay at all), so a transient failure closes cleanly and carries the
    // server's own Retry-After as the run loop's minimum sleep.
    const retryAfterMs =
      error instanceof ZeroHttpResponseError ? error.retryAfterMs : undefined
    this.emit('error', { error })
    this.emitMessage([
      'error',
      {
        kind: 'ServerOverloaded',
        message: errorMessage(error),
        // the backoff frame is the sync backend's to send, and Zero only
        // treats it as a server error when the origin says so.
        origin: 'zeroCache',
        // a server may hand back a window measured in hours (a daily quota).
        // sleeping that long strands the client, so cap what it will honor.
        ...(retryAfterMs === undefined
          ? {}
          : { minBackoffMs: retryAfterMs, maxBackoffMs: MAX_RETRY_AFTER_BACKOFF_MS }),
      },
    ])
    if (this.readyState !== this.CLOSED) this.close(1000, errorMessage(error))
  }

  private emitPoke(response: Exclude<PullResponse, { unchanged: true }>) {
    const currentServer = toHttpCookie(this.cookie)
    let nextCookie: string
    if (currentServer !== null && response.cookie < currentServer) {
      // the server watermark is BEHIND the client: a real reset/restore. mirror
      // the 409 stale path instead of poking the client backwards.
      throw new Error(
        `zero-http pull returned stale cookie ${response.cookie} for ${this.cookie}`
      )
    } else if (currentServer !== null && response.cookie === currentServer) {
      // same server watermark but a non-empty patch: a query-aware membership
      // delta (a desired-query change recomputes rows without advancing the
      // change log). bump a client-local cookie id so replicache sees a changed
      // cookie — otherwise it trips "cookie did not change, but patch is not
      // empty" and drops the patch.
      nextCookie = toLocalWebSocketCookie(response.cookie, ++this.nextLocalCookieID)
    } else {
      nextCookie = toWebSocketCookie(response.cookie) as string
    }

    const pokeID = `zero-http-${++this.state.nextPokeID}`
    let gotQueries = dedupeGotQueriesPatch(this.pendingGotQueriesPatch)
    this.pendingGotQueriesPatch = []
    const rowsCleared = response.rowsPatch.some(
      (op) => (op as { op?: string })?.op === 'clear'
    )
    if (rowsCleared && this.ackedGotHashes.size > 0) {
      gotQueries = dedupeGotQueriesPatch([
        ...[...this.ackedGotHashes].map((hash) => ({ op: 'put' as const, hash })),
        ...gotQueries,
      ])
    }
    this.recordAckedGotQueries(gotQueries)

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

  private emitGotQueriesPatch(cookie: number | null) {
    if (this.pendingGotQueriesPatch.length === 0) return

    const serverCookie = cookie ?? toHttpCookie(this.cookie)
    if (serverCookie === null) return
    const nextCookie = toLocalWebSocketCookie(serverCookie, ++this.nextLocalCookieID)
    const pokeID = `zero-http-${++this.state.nextPokeID}`
    const gotQueries = dedupeGotQueriesPatch(this.pendingGotQueriesPatch)
    this.pendingGotQueriesPatch = []
    this.recordAckedGotQueries(gotQueries)

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
        gotQueriesPatch: gotQueries,
      },
    ])
    this.emitMessage(['pokeEnd', { pokeID, cookie: nextCookie }])
    this.cookie = nextCookie
  }

  private recordAckedGotQueries(patch: GotQueryPatchOp[]) {
    for (const op of patch) {
      if (op.op === 'clear') this.ackedGotHashes.clear()
      else if (op.op === 'put') this.ackedGotHashes.add(op.hash)
      else this.ackedGotHashes.delete(op.hash)
    }
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

  private supersede() {
    this.settle('superseded')
  }

  private settle(
    type: Extract<HttpPullLifecycleEvent['type'], 'close' | 'superseded' | 'aborted'>,
    detail: Pick<HttpPullLifecycleEvent, 'code' | 'reason'> = {}
  ) {
    if (this.settled) return false
    this.settled = true
    if (this.openTimer !== undefined) clearTimeout(this.openTimer)
    if (this.pullTimer !== undefined) clearInterval(this.pullTimer)
    this.closeWakeChannel()
    this.readyState = this.CLOSED
    this.state.sockets.delete(this)
    if (this.state.activeSocketByClient.get(this.clientID) === this) {
      this.state.activeSocketByClient.delete(this.clientID)
      this.state.activeSocketGenerationByClient.delete(this.clientID)
    }
    this.emitLifecycle(type, detail)
    return true
  }

  private getAttemptAgeMs() {
    return this.attemptStartedAt === undefined
      ? undefined
      : performance.now() - this.attemptStartedAt
  }

  private emitLifecycle(
    type: HttpPullLifecycleEvent['type'],
    detail: Pick<
      HttpPullLifecycleEvent,
      'listener' | 'code' | 'reason' | 'pushFrameCount' | 'mutationCount'
    > = {}
  ) {
    this.state.lifecycle?.({
      type,
      pageID: this.state.pageID,
      transportID: this.state.transportID,
      zeroInstanceID: this.zeroInstanceID,
      clientGroupID: this.clientGroupID,
      connectionAttemptID: this.connectionAttemptID,
      socketID: this.socketID,
      generation: this.generation,
      activeGeneration:
        this.state.activeSocketGenerationByClient.get(this.clientID) ?? this.generation,
      clientID: this.clientID,
      wsid: this.wsid,
      timestamp: Date.now(),
      attemptStartedAt: this.attemptStartedAt,
      attemptAgeMs: this.getAttemptAgeMs(),
      ...detail,
    })
  }
}

function parsePushBody(value: unknown): PushBody | undefined {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return
  if (!('mutations' in value) || !Array.isArray(value.mutations)) return
  // the runtime checks above establish the only fields batching reads.
  return value as PushBody
}

function areCompatiblePushBodies(left: PushBody, right: PushBody) {
  return (
    left.clientGroupID === right.clientGroupID &&
    left.pushVersion === right.pushVersion &&
    left.schemaVersion === right.schemaVersion
  )
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

// initConnection and a racing changeDesiredQueries can acknowledge the same
// hash in one poke. Zero's complete tracking requires one final op per hash.
function dedupeGotQueriesPatch(patch: GotQueryPatchOp[]): GotQueryPatchOp[] {
  let clear = false
  const lastOpByHash = new Map<string, GotQueryPatchOp>()
  for (const op of patch) {
    if (op.op === 'clear') {
      clear = true
      lastOpByHash.clear()
      continue
    }
    lastOpByHash.delete(op.hash)
    lastOpByHash.set(op.hash, op)
  }
  const deduped: GotQueryPatchOp[] = clear ? [{ op: 'clear' }] : []
  deduped.push(...lastOpByHash.values())
  return deduped
}

function toHttpCookie(cookie: string | null): number | null {
  if (cookie === null || cookie === '') return null
  const parsed = Number(cookie.slice(0, COOKIE_WIDTH))
  if (!Number.isFinite(parsed)) {
    throw new Error(`zero-http cookie is not numeric: ${cookie}`)
  }
  return parsed
}

function toWebSocketCookie(cookie: number | null): string | null {
  return cookie === null ? null : String(cookie).padStart(COOKIE_WIDTH, '0')
}

function toLocalWebSocketCookie(cookie: number, localID: number): string {
  return `${String(cookie).padStart(COOKIE_WIDTH, '0')}#${String(localID).padStart(
    6,
    '0'
  )}`
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error)
}

class ZeroHttpResponseError extends Error {
  constructor(
    readonly path: '/pull' | '/push',
    readonly status: number,
    bodyPreview: string | undefined,
    readonly retryAfterMs: number | undefined
  ) {
    super(
      `zero-http ${path} failed with ${status}${bodyPreview ? `: ${bodyPreview}` : ''}`
    )
  }
}

// a throttled server states its own wait, either as `retryAfterMs` in the JSON
// error body or as the standard delta-seconds Retry-After header. an HTTP-date
// Retry-After is ignored: the client clock cannot be trusted to compare with it.
function retryAfterMsFromResponse(response: Response, body: string) {
  const header = response.headers.get('retry-after')
  const headerSeconds = header === null ? Number.NaN : Number(header)
  const headerMs = Number.isFinite(headerSeconds)
    ? Math.max(0, headerSeconds) * 1_000
    : undefined
  if (body) {
    try {
      const parsed: unknown = JSON.parse(body)
      const value = (parsed as { retryAfterMs?: unknown } | null)?.retryAfterMs
      if (typeof value === 'number' && Number.isFinite(value)) {
        return Math.max(0, value)
      }
    } catch {
      // an unparseable or truncated body leaves the header as the only hint
    }
  }
  return headerMs
}

function isAuthHTTPError(error: unknown): error is ZeroHttpResponseError {
  return (
    error instanceof ZeroHttpResponseError &&
    (error.status === 401 || error.status === 403)
  )
}

// keep only this client's own mutation results — zero-cache's pusher does the
// same per-client fan-out (results for clients without a live connection are
// dropped; their LMID advance arrives via the next pull's
// lastMutationIDChanges, which is how recovered mutations settle).
function filterMutationResultsToClient(pushResponse: unknown, clientID: string) {
  if (!pushResponse || typeof pushResponse !== 'object') return pushResponse
  const mutations = (pushResponse as { mutations?: unknown }).mutations
  if (!Array.isArray(mutations)) return pushResponse
  return {
    ...pushResponse,
    mutations: mutations.filter(
      (m) => (m as { id?: { clientID?: string } })?.id?.clientID === clientID
    ),
  }
}

function isStaleClientCookieError(error: unknown): error is ZeroHttpResponseError {
  return (
    error instanceof ZeroHttpResponseError &&
    error.path === '/pull' &&
    error.status === 409
  )
}
