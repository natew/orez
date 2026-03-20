/**
 * http service adapter for CF Workers / Durable Objects.
 *
 * bridges between a Durable Object's fetch() handler and zero-cache's
 * Fastify-based HTTP service. two routing modes:
 *
 * 1. HTTP routes — delegated to Fastify via inject() (health, keepalive, statz, etc.)
 * 2. WebSocket routes — handled via DO native WebSocket pairs, bypassing Fastify
 *    entirely since inject() cannot do upgrades.
 *
 * usage:
 *   const adapter = createHttpServiceAdapter()
 *   adapter.addWsRoute('/replication/v1/changes', handler)
 *   await adapter.initialize(fastifyInstance)
 *   // in DO fetch():
 *   return adapter.handleRequest(request)
 */

// CF Workers globals — not in Node.js types
declare const WebSocketPair: (new () => { 0: WebSocket; 1: WebSocket }) | undefined

// -- types for the adapter, kept minimal so we don't import fastify at module level --

/** handler for a WebSocket connection routed from a DO */
export type WebSocketHandler = (
  server: WebSocket,
  request: Request,
  url: URL
) => void | Promise<void>

/** minimal interface for what we need from fastify (avoids hard dep) */
export interface InjectableFastify {
  inject(opts: {
    method: string
    url: string
    headers?: Record<string, string>
    payload?: string | null
  }): Promise<{
    statusCode: number
    headers: Record<string, string>
    body: string
  }>
  ready(): Promise<void>
}

/** route match result */
interface RouteMatch {
  handler: WebSocketHandler
  pattern: string
}

/** result of preparing a WebSocket upgrade (before CF Response creation) */
export interface WebSocketUpgradeResult {
  client: WebSocket
  server: WebSocket
  handler: WebSocketHandler
  request: Request
  url: URL
}

/**
 * the http service adapter. manages routing between Fastify inject()
 * for regular HTTP and DO-native WebSocket pairs for upgrade requests.
 */
export class HttpServiceAdapter {
  private fastify: InjectableFastify | null = null
  private wsRoutes: Map<string, WebSocketHandler> = new Map()
  private wsPatterns: Array<{
    regex: RegExp
    pattern: string
    handler: WebSocketHandler
  }> = []
  private initialized = false

  /**
   * register a WebSocket route handler.
   * supports exact paths and simple patterns with wildcard segments.
   *
   * call before initialize() so routes are ready when requests arrive.
   *
   * examples:
   *   addWsRoute('/replication/v1/changes', handler)   -- exact
   *   addWsRoute('/replication/v[star]/changes', handler) -- wildcard (use *)
   */
  addWsRoute(path: string, handler: WebSocketHandler): void {
    if (path.includes('*')) {
      // convert glob-style pattern to regex
      const escaped = path.replace(/[.+?^${}()|[\]\\]/g, '\\$&')
      const regex = new RegExp('^' + escaped.replace(/\*/g, '[^/]+') + '$')
      this.wsPatterns.push({ regex, pattern: path, handler })
    } else {
      this.wsRoutes.set(path, handler)
    }
  }

  /**
   * initialize with a fastify instance. call after route registration
   * (e.g., after zero-cache's init callback has run) but before handling requests.
   * calls fastify.ready() to finalize route compilation.
   */
  async initialize(fastify: InjectableFastify): Promise<void> {
    this.fastify = fastify
    await fastify.ready()
    this.initialized = true
  }

  /** whether the adapter is ready to handle requests */
  get isReady(): boolean {
    return this.initialized
  }

  /**
   * handle an incoming request from a DO's fetch() method.
   * routes WebSocket upgrades to DO-native WebSocket handlers,
   * everything else to Fastify inject().
   */
  async handleRequest(request: Request): Promise<Response> {
    if (!this.initialized) {
      return new Response('service not ready', { status: 503 })
    }

    const url = new URL(request.url)

    // websocket upgrade — route to DO native handler
    if (request.headers.get('upgrade')?.toLowerCase() === 'websocket') {
      return this.handleWebSocket(request, url)
    }

    // regular HTTP — fastify inject
    return this.handleHttp(request, url)
  }

  /**
   * route a regular HTTP request through Fastify's inject() mechanism.
   * this works for all non-WebSocket routes (health, keepalive, statz, etc.).
   */
  private async handleHttp(request: Request, url: URL): Promise<Response> {
    if (!this.fastify) {
      return new Response('fastify not initialized', { status: 503 })
    }

    // build headers object from request
    const headers: Record<string, string> = {}
    request.headers.forEach((value, key) => {
      headers[key] = value
    })

    // read body for non-GET/HEAD requests
    let payload: string | null = null
    if (request.method !== 'GET' && request.method !== 'HEAD' && request.body) {
      payload = await request.text()
    }

    const result = await this.fastify.inject({
      method: request.method,
      url: url.pathname + url.search,
      headers,
      payload,
    })

    return new Response(result.body, {
      status: result.statusCode,
      headers: result.headers,
    })
  }

  /**
   * handle a WebSocket upgrade request using DO-native WebSocket pairs.
   *
   * creates a WebSocketPair, accepts the server side, and passes it
   * to the matched route handler. returns the 101 response with the
   * client side for the DO runtime to manage.
   *
   * note: WebSocketPair is a CF Workers global. in non-CF environments
   * this will throw — callers should polyfill or avoid ws routes.
   */
  private handleWebSocket(request: Request, url: URL): Response {
    const result = this.prepareWebSocketUpgrade(request, url)

    if (!result) {
      return new Response('no websocket handler for path', { status: 404 })
    }

    if (result instanceof Response) {
      return result
    }

    // invoke handler (fire-and-forget; handler manages the connection lifecycle)
    // errors in the handler should close the socket, not crash the DO
    Promise.resolve(result.handler(result.server, result.request, result.url)).catch(
      (err) => {
        try {
          result.server.close(1011, String(err))
        } catch {
          // socket may already be closed
        }
      }
    )

    // return 101 with client socket for DO runtime.
    // CF Workers Response constructor accepts status 101 + webSocket property.
    // standard fetch spec rejects status 101, so we try CF-style first and
    // fall back to a plain 101-like response for non-CF environments.
    try {
      return new Response(null, {
        status: 101,
        // @ts-expect-error — webSocket is a CF Workers Response extension
        webSocket: result.client,
      })
    } catch {
      // non-CF runtime (Node.js, vitest) — status 101 not allowed.
      // return a marker response that callers can detect.
      const resp = new Response(null, { status: 200 })
      ;(resp as any).__orez_websocket = result.client
      ;(resp as any).__orez_ws_upgrade = true
      return resp
    }
  }

  /**
   * prepare a WebSocket upgrade: match route, create pair, accept server side.
   * separated from handleWebSocket so tests can verify the setup logic without
   * needing the CF runtime's special Response(101) support.
   *
   * returns null if no route matches, a Response if WebSocketPair unavailable,
   * or a WebSocketUpgradeResult with the pair + handler ready to invoke.
   */
  prepareWebSocketUpgrade(
    request: Request,
    url: URL
  ): WebSocketUpgradeResult | Response | null {
    const match = this.matchWsRoute(url.pathname)
    if (!match) {
      return null
    }

    // WebSocketPair is a CF Workers runtime global
    const WsPair = (globalThis as any).WebSocketPair
    if (!WsPair) {
      return new Response('WebSocketPair not available in this runtime', { status: 500 })
    }

    const pair = new WsPair()
    const [client, server] = Object.values(pair) as [WebSocket, WebSocket]

    // accept the server side so we can send/receive (CF Workers WebSocket extension)
    ;(server as any).accept()

    return { client, server, handler: match.handler, request, url }
  }

  /**
   * match a pathname against registered WebSocket routes.
   * checks exact matches first, then pattern matches.
   */
  matchWsRoute(pathname: string): RouteMatch | null {
    // exact match first
    const exact = this.wsRoutes.get(pathname)
    if (exact) {
      return { handler: exact, pattern: pathname }
    }

    // pattern match
    for (const { regex, pattern, handler } of this.wsPatterns) {
      if (regex.test(pathname)) {
        return { handler, pattern }
      }
    }

    return null
  }
}

/**
 * factory function — creates a pre-configured adapter with the standard
 * zero-cache WebSocket routes registered.
 *
 * handlers are placeholders that will be replaced during Phase 3 integration
 * when we wire up the actual zero-cache WebSocket protocol handlers.
 */
export function createHttpServiceAdapter(): HttpServiceAdapter {
  const adapter = new HttpServiceAdapter()

  // register known zero-cache websocket route patterns.
  // actual handlers will be set during integration (Phase 3).
  // these patterns match zero-cache's replication endpoints.
  // for now, register the patterns so path matching works.

  return adapter
}
