/**
 * fastify shim for cloudflare workers.
 *
 * minimal fastify replacement that captures route registrations and
 * exposes them via inject() for request processing. zero-cache's
 * HttpService creates a Fastify instance, registers routes, and calls
 * listen(). on CF Workers we skip listen() and route DO fetch()
 * through inject().
 *
 * supports { websocket: true } routes: when a handoff event arrives on
 * the server, matches against websocket routes and calls the handler
 * with the socket directly. this enables the serving-replicator's
 * in-process WebSocket connection to the change-streamer.
 *
 * usage with bundler alias:
 *   alias: { 'fastify': './src/worker/shims/fastify.js' }
 */

import EventEmitter from 'node:events'

import { WebSocket as WsShim, WebSocketServer as WsServerShim } from './ws.js'

// -- types matching fastify's minimal surface used by zero-cache --

interface FastifyRequest {
  headers: Record<string, string | undefined>
  url: string
  method: string
  body?: unknown
  query?: Record<string, string>
  params?: Record<string, string>
}

interface FastifyReply {
  code(statusCode: number): FastifyReply
  header(name: string, value: string): FastifyReply
  send(payload?: unknown): void
  type(contentType: string): FastifyReply
  status(statusCode: number): FastifyReply
}

type RouteHandler = (
  request: FastifyRequest,
  reply: FastifyReply
) => unknown | Promise<unknown>

interface InjectOptions {
  method: string
  url: string
  headers?: Record<string, string>
  payload?: string | null
}

interface InjectResult {
  statusCode: number
  headers: Record<string, string>
  body: string
}

// -- fake http.Server replacement --
// uses EventEmitter with onMessageType for zero-cache's
// installWebSocketHandoff non-Server branch.

class FakeHttpServer extends EventEmitter {
  #address = { address: '0.0.0.0', port: 0, family: 'IPv4' as const }

  address() {
    return this.#address
  }

  /** match the onMessageType pattern from zero-cache processes.js */
  onMessageType(
    type: string,
    handler: (msg: unknown, sendHandle?: unknown) => void
  ): this {
    this.on('message', (data: unknown, sendHandle?: unknown) => {
      if (Array.isArray(data) && data.length === 2 && data[0] === type) {
        handler(data[1], sendHandle)
      }
    })
    return this
  }

  listen() {
    /* no-op on CF */
  }
  close() {
    /* no-op on CF */
  }
}

// use the real WebSocketServer from the WS shim — it wraps raw sockets
// in a proper WebSocket class with ping/pong/on/emit etc.

// -- route pattern matching --
// converts fastify route patterns like "/replication/:version/changes"
// to regex for matching incoming URLs

function patternToRegex(pattern: string): RegExp {
  const escaped = pattern
    .replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
    .replace(/:(\w+)/g, '(?<$1>[^/]+)')
  return new RegExp(`^${escaped}$`)
}

// -- fastify shim instance --

class FastifyShim {
  server: FakeHttpServer
  websocketServer: WsServerShim
  #routes = new Map<string, RouteHandler>()
  #wsRoutes: Array<{ pattern: RegExp; handler: (ws: unknown, req: any) => void }> = []
  #readyResolvers: Array<() => void> = []

  constructor() {
    this.server = new FakeHttpServer()
    this.websocketServer = new WsServerShim()
    this.#installWsHandoffHandler()
  }

  // listen for in-process WebSocket handoff events on the server.
  // when the WS shim creates an in-process connection, it emits a handoff
  // event. we match the URL against registered { websocket: true } routes
  // and call the handler with the socket.
  #installWsHandoffHandler() {
    this.server.onMessageType('handoff', (msg: any, socket?: any) => {
      this.tryHandoff(msg, socket)
    })
  }

  // try to match a handoff message against registered websocket routes.
  // returns true if a route matched, false otherwise.
  // this is public so callers (ws shim, browser-embed) can iterate
  // all fastify instances and stop at the first match.
  tryHandoff(msg: any, socket?: any): boolean {
    if (!socket || !msg?.message?.url) return false
    const url = msg.message.url
    const parsedUrl = new URL(url, 'http://localhost')
    const pathname = parsedUrl.pathname

    for (const route of this.#wsRoutes) {
      if (route.pattern.test(pathname)) {
        const req = {
          url,
          headers: msg.message.headers || {},
          method: msg.message.method || 'GET',
        }
        // wrap socket through handleUpgrade so it gets the full WS API
        // (ping, on, once, terminate, etc.) needed by zero-cache's streamOut
        this.websocketServer.handleUpgrade(
          req,
          socket,
          Buffer.from(new Uint8Array(0)),
          (ws: any) => {
            route.handler(ws, req)
          }
        )
        return true
      }
    }
    return false
  }

  // route registration — supports optional { websocket: true } option
  get(path: string, optsOrHandler: any, handler?: any) {
    if (typeof optsOrHandler === 'function') {
      this.#routes.set(`GET:${path}`, optsOrHandler)
    } else if (optsOrHandler?.websocket && handler) {
      // websocket route — register for handoff matching
      this.#wsRoutes.push({
        pattern: patternToRegex(path),
        handler,
      })
    } else if (handler) {
      this.#routes.set(`GET:${path}`, handler)
    }
  }
  post(path: string, handler: RouteHandler) {
    this.#routes.set(`POST:${path}`, handler)
  }
  put(path: string, handler: RouteHandler) {
    this.#routes.set(`PUT:${path}`, handler)
  }
  delete(path: string, handler: RouteHandler) {
    this.#routes.set(`DELETE:${path}`, handler)
  }

  // plugin registration (no-op — zero-cache registers @fastify/websocket here)
  register(_plugin: unknown, _opts?: unknown): this {
    return this
  }

  // lifecycle
  async ready(): Promise<void> {
    for (const resolve of this.#readyResolvers) resolve()
    this.#readyResolvers = []
  }

  async listen(_opts?: { host?: string; port?: number }): Promise<string> {
    await this.ready()
    return '0.0.0.0:0'
  }

  async close(): Promise<void> {
    // no-op on CF
  }

  // inject — process a request through registered routes
  async inject(opts: InjectOptions): Promise<InjectResult> {
    const method = opts.method.toUpperCase()
    const urlObj = new URL(opts.url, 'http://localhost')
    const pathname = urlObj.pathname

    // find matching route
    const handler = this.#routes.get(`${method}:${pathname}`)
    if (!handler) {
      return { statusCode: 404, headers: {}, body: 'Not Found' }
    }

    // build fake request
    const request: FastifyRequest = {
      headers: opts.headers || {},
      url: opts.url,
      method,
      body: opts.payload ? tryParseJson(opts.payload) : undefined,
      query: Object.fromEntries(urlObj.searchParams),
      params: {},
    }

    // build fake reply
    let statusCode = 200
    const headers: Record<string, string> = {}
    let body = ''
    let sent = false

    const reply: FastifyReply = {
      code(code: number) {
        statusCode = code
        return reply
      },
      status(code: number) {
        statusCode = code
        return reply
      },
      header(name: string, value: string) {
        headers[name.toLowerCase()] = value
        return reply
      },
      type(contentType: string) {
        headers['content-type'] = contentType
        return reply
      },
      send(payload?: unknown) {
        sent = true
        if (payload === undefined || payload === null) {
          body = ''
        } else if (typeof payload === 'string') {
          body = payload
        } else {
          body = JSON.stringify(payload)
          if (!headers['content-type']) {
            headers['content-type'] = 'application/json'
          }
        }
      },
    }

    try {
      const result = await handler(request, reply)
      // if handler returned a value and didn't call reply.send()
      if (!sent && result !== undefined) {
        reply.send(result)
      }
    } catch (err) {
      statusCode = 500
      body = String(err)
    }

    return { statusCode, headers, body }
  }
}

function tryParseJson(str: string): unknown {
  try {
    return JSON.parse(str)
  } catch {
    return str
  }
}

// -- default export matching fastify's API --

function Fastify(_opts?: unknown): FastifyShim {
  const instance = new FastifyShim()
  // always overwrite — the ZeroDispatcher (which has the WS handoff routes)
  // is created LAST, so the final instance is the one handleWebSocket needs.
  ;(globalThis as any).__orez_fastify_instance = instance
  // track all instances so callers can try handoff against each one
  ;(globalThis as any).__orez_fastify_instances = (globalThis as any).__orez_fastify_instances || []
  ;(globalThis as any).__orez_fastify_instances.push(instance)
  return instance
}

export default Fastify
export type { FastifyRequest, FastifyReply, FastifyShim }
