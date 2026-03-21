/**
 * fastify shim for cloudflare workers.
 *
 * minimal fastify replacement that captures route registrations and
 * exposes them via inject() for request processing. zero-cache's
 * HttpService creates a Fastify instance, registers routes, and calls
 * listen(). on CF Workers we skip listen() and route DO fetch()
 * through inject().
 *
 * usage with bundler alias:
 *   alias: { 'fastify': './src/worker/shims/fastify.js' }
 */

import EventEmitter from 'node:events'

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

// -- fastify shim instance --

class FastifyShim {
  server: FakeHttpServer
  #routes = new Map<string, RouteHandler>()
  #readyResolvers: Array<() => void> = []

  constructor() {
    this.server = new FakeHttpServer()
  }

  // route registration
  get(path: string, handler: RouteHandler) {
    this.#routes.set(`GET:${path}`, handler)
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

  // plugin registration (no-op for now — zero-cache doesn't use plugins)
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
  // register on globalThis so the CF embed can access it
  ;(globalThis as any).__orez_fastify_instance = instance
  return instance
}

export default Fastify
export type { FastifyRequest, FastifyReply, FastifyShim }
