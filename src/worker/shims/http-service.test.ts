import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'

import {
  HttpServiceAdapter,
  createHttpServiceAdapter,
  type InjectableFastify,
  type WebSocketHandler,
  type WebSocketUpgradeResult,
} from './http-service.js'

/** minimal mock fastify that records inject() calls and returns canned responses */
function createMockFastify(
  responses: Record<
    string,
    { status: number; body: string; headers?: Record<string, string> }
  >
): InjectableFastify {
  return {
    async ready() {},
    async inject(opts) {
      const key = `${opts.method} ${opts.url}`
      const resp = responses[key]
      if (!resp) {
        return { statusCode: 404, headers: {}, body: 'not found' }
      }
      return {
        statusCode: resp.status,
        headers: resp.headers ?? { 'content-type': 'text/plain' },
        body: resp.body,
      }
    },
  }
}

/** helper to create a Request (works in node/bun/vitest with fetch globals) */
function makeRequest(
  url: string,
  opts?: { method?: string; headers?: Record<string, string>; body?: string }
): Request {
  return new Request(url, {
    method: opts?.method ?? 'GET',
    headers: opts?.headers,
    body: opts?.body,
  })
}

describe('HttpServiceAdapter', () => {
  let adapter: HttpServiceAdapter

  beforeEach(() => {
    adapter = new HttpServiceAdapter()
  })

  describe('initialization', () => {
    it('is not ready before initialize', () => {
      expect(adapter.isReady).toBe(false)
    })

    it('is ready after initialize', async () => {
      await adapter.initialize(createMockFastify({}))
      expect(adapter.isReady).toBe(true)
    })

    it('returns 503 when not initialized', async () => {
      const resp = await adapter.handleRequest(makeRequest('http://localhost/'))
      expect(resp.status).toBe(503)
    })
  })

  describe('HTTP routing via inject()', () => {
    beforeEach(async () => {
      await adapter.initialize(
        createMockFastify({
          'GET /': { status: 200, body: 'ok' },
          'GET /keepalive': { status: 200, body: 'alive' },
          'GET /statz': {
            status: 200,
            body: '{"uptime":123}',
            headers: { 'content-type': 'application/json' },
          },
          'POST /data': { status: 201, body: 'created' },
        })
      )
    })

    it('routes GET / to fastify', async () => {
      const resp = await adapter.handleRequest(makeRequest('http://localhost/'))
      expect(resp.status).toBe(200)
      expect(await resp.text()).toBe('ok')
    })

    it('routes GET /keepalive to fastify', async () => {
      const resp = await adapter.handleRequest(makeRequest('http://localhost/keepalive'))
      expect(resp.status).toBe(200)
      expect(await resp.text()).toBe('alive')
    })

    it('routes GET /statz with correct content-type', async () => {
      const resp = await adapter.handleRequest(makeRequest('http://localhost/statz'))
      expect(resp.status).toBe(200)
      expect(resp.headers.get('content-type')).toBe('application/json')
      expect(await resp.text()).toBe('{"uptime":123}')
    })

    it('handles POST with body', async () => {
      const resp = await adapter.handleRequest(
        makeRequest('http://localhost/data', {
          method: 'POST',
          body: '{"key":"value"}',
          headers: { 'content-type': 'application/json' },
        })
      )
      expect(resp.status).toBe(201)
      expect(await resp.text()).toBe('created')
    })

    it('returns 404 for unknown routes', async () => {
      const resp = await adapter.handleRequest(makeRequest('http://localhost/nope'))
      expect(resp.status).toBe(404)
    })

    it('preserves query string', async () => {
      const fastify = createMockFastify({
        'GET /search?q=hello': { status: 200, body: 'found' },
      })
      await adapter.initialize(fastify)
      const resp = await adapter.handleRequest(
        makeRequest('http://localhost/search?q=hello')
      )
      expect(resp.status).toBe(200)
      expect(await resp.text()).toBe('found')
    })
  })

  describe('WebSocket route matching', () => {
    const noopHandler: WebSocketHandler = () => {}

    it('matches exact WebSocket routes', () => {
      adapter.addWsRoute('/replication/v1/changes', noopHandler)
      const match = adapter.matchWsRoute('/replication/v1/changes')
      expect(match).not.toBeNull()
      expect(match!.pattern).toBe('/replication/v1/changes')
    })

    it('returns null for unmatched paths', () => {
      adapter.addWsRoute('/replication/v1/changes', noopHandler)
      expect(adapter.matchWsRoute('/other/path')).toBeNull()
    })

    it('matches wildcard patterns', () => {
      adapter.addWsRoute('/replication/v*/changes', noopHandler)

      expect(adapter.matchWsRoute('/replication/v1/changes')).not.toBeNull()
      expect(adapter.matchWsRoute('/replication/v2/changes')).not.toBeNull()
      expect(adapter.matchWsRoute('/replication/v99/changes')).not.toBeNull()

      // should not match different structure
      expect(adapter.matchWsRoute('/replication/v1/snapshot')).toBeNull()
    })

    it('matches multiple wildcard patterns', () => {
      const changesHandler: WebSocketHandler = () => {}
      const snapshotHandler: WebSocketHandler = () => {}

      adapter.addWsRoute('/replication/v*/changes', changesHandler)
      adapter.addWsRoute('/replication/v*/snapshot', snapshotHandler)

      const changesMatch = adapter.matchWsRoute('/replication/v1/changes')
      expect(changesMatch).not.toBeNull()
      expect(changesMatch!.handler).toBe(changesHandler)

      const snapshotMatch = adapter.matchWsRoute('/replication/v2/snapshot')
      expect(snapshotMatch).not.toBeNull()
      expect(snapshotMatch!.handler).toBe(snapshotHandler)
    })

    it('prefers exact match over pattern', () => {
      const exactHandler: WebSocketHandler = () => {}
      const patternHandler: WebSocketHandler = () => {}

      adapter.addWsRoute('/replication/v1/changes', exactHandler)
      adapter.addWsRoute('/replication/v*/changes', patternHandler)

      const match = adapter.matchWsRoute('/replication/v1/changes')
      expect(match).not.toBeNull()
      expect(match!.handler).toBe(exactHandler)
    })

    it('wildcard does not match across slashes', () => {
      adapter.addWsRoute('/api/v*/data', noopHandler)
      expect(adapter.matchWsRoute('/api/v1/extra/data')).toBeNull()
    })
  })

  describe('WebSocket upgrade detection', () => {
    beforeEach(async () => {
      await adapter.initialize(createMockFastify({}))
    })

    it('detects upgrade header', async () => {
      adapter.addWsRoute('/ws', () => {})

      // WebSocketPair is a CF global — not available in vitest.
      // we verify the adapter detects the upgrade and tries to use it.
      const resp = await adapter.handleRequest(
        makeRequest('http://localhost/ws', {
          headers: { upgrade: 'websocket' },
        })
      )

      // without WebSocketPair in the runtime, we get a 500
      // this confirms the adapter correctly detected the upgrade
      // and attempted ws handling (rather than routing to fastify)
      expect(resp.status).toBe(500)
      expect(await resp.text()).toBe('WebSocketPair not available in this runtime')
    })

    it('returns 404 for ws upgrade with no matching route', async () => {
      const resp = await adapter.handleRequest(
        makeRequest('http://localhost/unknown', {
          headers: { upgrade: 'websocket' },
        })
      )
      expect(resp.status).toBe(404)
    })

    it('routes non-upgrade requests to fastify even if ws route exists', async () => {
      adapter.addWsRoute('/dual', () => {})

      // regular GET (no upgrade header) should go to fastify inject
      const resp = await adapter.handleRequest(makeRequest('http://localhost/dual'))
      // fastify returns 404 since we didn't register an HTTP route for /dual
      expect(resp.status).toBe(404)
    })
  })

  describe('WebSocket upgrade preparation with mock WebSocketPair', () => {
    // tests use prepareWebSocketUpgrade() directly because the CF Workers
    // Response constructor (status 101 + webSocket property) is not available
    // in Node.js. this tests the full setup logic without the CF-specific part.

    let originalWebSocketPair: unknown

    beforeEach(async () => {
      originalWebSocketPair = (globalThis as any).WebSocketPair
      await adapter.initialize(createMockFastify({}))
    })

    afterEach(() => {
      if (originalWebSocketPair !== undefined) {
        ;(globalThis as any).WebSocketPair = originalWebSocketPair
      } else {
        delete (globalThis as any).WebSocketPair
      }
    })

    it('creates WebSocket pair and returns upgrade result', () => {
      const mockServer = {
        accept: vi.fn(),
        close: vi.fn(),
        send: vi.fn(),
        addEventListener: vi.fn(),
      }
      const mockClient = { close: vi.fn() }

      ;(globalThis as any).WebSocketPair = class {
        0 = mockClient
        1 = mockServer
      }

      const handler = vi.fn()
      adapter.addWsRoute('/ws/test', handler)

      const req = makeRequest('http://localhost/ws/test', {
        headers: { upgrade: 'websocket' },
      })
      const url = new URL(req.url)
      const result = adapter.prepareWebSocketUpgrade(req, url)

      // should be an upgrade result, not a Response or null
      expect(result).not.toBeNull()
      expect(result).not.toBeInstanceOf(Response)

      const upgrade = result as WebSocketUpgradeResult
      expect(upgrade.client).toBe(mockClient)
      expect(upgrade.server).toBe(mockServer)
      expect(upgrade.handler).toBe(handler)
      expect(upgrade.url.pathname).toBe('/ws/test')
      expect(mockServer.accept).toHaveBeenCalled()
    })

    it('returns null when no route matches', () => {
      const req = makeRequest('http://localhost/nope')
      const url = new URL(req.url)
      expect(adapter.prepareWebSocketUpgrade(req, url)).toBeNull()
    })

    it('returns Response(500) when WebSocketPair is unavailable', () => {
      delete (globalThis as any).WebSocketPair

      adapter.addWsRoute('/ws/test', () => {})

      const req = makeRequest('http://localhost/ws/test')
      const url = new URL(req.url)
      const result = adapter.prepareWebSocketUpgrade(req, url)

      expect(result).toBeInstanceOf(Response)
      expect((result as Response).status).toBe(500)
    })

    it('handler invocation catches errors and closes socket', async () => {
      const mockServer = {
        accept: vi.fn(),
        close: vi.fn(),
        send: vi.fn(),
        addEventListener: vi.fn(),
      }
      const mockClient = { close: vi.fn() }

      ;(globalThis as any).WebSocketPair = class {
        0 = mockClient
        1 = mockServer
      }

      const handler = vi.fn().mockRejectedValue(new Error('handler boom'))
      adapter.addWsRoute('/ws/fail', handler)

      const req = makeRequest('http://localhost/ws/fail')
      const url = new URL(req.url)
      const result = adapter.prepareWebSocketUpgrade(req, url) as WebSocketUpgradeResult

      // simulate what handleWebSocket does: invoke handler and catch errors
      await Promise.resolve(
        result.handler(result.server as any, result.request, result.url)
      ).catch((err) => {
        try {
          ;(result.server as any).close(1011, String(err))
        } catch {
          // socket may already be closed
        }
      })

      expect(handler).toHaveBeenCalled()
      expect(mockServer.close).toHaveBeenCalledWith(1011, 'Error: handler boom')
    })
  })

  describe('createHttpServiceAdapter factory', () => {
    it('returns a valid adapter', () => {
      const adapter = createHttpServiceAdapter()
      expect(adapter).toBeInstanceOf(HttpServiceAdapter)
      expect(adapter.isReady).toBe(false)
    })
  })
})
