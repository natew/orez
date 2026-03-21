import { describe, it, expect, beforeEach, afterEach } from 'vitest'

import Fastify, { type FastifyShim } from './fastify.js'

describe('Fastify shim', () => {
  let app: FastifyShim
  let origGlobal: unknown

  beforeEach(() => {
    origGlobal = (globalThis as any).__orez_fastify_instance
    app = Fastify()
  })

  afterEach(() => {
    if (origGlobal !== undefined) {
      ;(globalThis as any).__orez_fastify_instance = origGlobal
    } else {
      delete (globalThis as any).__orez_fastify_instance
    }
  })

  describe('constructor', () => {
    it('creates an instance', () => {
      expect(app).toBeDefined()
      expect(app.server).toBeDefined()
    })

    it('registers itself on globalThis', () => {
      expect((globalThis as any).__orez_fastify_instance).toBe(app)
    })
  })

  describe('route registration', () => {
    it('registers GET routes', async () => {
      app.get('/', (_req, reply) => reply.send('ok'))
      const result = await app.inject({ method: 'GET', url: '/' })
      expect(result.statusCode).toBe(200)
      expect(result.body).toBe('ok')
    })

    it('registers POST routes', async () => {
      app.post('/data', (_req, reply) => reply.send('created'))
      const result = await app.inject({ method: 'POST', url: '/data' })
      expect(result.statusCode).toBe(200)
      expect(result.body).toBe('created')
    })

    it('registers PUT routes', async () => {
      app.put('/item', (_req, reply) => {
        reply.code(200).send('updated')
      })
      const result = await app.inject({ method: 'PUT', url: '/item' })
      expect(result.statusCode).toBe(200)
      expect(result.body).toBe('updated')
    })

    it('registers DELETE routes', async () => {
      app.delete('/item', (_req, reply) => {
        reply.code(204).send('')
      })
      const result = await app.inject({ method: 'DELETE', url: '/item' })
      expect(result.statusCode).toBe(204)
    })
  })

  describe('inject()', () => {
    it('returns 404 for unregistered routes', async () => {
      const result = await app.inject({ method: 'GET', url: '/nope' })
      expect(result.statusCode).toBe(404)
      expect(result.body).toBe('Not Found')
    })

    it('passes request headers to handler', async () => {
      let capturedHeaders: Record<string, string | undefined> = {}
      app.get('/headers', (req, reply) => {
        capturedHeaders = req.headers
        reply.send('ok')
      })
      await app.inject({
        method: 'GET',
        url: '/headers',
        headers: { 'x-custom': 'test-value' },
      })
      expect(capturedHeaders['x-custom']).toBe('test-value')
    })

    it('passes query parameters', async () => {
      let capturedQuery: Record<string, string> = {}
      app.get('/search', (req, reply) => {
        capturedQuery = req.query || {}
        reply.send('ok')
      })
      await app.inject({ method: 'GET', url: '/search?q=hello&page=2' })
      expect(capturedQuery.q).toBe('hello')
      expect(capturedQuery.page).toBe('2')
    })

    it('passes parsed JSON body', async () => {
      let capturedBody: unknown
      app.post('/json', (req, reply) => {
        capturedBody = req.body
        reply.send('ok')
      })
      await app.inject({
        method: 'POST',
        url: '/json',
        payload: '{"name":"test"}',
      })
      expect(capturedBody).toEqual({ name: 'test' })
    })

    it('passes raw string body if not JSON', async () => {
      let capturedBody: unknown
      app.post('/raw', (req, reply) => {
        capturedBody = req.body
        reply.send('ok')
      })
      await app.inject({
        method: 'POST',
        url: '/raw',
        payload: 'not json',
      })
      expect(capturedBody).toBe('not json')
    })

    it('reply.code() sets status code', async () => {
      app.get('/created', (_req, reply) => {
        reply.code(201).send('done')
      })
      const result = await app.inject({ method: 'GET', url: '/created' })
      expect(result.statusCode).toBe(201)
    })

    it('reply.header() sets response headers', async () => {
      app.get('/custom', (_req, reply) => {
        reply.header('X-Custom', 'value').send('ok')
      })
      const result = await app.inject({ method: 'GET', url: '/custom' })
      expect(result.headers['x-custom']).toBe('value')
    })

    it('reply.type() sets content-type', async () => {
      app.get('/typed', (_req, reply) => {
        reply.type('text/html').send('<h1>hi</h1>')
      })
      const result = await app.inject({ method: 'GET', url: '/typed' })
      expect(result.headers['content-type']).toBe('text/html')
    })

    it('auto-serializes object responses as JSON', async () => {
      app.get('/obj', (_req, reply) => {
        reply.send({ foo: 'bar' })
      })
      const result = await app.inject({ method: 'GET', url: '/obj' })
      expect(result.headers['content-type']).toBe('application/json')
      expect(JSON.parse(result.body)).toEqual({ foo: 'bar' })
    })

    it('uses handler return value if reply.send() not called', async () => {
      app.get('/return', () => 'returned')
      const result = await app.inject({ method: 'GET', url: '/return' })
      expect(result.body).toBe('returned')
    })

    it('returns 500 on handler error', async () => {
      app.get('/boom', () => {
        throw new Error('handler error')
      })
      const result = await app.inject({ method: 'GET', url: '/boom' })
      expect(result.statusCode).toBe(500)
    })

    it('handles async handlers', async () => {
      app.get('/async', async (_req, reply) => {
        await new Promise((r) => setTimeout(r, 5))
        reply.send('async ok')
      })
      const result = await app.inject({ method: 'GET', url: '/async' })
      expect(result.statusCode).toBe(200)
      expect(result.body).toBe('async ok')
    })

    it('is case-insensitive on method matching', async () => {
      app.get('/test', (_req, reply) => reply.send('ok'))
      const result = await app.inject({ method: 'get', url: '/test' })
      expect(result.statusCode).toBe(200)
    })
  })

  describe('lifecycle', () => {
    it('listen() resolves to an address string', async () => {
      const addr = await app.listen({ host: '::', port: 4848 })
      expect(typeof addr).toBe('string')
    })

    it('ready() resolves', async () => {
      await expect(app.ready()).resolves.toBeUndefined()
    })

    it('close() resolves', async () => {
      await expect(app.close()).resolves.toBeUndefined()
    })

    it('register() returns this for chaining', () => {
      const result = app.register(() => {})
      expect(result).toBe(app)
    })
  })

  describe('FakeHttpServer', () => {
    it('has address() method', () => {
      const addr = app.server.address()
      expect(addr).toHaveProperty('address')
      expect(addr).toHaveProperty('port')
    })

    it('supports onMessageType for EventEmitter IPC', () => {
      let received: unknown = null
      let receivedHandle: unknown = null

      app.server.onMessageType('handoff', (msg: unknown, handle?: unknown) => {
        received = msg
        receivedHandle = handle
      })

      const payload = { message: { url: '/test' }, head: new Uint8Array(0) }
      const fakeSocket = { accept: () => {} }

      app.server.emit('message', ['handoff', payload], fakeSocket)

      expect(received).toEqual(payload)
      expect(receivedHandle).toBe(fakeSocket)
    })

    it('onMessageType ignores non-matching types', () => {
      let called = false
      app.server.onMessageType('handoff', () => {
        called = true
      })

      app.server.emit('message', ['ready', { ready: true }])
      expect(called).toBe(false)
    })

    it('onMessageType ignores non-array messages', () => {
      let called = false
      app.server.onMessageType('handoff', () => {
        called = true
      })

      app.server.emit('message', 'not an array')
      expect(called).toBe(false)
    })
  })
})
