import { createServer as createHttpServer, type Server as HttpServer } from 'node:http'
import {
  createServer as createNetServer,
  type Server as NetServer,
  type Socket,
} from 'node:net'

import { afterEach, describe, expect, it } from 'vitest'

import { probeZeroCacheHttp } from './zero-health.js'

const servers = new Set<HttpServer | NetServer>()
const socketsByServer = new Map<HttpServer | NetServer, Set<Socket>>()

async function listen(server: HttpServer | NetServer): Promise<number> {
  servers.add(server)
  const sockets = new Set<Socket>()
  socketsByServer.set(server, sockets)
  server.on('connection', (socket) => {
    sockets.add(socket)
    socket.once('close', () => sockets.delete(socket))
  })
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', () => {
      server.off('error', reject)
      resolve()
    })
  })
  const address = server.address()
  if (!address || typeof address === 'string') {
    throw new Error('server did not bind to a TCP port')
  }
  return address.port
}

async function closeServer(server: HttpServer | NetServer): Promise<void> {
  servers.delete(server)
  const sockets = socketsByServer.get(server)
  socketsByServer.delete(server)
  for (const socket of sockets ?? []) {
    socket.destroy()
  }
  await new Promise<void>((resolve, reject) => {
    server.close((err) => {
      if (err) reject(err)
      else resolve()
    })
  })
}

afterEach(async () => {
  await Promise.all([...servers].map((server) => closeServer(server)))
})

describe('probeZeroCacheHttp', () => {
  it('accepts zero-cache root responses that indicate a live HTTP server', async () => {
    const okPort = await listen(
      createHttpServer((_req, res) => {
        res.end('OK')
      })
    )
    await expect(probeZeroCacheHttp(okPort, 100)).resolves.toEqual({ ok: true })

    const notFoundPort = await listen(
      createHttpServer((_req, res) => {
        res.statusCode = 404
        res.end('not found')
      })
    )
    await expect(probeZeroCacheHttp(notFoundPort, 100)).resolves.toEqual({
      ok: true,
    })
  })

  it('rejects HTTP errors and empty replies from a TCP listener', async () => {
    const errorPort = await listen(
      createHttpServer((_req, res) => {
        res.statusCode = 503
        res.end('unavailable')
      })
    )
    await expect(probeZeroCacheHttp(errorPort, 100)).resolves.toEqual({
      ok: false,
      reason: 'HTTP 503',
    })

    const emptyReplyPort = await listen(
      createNetServer((socket) => {
        socket.end()
      })
    )
    const emptyReply = await probeZeroCacheHttp(emptyReplyPort, 100)
    expect(emptyReply.ok).toBe(false)
  })
})
