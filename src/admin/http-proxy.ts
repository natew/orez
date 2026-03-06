import {
  createServer,
  request as httpRequest,
  type Server,
  type IncomingMessage,
  type ServerResponse,
} from 'node:http'

import type { Socket } from 'node:net'

export interface HttpLogEntry {
  id: number
  ts: number
  method: string
  path: string
  status: number
  duration: number
  reqSize: number
  resSize: number
  reqHeaders: Record<string, string>
  resHeaders: Record<string, string>
}

export interface HttpLogStore {
  push(entry: Omit<HttpLogEntry, 'id'>): void
  query(opts?: { since?: number; path?: string }): {
    entries: HttpLogEntry[]
    cursor: number
  }
  clear(): void
}

const MAX_ENTRIES = 10_000

export function createHttpLogStore(): HttpLogStore {
  const entries: HttpLogEntry[] = []
  let nextId = 1

  function push(entry: Omit<HttpLogEntry, 'id'>) {
    const full: HttpLogEntry = { ...entry, id: nextId++ }
    entries.push(full)
    if (entries.length > MAX_ENTRIES) entries.splice(0, entries.length - MAX_ENTRIES)
  }

  function query(opts?: { since?: number; path?: string }) {
    let result: HttpLogEntry[] = entries
    if (opts?.since) {
      const since = opts.since
      let lo = 0
      let hi = result.length
      while (lo < hi) {
        const mid = (lo + hi) >>> 1
        if (result[mid].id <= since) lo = mid + 1
        else hi = mid
      }
      result = result.slice(lo)
    }
    if (opts?.path) {
      const p = opts.path
      result = result.filter((e) => e.path.includes(p))
    }
    return {
      entries: result,
      cursor: entries.length > 0 ? entries[entries.length - 1].id : 0,
    }
  }

  function clear() {
    entries.length = 0
  }

  return { push, query, clear }
}

function flatHeaders(headers: Record<string, any>): Record<string, string> {
  const out: Record<string, string> = {}
  for (const [k, v] of Object.entries(headers)) {
    out[k] = Array.isArray(v) ? v.join(', ') : String(v ?? '')
  }
  return out
}

export function startHttpProxy(opts: {
  listenPort: number
  targetPort: number
  httpLog: HttpLogStore
}): Promise<Server> {
  const { listenPort, targetPort, httpLog } = opts

  const server = createServer((req: IncomingMessage, res: ServerResponse) => {
    const start = Date.now()
    let reqSize = 0
    const reqChunks: Buffer[] = []

    req.on('data', (chunk: Buffer) => {
      reqSize += chunk.length
      reqChunks.push(chunk)
    })

    req.on('end', () => {
      const proxyReq = httpRequest(
        {
          hostname: '127.0.0.1',
          port: targetPort,
          path: req.url,
          method: req.method,
          headers: req.headers,
        },
        (proxyRes) => {
          let resSize = 0
          proxyRes.on('data', (chunk: Buffer) => {
            resSize += chunk.length
          })
          proxyRes.on('end', () => {
            httpLog.push({
              ts: start,
              method: req.method || 'GET',
              path: req.url || '/',
              status: proxyRes.statusCode || 0,
              duration: Date.now() - start,
              reqSize,
              resSize,
              reqHeaders: flatHeaders(req.headers),
              resHeaders: flatHeaders(proxyRes.headers),
            })
          })
          res.writeHead(proxyRes.statusCode || 200, proxyRes.headers)
          proxyRes.pipe(res)
        }
      )

      proxyReq.on('error', (err) => {
        res.writeHead(502)
        res.end('proxy error: ' + err.message)
      })

      for (const chunk of reqChunks) proxyReq.write(chunk)
      proxyReq.end()
    })
  })

  // websocket upgrade passthrough
  server.on('upgrade', (req: IncomingMessage, socket: Socket, head: Buffer) => {
    const start = Date.now()
    const proxyReq = httpRequest({
      hostname: '127.0.0.1',
      port: targetPort,
      path: req.url,
      method: req.method,
      headers: req.headers,
    })

    proxyReq.on('upgrade', (proxyRes, proxySocket, proxyHead) => {
      httpLog.push({
        ts: start,
        method: 'WS',
        path: req.url || '/',
        status: 101,
        duration: Date.now() - start,
        reqSize: 0,
        resSize: 0,
        reqHeaders: flatHeaders(req.headers),
        resHeaders: flatHeaders(proxyRes.headers),
      })

      // forward the 101 response
      let rawHeaders = 'HTTP/1.1 101 Switching Protocols\r\n'
      for (const [k, v] of Object.entries(proxyRes.headers)) {
        rawHeaders += `${k}: ${Array.isArray(v) ? v.join(', ') : v}\r\n`
      }
      rawHeaders += '\r\n'
      socket.write(rawHeaders)
      if (proxyHead.length) socket.write(proxyHead)

      proxySocket.pipe(socket)
      socket.pipe(proxySocket)
      proxySocket.on('error', () => socket.destroy())
      socket.on('error', () => proxySocket.destroy())
    })

    proxyReq.on('error', () => socket.destroy())
    // disable default socket timeout - websocket connections are long-lived
    proxyReq.setTimeout(0)
    proxyReq.write(head)
    proxyReq.end()
  })

  return new Promise((resolve, reject) => {
    server.listen(listenPort, '127.0.0.1', () => resolve(server))
    server.on('error', reject)
  })
}
