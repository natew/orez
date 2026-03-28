import { createServer, connect, type Socket, type Server } from 'node:net'

import type { ZeroLiteConfig } from '../config.js'
import type { LogStore } from './log-store.js'

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
const TRIM_BATCH = Math.floor(MAX_ENTRIES * 0.1)

export function createHttpLogStore(): HttpLogStore {
  const entries: HttpLogEntry[] = []
  let nextId = 1

  function push(entry: Omit<HttpLogEntry, 'id'>) {
    const full: HttpLogEntry = { ...entry, id: nextId++ }
    entries.push(full)
    if (entries.length > MAX_ENTRIES + TRIM_BATCH) {
      entries.splice(0, entries.length - MAX_ENTRIES)
    }
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

function parseHeaders(raw: string): Record<string, string> {
  const out: Record<string, string> = {}
  const lines = raw.split('\r\n')
  for (let i = 1; i < lines.length; i++) {
    if (lines[i] === '') break
    const idx = lines[i].indexOf(': ')
    if (idx > 0) {
      out[lines[i].slice(0, idx).toLowerCase()] = lines[i].slice(idx + 2)
    }
  }
  return out
}

// public API routes served directly by the proxy (read-only, no auth)
// these are available at the sprite's public URL under /__orez/
const CORS =
  'Access-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, OPTIONS\r\nAccess-Control-Allow-Headers: *'

function httpResponse(
  status: number,
  body: string,
  contentType = 'application/json'
): Buffer {
  const headers = `HTTP/1.1 ${status} ${status === 200 ? 'OK' : 'Error'}\r\nContent-Type: ${contentType}\r\nContent-Length: ${Buffer.byteLength(body)}\r\n${CORS}\r\nConnection: close\r\n\r\n`
  return Buffer.from(headers + body)
}

function handleOrezRoute(
  path: string,
  method: string,
  logStore?: LogStore,
  config?: ZeroLiteConfig,
  startTime?: number
): Buffer | null {
  if (method === 'OPTIONS') {
    return httpResponse(200, '')
  }

  if (method !== 'GET') {
    return httpResponse(405, JSON.stringify({ error: 'method not allowed' }))
  }

  const url = new URL(path, 'http://localhost')
  const route = url.pathname.replace(/^\/__orez/, '')

  if (route === '/api/logs' && logStore) {
    const source = url.searchParams.get('source') || undefined
    const level = url.searchParams.get('level') || undefined
    const sinceStr = url.searchParams.get('since')
    const limitStr = url.searchParams.get('limit')
    const since = sinceStr ? Number(sinceStr) : undefined
    const limit = limitStr ? Number(limitStr) : undefined
    return httpResponse(
      200,
      JSON.stringify(logStore.query({ source, level, since, limit }))
    )
  }

  if (route === '/api/status' && config) {
    return httpResponse(
      200,
      JSON.stringify({
        uptime: Math.floor((Date.now() - (startTime || Date.now())) / 1000),
        logLevel: config.logLevel,
        sqliteMode: config.disableWasmSqlite ? 'native' : 'wasm',
      })
    )
  }

  return httpResponse(404, JSON.stringify({ error: 'not found' }))
}

// raw tcp proxy that avoids bun's broken node:http upgrade handling.
// bun silently drops socket.write() data in http server upgrade events,
// so we do everything at the net level instead.
//
// intercepts /__orez/* paths to serve read-only API (logs, status)
// directly without forwarding to zero-cache.
export function startHttpProxy(opts: {
  listenPort: number
  targetPort: number
  httpLog: HttpLogStore
  logStore?: LogStore
  config?: ZeroLiteConfig
  startTime?: number
}): Promise<Server> {
  const { listenPort, targetPort, httpLog, logStore, config, startTime } = opts

  const server = createServer((client: Socket) => {
    const start = Date.now()

    let logged = false
    let reqMethod = ''
    let reqPath = ''
    let reqHeaders: Record<string, string> = {}

    // intercept first client chunk to extract request info
    client.once('data', (chunk: Buffer) => {
      const str = chunk.toString('utf8')
      const firstLine = str.split('\r\n')[0] || ''
      const parts = firstLine.split(' ')
      reqMethod = parts[0] || 'GET'
      reqPath = parts[1] || '/'
      reqHeaders = parseHeaders(str)

      // intercept /__orez/ paths — serve directly, don't forward to zero-cache
      // check char 0 first to skip the startsWith on hot-path sync/ws traffic
      if (
        reqPath.charCodeAt(0) === 47 &&
        reqPath.charCodeAt(1) === 95 &&
        reqPath.startsWith('/__orez/')
      ) {
        const response = handleOrezRoute(reqPath, reqMethod, logStore, config, startTime)
        if (response) {
          client.write(response)
          client.end()
          httpLog.push({
            ts: start,
            method: reqMethod,
            path: reqPath,
            status: 200,
            duration: Date.now() - start,
            reqSize: chunk.length,
            resSize: response.length,
            reqHeaders,
            resHeaders: {},
          })
          return
        }
      }

      // forward to zero-cache
      const target = connect(targetPort, '127.0.0.1')

      target.setKeepAlive(true, 30_000)
      target.setTimeout(0)
      client.setKeepAlive(true, 30_000)
      client.setTimeout(0)

      target.write(chunk)
      client.pipe(target)

      // intercept first target chunk to extract response info and log
      target.once('data', (resChunk: Buffer) => {
        const resStr = resChunk.toString('utf8')
        const resFirstLine = resStr.split('\r\n')[0] || ''
        const status = parseInt(resFirstLine.split(' ')[1]) || 0
        const resHeaders = parseHeaders(resStr)

        if (!logged) {
          logged = true
          httpLog.push({
            ts: start,
            method: status === 101 ? 'WS' : reqMethod,
            path: reqPath,
            status,
            duration: Date.now() - start,
            reqSize: 0,
            resSize: resChunk.length,
            reqHeaders,
            resHeaders,
          })
        }

        client.write(resChunk)
        target.pipe(client)
      })

      target.on('error', () => client.destroy())
      client.on('error', () => target.destroy())
      target.on('close', () => client.destroy())
      client.on('close', () => target.destroy())
    })
  })

  return new Promise((resolve, reject) => {
    server.listen(listenPort, '127.0.0.1', () => resolve(server as any))
    server.on('error', reject)
  })
}
