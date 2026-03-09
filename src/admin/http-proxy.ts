import { createServer, connect, type Socket, type Server } from 'node:net'

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

// raw tcp proxy that avoids bun's broken node:http upgrade handling.
// bun silently drops socket.write() data in http server upgrade events,
// so we do everything at the net level instead.
export function startHttpProxy(opts: {
  listenPort: number
  targetPort: number
  httpLog: HttpLogStore
}): Promise<Server> {
  const { listenPort, targetPort, httpLog } = opts

  const server = createServer((client: Socket) => {
    const start = Date.now()
    const target = connect(targetPort, '127.0.0.1')

    // keep websocket connections alive (zero client uses long-lived ws)
    client.setKeepAlive(true, 30_000)
    client.setTimeout(0)
    target.setKeepAlive(true, 30_000)
    target.setTimeout(0)

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

      target.write(chunk)
      client.pipe(target)
    })

    // intercept first target chunk to extract response info and log
    target.once('data', (chunk: Buffer) => {
      const str = chunk.toString('utf8')
      const firstLine = str.split('\r\n')[0] || ''
      const status = parseInt(firstLine.split(' ')[1]) || 0
      const resHeaders = parseHeaders(str)

      if (!logged) {
        logged = true
        httpLog.push({
          ts: start,
          method: status === 101 ? 'WS' : reqMethod,
          path: reqPath,
          status,
          duration: Date.now() - start,
          reqSize: 0,
          resSize: chunk.length,
          reqHeaders,
          resHeaders,
        })
      }

      client.write(chunk)
      target.pipe(client)
    })

    target.on('error', () => client.destroy())
    client.on('error', () => target.destroy())
    target.on('close', () => client.destroy())
    client.on('close', () => target.destroy())
  })

  return new Promise((resolve, reject) => {
    server.listen(listenPort, '127.0.0.1', () => resolve(server as any))
    server.on('error', reject)
  })
}
