import { mkdirSync, appendFile, stat, rename, unlink, readdirSync } from 'node:fs'
import { join } from 'node:path'

export interface LogEntry {
  id: number
  ts: number
  source: string
  level: string
  msg: string
}

export interface LogStore {
  push(source: string, level: string, msg: string): void
  query(opts?: { source?: string; level?: string; since?: number; limit?: number }): {
    entries: LogEntry[]
    cursor: number
  }
  getAll(): LogEntry[]
  clear(): void
}

const ANSI_RE = /\x1b\[[0-9;]*m/g
const MAX_ENTRIES = 20_000
// trim in batches of 10% to avoid O(n) splice on every single push
const TRIM_BATCH = Math.floor(MAX_ENTRIES * 0.1)
const MAX_FILE_SIZE = 2 * 1024 * 1024
const MAX_QUERY_LIMIT = 5000
const LEVEL_PRIORITY: Record<string, number> = { error: 0, warn: 1, info: 2, debug: 3 }
const VALID_SOURCES = new Set(['orez', 'zero', 'pglite', 'proxy', 's3'])
const VALID_LEVELS = new Set(['error', 'warn', 'info', 'debug'])

export function createLogStore(
  dataDir: string,
  writeToDisk = true,
  maxFileSize = MAX_FILE_SIZE
): LogStore {
  const entries: LogEntry[] = []
  let nextId = 1

  const logsDir = join(dataDir, 'logs')

  if (writeToDisk) {
    mkdirSync(logsDir, { recursive: true })
    // clean up old rotated log files on startup
    try {
      for (const f of readdirSync(logsDir)) {
        if (/\.log\.\d+$/.test(f)) {
          unlink(join(logsDir, f), () => {})
        }
      }
    } catch {}
  }

  // track file sizes and rotation state per-source
  const fileSizes: Record<string, number> = {}
  const rotating: Record<string, boolean> = {}

  // buffered async disk writes — avoids appendFileSync blocking the event loop
  const writeBuffers: Record<string, string[]> = {}
  const MAX_BUFFER_SIZE = 10_000
  const FLUSH_INTERVAL_MS = 3000

  function getLogFile(source: string): string {
    return join(logsDir, `${source}.log`)
  }

  function rotateIfNeeded(source: string) {
    if (!writeToDisk || rotating[source]) return
    rotating[source] = true
    const logFile = getLogFile(source)
    stat(logFile, (err, stats) => {
      if (err) {
        rotating[source] = false
        return
      }
      fileSizes[source] = stats.size
      if (stats.size > maxFileSize) {
        // delete old backup first, then rename current
        unlink(logFile + '.1', () => {
          rename(logFile, logFile + '.1', () => {
            fileSizes[source] = 0
            rotating[source] = false
          })
        })
      } else {
        rotating[source] = false
      }
    })
  }

  function flushBuffers() {
    for (const source in writeBuffers) {
      const buf = writeBuffers[source]
      if (buf.length === 0) continue
      const data = buf.join('')
      buf.length = 0
      const logFile = getLogFile(source)
      appendFile(logFile, data, (err) => {
        if (err) return
        fileSizes[source] = (fileSizes[source] || 0) + data.length
        if (fileSizes[source] > maxFileSize) {
          rotateIfNeeded(source)
        }
      })
    }
  }

  if (writeToDisk) {
    const timer = setInterval(flushBuffers, FLUSH_INTERVAL_MS)
    if (timer.unref) timer.unref()
  }

  function push(source: string, level: string, msg: string) {
    const entry: LogEntry = {
      id: nextId++,
      ts: Date.now(),
      source,
      level,
      msg: msg.replace(ANSI_RE, ''),
    }
    entries.push(entry)
    // trim in batches to amortize the O(n) splice cost — instead of shifting
    // 50k elements on every push, we shift once every ~5k pushes
    if (entries.length > MAX_ENTRIES + TRIM_BATCH) {
      entries.splice(0, entries.length - MAX_ENTRIES)
    }
    if (writeToDisk) {
      const ts = new Date(entry.ts).toISOString()
      const line = `[${ts}] [${level}] ${entry.msg}\n`
      if (!writeBuffers[source]) writeBuffers[source] = []
      const buf = writeBuffers[source]
      buf.push(line)
      // cap buffer size to prevent unbounded growth if flushBuffers is delayed
      if (buf.length > MAX_BUFFER_SIZE) {
        buf.splice(0, buf.length - MAX_BUFFER_SIZE)
      }
    }
  }

  function query(opts?: {
    source?: string
    level?: string
    since?: number
    limit?: number
  }) {
    let result = entries
    // clamp limit to prevent oversized responses
    const limit = Math.min(Math.max(opts?.limit ?? 1000, 1), MAX_QUERY_LIMIT)

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

    if (opts?.source && VALID_SOURCES.has(opts.source)) {
      const source = opts.source
      result = result.filter((e) => e.source === source)
    }

    if (opts?.level && VALID_LEVELS.has(opts.level)) {
      const maxPriority = LEVEL_PRIORITY[opts.level] ?? 3
      result = result.filter((e) => (LEVEL_PRIORITY[e.level] ?? 3) <= maxPriority)
    }

    // limit results to prevent UI slowdown
    if (result.length > limit) {
      result = result.slice(-limit)
    }

    return {
      entries: result,
      cursor: entries.length > 0 ? entries[entries.length - 1].id : 0,
    }
  }

  function getAll() {
    return [...entries]
  }

  function clear() {
    entries.length = 0
  }

  return { push, query, getAll, clear }
}
