import { existsSync, mkdirSync, appendFile, statSync, renameSync } from 'node:fs'
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
const MAX_ENTRIES = 50_000
// trim in batches of 10% to avoid O(n) splice on every single push
const TRIM_BATCH = Math.floor(MAX_ENTRIES * 0.1)
const MAX_FILE_SIZE = 5 * 1024 * 1024
const LEVEL_PRIORITY: Record<string, number> = { error: 0, warn: 1, info: 2, debug: 3 }

export function createLogStore(dataDir: string, writeToDisk = true): LogStore {
  const entries: LogEntry[] = []
  let nextId = 1

  const logsDir = join(dataDir, 'logs')

  if (writeToDisk) {
    mkdirSync(logsDir, { recursive: true })
  }

  // track file sizes to rotate per-source
  const fileSizes: Record<string, number> = {}

  // buffered async disk writes — avoids appendFileSync blocking the event loop
  const writeBuffers: Record<string, string[]> = {}
  const FLUSH_INTERVAL_MS = 2000

  function getLogFile(source: string): string {
    return join(logsDir, `${source}.log`)
  }

  function rotateIfNeeded(source: string) {
    if (!writeToDisk) return
    try {
      const logFile = getLogFile(source)
      if (!existsSync(logFile)) return
      const stat = statSync(logFile)
      fileSizes[source] = stat.size
      if (stat.size > MAX_FILE_SIZE) {
        renameSync(logFile, logFile + '.1')
        fileSizes[source] = 0
      }
    } catch {}
  }

  function flushBuffers() {
    for (const source in writeBuffers) {
      const buf = writeBuffers[source]
      if (buf.length === 0) continue
      const data = buf.join('')
      buf.length = 0
      try {
        const logFile = getLogFile(source)
        appendFile(logFile, data, () => {})
        fileSizes[source] = (fileSizes[source] || 0) + data.length
        if (fileSizes[source] > MAX_FILE_SIZE) {
          rotateIfNeeded(source)
        }
      } catch {}
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
      writeBuffers[source].push(line)
    }
  }

  function query(opts?: {
    source?: string
    level?: string
    since?: number
    limit?: number
  }) {
    let result = entries
    const limit = opts?.limit ?? 1000

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

    if (opts?.source) {
      const source = opts.source
      result = result.filter((e) => e.source === source)
    }

    if (opts?.level) {
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
