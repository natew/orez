import { existsSync, mkdirSync, appendFileSync, statSync, renameSync } from 'node:fs'
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
const MAX_FILE_SIZE = 5 * 1024 * 1024
const LEVEL_PRIORITY: Record<string, number> = { error: 0, warn: 1, info: 2, debug: 3 }

export function createLogStore(dataDir: string, writeToDisk = true): LogStore {
  const entries: LogEntry[] = []
  let nextId = 1

  const logsDir = join(dataDir, 'logs')
  const logFile = join(logsDir, 'orez.log')
  const backupFile = join(logsDir, 'orez.log.1')

  if (writeToDisk) {
    mkdirSync(logsDir, { recursive: true })
  }

  function rotateIfNeeded() {
    if (!writeToDisk) return
    try {
      if (!existsSync(logFile)) return
      const stat = statSync(logFile)
      if (stat.size > MAX_FILE_SIZE) {
        renameSync(logFile, backupFile)
      }
    } catch {}
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
    if (entries.length > MAX_ENTRIES) {
      entries.splice(0, entries.length - MAX_ENTRIES)
    }
    if (writeToDisk) {
      try {
        const ts = new Date(entry.ts).toISOString()
        appendFileSync(
          logFile,
          '[' + ts + '] [' + source + '] [' + level + '] ' + entry.msg + '\n'
        )
        rotateIfNeeded()
      } catch {}
    }
  }

  function query(opts?: { source?: string; level?: string; since?: number; limit?: number }) {
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
