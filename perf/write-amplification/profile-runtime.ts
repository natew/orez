import { trackSqlCursorRowsWritten } from '../../src/do-sql-tracking.js'

export type ProfileMeasurement = {
  phase: string
  route: string
  sql: string
  rowsWritten: number
}

function normalizedSql(sql: string): string {
  return sql
    .replace(/_orez_tx_(?!manifest\b|schema\b)[A-Za-z0-9_-]+/g, '_orez_tx_<id>')
    .replace(/\s+/g, ' ')
    .trim()
}

function targetTable(sql: string): string {
  const text = normalizedSql(sql)
  const patterns = [
    /\bCREATE\s+(?:UNIQUE\s+)?INDEX(?:\s+IF\s+NOT\s+EXISTS)?\s+["`]?[^"`\s]+["`]?\s+ON\s+["`]?([^"`\s(;]+)/i,
    /\bCREATE\s+TRIGGER(?:\s+IF\s+NOT\s+EXISTS)?\s+["`]?[^"`\s]+["`]?[\s\S]*?\bON\s+["`]?([^"`\s(;]+)/i,
    /\b(?:INSERT(?:\s+OR\s+\w+)?\s+INTO|REPLACE\s+INTO|UPDATE|DELETE\s+FROM|ALTER\s+TABLE|DROP\s+TABLE(?:\s+IF\s+EXISTS)?|CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?)\s+["`]?([^"`\s(;]+)/i,
  ]
  for (const pattern of patterns) {
    const match = pattern.exec(text)
    if (match?.[1]) return match[1]
  }
  return '(unattributed)'
}

function summarizeEntries(entries: ProfileMeasurement[]) {
  const aggregate = (keyFor: (entry: ProfileMeasurement) => string, key: string) => {
    const groups = new Map<string, { calls: number; rowsWritten: number }>()
    for (const entry of entries) {
      if (entry.rowsWritten <= 0) continue
      const group = keyFor(entry)
      const current = groups.get(group) ?? { calls: 0, rowsWritten: 0 }
      current.calls++
      current.rowsWritten += entry.rowsWritten
      groups.set(group, current)
    }
    return [...groups]
      .map(([group, value]) => ({ [key]: group, ...value }))
      .sort((a, b) => b.rowsWritten - a.rowsWritten)
      .slice(0, 40)
  }

  return {
    rowsWritten: entries.reduce((sum, entry) => sum + entry.rowsWritten, 0),
    measuredStatements: entries.length,
    topRoutes: aggregate((entry) => entry.route, 'route'),
    topStatements: aggregate((entry) => normalizedSql(entry.sql), 'sql'),
    topTables: aggregate((entry) => targetTable(entry.sql), 'table'),
  }
}

export function installSqlProfiler(sqlStorage: { exec: Function }) {
  let phase = 'idle'
  let route = 'constructor'
  const entries: ProfileMeasurement[] = []
  const rawExec = sqlStorage.exec.bind(sqlStorage)
  sqlStorage.exec = (sql: string, ...params: unknown[]) => {
    const cursor = rawExec(sql, ...params)
    return trackSqlCursorRowsWritten(cursor, (rowsWritten) => {
      entries.push({ phase, route, sql, rowsWritten })
    })
  }

  return {
    setPhase(next: string) {
      phase = next
    },
    setRoute(next: string) {
      route = next
    },
    report() {
      const phases = new Set(entries.map((entry) => entry.phase))
      return Object.fromEntries(
        [...phases].map((entryPhase) => [
          entryPhase,
          summarizeEntries(entries.filter((entry) => entry.phase === entryPhase)),
        ])
      )
    },
  }
}

class LocalMessagePort {
  peer: LocalMessagePort | undefined
  handler: ((event: { data: unknown }) => void) | null = null
  queue: unknown[] = []
  closed = false

  get onmessage() {
    return this.handler
  }

  set onmessage(handler: ((event: { data: unknown }) => void) | null) {
    this.handler = handler
    this.flush()
  }

  postMessage(data: unknown) {
    if (!this.closed) this.peer?.enqueue(data)
  }

  private enqueue(data: unknown) {
    if (this.closed) return
    this.queue.push(data)
    this.flush()
  }

  private flush() {
    if (!this.handler || this.closed) return
    for (const data of this.queue.splice(0)) {
      queueMicrotask(() => this.handler?.({ data }))
    }
  }

  start() {
    this.flush()
  }

  close() {
    this.closed = true
    this.queue.length = 0
  }
}

class LocalMessageChannel {
  port1 = new LocalMessagePort()
  port2 = new LocalMessagePort()

  constructor() {
    this.port1.peer = this.port2
    this.port2.peer = this.port1
  }
}

export function installLocalProfileGlobals() {
  globalThis.MessageChannel ??=
    LocalMessageChannel as unknown as typeof globalThis.MessageChannel
  globalThis.setTimeout = globalThis.setTimeout.bind(globalThis)
}
