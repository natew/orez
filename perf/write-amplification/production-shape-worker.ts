// @ts-nocheck -- this file is bundled for workerd, without workers-types
import { DurableObject } from 'cloudflare:workers'

import { ZeroDO } from '../../src/cf-do/worker.js'
import { trackSqlCursorRowsWritten } from '../../src/do-sql-tracking.js'
import {
  doSqliteStorage,
  installDoForbiddenSqliteGuard,
} from '../../src/worker/zero-cache-do-sqlite.js'
import { startZeroCacheEmbedCF } from '../../src/worker/zero-cache-embed-cf.js'

// local workerd omits the browser MessageChannel global that deployed Workers
// expose. Both ports stay in this isolate, so a queued microtask transport has
// the same ordering and copy semantics needed by the postgres wire bridge.
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

  enqueue(data: unknown) {
    if (this.closed) return
    this.queue.push(data)
    this.flush()
  }

  flush() {
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

globalThis.MessageChannel ??= LocalMessageChannel as unknown as typeof MessageChannel
globalThis.setTimeout = globalThis.setTimeout.bind(globalThis)

type Measurement = {
  phase: string
  route: string
  sql: string
  rowsWritten: number
}

type Env = {
  ZERO_SQL_DO: DurableObjectNamespace
  ZERO_CACHE_DO: DurableObjectNamespace
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

function summarize(entries: Measurement[]) {
  const aggregate = (keyFor: (entry: Measurement) => string, includeSql: boolean) => {
    const groups = new Map<string, { calls: number; rowsWritten: number }>()
    for (const entry of entries) {
      if (entry.rowsWritten <= 0) continue
      const key = keyFor(entry)
      const current = groups.get(key) ?? { calls: 0, rowsWritten: 0 }
      current.calls++
      current.rowsWritten += entry.rowsWritten
      groups.set(key, current)
    }
    return [...groups]
      .map(([key, value]) => ({ [includeSql ? 'sql' : 'table']: key, ...value }))
      .sort((a, b) => b.rowsWritten - a.rowsWritten)
      .slice(0, 40)
  }

  return {
    rowsWritten: entries.reduce((sum, entry) => sum + entry.rowsWritten, 0),
    measuredStatements: entries.length,
    topRoutes: aggregate((entry) => entry.route, false).map(
      ({ table: route, ...value }) => ({
        route,
        ...value,
      })
    ),
    topStatements: aggregate((entry) => normalizedSql(entry.sql), true),
    topTables: aggregate((entry) => targetTable(entry.sql), false),
  }
}

export class ProfileZeroSqlDO extends ZeroDO {}

export class ProfileZeroCacheDO extends DurableObject {
  private phase = 'idle'
  private sourceMeasurements: Measurement[] = []
  private cacheMeasurements: Measurement[] = []
  private zeroCache: Awaited<ReturnType<typeof startZeroCacheEmbedCF>> | undefined

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    this.ctx = ctx
    this.env = env
    installDoForbiddenSqliteGuard(ctx.storage.sql)
    const rawExec = ctx.storage.sql.exec.bind(ctx.storage.sql)
    ctx.storage.sql.exec = (sql: string, ...params: unknown[]) => {
      const cursor = rawExec(sql, ...params)
      return trackSqlCursorRowsWritten(cursor, (rowsWritten) => {
        this.cacheMeasurements.push({
          phase: this.phase,
          route: 'do-sqlite',
          sql,
          rowsWritten,
        })
      })
    }
  }

  private sourceFetch(sourceNamespace: string): typeof fetch {
    return async (input, init) => {
      const request = new Request(input, init)
      request.headers.set('x-orez-measure-writes', '1')
      const stub = this.env.ZERO_SQL_DO.get(
        this.env.ZERO_SQL_DO.idFromName(sourceNamespace)
      )
      const response = await stub.fetch(request)
      try {
        const body = await response.clone().json<{
          writeMeasurements?: Array<{ sql: string; rowsWritten: number }>
        }>()
        for (const measurement of body.writeMeasurements ?? []) {
          this.sourceMeasurements.push({
            phase: this.phase,
            route: new URL(request.url).pathname,
            ...measurement,
          })
        }
      } catch {}
      return response
    }
  }

  private report() {
    const phases = new Set([
      ...this.sourceMeasurements.map((entry) => entry.phase),
      ...this.cacheMeasurements.map((entry) => entry.phase),
    ])
    return Object.fromEntries(
      [...phases].map((phase) => [
        phase,
        {
          source: summarize(
            this.sourceMeasurements.filter((entry) => entry.phase === phase)
          ),
          cache: summarize(
            this.cacheMeasurements.filter((entry) => entry.phase === phase)
          ),
        },
      ])
    )
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)
    if (url.pathname === '/report') return Response.json(this.report())
    if (url.pathname === '/stop') {
      await this.zeroCache?.stop()
      this.zeroCache = undefined
      return Response.json({ ok: true })
    }
    if (url.pathname !== '/boot') return new Response('not found', { status: 404 })

    const sourceNamespace = url.searchParams.get('source') || 'fixture'
    this.phase = url.searchParams.get('phase') || 'boot'
    const readyTimeout = Number(url.searchParams.get('readyTimeout') || 120_000)
    const startedAt = Date.now()
    try {
      this.zeroCache = await startZeroCacheEmbedCF({
        doSqlite: doSqliteStorage(this.ctx),
        backendFetch: this.sourceFetch(sourceNamespace),
        backendNamespace: sourceNamespace,
        appId: 'profile',
        publications: ['profile_publication'],
        readyTimeout,
        env: {
          OREZ_LOG_LEVEL: 'warn',
          ZERO_LOG_LEVEL: 'warn',
          ZERO_LITESTREAM_ENABLED: 'false',
        },
      })
      return Response.json({
        ok: true,
        ready: this.zeroCache.ready,
        durationMs: Date.now() - startedAt,
      })
    } catch (error) {
      this.zeroCache = undefined
      return Response.json(
        {
          ok: false,
          durationMs: Date.now() - startedAt,
          error: String((error as Error)?.message ?? error),
        },
        { status: 504 }
      )
    }
  }
}

function forwardSource(request: Request, env: Env, url: URL): Promise<Response> {
  const namespace = url.searchParams.get('ns') || 'fixture'
  const target = new URL(request.url)
  target.pathname = target.pathname.slice('/source'.length) || '/'
  const forwarded = new Request(target, request)
  const stub = env.ZERO_SQL_DO.get(env.ZERO_SQL_DO.idFromName(namespace))
  return stub.fetch(forwarded)
}

function forwardCache(request: Request, env: Env, url: URL): Promise<Response> {
  const cacheNamespace = url.searchParams.get('cache') || 'profile'
  const target = new URL(request.url)
  target.pathname = target.pathname.slice('/cache'.length) || '/'
  const forwarded = new Request(target, request)
  const stub = env.ZERO_CACHE_DO.get(env.ZERO_CACHE_DO.idFromName(cacheNamespace))
  return stub.fetch(forwarded)
}

export default {
  fetch(request: Request, env: Env): Response | Promise<Response> {
    const url = new URL(request.url)
    if (url.pathname === '/health') return Response.json({ ok: true })
    if (url.pathname.startsWith('/source/')) return forwardSource(request, env, url)
    if (url.pathname.startsWith('/cache/')) return forwardCache(request, env, url)
    return new Response('not found', { status: 404 })
  },
}
