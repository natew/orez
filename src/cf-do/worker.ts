// @ts-nocheck — cloudflare:workers types not available in orez
import { DurableObject } from 'cloudflare:workers'
import { trackedChangeRow } from '../do-sql-tracking.js'
import { DurableWatermarkState } from './watermark.js'

/**
 * zero-do: Durable Object that speaks the Zero sync protocol + raw SQL execution.
 * Replaces zero-cache + pg-proxy + PGlite for development.
 *
 * Two modes:
 *   WS /sync/v49/connect — Zero sync protocol (client-initiated)
 *   POST /exec — raw SQL execution (from DoBackend adapter)
 *   POST /batch — atomic batch execution via ctx.storage.transaction()
 */

interface Env {
  ZERO_DO: DurableObjectNamespace
}
interface SchemaTable {
  primaryKey: string[]
  columns: Record<string, { type: string }>
}
interface ClientSchema {
  tables: Record<string, SchemaTable>
}
interface DesiredQueryPatchOp {
  op: 'put' | 'del' | 'clear'
  hash?: string
  ast?: any
}
interface CrudOp {
  op: 'insert' | 'update' | 'upsert' | 'delete'
  tableName: string
  value?: Record<string, unknown>
  primaryKey?: string[]
}
interface PushMutation {
  type: string
  name: string
  clientID: string
  id: number
  args: unknown[]
}
interface PushBody {
  mutations: PushMutation[]
}
interface SqlTrack {
  tableName: string
  operation: 'INSERT' | 'UPDATE' | 'DELETE'
  returnRows?: boolean
  rowColumns?: string[]
}
interface SqlExecStatement {
  sql: string
  track?: SqlTrack
}
interface SocketAttachment {
  clientID: string
  clientGroupID: string
  userID: string
  cookie: string | null
  desiredTableNames: string[]
}

const SCHEMA_VERSION = 1
const SQL_ERROR_SNIPPET_RADIUS = 1600
const SQL_ERROR_FALLBACK_LIMIT = 4000

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`
}

function sqliteErrorOffset(message: string): number | null {
  const marker = 'offset '
  const start = message.indexOf(marker)
  if (start < 0) return null
  let index = start + marker.length
  let digits = ''
  while (index < message.length) {
    const code = message.charCodeAt(index)
    if (code < 48 || code > 57) break
    digits += message[index]
    index++
  }
  if (!digits) return null
  const offset = Number(digits)
  return Number.isFinite(offset) ? offset : null
}

function sqlErrorSnippet(sql: string, message: string): string {
  const offset = sqliteErrorOffset(message)
  if (offset !== null) {
    const start = Math.max(0, offset - SQL_ERROR_SNIPPET_RADIUS)
    const end = Math.min(sql.length, offset + SQL_ERROR_SNIPPET_RADIUS)
    return `${start > 0 ? '...' : ''}${sql.slice(start, end)}${end < sql.length ? '...' : ''}`
  }
  if (sql.length <= SQL_ERROR_FALLBACK_LIMIT) return sql
  return `${sql.slice(0, SQL_ERROR_FALLBACK_LIMIT)}...`
}

export class ZeroDO extends DurableObject {
  private sql: any
  private watermarks: DurableWatermarkState
  private schemaTables = new Set<string>()
  private socketAttachments = new Map<WebSocket, SocketAttachment>()

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    this.sql = ctx.storage.sql
    this.watermarks = new DurableWatermarkState(this.sql)
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
          'Access-Control-Allow-Headers': '*',
        },
      })
    }
    if (url.pathname === '/sync/v49/connect') return this.handleSyncConnect(request, url)
    if (url.pathname === '/zero/push' && request.method === 'POST')
      return this.handleHttpPush(request)
    if (url.pathname === '/exec' && request.method === 'POST')
      return this.handleExec(request)
    if (url.pathname === '/batch' && request.method === 'POST')
      return this.handleBatch(request)
    return new Response('not found', { status: 404 })
  }

  // ── Zero sync protocol ──────────────────────────────────────────────────

  private handleSyncConnect(request: Request, url: URL): Response {
    if (request.headers.get('upgrade')?.toLowerCase() !== 'websocket') {
      return new Response('expected websocket upgrade', { status: 426 })
    }
    const pair = new WebSocketPair()
    const client = pair[0]
    const server = pair[1]

    const clientID = url.searchParams.get('clientID') || 'anon'
    const clientGroupID = url.searchParams.get('clientGroupID') || 'default'
    const userID = url.searchParams.get('userID') || 'anon'
    const wsid = url.searchParams.get('wsid') || crypto.randomUUID()

    this.ctx.acceptWebSocket(server)
    this.socketAttachments.set(server, {
      clientID,
      clientGroupID,
      userID,
      cookie: null,
      desiredTableNames: [],
    })
    this.sendJSON(server, ['connected', { wsid, timestamp: Date.now() }])

    const secProtocol = request.headers.get('sec-websocket-protocol')
    if (secProtocol) {
      try {
        const decoded = atob(secProtocol)
        const parsed = JSON.parse(decoded)
        const initData = Array.isArray(parsed) ? parsed : [null, parsed]
        const clientSchema = initData[1]?.clientSchema as ClientSchema | undefined
        const patch = (initData[1]?.desiredQueriesPatch || []) as DesiredQueryPatchOp[]
        this.applyDesiredQueries(server, patch, clientSchema)
      } catch {
        /* header too large or invalid */
      }
    }
    return new Response(null, { status: 101, webSocket: client })
  }

  async webSocketMessage(socket: WebSocket, messageData: string | ArrayBuffer) {
    this.watermarks.ensureTables()
    const attachment = this.socketAttachments.get(socket)
    if (!attachment) return
    const message = this.parseMessage(messageData)
    if (!message) return
    const body = message[1] || {}

    switch (message[0]) {
      case 'initConnection':
      case 'changeDesiredQueries':
        this.applyDesiredQueries(
          socket,
          (body.desiredQueriesPatch || []) as DesiredQueryPatchOp[],
          body.clientSchema as ClientSchema | undefined
        )
        break
      case 'push':
        this.handlePush(socket, attachment, message[1] as PushBody)
        break
      case 'ping':
        this.sendJSON(socket, ['pong', {}])
        break
    }
  }

  webSocketClose(socket: WebSocket, _code: number, _reason: string, _wasClean: boolean) {
    this.socketAttachments.delete(socket)
  }

  private applyDesiredQueries(
    socket: WebSocket,
    patch: DesiredQueryPatchOp[],
    clientSchema?: ClientSchema
  ) {
    const attachment = this.socketAttachments.get(socket)
    if (!attachment) return
    if (clientSchema) this.ensureSchemaTables(clientSchema)

    const tableNames = [
      ...new Set([
        ...attachment.desiredTableNames,
        ...this.resolveTablesFromPatch(patch),
      ]),
    ]
    this.socketAttachments.set(socket, { ...attachment, desiredTableNames: tableNames })

    if (tableNames.length > 0) {
      const rowsPatch: any[] = []
      for (const tn of tableNames) {
        if (!this.tableExists(tn)) continue
        for (const row of this.readAllRows(tn))
          rowsPatch.push({ op: 'put', tableName: tn, value: row })
      }
      if (rowsPatch.length > 0) this.sendSyncPoke(socket, attachment, { rowsPatch })
      else this.sendEmptyPoke(socket, attachment)
    }
  }

  private resolveTablesFromPatch(patch: DesiredQueryPatchOp[]): string[] {
    const tables: string[] = []
    for (const op of patch) if (op.ast) this.extractTableFromAST(op.ast, tables)
    return tables
  }

  private extractTableFromAST(ast: any, tables: string[]) {
    if (ast?.table) tables.push(ast.table)
    if (ast?.related)
      for (const rel of ast.related) {
        if (rel?.subquery?.table) tables.push(rel.subquery.table)
        if (rel?.subquery?.related) this.extractTableFromAST(rel.subquery, tables)
      }
  }

  private handlePush(socket: WebSocket, attachment: SocketAttachment, body: PushBody) {
    const mutations = Array.isArray(body?.mutations) ? body.mutations : []
    const before = this.watermark()
    const mutationResults: any[] = []
    const lastMutationIDChanges: Record<string, number> = {}
    for (const m of mutations) {
      if (m.type === 'crud' && m.name === '_zero_crud') this.applyCrudMutation(m)
      mutationResults.push({ id: { clientID: m.clientID, id: m.id }, result: {} })
      lastMutationIDChanges[m.clientID] = m.id
    }
    this.sendJSON(socket, ['pushResponse', { mutations: mutationResults }])
    const after = this.watermark()
    if (after > before) {
      const changes = this.readChangesSince(before)
      const rowsPatch = changes.map((c) => this.syncRowPatchFromChange(c))
      if (rowsPatch.length > 0)
        this.broadcastPoke(attachment.clientGroupID, { lastMutationIDChanges, rowsPatch })
    }
  }

  private async handleHttpPush(request: Request): Promise<Response> {
    try {
      const body = (await request.json()) as any
      const before = this.watermark()
      const mutations = Array.isArray(body?.mutations) ? body.mutations : []
      for (const m of mutations)
        if (m.type === 'crud' && m.name === '_zero_crud') this.applyCrudMutation(m)
      const after = this.watermark()
      if (after > before) {
        const changes = this.readChangesSince(before)
        const rowsPatch = changes.map((c) => this.syncRowPatchFromChange(c))
        if (rowsPatch.length > 0) this.broadcastPoke('default', { rowsPatch })
      }
      return Response.json({ ok: true })
    } catch (err: any) {
      return Response.json({ error: err.message }, { status: 500 })
    }
  }

  // ── SQL execution endpoints ─────────────────────────────────────────────

  private async handleExec(request: Request): Promise<Response> {
    let sql = ''
    try {
      const body = (await request.json()) as {
        sql: string
        params?: unknown[]
        track?: SqlTrack
      }
      sql = body.sql
      const params = Array.isArray(body.params) ? body.params : []
      const result = await this.ctx.storage.transaction(() =>
        this.executeSQL(sql, params, body.track)
      )
      return Response.json(result)
    } catch (err: any) {
      const suffix = sql ? ` while executing: ${sqlErrorSnippet(sql, err.message)}` : ''
      console.error(`[exec-500] ${err.message} :: SQL=${sql.slice(0, 800)}`)
      return Response.json({ error: `${err.message}${suffix}` }, { status: 500 })
    }
  }

  /** Execute multiple statements atomically via ctx.storage.transaction() */
  private async handleBatch(request: Request): Promise<Response> {
    try {
      const { statements } = (await request.json()) as {
        statements: Array<string | SqlExecStatement>
      }
      const allRows = await this.ctx.storage.transaction(() => {
        const results: any[] = []
        for (const statement of statements) {
          const item =
            typeof statement === 'string' ? { sql: statement } : statement
          if (!item?.sql?.trim()) continue
          try {
            results.push(this.executeSQL(item.sql, [], item.track))
          } catch (err: any) {
            throw new Error(
              `${err.message} while executing: ${sqlErrorSnippet(item.sql, err.message)}`
            )
          }
        }
        return results
      })
      return Response.json({ results: allRows })
    } catch (err: any) {
      return Response.json({ error: err.message }, { status: 500 })
    }
  }

  private executeSQL(
    sql: string,
    params: unknown[] = [],
    track?: SqlTrack
  ): { rows: Record<string, unknown>[]; columns: string[]; affectedRows?: number } {
    const cursor = this.sql.exec(sql, ...params)
    const columns = Array.isArray(cursor.columnNames) ? cursor.columnNames : []
    const rows = this.cursorRows(cursor)
    if (!track) return { rows, columns }

    for (const row of rows) {
      const trackedRow = trackedChangeRow(row, track)
      if (track.operation === 'DELETE')
        this.appendTrackedChange(track.tableName, 'DELETE', null, trackedRow)
      else this.appendTrackedChange(track.tableName, track.operation, trackedRow, null)
    }

    return {
      rows: track.returnRows ? rows : [],
      columns: track.returnRows ? columns : [],
      affectedRows: rows.length,
    }
  }

  private cursorRows(cursor: any): Record<string, unknown>[] {
    return cursor.toArray().map((row: any) => {
      const obj: Record<string, unknown> = {}
      for (const k of Object.keys(row)) obj[k] = row[k]
      return obj
    })
  }

  // ── CRUD operations ──────────────────────────────────────────────────────

  private applyCrudMutation(mutation: PushMutation) {
    const arg = mutation.args[0] as { ops?: CrudOp[] } | undefined
    const ops = Array.isArray(arg?.ops) ? arg.ops : []
    for (const crud of ops) {
      if (!crud || !crud.tableName || !this.tableExists(crud.tableName)) continue
      const value = crud.value || {}
      if (crud.op === 'insert' || crud.op === 'upsert')
        this.upsertRow(crud.tableName, value)
      else if (crud.op === 'update')
        this.updateRow(crud.tableName, value, crud.primaryKey || [])
      else if (crud.op === 'delete')
        this.deleteRow(crud.tableName, value, crud.primaryKey || [])
    }
  }

  private upsertRow(tn: string, value: Record<string, unknown>) {
    const cols = Object.keys(value)
    if (!cols.length) return
    const qc = cols.map((c) => `"${c}"`).join(', ')
    const ph = cols.map(() => '?').join(', ')
    const us = cols.map((c) => `"${c}" = ?`).join(', ')
    this.sql.exec(
      `INSERT INTO "${tn}" (${qc}) VALUES (${ph}) ON CONFLICT DO UPDATE SET ${us}`,
      ...cols.map((c) => value[c]),
      ...cols.map((c) => value[c])
    )
    this.appendChange(tn, 'INSERT', value, null)
  }

  private updateRow(tn: string, value: Record<string, unknown>, pk: string[]) {
    const nk = Object.keys(value).filter((c) => !pk.includes(c))
    if (!nk.length) return
    this.sql.exec(
      `UPDATE "${tn}" SET ${nk.map((c) => `"${c}" = ?`).join(', ')} WHERE ${pk.map((c) => `"${c}" = ?`).join(' AND ')}`,
      ...nk.map((c) => value[c]),
      ...pk.map((c) => value[c])
    )
    this.appendChange(tn, 'UPDATE', value, null)
  }

  private deleteRow(tn: string, value: Record<string, unknown>, pk: string[]) {
    this.sql.exec(
      `DELETE FROM "${tn}" WHERE ${pk.map((c) => `"${c}" = ?`).join(' AND ')}`,
      ...pk.map((c) => value[c])
    )
    this.appendChange(tn, 'DELETE', null, value)
  }

  private appendChange(
    tn: string,
    op: 'INSERT' | 'UPDATE' | 'DELETE',
    rowData: Record<string, unknown> | null,
    oldData: Record<string, unknown> | null
  ) {
    this.appendTrackedChange(tn, op, rowData, oldData)
  }

  private appendTrackedChange(
    tableName: string,
    op: 'INSERT' | 'UPDATE' | 'DELETE',
    rowData: Record<string, unknown> | null,
    oldData: Record<string, unknown> | null
  ) {
    this.watermarks.ensureTables()
    const watermark = this.watermarks.next()
    this.sql.exec(
      'INSERT INTO _zero_changes (watermark, table_name, op, row_data, old_data) VALUES (?, ?, ?, ?, ?)',
      watermark,
      tableName,
      op,
      rowData ? JSON.stringify(rowData) : null,
      oldData ? JSON.stringify(oldData) : null
    )
    this.watermarks.mark(watermark)
  }

  private readChangesSince(watermark: number) {
    this.watermarks.ensureTables()
    return this.sql
      .exec(
        'SELECT watermark, table_name, op, row_data, old_data FROM _zero_changes WHERE watermark > ? ORDER BY watermark',
        watermark
      )
      .toArray()
      .map((row: any) => ({
        watermark: Number(row.watermark),
        tableName: String(row.table_name),
        op: String(row.op),
        rowData: row.row_data ? JSON.parse(String(row.row_data)) : null,
        oldData: row.old_data ? JSON.parse(String(row.old_data)) : null,
      }))
  }

  private watermark(): number {
    return this.watermarks.current()
  }

  private ensureSchemaTables(clientSchema: ClientSchema) {
    for (const [name, def] of Object.entries(clientSchema.tables)) {
      if (this.schemaTables.has(name)) continue
      const pk = def.primaryKey.map((c) => `"${c}"`)
      const pkClause = pk.length ? `, PRIMARY KEY (${pk.join(', ')})` : ''
      const colDefs = Object.entries(def.columns).map(([cn, cd]) => {
        const t: Record<string, string> = {
          string: 'TEXT',
          number: 'REAL',
          boolean: 'INTEGER',
          json: 'TEXT',
          bigint: 'TEXT',
        }
        return `"${cn}" ${t[cd.type] || 'TEXT'}`
      })
      this.sql.exec(
        `CREATE TABLE IF NOT EXISTS "${name}" (${colDefs.join(', ')}${pkClause})`
      )
      this.schemaTables.add(name)
    }
  }

  private tableExists(n: string): boolean {
    try {
      return !!this.sql
        .exec("SELECT name FROM sqlite_master WHERE type='table' AND name=?", n)
        .one()
    } catch {
      return false
    }
  }

  private readAllRows(tn: string): Record<string, unknown>[] {
    try {
      return this.sql
        .exec(`SELECT * FROM "${tn}"`)
        .toArray()
        .map((row: any) => {
          const r: Record<string, unknown> = {}
          for (const k of Object.keys(row)) r[k] = row[k]
          return r
        })
    } catch {
      return []
    }
  }

  private sendSyncPoke(
    socket: WebSocket,
    attachment: SocketAttachment,
    part: { rowsPatch?: any[]; lastMutationIDChanges?: Record<string, number> }
  ) {
    const watermark = this.watermark()
    const cookie = String(watermark).padStart(20, '0')
    const pokeID = crypto.randomUUID()
    this.sendJSON(socket, [
      'pokeStart',
      {
        pokeID,
        baseCookie: attachment.cookie,
        schemaVersions: {
          minSupportedVersion: SCHEMA_VERSION,
          maxSupportedVersion: SCHEMA_VERSION,
        },
        timestamp: Date.now(),
      },
    ])
    this.sendJSON(socket, ['pokePart', { pokeID, ...part }])
    this.sendJSON(socket, ['pokeEnd', { pokeID, cookie }])
    this.socketAttachments.set(socket, { ...attachment, cookie })
  }

  private sendEmptyPoke(socket: WebSocket, attachment: SocketAttachment) {
    this.sendSyncPoke(socket, attachment, {})
  }

  private broadcastPoke(
    clientGroupID: string,
    part: { rowsPatch?: any[]; lastMutationIDChanges?: Record<string, number> }
  ) {
    for (const [socket, attachment] of this.socketAttachments) {
      if (attachment.clientGroupID !== clientGroupID) continue
      this.sendSyncPoke(socket, attachment, part)
    }
  }

  private syncRowPatchFromChange(change: any): any {
    if (change.op === 'DELETE')
      return {
        op: 'del',
        tableName: change.tableName,
        id: change.oldData ? { id: change.oldData.id ?? undefined } : {},
      }
    return { op: 'put', tableName: change.tableName, value: change.rowData }
  }

  private sendJSON(socket: WebSocket, msg: unknown) {
    try {
      socket.send(JSON.stringify(msg))
    } catch {}
  }
  private parseMessage(data: string | ArrayBuffer): unknown {
    try {
      return JSON.parse(typeof data === 'string' ? data : new TextDecoder().decode(data))
    } catch {
      return null
    }
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)
    if (url.pathname === '/sync/v49/connect') {
      const ns = url.searchParams.get('ns') || 'default'
      const cg = url.searchParams.get('clientGroupID') || 'default'
      return env.ZERO_DO.get(env.ZERO_DO.idFromName(`${ns}:${cg}`)).fetch(request)
    }
    if (url.pathname === '/zero/push' && request.method === 'POST') {
      const ns = url.searchParams.get('ns') || 'default'
      return env.ZERO_DO.get(env.ZERO_DO.idFromName(`${ns}:default`)).fetch(request)
    }
    if (url.pathname === '/exec' && request.method === 'POST') {
      const db = url.searchParams.get('db') || 'default'
      const ns = url.searchParams.get('ns') || 'default'
      return env.ZERO_DO.get(env.ZERO_DO.idFromName(`${ns}:${db}`)).fetch(request)
    }
    if (url.pathname === '/batch' && request.method === 'POST') {
      const db = url.searchParams.get('db') || 'default'
      const ns = url.searchParams.get('ns') || 'default'
      return env.ZERO_DO.get(env.ZERO_DO.idFromName(`${ns}:${db}`)).fetch(request)
    }
    return new Response('not found', { status: 404 })
  },
}

interface DurableObjectState {
  storage: { sql: any; transaction<T>(fn: () => T | Promise<T>): Promise<T> }
  acceptWebSocket(socket: WebSocket, tags?: string[]): void
  getWebSockets(tag?: string): WebSocket[]
}
interface WebSocketPair {
  0: WebSocket
  1: WebSocket
}
declare const WebSocketPair: { new (): { 0: WebSocket; 1: WebSocket } }
