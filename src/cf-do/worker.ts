// @ts-nocheck — cloudflare:workers types not available in orez
import { DurableObject } from 'cloudflare:workers'

import { trackedChangeRow } from '../do-sql-tracking.js'
import { DurableWatermarkState } from './watermark.js'

/**
 * zero-do: Durable Object that exposes raw SQL execution over ctx.storage.sql.
 *
 * The production Cloudflare path runs real zero-cache via
 * src/worker/zero-cache-embed-cf.ts, with DoBackend calling this DO for
 * Postgres-protocol-backed SQL. The WS sync handler here is kept for
 * development/protocol experiments only; it is not the production replacement
 * for zero-cache.
 *
 * Modes:
 *   WS /sync/v49/connect — bespoke Zero sync protocol (dev/protocol testing)
 *   POST /exec — raw SQL execution (from DoBackend adapter)
 *   POST /batch — atomic batch execution via ctx.storage.transaction()
 */

interface Env {
  ZERO_DO: DurableObjectNamespace
}
interface SchemaTable {
  primaryKey: string[]
  columns: Record<string, { type: string; optional?: boolean }>
}
interface ClientSchema {
  tables: Record<string, SchemaTable>
}
interface DesiredQuery {
  hash: string
  tableNames: string[]
}
interface DesiredQueryPatchOp {
  op: 'put' | 'del' | 'clear'
  hash?: string
  name?: string
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
  clientGroupID?: string
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
  params?: unknown[]
  track?: SqlTrack
}
interface SocketAttachment {
  clientID: string
  clientGroupID: string
  userID: string
  cookie: string | null
  initialized: boolean
  desiredTableNames: string[]
  desiredQueries: DesiredQuery[]
}
interface HibernatableWebSocket extends WebSocket {
  serializeAttachment(value: SocketAttachment): void
  deserializeAttachment(): SocketAttachment | undefined
}

const SCHEMA_VERSION = 1
const SQL_ERROR_SNIPPET_RADIUS = 1600
const SQL_ERROR_FALLBACK_LIMIT = 4000

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`
}

function sqliteTypeForSchemaColumn(type: string): string {
  const types: Record<string, string> = {
    string: 'TEXT',
    number: 'REAL',
    boolean: 'INTEGER',
    json: 'TEXT',
    bigint: 'TEXT',
  }
  return types[type] || 'TEXT'
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
  private tableSchemas = new Map<string, SchemaTable>()

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
    if (url.pathname.startsWith('/sync/v') && url.pathname.endsWith('/connect'))
      return this.handleSyncConnect(request, url)
    if (
      (url.pathname === '/zero/push' || url.pathname === '/api/zero/push') &&
      request.method === 'POST'
    )
      return this.handleHttpPush(request)
    if (url.pathname === '/exec' && request.method === 'POST')
      return this.handleExec(request)
    if (url.pathname === '/batch' && request.method === 'POST')
      return this.handleBatch(request)
    if (
      url.pathname === '/changes' &&
      (request.method === 'GET' || request.method === 'POST')
    )
      return this.handleChanges(request, url)
    if (url.pathname === '/notify' && request.method === 'POST')
      return Response.json({ ok: true, cookie: this.cookie() })
    return new Response('not found', { status: 404 })
  }

  // ── Zero sync protocol ──────────────────────────────────────────────────

  private handleSyncConnect(request: Request, url: URL): Response {
    if (request.headers.get('upgrade')?.toLowerCase() !== 'websocket') {
      return new Response('expected websocket upgrade', { status: 426 })
    }
    const pair = new WebSocketPair()
    const client = pair[0]
    const server = pair[1] as HibernatableWebSocket

    const clientID = url.searchParams.get('clientID') || 'anon'
    const clientGroupID = url.searchParams.get('clientGroupID') || 'default'
    const userID = url.searchParams.get('userID') || 'anon'
    const wsid = url.searchParams.get('wsid') || crypto.randomUUID()
    const baseCookie = url.searchParams.get('baseCookie')

    this.ctx.acceptWebSocket(server)
    server.serializeAttachment({
      clientID,
      clientGroupID,
      userID,
      cookie: baseCookie ? baseCookie : null,
      initialized: false,
      desiredTableNames: [],
      desiredQueries: [],
    })
    this.sendJSON(server, ['connected', { wsid, timestamp: Date.now() }])

    const secProtocol = request.headers.get('sec-websocket-protocol')
    if (secProtocol) {
      const initData = decodeInitConnection(secProtocol)
      if (initData) {
        const clientSchema = initData[1]?.clientSchema as ClientSchema | undefined
        const patch = (initData[1]?.desiredQueriesPatch || []) as DesiredQueryPatchOp[]
        this.applyDesiredQueries(server, patch, clientSchema)
      }
    }
    return new Response(null, {
      status: 101,
      headers: secProtocol ? { 'Sec-WebSocket-Protocol': secProtocol } : undefined,
      webSocket: client,
    } as ResponseInit & { webSocket: WebSocket })
  }

  async webSocketMessage(socket: WebSocket, messageData: string | ArrayBuffer) {
    this.watermarks.ensureTables()
    const ws = socket as HibernatableWebSocket
    const attachment = this.readSocketAttachment(ws)
    if (!attachment) return
    const message = this.parseMessage(messageData)
    if (!message) return
    const body = message[1] || {}

    switch (message[0]) {
      case 'initConnection':
      case 'changeDesiredQueries':
        this.applyDesiredQueries(
          ws,
          (body.desiredQueriesPatch || []) as DesiredQueryPatchOp[],
          body.clientSchema as ClientSchema | undefined
        )
        break
      case 'push':
        this.handlePush(ws, attachment, message[1] as PushBody)
        break
      case 'pull':
        this.handlePull(ws, message[1] as any)
        break
      case 'ping':
        this.sendJSON(ws, ['pong', {}])
        break
    }
  }

  webSocketClose(
    _socket: WebSocket,
    _code: number,
    _reason: string,
    _wasClean: boolean
  ) {}

  private applyDesiredQueries(
    socket: HibernatableWebSocket,
    patch: DesiredQueryPatchOp[],
    clientSchema?: ClientSchema
  ) {
    const attachment = this.readSocketAttachment(socket)
    if (!attachment) return
    if (clientSchema) this.ensureSchemaTables(clientSchema)

    let nextAttachment = this.applyDesiredQueryPatch(attachment, patch)
    socket.serializeAttachment(nextAttachment)

    if (!nextAttachment.initialized) {
      nextAttachment = this.sendSyncPoke(
        socket,
        { ...nextAttachment, initialized: true },
        { lastMutationIDChanges: {}, rowsPatch: [] }
      )
    }

    if (patch.length === 0) return

    const rowsPatch = [
      { op: 'clear' as const },
      ...this.rowsPatchForTables(nextAttachment.desiredTableNames),
    ]
    this.sendSyncPoke(socket, nextAttachment, {
      gotQueriesPatch: this.gotQueriesPatch(patch),
      rowsPatch,
    })
  }

  private applyDesiredQueryPatch(
    attachment: SocketAttachment,
    patch: DesiredQueryPatchOp[]
  ): SocketAttachment {
    const desiredQueries = new Map<string, string[]>()
    for (const query of attachment.desiredQueries || [])
      desiredQueries.set(query.hash, query.tableNames)

    for (const op of patch) {
      if (op.op === 'clear') {
        desiredQueries.clear()
      } else if (op.op === 'put' && op.hash) {
        desiredQueries.set(op.hash, this.resolveTablesFromPatch([op]))
      } else if (op.op === 'del' && op.hash) {
        desiredQueries.delete(op.hash)
      }
    }

    const queries = [...desiredQueries.entries()].map(([hash, tableNames]) => ({
      hash,
      tableNames,
    }))
    return {
      ...attachment,
      desiredQueries: queries,
      desiredTableNames: [...new Set(queries.flatMap((query) => query.tableNames))],
    }
  }

  private gotQueriesPatch(patch: DesiredQueryPatchOp[]) {
    const got: Array<{ op: 'put' | 'del'; hash: string } | { op: 'clear' }> = []
    for (const op of patch) {
      if (op.op === 'clear') got.push({ op: 'clear' })
      else if (op.hash) got.push({ op: op.op, hash: op.hash })
    }
    return got
  }

  private rowsPatchForTables(tableNames: string[]): any[] {
    const rowsPatch: any[] = []
    for (const tn of tableNames) {
      if (!this.tableExists(tn)) continue
      for (const row of this.readAllRows(tn))
        rowsPatch.push({ op: 'put', tableName: tn, value: row })
    }
    return rowsPatch
  }

  private resolveTablesFromPatch(patch: DesiredQueryPatchOp[]): string[] {
    const tables: string[] = []
    for (const op of patch) {
      const tableFromName = this.tableNameFromOperationName(op.name)
      if (tableFromName) tables.push(tableFromName)
      if (op.ast) this.extractTableFromAST(op.ast, tables)
    }
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
      const result = this.applyMutation(m)
      mutationResults.push({ id: { clientID: m.clientID, id: m.id }, result })
      lastMutationIDChanges[m.clientID] = m.id
    }
    this.sendJSON(socket, ['pushResponse', { mutations: mutationResults }])
    const after = this.watermark()
    const changes = after > before ? this.readChangesSince(before) : []
    const rowsPatch = changes.map((c) => this.syncRowPatchFromChange(c))
    if (Object.keys(lastMutationIDChanges).length > 0 || rowsPatch.length > 0)
      this.broadcastMutationPoke(attachment, {
        lastMutationIDChanges,
        rowsPatch,
      })
  }

  private async handleHttpPush(request: Request): Promise<Response> {
    try {
      const body = (await request.json()) as any
      const before = this.watermark()
      const mutations = Array.isArray(body?.mutations) ? body.mutations : []
      const mutationResults: any[] = []
      const lastMutationIDChanges: Record<string, number> = {}
      for (const m of mutations) {
        const result = this.applyMutation(m)
        mutationResults.push({ id: { clientID: m.clientID, id: m.id }, result })
        lastMutationIDChanges[m.clientID] = m.id
      }
      const after = this.watermark()
      const changes = after > before ? this.readChangesSince(before) : []
      const rowsPatch = changes.map((c) => this.syncRowPatchFromChange(c))
      if (Object.keys(lastMutationIDChanges).length > 0 || rowsPatch.length > 0)
        this.broadcastPoke(body?.clientGroupID || 'default', {
          lastMutationIDChanges,
          rowsPatch,
        })
      return Response.json({ mutations: mutationResults })
    } catch (err: any) {
      return Response.json({ error: err.message }, { status: 500 })
    }
  }

  private handlePull(socket: HibernatableWebSocket, body: { requestID?: string }) {
    this.sendJSON(socket, [
      'pull',
      {
        requestID: body?.requestID || crypto.randomUUID(),
        cookie: this.cookie(),
        lastMutationIDChanges: {},
        patch: [],
      },
    ])
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
      // Only wrap in ctx.storage.transaction() when the call has change-tracking
      // side effects (executeSQL writes BOTH the user table AND _zero_changes,
      // which must commit together to keep source-tab sync flicker-free). A
      // bare /exec is single-statement and ctx.storage.sql already serializes;
      // the transaction wrap was adding ~2-5ms × every call, which on chat's
      // 27k-stmt boot pushed orez backend startup past chat's 60s wait-for-port.
      const result = body.track
        ? await this.ctx.storage.transaction(() =>
            this.executeSQL(sql, params, body.track)
          )
        : this.executeSQL(sql, params)
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
          const item = typeof statement === 'string' ? { sql: statement } : statement
          if (!item?.sql?.trim()) continue
          try {
            results.push(
              this.executeSQL(
                item.sql,
                Array.isArray(item.params) ? item.params : [],
                item.track
              )
            )
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

  private async handleChanges(request: Request, url: URL): Promise<Response> {
    try {
      let watermark = Number(
        url.searchParams.get('watermark') ?? url.searchParams.get('since') ?? 0
      )
      let limit = Number(url.searchParams.get('limit') ?? 1000)
      if (request.method === 'POST') {
        const body = (await request.json().catch(() => ({}))) as {
          watermark?: unknown
          since?: unknown
          limit?: unknown
        }
        watermark = Number(body.watermark ?? body.since ?? watermark)
        limit = Number(body.limit ?? limit)
      }
      if (!Number.isFinite(watermark) || watermark < 0) watermark = 0
      if (!Number.isFinite(limit) || limit <= 0) limit = 1000
      return Response.json({
        watermark: this.watermark(),
        changes: this.readChangesSince(watermark).slice(0, Math.min(limit, 10_000)),
      })
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

  private applyMutation(mutation: PushMutation) {
    if (mutation.type === 'crud' && mutation.name === '_zero_crud') {
      return this.applyCrudMutation(mutation)
    }
    if (mutation.name === '_zero_cleanupResults') return {}
    if (mutation.type === 'custom') return this.applyTableMutation(mutation)
    return {
      error: 'app',
      message: `unsupported mutation ${mutation.type}:${mutation.name}`,
    }
  }

  private applyTableMutation(mutation: PushMutation) {
    const [tableName, action] = this.tableActionFromMutationName(mutation.name)
    if (!tableName || !action)
      return { error: 'app', message: `invalid mutation name ${mutation.name}` }
    if (!this.tableExists(tableName))
      return { error: 'app', message: `unknown table ${tableName}` }
    const value = (mutation.args[0] || {}) as Record<string, unknown>
    const primaryKey = this.primaryKeyForTable(tableName, [])

    if (action === 'insert') this.insertRow(tableName, value, primaryKey)
    else if (action === 'upsert') this.upsertRow(tableName, value, primaryKey)
    else if (action === 'delete') this.deleteRow(tableName, value, primaryKey)
    else this.updateRow(tableName, value, primaryKey)
    return {}
  }

  private tableActionFromMutationName(name: string): [string, string] {
    if (name.includes('|')) return name.split('|', 2) as [string, string]
    return name.split('.', 2) as [string, string]
  }

  private tableNameFromOperationName(name?: string): string | null {
    if (!name) return null
    return name.split(/[.|]/, 1)[0] || null
  }

  private applyCrudMutation(mutation: PushMutation) {
    const arg = mutation.args[0] as { ops?: CrudOp[] } | undefined
    const ops = Array.isArray(arg?.ops) ? arg.ops : []
    for (const crud of ops) {
      if (!crud?.tableName) return { error: 'app', message: 'invalid crud mutation' }
      if (!this.tableExists(crud.tableName))
        return { error: 'app', message: `unknown table ${crud.tableName}` }
      const value = crud.value || {}
      const primaryKey = this.primaryKeyForTable(crud.tableName, crud.primaryKey || [])
      if (crud.op === 'insert') this.insertRow(crud.tableName, value, primaryKey)
      else if (crud.op === 'upsert') this.upsertRow(crud.tableName, value, primaryKey)
      else if (crud.op === 'update') this.updateRow(crud.tableName, value, primaryKey)
      else if (crud.op === 'delete') this.deleteRow(crud.tableName, value, primaryKey)
    }
    return {}
  }

  private insertRow(tn: string, value: Record<string, unknown>, pk: string[]) {
    if (this.readRowByPrimaryKey(tn, value, pk)) return
    const row = this.storageRow(tn, value, true)
    const cols = Object.keys(row)
    if (!cols.length) return
    const qc = cols.map((c) => quoteIdent(c)).join(', ')
    const ph = cols.map(() => '?').join(', ')
    this.sql.exec(
      `INSERT INTO ${quoteIdent(tn)} (${qc}) VALUES (${ph})`,
      ...cols.map((c) => row[c])
    )
    const next = this.readRowByPrimaryKey(tn, value, pk) || this.normalizeRow(tn, row)
    this.appendChange(tn, 'INSERT', next, null)
  }

  private upsertRow(tn: string, value: Record<string, unknown>, pk: string[]) {
    const existing = this.readRowByPrimaryKey(tn, value, pk)
    if (existing) {
      this.updateRow(tn, value, pk)
      return
    }
    this.insertRow(tn, value, pk)
  }

  private updateRow(tn: string, value: Record<string, unknown>, pk: string[]) {
    if (!pk.length) return
    const existing = this.readRowByPrimaryKey(tn, value, pk)
    if (!existing) return
    const nk = Object.keys(value).filter((c) => !pk.includes(c))
    if (!nk.length) return
    const storage = this.storageRow(tn, value, false)
    this.sql.exec(
      `UPDATE ${quoteIdent(tn)} SET ${nk.map((c) => `${quoteIdent(c)} = ?`).join(', ')} WHERE ${this.primaryKeyWhere(pk)}`,
      ...nk.map((c) => storage[c]),
      ...pk.map((c) => this.storageColumnValue(tn, c, value[c]))
    )
    const next = this.readRowByPrimaryKey(tn, value, pk)
    if (next) this.appendChange(tn, 'UPDATE', next, existing)
  }

  private deleteRow(tn: string, value: Record<string, unknown>, pk: string[]) {
    if (!pk.length) return
    const existing = this.readRowByPrimaryKey(tn, value, pk)
    if (!existing) return
    this.sql.exec(
      `DELETE FROM ${quoteIdent(tn)} WHERE ${this.primaryKeyWhere(pk)}`,
      ...pk.map((c) => this.storageColumnValue(tn, c, value[c]))
    )
    this.appendChange(tn, 'DELETE', null, existing)
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
    this.ensureSchemaMetadataTable()
    for (const [name, def] of Object.entries(clientSchema.tables)) {
      this.tableSchemas.set(name, def)
      this.createSchemaTable(name, def)
      this.ensureSchemaColumns(name, def)
      this.sql.exec(
        'INSERT OR REPLACE INTO _zero_schema_tables (name, schema_json) VALUES (?, ?)',
        name,
        JSON.stringify(def)
      )
      this.schemaTables.add(name)
    }
  }

  private createSchemaTable(name: string, def: SchemaTable) {
    const pk = def.primaryKey.map((c) => quoteIdent(c))
    const pkClause = pk.length ? `, PRIMARY KEY (${pk.join(', ')})` : ''
    const colDefs = Object.entries(def.columns).map(
      ([cn, cd]) => `${quoteIdent(cn)} ${sqliteTypeForSchemaColumn(cd.type)}`
    )
    this.sql.exec(
      `CREATE TABLE IF NOT EXISTS ${quoteIdent(name)} (${colDefs.join(', ')}${pkClause})`
    )
  }

  private ensureSchemaColumns(name: string, def: SchemaTable) {
    const existing = this.columnNamesForTable(name)
    for (const [columnName, column] of Object.entries(def.columns)) {
      if (existing.has(columnName)) continue
      this.sql.exec(
        `ALTER TABLE ${quoteIdent(name)} ADD COLUMN ${quoteIdent(columnName)} ${sqliteTypeForSchemaColumn(column.type)}`
      )
    }
  }

  private columnNamesForTable(name: string): Set<string> {
    try {
      return new Set(
        this.sql
          .exec(`PRAGMA table_info(${quoteIdent(name)})`)
          .toArray()
          .map((row: any) => String(row.name))
      )
    } catch {
      return new Set()
    }
  }

  private ensureSchemaMetadataTable() {
    this.sql.exec(
      'CREATE TABLE IF NOT EXISTS _zero_schema_tables (name TEXT PRIMARY KEY, schema_json TEXT NOT NULL)'
    )
  }

  private schemaForTable(tableName: string): SchemaTable | undefined {
    const cached = this.tableSchemas.get(tableName)
    if (cached) return cached
    try {
      this.ensureSchemaMetadataTable()
      const row = this.sql
        .exec('SELECT schema_json FROM _zero_schema_tables WHERE name = ?', tableName)
        .one()
      if (!row?.schema_json) return undefined
      const schema = JSON.parse(String(row.schema_json)) as SchemaTable
      this.tableSchemas.set(tableName, schema)
      return schema
    } catch {
      return undefined
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
        .exec(`SELECT * FROM ${quoteIdent(tn)}`)
        .toArray()
        .map((row: any) => this.normalizeRow(tn, row))
    } catch {
      return []
    }
  }

  private readRowByPrimaryKey(
    tn: string,
    value: Record<string, unknown>,
    pk: string[]
  ): Record<string, unknown> | null {
    if (!pk.length) return null
    try {
      const row = this.sql
        .exec(
          `SELECT * FROM ${quoteIdent(tn)} WHERE ${this.primaryKeyWhere(pk)}`,
          ...pk.map((c) => this.storageColumnValue(tn, c, value[c]))
        )
        .one()
      return row ? this.normalizeRow(tn, row) : null
    } catch {
      return null
    }
  }

  private primaryKeyWhere(pk: string[]): string {
    return pk.map((c) => `${quoteIdent(c)} = ?`).join(' AND ')
  }

  private primaryKeyForTable(tn: string, fallback: string[]): string[] {
    const schema = this.schemaForTable(tn)
    if (schema?.primaryKey?.length) return schema.primaryKey
    return fallback
  }

  private storageRow(
    tn: string,
    value: Record<string, unknown>,
    includeMissingSchemaColumns: boolean
  ): Record<string, unknown> {
    const schema = this.schemaForTable(tn)
    const row: Record<string, unknown> = {}
    if (schema && includeMissingSchemaColumns) {
      for (const column of Object.keys(schema.columns))
        row[column] = this.storageColumnValue(tn, column, value[column] ?? null)
    }
    for (const column of Object.keys(value)) {
      if (value[column] !== undefined)
        row[column] = this.storageColumnValue(tn, column, value[column])
    }
    return row
  }

  private storageColumnValue(tn: string, column: string, value: unknown): unknown {
    if (value === undefined || value === null) return null
    const type = this.schemaForTable(tn)?.columns?.[column]?.type
    if (type === 'boolean') return value ? 1 : 0
    if (type === 'json') return typeof value === 'string' ? value : JSON.stringify(value)
    if (type === 'number') return Number(value)
    if (type === 'bigint') return String(value)
    return value
  }

  private normalizeRow(
    tn: string,
    row: Record<string, unknown>
  ): Record<string, unknown> {
    const schema = this.schemaForTable(tn)
    const normalized: Record<string, unknown> = {}
    for (const key of Object.keys(row)) {
      const type = schema?.columns?.[key]?.type
      const value = row[key]
      if (value === null || value === undefined) {
        normalized[key] = null
      } else if (type === 'boolean') {
        normalized[key] =
          value === true || value === 1 || value === '1' || value === 'true'
      } else if (type === 'number') {
        normalized[key] = Number(value)
      } else if (type === 'json' && typeof value === 'string') {
        try {
          normalized[key] = JSON.parse(value)
        } catch {
          normalized[key] = value
        }
      } else {
        normalized[key] = value
      }
    }
    return normalized
  }

  private sendSyncPoke(
    socket: HibernatableWebSocket,
    attachment: SocketAttachment,
    part: {
      rowsPatch?: any[]
      gotQueriesPatch?: any[]
      lastMutationIDChanges?: Record<string, number>
    }
  ): SocketAttachment {
    const cookie = this.nextCookie()
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
    const nextAttachment = { ...attachment, cookie }
    socket.serializeAttachment(nextAttachment)
    return nextAttachment
  }

  private broadcastPoke(
    clientGroupID: string,
    part: { rowsPatch?: any[]; lastMutationIDChanges?: Record<string, number> }
  ) {
    for (const socket of this.ctx.getWebSockets()) {
      const ws = socket as HibernatableWebSocket
      const attachment = this.readSocketAttachment(ws)
      if (!attachment) continue
      if (attachment.clientGroupID !== clientGroupID) continue
      this.sendSyncPoke(ws, attachment, part)
    }
  }

  private broadcastMutationPoke(
    sourceAttachment: SocketAttachment,
    part: { rowsPatch?: any[]; lastMutationIDChanges?: Record<string, number> }
  ) {
    const rowsPatch = part.rowsPatch || []
    const changedTables = new Set(
      rowsPatch
        .map((op) => op?.tableName)
        .filter((tableName): tableName is string => !!tableName)
    )
    const hasLastMutationIDChanges =
      Object.keys(part.lastMutationIDChanges || {}).length > 0

    for (const socket of this.ctx.getWebSockets()) {
      const ws = socket as HibernatableWebSocket
      const attachment = this.readSocketAttachment(ws)
      if (!attachment) continue
      if (attachment.userID !== sourceAttachment.userID) continue

      const isSourceClientGroup =
        attachment.clientGroupID === sourceAttachment.clientGroupID
      const wantsChangedRows =
        changedTables.size > 0 &&
        attachment.desiredTableNames.some((tableName) => changedTables.has(tableName))

      const nextPart: {
        rowsPatch?: any[]
        lastMutationIDChanges?: Record<string, number>
      } = {}
      if (wantsChangedRows) nextPart.rowsPatch = rowsPatch
      if (isSourceClientGroup && hasLastMutationIDChanges)
        nextPart.lastMutationIDChanges = part.lastMutationIDChanges

      if (!nextPart.rowsPatch && !nextPart.lastMutationIDChanges) continue
      this.sendSyncPoke(ws, attachment, nextPart)
    }
  }

  private syncRowPatchFromChange(change: any): any {
    if (change.op === 'DELETE')
      return {
        op: 'del',
        tableName: change.tableName,
        id: this.primaryKeyValue(change.tableName, change.oldData || {}),
      }
    return {
      op: 'put',
      tableName: change.tableName,
      value: this.normalizeRow(change.tableName, change.rowData || {}),
    }
  }

  private primaryKeyValue(
    tableName: string,
    row: Record<string, unknown>
  ): Record<string, unknown> {
    const pk = this.primaryKeyForTable(tableName, [])
    if (pk.length) return Object.fromEntries(pk.map((column) => [column, row[column]]))
    if ('id' in row) return { id: row.id }
    return row
  }

  private cookie(): string {
    return String(this.watermark()).padStart(20, '0')
  }

  private nextCookie(): string {
    const watermark = this.watermarks.next()
    this.watermarks.mark(watermark)
    return String(watermark).padStart(20, '0')
  }

  private readSocketAttachment(socket: HibernatableWebSocket): SocketAttachment | null {
    const attachment = socket.deserializeAttachment()
    if (!attachment) return null
    return {
      ...attachment,
      initialized: attachment.initialized === true,
      desiredTableNames: attachment.desiredTableNames || [],
      desiredQueries: attachment.desiredQueries || [],
    }
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
    const id = env.ZERO_DO.idFromName('singleton')
    if (url.pathname.startsWith('/sync/v') && url.pathname.endsWith('/connect')) {
      return env.ZERO_DO.get(id).fetch(request)
    }
    if (
      (url.pathname === '/zero/push' || url.pathname === '/api/zero/push') &&
      request.method === 'POST'
    ) {
      return env.ZERO_DO.get(id).fetch(request)
    }
    if (url.pathname === '/exec' && request.method === 'POST') {
      return env.ZERO_DO.get(id).fetch(request)
    }
    if (url.pathname === '/batch' && request.method === 'POST') {
      return env.ZERO_DO.get(id).fetch(request)
    }
    if (
      url.pathname === '/changes' &&
      (request.method === 'GET' || request.method === 'POST')
    ) {
      return env.ZERO_DO.get(id).fetch(request)
    }
    if (url.pathname === '/notify' && request.method === 'POST') {
      return env.ZERO_DO.get(id).fetch(request)
    }
    return new Response('not found', { status: 404 })
  },
}

function decodeInitConnection(
  secProtocol: string
): [string, Record<string, unknown>] | null {
  try {
    const decoded = decodeURIComponent(secProtocol)
    const bytes = Uint8Array.from(atob(decoded), (char) => char.charCodeAt(0))
    const protocols = JSON.parse(new TextDecoder().decode(bytes)) as {
      initConnectionMessage?: unknown
    }
    const message = protocols.initConnectionMessage
    if (Array.isArray(message) && message[0] === 'initConnection') {
      return message as [string, Record<string, unknown>]
    }
    return null
  } catch {
    return null
  }
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
