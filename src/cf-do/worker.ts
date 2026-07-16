// @ts-nocheck — cloudflare:workers types not available in orez
import { DurableObject } from 'cloudflare:workers'
import {
  createPostCommitEffects,
  type DeferredEffect,
} from 'orez-sync-cf-host/post-commit'
import {
  executeTransactionQueryPlan,
  type CompiledTransactionQueryPlan,
  type TransactionQueryBudget,
  type TransactionQueryFormat,
} from 'orez-sync-cf-host/transaction-query'

import {
  isSqlMutation,
  isSqlRowMutation,
  RollingRowWriteBudget,
  trackSqlCursorRowsWritten,
  trackedChangeRow,
  WriteBudgetExceededError,
} from '../do-sql-tracking.js'
import {
  TransactionalCdc,
  type CapturedRowChange,
  type CdcTableRegistration,
} from './cdc.js'
import {
  appendPendingChange,
  deletePendingChanges,
  ensurePendingChangesTable,
  rollbackPendingChanges,
} from './row-undo.js'
import {
  commitTxJournal,
  recoverTxJournal,
  rollbackTxJournal,
  snapshotSideEffectWriteTables,
  snapshotTxSchema,
  upgradeToTableSnapshot,
} from './tx-journal.js'
import { DurableWatermarkState, type DurableSqlStorage } from './watermark.js'
import type {
  ApplicationSqlTable,
} from './application-sql.js'
import type { SqlStatementMetadata } from 'orez-sync-cf-host'

export { createApplicationSqlClient } from './application-sql.js'
export type {
  ApplicationSqlClient,
  ApplicationSqlDurableObjectNamespace,
  ApplicationSqlQueryCompiler,
  ApplicationSqlRpc,
  ApplicationSqlTable,
  ApplicationSqlTransaction,
  ApplicationSqlTransactionContext,
  ApplicationSqlTransactionWork,
} from './application-sql.js'
export type { SqlStatementMetadata } from 'orez-sync-cf-host'

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
 *   WS /sync/v51/connect — bespoke Zero sync protocol (dev/protocol testing)
 *   POST /exec — raw SQL execution (from DoBackend adapter)
 *   POST /batch — atomic batch execution via ctx.storage.transaction()
 */

interface Env {
  ZERO_DO: DurableObjectNamespace
  OREZ_DO_WRITE_BUDGET_ROWS?: string
  OREZ_DO_WRITE_BUDGET_WINDOW_MS?: string
  OREZ_DO_WRITE_BUDGET_ADMIN_TOKEN?: string
  OREZ_DO_WRITE_BUDGET_DISABLED?: string
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
  physicalTableName?: string
  operation: 'INSERT' | 'UPDATE' | 'DELETE' | 'UPSERT'
  returnRows?: boolean
  rowColumns?: string[]
  transactionID?: string
  /** False records row images for rollback without publishing a change. */
  publish?: boolean
}
interface SqlExecStatement {
  sql: string
  params?: unknown[]
  track?: SqlTrack
  transactionID?: string
  // runtime-conditional DDL: the deploy-time rewriter can't know a target
  // namespace's current shape, so ALTER TABLE ... ADD/DROP COLUMN IF [NOT]
  // EXISTS ships as an unconditional statement plus a skip condition the DO
  // evaluates against pragma_table_info at apply time (mirrors DoBackend's
  // client-side handling for the embedded path).
  skipIfColumnExists?: { table: string; column: string }
  skipIfColumnMissing?: { table: string; column: string }
}
interface SqlWriteMeasurement {
  sql: string
  rowsWritten: number
}

export type ZeroDOQueryCompiler = (
  ast: unknown,
  format: TransactionQueryFormat
) => CompiledTransactionQueryPlan | Promise<CompiledTransactionQueryPlan>

export type ZeroDOTransactionExecutor = {
  exec(
    sql: string,
    params?: readonly unknown[],
    metadata?: SqlStatementMetadata
  ): Promise<void>
  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[]
  ): Promise<Row[]>
  queryAst<Result = unknown>(
    ast: unknown,
    format: TransactionQueryFormat,
    queryName?: string
  ): Promise<Result>
}

export type ZeroDOTransactionContext = {
  defer(effect: DeferredEffect): void
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
const DEFAULT_WRITE_BUDGET_ROWS = 150_000
const DEFAULT_WRITE_BUDGET_WINDOW_MS = 5 * 60 * 1000
const WRITE_BUDGET_TRIPPED_KEY = '_orez_write_budget_tripped_at'
const SCHEMA_PROVISIONING_WAIT_MS = 20_000
const SCHEMA_PROVISIONING_MAX_DELAY_MS = 500
const DEFAULT_SNAPSHOT_PAGE_ROWS = 2_000
const MAX_SNAPSHOT_PAGE_ROWS = 10_000
const TRANSACTION_CONTROL_SQL =
  /^\s*(?:BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE)(?=\s|;|$)/i

function positiveEnvInteger(value: string | undefined, fallback: number): number {
  const parsed = Number(value)
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : fallback
}

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

function assertApplicationTransactionSQL(sql: string): void {
  if (TRANSACTION_CONTROL_SQL.test(sql)) {
    throw new TypeError('transaction SQL is owned by ZeroDO')
  }
}

function applicationSqlTrack(metadata: SqlStatementMetadata | undefined): SqlTrack | undefined {
  if (!metadata) return undefined
  const operations = {
    insert: 'INSERT',
    update: 'UPDATE',
    delete: 'DELETE',
    upsert: 'UPSERT',
  } satisfies Record<SqlStatementMetadata['kind'], SqlTrack['operation']>
  return {
    tableName: metadata.publicTable,
    physicalTableName: metadata.table,
    operation: operations[metadata.kind],
  }
}

export class ZeroDO extends DurableObject {
  private sql: any
  private watermarks: DurableWatermarkState
  private cdc: TransactionalCdc
  private schemaTables = new Set<string>()
  private tableSchemas = new Map<string, SchemaTable>()
  private writeBudget: RollingRowWriteBudget
  private writeBudgetDisabled: boolean
  private writeBudgetAdminToken: string | undefined
  private activeWriteMeasurements: SqlWriteMeasurement[] | null = null
  private pendingChangesSchemaReady = false
  private applicationSqlSessionID: string | null = null

  private recordWriteBudgetRows(rows: number): void {
    const wasTripped = this.writeBudget.status().tripped
    try {
      this.writeBudget.recordBillable(rows)
    } catch (error) {
      if (error instanceof WriteBudgetExceededError && !wasTripped) {
        const status = this.writeBudget.status()
        console.error(
          JSON.stringify({
            event: 'orez_do_write_budget_tripped',
            windowRows: status.windowRows,
            billableRows: status.billableRows,
            logicalRows: status.logicalRows,
            budget: status.budget,
            windowMs: status.windowMs,
            trippedAt: status.trippedAt,
          })
        )
      }
      throw error
    }
  }

  // persisting the sticky trip from inside the throwing request is unreliable:
  // the trip fires during cursor consumption, often inside
  // ctx.storage.transaction(), and the abort rolls back a put made in that
  // scope (prod booted un-tripped this way on 2026-07-11). every 429 response
  // site awaits this instead, which runs after the aborted transaction and
  // re-asserts the flag on each subsequent 429.
  private async writeBudgetErrorResponse(error: unknown): Promise<Response | null> {
    if (!(error instanceof WriteBudgetExceededError)) return null
    const trippedAt = this.writeBudget.status().trippedAt
    await this.ctx.storage.put(WRITE_BUDGET_TRIPPED_KEY, trippedAt ?? Date.now())
    return Response.json(error.toJSON(), { status: 429 })
  }

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    this.sql = ctx.storage.sql
    this.writeBudgetDisabled = /^(?:1|true)$/i.test(
      env.OREZ_DO_WRITE_BUDGET_DISABLED ?? ''
    )
    this.writeBudgetAdminToken = env.OREZ_DO_WRITE_BUDGET_ADMIN_TOKEN
    this.writeBudget = new RollingRowWriteBudget({
      budgetRows: positiveEnvInteger(
        env.OREZ_DO_WRITE_BUDGET_ROWS,
        DEFAULT_WRITE_BUDGET_ROWS
      ),
      windowMs: positiveEnvInteger(
        env.OREZ_DO_WRITE_BUDGET_WINDOW_MS,
        DEFAULT_WRITE_BUDGET_WINDOW_MS
      ),
      now: () => Date.now(),
    })
    if (this.writeBudgetDisabled) {
      console.error(
        JSON.stringify({
          event: 'orez_do_write_budget_disabled',
          warning: 'row write circuit breaker explicitly disabled',
        })
      )
    }
    const rawExec = this.sql.exec.bind(this.sql)
    this.sql.exec = (statement: string, ...params: unknown[]) => {
      const mutation = isSqlMutation(statement)
      if (mutation && !this.writeBudgetDisabled) this.writeBudget.assertOpen()
      const measurement = this.activeWriteMeasurements
        ? { sql: statement, rowsWritten: 0 }
        : null
      if (measurement) this.activeWriteMeasurements!.push(measurement)
      const cursor = rawExec(statement, ...params)
      if (!mutation) return cursor
      return trackSqlCursorRowsWritten(cursor, (rows) => {
        if (measurement) measurement.rowsWritten += rows
        if (!this.writeBudgetDisabled) this.recordWriteBudgetRows(rows)
      })
    }
    this.cdc = new TransactionalCdc(this.sql)
    this.watermarks = new DurableWatermarkState(this.sql)
    ctx.blockConcurrencyWhile(async () => {
      if (!this.writeBudgetDisabled) {
        const trippedAt = await ctx.storage.get<number>(WRITE_BUDGET_TRIPPED_KEY)
        if (trippedAt) this.writeBudget.restoreTrip(trippedAt)
      }
      const recovered = await this.atomically(() => {
        const transactionIDs = recoverTxJournal(this.sql, 'application', (transactionID) => {
          this.rollbackPendingTrackedChanges(transactionID)
        })
        for (const transactionID of transactionIDs) this.deletePendingTrackedChanges(transactionID)
        return transactionIDs
      })
      if (recovered.length) this.invalidateSchemaCaches()
    })
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
    if (url.pathname === '/_orez/write-budget' && request.method === 'GET')
      return Response.json({
        enabled: !this.writeBudgetDisabled,
        ...this.writeBudget.status(),
      })
    if (url.pathname === '/_orez/write-budget/reopen' && request.method === 'POST')
      return this.handleWriteBudgetReopen(request)
    if (
      (url.pathname === '/zero/push' || url.pathname === '/api/zero/push') &&
      request.method === 'POST'
    )
      return this.handleHttpPush(request)
    if (url.pathname === '/exec' && request.method === 'POST')
      return this.handleExec(request)
    if (url.pathname === '/batch' && request.method === 'POST')
      return this.handleBatch(request)
    if (url.pathname === '/snapshot-tx-schema' && request.method === 'POST')
      return this.handleSnapshotTransactionSchema(request)
    if (url.pathname === '/commit-tx' && request.method === 'POST')
      return this.handleCommitTransaction(request)
    if (url.pathname === '/rollback-tx' && request.method === 'POST')
      return this.handleRollbackTransaction(request)
    if (url.pathname === '/recover-txs' && request.method === 'POST')
      return this.handleRecoverTransactions(request)
    if (
      url.pathname === '/changes' &&
      (request.method === 'GET' || request.method === 'POST')
    )
      return this.handleChanges(request, url)
    if (url.pathname === '/snapshot' && request.method === 'GET')
      return this.handleSnapshot(url)
    if (url.pathname === '/notify' && request.method === 'POST')
      return Response.json({ ok: true, cookie: this.cookie() })
    return new Response('not found', { status: 404 })
  }

  private async handleWriteBudgetReopen(request: Request): Promise<Response> {
    const supplied =
      request.headers.get('x-orez-admin-token') ??
      request.headers.get('authorization')?.replace(/^Bearer\s+/i, '')
    if (!this.writeBudgetAdminToken || supplied !== this.writeBudgetAdminToken)
      return Response.json({ error: 'forbidden' }, { status: 403 })
    await this.ctx.storage.delete(WRITE_BUDGET_TRIPPED_KEY)
    const status = this.writeBudget.reopen()
    console.log(
      JSON.stringify({ event: 'orez_do_write_budget_reopened', reopenedAt: Date.now() })
    )
    return Response.json({ ok: true, enabled: !this.writeBudgetDisabled, ...status })
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
      const budgetResponse = await this.writeBudgetErrorResponse(err)
      if (budgetResponse) return budgetResponse
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
    const measurements = this.startWriteMeasurement(request)
    try {
      const body = (await request.json()) as {
        sql: string
        params?: unknown[]
        track?: SqlTrack
        transactionID?: unknown
      }
      sql = body.sql
      const params = Array.isArray(body.params) ? body.params : []
      const transactionID = String(body.transactionID || body.track?.transactionID || '')
      // CDC rows are drained into the durable change log before the storage
      // transaction returns. Keep unrelated DDL/read calls on the fast path:
      // wrapping every /exec adds ~2-5ms and materially slows large schemas.
      const needsAtomicCapture =
        !!body.track ||
        (this.cdc.active && (isSqlRowMutation(sql) || this.cdc.capturesSchemaChange(sql)))
      const result = await this.withSchemaProvisioningWait(() =>
        needsAtomicCapture
          ? this.atomically(() =>
              this.executeSQL(sql, params, body.track, transactionID || undefined)
            )
          : this.executeSQL(sql, params, undefined, transactionID || undefined)
      )
      return Response.json(
        measurements ? { ...result, writeMeasurements: measurements } : result
      )
    } catch (err: any) {
      const budgetResponse = await this.writeBudgetErrorResponse(err)
      if (budgetResponse) return budgetResponse
      const suffix = sql ? ` while executing: ${sqlErrorSnippet(sql, err.message)}` : ''
      console.error(`[exec-500] ${err.message} :: SQL=${sql.slice(0, 800)}`)
      return Response.json({ error: `${err.message}${suffix}` }, { status: 500 })
    } finally {
      if (measurements) this.activeWriteMeasurements = null
    }
  }

  /** Execute multiple statements atomically via ctx.storage.transaction() */
  private async handleBatch(request: Request): Promise<Response> {
    const measurements = this.startWriteMeasurement(request)
    try {
      const { statements, cdcTables } = (await request.json()) as {
        statements: Array<string | SqlExecStatement>
        cdcTables?: CdcTableRegistration[]
      }
      const allRows = await this.withSchemaProvisioningWait(() =>
        this.atomically(() => {
          const results: any[] = []
          if (Array.isArray(cdcTables)) this.cdc.syncTables(cdcTables)
          for (const statement of statements) {
            const item = typeof statement === 'string' ? { sql: statement } : statement
            if (!item?.sql?.trim()) continue
            if (
              item.skipIfColumnExists &&
              this.tableHasColumn(
                item.skipIfColumnExists.table,
                item.skipIfColumnExists.column
              )
            ) {
              continue
            }
            if (
              item.skipIfColumnMissing &&
              !this.tableHasColumn(
                item.skipIfColumnMissing.table,
                item.skipIfColumnMissing.column
              )
            ) {
              continue
            }
            try {
              results.push(
                this.executeSQL(
                  item.sql,
                  Array.isArray(item.params) ? item.params : [],
                  item.track,
                  item.transactionID || item.track?.transactionID
                )
              )
            } catch (err: any) {
              if (err instanceof WriteBudgetExceededError) throw err
              throw new Error(
                `${err.message} while executing: ${sqlErrorSnippet(item.sql, err.message)}`
              )
            }
          }
          return results
        })
      )
      return Response.json({
        results: allRows,
        capturedChanges: allRows.reduce(
          (total, result) => total + Number(result.capturedChanges ?? 0),
          0
        ),
        ...(measurements ? { writeMeasurements: measurements } : null),
      })
    } catch (err: any) {
      const budgetResponse = await this.writeBudgetErrorResponse(err)
      if (budgetResponse) return budgetResponse
      return Response.json({ error: err.message }, { status: 500 })
    } finally {
      if (measurements) this.activeWriteMeasurements = null
    }
  }

  // Fresh project traffic can arrive while the deploy shim is still applying
  // its schema through a separate request to this same DO. Yield on SQLite's
  // undefined-table error so that migration request can commit, then retry the
  // read/batch. The operation that failed did not execute, and a failed batch
  // transaction has rolled back, so replay is safe.
  private async withSchemaProvisioningWait<T>(
    operation: () => T | Promise<T>
  ): Promise<T> {
    const deadline = Date.now() + SCHEMA_PROVISIONING_WAIT_MS
    let delayMs = 25
    for (;;) {
      try {
        return await operation()
      } catch (error) {
        if (!/no such table:/i.test(String((error as Error)?.message ?? error)))
          throw error
        if (Date.now() >= deadline) throw error
        await scheduler.wait(delayMs)
        delayMs = Math.min(SCHEMA_PROVISIONING_MAX_DELAY_MS, delayMs * 2)
      }
    }
  }

  private startWriteMeasurement(request: Request): SqlWriteMeasurement[] | null {
    if (request.headers.get('x-orez-measure-writes') !== '1') return null
    const measurements: SqlWriteMeasurement[] = []
    this.activeWriteMeasurements = measurements
    return measurements
  }

  /**
   * atomic commit point for a DoBackend-emulated pg transaction. promotes the
   * tx's pending tracked changes into _zero_changes (allocating watermarks)
   * and clears its journal (drops snapshots + manifest rows) in ONE storage
   * transaction, so a DO kill can never leave a tx half-committed: either the
   * manifest rows are gone (committed) or recovery rolls the tx back.
   */
  private async handleCommitTransaction(request: Request): Promise<Response> {
    const measurements = this.startWriteMeasurement(request)
    try {
      const body = (await request.json()) as { transactionID?: unknown }
      const transactionID = String(body.transactionID || '')
      if (!transactionID) throw new Error('missing transactionID')
      const count = await this.atomically(() => {
        const committed = this.commitPendingTrackedChanges(transactionID)
        commitTxJournal(this.sql, transactionID)
        return committed
      })
      return Response.json({
        ok: true,
        count,
        ...(measurements ? { writeMeasurements: measurements } : null),
      })
    } catch (err: any) {
      const budgetResponse = await this.writeBudgetErrorResponse(err)
      if (budgetResponse) return budgetResponse
      return Response.json({ error: err.message }, { status: 500 })
    } finally {
      if (measurements) this.activeWriteMeasurements = null
    }
  }

  private async handleSnapshotTransactionSchema(request: Request): Promise<Response> {
    try {
      const body = (await request.json()) as {
        transactionID?: unknown
        owner?: unknown
        affectedTables?: unknown
      }
      const transactionID = String(body.transactionID || '')
      if (!transactionID) throw new Error('missing transactionID')
      const owner = body.owner === undefined ? 'default' : String(body.owner)
      const affectedTables = Array.isArray(body.affectedTables)
        ? body.affectedTables.map(String)
        : []
      await this.atomically(() =>
        snapshotTxSchema(this.sql, transactionID, owner, affectedTables)
      )
      return Response.json({ ok: true })
    } catch (err: any) {
      const budgetResponse = await this.writeBudgetErrorResponse(err)
      if (budgetResponse) return budgetResponse
      return Response.json({ error: err.message }, { status: 500 })
    }
  }

  private async handleRollbackTransaction(request: Request): Promise<Response> {
    const measurements = this.startWriteMeasurement(request)
    try {
      const body = (await request.json()) as { transactionID?: unknown }
      const transactionID = String(body.transactionID || '')
      if (!transactionID) throw new Error('missing transactionID')
      const count = await this.atomically(() => {
        this.rollbackPendingTrackedChanges(transactionID)
        rollbackTxJournal(this.sql, transactionID)
        return this.deletePendingTrackedChanges(transactionID)
      })
      this.invalidateSchemaCaches()
      return Response.json({
        ok: true,
        count,
        ...(measurements ? { writeMeasurements: measurements } : null),
      })
    } catch (err: any) {
      const budgetResponse = await this.writeBudgetErrorResponse(err)
      if (budgetResponse) return budgetResponse
      return Response.json({ error: err.message }, { status: 500 })
    } finally {
      if (measurements) this.activeWriteMeasurements = null
    }
  }

  /**
   * roll back orphaned transactions for a dead process generation. callers
   * (e.g. the zero-cache embed at boot, before opening pg sessions) own the
   * liveness guarantee: every journaled tx for `owner` is dead.
   */
  private async handleRecoverTransactions(request: Request): Promise<Response> {
    try {
      const body = (await request.json().catch(() => ({}))) as { owner?: unknown }
      const owner = body.owner === undefined ? undefined : String(body.owner)
      const transactionIDs = await this.atomically(() => {
        const recovered = recoverTxJournal(this.sql, owner, (txID) => {
          this.rollbackPendingTrackedChanges(txID)
        })
        for (const txID of recovered) this.deletePendingTrackedChanges(txID)
        return recovered
      })
      this.invalidateSchemaCaches()
      return Response.json({ ok: true, transactionIDs })
    } catch (err: any) {
      const budgetResponse = await this.writeBudgetErrorResponse(err)
      if (budgetResponse) return budgetResponse
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
      const changeLimit = Math.trunc(Math.min(limit, 10_000))
      const head = this.watermark()
      const first = this.sql
        .exec('SELECT MIN(watermark) AS watermark FROM _zero_changes')
        .one() as { watermark?: number | null } | null
      const oldest = first?.watermark == null ? null : Number(first.watermark)
      if (watermark < head && (oldest === null || oldest > watermark + 1)) {
        return Response.json(
          { error: 'watermarkTooOld', watermark: head, oldestWatermark: oldest },
          { status: 410 }
        )
      }
      return Response.json({
        watermark: head,
        changes: this.readChangesSince(watermark, changeLimit),
      })
    } catch (err: any) {
      const budgetResponse = await this.writeBudgetErrorResponse(err)
      if (budgetResponse) return budgetResponse
      return Response.json({ error: err.message }, { status: 500 })
    }
  }

  private async handleSnapshot(url?: URL): Promise<Response> {
    try {
      const paged =
        url &&
        ['table', 'cursor', 'limit'].some((parameter) => url.searchParams.has(parameter))
      if (paged) {
        const table = url.searchParams.get('table')
        if (!table)
          return Response.json(
            { error: 'paged snapshot requires a table parameter' },
            { status: 400 }
          )
        const limitValue = url.searchParams.get('limit')
        const limit =
          limitValue === null ? DEFAULT_SNAPSHOT_PAGE_ROWS : Number(limitValue)
        if (
          !Number.isSafeInteger(limit) ||
          limit <= 0 ||
          limit > MAX_SNAPSHOT_PAGE_ROWS
        ) {
          return Response.json(
            {
              error: `snapshot limit must be an integer from 1 to ${MAX_SNAPSHOT_PAGE_ROWS}`,
            },
            { status: 400 }
          )
        }

        return this.atomicallySync(() => {
          this.ensureSchemaMetadataTable()
          const schemaRow = this.sql
            .exec('SELECT schema_json FROM _zero_schema_tables WHERE name = ?', table)
            .one()
          if (!schemaRow?.schema_json)
            return Response.json(
              { error: `snapshot table ${JSON.stringify(table)} is not modeled` },
              { status: 400 }
            )
          const schema = JSON.parse(String(schemaRow.schema_json)) as SchemaTable
          if (
            !Array.isArray(schema.primaryKey) ||
            !schema.primaryKey.length ||
            schema.primaryKey.some((column) => typeof column !== 'string' || !column)
          ) {
            throw new Error(
              `snapshot table ${JSON.stringify(table)} has no valid primary key`
            )
          }
          this.tableSchemas.set(table, schema)

          const cursor = url.searchParams.get('cursor')
          let cursorValues: unknown[] | null = null
          if (cursor !== null) {
            try {
              const decoded = JSON.parse(cursor)
              if (
                !Array.isArray(decoded) ||
                decoded.length !== schema.primaryKey.length ||
                decoded.some(
                  (value) =>
                    !['string', 'number', 'boolean'].includes(typeof value) ||
                    (typeof value === 'number' && !Number.isFinite(value))
                )
              ) {
                return Response.json(
                  { error: 'snapshot cursor does not match the table primary key' },
                  { status: 400 }
                )
              }
              cursorValues = decoded
            } catch {
              return Response.json(
                { error: 'snapshot cursor is invalid' },
                { status: 400 }
              )
            }
          }

          const primaryKey = schema.primaryKey.map(quoteIdent)
          const keyColumns =
            primaryKey.length === 1 ? primaryKey[0] : `(${primaryKey.join(', ')})`
          const keyParams =
            primaryKey.length === 1 ? '?' : `(${primaryKey.map(() => '?').join(', ')})`
          const where = cursorValues ? ` WHERE ${keyColumns} > ${keyParams}` : ''
          // one look-ahead row distinguishes an exact final page from a page
          // with more data without issuing an unbounded count or second read.
          const page = this.sql
            .exec(
              `SELECT * FROM ${quoteIdent(table)}${where} ORDER BY ${primaryKey.join(', ')} LIMIT ?`,
              ...(cursorValues ?? []),
              limit + 1
            )
            .toArray() as Record<string, unknown>[]
          const hasMore = page.length > limit
          const rawRows = hasMore ? page.slice(0, limit) : page
          let nextCursor: string | null = null
          if (hasMore) {
            const last = rawRows[rawRows.length - 1]
            const values = schema.primaryKey.map((column) => last[column])
            if (
              values.some(
                (value) =>
                  !['string', 'number', 'boolean'].includes(typeof value) ||
                  (typeof value === 'number' && !Number.isFinite(value))
              )
            ) {
              throw new Error(
                `snapshot table ${JSON.stringify(table)} returned an invalid primary key`
              )
            }
            nextCursor = JSON.stringify(values)
          }
          return Response.json({
            watermark: this.watermark(),
            rows: rawRows.map((row) => this.normalizeRow(table, row)),
            nextCursor,
          })
        })
      }

      return this.atomicallySync(() => {
        this.ensureSchemaMetadataTable()
        const names = this.sql
          .exec('SELECT name FROM _zero_schema_tables ORDER BY name')
          .toArray()
          .map((row: any) => String(row.name))
        const tables: Record<string, Record<string, unknown>[]> = {}
        for (const name of names) tables[name] = this.readAllRows(name)
        return Response.json({ watermark: this.watermark(), tables })
      })
    } catch (err: any) {
      const budgetResponse = await this.writeBudgetErrorResponse(err)
      if (budgetResponse) return budgetResponse
      return Response.json({ error: err.message }, { status: 500 })
    }
  }

  /**
   * Run work in a storage transaction, re-deriving every in-memory schema cache
   * from SQLite if it aborts.
   *
   * ctx.storage.transaction() rolls the SQLite side back on throw, but the
   * caches are plain fields that keep asserting state SQLite no longer has: a
   * CDC table stays "registered and verified" with no trigger left on disk, so
   * ensureTable short-circuits and every later write to it goes silently
   * uncaptured. The readiness flags are the same class: their CREATE TABLE is
   * rolled back while the flag still says the table exists.
   */
  private async atomically<T>(work: () => T): Promise<T> {
    try {
      return await this.ctx.storage.transaction(work)
    } catch (error) {
      this.invalidateSchemaCaches()
      throw error
    }
  }

  /**
   * execute trusted subclass work in this object's SQLite transaction.
   *
   * the method is protected so the base public fetch surface cannot invoke it.
   * every SQL cursor is consumed before an executor promise is returned, and
   * external effects run only after the storage transaction commits.
   */
  protected async runApplicationTransaction<T>(
    compileQuery: ZeroDOQueryCompiler,
    work: (
      tx: ZeroDOTransactionExecutor,
      context: ZeroDOTransactionContext
    ) => T | Promise<T>,
    queryBudget?: Partial<TransactionQueryBudget>
  ): Promise<T> {
    const effects = createPostCommitEffects()
    const execute = (
      sql: string,
      params: readonly unknown[] = [],
      metadata?: SqlStatementMetadata
    ) => {
      assertApplicationTransactionSQL(sql)
      return this.executeSQL(sql, [...params], applicationSqlTrack(metadata))
    }
    const tx: ZeroDOTransactionExecutor = {
      async exec(sql, params = [], metadata) {
        execute(sql, params, metadata)
      },
      async query<Row extends Record<string, unknown>>(sql, params = []) {
        return execute(sql, params).rows as Row[]
      },
      async queryAst<Result>(
        ast: unknown,
        format: TransactionQueryFormat,
        queryName?: string
      ) {
        const compiled = await compileQuery(ast, format)
        return executeTransactionQueryPlan<Result>(
          compiled,
          (sql, params) => execute(sql, params).rows,
          { queryName, budget: queryBudget }
        )
      },
    }

    const value = await this.withSchemaProvisioningWait(() =>
      this.atomically(async () => {
        effects.beginAttempt()
        return work(tx, { defer: effects.defer })
      })
    )

    await effects.runAfterCommit((error) => {
      console.error(
        JSON.stringify({
          event: 'orez_do_external_effect_error',
          error: error instanceof Error ? error.message : String(error),
        })
      )
    })
    return value
  }

  private assertApplicationSqlSession(sessionID: string): void {
    if (!sessionID || this.applicationSqlSessionID !== sessionID) {
      throw new Error('application SQLite session is not active')
    }
  }

  private assertNoApplicationSqlSession(): void {
    if (this.applicationSqlSessionID) {
      throw new Error('application SQLite session is active')
    }
  }

  private registerApplicationSqlTables(tables: readonly ApplicationSqlTable[]): void {
    for (const table of tables) {
      this.cdc.ensureTable({
        physicalTableName: table.table,
        tableName: table.publicTable,
        ...(table.publish === false ? { publish: false } : null),
      })
    }
  }

  /**
   * Private Durable Object RPC surface for the application SQLite client.
   *
   * The client owns its callback and sends serialized session turns. This
   * avoids re-entering a Durable Object while it waits on a callback. These
   * methods are intentionally absent from fetch().
   */
  async applicationSqlQuery<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params: readonly unknown[] = []
  ): Promise<Row[]> {
    this.assertNoApplicationSqlSession()
    return this.runApplicationTransaction(() => {
      throw new Error('queryAst requires an application SQLite transaction compiler')
    }, (tx) => tx.query<Row>(sql, params))
  }

  async applicationSqlExec(
    sql: string,
    params: readonly unknown[] = [],
    metadata?: SqlStatementMetadata
  ): Promise<void> {
    this.assertNoApplicationSqlSession()
    await this.runApplicationTransaction(
      () => {
        throw new Error('queryAst requires an application SQLite transaction compiler')
      },
      (tx) => tx.exec(sql, params, metadata)
    )
  }

  async applicationSqlRegisterTables(tables: readonly ApplicationSqlTable[]): Promise<void> {
    this.assertNoApplicationSqlSession()
    await this.atomically(() => this.registerApplicationSqlTables(tables))
  }

  async applicationSqlBegin(sessionID: string): Promise<void> {
    this.assertNoApplicationSqlSession()
    if (!sessionID) throw new TypeError('application SQLite session id is required')
    await this.atomically(() => snapshotTxSchema(this.sql, sessionID, 'application'))
    this.applicationSqlSessionID = sessionID
  }

  async applicationSqlSessionQuery<Row extends Record<string, unknown> = Record<string, unknown>>(
    sessionID: string,
    sql: string,
    params: readonly unknown[] = []
  ): Promise<Row[]> {
    this.assertApplicationSqlSession(sessionID)
    return this.atomically(() => this.executeSQL(sql, [...params], undefined, sessionID).rows as Row[])
  }

  async applicationSqlSessionExec(
    sessionID: string,
    sql: string,
    params: readonly unknown[] = [],
    metadata?: SqlStatementMetadata
  ): Promise<void> {
    this.assertApplicationSqlSession(sessionID)
    await this.atomically(() =>
      this.executeSQL(sql, [...params], applicationSqlTrack(metadata), sessionID)
    )
  }

  async applicationSqlSessionQueryPlan<Result = unknown>(
    sessionID: string,
    plan: CompiledTransactionQueryPlan,
    queryName?: string,
    queryBudget?: Partial<TransactionQueryBudget>
  ): Promise<Result> {
    this.assertApplicationSqlSession(sessionID)
    return this.atomically(() =>
      executeTransactionQueryPlan<Result>(
        plan,
        (sql, params) => this.executeSQL(sql, params, undefined, sessionID).rows,
        { queryName, budget: queryBudget }
      )
    )
  }

  async applicationSqlSessionRegisterTables(
    sessionID: string,
    tables: readonly ApplicationSqlTable[]
  ): Promise<void> {
    this.assertApplicationSqlSession(sessionID)
    await this.atomically(() => this.registerApplicationSqlTables(tables))
  }

  async applicationSqlCommit(sessionID: string): Promise<void> {
    this.assertApplicationSqlSession(sessionID)
    try {
      await this.atomically(() => {
        this.commitPendingTrackedChanges(sessionID)
        commitTxJournal(this.sql, sessionID)
      })
    } finally {
      this.applicationSqlSessionID = null
    }
  }

  async applicationSqlRollback(sessionID: string): Promise<void> {
    this.assertApplicationSqlSession(sessionID)
    try {
      await this.atomically(() => {
        this.rollbackPendingTrackedChanges(sessionID)
        rollbackTxJournal(this.sql, sessionID)
        this.deletePendingTrackedChanges(sessionID)
      })
      this.invalidateSchemaCaches()
    } finally {
      this.applicationSqlSessionID = null
    }
  }

  private atomicallySync<T>(work: () => T): T {
    try {
      return this.ctx.storage.transactionSync(work)
    } catch (error) {
      this.invalidateSchemaCaches()
      throw error
    }
  }

  private invalidateSchemaCaches(): void {
    this.watermarks.invalidateCache()
    this.pendingChangesSchemaReady = false
    // Reload is intentionally last because corrupt persisted CDC metadata must
    // throw (fail closed), without preventing the other caches from invalidating.
    this.cdc.reload()
  }

  // sync (storage.transaction-safe) column presence check for the /batch
  // skipIfColumnExists/skipIfColumnMissing conditions. a missing table reads
  // as "no columns", which makes ADD COLUMN skips behave like pg's
  // IF NOT EXISTS on a table the same batch is about to create.
  private tableHasColumn(table: string, column: string): boolean {
    try {
      const cursor = this.sql.exec(
        'SELECT 1 FROM pragma_table_info(?) WHERE name = ? LIMIT 1',
        table,
        column
      )
      return this.cursorRows(cursor).length > 0
    } catch {
      return false
    }
  }

  private executeSQL(
    sql: string,
    params: unknown[] = [],
    track?: SqlTrack,
    transactionID?: string
  ): {
    rows: Record<string, unknown>[]
    columns: string[]
    affectedRows?: number
    capturedChanges?: number
  } {
    let capturesTrackedTable = false
    if (track?.physicalTableName) {
      capturesTrackedTable = this.cdc.ensureTable({
        physicalTableName: track.physicalTableName,
        tableName: track.tableName,
        ...(track.publish === false ? { publish: false } : null),
        ...(track.rowColumns?.length ? { columns: track.rowColumns } : null),
      })
    } else if (track) {
      capturesTrackedTable = this.cdc.capturesTable(track.tableName)
    }

    // SQLite decides whether an INSERT ... ON CONFLICT write inserted or
    // updated. Only the installed trigger sees that result. Falling back to a
    // caller-declared operation would publish a false change shape.
    if (track?.operation === 'UPSERT' && !capturesTrackedTable) {
      throw new Error(`upsert requires CDC registration for ${track.tableName}`)
    }

    // DoBackend already marked this table row-journaled, betting the DO could
    // capture before/after images for it. When it cannot, that marker promises
    // a rollback nothing can perform, so take the table snapshot the journal
    // would otherwise have taken. It has to happen before the DML, while the
    // table still holds its pre-transaction contents.
    const trackedTransactionID = track ? transactionID || track.transactionID : undefined
    let snapshotsOwnStatement = false
    if (track && !capturesTrackedTable && trackedTransactionID) {
      const physicalTableName =
        track.physicalTableName || track.tableName.replace(/^public\./, '')
      if (this.tableExists(physicalTableName)) {
        upgradeToTableSnapshot(this.sql, trackedTransactionID, physicalTableName)
      }
    }
    if (track && trackedTransactionID) {
      const physicalTableName =
        track.physicalTableName || track.tableName.replace(/^public\./, '')
      snapshotsOwnStatement = snapshotSideEffectWriteTables(
        this.sql,
        trackedTransactionID,
        physicalTableName
      )
    }

    const suspendedCdc = this.cdc.beginSchemaChange(sql)
    let cursor: ReturnType<DurableSqlStorage['exec']>
    try {
      cursor = this.sql.exec(sql, ...params)
    } catch (error) {
      // Restore capture against the unchanged schema before propagating a DDL
      // failure. In normal /exec and /batch paths this is also protected by the
      // surrounding storage transaction.
      this.cdc.finishSchemaChange(suspendedCdc)
      throw error
    }
    this.cdc.finishSchemaChange(suspendedCdc)
    const columns = Array.isArray(cursor.columnNames) ? cursor.columnNames : []
    const rows = this.cursorRows(cursor, columns)
    const mutation = isSqlMutation(sql)
    if (mutation) this.writeBudget.recordLogical(rows.length)
    if (mutation && !isSqlRowMutation(sql)) this.cdc.invalidateSchema()
    const captured =
      track || (this.cdc.active && isSqlRowMutation(sql)) ? this.cdc.drain() : []
    for (const change of captured) {
      this.appendCapturedChange(
        change,
        transactionID || track?.transactionID,
        !snapshotsOwnStatement
      )
    }

    // Backward compatibility for callers that have not yet supplied the
    // physical SQLite table identity needed to install a trigger. Once a
    // table is registered, trigger capture is the sole source of truth and
    // includes arbitrary business-trigger side effects.
    //
    // These rows carry no before-image, so they only feed the changefeed. Their
    // rollback is owned by the table snapshot taken above, and `undoable: false`
    // keeps the row-undo pass from trying to restore them from a wire image that
    // cannot round-trip a blob or an int64.
    if (track && !capturesTrackedTable) {
      const physicalTableName =
        track.physicalTableName || track.tableName.replace(/^public\./, '')
      for (const row of rows) {
        const trackedRow = trackedChangeRow(row, track)
        const isDelete = track.operation === 'DELETE'
        this.appendTrackedChange({
          tableName: track.tableName,
          op: isDelete ? 'DELETE' : track.operation,
          rowData: isDelete ? null : trackedRow,
          oldData: isDelete ? trackedRow : null,
          transactionID: transactionID || track.transactionID,
          physicalTableName,
          publish: track.publish !== false,
          undoable: false,
        })
      }
      if (track.publish !== false) this.appendDerivedTrackedChanges(track, rows)
    }

    const publishedCaptured = captured.filter((change) => change.publish !== false).length
    if (!track) return { rows, columns, capturedChanges: publishedCaptured }

    return {
      rows: track.returnRows ? rows : [],
      columns: track.returnRows ? columns : [],
      affectedRows: rows.length,
      capturedChanges:
        publishedCaptured ||
        (capturesTrackedTable || track.publish === false ? 0 : rows.length),
    }
  }

  private cursorRows(cursor: any, columns?: string[]): Record<string, unknown>[] {
    const cols = Array.isArray(columns) && columns.length > 0 ? columns : null
    return cursor.toArray().map((row: any) => {
      const obj: Record<string, unknown> = {}
      if (cols) {
        // include EVERY selected column, even SQL NULLs the DO cursor omits from
        // the row object — pg/drizzle consumers index results positionally, so a
        // dropped null column shifts every later value (e.g. trailing nullable
        // timestamps read back undefined and crash the type decoder).
        for (const k of cols) obj[k] = k in row ? row[k] : null
      } else {
        for (const k of Object.keys(row)) obj[k] = row[k]
      }
      return obj
    })
  }

  private appendDerivedTrackedChanges(track: SqlTrack, rows: Record<string, unknown>[]) {
    if (!rows.length) return
    const table = track.tableName.replace(/^public\./, '')
    if (table !== 'message') return

    const channelIds = new Set<string>()
    const threadIds = new Set<string>()
    for (const row of rows) {
      const channelId = String(row.channelId || '')
      const threadId = String(row.threadId || '')
      if (this.messageRowUpdatesChannelLatestOrder(row) && channelId) {
        channelIds.add(channelId)
      }
      if (this.messageRowUpdatesThreadReplyCount(row) && threadId) {
        threadIds.add(threadId)
      }
    }

    this.appendRowsAsUpdates(
      'public.channel',
      'channel',
      'id',
      channelIds,
      track.transactionID
    )
    this.appendRowsAsUpdates(
      'public.thread',
      'thread',
      'id',
      threadIds,
      track.transactionID
    )
  }

  private appendRowsAsUpdates(
    publicTableName: string,
    sqliteTableName: string,
    keyColumn: string,
    keys: Set<string>,
    transactionID?: string
  ) {
    if (keys.size === 0) return
    const values = [...keys]
    const placeholders = values.map(() => '?').join(', ')
    const rows = this.sql
      .exec(
        `SELECT * FROM ${quoteIdent(sqliteTableName)} WHERE ${quoteIdent(keyColumn)} IN (${placeholders})`,
        ...values
      )
      .toArray()
    // Derived notifications: the rows were written by business triggers and are
    // captured for the changefeed only. They carry no physical table name, so
    // the row-undo pass never sees them, and their real writes are rolled back
    // by whichever journal entry owns the table.
    for (const row of rows) {
      this.appendTrackedChange({
        tableName: publicTableName,
        op: 'UPDATE',
        rowData: row,
        oldData: null,
        transactionID,
        undoable: false,
      })
    }
  }

  private messageRowUpdatesChannelLatestOrder(row: Record<string, unknown>): boolean {
    return (
      row.type !== 'draft' &&
      row.type !== 'hidden' &&
      !row.deleted &&
      !row.isThreadReply &&
      row.order !== null &&
      row.order !== undefined
    )
  }

  private messageRowUpdatesThreadReplyCount(row: Record<string, unknown>): boolean {
    return !!row.threadId && row.type !== 'draft' && !row.deleted && row.isThreadReply
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
    this.writeBudget.recordLogical(1)
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
    this.writeBudget.recordLogical(1)
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
    this.writeBudget.recordLogical(1)
    this.appendChange(tn, 'DELETE', null, existing)
  }

  private appendChange(
    tn: string,
    op: 'INSERT' | 'UPDATE' | 'DELETE',
    rowData: Record<string, unknown> | null,
    oldData: Record<string, unknown> | null
  ) {
    this.appendTrackedChange({ tableName: tn, op, rowData, oldData })
  }

  /** Record a trigger-captured change, before-images and row identity included. */
  private appendCapturedChange(
    change: CapturedRowChange,
    transactionID?: string,
    undoable = true
  ) {
    this.appendTrackedChange({
      tableName: change.tableName,
      op: change.op,
      rowData: change.rowData,
      oldData: change.oldData,
      transactionID,
      physicalTableName: change.physicalTableName,
      publish: change.publish !== false,
      rowJournal: change.rowJournal,
      oldJournal: change.oldJournal,
      newRowid: change.newRowid,
      oldRowid: change.oldRowid,
      undoable,
    })
  }

  private appendTrackedChange(change: {
    tableName: string
    op: 'INSERT' | 'UPDATE' | 'DELETE'
    rowData: Record<string, unknown> | null
    oldData: Record<string, unknown> | null
    transactionID?: string
    physicalTableName?: string
    publish?: boolean
    rowJournal?: Record<string, string> | null
    oldJournal?: Record<string, string> | null
    newRowid?: string | null
    oldRowid?: string | null
    undoable?: boolean
  }) {
    const publish = change.publish !== false
    if (!publish && !change.transactionID) return
    if (change.transactionID) {
      this.ensurePendingTrackedChangesTable()
      appendPendingChange(this.sql, {
        transactionID: change.transactionID,
        physicalTableName: change.physicalTableName,
        tableName: change.tableName,
        publish,
        op: change.op,
        rowData: change.rowData,
        oldData: change.oldData,
        rowJournal: change.rowJournal ?? null,
        oldJournal: change.oldJournal ?? null,
        newRowid: change.newRowid ?? null,
        oldRowid: change.oldRowid ?? null,
        undoable: change.undoable === true,
      })
      return
    }
    this.appendCommittedTrackedChange(
      change.tableName,
      change.op,
      change.rowData,
      change.oldData
    )
  }

  private appendCommittedTrackedChange(
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

  private ensurePendingTrackedChangesTable() {
    if (this.pendingChangesSchemaReady) return
    ensurePendingChangesTable(this.sql)
    this.pendingChangesSchemaReady = true
  }

  private rollbackPendingTrackedChanges(transactionID: string): number {
    this.ensurePendingTrackedChangesTable()
    return rollbackPendingChanges(this.sql, transactionID)
  }

  private commitPendingTrackedChanges(transactionID: string): number {
    this.ensurePendingTrackedChangesTable()
    this.watermarks.ensureTables()
    const rows = this.sql
      .exec(
        `INSERT INTO _zero_changes (table_name, op, row_data, old_data)
         SELECT table_name, op, row_data, old_data
         FROM _zero_pending_changes
         WHERE transaction_id = ? AND publish != 0
         ORDER BY id
         RETURNING watermark`,
        transactionID
      )
      .toArray()
    let watermark = 0
    for (const row of rows) watermark = Math.max(watermark, Number(row.watermark ?? 0))
    if (watermark > 0) {
      this.watermarks.mark(watermark)
    }
    this.deletePendingTrackedChanges(transactionID)
    return rows.length
  }

  private deletePendingTrackedChanges(transactionID: string): number {
    this.ensurePendingTrackedChangesTable()
    return deletePendingChanges(this.sql, transactionID)
  }

  private readChangesSince(watermark: number, limit?: number) {
    this.watermarks.ensureTables()
    const statement =
      'SELECT watermark, table_name, op, row_data, old_data FROM _zero_changes WHERE watermark > ? ORDER BY watermark' +
      (limit === undefined ? '' : ' LIMIT ?')
    const params = limit === undefined ? [watermark] : [watermark, limit]
    return this.sql
      .exec(statement, ...params)
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
    return this.sql
      .exec(`SELECT * FROM ${quoteIdent(tn)}`)
      .toArray()
      .map((row: any) => this.normalizeRow(tn, row))
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
        // timestamp/timestamptz columns are declared `number` in the zero
        // schema but stored as postgres timestamp TEXT (pg-proxy-do-backend
        // `postgresTimestampText`, e.g. "2026-07-11 13:34:46.000+00"). Coercing
        // that text with Number() yields NaN, which JSON serializes as null and
        // silently wipes every timestamp reaching the sync-cf-host snapshot
        // feed. Forward a non-numeric value untouched so the engine's
        // timestamp_text_to_epoch_ms decodes it, matching the /changes feed
        // which forwards the raw text.
        const numeric = Number(value)
        normalized[key] = Number.isFinite(numeric) ? numeric : value
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
    // every endpoint is served by the one singleton ZeroDO (its fetch() does the
    // routing, CORS preflight, and 404s). forward unconditionally rather than
    // re-listing each path here — a second route table only drifts from the DO's.
    const id = env.ZERO_DO.idFromName('singleton')
    return env.ZERO_DO.get(id).fetch(request)
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
