// @ts-nocheck — cloudflare:workers types not available in orez
import { DurableObject, RpcTarget } from 'cloudflare:workers'
import {
  executeTransactionQueryPlan,
  type CompiledTransactionQueryPlan,
  type TransactionQueryBudget,
} from 'orez-sync-cf-host/transaction-query'
import { trackBillableCursorRows } from 'orez-sync-executor/sql-billing'

import {
  classifySql,
  isSqlMutation,
  isSqlRowMutation,
  RollingRowWriteBudget,
  stripPublicPrefix,
  trackedChangeRow,
  WriteBudgetExceededError,
  type RowWriteBudgetTrip,
} from '../do-sql-tracking.js'
import { applicationSqlPreemptibleValue } from './application-sql.js'
import {
  TransactionalCdc,
  schemaChangeTargets,
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
  beginTxJournal,
  commitTxJournal,
  recoverTxJournal,
  rollbackTxJournal,
  snapshotSideEffectWriteTables,
  snapshotTxSchema,
  upgradeToTableSnapshot,
  ZSYNC_CHANGES_TABLE,
  ZSYNC_LOG_SEGMENTS_TABLE,
} from './tx-journal.js'
import { DurableWatermarkState, type DurableSqlStorage } from './watermark.js'
import {
  namespaceClassFromObjectName,
  WriteAttributionCollector,
  type WriteAttributionFields,
} from './write-attribution.js'

import type {
  ApplicationSqlClient,
  ApplicationSqlClientOptions,
  ApplicationSqlExecResult,
  ApplicationSqlPreemptibleResult,
  ApplicationSqlSessionPriority,
  ApplicationSqlSessionOptions,
  ApplicationSqlTable,
  ApplicationSqlTransactionWork,
} from './application-sql.js'
import type { SqlStatementMetadata, TransactionQueryFormat } from 'orez-sync-executor'

export {
  ApplicationSqlSessionPreemptedError,
  createApplicationSqlClient,
} from './application-sql.js'
export type {
  ApplicationSqlClient,
  ApplicationSqlClientOptions,
  ApplicationSqlDurableObjectNamespace,
  ApplicationSqlExecResult,
  ApplicationSqlPreemptibleResult,
  ApplicationSqlQueryCompiler,
  ApplicationSqlRpc,
  ApplicationSqlSessionPriority,
  ApplicationSqlSessionOptions,
  ApplicationSqlSessionRpc,
  ApplicationSqlTable,
  ApplicationSqlTransaction,
  ApplicationSqlTransactionWork,
} from './application-sql.js'
export type { SqlStatementMetadata } from 'orez-sync-executor'

/**
 * SQLite Durable Object used by Orez Lite.
 *
 * Application code uses the typed ApplicationSql RPC surface. The HTTP SQL and
 * websocket routes remain development/protocol tools; they are not a Postgres
 * compatibility layer and are not used by the production sync host.
 */

interface Env {
  ZERO_DO: DurableObjectNamespace
  CF_VERSION?: { id?: string }
  OREZ_DO_WRITE_BUDGET_ROWS?: string
  OREZ_DO_WRITE_BUDGET_WINDOW_MS?: string
  OREZ_DO_WRITE_BUDGET_ADMIN_TOKEN?: string
  OREZ_DO_WRITE_BUDGET_DISABLED?: string
  OREZ_SQL_TELEMETRY_SAMPLE_RATE?: string
}
interface SchemaTable {
  primaryKey: string[]
  physicalName?: string
  columns: Record<string, { type: string; optional?: boolean; serverName?: string }>
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
  // evaluates against pragma_table_info at apply time (mirrors the transaction client's
  // client-side handling for the embedded path).
  skipIfColumnExists?: { table: string; column: string }
  skipIfColumnMissing?: { table: string; column: string }
  migrateIfColumnType?: {
    table: string
    column: string
    affinity?: 'blob' | 'integer' | 'numeric' | 'real' | 'text'
    declaredType?: string
  }
}
interface SqlWriteMeasurement {
  sql: string
  rowsWritten: number
}
interface SqlTelemetrySample {
  startedAt: number
  admittedAt: number | null
  rowsReturned: number
  rowsChanged: number
  statements: number
  attribution: WriteAttributionCollector
}
type PersistedWriteBudgetTrip = RowWriteBudgetTrip & {
  statement?: SqlWriteMeasurement
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
  ): Promise<ApplicationSqlExecResult>
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
const APPLICATION_SQL_TURN_WAIT_MS = 30_000
const WRITE_GRANT_WAIT_SAMPLE_CAPACITY = 4_096
const DEFAULT_SQL_TELEMETRY_SAMPLE_RATE = 0.01
const DEFAULT_SNAPSHOT_PAGE_ROWS = 2_000
const MAX_SNAPSHOT_PAGE_ROWS = 10_000
const TRANSACTION_CONTROL_SQL =
  /^\s*(?:BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE)(?=\s|;|$)/i

class RecentLatencySamples {
  private readonly samples: number[] = []
  private next = 0
  private observed = 0

  constructor(private readonly capacity: number) {}

  record(value: number): void {
    if (!Number.isFinite(value) || value < 0) return
    this.observed++
    if (this.samples.length < this.capacity) {
      this.samples.push(value)
      return
    }
    this.samples[this.next] = value
    this.next = (this.next + 1) % this.capacity
  }

  status(): {
    observed: number
    sampled: number
    capacity: number
    p50: number | null
    p99: number | null
    max: number | null
  } {
    const sorted = [...this.samples].sort((left, right) => left - right)
    const percentile = (fraction: number): number | null => {
      if (sorted.length === 0) return null
      return sorted[Math.max(0, Math.ceil(sorted.length * fraction) - 1)]!
    }
    return {
      observed: this.observed,
      sampled: sorted.length,
      capacity: this.capacity,
      p50: percentile(0.5),
      p99: percentile(0.99),
      max: sorted.at(-1) ?? null,
    }
  }
}

function positiveEnvInteger(value: string | undefined, fallback: number): number {
  const parsed = Number(value)
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : fallback
}

function probabilityEnv(value: string | undefined, fallback: number): number {
  if (value === undefined) return fallback
  const parsed = Number(value)
  return Number.isFinite(parsed) && parsed >= 0 && parsed <= 1 ? parsed : fallback
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

function normalizeDeclaredSqlType(value: unknown): string {
  return String(value).trim().toLowerCase().replaceAll(/\s+/g, ' ')
}

function declaredSqlTypeAffinity(value: unknown): string {
  const type = normalizeDeclaredSqlType(value)
  if (type.includes('int')) return 'integer'
  if (type.includes('char') || type.includes('clob') || type.includes('text')) {
    return 'text'
  }
  if (!type || type.includes('blob')) return 'blob'
  if (type.includes('real') || type.includes('floa') || type.includes('doub')) {
    return 'real'
  }
  return 'numeric'
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

function applicationSqlTrack(
  metadata: SqlStatementMetadata | undefined
): SqlTrack | undefined {
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

type ApplicationSqlSessionState =
  | 'created'
  | 'waiting'
  | 'active'
  | 'preempted'
  | 'closed'

type ApplicationSqlWaiter = {
  session: ApplicationSqlSessionTarget
  queuedAt: number
  admit: () => void
  reject: (error: unknown) => void
  timer: ReturnType<typeof setTimeout>
}

const APPLICATION_SQL_ACQUIRE = Symbol('applicationSqlAcquire')
const APPLICATION_SQL_QUERY = Symbol('applicationSqlQuery')
const APPLICATION_SQL_QUERY_PREEMPTIBLE = Symbol('applicationSqlQueryPreemptible')
const APPLICATION_SQL_EXEC = Symbol('applicationSqlExec')
const APPLICATION_SQL_QUERY_PLAN = Symbol('applicationSqlQueryPlan')
const APPLICATION_SQL_QUERY_PLAN_PREEMPTIBLE = Symbol(
  'applicationSqlQueryPlanPreemptible'
)
const APPLICATION_SQL_REGISTER_TABLES = Symbol('applicationSqlRegisterTables')
const APPLICATION_SQL_COMMIT = Symbol('applicationSqlCommit')
const APPLICATION_SQL_COMMIT_PREEMPTIBLE = Symbol('applicationSqlCommitPreemptible')
const APPLICATION_SQL_ROLLBACK = Symbol('applicationSqlRollback')
const APPLICATION_SQL_DISPOSE = Symbol('applicationSqlDispose')

class ApplicationSqlSessionTarget extends RpcTarget {
  state: ApplicationSqlSessionState = 'created'
  mutated = false
  telemetryFinished = false

  constructor(
    readonly owner: ZeroDO,
    readonly sessionID: string,
    readonly readOnly: boolean,
    readonly priority: ApplicationSqlSessionPriority,
    readonly telemetry: SqlTelemetrySample | null
  ) {
    super()
  }

  begin(): Promise<void> {
    return this.owner[APPLICATION_SQL_ACQUIRE](this)
  }

  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params: readonly unknown[] = []
  ): Promise<Row[]> {
    return this.owner[APPLICATION_SQL_QUERY](this, sql, params)
  }

  queryPreemptible<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params: readonly unknown[] = []
  ): Promise<ApplicationSqlPreemptibleResult<Row[]>> {
    return this.owner[APPLICATION_SQL_QUERY_PREEMPTIBLE](this, sql, params)
  }

  exec(
    sql: string,
    params: readonly unknown[] = [],
    metadata?: SqlStatementMetadata
  ): Promise<ApplicationSqlExecResult> {
    return this.owner[APPLICATION_SQL_EXEC](this, sql, params, metadata)
  }

  queryPlan<Result = unknown>(
    plan: CompiledTransactionQueryPlan,
    queryName?: string,
    queryBudget?: Partial<TransactionQueryBudget>
  ): Promise<Result> {
    return this.owner[APPLICATION_SQL_QUERY_PLAN](this, plan, queryName, queryBudget)
  }

  queryPlanPreemptible<Result = unknown>(
    plan: CompiledTransactionQueryPlan,
    queryName?: string,
    queryBudget?: Partial<TransactionQueryBudget>
  ): Promise<ApplicationSqlPreemptibleResult<Result>> {
    return this.owner[APPLICATION_SQL_QUERY_PLAN_PREEMPTIBLE](
      this,
      plan,
      queryName,
      queryBudget
    )
  }

  registerTables(tables: readonly ApplicationSqlTable[]): Promise<void> {
    return this.owner[APPLICATION_SQL_REGISTER_TABLES](this, tables)
  }

  commit(): Promise<void> {
    return this.owner[APPLICATION_SQL_COMMIT](this)
  }

  commitPreemptible(): Promise<ApplicationSqlPreemptibleResult<void>> {
    return this.owner[APPLICATION_SQL_COMMIT_PREEMPTIBLE](this)
  }

  rollback(): Promise<void> {
    return this.owner[APPLICATION_SQL_ROLLBACK](this)
  }

  [Symbol.dispose](): void {
    this.owner[APPLICATION_SQL_DISPOSE](this)
  }
}

export class ZeroDO extends DurableObject {
  private readonly bootID = crypto.randomUUID()
  private readonly bootedAt = Date.now()
  private readonly requestsSinceBoot = {
    fetch: 0,
    applicationSqlSessions: 0,
    applicationSqlReadSessions: 0,
    applicationSqlWriteSessions: 0,
    sqlStatements: 0,
  }
  private readonly sqlBillingSinceBoot = { rowsRead: 0, rowsWritten: 0 }
  private readonly sqlTelemetrySampleRate: number
  private readonly workerVersion: string
  private activeAttribution: WriteAttributionCollector | null = null
  private readonly writeGrantWaitMs = new RecentLatencySamples(
    WRITE_GRANT_WAIT_SAMPLE_CAPACITY
  )
  private sql: any
  private watermarks: DurableWatermarkState
  private cdc: TransactionalCdc
  private schemaTables = new Set<string>()
  // `null` records a table confirmed absent from _zero_schema_tables
  private tableSchemas = new Map<string, SchemaTable | null>()
  private writeBudget: RollingRowWriteBudget
  private writeBudgetDisabled: boolean
  private writeBudgetAdminToken: string | undefined
  private activeWriteMeasurements: SqlWriteMeasurement[] | null = null
  private writeBudgetTripStatement: SqlWriteMeasurement | undefined
  private pendingChangesSchemaReady = false
  private applicationSqlWriter: ApplicationSqlSessionTarget | null = null
  private applicationSqlReaders = new Set<ApplicationSqlSessionTarget>()
  private applicationSqlQueue: ApplicationSqlWaiter[] = []
  protected applicationSqlDidCommit(_published: boolean, _mutated: boolean): void {}

  private durableObjectIdentity(): { objectId: string; objectName: string | null } {
    return {
      objectId: this.ctx.id.toString(),
      objectName: typeof this.ctx.id.name === 'string' ? this.ctx.id.name : null,
    }
  }

  private startSqlTelemetrySample(): SqlTelemetrySample | null {
    const rate = Number(this.sqlTelemetrySampleRate)
    if (!Number.isFinite(rate) || rate <= 0 || (rate < 1 && Math.random() >= rate)) {
      return null
    }
    return {
      startedAt: performance.now(),
      admittedAt: null,
      rowsReturned: 0,
      rowsChanged: 0,
      statements: 0,
      attribution: new WriteAttributionCollector(),
    }
  }

  private recordSqlTelemetry(
    sample: SqlTelemetrySample,
    result: { rows: readonly unknown[]; changes: number }
  ): void {
    sample.rowsReturned += result.rows.length
    sample.rowsChanged += result.changes
  }

  private emitSqlTelemetry(
    event: 'orez_sql_query_sample' | 'orez_sql_transaction_sample',
    name: string,
    outcome: 'committed' | 'error' | 'rolled_back' | 'success',
    sample: SqlTelemetrySample | null,
    error?: unknown,
    attribution?: WriteAttributionFields | null
  ): void {
    if (!sample) return
    try {
      console.log(
        JSON.stringify({
          event,
          name: name.slice(0, 200),
          outcome,
          durationMs: Math.round((performance.now() - sample.startedAt) * 1_000) / 1_000,
          ...(sample.admittedAt === null
            ? null
            : {
                queueMs:
                  Math.round((sample.admittedAt - sample.startedAt) * 1_000) / 1_000,
              }),
          rowsReturned: sample.rowsReturned,
          rowsChanged: sample.rowsChanged,
          statements: sample.statements,
          sampleRate: this.sqlTelemetrySampleRate,
          ...(attribution ?? null),
          ...(error
            ? {
                errorName: (error instanceof Error ? error.name : typeof error).slice(
                  0,
                  100
                ),
              }
            : null),
        })
      )
    } catch {}
  }

  private finishApplicationSqlTelemetry(
    session: ApplicationSqlSessionTarget,
    outcome: 'committed' | 'error' | 'rolled_back',
    error?: unknown
  ): void {
    if (session.telemetryFinished) return
    session.telemetryFinished = true
    if (this.activeAttribution === session.telemetry?.attribution) {
      this.activeAttribution = null
    }
    let attribution: WriteAttributionFields | null = null
    try {
      attribution = session.telemetry
        ? session.telemetry.attribution.summarize({
            workerVersion: this.workerVersion,
            namespaceClass: namespaceClassFromObjectName(
              typeof this.ctx.id?.name === 'string' ? this.ctx.id.name : null
            ),
            processStartedAt: this.bootedAt,
            sampleRate: this.sqlTelemetrySampleRate,
            observedAt: Date.now(),
            outcome,
          })
        : null
    } catch {
      attribution = null
    }
    this.emitSqlTelemetry(
      'orez_sql_transaction_sample',
      session.readOnly ? 'application_sql_read' : 'application_sql_write',
      outcome,
      session.telemetry,
      error,
      attribution
    )
  }

  private recordWriteBudgetRows(rows: number, statement?: SqlWriteMeasurement): void {
    const wasTripped = this.writeBudget.status().tripped
    try {
      this.writeBudget.recordBillable(rows)
    } catch (error) {
      if (error instanceof WriteBudgetExceededError && !wasTripped) {
        this.writeBudgetTripStatement = statement
          ? {
              sql: statement.sql
                .replace(/'(?:''|[^'])*'/g, '?')
                .replace(/\b\d+(?:\.\d+)?\b/g, '?')
                .replace(/\s+/g, ' ')
                .trim()
                .slice(0, 1_000),
              rowsWritten: statement.rowsWritten,
            }
          : undefined
        const status = this.writeBudget.status()
        console.error(
          JSON.stringify({
            event: 'orez_do_write_budget_tripped',
            ...this.durableObjectIdentity(),
            windowRows: status.windowRows,
            billableRows: status.billableRows,
            logicalRows: status.logicalRows,
            budget: status.budget,
            windowMs: status.windowMs,
            trippedAt: status.trippedAt,
            statement: this.writeBudgetTripStatement,
          })
        )
        this.persistWriteBudgetTrip()
      }
      throw error
    }
  }

  /**
   * Make the trip sticky, from outside the transaction that is aborting.
   *
   * The trip fires during cursor consumption, which is almost always inside
   * ctx.storage.transaction(). A put in that scope is rolled back with the
   * write (prod booted un-tripped this way on 2026-07-11), and code after the
   * transaction does not reliably get to run either: measured under workerd,
   * a write budget tripped on the application SQL path resolved neither the
   * transaction's success nor its failure path, so a namespace tripped by
   * soot's real write path persisted nothing and came back OPEN on the next
   * eviction. blockConcurrencyWhile is the seam that survives both: the
   * runtime defers this until the in-flight storage work is done, so it lands
   * after the rollback rather than inside it.
   *
   * The count rides along with the timestamp. The rolling window keeps decaying
   * while the circuit stays sticky and a restored object starts with none of
   * it, so a status read that consults the live meter reports 0/budget and
   * erases how far over the namespace actually went.
   */
  private persistWriteBudgetTrip(): void {
    const trip = this.writeBudget.trip()
    if (!trip) return
    void this.ctx
      .blockConcurrencyWhile(async () => {
        await this.ctx.storage.put(WRITE_BUDGET_TRIPPED_KEY, {
          ...trip,
          statement: this.writeBudgetTripStatement,
        } satisfies PersistedWriteBudgetTrip)
      })
      .catch((error: unknown) => {
        console.error(
          JSON.stringify({
            event: 'orez_do_write_budget_persist_failed',
            ...this.durableObjectIdentity(),
            message: error instanceof Error ? error.message : String(error),
          })
        )
      })
  }

  // The trip itself is persisted by persistWriteBudgetTrip at the moment it
  // fires, which is the one place every path passes through, so this only has
  // to shape the response.
  private writeBudgetErrorResponse(error: unknown): Response | null {
    if (!(error instanceof WriteBudgetExceededError)) return null
    return Response.json(error.toJSON(), { status: 429 })
  }

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    this.sql = ctx.storage.sql
    this.writeBudgetDisabled = /^(?:1|true)$/i.test(
      env.OREZ_DO_WRITE_BUDGET_DISABLED ?? ''
    )
    this.writeBudgetAdminToken = env.OREZ_DO_WRITE_BUDGET_ADMIN_TOKEN
    this.sqlTelemetrySampleRate = probabilityEnv(
      env.OREZ_SQL_TELEMETRY_SAMPLE_RATE,
      DEFAULT_SQL_TELEMETRY_SAMPLE_RATE
    )
    this.workerVersion =
      typeof env.CF_VERSION?.id === 'string' && env.CF_VERSION.id.length > 0
        ? env.CF_VERSION.id
        : 'local'
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
      this.requestsSinceBoot.sqlStatements++
      const mutation = isSqlMutation(statement)
      if (mutation && !this.writeBudgetDisabled) this.writeBudget.assertOpen()
      const cursor = rawExec(statement, ...params)
      const measurement: SqlWriteMeasurement | undefined = mutation
        ? { sql: statement, rowsWritten: 0 }
        : undefined
      if (measurement && this.activeWriteMeasurements) {
        this.activeWriteMeasurements.push(measurement)
      }
      return trackBillableCursorRows(
        cursor,
        (rows) => {
          this.sqlBillingSinceBoot.rowsWritten += rows
          if (!measurement) return
          measurement.rowsWritten += rows
          try {
            this.activeAttribution?.recordPhysical(statement, rows)
          } catch {}
          if (!this.writeBudgetDisabled) this.recordWriteBudgetRows(rows, measurement)
        },
        (rows) => {
          this.sqlBillingSinceBoot.rowsRead += rows
        }
      )
    }
    this.cdc = new TransactionalCdc(this.sql)
    this.watermarks = new DurableWatermarkState(this.sql)
    ctx.blockConcurrencyWhile(async () => {
      const recovered = this.rollbackAtomicallyWithoutForeignKeys(() => {
        const transactionIDs = recoverTxJournal(
          this.sql,
          'application',
          (transactionID) => {
            this.rollbackPendingTrackedChanges(transactionID)
          }
        )
        for (const transactionID of transactionIDs)
          this.deletePendingTrackedChanges(transactionID)
        return transactionIDs
      })
      if (recovered.length) this.invalidateSchemaCaches()
      // after recovery, which may still restore journal rows or re-create the
      // residual triggers from a pre-cleanup schema snapshot, and before the
      // trip restore, so the one-time drops run against a fresh write budget.
      this.dropZeroHttpJournalResidue()
      if (!this.writeBudgetDisabled) {
        const persisted = await ctx.storage.get<number | PersistedWriteBudgetTrip>(
          WRITE_BUDGET_TRIPPED_KEY
        )
        if (persisted) {
          this.writeBudget.restoreTrip(persisted)
          if (typeof persisted !== 'number')
            this.writeBudgetTripStatement = persisted.statement
        }
      }
    })
  }

  /**
   * Drop what the retired zero-http full-projection engine left in this
   * database. That engine's mount installed `_zsync_tr_<table>_{i,u,d}` AFTER
   * triggers on every synced table, appending a change envelope to
   * `_zsync_changes` per write plus a `_zsync_meta` marker row per UPDATE. The
   * engine was deleted on 2026-07-29, its drop logic with it, and nothing has
   * read either table since the packed ledger replaced that pull path. The
   * triggers stayed installed, though, so every synced write on a namespace
   * mounted before the deletion still paid one or two billable journal rows
   * forever (86,082 rows and ~2,600/day measured on the largest production
   * namespace).
   *
   * Order matters for crash safety, and every step is idempotent:
   *
   * 1. preserve the journal's high watermark in `_zsync_watermark`, which
   *    `initializePackedLedger` already consults, so a namespace whose packed
   *    ledger has not initialized yet still seeds above every cookie it ever
   *    issued after the journal is gone;
   * 2. drop every trigger whose body references `_zsync_changes`; `_zsync_*`
   *    is orez's reserved namespace, so no application trigger can
   *    legitimately write it;
   * 3. drop both tables and the journal's rollback-capture registration.
   *
   * Steady state costs one sqlite_master read per boot.
   */
  private dropZeroHttpJournalResidue(): void {
    const residue = this.sql
      .exec(
        'SELECT name, type FROM sqlite_master ' +
          "WHERE (type = 'trigger' AND sql LIKE '%\\_zsync\\_changes%' ESCAPE '\\') " +
          `OR (type = 'table' AND name IN ('${ZSYNC_CHANGES_TABLE}', '_zsync_meta'))`
      )
      .toArray()
      .map((row) => ({ name: String(row.name), type: String(row.type) }))
    if (residue.length === 0) return
    if (residue.some((row) => row.type === 'table' && row.name === ZSYNC_CHANGES_TABLE)) {
      const high = Number(
        this.sql
          .exec(
            `SELECT COALESCE(MAX(watermark), 0) AS high FROM ${quoteIdent(ZSYNC_CHANGES_TABLE)}`
          )
          .toArray()[0]?.high ?? 0
      )
      if (high > 0) {
        this.sql.exec(
          'CREATE TABLE IF NOT EXISTS _zsync_watermark (' +
            'lock INTEGER PRIMARY KEY CHECK (lock = 1), high INTEGER NOT NULL)'
        )
        this.sql.exec(
          'INSERT INTO _zsync_watermark (lock, high) VALUES (1, ?) ' +
            'ON CONFLICT (lock) DO UPDATE SET high = MAX(high, excluded.high)',
          high
        )
      }
    }
    for (const row of residue) {
      if (row.type === 'trigger') {
        this.sql.exec(`DROP TRIGGER IF EXISTS ${quoteIdent(row.name)}`)
      }
    }
    this.sql.exec(`DROP TABLE IF EXISTS ${quoteIdent(ZSYNC_CHANGES_TABLE)}`)
    this.sql.exec('DROP TABLE IF EXISTS _zsync_meta')
    const cdcRegistry = this.sql
      .exec(
        "SELECT 1 AS ok FROM sqlite_master WHERE type = 'table' AND name = '_orez_cdc_tables' LIMIT 1"
      )
      .toArray()
    if (cdcRegistry.length > 0) {
      this.sql.exec(
        'DELETE FROM _orez_cdc_tables WHERE physical_table = ?',
        ZSYNC_CHANGES_TABLE
      )
      this.cdc.reload()
    }
  }

  async fetch(request: Request): Promise<Response> {
    this.requestsSinceBoot.fetch++
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
        trippedStatement: this.writeBudgetTripStatement,
      })
    if (url.pathname === '/_orez/status' && request.method === 'GET')
      return this.handleStatus(request)
    if (url.pathname === '/_orez/write-budget/trip' && request.method === 'POST')
      return this.handleWriteBudgetTrip(request)
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
      return this.withApplicationSqlTurn(() => this.handleSnapshot(url), request.signal)
    if (url.pathname === '/notify' && request.method === 'POST')
      return Response.json({ ok: true, cookie: this.cookie() })
    return new Response('not found', { status: 404 })
  }

  private handleStatus(request: Request): Response {
    if (!this.hasAdminToken(request)) {
      return Response.json({ error: 'forbidden' }, { status: 403 })
    }
    const queuedReadSessions = this.applicationSqlQueue.filter(
      (waiter) => waiter.session.readOnly
    ).length
    return Response.json({
      bootID: this.bootID,
      bootedAt: this.bootedAt,
      uptimeMs: Math.max(0, Date.now() - this.bootedAt),
      ns: request.headers.get('x-orez-do-instance'),
      objectId: this.ctx.id.toString(),
      databaseSizeBytes: this.ctx.storage.sql.databaseSize,
      requestsSinceBoot: { ...this.requestsSinceBoot },
      sqlBillingSinceBoot: { ...this.sqlBillingSinceBoot },
      applicationSql: {
        activeReaders: this.applicationSqlReaders.size,
        writerActive: this.applicationSqlWriter !== null,
        queuedReaders: queuedReadSessions,
        queuedWriters: this.applicationSqlQueue.length - queuedReadSessions,
        writeGrantWaitMs: this.writeGrantWaitMs.status(),
      },
      writeBudget: {
        enabled: !this.writeBudgetDisabled,
        ...this.writeBudget.status(),
        trippedStatement: this.writeBudgetTripStatement,
      },
    })
  }

  private async handleWriteBudgetReopen(request: Request): Promise<Response> {
    if (!this.hasAdminToken(request))
      return Response.json({ error: 'forbidden' }, { status: 403 })
    await this.ctx.storage.delete(WRITE_BUDGET_TRIPPED_KEY)
    this.writeBudgetTripStatement = undefined
    const status = this.writeBudget.reopen()
    console.log(
      JSON.stringify({
        event: 'orez_do_write_budget_reopened',
        ...this.durableObjectIdentity(),
        reopenedAt: Date.now(),
      })
    )
    return Response.json({ ok: true, enabled: !this.writeBudgetDisabled, ...status })
  }

  private async handleWriteBudgetTrip(request: Request): Promise<Response> {
    if (!this.hasAdminToken(request))
      return Response.json({ error: 'forbidden' }, { status: 403 })
    const status = this.writeBudget.forceTrip()
    this.writeBudgetTripStatement = {
      sql: 'operator force-trip',
      rowsWritten: 0,
    }
    await this.ctx.storage.put(WRITE_BUDGET_TRIPPED_KEY, {
      ...this.writeBudget.trip(),
      statement: this.writeBudgetTripStatement,
    } satisfies PersistedWriteBudgetTrip)
    console.error(
      JSON.stringify({
        event: 'orez_do_write_budget_force_tripped',
        ...this.durableObjectIdentity(),
        trippedAt: Date.now(),
      })
    )
    return Response.json({
      ok: true,
      enabled: !this.writeBudgetDisabled,
      ...status,
      trippedStatement: this.writeBudgetTripStatement,
    })
  }

  private hasAdminToken(request: Request): boolean {
    const supplied =
      request.headers.get('x-orez-admin-token') ??
      request.headers.get('authorization')?.replace(/^Bearer\s+/i, '')
    return Boolean(this.writeBudgetAdminToken && supplied === this.writeBudgetAdminToken)
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
      const budgetResponse = this.writeBudgetErrorResponse(err)
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
      const budgetResponse = this.writeBudgetErrorResponse(err)
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
            if (
              item.migrateIfColumnType &&
              !this.columnTypeGuardMatches(item.migrateIfColumnType)
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
      const budgetResponse = this.writeBudgetErrorResponse(err)
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
   * Atomic commit point for an application-SQL transaction. Promotes the
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
      const budgetResponse = this.writeBudgetErrorResponse(err)
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
      const budgetResponse = this.writeBudgetErrorResponse(err)
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
      const count = this.rollbackAtomicallyWithoutForeignKeys(() => {
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
      const budgetResponse = this.writeBudgetErrorResponse(err)
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
      const transactionIDs = this.rollbackAtomicallyWithoutForeignKeys(() => {
        const recovered = recoverTxJournal(this.sql, owner, (txID) => {
          this.rollbackPendingTrackedChanges(txID)
        })
        for (const txID of recovered) this.deletePendingTrackedChanges(txID)
        return recovered
      })
      this.invalidateSchemaCaches()
      return Response.json({ ok: true, transactionIDs })
    } catch (err: any) {
      const budgetResponse = this.writeBudgetErrorResponse(err)
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
      const changes = this.readChangesSince(watermark, changeLimit)
      return Response.json({
        watermark: head,
        changes,
        oldestCommitTimeMs:
          changes.length === 0
            ? undefined
            : Math.min(...changes.map((change) => change.commitTimeMs)),
        // Stamped after the single change-log read. The sync host brackets this
        // request with its own clock and uses the midpoint to estimate skew.
        sourceTimeMs: Date.now(),
      })
    } catch (err: any) {
      const budgetResponse = this.writeBudgetErrorResponse(err)
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
          const physicalTable = schema.physicalName || table
          const physicalPrimaryKey = schema.primaryKey.map(
            (column) => schema.columns[column]?.serverName || column
          )

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

          const primaryKey = physicalPrimaryKey.map(quoteIdent)
          const keyColumns =
            primaryKey.length === 1 ? primaryKey[0] : `(${primaryKey.join(', ')})`
          const keyParams =
            primaryKey.length === 1 ? '?' : `(${primaryKey.map(() => '?').join(', ')})`
          const where = cursorValues ? ` WHERE ${keyColumns} > ${keyParams}` : ''
          // one look-ahead row distinguishes an exact final page from a page
          // with more data without issuing an unbounded count or second read.
          const page = this.sql
            .exec(
              `SELECT * FROM ${quoteIdent(physicalTable)}${where} ORDER BY ${primaryKey.join(', ')} LIMIT ?`,
              ...(cursorValues ?? []),
              limit + 1
            )
            .toArray() as Record<string, unknown>[]
          const hasMore = page.length > limit
          const rawRows = hasMore ? page.slice(0, limit) : page
          let nextCursor: string | null = null
          if (hasMore) {
            const last = rawRows[rawRows.length - 1]
            const values = physicalPrimaryKey.map((column) => last[column])
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
        for (const name of names) {
          const schema = this.schemaForTable(name)
          const physicalName = schema?.physicalName || name
          tables[name] = this.sql
            .exec(`SELECT * FROM ${quoteIdent(physicalName)}`)
            .toArray()
            .map((row: any) => this.normalizeRow(name, row))
        }
        return Response.json({ watermark: this.watermark(), tables })
      })
    } catch (err: any) {
      const budgetResponse = this.writeBudgetErrorResponse(err)
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
   * every SQL cursor is consumed before an executor promise is returned.
   */
  protected async runApplicationTransaction<T>(
    compileQuery: ZeroDOQueryCompiler,
    work: (tx: ZeroDOTransactionExecutor) => T | Promise<T>,
    queryBudget?: Partial<TransactionQueryBudget>
  ): Promise<T> {
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
        const result = execute(sql, params, metadata)
        return { changes: result.changes }
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

    return this.withSchemaProvisioningWait(() =>
      this.atomically(async () => {
        return work(tx)
      })
    )
  }

  private assertApplicationSqlSession(session: ApplicationSqlSessionTarget): void {
    const admitted = session.readOnly
      ? this.applicationSqlReaders.has(session)
      : this.applicationSqlWriter === session
    if (session.state !== 'active' || !admitted) {
      // the three fields distinguish the failure modes: a stale session that
      // already closed, a reader evicted from the set, and a writer displaced
      // by a newer one all read identically without them
      throw new Error(
        `application SQLite session is not active (state=${session.state}, admitted=${admitted}, readOnly=${session.readOnly})`
      )
    }
  }

  /**
   * Read sessions never mutate, so no lane can escalate and there is nothing to
   * deadlock over: a reader waits only on the writer, and the writer waits only
   * on the reader set draining.
   */
  private canAdmitApplicationSqlSession(session: ApplicationSqlSessionTarget): boolean {
    if (this.applicationSqlWriter) return false
    return session.readOnly || this.applicationSqlReaders.size === 0
  }

  /**
   * admit from the head of the ordered queue and stop at the first waiter that
   * cannot run yet.
   *
   * the stop keeps ordering within each priority class. scanning past a blocked
   * writer to admit younger readers would let a steady read load hold the turn
   * away from it
   * indefinitely, which is the starvation the competitive `begin()` poll this
   * replaces produced in production: callers raced a 25 ms timer for one
   * ownership flag, so admission order was decided by poll phase, and sessions
   * that had arrived ten seconds earlier watched later ones complete.
   */
  private pumpApplicationSqlQueue(): void {
    while (this.applicationSqlQueue.length > 0) {
      const waiter = this.applicationSqlQueue[0]
      if (waiter.session.state !== 'waiting') {
        this.applicationSqlQueue.shift()
        clearTimeout(waiter.timer)
        continue
      }
      if (!this.canAdmitApplicationSqlSession(waiter.session)) return
      this.applicationSqlQueue.shift()
      clearTimeout(waiter.timer)
      waiter.session.state = 'active'
      if (waiter.session.telemetry) {
        waiter.session.telemetry.admittedAt = performance.now()
      }
      if (waiter.session.readOnly) this.applicationSqlReaders.add(waiter.session)
      else {
        this.applicationSqlWriter = waiter.session
        this.activeAttribution = waiter.session.telemetry?.attribution ?? null
        this.writeGrantWaitMs.record(performance.now() - waiter.queuedAt)
      }
      waiter.admit()
    }
  }

  /**
   * join the ordered queue and resolve when this session owns its turn.
   *
   * A waiting session holds one queue entry and one timer, and nothing else:
   * cancellation, rollback and RPC stub disposal all route through
   * `releaseApplicationSqlTurn`, which drops the entry and re-pumps. The
   * deadline bounds a queue entry whose caller neither cancels nor disposes,
   * and reports the wait as an error instead of hanging the request.
   */
  [APPLICATION_SQL_ACQUIRE](session: ApplicationSqlSessionTarget): Promise<void> {
    if (session.state !== 'created') {
      return Promise.reject(new Error('application SQLite session cannot begin again'))
    }
    session.state = 'waiting'
    const admission = new Promise<void>((resolve, reject) => {
      const waiter: ApplicationSqlWaiter = {
        session,
        queuedAt: performance.now(),
        admit: resolve,
        reject,
        timer: setTimeout(() => {
          const index = this.applicationSqlQueue.indexOf(waiter)
          if (index < 0) return
          this.applicationSqlQueue.splice(index, 1)
          session.state = 'closed'
          reject(new Error('timed out acquiring the application SQLite session'))
        }, APPLICATION_SQL_TURN_WAIT_MS),
      }
      const firstLowerPriority = this.applicationSqlQueue.findIndex((queued) => {
        if (session.priority === 'latency-sensitive') {
          return queued.session.priority !== 'latency-sensitive'
        }
        return session.priority === 'normal' && queued.session.priority === 'background'
      })
      if (firstLowerPriority === -1) this.applicationSqlQueue.push(waiter)
      else this.applicationSqlQueue.splice(firstLowerPriority, 0, waiter)

      // a consistent backup owns a read session across its R2 uploads. it is
      // allowed to lose that work, but request writes cannot wait behind the
      // network. closing the background reader makes its next query or commit
      // fail, so it cannot publish a partial backup.
      if (!session.readOnly) {
        for (const reader of [...this.applicationSqlReaders]) {
          if (reader.priority === 'background') {
            reader.state = 'preempted'
            this.applicationSqlReaders.delete(reader)
          }
        }
      }
      this.pumpApplicationSqlQueue()
    })
    return admission
  }

  /**
   * Close a session in any state and hand the turn to the next arrival.
   *
   * A session that is still queued has never touched SQLite, so dropping its
   * queue entry is the whole of its cleanup.
   */
  private releaseApplicationSqlTurn(
    session: ApplicationSqlSessionTarget,
    options: { pump?: boolean } = {}
  ): void {
    if (session.state === 'closed') return
    if (session.state === 'preempted') {
      session.state = 'closed'
      return
    }
    if (session.state === 'created') {
      session.state = 'closed'
      return
    }
    if (session.state === 'waiting') {
      const index = this.applicationSqlQueue.findIndex(
        (waiter) => waiter.session === session
      )
      if (index >= 0) {
        const [waiter] = this.applicationSqlQueue.splice(index, 1)
        clearTimeout(waiter.timer)
      }
      // The in-flight `begin()` is left unsettled on purpose. Only this
      // session's own caller can close it while it waits, and by then that
      // caller has stopped awaiting admission and is on its way to disposing
      // the stub, which cancels the call. Rejecting instead delivers an error
      // nobody is listening for, and workerd reports every one of those as an
      // uncaught error — measured, one per cancellation.
      session.state = 'closed'
      return
    }
    this.assertApplicationSqlSession(session)
    session.state = 'closed'
    if (session.readOnly) this.applicationSqlReaders.delete(session)
    else {
      if (
        options.pump !== false &&
        this.activeAttribution === session.telemetry?.attribution
      ) {
        this.activeAttribution = null
      }
      this.applicationSqlWriter = null
    }
    if (options.pump !== false) this.pumpApplicationSqlQueue()
  }

  private async withApplicationSqlTurn<Value>(
    work: () => Value | Promise<Value>,
    signal?: AbortSignal
  ): Promise<Value> {
    const session = await this.applicationSqlSession(crypto.randomUUID(), {
      readOnly: true,
    })
    const dispose = () => session[Symbol.dispose]()
    signal?.addEventListener('abort', dispose, { once: true })
    try {
      await session.begin()
      return await work()
    } finally {
      signal?.removeEventListener('abort', dispose)
      dispose()
    }
  }

  /**
   * drive one application SQLite session from INSIDE this durable object.
   *
   * identical to the client-side session helper in application-sql.ts — same
   * admission, same per-statement journal, same commit/rollback — except every
   * call is local instead of an RPC. the caller must already be running in this
   * object.
   */
  private async withLocalApplicationSqlSession<Value>(
    readOnly: boolean,
    work: (session: ApplicationSqlSessionTarget) => Value | Promise<Value>,
    priority: ApplicationSqlSessionPriority = 'normal'
  ): Promise<Value> {
    const session = await this.applicationSqlSession(crypto.randomUUID(), {
      readOnly,
      priority,
    })
    try {
      await session.begin()
      const value = await work(session)
      if (priority === 'background') {
        applicationSqlPreemptibleValue(await session.commitPreemptible())
      } else {
        await session.commit()
      }
      return value
    } catch (error) {
      await session.rollback().catch(() => {})
      throw error
    } finally {
      if (session.state !== 'closed') session[Symbol.dispose]()
    }
  }

  /**
   * an ApplicationSqlClient bound to THIS object, for subclass work that would
   * otherwise call back into itself over RPC.
   *
   * a schema migration is the motivating case: driven from a worker it costs one
   * round trip per statement, so replaying a full migration history into a fresh
   * namespace ran ~1000 sequential round trips and measured 35.9s wall against
   * 2.8s of durable-object cpu — 92% of it spent idle between statements. the
   * statements, their order and their transaction semantics are unchanged; only
   * the transport is.
   */
  protected applicationSqlLocalClient(
    namespace: string,
    options: Pick<ApplicationSqlClientOptions, 'priority'> = {}
  ): ApplicationSqlClient {
    const run = <Value>(
      readOnly: boolean,
      work: (session: ApplicationSqlSessionTarget) => Promise<Value>
    ) => this.withLocalApplicationSqlSession(readOnly, work, options.priority ?? 'normal')
    const transaction = <Value>(
      readOnly: boolean,
      compileQuery: ZeroDOQueryCompiler,
      work: ApplicationSqlTransactionWork<Value>,
      queryBudget?: Partial<TransactionQueryBudget>
    ) =>
      run(readOnly, async (session) =>
        work({
          exec: (sql, params = [], metadata) => session.exec(sql, params, metadata),
          query: async (sql, params = []) =>
            options.priority === 'background'
              ? applicationSqlPreemptibleValue(
                  await session.queryPreemptible(sql, params)
                )
              : session.query(sql, params),
          registerTables: (tables) => session.registerTables(tables),
          async queryAst(ast, format, queryName) {
            const plan = await compileQuery(ast, format)
            if (options.priority === 'background') {
              return applicationSqlPreemptibleValue(
                await session.queryPlanPreemptible(plan, queryName, queryBudget)
              )
            }
            return session.queryPlan(plan, queryName, queryBudget)
          },
        })
      )
    return {
      namespace,
      query: (sql, params = []) => run(true, (session) => session.query(sql, params)),
      exec: (sql, params = [], metadata) =>
        run(false, (session) => session.exec(sql, params, metadata)),
      registerTables: (tables) => run(false, (session) => session.registerTables(tables)),
      transaction: (compileQuery, work, queryBudget) =>
        transaction(false, compileQuery, work, queryBudget),
      readTransaction: (compileQuery, work, queryBudget) =>
        transaction(true, compileQuery, work, queryBudget),
    }
  }

  private registerApplicationSqlTables(tables: readonly ApplicationSqlTable[]): void {
    for (const table of tables) {
      // registration carries the schema's declared capture set, so its publish
      // state is authoritative: this is the runtime path that can demote an
      // already published table to rollback-only capture.
      this.cdc.ensureTable(
        {
          physicalTableName: table.table,
          tableName: table.publicTable,
          ...(table.publish === false ? { publish: false } : null),
        },
        false,
        true
      )
    }
  }

  /**
   * private durable object RPC surface for the application SQLite client. the
   * disposable target exists before admission, so a queued caller can cancel by
   * disposing it and an active one rolls its transaction back. this method is
   * intentionally absent from fetch().
   */
  async applicationSqlSession(
    sessionID: string,
    options: ApplicationSqlSessionOptions = {}
  ): Promise<ApplicationSqlSessionTarget> {
    if (!sessionID) throw new TypeError('application SQLite session id is required')
    const priority = options.priority ?? 'normal'
    if (
      priority !== 'background' &&
      priority !== 'normal' &&
      priority !== 'latency-sensitive'
    ) {
      throw new TypeError('invalid application SQLite session priority')
    }
    if (priority === 'background' && options.readOnly !== true) {
      throw new TypeError('background application SQLite sessions must be read-only')
    }
    this.requestsSinceBoot.applicationSqlSessions++
    if (options.readOnly === true) this.requestsSinceBoot.applicationSqlReadSessions++
    else this.requestsSinceBoot.applicationSqlWriteSessions++
    return new ApplicationSqlSessionTarget(
      this,
      sessionID,
      options.readOnly === true,
      priority,
      this.startSqlTelemetrySample()
    )
  }

  /**
   * Run one read statement end to end in a single round trip.
   *
   * A single statement is already atomic, so a session that exists only to
   * carry it costs four sequential RPCs (open, admit, run, commit) for one
   * SELECT — the shape every authenticated read on the sync path pays. This
   * opens, admits, runs and closes the same read session locally instead.
   */
  async applicationSqlQuery<
    Row extends Record<string, unknown> = Record<string, unknown>,
  >(
    sql: string,
    params: readonly unknown[] = [],
    options: Pick<ApplicationSqlSessionOptions, 'priority'> = {}
  ): Promise<Row[]> {
    return this.withLocalApplicationSqlSession(
      true,
      (session) => session.query<Row>(sql, params),
      options.priority
    )
  }

  private prepareApplicationSqlMutation(sessionID: string, sql: string): boolean {
    if (!isSqlMutation(sql)) return false
    if (!isSqlRowMutation(sql)) {
      const targets = schemaChangeTargets(sql)
      if (
        /^\s*CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\b/i.test(sql) &&
        targets.length > 0 &&
        targets.every((table) => this.tableExists(table))
      ) {
        return false
      }
      beginTxJournal(this.sql, sessionID, 'application')
      snapshotTxSchema(this.sql, sessionID, 'application', targets)
      return true
    }
    // register the packed ledger for rollback-only row undo before the
    // statement that writes it runs, in the same storage transaction. see
    // ZSYNC_LOG_SEGMENTS_TABLE for why nothing else registers it and what an
    // unrestored captureMode does to the namespace.
    if (sql.includes(ZSYNC_LOG_SEGMENTS_TABLE)) {
      this.cdc.ensureTable({
        physicalTableName: ZSYNC_LOG_SEGMENTS_TABLE,
        tableName: ZSYNC_LOG_SEGMENTS_TABLE,
        publish: false,
      })
    }
    beginTxJournal(this.sql, sessionID, 'application')
    return true
  }

  /**
   * A read session declared its lane before it was admitted, and readers run
   * alongside each other on the strength of that declaration. Escalating one
   * mid-session would strand every other reader on a state that is no longer
   * committed, so a mutation here is an error rather than an upgrade.
   */
  private assertApplicationSqlStatement(
    session: ApplicationSqlSessionTarget,
    sql: string
  ): void {
    this.assertApplicationSqlSession(session)
    if (session.readOnly && classifySql(sql).mutation) {
      throw new Error('read-only application SQLite session cannot execute a mutation')
    }
  }

  async [APPLICATION_SQL_QUERY]<
    Row extends Record<string, unknown> = Record<string, unknown>,
  >(
    session: ApplicationSqlSessionTarget,
    sql: string,
    params: readonly unknown[] = []
  ): Promise<Row[]> {
    this.assertApplicationSqlStatement(session, sql)
    return this.atomically(() => {
      if (this.prepareApplicationSqlMutation(session.sessionID, sql)) {
        session.mutated = true
      }
      return this.executeSQL(
        sql,
        [...params],
        undefined,
        session.sessionID,
        session.telemetry
      ).rows as Row[]
    })
  }

  async [APPLICATION_SQL_QUERY_PREEMPTIBLE]<
    Row extends Record<string, unknown> = Record<string, unknown>,
  >(
    session: ApplicationSqlSessionTarget,
    sql: string,
    params: readonly unknown[] = []
  ): Promise<ApplicationSqlPreemptibleResult<Row[]>> {
    if (session.state === 'preempted') return { outcome: 'preempted' }
    return {
      outcome: 'completed',
      value: await this[APPLICATION_SQL_QUERY]<Row>(session, sql, params),
    }
  }

  async [APPLICATION_SQL_EXEC](
    session: ApplicationSqlSessionTarget,
    sql: string,
    params: readonly unknown[] = [],
    metadata?: SqlStatementMetadata
  ): Promise<ApplicationSqlExecResult> {
    this.assertApplicationSqlStatement(session, sql)
    return this.atomically(() => {
      if (this.prepareApplicationSqlMutation(session.sessionID, sql)) {
        session.mutated = true
      }
      const result = this.executeSQL(
        sql,
        [...params],
        applicationSqlTrack(metadata),
        session.sessionID,
        session.telemetry
      )
      return { changes: result.changes }
    })
  }

  async [APPLICATION_SQL_QUERY_PLAN]<Result = unknown>(
    session: ApplicationSqlSessionTarget,
    plan: CompiledTransactionQueryPlan,
    queryName?: string,
    queryBudget?: Partial<TransactionQueryBudget>
  ): Promise<Result> {
    this.assertApplicationSqlSession(session)
    const sample = this.startSqlTelemetrySample()
    const name = queryName?.trim() || `${plan.rootTable}:${plan.planHash}`
    try {
      const result = await this.atomically(() =>
        executeTransactionQueryPlan<Result>(
          plan,
          (sql, params) => {
            this.assertApplicationSqlStatement(session, sql)
            return this.executeSQL(
              sql,
              params,
              undefined,
              session.sessionID,
              session.telemetry,
              sample
            ).rows
          },
          { queryName, budget: queryBudget }
        )
      )
      this.emitSqlTelemetry('orez_sql_query_sample', name, 'success', sample)
      return result
    } catch (error) {
      this.emitSqlTelemetry('orez_sql_query_sample', name, 'error', sample, error)
      throw error
    }
  }

  async [APPLICATION_SQL_QUERY_PLAN_PREEMPTIBLE]<Result = unknown>(
    session: ApplicationSqlSessionTarget,
    plan: CompiledTransactionQueryPlan,
    queryName?: string,
    queryBudget?: Partial<TransactionQueryBudget>
  ): Promise<ApplicationSqlPreemptibleResult<Result>> {
    if (session.state === 'preempted') return { outcome: 'preempted' }
    return {
      outcome: 'completed',
      value: await this[APPLICATION_SQL_QUERY_PLAN](
        session,
        plan,
        queryName,
        queryBudget
      ),
    }
  }

  async [APPLICATION_SQL_REGISTER_TABLES](
    session: ApplicationSqlSessionTarget,
    tables: readonly ApplicationSqlTable[]
  ): Promise<void> {
    this.assertApplicationSqlSession(session)
    if (session.readOnly) {
      throw new Error('read-only application SQLite session cannot register tables')
    }
    await this.atomically(() => this.registerApplicationSqlTables(tables))
  }

  async [APPLICATION_SQL_COMMIT](session: ApplicationSqlSessionTarget): Promise<void> {
    this.assertApplicationSqlSession(session)
    let published = false
    let outcome: 'committed' | 'error' = 'committed'
    let error: unknown
    try {
      if (session.mutated) {
        published = await this.atomically(() => {
          const committed = this.commitPendingTrackedChanges(session.sessionID)
          commitTxJournal(this.sql, session.sessionID)
          return committed > 0
        })
      }
    } catch (caught) {
      outcome = 'error'
      error = caught
    }
    this.releaseApplicationSqlTurn(session, { pump: false })
    try {
      if (outcome === 'committed') {
        this.applicationSqlDidCommit(published, session.mutated)
      }
    } catch (caught) {
      outcome = 'error'
      error = caught
    } finally {
      this.finishApplicationSqlTelemetry(
        session,
        outcome,
        outcome === 'error' ? error : undefined
      )
      this.pumpApplicationSqlQueue()
    }
    if (outcome === 'error') throw error
  }

  async [APPLICATION_SQL_COMMIT_PREEMPTIBLE](
    session: ApplicationSqlSessionTarget
  ): Promise<ApplicationSqlPreemptibleResult<void>> {
    if (session.state === 'preempted') {
      this.releaseApplicationSqlTurn(session)
      this.finishApplicationSqlTelemetry(session, 'rolled_back')
      return { outcome: 'preempted' }
    }
    await this[APPLICATION_SQL_COMMIT](session)
    return { outcome: 'completed', value: undefined }
  }

  /**
   * Undo whatever this session wrote and hand its turn on.
   *
   * Rollback, cancellation and RPC stub disposal are the same operation: a
   * session that never reached SQLite only has to leave the queue, and one that
   * did has to replay its row images before releasing.
   */
  private closeApplicationSqlSession(session: ApplicationSqlSessionTarget): void {
    if (session.state === 'active' && session.mutated) {
      try {
        this.rollbackAtomicallyWithoutForeignKeys(() => {
          this.rollbackPendingTrackedChanges(session.sessionID)
          rollbackTxJournal(this.sql, session.sessionID)
          this.deletePendingTrackedChanges(session.sessionID)
        })
        this.invalidateSchemaCaches()
      } finally {
        this.releaseApplicationSqlTurn(session)
      }
      return
    }
    this.releaseApplicationSqlTurn(session)
  }

  async [APPLICATION_SQL_ROLLBACK](session: ApplicationSqlSessionTarget): Promise<void> {
    try {
      this.closeApplicationSqlSession(session)
      this.finishApplicationSqlTelemetry(session, 'rolled_back')
    } catch (error) {
      this.finishApplicationSqlTelemetry(session, 'error', error)
      throw error
    }
  }

  [APPLICATION_SQL_DISPOSE](session: ApplicationSqlSessionTarget): void {
    try {
      this.closeApplicationSqlSession(session)
      this.finishApplicationSqlTelemetry(session, 'rolled_back')
    } catch (error) {
      this.finishApplicationSqlTelemetry(session, 'error', error)
      throw error
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

  /**
   * Run journal rollback/recovery with foreign-key enforcement off.
   *
   * Rollback restores pre-transaction images verbatim, so the target state is
   * the pre-tx reality whatever its referential health. Enforcement is also
   * fatal here: workerd always runs with foreign_keys on, and in a namespace
   * whose parent table is missing (an interrupted table rebuild) EVERY
   * statement on a referencing table fails at compile with "no such table:
   * main.<parent>". That made recovery unable to ever finish, and because the
   * storage transaction rolled the cleanup back while Cloudflare still billed
   * the rows written, every wake re-ran the same doomed replay forever (the
   * 2026-07-25 rows-written runaway). The pragma cannot change inside a
   * transaction, so it brackets one synchronous transaction; the whole helper
   * is synchronous, so no other event can execute while enforcement is off.
   * The cursors must be consumed: workerd executes a statement only as its
   * cursor is read, so a discarded PRAGMA cursor silently never runs.
   */
  private rollbackAtomicallyWithoutForeignKeys<T>(work: () => T): T {
    this.sql.exec('PRAGMA foreign_keys = OFF').toArray()
    try {
      return this.atomicallySync(work)
    } finally {
      this.restoreForeignKeyEnforcement()
    }
  }

  /**
   * Measured under workerd: only the first foreign_keys change in an event
   * applies, so the OFF above latches and an inline reset here is silently
   * ignored — the object would then serve every later write unenforced.
   * Verify, and when the inline reset did not stick, re-run it from
   * blockConcurrencyWhile: the runtime defers that until the in-flight
   * storage work is done, the same seam persistWriteBudgetTrip relies on.
   */
  private restoreForeignKeyEnforcement(): void {
    this.sql.exec('PRAGMA foreign_keys = ON').toArray()
    const state = this.sql.exec('PRAGMA foreign_keys').one() as
      | { foreign_keys?: unknown }
      | undefined
    if (Number(state?.foreign_keys ?? 0) === 1) return
    void this.ctx
      .blockConcurrencyWhile(async () => {
        this.sql.exec('PRAGMA foreign_keys = ON').toArray()
      })
      .catch((error: unknown) => {
        console.error(
          JSON.stringify({
            event: 'orez_do_foreign_keys_restore_failed',
            message: error instanceof Error ? error.message : String(error),
          })
        )
      })
  }

  protected invalidateSchemaCaches(): void {
    this.watermarks.invalidateCache()
    this.pendingChangesSchemaReady = false
    // memoized application table shapes revert with the SQLite row on rollback,
    // so every path that discards a transaction must also discard this map
    this.tableSchemas.clear()
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

  private columnTypeGuardMatches(condition: {
    table: string
    column: string
    affinity?: string
    declaredType?: string
  }): boolean {
    let actualType: unknown
    try {
      const cursor = this.sql.exec(
        'SELECT type FROM pragma_table_info(?) WHERE name = ? LIMIT 1',
        condition.table,
        condition.column
      )
      actualType = this.cursorRows(cursor)[0]?.type
    } catch {
      return false
    }
    if (actualType === undefined) return false
    if (typeof condition.declaredType === 'string') {
      return (
        normalizeDeclaredSqlType(actualType) ===
        normalizeDeclaredSqlType(condition.declaredType)
      )
    }
    if (
      condition.affinity === 'blob' ||
      condition.affinity === 'integer' ||
      condition.affinity === 'numeric' ||
      condition.affinity === 'real' ||
      condition.affinity === 'text'
    ) {
      return declaredSqlTypeAffinity(actualType) === condition.affinity
    }
    throw new TypeError('migrateIfColumnType requires declaredType or a SQLite affinity')
  }

  private executeSQL(
    sql: string,
    params: unknown[] = [],
    track?: SqlTrack,
    transactionID?: string,
    transactionTelemetry: SqlTelemetrySample | null = null,
    queryTelemetry: SqlTelemetrySample | null = null
  ): {
    rows: Record<string, unknown>[]
    columns: string[]
    changes: number
    affectedRows?: number
    capturedChanges?: number
  } {
    if (transactionTelemetry) transactionTelemetry.statements++
    if (queryTelemetry && queryTelemetry !== transactionTelemetry) {
      queryTelemetry.statements++
    }
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

    // The transaction owner marked this table row-journaled, expecting the DO to
    // capture before/after images for it. When it cannot, that marker promises
    // a rollback nothing can perform, so take the table snapshot the journal
    // would otherwise have taken. It has to happen before the DML, while the
    // table still holds its pre-transaction contents.
    const trackedTransactionID = track ? transactionID || track.transactionID : undefined
    // Which tables a table snapshot now owns the rollback of. Everything else
    // this statement captures stays row-undoable, so snapshotting one
    // uncoverable side-effect target cannot strand the rest.
    const snapshotted = new Set<string>()
    if (track && !capturesTrackedTable && trackedTransactionID) {
      const physicalTableName =
        track.physicalTableName || stripPublicPrefix(track.tableName)
      if (this.tableExists(physicalTableName)) {
        upgradeToTableSnapshot(this.sql, trackedTransactionID, physicalTableName)
        snapshotted.add(physicalTableName)
      }
    }
    if (track && trackedTransactionID) {
      const physicalTableName =
        track.physicalTableName || stripPublicPrefix(track.tableName)
      for (const table of snapshotSideEffectWriteTables(
        this.sql,
        trackedTransactionID,
        physicalTableName,
        track.operation,
        (table) => this.cdc.coversRowUndo(table)
      )) {
        snapshotted.add(table)
      }
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
    const { mutation, rowMutation } = classifySql(sql)
    const changes = rowMutation
      ? Number(this.sql.exec('SELECT changes() AS changes').one()?.changes ?? 0)
      : 0
    // Application rows the statement changed, which is `changes()` and not the
    // rows it RETURNED. Every statement `executeCrud` emits is a bare
    // INSERT/UPDATE/DELETE with no RETURNING clause, so `rows.length` is zero
    // on the application-SQL path that carries all of production's writes, and
    // the ratio this denominator exists to report divided by zero. `changes()`
    // is already the affected-row count this method returns to the executor,
    // and SQLite excludes trigger and referential-action rows from it, which is
    // exactly the "logical" half of the billable/logical pair.
    if (rowMutation) this.writeBudget.recordLogical(changes)
    if (mutation && !rowMutation) this.cdc.invalidateSchema()
    const captured = track || (this.cdc.active && rowMutation) ? this.cdc.drain() : []
    try {
      const attribution = transactionTelemetry?.attribution
      if (attribution && captured.length > 0) {
        attribution.noteTriggerCaptures(captured.length)
        for (const change of captured) {
          const visibility =
            change.publish === false
              ? 'private'
              : (this.cdc.tableVisibility(change.physicalTableName) ?? 'synced')
          attribution.recordLogicalCapture({
            table: change.physicalTableName,
            op: change.op,
            visibility,
            publish: change.publish !== false && visibility === 'synced',
          })
        }
      } else if (attribution && rowMutation && changes > 0) {
        attribution.recordUncapturedLogical(changes)
      }
    } catch {}
    for (const change of captured) {
      this.appendCapturedChange(
        change,
        transactionID || track?.transactionID,
        !snapshotted.has(change.physicalTableName)
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
        track.physicalTableName || stripPublicPrefix(track.tableName)
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
    const result = !track
      ? { rows, columns, changes, capturedChanges: publishedCaptured }
      : {
          rows: track.returnRows ? rows : [],
          columns: track.returnRows ? columns : [],
          changes,
          affectedRows: rows.length,
          capturedChanges:
            publishedCaptured ||
            (capturesTrackedTable || track.publish === false ? 0 : rows.length),
        }
    if (transactionTelemetry) this.recordSqlTelemetry(transactionTelemetry, result)
    if (queryTelemetry && queryTelemetry !== transactionTelemetry) {
      this.recordSqlTelemetry(queryTelemetry, result)
    }
    return result
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
    const table = stripPublicPrefix(track.tableName)
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
      'SELECT watermark, table_name, op, row_data, old_data, created_at FROM _zero_changes WHERE watermark > ? ORDER BY watermark' +
      (limit === undefined ? '' : ' LIMIT ?')
    const params = limit === undefined ? [watermark] : [watermark, limit]
    return this.sql
      .exec(statement, ...params)
      .toArray()
      .map((row: any) => {
        const tableName = String(row.table_name)
        const rowData = row.row_data ? JSON.parse(String(row.row_data)) : null
        const oldData = row.old_data ? JSON.parse(String(row.old_data)) : null
        return {
          watermark: Number(row.watermark),
          // Existing databases store unixepoch() seconds. Accept millisecond
          // values too so a future precision migration is wire-compatible.
          commitTimeMs:
            Number(row.created_at) < 10_000_000_000
              ? Number(row.created_at) * 1_000
              : Number(row.created_at),
          tableName,
          op: String(row.op),
          rowData: rowData ? this.normalizeRow(tableName, rowData) : null,
          oldData: oldData ? this.normalizeRow(tableName, oldData) : null,
        }
      })
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
    const schemaTableName = stripPublicPrefix(tableName)
    const tableSchemas = (this.tableSchemas ??= new Map())
    // `null` is a cached miss. Only `undefined` means "not looked up yet", so
    // an unmodeled table costs one lookup per cache generation instead of one
    // per row: normalizeRow asks for every change row's schema, and the change
    // feed always carries tables that are unmodeled by design (the mutation
    // cursor sources `_zsync_clients` and `<app>_0.clients|mutations`, which
    // applications must never put in their Zero schema). Without this, a pull
    // whose rows are unmodeled runs three SQLite statements and one
    // thrown-and-caught `.one()` error per row, and measures ~3x slower than
    // the identical pull over a modeled table.
    const cached = tableSchemas.get(schemaTableName)
    if (cached !== undefined) return cached ?? undefined
    // Every path that can change what this returns already discards the whole
    // map: ensureSchemaTables sets entries directly, and migration, rollback,
    // batch DDL, and recovery all call invalidateSchemaCaches().
    try {
      this.ensureSchemaMetadataTable()
      // toArray()[0] rather than one(), which throws when a table is unmodeled
      const row = this.sql
        .exec(
          'SELECT schema_json FROM _zero_schema_tables WHERE name = ?',
          schemaTableName
        )
        .toArray()[0]
      if (!row?.schema_json) {
        tableSchemas.set(schemaTableName, null)
        return undefined
      }
      const schema = JSON.parse(String(row.schema_json)) as SchemaTable
      tableSchemas.set(schemaTableName, schema)
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
    const schema = this.schemaForTable(tn)
    const type =
      schema?.columns?.[column]?.type ??
      Object.values(schema?.columns ?? {}).find(
        (candidate) => candidate.serverName === column
      )?.type
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
      const type =
        schema?.columns?.[key]?.type ??
        Object.values(schema?.columns ?? {}).find(
          (candidate) => candidate.serverName === key
        )?.type
      const value = row[key]
      if (value === null || value === undefined) {
        normalized[key] = null
      } else if (type === 'boolean') {
        normalized[key] =
          value === true || value === 1 || value === '1' || value === 'true'
      } else if (type === 'number') {
        // timestamp/timestamptz columns are declared `number` in the zero
        // schema but may be stored as timestamp text (for example
        // "2026-07-11 13:34:46.000+00"). Coercing
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
