import { AsyncLocalStorage } from 'node:async_hooks'

import { stripPublicPrefix } from '../do-sql-tracking.js'
import { createApplicationSqlClient } from './application-sql.js'
import {
  createNamespaceBackupManager,
  type NamespaceBackupBucket,
  type NamespaceBackupManager,
  type NamespaceBackupOptions,
  type NamespaceBackupStatement,
} from './namespace-backup.js'
import { ZeroDO as OrezZeroDO } from './worker.js'

import type { NamespaceRoutingOptions } from '../worker/cf-do-shim.js'
import type {
  ApplicationSqlClient,
  ApplicationSqlDurableObjectNamespace,
  ApplicationSqlRpc,
} from './application-sql.js'
import type { Schema } from '@rocicorp/zero'

type MaybePromise<Value> = Value | Promise<Value>
type JsonRecord = Record<string, unknown>

export interface OrezSchemaMigrationOptions {
  client?: ApplicationSqlClient
  force?: boolean
  instance?: string
  registrationOnly?: boolean
  schemaOnly?: boolean
}

/**
 * The deploy-generated module is the sole schema authority for the Lite
 * worker. In particular, feed projections come from `schema`; applications do
 * not maintain a second list of Rust-visible columns.
 */
export interface OrezAppSchemaDescriptor<S extends Schema = Schema> {
  version: string
  schema: S
  publicTables: readonly {
    /** Physical SQLite table registered with the Orez DO. */
    table: string
    /** Published name recorded in the Orez change feed. */
    publicTable: string
  }[]
  migrate(options?: OrezSchemaMigrationOptions): Promise<unknown>
}

export interface OrezDurableObjectId {
  toString(): string
}

export interface OrezDataWorkerStub extends ApplicationSqlRpc {
  fetch(request: Request): Promise<Response>
  orezApplicationSchemaStatus(version: string): Promise<OrezSchemaStatus>
  orezRunApplicationSchema(
    version: string,
    instance: string,
    options?: { force?: boolean }
  ): Promise<unknown>
  orezStartApplicationSchema(version: string, instance: string): Promise<OrezSchemaStatus>
  orezImportBatch(statements: readonly NamespaceBackupStatement[]): Promise<void>
  orezBeginRestore(): Promise<void>
}

export interface OrezDataWorkerNamespace extends ApplicationSqlDurableObjectNamespace {
  idFromName(name: string): OrezDurableObjectId
  get(id: unknown): OrezDataWorkerStub
}

export interface OrezDataWorkerEnv {
  CF_VERSION?: { id?: string }
  ZERO_SQL_DO: OrezDataWorkerNamespace
  OREZ_DO_WRITE_BUDGET_ROWS?: string
  OREZ_DO_WRITE_BUDGET_WINDOW_MS?: string
  OREZ_DO_WRITE_BUDGET_ADMIN_TOKEN?: string
  OREZ_DO_WRITE_BUDGET_DISABLED?: string
  [binding: string]: unknown
}

export interface OrezExecutionContext {
  waitUntil(promise: Promise<unknown>): void
}

export interface OrezScheduledEvent {
  cron?: string
  scheduledTime?: number
}

export interface OrezErrorContext<Env extends OrezDataWorkerEnv> {
  env: Env
  executionContext: OrezExecutionContext
  route: string
  request?: Request
  event?: OrezScheduledEvent
}

export interface OrezRequestContext<Env extends OrezDataWorkerEnv> {
  env: Env
  executionContext: OrezExecutionContext
  instance: string
  request: Request
  url: URL
  applicationSql(): ApplicationSqlClient
  ensureSchema(options?: { force?: boolean }): Promise<unknown>
}

export interface OrezBackupConfig<Env extends OrezDataWorkerEnv> extends Partial<
  Pick<
    NamespaceBackupOptions<Env>,
    | 'acceptedFormats'
    | 'chunkTargetBytes'
    | 'controlPlaneNamespace'
    | 'excludedTables'
    | 'keep'
    | 'keepControlPlane'
    | 'partBytes'
    | 'prefix'
    | 'runBudgetMs'
  >
> {
  bucket(env: Env): NamespaceBackupBucket
  /**
   * Return application namespace names from the control-plane database.
   * Orez supplies the SQL client and canonicalizes/deduplicates the result.
   */
  inventory(sql: ApplicationSqlClient, env: Env): MaybePromise<readonly string[]>
  authorize(
    request: Request,
    env: Env,
    action: 'export' | 'restore'
  ): MaybePromise<boolean>
  format?: string
}

export interface OrezDataWorkerOptions<
  Env extends OrezDataWorkerEnv,
  S extends Schema = Schema,
> {
  /** Lowercase app identifier used only for generated runtime globals/logs. */
  name: string
  /** Deploy-generated application schema descriptor. */
  schema: OrezAppSchemaDescriptor<S>
  /**
   * Prefix for durable control tables. Existing deployments can retain their
   * live names (for example `_soot`); new deployments default to `_orez`.
   */
  tablePrefix?: `_${string}`
  namespace?: NamespaceRoutingOptions
  backup?: OrezBackupConfig<Env>
  migrations?: {
    /** Consecutive failures for a schema version before automatic retries stop. */
    suspendAfterFailures?: number
  }
  /** Per-object rolling row-write policy. Environment variables remain supported. */
  writeBudget?: {
    rows?: number
    windowMs?: number
  }
  setup?(context: OrezRequestContext<Env>): MaybePromise<void>
  routes?(context: OrezRequestContext<Env>): MaybePromise<Response | null | undefined>
  onError?(error: unknown, context: OrezErrorContext<Env>): MaybePromise<void>
  /**
   * Exact cron handlers owned by the application. An unclaimed cron runs the
   * Orez backup sweep when backups are configured.
   */
  scheduled?: Readonly<
    Record<
      string,
      (
        event: OrezScheduledEvent,
        env: Env,
        ctx: OrezExecutionContext
      ) => MaybePromise<void>
    >
  >
}

export interface OrezSchemaStatus {
  ready: boolean
  running: boolean
  restoring: boolean
  attemptCount: number
  lastError: string | null
}

export interface OrezResolvedDataRequest {
  instance: string
  pathname: string
  url: URL
}

export type OrezDataWorkerDurableObject = OrezZeroDO & {
  orezApplicationSchemaStatus(version: string): OrezSchemaStatus
  orezRunApplicationSchema(
    version: string,
    instance: string,
    options?: { force?: boolean }
  ): Promise<unknown>
  orezStartApplicationSchema(version: string, instance: string): OrezSchemaStatus
  orezImportBatch(statements: readonly NamespaceBackupStatement[]): Promise<void>
  orezBeginRestore(): Promise<void>
}

export interface OrezDataWorkerResult<Env extends OrezDataWorkerEnv> {
  ZeroSqlDO: new (ctx: any, env: Env) => OrezDataWorkerDurableObject
  ZeroDO: new (ctx: any, env: Env) => OrezDataWorkerDurableObject
  fetch(request: Request, env: Env, ctx: OrezExecutionContext): Promise<Response>
  scheduled(event: OrezScheduledEvent, env: Env, ctx: OrezExecutionContext): Promise<void>
  applicationSqlClient(
    env: Env,
    namespace?: string,
    signal?: AbortSignal
  ): ApplicationSqlClient
  ensureNamespaceSchema(
    env: Env,
    namespace?: string,
    options?: { force?: boolean }
  ): Promise<unknown>
  backupManager: NamespaceBackupManager<Env> | null
}

const STANDARD_DATA_PATHS = [
  '/_orez/backup/restore',
  '/_orez/backup/export',
  '/_orez/write-budget/trip',
  '/_orez/write-budget/reopen',
  '/_orez/write-budget',
  '/_orez/schema-status',
  '/_orez/schema/migrate',
  '/snapshot',
  '/changes',
] as const

function isRecord(value: unknown): value is JsonRecord {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function validScopedNamespace(value: string, options: NamespaceRoutingOptions): boolean {
  const scopes = options.scopes ?? ['proj', 'test']
  return scopes.some((scope) => {
    if (!scope || value.slice(0, scope.length + 1) !== `${scope}-`) return false
    const id = value.slice(scope.length + 1)
    return id.length >= 1 && id.length <= 64 && /^[A-Za-z0-9_-]+$/.test(id)
  })
}

/**
 * Accept a user-facing namespace or an already-canonical DO instance name.
 * Every factory surface uses this function, so headers, path mounts, backups,
 * SQL clients, and migration RPCs cannot disagree about object identity.
 */
export function canonicalOrezNamespace(
  namespace: string | null | undefined,
  options: NamespaceRoutingOptions = {}
): string | null {
  const value = String(namespace ?? '')
  if (
    value === '' ||
    value === 'singleton' ||
    (options.controlPlaneNamespaces ?? []).includes(value)
  ) {
    return 'singleton'
  }
  const raw = value.startsWith('ns:') ? value.slice(3) : value
  return validScopedNamespace(raw, options) ? `ns:${raw}` : null
}

function pathMountedRoute(
  pathname: string,
  options: NamespaceRoutingOptions
): { instance: string; pathname: string } | null {
  for (const route of STANDARD_DATA_PATHS) {
    if (!pathname.endsWith(route) || pathname === route) continue
    const prefix = pathname.slice(0, -route.length)
    if (!prefix.startsWith('/') || prefix.indexOf('/', 1) !== -1) continue
    let namespace: string
    try {
      namespace = decodeURIComponent(prefix.slice(1))
    } catch {
      return null
    }
    const instance = canonicalOrezNamespace(namespace, options)
    if (instance) return { instance, pathname: route }
  }
  return null
}

/**
 * Resolve both standard root mounts (`/changes?ns=proj-a`) and Rust binding
 * mounts (`/proj-a/changes`) to one canonical namespace and one route.
 */
export function resolveOrezDataRequest(
  request: Request,
  options: NamespaceRoutingOptions = {}
): OrezResolvedDataRequest | null {
  const url = new URL(request.url)
  const mounted = pathMountedRoute(url.pathname, options)
  if (mounted) {
    url.pathname = mounted.pathname
    return { ...mounted, url }
  }
  const namespace =
    request.headers.get(options.nsHeader ?? 'x-orez-ns') ||
    url.searchParams.get('ns') ||
    ''
  const instance = canonicalOrezNamespace(namespace, options)
  return instance ? { instance, pathname: url.pathname, url } : null
}

type ProjectedTable = {
  name: string
  columns: ReadonlyMap<string, string>
}

function schemaFeedTables(
  descriptor: OrezAppSchemaDescriptor
): ReadonlyMap<string, ProjectedTable> {
  const result = new Map<string, ProjectedTable>()
  const schemaTables = Object.entries(descriptor.schema.tables)
  for (const published of descriptor.publicTables) {
    const publicName = stripPublicPrefix(published.publicTable)
    const matched = schemaTables.find(([logicalName, table]) => {
      const physicalName = table.serverName ?? table.name
      return (
        physicalName === published.table ||
        table.name === publicName ||
        logicalName === publicName
      )
    })
    if (!matched) {
      throw new TypeError(
        `public table ${JSON.stringify(published.publicTable)} (${JSON.stringify(
          published.table
        )}) is absent from the Zero schema`
      )
    }
    const [logicalName, table] = matched
    const columns = new Map<string, string>()
    for (const [columnName, column] of Object.entries(table.columns)) {
      columns.set(column.serverName ?? columnName, columnName)
      columns.set(columnName, columnName)
    }
    const projection = { name: logicalName, columns }
    for (const alias of [
      published.table,
      published.publicTable,
      publicName,
      table.name,
      table.serverName,
      logicalName,
    ]) {
      if (alias) result.set(stripPublicPrefix(alias), projection)
    }
  }
  return result
}

function projectedRow(table: ProjectedTable, row: unknown): unknown {
  if (!isRecord(row)) return row
  const projected: JsonRecord = {}
  for (const [source, target] of table.columns) {
    if (source in row && !(target in projected)) projected[target] = row[source]
  }
  return projected
}

function isInternalCursorTable(tableName: string): boolean {
  const name = stripPublicPrefix(tableName)
  return (
    name === '_zsync_clients' || /^[A-Za-z0-9_]+_0\.(?:clients|mutations)$/.test(name)
  )
}

/**
 * Convert the authoritative SQLite response into the actual public Zero
 * schema. Unknown/unpublished tables and columns are removed. Internal
 * mutation-cursor sources are projected to `syncCursor` before table lookup,
 * so applications never add them to their schema or a column allow-list.
 */
export function projectOrezFeedBody(
  descriptor: OrezAppSchemaDescriptor,
  value: unknown,
  snapshotTable?: string | null
): unknown {
  return projectFeedBody(schemaFeedTables(descriptor), value, snapshotTable)
}

function projectFeedBody(
  feedTables: ReadonlyMap<string, ProjectedTable>,
  value: unknown,
  snapshotTable?: string | null
): unknown {
  if (!isRecord(value)) return value
  if (feedTables.size === 0) {
    throw new Error('Orez data feeds require at least one published Zero table')
  }
  const projected: JsonRecord = { ...value }

  if (snapshotTable && Array.isArray(value.rows)) {
    if (snapshotTable === 'syncCursor') {
      projected.rows = [{ id: 'zero-http', watermark: Number(value.watermark ?? 0) }]
    } else {
      const table = feedTables.get(stripPublicPrefix(snapshotTable))
      if (!table) {
        throw new Error(
          `snapshot table ${JSON.stringify(snapshotTable)} is not published`
        )
      }
      projected.rows = value.rows.map((row) => projectedRow(table, row))
    }
  }

  if (isRecord(value.tables)) {
    const tables: Record<string, unknown> = {}
    for (const [rawName, rows] of Object.entries(value.tables)) {
      const table = feedTables.get(stripPublicPrefix(rawName))
      if (!table) continue
      tables[table.name] = Array.isArray(rows)
        ? rows.map((row) => projectedRow(table, row))
        : rows
    }
    tables.syncCursor = [{ id: 'zero-http', watermark: Number(value.watermark ?? 0) }]
    projected.tables = tables
  }

  if (Array.isArray(value.changes)) {
    const changes: unknown[] = []
    for (const rawChange of value.changes) {
      if (!isRecord(rawChange)) continue
      const rawName = stripPublicPrefix(String(rawChange.tableName ?? ''))
      if (isInternalCursorTable(rawName)) {
        changes.push({
          watermark: rawChange.watermark,
          tableName: 'syncCursor',
          op: 'INSERT',
          rowData: { id: 'zero-http', watermark: rawChange.watermark },
          oldData: null,
        })
        continue
      }
      const table = feedTables.get(rawName)
      if (!table) continue
      changes.push({
        ...rawChange,
        tableName: table.name,
        rowData: projectedRow(table, rawChange.rowData),
        oldData: projectedRow(table, rawChange.oldData),
      })
    }
    projected.changes = changes
  }

  return projected
}

function requireAppName(name: string): void {
  if (!/^[a-z][a-z0-9]*$/.test(name)) {
    throw new TypeError(
      `Orez data worker name must be a lowercase identifier, got ${JSON.stringify(name)}`
    )
  }
}

/**
 * Build the complete application-data Worker and the concrete SQLite Durable
 * Object class Cloudflare requires. Consumers export the returned class; they
 * never subclass Orez internals.
 */
export function createOrezDataWorker<
  Env extends OrezDataWorkerEnv,
  S extends Schema = Schema,
>(options: OrezDataWorkerOptions<Env, S>): OrezDataWorkerResult<Env> {
  requireAppName(options.name)
  const tablePrefix = options.tablePrefix ?? '_orez'
  if (!/^_[a-z][a-z0-9_]*$/.test(tablePrefix)) {
    throw new TypeError(
      `Orez tablePrefix must be an underscore-prefixed lowercase identifier, got ${JSON.stringify(
        tablePrefix
      )}`
    )
  }
  // Validate once and retain the immutable projection for the worker lifetime.
  const feedTables = schemaFeedTables(options.schema)
  const namespaceOptions: NamespaceRoutingOptions = {
    ...options.namespace,
    nsHeader: options.namespace?.nsHeader ?? 'x-orez-ns',
  }
  const suspendAfterFailures = options.migrations?.suspendAfterFailures ?? 20
  if (!Number.isSafeInteger(suspendAfterFailures) || suspendAfterFailures < 1) {
    throw new TypeError('migrations.suspendAfterFailures must be a positive safe integer')
  }
  for (const [name, value] of Object.entries(options.writeBudget ?? {})) {
    if (!Number.isSafeInteger(value) || Number(value) < 1) {
      throw new TypeError(`writeBudget.${name} must be a positive safe integer`)
    }
  }

  const requestSignals = new AsyncLocalStorage<AbortSignal>()
  const readyTable = `${tablePrefix}_application_schema`
  const attemptTable = `${tablePrefix}_application_schema_attempt`
  const backupMarkerTable = `${tablePrefix}_backup_meta`
  const restoreTable = `${tablePrefix}_restore`
  const applicationClientGlobal = `__${options.name}_cf_application_sql_client`
  const bucketGlobal = `__${options.name}_cf_r2_bucket`

  const canonical = (namespace?: string): string => {
    const instance = canonicalOrezNamespace(namespace, namespaceOptions)
    if (!instance) throw new TypeError('invalid application SQLite namespace')
    return instance
  }

  const applicationSqlClient = (
    env: Env,
    namespace = 'singleton',
    signal = requestSignals.getStore()
  ): ApplicationSqlClient =>
    createApplicationSqlClient(env.ZERO_SQL_DO, canonical(namespace), { signal })

  const installRuntimeGlobals = (env: Env): void => {
    const globals = globalThis as typeof globalThis & Record<string, unknown>
    globals[applicationClientGlobal] = (namespace = 'singleton') =>
      applicationSqlClient(env, String(namespace))
    if (options.backup) globals[bucketGlobal] = options.backup.bucket(env)
  }

  class ZeroSqlDO extends OrezZeroDO {
    private readonly orezStorage: any
    private readonly orezWorkerVersion: string
    private orezSchemaRunVersion: string | null = null
    private orezSchemaRun: Promise<unknown> | null = null
    private orezReadyVersion: string | null = null

    constructor(ctx: any, env: Env) {
      // The base DO's private Env includes its historical binding spelling;
      // its constructor only consumes the write-budget environment fields.
      super(
        ctx,
        (options.writeBudget
          ? {
              ...env,
              ...(options.writeBudget.rows
                ? { OREZ_DO_WRITE_BUDGET_ROWS: String(options.writeBudget.rows) }
                : null),
              ...(options.writeBudget.windowMs
                ? {
                    OREZ_DO_WRITE_BUDGET_WINDOW_MS: String(options.writeBudget.windowMs),
                  }
                : null),
            }
          : env) as never
      )
      this.orezStorage = ctx.storage
      this.orezWorkerVersion = env.CF_VERSION?.id ?? ''
      ctx.storage.sql.exec(
        `CREATE TABLE IF NOT EXISTS ${readyTable} (id INTEGER PRIMARY KEY CHECK (id = 1), version TEXT NOT NULL)`
      )
      ctx.storage.sql.exec(
        `CREATE TABLE IF NOT EXISTS ${attemptTable} (id INTEGER PRIMARY KEY CHECK (id = 1), version TEXT NOT NULL, attempt_count INTEGER NOT NULL, last_error TEXT)`
      )
      ctx.storage.sql.exec(
        `CREATE TABLE IF NOT EXISTS ${restoreTable} (id INTEGER PRIMARY KEY CHECK (id = 1), started_at INTEGER NOT NULL)`
      )
    }

    protected applicationSqlDidCommit(_changed: boolean, mutated: boolean): void {
      if (mutated) this.orezBumpBackupMarker()
    }

    async orezImportBatch(
      statements: readonly NamespaceBackupStatement[]
    ): Promise<void> {
      if (statements.length === 0) return
      await this.runApplicationTransaction(
        () => {
          throw new Error('backup imports do not compile query ASTs')
        },
        async (tx) => {
          for (const statement of statements) {
            await tx.exec(statement.sql, statement.params ?? [])
          }
        }
      )
    }

    async orezRunApplicationSchema(
      schemaVersion: string,
      instance: string,
      runOptions: { force?: boolean } = {}
    ): Promise<unknown> {
      const client = this.applicationSqlLocalClient(instance)
      const finishingRestore = this.orezRestoreInProgress()
      if (finishingRestore) {
        if (!runOptions.force) {
          throw new Error(
            'namespace restore is in progress; retry the restore or force schema reconciliation'
          )
        }
      }
      if (this.orezSchemaRunVersion === schemaVersion && this.orezSchemaRun) {
        return this.orezSchemaRun
      }
      if (!runOptions.force && this.orezApplicationSchemaReady(schemaVersion)) {
        return
      }
      if (!runOptions.force) {
        const attemptKey = this.orezSchemaAttemptKey(schemaVersion)
        const attempt = this.orezStorage.sql
          .exec(
            `SELECT attempt_count, last_error FROM ${attemptTable} WHERE id = 1 AND version = ?`,
            attemptKey
          )
          .toArray()[0]
        if (
          attempt?.last_error &&
          Number(attempt.attempt_count) >= suspendAfterFailures
        ) {
          throw new Error(
            `schema reconcile suspended after ${attempt.attempt_count} failed attempts for schema ${schemaVersion}; last error: ${attempt.last_error} — deploy a new worker or force a retry`
          )
        }
      }
      const attemptKey = this.orezSchemaAttemptKey(schemaVersion)
      this.orezStorage.sql.exec(
        `INSERT INTO ${attemptTable} (id, version, attempt_count, last_error) VALUES (1, ?, 1, NULL) ON CONFLICT (id) DO UPDATE SET version = excluded.version, attempt_count = CASE WHEN version = excluded.version THEN attempt_count + 1 ELSE 1 END, last_error = CASE WHEN version = excluded.version THEN last_error ELSE NULL END`,
        attemptKey
      )
      this.orezSchemaRunVersion = schemaVersion
      this.orezSchemaRun = (async () => {
        try {
          this.orezBeginApplicationSchemaReconcile()
          const result = await options.schema.migrate({ client, instance })
          if (finishingRestore) {
            this.orezStorage.sql.exec(`DELETE FROM ${restoreTable} WHERE id = 1`)
          }
          this.orezMarkApplicationSchemaReady(schemaVersion)
          this.orezStorage.sql.exec(
            `UPDATE ${attemptTable} SET last_error = NULL WHERE id = 1 AND version = ?`,
            attemptKey
          )
          return result
        } catch (error) {
          this.orezStorage.sql.exec(
            `UPDATE ${attemptTable} SET last_error = ? WHERE id = 1 AND version = ?`,
            errorMessage(error).slice(0, 4_000),
            attemptKey
          )
          throw error
        }
      })()
      this.orezSchemaRun.catch(() => {
        this.orezSchemaRun = null
        this.orezSchemaRunVersion = null
      })
      return this.orezSchemaRun
    }

    orezStartApplicationSchema(
      schemaVersion: string,
      instance: string
    ): OrezSchemaStatus {
      const status = this.orezApplicationSchemaStatus(schemaVersion)
      if (status.ready || status.running) return status
      this.orezRunApplicationSchema(schemaVersion, instance).catch(() => {})
      return this.orezApplicationSchemaStatus(schemaVersion)
    }

    orezApplicationSchemaStatus(schemaVersion: string): OrezSchemaStatus {
      const ready = this.orezApplicationSchemaReady(schemaVersion)
      const attemptKey = this.orezSchemaAttemptKey(schemaVersion)
      const attempt = this.orezStorage.sql
        .exec(
          `SELECT attempt_count, last_error FROM ${attemptTable} WHERE id = 1 AND version = ?`,
          attemptKey
        )
        .toArray()[0]
      return {
        ready,
        running:
          !ready &&
          this.orezSchemaRunVersion === schemaVersion &&
          this.orezSchemaRun !== null,
        restoring: this.orezRestoreInProgress(),
        attemptCount: Number(attempt?.attempt_count ?? 0),
        lastError: attempt?.last_error ? String(attempt.last_error) : null,
      }
    }

    orezApplicationSchemaReady(schemaVersion: string): boolean {
      if (this.orezReadyVersion === schemaVersion) return true
      const stored = this.orezStorage.sql
        .exec(`SELECT version FROM ${readyTable} WHERE id = 1`)
        .toArray()[0]
      if (stored?.version !== schemaVersion) return false
      this.orezReadyVersion = schemaVersion
      return true
    }

    orezBeginApplicationSchemaReconcile(): void {
      this.orezReadyVersion = null
      this.orezStorage.sql.exec(`DELETE FROM ${readyTable} WHERE id = 1`)
    }

    orezMarkApplicationSchemaReady(schemaVersion: string): void {
      this.orezStorage.sql.exec(
        `INSERT INTO ${readyTable} (id, version) VALUES (1, ?) ON CONFLICT (id) DO UPDATE SET version = excluded.version`,
        schemaVersion
      )
      this.orezReadyVersion = schemaVersion
    }

    private orezBumpBackupMarker(): void {
      this.orezStorage.sql.exec(
        `CREATE TABLE IF NOT EXISTS ${backupMarkerTable} (id INTEGER PRIMARY KEY CHECK (id = 1), write_seq INTEGER NOT NULL DEFAULT 0)`
      )
      this.orezStorage.sql.exec(
        `INSERT INTO ${backupMarkerTable} (id, write_seq) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET write_seq = write_seq + 1`
      )
    }

    async orezBeginRestore(): Promise<void> {
      this.orezBeginApplicationSchemaReconcile()
      this.orezStorage.sql.exec(
        `INSERT INTO ${restoreTable} (id, started_at) VALUES (1, ?) ON CONFLICT (id) DO UPDATE SET started_at = excluded.started_at`,
        Date.now()
      )
    }

    private orezRestoreInProgress(): boolean {
      return Boolean(
        this.orezStorage.sql
          .exec(`SELECT id FROM ${restoreTable} WHERE id = 1`)
          .toArray()[0]
      )
    }

    private orezSchemaAttemptKey(schemaVersion: string): string {
      return this.orezWorkerVersion
        ? `${schemaVersion}@${this.orezWorkerVersion}`
        : schemaVersion
    }
  }

  const ensureNamespaceSchema = (
    env: Env,
    namespace = 'singleton',
    runOptions: { force?: boolean } = {}
  ): Promise<unknown> => {
    installRuntimeGlobals(env)
    const instance = canonical(namespace)
    const id = env.ZERO_SQL_DO.idFromName(instance)
    return env.ZERO_SQL_DO.get(id).orezRunApplicationSchema(
      options.schema.version,
      instance,
      runOptions
    )
  }

  const queryNamespace = async (
    env: Env,
    namespace: string,
    sql: string,
    params: readonly unknown[] = []
  ): Promise<Record<string, any>[]> => {
    const client = applicationSqlClient(env, namespace)
    if (/^\s*(?:SELECT|PRAGMA)\b/i.test(sql)) return client.query(sql, params)
    await client.exec(sql, params)
    return []
  }

  const backupManager = options.backup
    ? createNamespaceBackupManager<Env>({
        format: options.backup.format ?? 'orez-backup-v1',
        markerTable: backupMarkerTable,
        files: options.backup.bucket,
        query: queryNamespace,
        batch: async (env, namespace, statements) => {
          const instance = canonical(namespace)
          const id = env.ZERO_SQL_DO.idFromName(instance)
          await env.ZERO_SQL_DO.get(id).orezImportBatch(statements)
        },
        listNamespaces: async (env) => {
          const listed = await options.backup!.inventory(
            applicationSqlClient(env, 'singleton'),
            env
          )
          const namespaces = new Set<string>(['singleton'])
          for (const namespace of listed) namespaces.add(canonical(namespace))
          return [...namespaces]
        },
        logPrefix: `[${options.name}]`,
        excludedTables: [
          readyTable,
          attemptTable,
          backupMarkerTable,
          restoreTable,
          ...(options.backup.excludedTables ?? []),
        ],
        ...Object.fromEntries(
          Object.entries(options.backup).filter(([key]) =>
            [
              'chunkTargetBytes',
              'acceptedFormats',
              'controlPlaneNamespace',
              'keep',
              'keepControlPlane',
              'partBytes',
              'prefix',
              'runBudgetMs',
            ].includes(key)
          )
        ),
      })
    : null

  const feedResponse = async (
    request: Request,
    env: Env,
    resolved: OrezResolvedDataRequest
  ): Promise<Response> => {
    const isApplicationFeed =
      resolved.pathname === '/changes' || resolved.pathname === '/snapshot'
    if (isApplicationFeed && options.schema.publicTables.length === 0) {
      return new Response('Orez data feeds require a published application schema', {
        status: 503,
      })
    }
    const id = env.ZERO_SQL_DO.idFromName(resolved.instance)
    const stub = env.ZERO_SQL_DO.get(id)
    if (isApplicationFeed) {
      const status = await stub.orezApplicationSchemaStatus(options.schema.version)
      if (!status.ready) {
        const pending = await stub.orezStartApplicationSchema(
          options.schema.version,
          resolved.instance
        )
        if (!pending.ready) {
          return new Response(
            pending.lastError
              ? `${pending.lastError} (schema retry attempt ${pending.attemptCount} is in progress)`
              : 'namespace schema migration in progress',
            { status: 503, headers: { 'retry-after': '10' } }
          )
        }
      }
    }
    const headers = new Headers(request.headers)
    headers.set('x-orez-do-instance', resolved.instance)
    const snapshotTable =
      resolved.pathname === '/snapshot' ? resolved.url.searchParams.get('table') : null
    if (snapshotTable === 'syncCursor') {
      const headUrl = new URL(resolved.url)
      headUrl.pathname = '/changes'
      headUrl.search = ''
      headUrl.searchParams.set('watermark', String(Number.MAX_SAFE_INTEGER))
      const headResponse = await stub.fetch(
        new Request(headUrl.toString(), {
          headers,
          signal: request.signal,
        })
      )
      if (!headResponse.ok) return headResponse
      const head = await headResponse.json()
      const watermark = isRecord(head) ? Number(head.watermark ?? 0) : 0
      return Response.json({
        watermark,
        rows: [{ id: 'zero-http', watermark }],
        nextCursor: null,
      })
    }
    const forward = new Request(resolved.url.toString(), request)
    for (const [key, value] of headers) forward.headers.set(key, value)
    const response = await stub.fetch(forward)
    if (
      !response.ok ||
      (resolved.pathname !== '/changes' && resolved.pathname !== '/snapshot')
    ) {
      return response
    }
    const body = projectFeedBody(feedTables, await response.json(), snapshotTable)
    const responseHeaders = new Headers(response.headers)
    responseHeaders.delete('content-length')
    responseHeaders.set('content-type', 'application/json')
    return new Response(JSON.stringify(body), {
      status: response.status,
      statusText: response.statusText,
      headers: responseHeaders,
    })
  }

  const reportError = (error: unknown, context: OrezErrorContext<Env>): void => {
    if (!options.onError) return
    try {
      context.executionContext.waitUntil(
        Promise.resolve(options.onError(error, context)).catch(() => {})
      )
    } catch {
      // Observability must never replace the request's actual outcome.
    }
  }

  const worker = {
    async fetch(
      request: Request,
      env: Env,
      executionContext: OrezExecutionContext
    ): Promise<Response> {
      const route = new URL(request.url).pathname
      try {
        const response = await requestSignals.run(request.signal, async () => {
          installRuntimeGlobals(env)
          const resolved = resolveOrezDataRequest(request, namespaceOptions)
          if (!resolved) return new Response('invalid namespace', { status: 400 })
          const routedHeaders = new Headers(request.headers)
          routedHeaders.set(
            namespaceOptions.nsHeader!,
            resolved.instance === 'singleton'
              ? 'singleton'
              : resolved.instance.slice('ns:'.length)
          )
          const routedRequest = new Request(resolved.url.toString(), {
            method: request.method,
            headers: routedHeaders,
            body:
              request.method === 'GET' || request.method === 'HEAD'
                ? undefined
                : request.body,
            redirect: request.redirect,
            signal: request.signal,
          })
          const context: OrezRequestContext<Env> = {
            env,
            executionContext,
            instance: resolved.instance,
            request: routedRequest,
            url: resolved.url,
            applicationSql: () =>
              applicationSqlClient(env, resolved.instance, request.signal),
            ensureSchema: (runOptions) =>
              ensureNamespaceSchema(env, resolved.instance, runOptions),
          }
          await options.setup?.(context)
          const custom = await options.routes?.(context)
          if (custom) return custom

          if (resolved.pathname === '/_orez/schema-status') {
            const id = env.ZERO_SQL_DO.idFromName(resolved.instance)
            const status = await env.ZERO_SQL_DO.get(id).orezApplicationSchemaStatus(
              options.schema.version
            )
            return Response.json({
              ns: resolved.instance,
              objectId: id.toString(),
              schemaVersion: options.schema.version,
              ...status,
            })
          }
          if (
            resolved.pathname === '/_orez/schema/migrate' &&
            request.method === 'POST'
          ) {
            await context.ensureSchema({
              force: resolved.url.searchParams.get('force') === '1',
            })
            return Response.json({
              ok: true,
              ns: resolved.instance,
              schemaVersion: options.schema.version,
            })
          }
          if (
            resolved.pathname === '/changes' ||
            resolved.pathname === '/snapshot' ||
            resolved.pathname === '/_orez/write-budget' ||
            resolved.pathname === '/_orez/write-budget/trip' ||
            resolved.pathname === '/_orez/write-budget/reopen'
          ) {
            return feedResponse(request, env, resolved)
          }
          if (
            backupManager &&
            (resolved.pathname === '/_orez/backup/export' ||
              resolved.pathname === '/_orez/backup/restore')
          ) {
            const action =
              resolved.pathname === '/_orez/backup/export' ? 'export' : 'restore'
            if (!(await options.backup!.authorize(request, env, action))) {
              return new Response('forbidden', { status: 403 })
            }
            try {
              if (action === 'export') {
                const summary = await backupManager.exportNamespace(
                  env,
                  resolved.instance
                )
                await backupManager.pruneBackups(env, resolved.instance)
                return Response.json({ ok: true, ...summary })
              }
              if (request.method !== 'POST') {
                return new Response('restore requires POST', { status: 405 })
              }
              const confirmation = resolved.url.searchParams.get('confirm')
              if (!confirmation || canonical(confirmation) !== resolved.instance) {
                return new Response(`restore requires ?confirm=${resolved.instance}`, {
                  status: 400,
                })
              }
              const body = await request.json().catch(() => null)
              const key = isRecord(body) ? String(body.key ?? '') : ''
              if (!key)
                return new Response('restore requires a backup key', { status: 400 })
              const id = env.ZERO_SQL_DO.idFromName(resolved.instance)
              const stub = env.ZERO_SQL_DO.get(id)
              await stub.orezBeginRestore()
              const summary = await backupManager.importNamespace(
                env,
                resolved.instance,
                key
              )
              await stub.orezRunApplicationSchema(
                options.schema.version,
                resolved.instance,
                { force: true }
              )
              return Response.json(summary)
            } catch (error) {
              return new Response(`${action} failed: ${errorMessage(error)}`, {
                status: 500,
              })
            }
          }

          return new Response('orez data worker: not found', { status: 404 })
        })
        const isStandardRoute = STANDARD_DATA_PATHS.some(
          (standardPath) =>
            route === standardPath || route.endsWith(`/${standardPath.slice(1)}`)
        )
        let finalResponse = response
        if (isStandardRoute) {
          const headers = new Headers(response.headers)
          headers.set('x-orez-schema-version', options.schema.version)
          finalResponse = new Response(response.body, {
            status: response.status,
            statusText: response.statusText,
            headers,
          })
        }
        if (finalResponse.status >= 500 && options.onError) {
          const error = new Error(`HTTP ${response.status} response from ${route}`)
          error.name = 'HttpServerError'
          reportError(error, { env, executionContext, route, request })
        }
        return finalResponse
      } catch (error) {
        reportError(error, { env, executionContext, route, request })
        throw error
      }
    },

    async scheduled(
      event: OrezScheduledEvent,
      env: Env,
      ctx: OrezExecutionContext
    ): Promise<void> {
      installRuntimeGlobals(env)
      const applicationHandler = options.scheduled?.[event.cron ?? '']
      if (applicationHandler) {
        ctx.waitUntil(
          Promise.resolve(applicationHandler(event, env, ctx)).catch((error) => {
            reportError(error, {
              env,
              executionContext: ctx,
              route: `cron:${event.cron ?? ''}`,
              event,
            })
            throw error
          })
        )
      } else if (backupManager) {
        ctx.waitUntil(backupManager.runScheduledBackups(env))
      }
    },
  }

  return {
    ZeroSqlDO,
    ZeroDO: ZeroSqlDO,
    fetch: worker.fetch,
    scheduled: worker.scheduled,
    applicationSqlClient,
    ensureNamespaceSchema,
    backupManager,
  }
}
