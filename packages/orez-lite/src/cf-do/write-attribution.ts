/**
 * per-transaction physical write attribution for application SQL.
 *
 * Cloudflare bills Durable Object SQLite by post-consume `rowsWritten`. this
 * module classifies those rows from already-owned statement text and CDC
 * capture metadata. it never logs SQL parameters, row values, or production
 * identifiers, and it never writes SQLite.
 */

export const WRITE_ATTRIBUTION_EVENT = 'orez_sql_transaction_sample' as const

export const WORKERS_LOG_SAMPLING = 'workers_observability_may_sample_or_drop' as const

export type NamespaceClass = 'control' | 'project' | 'test'

export type WriteOp = 'INSERT' | 'UPDATE' | 'DELETE' | 'UPSERT' | 'DDL' | 'OTHER'

export type PhysicalSource =
  | 'application'
  | 'cdc_buffer'
  | 'pending_changes'
  | 'zero_changes'
  | 'bookkeeping'
  | 'unclassified'

export type TableVisibility = 'private' | 'synced'

export type ApplicationTableBreakdown = {
  table: string
  op: WriteOp
  visibility: TableVisibility
  logicalRows: number
  physicalRows: number
  indexRows: number
}

export type WriteAttributionBreakdown = {
  application: ApplicationTableBreakdown[]
  cdcBuffer: number
  pendingChanges: number
  zeroChanges: number
  bookkeeping: number
  unclassified: number
}

export type WriteAttributionMeta = {
  workerVersion: string
  namespaceClass: NamespaceClass
  processStartedAt: number
  sampleRate: number
  observedAt: number
}

export type WriteAttributionFields = WriteAttributionMeta & {
  complete: boolean
  logSampling: typeof WORKERS_LOG_SAMPLING
  logicalTotal: number
  physicalTotal: number
  rustVisibleRows: number
  breakdown: WriteAttributionBreakdown
}

export type ClassifiedPhysicalStatement = {
  source: PhysicalSource
  table: string | null
  op: WriteOp
}

export const FORBIDDEN_ATTRIBUTION_KEYS = [
  'sql',
  'params',
  'bindings',
  'namespace',
  'objectId',
  'objectName',
  'rowData',
  'oldData',
  'email',
  'token',
  'secret',
  'prompt',
] as const

const INTERNAL_SOURCE: Record<
  string,
  Exclude<PhysicalSource, 'application' | 'unclassified'>
> = {
  _orez_cdc_buffer: 'cdc_buffer',
  _zero_pending_changes: 'pending_changes',
  _zero_changes: 'zero_changes',
  _orez_tx_manifest: 'bookkeeping',
  _orez_tx_schema: 'bookkeeping',
  _orez_cdc_tables: 'bookkeeping',
  _zero_change_state: 'bookkeeping',
  _zero_schema_tables: 'bookkeeping',
  _zsync_log_segments: 'bookkeeping',
  _zsync_clients: 'bookkeeping',
  _zsync_watermark: 'bookkeeping',
  _zsync_changes: 'bookkeeping',
  _zsync_meta: 'bookkeeping',
}

const SQL_IDENTIFIER =
  '("(?:[^"]|"")*"|`(?:[^`]|``)*`|\\[[^\\]]+\\]|[A-Za-z_][A-Za-z0-9_]*)'

function sqlWithoutLeadingTrivia(sql: string): string {
  if (!sql.includes('--') && !sql.includes('/*')) return sql
  return sql.replace(/(^|;)\s*(?:(?:--[^\n]*(?:\n|$))|(?:\/\*[\s\S]*?\*\/))\s*/g, '$1')
}

function unquoteSqlIdentifier(identifier: string): string {
  if (identifier.startsWith('"') && identifier.endsWith('"')) {
    return identifier.slice(1, -1).replace(/""/g, '"')
  }
  if (identifier.startsWith('`') && identifier.endsWith('`')) {
    return identifier.slice(1, -1).replace(/``/g, '`')
  }
  if (identifier.startsWith('[') && identifier.endsWith(']')) {
    return identifier.slice(1, -1)
  }
  return identifier
}

function physicalTableName(raw: string): string {
  const name = unquoteSqlIdentifier(raw.trim())
  const dot = name.lastIndexOf('.')
  return dot === -1 ? name : name.slice(dot + 1)
}

function internalSource(table: string): PhysicalSource | null {
  const known = INTERNAL_SOURCE[table]
  if (known) return known
  if (
    table.startsWith('_orez_') ||
    table.startsWith('_zero_') ||
    table.startsWith('_zsync_') ||
    table.startsWith('sqlite_')
  ) {
    return 'bookkeeping'
  }
  return null
}

function matchDml(sql: string): { op: WriteOp; table: string } | null {
  const insert = sql.match(
    new RegExp(
      `\\bINSERT\\s+(?:OR\\s+\\w+\\s+)?INTO\\s+(?:TEMP(?:ORARY)?\\s+)?${SQL_IDENTIFIER}`,
      'i'
    )
  )
  if (insert?.[1]) return { op: 'INSERT', table: physicalTableName(insert[1]) }
  const replace = sql.match(
    new RegExp(`\\bREPLACE\\s+INTO\\s+(?:TEMP(?:ORARY)?\\s+)?${SQL_IDENTIFIER}`, 'i')
  )
  if (replace?.[1]) return { op: 'INSERT', table: physicalTableName(replace[1]) }
  const update = sql.match(
    new RegExp(`\\bUPDATE\\s+(?:OR\\s+\\w+\\s+)?${SQL_IDENTIFIER}`, 'i')
  )
  if (update?.[1]) return { op: 'UPDATE', table: physicalTableName(update[1]) }
  const del = sql.match(
    new RegExp(`\\bDELETE\\s+FROM\\s+(?:TEMP(?:ORARY)?\\s+)?${SQL_IDENTIFIER}`, 'i')
  )
  if (del?.[1]) return { op: 'DELETE', table: physicalTableName(del[1]) }
  return null
}

function matchDdlTable(sql: string): string | null {
  const match = sql.match(
    new RegExp(
      `\\b(?:CREATE|ALTER|DROP)\\s+TABLE\\s+(?:IF\\s+(?:NOT\\s+)?EXISTS\\s+)?${SQL_IDENTIFIER}`,
      'i'
    )
  )
  return match?.[1] ? physicalTableName(match[1]) : null
}

export function classifyPhysicalStatement(sql: unknown): ClassifiedPhysicalStatement {
  const text = sqlWithoutLeadingTrivia(String(sql ?? ''))
  const dml = matchDml(text)
  if (dml) {
    const source = internalSource(dml.table)
    return {
      source: source ?? 'application',
      table: dml.table,
      op: dml.op,
    }
  }
  const ddlTable = matchDdlTable(text)
  if (ddlTable) {
    const source = internalSource(ddlTable)
    return {
      source: source ?? 'bookkeeping',
      table: ddlTable,
      op: 'DDL',
    }
  }
  if (/\b(?:CREATE|ALTER|DROP|REINDEX|VACUUM|ANALYZE|PRAGMA)\b/i.test(text)) {
    return { source: 'bookkeeping', table: null, op: 'DDL' }
  }
  return { source: 'unclassified', table: null, op: 'OTHER' }
}

export function namespaceClassFromObjectName(
  name: string | null | undefined
): NamespaceClass {
  if (name == null || name === '') return 'test'
  const n = name.startsWith('ns:') ? name.slice(3) : name
  if (n === 'singleton' || n === 'soot' || n === 'control') return 'control'
  if (n.startsWith('proj-')) return 'project'
  return 'test'
}

export function physicalBreakdownTotal(breakdown: WriteAttributionBreakdown): number {
  return (
    breakdown.application.reduce((sum, row) => sum + row.physicalRows, 0) +
    breakdown.cdcBuffer +
    breakdown.pendingChanges +
    breakdown.zeroChanges +
    breakdown.bookkeeping +
    breakdown.unclassified
  )
}

export function assertWriteAttributionReconciles(event: {
  physicalTotal: number
  logicalTotal: number
  complete?: boolean
  breakdown: WriteAttributionBreakdown
}): void {
  const physical = physicalBreakdownTotal(event.breakdown)
  if (physical !== event.physicalTotal) {
    throw new Error(
      `write attribution physical breakdown ${physical} does not equal physicalTotal ${event.physicalTotal}`
    )
  }
  const logical = event.breakdown.application.reduce(
    (sum, row) => sum + row.logicalRows,
    0
  )
  if (logical !== event.logicalTotal) {
    throw new Error(
      `write attribution logical breakdown ${logical} does not equal logicalTotal ${event.logicalTotal}`
    )
  }
  for (const row of event.breakdown.application) {
    if (row.indexRows < 0 || row.physicalRows < 0 || row.logicalRows < 0) {
      throw new Error(`write attribution ${row.table} ${row.op} has a negative bucket`)
    }
    if (row.indexRows > row.physicalRows) {
      throw new Error(
        `write attribution ${row.table} ${row.op} indexRows exceed physicalRows`
      )
    }
  }
  if (event.complete === true && event.breakdown.unclassified > 0) {
    throw new Error('complete write attribution cannot include unclassified rows')
  }
  if (event.breakdown.unclassified > 0 && event.complete !== false) {
    throw new Error('write attribution has unclassified rows but complete is not false')
  }
}

export function assertNoForbiddenAttributionFields(event: Record<string, unknown>): void {
  for (const key of FORBIDDEN_ATTRIBUTION_KEYS) {
    if (Object.hasOwn(event, key)) {
      throw new Error(`write attribution event includes forbidden field ${key}`)
    }
  }
}

type PhysicalEntry = {
  source: PhysicalSource
  table: string | null
  op: WriteOp
  rows: number
  triggerCaptureRows: number
}

type LogicalEntry = {
  table: string
  op: WriteOp
  visibility: TableVisibility
  rows: number
}

function keyOf(table: string, op: WriteOp): string {
  return `${table}\0${op}`
}

export class WriteAttributionCollector {
  #physicalTotal = 0
  #rustVisibleRows = 0
  readonly #entries: PhysicalEntry[] = []
  readonly #logical = new Map<string, LogicalEntry>()

  recordPhysical(sql: unknown, rows: unknown): void {
    try {
      const count = Number(rows)
      if (!Number.isSafeInteger(count) || count <= 0) return
      this.#physicalTotal += count
      const classified = classifyPhysicalStatement(sql)
      const last = this.#entries.at(-1)
      if (
        last &&
        last.source === classified.source &&
        last.table === classified.table &&
        last.op === classified.op
      ) {
        last.rows += count
        return
      }
      this.#entries.push({ ...classified, rows: count, triggerCaptureRows: 0 })
    } catch {
      const count = Number(rows)
      if (!Number.isSafeInteger(count) || count <= 0) return
      this.#physicalTotal += count
      this.#entries.push({
        source: 'unclassified',
        table: null,
        op: 'OTHER',
        rows: count,
        triggerCaptureRows: 0,
      })
    }
  }

  noteTriggerCaptures(count: unknown): void {
    try {
      const n = Number(count)
      if (!Number.isSafeInteger(n) || n <= 0) return
      for (let index = this.#entries.length - 1; index >= 0; index--) {
        const entry = this.#entries[index]
        if (entry.source === 'application') {
          entry.triggerCaptureRows += n
          return
        }
      }
    } catch {}
  }

  recordLogicalCapture(change: {
    table: string
    op: string
    visibility: TableVisibility
    publish?: boolean
    rows?: number
  }): void {
    try {
      const table = String(change.table ?? '')
      if (!table || internalSource(table)) return
      const op = normalizeOp(change.op)
      if (op !== 'INSERT' && op !== 'UPDATE' && op !== 'DELETE' && op !== 'UPSERT') return
      const rows = Number(change.rows ?? 1)
      if (!Number.isSafeInteger(rows) || rows <= 0) return
      const key = keyOf(table, op)
      const existing = this.#logical.get(key)
      const visibility: TableVisibility =
        existing?.visibility === 'synced' || change.visibility === 'synced'
          ? 'synced'
          : 'private'
      this.#logical.set(key, {
        table,
        op,
        visibility,
        rows: (existing?.rows ?? 0) + rows,
      })
      if (change.publish !== false && change.visibility === 'synced') {
        this.#rustVisibleRows += rows
      }
    } catch {}
  }

  recordUncapturedLogical(rows: unknown): void {
    try {
      const count = Number(rows)
      if (!Number.isSafeInteger(count) || count <= 0) return
      for (let index = this.#entries.length - 1; index >= 0; index--) {
        const entry = this.#entries[index]
        if (entry.source === 'application' && entry.table) {
          this.recordLogicalCapture({
            table: entry.table,
            op: entry.op,
            visibility: 'private',
            publish: false,
            rows: count,
          })
          return
        }
      }
    } catch {}
  }

  summarize(meta: WriteAttributionMeta): WriteAttributionFields {
    try {
      return this.#build(meta)
    } catch {
      return incompleteAttribution(sanitizeMeta(meta), this.#physicalTotal)
    }
  }

  #build(meta: WriteAttributionMeta): WriteAttributionFields {
    const breakdown: WriteAttributionBreakdown = {
      application: [],
      cdcBuffer: 0,
      pendingChanges: 0,
      zeroChanges: 0,
      bookkeeping: 0,
      unclassified: 0,
    }
    const application = new Map<string, ApplicationTableBreakdown>()

    for (const entry of this.#entries) {
      const triggerRows = Math.min(Math.max(0, entry.triggerCaptureRows), entry.rows)
      const remaining = entry.rows - triggerRows
      breakdown.cdcBuffer += triggerRows
      if (entry.source === 'cdc_buffer') {
        breakdown.cdcBuffer += remaining
        continue
      }
      if (entry.source === 'pending_changes') {
        breakdown.pendingChanges += remaining
        continue
      }
      if (entry.source === 'zero_changes') {
        breakdown.zeroChanges += remaining
        continue
      }
      if (entry.source === 'bookkeeping') {
        breakdown.bookkeeping += remaining
        continue
      }
      if (entry.source === 'application' && entry.table) {
        const logical = this.#logical.get(keyOf(entry.table, entry.op))
        addApplicationRow(application, {
          table: entry.table,
          op: entry.op,
          visibility: logical?.visibility ?? 'private',
          logicalRows: 0,
          physicalRows: remaining,
          indexRows: 0,
        })
        continue
      }
      breakdown.unclassified += remaining
    }

    for (const logical of this.#logical.values()) {
      addApplicationRow(application, {
        table: logical.table,
        op: logical.op,
        visibility: logical.visibility,
        logicalRows: logical.rows,
        physicalRows: 0,
      })
    }

    for (const row of application.values()) {
      row.indexRows = Math.max(0, row.physicalRows - row.logicalRows)
      breakdown.application.push(row)
    }
    breakdown.application.sort((a, b) =>
      a.table === b.table ? a.op.localeCompare(b.op) : a.table.localeCompare(b.table)
    )

    const logicalTotal = breakdown.application.reduce(
      (sum, row) => sum + row.logicalRows,
      0
    )
    const fields: WriteAttributionFields = {
      ...sanitizeMeta(meta),
      complete: breakdown.unclassified === 0,
      logSampling: WORKERS_LOG_SAMPLING,
      logicalTotal,
      physicalTotal: this.#physicalTotal,
      rustVisibleRows: this.#rustVisibleRows,
      breakdown,
    }
    assertWriteAttributionReconciles(fields)
    return fields
  }
}

function normalizeOp(op: string): WriteOp {
  const value = String(op ?? '').toUpperCase()
  if (
    value === 'INSERT' ||
    value === 'UPDATE' ||
    value === 'DELETE' ||
    value === 'UPSERT'
  ) {
    return value
  }
  if (value === 'DDL') return 'DDL'
  return 'OTHER'
}

function sanitizeMeta(meta: WriteAttributionMeta): WriteAttributionMeta {
  const sampleRate = Number(meta.sampleRate)
  return {
    workerVersion: String(meta.workerVersion ?? 'local').slice(0, 200),
    namespaceClass:
      meta.namespaceClass === 'control' || meta.namespaceClass === 'project'
        ? meta.namespaceClass
        : 'test',
    processStartedAt: Number.isFinite(meta.processStartedAt) ? meta.processStartedAt : 0,
    sampleRate:
      Number.isFinite(sampleRate) && sampleRate >= 0 && sampleRate <= 1 ? sampleRate : 0,
    observedAt: Number.isFinite(meta.observedAt) ? meta.observedAt : 0,
  }
}

function addApplicationRow(
  byKey: Map<string, ApplicationTableBreakdown>,
  row: Omit<ApplicationTableBreakdown, 'indexRows'> & { indexRows?: number }
): void {
  const key = keyOf(row.table, row.op)
  const existing = byKey.get(key)
  if (!existing) {
    byKey.set(key, {
      table: row.table,
      op: row.op,
      visibility: row.visibility,
      logicalRows: row.logicalRows,
      physicalRows: row.physicalRows,
      indexRows: row.indexRows ?? 0,
    })
    return
  }
  existing.logicalRows += row.logicalRows
  existing.physicalRows += row.physicalRows
  existing.indexRows += row.indexRows ?? 0
  if (row.visibility === 'synced') existing.visibility = 'synced'
}

function incompleteAttribution(
  meta: WriteAttributionMeta,
  physicalTotal: number
): WriteAttributionFields {
  return {
    ...meta,
    complete: false,
    logSampling: WORKERS_LOG_SAMPLING,
    logicalTotal: 0,
    physicalTotal,
    rustVisibleRows: 0,
    breakdown: {
      application: [],
      cdcBuffer: 0,
      pendingChanges: 0,
      zeroChanges: 0,
      bookkeeping: 0,
      unclassified: physicalTotal,
    },
  }
}
