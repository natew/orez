import type { DurableSqlStorage } from './watermark.js'

/** Bump whenever the generated trigger bodies or buffer shape change. */
export const CDC_SCHEMA_VERSION = 2

const CDC_TABLES = '_orez_cdc_tables'
const CDC_BUFFER = '_orez_cdc_buffer'
const CDC_TRIGGER_PREFIX = '_orez_cdc_'
const JSON_OBJECT_COLUMNS_PER_CHUNK = 50
const CDC_BUFFER_COLUMNS = [
  'seq',
  'table_name',
  'op',
  'row_json',
  'old_json',
  'new_rowid',
  'old_rowid',
]

export interface CdcTableRegistration {
  /** Physical, flattened SQLite table name. */
  physicalTableName: string
  /** Schema-qualified identity exposed in `_zero_changes`. */
  tableName: string
  /** Optional known column list. The live SQLite schema remains authoritative. */
  columns?: string[]
  /** False captures before/after images for transaction rollback only. */
  publish?: boolean
}

/**
 * A row image in storage-journal form: one tagged string per column, carrying
 * the value's SQLite storage class and an exact, lossless payload. This is the
 * undo encoding, deliberately separate from the Zero wire encoding that
 * `journalToWire` derives.
 */
export type JournalRecord = Record<string, string>

export interface CapturedRowChange {
  physicalTableName: string
  tableName: string
  op: 'INSERT' | 'UPDATE' | 'DELETE'
  /** Zero wire image (JSON-safe; blobs as postgres bytea text). */
  rowData: Record<string, unknown> | null
  oldData: Record<string, unknown> | null
  /** Lossless storage-journal image used only for undo. */
  rowJournal: JournalRecord | null
  oldJournal: JournalRecord | null
  /** Exact decimal rowid text, or null for WITHOUT ROWID tables. */
  newRowid: string | null
  oldRowid: string | null
  /** Omitted for normal published changes; false means rollback-only. */
  publish?: boolean
}

export interface SuspendedCdcTable {
  physicalTableName: string
  tableName: string
  publish?: boolean
}

/** Stable-identity and writability metadata read from the live SQLite schema. */
export interface TableIdentity {
  /** rowid alias usable in SQL, or null for a WITHOUT ROWID table. */
  rowidAlias: string | null
  /** The INTEGER PRIMARY KEY column, which *is* the rowid, when one exists. */
  rowidColumn: string | null
  /** Declared primary-key columns in key order. */
  keyColumns: string[]
  /** Columns undo may write. Excludes generated columns. */
  writableColumns: string[]
  /** Every column captured in a row image, generated columns included. */
  columns: string[]
}

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`
}

function quoteLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`
}

function triggerStem(table: string): string {
  const bytes = new TextEncoder().encode(table)
  let encoded = ''
  for (const byte of bytes) encoded += byte.toString(16).padStart(2, '0')
  return `${CDC_TRIGGER_PREFIX}${encoded}`
}

function triggerNames(table: string): string[] {
  const stem = triggerStem(table)
  return [`${stem}_insert`, `${stem}_update`, `${stem}_delete`]
}

// ── storage-journal value codec ────────────────────────────────────────────
//
// Every column is captured as a tagged string so the undo path can rebuild the
// exact SQLite value, storage class included:
//
//   n            NULL
//   i<decimal>   INTEGER, full signed 64-bit range as text
//   r<%!.17g>    REAL, 17 significant digits round-trips a double exactly.
//                'Inf'/'-Inf' for the infinities.
//   s<text>      TEXT
//   b<hex>       BLOB, lowercase hex
//
// A JSON number cannot carry an int64 (JSON.parse silently rounds anything past
// 2^53), CAST(real AS TEXT) is lossy (SQLite's default 15 digits turns
// MAX_DOUBLE into Inf), and SQLite's JSON functions reject BLOB values outright.
// Text tags dodge all three, plus every driver's differing int64/BigInt support.

/**
 * Build the SQL expression that encodes one column for the journal.
 *
 * The `!` in `%!.17g` is SQLite's alternate-form-2 flag and it is load-bearing:
 * it forces all 17 requested digits. Plain `%.17g` still applies SQLite's
 * shortest-representation logic and silently drops digits, which turns
 * MAX_DOUBLE into a value that reads back as `Inf` and collapses
 * 0.30000000000000004 to 0.3. Do not "simplify" it away.
 */
function journalValueSql(alias: 'NEW' | 'OLD', column: string): string {
  const value = `${alias}.${quoteIdent(column)}`
  return (
    `CASE typeof(${value})` +
    ` WHEN 'integer' THEN 'i' || CAST(${value} AS TEXT)` +
    ` WHEN 'real' THEN 'r' || printf('%!.17g', ${value})` +
    ` WHEN 'text' THEN 's' || ${value}` +
    ` WHEN 'blob' THEN 'b' || lower(hex(${value}))` +
    ` ELSE 'n' END`
  )
}

// SQLite's CAST is forgiving in exactly the wrong way for a journal: it reads
// '12junk' as 12 and 'junk' as 0.0, so a corrupt payload would restore a
// plausible but wrong value instead of failing. Every numeric payload is
// therefore checked against the canonical form this codec emits.
const INTEGER_PAYLOAD = /^-?(?:0|[1-9]\d*)$/
const REAL_PAYLOAD = /^-?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/
const INT64_MIN = -(2n ** 63n)
const INT64_MAX = 2n ** 63n - 1n

function integerPayload(body: string): string {
  if (!INTEGER_PAYLOAD.test(body)) {
    throw new Error(`cdc journal: corrupt integer payload ${JSON.stringify(body)}`)
  }
  // An out-of-range literal saturates rather than errors, which would restore a
  // clamped value under the same silent-corruption failure mode.
  const value = BigInt(body)
  if (value < INT64_MIN || value > INT64_MAX) {
    throw new Error(`cdc journal: integer payload out of int64 range ${body}`)
  }
  return body
}

function realPayload(body: string): string {
  if (body === 'Inf' || body === '-Inf') return body
  if (REAL_PAYLOAD.test(body)) {
    const value = Number(body)
    const mantissa = body.split(/[eE]/, 1)[0] ?? body
    if (Number.isFinite(value) && (value !== 0 || !/[1-9]/.test(mantissa))) {
      return body
    }
  }
  throw new Error(`cdc journal: corrupt real payload ${JSON.stringify(body)}`)
}

function realFromText(text: string): number {
  if (text === 'Inf') return Number.POSITIVE_INFINITY
  if (text === '-Inf') return Number.NEGATIVE_INFINITY
  return Number(text)
}

/**
 * Convert a journal image to the Zero wire image: plain JSON, with blobs in
 * postgres's bytea text format, which is what the pgoutput consumer downstream
 * of `_zero_changes` expects.
 */
function journalToWire(record: JournalRecord | null): Record<string, unknown> | null {
  if (!record) return null
  const wire: Record<string, unknown> = {}
  for (const [column, encoded] of Object.entries(record)) {
    const body = encoded.slice(1)
    switch (encoded[0]) {
      case 'i': {
        // A JSON number cannot hold an int64. Past 2^53 the nearest double is a
        // different integer, so a snowflake id would reach the changefeed as
        // the wrong value. Those keep their exact decimal text; everything in
        // range stays a plain number.
        const exact = integerPayload(body)
        const numeric = Number(exact)
        wire[column] = Number.isSafeInteger(numeric) ? numeric : exact
        break
      }
      case 'r': {
        const real = realPayload(body)
        // JSON.stringify turns JS infinities into null. PostgreSQL accepts
        // these spellings in float text format, and strings remain exact in
        // the JSON-backed change log.
        wire[column] =
          real === 'Inf' ? 'Infinity' : real === '-Inf' ? '-Infinity' : realFromText(real)
        break
      }
      case 's':
        wire[column] = body
        break
      case 'b':
        if (!/^(?:[0-9a-f]{2})*$/.test(body)) {
          throw new Error(`cdc journal: corrupt blob payload ${JSON.stringify(body)}`)
        }
        wire[column] = `\\x${body}`
        break
      case 'n':
        if (body !== '') {
          throw new Error(`cdc journal: corrupt null payload ${JSON.stringify(body)}`)
        }
        wire[column] = null
        break
      default:
        throw new Error(`cdc journal: unknown value tag for column ${column}`)
    }
  }
  return wire
}

/**
 * Rebuild the exact SQLite value for an undo statement. Integers and reals go
 * back through CAST so the full int64 range and every double survive without
 * depending on the driver's numeric binding.
 */
export function journalValueSqlBinding(encoded: string): {
  expr: string
  params: unknown[]
} {
  const body = encoded.slice(1)
  switch (encoded[0]) {
    case 'i':
      return { expr: 'CAST(? AS INTEGER)', params: [integerPayload(body)] }
    case 'r': {
      const real = realPayload(body)
      if (real === 'Inf') return { expr: '9e999', params: [] }
      if (real === '-Inf') return { expr: '-9e999', params: [] }
      return { expr: 'CAST(? AS REAL)', params: [real] }
    }
    case 's':
      return { expr: '?', params: [body] }
    case 'b':
      if (!/^(?:[0-9a-f]{2})*$/.test(body)) {
        throw new Error(`cdc journal: corrupt blob payload ${JSON.stringify(body)}`)
      }
      return { expr: `x'${body}'`, params: [] }
    case 'n':
      if (body !== '') {
        throw new Error(`cdc journal: corrupt null payload ${JSON.stringify(body)}`)
      }
      return { expr: 'NULL', params: [] }
    default:
      // Never fail open to NULL. An unrecognized tag means the journal is
      // corrupt, and quietly restoring the column as NULL would destroy the
      // value the rollback exists to bring back.
      throw new Error(`cdc journal: unknown value tag ${JSON.stringify(encoded)}`)
  }
}

export function parseJournalRecord(value: unknown): JournalRecord | null {
  if (value === null || value === undefined || value === '') return null
  const parsed = typeof value === 'object' ? value : JSON.parse(String(value))
  if (
    parsed === null ||
    Array.isArray(parsed) ||
    typeof parsed !== 'object' ||
    Object.values(parsed).some((encoded) => typeof encoded !== 'string')
  ) {
    throw new Error('cdc journal: corrupt row image')
  }
  return parsed as JournalRecord
}

// ── schema introspection ──────────────────────────────────────────────────

/**
 * Read stable-identity metadata from the live schema. Returns null when the
 * table cannot be undone row-by-row (no rowid and no primary key), which keeps
 * capture off it entirely so the caller falls back to table snapshots.
 */
export function tableIdentity(
  sql: DurableSqlStorage,
  table: string
): TableIdentity | null {
  let info: Array<Record<string, unknown>>
  try {
    info = sql.exec(`PRAGMA table_xinfo(${quoteIdent(table)})`).toArray()
  } catch {
    return null
  }
  const columns = info.map((row) => String(row.name ?? '')).filter(Boolean)
  if (columns.length === 0) return null

  const writableColumns = writableColumnsOf(info)
  const keyColumns = info
    .filter((row) => Number(row.pk ?? 0) > 0)
    .sort((a, b) => Number(a.pk ?? 0) - Number(b.pk ?? 0))
    .map((row) => String(row.name ?? ''))
    .filter(Boolean)

  const rowidAlias = detectRowidAlias(sql, table, columns)
  // A primary key that IS the rowid needs no restoring of its own: writing the
  // column back puts the row at its original rowid.
  //
  // Ask the schema, not the declared type. "INTEGER PRIMARY KEY" is a rowid
  // alias but "INTEGER PRIMARY KEY DESC" is not, and neither is "INT PRIMARY
  // KEY", so matching on the type string gets both wrong. SQLite builds a real
  // index for every primary key EXCEPT a rowid alias, so the absence of an
  // origin='pk' index is the exact test.
  const rowidColumn =
    rowidAlias && keyColumns.length === 1 && !hasPrimaryKeyIndex(sql, table)
      ? keyColumns[0]
      : null

  if (!rowidAlias && keyColumns.length === 0) return null
  return { rowidAlias, rowidColumn, keyColumns, writableColumns, columns }
}

function hasPrimaryKeyIndex(sql: DurableSqlStorage, table: string): boolean {
  try {
    return sql
      .exec(`PRAGMA index_list(${quoteIdent(table)})`)
      .toArray()
      .some((row) => String(row.origin ?? '') === 'pk')
  } catch {
    return true
  }
}

/**
 * table_xinfo.hidden: 0 ordinary, 1 virtual-table hidden, 2 VIRTUAL generated,
 * 3 STORED generated. SQLite refuses to INSERT or UPDATE a generated column, so
 * no restore may ever name one.
 */
function writableColumnsOf(info: Array<Record<string, unknown>>): string[] {
  return info
    .filter((row) => Number(row.hidden ?? 0) === 0)
    .map((row) => String(row.name ?? ''))
    .filter(Boolean)
}

export interface TriggerDefinition {
  name: string
  sql: string
}

/**
 * Drop every trigger on these tables and return their definitions.
 *
 * A restore must not fire the table's own business triggers. Undoing an INSERT
 * with a DELETE would run its AFTER DELETE trigger and write side effects the
 * original transaction never made, and if that side effect lands on a captured
 * table, its CDC trigger stages a phantom change on top. Dropping only the
 * generated CDC triggers leaves both holes open, so every trigger goes.
 */
export function suspendTriggers(
  sql: DurableSqlStorage,
  tables: Iterable<string>
): TriggerDefinition[] {
  const unique = [...new Set(tables)].filter(Boolean)
  if (unique.length === 0) return []
  const placeholders = unique.map(() => '?').join(', ')
  const triggers = sql
    .exec(
      `SELECT name, sql FROM sqlite_master WHERE type = 'trigger' AND tbl_name IN (${placeholders}) ORDER BY name`,
      ...unique
    )
    .toArray()
    .map((row) => ({ name: String(row.name ?? ''), sql: String(row.sql ?? '') }))
    .filter((trigger) => trigger.name && trigger.sql)
  for (const trigger of triggers) {
    sql.exec(`DROP TRIGGER IF EXISTS ${quoteIdent(trigger.name)}`)
  }
  return triggers
}

/** Recreate triggers suspended for a restore, verbatim. */
export function restoreTriggers(
  sql: DurableSqlStorage,
  triggers: TriggerDefinition[]
): void {
  for (const trigger of triggers) sql.exec(trigger.sql)
}

/** The columns a restore is allowed to write. Empty when the table is gone. */
export function writableColumns(sql: DurableSqlStorage, table: string): string[] {
  try {
    return writableColumnsOf(
      sql.exec(`PRAGMA table_xinfo(${quoteIdent(table)})`).toArray()
    )
  } catch {
    return []
  }
}

function detectRowidAlias(
  sql: DurableSqlStorage,
  table: string,
  columns: string[]
): string | null {
  const shadowed = new Set(columns.map((column) => column.toLowerCase()))
  for (const alias of ['_rowid_', 'rowid', 'oid']) {
    if (shadowed.has(alias)) continue
    try {
      sql.exec(`SELECT ${alias} FROM ${quoteIdent(table)} LIMIT 0`).toArray()
      return alias
    } catch {
      // WITHOUT ROWID: no alias resolves, so stop probing.
      return null
    }
  }
  return null
}

// ── trigger generation ────────────────────────────────────────────────────

function jsonPath(column: string): string {
  return quoteLiteral(`$.${JSON.stringify(column)}`)
}

function jsonObject(alias: 'NEW' | 'OLD', columns: string[]): string {
  if (columns.length === 0) return `json_object()`
  const first = columns.slice(0, JSON_OBJECT_COLUMNS_PER_CHUNK)
  let expression = `json_object(${first
    .flatMap((column) => [quoteLiteral(column), journalValueSql(alias, column)])
    .join(', ')})`
  for (
    let offset = JSON_OBJECT_COLUMNS_PER_CHUNK;
    offset < columns.length;
    offset += JSON_OBJECT_COLUMNS_PER_CHUNK
  ) {
    const args = columns
      .slice(offset, offset + JSON_OBJECT_COLUMNS_PER_CHUNK)
      .flatMap((column) => [jsonPath(column), journalValueSql(alias, column)])
    // json_patch implements RFC 7396 and would DELETE keys whose values are
    // SQL/JSON null. json_set preserves those keys while keeping each call
    // below older SQLite builds' function-argument limit.
    expression = `json_set(${expression}, ${args.join(', ')})`
  }
  return expression
}

function rowidSql(alias: 'NEW' | 'OLD', identity: TableIdentity): string {
  if (!identity.rowidAlias) return 'NULL'
  return `CAST(${alias}.${identity.rowidAlias} AS TEXT)`
}

function changedWhen(columns: string[]): string {
  if (columns.length === 0) return '0'
  return columns
    .map((column) => `OLD.${quoteIdent(column)} IS NOT NEW.${quoteIdent(column)}`)
    .join(' OR ')
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

/**
 * Return physical tables whose SQLite schema may be changed by this statement.
 * The DO compiler emits one SQLite DDL statement at a time and quotes physical
 * names, but accepting every SQLite identifier form also covers direct /exec
 * callers and keeps CDC independent of the compiler.
 */
function schemaChangeTargets(sql: string): string[] {
  const identifier = '("(?:[^"]|"")*"|`(?:[^`]|``)*`|\\[[^\\]]+\\]|[^\\s;(]+)'
  const patterns = [
    new RegExp(`\\bALTER\\s+TABLE\\s+(?:IF\\s+EXISTS\\s+)?${identifier}`, 'gi'),
    new RegExp(
      `\\b(?:CREATE|DROP)\\s+TABLE\\s+(?:IF\\s+(?:NOT\\s+)?EXISTS\\s+)?${identifier}`,
      'gi'
    ),
  ]
  const targets = new Set<string>()
  for (const pattern of patterns) {
    for (const match of sql.matchAll(pattern)) {
      const name = match[1]
      if (name) targets.add(unquoteSqlIdentifier(name))
    }
  }
  return [...targets]
}

interface Registration {
  tableName: string
  columns: string[]
  publish: boolean
  version: number
}

/**
 * Transactional logical-row capture for the authoritative SQLite database.
 *
 * Generated AFTER triggers write full before/after row images to a staging
 * table in the same SQLite statement as the application write. The owner
 * drains that staging table before its storage transaction returns and moves
 * the rows to either `_zero_changes` or `_zero_pending_changes`. Consequently
 * a failed statement or storage transaction cannot leave an orphaned change.
 *
 * Every mutator of the in-memory registration cache runs inside the owner's
 * storage transaction, so the cache can outlive an abort that rolled its SQLite
 * side back. `reload()` re-derives the whole cache from SQLite and MUST be
 * called on any aborted transaction; see `atomically` in worker.ts.
 */
export class TransactionalCdc {
  #active = false
  #registrations = new Map<string, Registration>()
  #verified = new Set<string>()

  constructor(private readonly sql: DurableSqlStorage) {
    this.#registrations = this.loadRegistrations()
    this.#active = this.#registrations.size > 0
  }

  get active(): boolean {
    return this.#active
  }

  /** Force the next per-table ensure to re-read SQLite's live schema. */
  invalidateSchema(): void {
    this.#verified.clear()
  }

  /**
   * Re-derive every cached decision from SQLite. Called after an aborted
   * storage transaction, where SQLite rolled back the trigger and metadata
   * writes but this object still remembers making them. Without it a table can
   * stay "registered and verified" in memory with no trigger on disk, and every
   * later write to it is silently uncaptured.
   */
  reload(): void {
    // Build and validate the replacement before publishing it. A corrupt row
    // must fail the request without erasing a still-live capture decision.
    // Verification is different: an aborted transaction may have rolled its
    // triggers back, so no cached verification can survive even a failed load.
    this.#verified.clear()
    const registrations = this.loadRegistrations()
    this.#registrations = registrations
    this.#active = this.#registrations.size > 0
  }

  private loadRegistrations(): Map<string, Registration> {
    const table = this.sql
      .exec(
        "SELECT 1 AS ok FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        CDC_TABLES
      )
      .toArray()
    if (table.length === 0) return new Map()
    this.ensureTables()
    const rows = this.sql
      .exec(
        `SELECT physical_table, table_name, columns_json, publish, schema_version FROM ${quoteIdent(CDC_TABLES)}`
      )
      .toArray()
    const registrations = new Map<string, Registration>()
    for (const row of rows) {
      const physicalTable = String(row.physical_table ?? '')
      const tableName = String(row.table_name ?? '')
      if (!physicalTable || !tableName) {
        throw new Error('cdc registrations: corrupt table identity')
      }

      let columns: unknown
      try {
        columns = JSON.parse(String(row.columns_json))
      } catch (error) {
        throw new Error(
          `cdc registrations: corrupt columns_json for ${JSON.stringify(physicalTable)}`,
          { cause: error }
        )
      }
      if (
        !Array.isArray(columns) ||
        columns.length === 0 ||
        columns.some((column) => typeof column !== 'string') ||
        new Set(columns).size !== columns.length
      ) {
        throw new Error(
          `cdc registrations: invalid columns_json for ${JSON.stringify(physicalTable)}`
        )
      }

      const publish = Number(row.publish)
      const version = Number(row.schema_version)
      if (
        (publish !== 0 && publish !== 1) ||
        !Number.isSafeInteger(version) ||
        version < 0
      ) {
        throw new Error(
          `cdc registrations: invalid metadata for ${JSON.stringify(physicalTable)}`
        )
      }
      registrations.set(physicalTable, {
        tableName,
        columns,
        publish: publish === 1,
        version,
      })
    }
    return registrations
  }

  private ensureTables(): void {
    this.sql.exec(
      `CREATE TABLE IF NOT EXISTS ${quoteIdent(CDC_TABLES)} (` +
        'physical_table TEXT PRIMARY KEY, ' +
        'table_name TEXT NOT NULL, ' +
        'columns_json TEXT NOT NULL, ' +
        'publish INTEGER NOT NULL DEFAULT 1, ' +
        `schema_version INTEGER NOT NULL DEFAULT ${CDC_SCHEMA_VERSION})`
    )
    const tableColumns = this.sql
      .exec(`PRAGMA table_info(${quoteIdent(CDC_TABLES)})`)
      .toArray()
    if (!tableColumns.some((column) => String(column.name) === 'publish')) {
      this.sql.exec(
        `ALTER TABLE ${quoteIdent(CDC_TABLES)} ADD COLUMN publish INTEGER NOT NULL DEFAULT 1`
      )
    }
    if (!tableColumns.some((column) => String(column.name) === 'schema_version')) {
      this.sql.exec(
        `ALTER TABLE ${quoteIdent(CDC_TABLES)} ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 0`
      )
    }
    // The buffer only ever holds rows for the statement being drained, so a
    // shape change from an older schema version can simply replace it.
    const bufferColumns = this.sql
      .exec(`PRAGMA table_info(${quoteIdent(CDC_BUFFER)})`)
      .toArray()
      .map((column) => String(column.name ?? ''))
    if (
      bufferColumns.length > 0 &&
      !CDC_BUFFER_COLUMNS.every((column) => bufferColumns.includes(column))
    ) {
      this.sql.exec(`DROP TABLE IF EXISTS ${quoteIdent(CDC_BUFFER)}`)
    }
    this.sql.exec(
      `CREATE TABLE IF NOT EXISTS ${quoteIdent(CDC_BUFFER)} (` +
        'seq INTEGER PRIMARY KEY, ' +
        'table_name TEXT NOT NULL, ' +
        "op TEXT NOT NULL CHECK (op IN ('INSERT', 'UPDATE', 'DELETE')), " +
        'row_json TEXT, ' +
        'old_json TEXT, ' +
        'new_rowid TEXT, ' +
        'old_rowid TEXT)'
    )
  }

  private tableExists(table: string): boolean {
    // Durable Object SQL's `one()` throws when a query returns zero rows,
    // unlike several SQLite adapters that return undefined. Existence checks
    // need the portable zero-or-one-row cursor contract.
    return (
      this.sql
        .exec(
          "SELECT 1 AS ok FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
          table
        )
        .toArray().length > 0
    )
  }

  private triggersExist(table: string): boolean {
    const names = triggerNames(table)
    const placeholders = names.map(() => '?').join(', ')
    return (
      this.sql
        .exec(
          `SELECT name FROM sqlite_master WHERE type = 'trigger' AND name IN (${placeholders})`,
          ...names
        )
        .toArray().length === names.length
    )
  }

  private registered(table: string): Registration | null {
    return this.#registrations.get(table) ?? null
  }

  capturesTable(tableName: string): boolean {
    return (
      this.#active &&
      [...this.#registrations.values()].some(
        (registration) => registration.publish && registration.tableName === tableName
      )
    )
  }

  /** True when a DDL statement must atomically suspend and rebuild CDC triggers. */
  capturesSchemaChange(sql: string): boolean {
    return schemaChangeTargets(sql).some((table) => this.#registrations.has(table))
  }

  private dropTriggers(table: string): void {
    for (const name of triggerNames(table)) {
      this.sql.exec(`DROP TRIGGER IF EXISTS ${quoteIdent(name)}`)
    }
  }

  /**
   * Drop generated triggers before SQLite rewrites a captured table. SQLite
   * refuses DROP/RENAME COLUMN while any trigger still references the old row
   * shape. The caller must run this, the DDL, and finishSchemaChange in one
   * storage transaction so capture is never observably disabled.
   */
  beginSchemaChange(sql: string): SuspendedCdcTable[] {
    const suspended: SuspendedCdcTable[] = []
    for (const physicalTableName of schemaChangeTargets(sql)) {
      const registration = this.registered(physicalTableName)
      if (!registration) continue
      this.dropTriggers(physicalTableName)
      this.#verified.delete(physicalTableName)
      suspended.push({
        physicalTableName,
        tableName: registration.tableName,
        ...(registration.publish ? null : { publish: false }),
      })
    }
    return suspended
  }

  /** Re-introspect changed tables and rebuild their triggers from the live shape. */
  finishSchemaChange(suspended: SuspendedCdcTable[]): void {
    for (const registration of suspended) {
      if (this.ensureTable(registration, true)) continue
      this.sql.exec(
        `DELETE FROM ${quoteIdent(CDC_TABLES)} WHERE physical_table = ?`,
        registration.physicalTableName
      )
      this.#registrations.delete(registration.physicalTableName)
      this.#verified.delete(registration.physicalTableName)
    }
    this.#active = this.#registrations.size > 0
  }

  private installTriggers(
    registration: { physicalTableName: string; tableName: string },
    identity: TableIdentity
  ): void {
    const physicalTable = quoteIdent(registration.physicalTableName)
    const logicalTable = quoteLiteral(registration.tableName)
    const [insertName, updateName, deleteName] = triggerNames(
      registration.physicalTableName
    ).map(quoteIdent)
    const columns = identity.columns
    const newRow = jsonObject('NEW', columns)
    const oldRow = jsonObject('OLD', columns)
    const newRowid = rowidSql('NEW', identity)
    const oldRowid = rowidSql('OLD', identity)
    const insertInto =
      `INSERT INTO ${quoteIdent(CDC_BUFFER)} ` +
      '(table_name, op, row_json, old_json, new_rowid, old_rowid)'

    this.sql.exec(
      `CREATE TRIGGER ${insertName} AFTER INSERT ON ${physicalTable} BEGIN ` +
        `${insertInto} VALUES (${logicalTable}, 'INSERT', ${newRow}, NULL, ${newRowid}, NULL); END`
    )
    this.sql.exec(
      `CREATE TRIGGER ${updateName} AFTER UPDATE ON ${physicalTable} ` +
        `WHEN ${changedWhen(columns)} BEGIN ` +
        `${insertInto} VALUES (${logicalTable}, 'UPDATE', ${newRow}, ${oldRow}, ${newRowid}, ${oldRowid}); END`
    )
    this.sql.exec(
      `CREATE TRIGGER ${deleteName} AFTER DELETE ON ${physicalTable} BEGIN ` +
        `${insertInto} VALUES (${logicalTable}, 'DELETE', NULL, ${oldRow}, NULL, ${oldRowid}); END`
    )
  }

  /**
   * Ensure one table is captured. Returns false when the table does not exist
   * or cannot be undone row-by-row, which leaves the caller on its table
   * snapshot path rather than capturing changes it could never roll back.
   */
  ensureTable(registration: CdcTableRegistration, refresh = false): boolean {
    const physicalTableName = String(registration.physicalTableName || '')
    const tableName = String(registration.tableName || '')
    const cached = this.registered(physicalTableName)
    // A rollback-only request must never demote an already published table.
    const publish = cached?.publish || registration.publish !== false
    if (
      !refresh &&
      cached?.tableName === tableName &&
      cached.publish === publish &&
      cached.version === CDC_SCHEMA_VERSION &&
      this.#verified.has(physicalTableName)
    ) {
      return true
    }
    if (!physicalTableName || !tableName || !this.tableExists(physicalTableName)) {
      return false
    }

    this.ensureTables()
    // The request's column list is a cache hint, not authority. A backend can
    // race an out-of-band migration with stale metadata; narrowing the trigger
    // to that stale list would silently omit the new column forever.
    const identity = tableIdentity(this.sql, physicalTableName)
    if (!identity) return false
    const capturedColumns = identity.columns
    const previous = this.registered(physicalTableName)
    const unchanged =
      previous?.tableName === tableName &&
      previous.publish === publish &&
      previous.version === CDC_SCHEMA_VERSION &&
      JSON.stringify(previous.columns) === JSON.stringify(capturedColumns) &&
      this.triggersExist(physicalTableName)
    if (!unchanged) {
      this.dropTriggers(physicalTableName)
      this.installTriggers({ physicalTableName, tableName }, identity)
      this.sql.exec(
        `INSERT OR REPLACE INTO ${quoteIdent(CDC_TABLES)} ` +
          '(physical_table, table_name, columns_json, publish, schema_version) VALUES (?, ?, ?, ?, ?)',
        physicalTableName,
        tableName,
        JSON.stringify(capturedColumns),
        publish ? 1 : 0,
        CDC_SCHEMA_VERSION
      )
      this.#registrations.set(physicalTableName, {
        tableName,
        columns: capturedColumns,
        publish,
        version: CDC_SCHEMA_VERSION,
      })
    }
    this.#verified.add(physicalTableName)
    this.#active = true
    return true
  }

  /** Replace the captured-table set with the caller's authoritative list. */
  syncTables(registrations: CdcTableRegistration[]): void {
    const desired = new Map<string, CdcTableRegistration>()
    for (const registration of registrations ?? []) {
      const physicalTableName = String(registration?.physicalTableName || '')
      const tableName = String(registration?.tableName || '')
      if (!physicalTableName || !tableName) continue
      desired.set(physicalTableName, {
        physicalTableName,
        tableName,
        publish: true,
        ...(registration.columns?.length
          ? { columns: registration.columns.map(String) }
          : null),
      })
    }

    if (!this.#active && desired.size === 0 && this.#registrations.size === 0) return
    this.ensureTables()
    const installed = [...this.#registrations.keys()]
    for (const table of installed) {
      const registration = this.#registrations.get(table)
      if (desired.has(table) || registration?.publish === false) continue
      this.dropTriggers(table)
      this.sql.exec(
        `DELETE FROM ${quoteIdent(CDC_TABLES)} WHERE physical_table = ?`,
        table
      )
      this.#registrations.delete(table)
      this.#verified.delete(table)
    }
    for (const registration of desired.values()) this.ensureTable(registration, true)
    this.#active = this.#registrations.size > 0
  }

  /** Drain all changes captured by the just-completed SQLite statement. */
  drain(): CapturedRowChange[] {
    if (!this.#active) return []
    const rows = this.sql
      .exec(
        `SELECT seq, table_name, op, row_json, old_json, new_rowid, old_rowid ` +
          `FROM ${quoteIdent(CDC_BUFFER)} ORDER BY seq`
      )
      .toArray()
    if (rows.length === 0) return []
    this.sql.exec(`DELETE FROM ${quoteIdent(CDC_BUFFER)}`)
    return rows.map((row) => {
      const tableName = String(row.table_name)
      const physicalTableName =
        [...this.#registrations].find(
          ([, registration]) => registration.tableName === tableName
        )?.[0] ?? tableName.replace(/^public\./, '')
      const registration = this.#registrations.get(physicalTableName)
      const rowJournal = parseJournalRecord(row.row_json)
      const oldJournal = parseJournalRecord(row.old_json)
      return {
        physicalTableName,
        tableName,
        op: String(row.op) as CapturedRowChange['op'],
        rowData: journalToWire(rowJournal),
        oldData: journalToWire(oldJournal),
        rowJournal,
        oldJournal,
        newRowid: row.new_rowid === null ? null : String(row.new_rowid),
        oldRowid: row.old_rowid === null ? null : String(row.old_rowid),
        ...(registration?.publish === false ? { publish: false } : null),
      }
    })
  }
}
