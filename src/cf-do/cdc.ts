import type { DurableSqlStorage } from './watermark.js'

export const CDC_SCHEMA_VERSION = 1

const CDC_TABLES = '_orez_cdc_tables'
const CDC_BUFFER = '_orez_cdc_buffer'
const CDC_TRIGGER_PREFIX = '_orez_cdc_'
const JSON_OBJECT_COLUMNS_PER_CHUNK = 50

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

export interface CapturedRowChange {
  physicalTableName: string
  tableName: string
  op: 'INSERT' | 'UPDATE' | 'DELETE'
  rowData: Record<string, unknown> | null
  oldData: Record<string, unknown> | null
  /** Omitted for normal published changes; false means rollback-only. */
  publish?: boolean
}

export interface SuspendedCdcTable {
  physicalTableName: string
  tableName: string
  publish?: boolean
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

function jsonValue(alias: 'NEW' | 'OLD', column: string): string {
  const value = `${alias}.${quoteIdent(column)}`
  // SQLite's JSON functions reject BLOB values. PostgreSQL's text-format
  // bytea representation is a lossless JSON string and is what pgoutput's
  // consumer expects later in the pipeline.
  return `CASE WHEN typeof(${value}) = 'blob' THEN '\\x' || lower(hex(${value})) ELSE ${value} END`
}

function jsonPath(column: string): string {
  return quoteLiteral(`$.${JSON.stringify(column)}`)
}

function jsonObject(alias: 'NEW' | 'OLD', columns: string[]): string {
  if (columns.length === 0) return `json_object()`
  const first = columns.slice(0, JSON_OBJECT_COLUMNS_PER_CHUNK)
  let expression = `json_object(${first
    .flatMap((column) => [quoteLiteral(column), jsonValue(alias, column)])
    .join(', ')})`
  for (
    let offset = JSON_OBJECT_COLUMNS_PER_CHUNK;
    offset < columns.length;
    offset += JSON_OBJECT_COLUMNS_PER_CHUNK
  ) {
    const args = columns
      .slice(offset, offset + JSON_OBJECT_COLUMNS_PER_CHUNK)
      .flatMap((column) => [jsonPath(column), jsonValue(alias, column)])
    // json_patch implements RFC 7396 and would DELETE keys whose values are
    // SQL/JSON null. json_set preserves those keys while keeping each call
    // below older SQLite builds' function-argument limit.
    expression = `json_set(${expression}, ${args.join(', ')})`
  }
  return expression
}

function changedWhen(columns: string[]): string {
  if (columns.length === 0) return '0'
  return columns
    .map((column) => `OLD.${quoteIdent(column)} IS NOT NEW.${quoteIdent(column)}`)
    .join(' OR ')
}

function parseJsonRecord(value: unknown): Record<string, unknown> | null {
  if (value === null || value === undefined || value === '') return null
  if (typeof value === 'object') return value as Record<string, unknown>
  return JSON.parse(String(value)) as Record<string, unknown>
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

/**
 * Transactional logical-row capture for the authoritative SQLite database.
 *
 * Generated AFTER triggers write full before/after row images to a staging
 * table in the same SQLite statement as the application write. The owner
 * drains that staging table before its storage transaction returns and moves
 * the rows to either `_zero_changes` or `_zero_pending_changes`. Consequently
 * a failed statement or storage transaction cannot leave an orphaned change.
 */
export class TransactionalCdc {
  #active = false
  #registrations = new Map<
    string,
    { tableName: string; columns: string[]; publish: boolean }
  >()
  #verified = new Set<string>()

  constructor(private readonly sql: DurableSqlStorage) {
    this.loadRegistrations()
    this.#active = this.#registrations.size > 0
  }

  get active(): boolean {
    return this.#active
  }

  /** Force the next per-table ensure to re-read SQLite's live schema. */
  invalidateSchema(): void {
    this.#verified.clear()
  }

  private loadRegistrations(): void {
    try {
      const table = this.sql
        .exec(
          "SELECT 1 AS ok FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
          CDC_TABLES
        )
        .one()
      if (!table) return
      this.ensureTables()
      const rows = this.sql
        .exec(
          `SELECT physical_table, table_name, columns_json, publish FROM ${quoteIdent(CDC_TABLES)}`
        )
        .toArray()
      for (const row of rows) {
        const physicalTable = String(row.physical_table ?? '')
        const tableName = String(row.table_name ?? '')
        if (!physicalTable || !tableName) continue
        let columns: string[] = []
        try {
          const parsed = JSON.parse(String(row.columns_json))
          if (Array.isArray(parsed)) columns = parsed.map(String)
        } catch {}
        this.#registrations.set(physicalTable, {
          tableName,
          columns,
          publish: Number(row.publish ?? 1) !== 0,
        })
      }
    } catch {
      this.#registrations.clear()
    }
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
    this.sql.exec(
      `CREATE TABLE IF NOT EXISTS ${quoteIdent(CDC_BUFFER)} (` +
        'seq INTEGER PRIMARY KEY, ' +
        'table_name TEXT NOT NULL, ' +
        "op TEXT NOT NULL CHECK (op IN ('INSERT', 'UPDATE', 'DELETE')), " +
        'row_data TEXT, ' +
        'old_data TEXT)'
    )
  }

  private liveColumns(table: string): string[] {
    try {
      return this.sql
        .exec(`PRAGMA table_xinfo(${quoteIdent(table)})`)
        .toArray()
        .map((row) => String(row.name ?? ''))
        .filter(Boolean)
    } catch {
      return []
    }
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

  private registered(
    table: string
  ): { tableName: string; columns: string[]; publish: boolean } | null {
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

  /** Temporarily detach capture while an owner performs internal row repair. */
  suspendTables(tables: Iterable<string>): SuspendedCdcTable[] {
    const suspended: SuspendedCdcTable[] = []
    for (const physicalTableName of new Set(tables)) {
      const registration = this.#registrations.get(physicalTableName)
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

  /** Reinstall capture after suspendTables, using the live repaired schema. */
  resumeTables(tables: SuspendedCdcTable[]): void {
    for (const table of tables) {
      this.ensureTable(
        {
          physicalTableName: table.physicalTableName,
          tableName: table.tableName,
          ...(table.publish === false ? { publish: false } : null),
        },
        true
      )
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

  private installTriggers(registration: CdcTableRegistration, columns: string[]): void {
    const physicalTable = quoteIdent(registration.physicalTableName)
    const logicalTable = quoteLiteral(registration.tableName)
    const [insertName, updateName, deleteName] = triggerNames(
      registration.physicalTableName
    ).map(quoteIdent)
    const newRow = jsonObject('NEW', columns)
    const oldRow = jsonObject('OLD', columns)

    this.sql.exec(
      `CREATE TRIGGER ${insertName} AFTER INSERT ON ${physicalTable} BEGIN ` +
        `INSERT INTO ${quoteIdent(CDC_BUFFER)} (table_name, op, row_data, old_data) ` +
        `VALUES (${logicalTable}, 'INSERT', ${newRow}, NULL); END`
    )
    this.sql.exec(
      `CREATE TRIGGER ${updateName} AFTER UPDATE ON ${physicalTable} ` +
        `WHEN ${changedWhen(columns)} BEGIN ` +
        `INSERT INTO ${quoteIdent(CDC_BUFFER)} (table_name, op, row_data, old_data) ` +
        `VALUES (${logicalTable}, 'UPDATE', ${newRow}, ${oldRow}); END`
    )
    this.sql.exec(
      `CREATE TRIGGER ${deleteName} AFTER DELETE ON ${physicalTable} BEGIN ` +
        `INSERT INTO ${quoteIdent(CDC_BUFFER)} (table_name, op, row_data, old_data) ` +
        `VALUES (${logicalTable}, 'DELETE', NULL, ${oldRow}); END`
    )
  }

  /** Ensure one table is captured. Returns false when the table does not exist. */
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
      this.#verified.has(physicalTableName)
    ) {
      return true
    }
    if (!physicalTableName || !tableName || !this.tableExists(physicalTableName)) {
      return false
    }

    this.ensureTables()
    const liveColumns = this.liveColumns(physicalTableName)
    if (liveColumns.length === 0) return false
    // The request's column list is a cache hint, not authority. A backend can
    // race an out-of-band migration with stale metadata; narrowing the trigger
    // to that stale list would silently omit the new column forever.
    const capturedColumns = liveColumns
    const previous = this.registered(physicalTableName)
    const unchanged =
      previous?.tableName === tableName &&
      previous.publish === publish &&
      JSON.stringify(previous.columns) === JSON.stringify(capturedColumns) &&
      this.triggersExist(physicalTableName)
    if (!unchanged) {
      this.dropTriggers(physicalTableName)
      this.installTriggers(
        { physicalTableName, tableName, columns: capturedColumns },
        capturedColumns
      )
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
        `SELECT seq, table_name, op, row_data, old_data FROM ${quoteIdent(CDC_BUFFER)} ORDER BY seq`
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
      return {
        physicalTableName,
        tableName,
        op: String(row.op) as CapturedRowChange['op'],
        rowData: parseJsonRecord(row.row_data),
        oldData: parseJsonRecord(row.old_data),
        ...(registration?.publish === false ? { publish: false } : null),
      }
    })
  }
}
