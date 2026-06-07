// NOTE THIS IS NOT OREZ NODE THIS IS NOT A GOOD REFERENCE BECAUSE ITS OUR EARLY GUESS AT WHAT COULD WORK
// DO NOT STUDY THIS, THE OTHER STUFF IN SRC IS WHERE YOU EANT TO LOOK

/**
 * sqlite shim for cloudflare durable objects.
 *
 * wraps the DO SqlStorage interface (`this.ctx.storage.sql`) to implement
 * the better-sqlite3 / @rocicorp/zero-sqlite3 api that zero-cache uses.
 *
 * all operations are synchronous, matching both DO sqlite and better-sqlite3.
 *
 * usage in a durable object:
 *
 *   import { Database } from 'orez/worker/shims/sqlite'
 *
 *   export class MyDO extends DurableObject {
 *     db: Database
 *     constructor(ctx: DurableObjectState, env: Env) {
 *       super(ctx, env)
 *       this.db = new Database(ctx.storage.sql)
 *     }
 *   }
 */

// -- abstract interface for DO SqlStorage --

export type SqlStorageValue = string | number | null | ArrayBuffer

export interface SqlStorageCursor {
  toArray(): Record<string, SqlStorageValue>[]
  readonly rowsRead: number
  readonly rowsWritten: number
  readonly columnNames: string[]
}

export interface SqlStorageLike {
  exec(query: string, ...bindings: SqlStorageValue[]): SqlStorageCursor
  /** DO transaction API — if available, used instead of raw BEGIN/COMMIT */
  transactionSync?<T>(fn: () => T): T
}

type SqliteConnectionRole = 'default' | 'replica-writer'
const activeSnapshotPrefixes = new Set<string>()

// -- SqliteError --

export class SqliteError extends Error {
  code: string

  constructor(message: string, code: string) {
    super(message)
    this.name = 'SqliteError'
    this.code = code
  }
}

// -- RunResult --

export interface RunResult {
  changes: number
  lastInsertRowid: number | bigint
}

// -- parameter serialization --
// sqlite only accepts scalar values; serialize objects/booleans/dates/etc.
// special case: a single object argument means named parameters (@key syntax)
// convert named params (@key) in SQL to positional (?) and extract values in order.
// CF DO SqlStorage doesn't support @key syntax, only ? placeholders.
function convertNamedParams(
  sql: string,
  params: Record<string, unknown>
): { sql: string; values: SqlStorageValue[] } {
  const values: SqlStorageValue[] = []
  // each @param occurrence gets its own ? placeholder and value
  const converted = sql.replace(/@(\w+)/g, (_, name) => {
    values.push(serializeValue(params[name]))
    return '?'
  })
  return { sql: converted, values }
}

function serializeValue(p: unknown): SqlStorageValue {
  if (p === null || p === undefined) return null
  if (typeof p === 'string' || typeof p === 'number') return p
  if (typeof p === 'boolean') return p ? 1 : 0
  if (typeof p === 'bigint') return Number(p)
  if (p instanceof ArrayBuffer || p instanceof Uint8Array) return p as any
  if (typeof p === 'object') return JSON.stringify(p)
  return String(p)
}

function serializeSqliteParams(params: unknown[]): SqlStorageValue[] {
  // better-sqlite3 API: stmt.run([val1, val2, ...]) spreads the array
  // as positional parameters. detect this: single array argument.
  if (params.length === 1 && Array.isArray(params[0])) {
    return serializeSqliteParams(params[0])
  }

  // named parameters: .run({key: value}) → extract values matching @key placeholders
  // handled at the exec level via convertNamedParams, return as marker here
  if (
    params.length === 1 &&
    params[0] !== null &&
    typeof params[0] === 'object' &&
    !Array.isArray(params[0]) &&
    !(params[0] instanceof ArrayBuffer) &&
    !(params[0] instanceof Uint8Array)
  ) {
    return params as any // marker: the Statement methods detect this and convert
  }

  return params.map(serializeValue)
}

function quoteIdentifier(value: string): string {
  return `"${value.replace(/"/g, '""')}"`
}

function sqlErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function logSqlStorageError(
  error: unknown,
  label: string,
  sql: string,
  bindings: readonly SqlStorageValue[]
): void {
  const message = sqlErrorMessage(error)
  if (!message.includes('SQLITE_AUTH') && !message.includes('not authorized')) return
  console.log(
    `[orez-sqlite] ${label} auth error sql=${sql.replace(/\s+/g, ' ').slice(0, 800)} bindings=${bindings.length}`
  )
}

function wrapSqlCursor(
  cursor: SqlStorageCursor,
  sql: string,
  bindings: readonly SqlStorageValue[],
  label: string
): SqlStorageCursor {
  return {
    toArray() {
      try {
        return cursor.toArray()
      } catch (error) {
        logSqlStorageError(error, `${label}.toArray`, sql, bindings)
        throw error
      }
    },
    get rowsRead() {
      try {
        return cursor.rowsRead
      } catch (error) {
        logSqlStorageError(error, `${label}.rowsRead`, sql, bindings)
        throw error
      }
    },
    get rowsWritten() {
      try {
        return cursor.rowsWritten
      } catch (error) {
        logSqlStorageError(error, `${label}.rowsWritten`, sql, bindings)
        throw error
      }
    },
    get columnNames() {
      try {
        return cursor.columnNames
      } catch (error) {
        logSqlStorageError(error, `${label}.columnNames`, sql, bindings)
        throw error
      }
    },
  }
}

function execSql(
  sqlStorage: SqlStorageLike,
  sql: string,
  bindings: readonly SqlStorageValue[] = [],
  label = 'exec'
): SqlStorageCursor {
  try {
    return wrapSqlCursor(sqlStorage.exec(sql, ...bindings), sql, bindings, label)
  } catch (error) {
    logSqlStorageError(error, label, sql, bindings)
    throw error
  }
}

function arrayCursor(
  rows: Record<string, SqlStorageValue>[],
  columnNames: string[]
): SqlStorageCursor {
  return {
    toArray: () => rows,
    rowsRead: rows.length,
    rowsWritten: 0,
    columnNames,
  }
}

function normalizeSql(sql: string): string {
  return sql.replace(/\s+/g, ' ').trim().toLowerCase()
}

function usesSqliteCatalogAlias(sql: string, alias: string): boolean {
  return (
    sql.includes(`from sqlite_master as ${alias}`) ||
    sql.includes(`from sqlite_schema as ${alias}`)
  )
}

function shouldExposeZeroUserTable(name: string): boolean {
  return (
    !name.startsWith('sqlite_') &&
    !name.startsWith('_zero.') &&
    !name.startsWith('_litestream_') &&
    !name.startsWith('_cf_') &&
    !isSnapshotInternalTable(name)
  )
}

function isZeroTableInfoQuery(sql: string): boolean {
  const normalized = normalizeSql(sql)
  return (
    usesSqliteCatalogAlias(normalized, 'm') &&
    normalized.includes('left join pragma_table_info(m.name) as p')
  )
}

function isZeroIndexInfoQuery(sql: string): boolean {
  const normalized = normalizeSql(sql)
  return (
    usesSqliteCatalogAlias(normalized, 'idx') &&
    normalized.includes('join pragma_index_list(idx.tbl_name) as info') &&
    normalized.includes('join pragma_index_xinfo(idx.name) as col')
  )
}

function isZeroUniqueIndexInfoQuery(sql: string): boolean {
  const normalized = normalizeSql(sql)
  return (
    usesSqliteCatalogAlias(normalized, 'idx') &&
    normalized.includes('join pragma_index_list(idx.tbl_name) as info') &&
    normalized.includes('join pragma_index_info(idx.name) as col') &&
    normalized.includes('where idx.tbl_name = ?')
  )
}

function zeroTableInfoRows(
  sqlStorage: SqlStorageLike
): Record<string, SqlStorageValue>[] {
  const tables = execSql(
    sqlStorage,
    `SELECT name FROM sqlite_master WHERE type = 'table'`,
    [],
    'zero.table-info.tables'
  )
    .toArray()
    .map((row) => String(row.name ?? ''))
    .filter(shouldExposeZeroUserTable)

  const rows: Record<string, SqlStorageValue>[] = []
  for (const table of tables) {
    const columns = execSql(
      sqlStorage,
      `PRAGMA table_info(${quoteIdentifier(table)})`,
      [],
      'zero.table-info.columns'
    ).toArray()
    for (const column of columns) {
      rows.push({
        table,
        name: column.name ?? null,
        type: column.type ?? null,
        notNull: column.notnull ?? 0,
        dflt: column.dflt_value ?? null,
        keyPos: column.pk ?? 0,
      })
    }
  }
  return rows
}

function zeroIndexInfoRows(
  sqlStorage: SqlStorageLike
): Record<string, SqlStorageValue>[] {
  const indexes = execSql(
    sqlStorage,
    `SELECT name, tbl_name FROM sqlite_master WHERE type = 'index'`,
    [],
    'zero.index-info.indexes'
  )
    .toArray()
    .map((row) => ({
      indexName: String(row.name ?? ''),
      tableName: String(row.tbl_name ?? ''),
    }))
    .filter(
      ({ indexName, tableName }) => indexName && shouldExposeZeroUserTable(tableName)
    )

  const rows: Array<Record<string, SqlStorageValue> & { seqno: number }> = []
  for (const { indexName, tableName } of indexes) {
    const indexInfo = execSql(
      sqlStorage,
      `PRAGMA index_list(${quoteIdentifier(tableName)})`,
      [],
      'zero.index-info.list'
    )
      .toArray()
      .find((row) => row.name === indexName)
    if (!indexInfo) continue

    const columns = execSql(
      sqlStorage,
      `PRAGMA index_xinfo(${quoteIdentifier(indexName)})`,
      [],
      'zero.index-info.columns'
    ).toArray()
    for (const column of columns) {
      if (Number(column.key ?? 0) !== 1) continue
      rows.push({
        indexName,
        tableName,
        unique: indexInfo.unique ?? 0,
        column: column.name ?? null,
        dir: Number(column.desc ?? 0) === 0 ? 'ASC' : 'DESC',
        seqno: Number(column.seqno ?? 0),
      })
    }
  }

  rows.sort((a, b) => {
    const byIndex = String(a.indexName).localeCompare(String(b.indexName))
    return byIndex === 0 ? a.seqno - b.seqno : byIndex
  })
  return rows.map(({ seqno: _seqno, ...row }) => row)
}

function zeroUniqueIndexInfoRows(
  sqlStorage: SqlStorageLike,
  values: readonly SqlStorageValue[]
): Record<string, SqlStorageValue>[] {
  const tableName = values[0]
  if (typeof tableName !== 'string' || !shouldExposeZeroUserTable(tableName)) {
    return []
  }

  const indexes = execSql(
    sqlStorage,
    `PRAGMA index_list(${quoteIdentifier(tableName)})`,
    [],
    'zero.unique-index-info.list'
  )
    .toArray()
    .map((row) => ({
      name: String(row.name ?? ''),
      unique: Number(row.unique ?? 0),
    }))
    .filter((row) => row.name && row.unique !== 0)

  const rows: Record<string, SqlStorageValue>[] = []
  for (const index of indexes.sort((a, b) => a.name.localeCompare(b.name))) {
    const columns = execSql(
      sqlStorage,
      `PRAGMA index_info(${quoteIdentifier(index.name)})`,
      [],
      'zero.unique-index-info.columns'
    )
      .toArray()
      .sort((a, b) => Number(a.seqno ?? 0) - Number(b.seqno ?? 0))
      .map((row) => row.name)
      .filter((name): name is string => typeof name === 'string')
    rows.push({
      name: index.name,
      columnsJSON: JSON.stringify(columns),
    })
  }
  return rows
}

function maybeZeroIntrospectionCursor(
  sqlStorage: SqlStorageLike,
  sql: string,
  values: readonly SqlStorageValue[]
): SqlStorageCursor | undefined {
  if (isZeroTableInfoQuery(sql)) {
    return arrayCursor(zeroTableInfoRows(sqlStorage), [
      'table',
      'name',
      'type',
      'notNull',
      'dflt',
      'keyPos',
    ])
  }
  if (isZeroIndexInfoQuery(sql)) {
    return arrayCursor(zeroIndexInfoRows(sqlStorage), [
      'indexName',
      'tableName',
      'unique',
      'column',
      'dir',
    ])
  }
  if (isZeroUniqueIndexInfoQuery(sql)) {
    return arrayCursor(zeroUniqueIndexInfoRows(sqlStorage, values), [
      'name',
      'columnsJSON',
    ])
  }
  return undefined
}

function snapshotTableName(prefix: string, table: string): string {
  return `${prefix}_${table.replace(/[^A-Za-z0-9_]/g, '_')}`
}

function isSnapshotInternalTable(name: string): boolean {
  return name.startsWith('_orez_snapshot_')
}

function createSnapshotPrefix(): string {
  const uuid =
    typeof globalThis.crypto?.randomUUID === 'function'
      ? globalThis.crypto.randomUUID().replace(/-/g, '').slice(0, 16)
      : Math.random().toString(36).slice(2, 18)
  return `_orez_snapshot_${Date.now().toString(36)}_${uuid}`
}

function isActiveSnapshotTable(name: string): boolean {
  for (const prefix of activeSnapshotPrefixes) {
    if (name.startsWith(`${prefix}_`)) return true
  }
  return false
}

function cleanupInactiveSnapshotTables(sql: SqlStorageLike): void {
  try {
    const rows = execSql(
      sql,
      `SELECT name FROM sqlite_master
         WHERE type = 'table'
           AND name LIKE '_orez_snapshot_%'`,
      [],
      'cleanup.snapshot-list'
    ).toArray()
    for (const row of rows) {
      const name = String(row.name ?? '')
      if (!isSnapshotInternalTable(name) || isActiveSnapshotTable(name)) continue
      try {
        execSql(
          sql,
          `DROP TABLE IF EXISTS ${quoteIdentifier(name)}`,
          [],
          'cleanup.snapshot-drop'
        )
      } catch {}
    }
  } catch {}
}

function shouldSnapshotTable(name: string): boolean {
  return (
    name !== '__miniflare_do_name' &&
    name !== 'storage' &&
    !name.startsWith('sqlite_') &&
    !name.startsWith('_cf_') &&
    !isSnapshotInternalTable(name)
  )
}

function currentConnectionRole(): SqliteConnectionRole {
  return (globalThis as any).__orez_zero_sqlite_role === 'replica-writer'
    ? 'replica-writer'
    : 'default'
}

function isSqliteCatalogQuery(sql: string): boolean {
  return /\bsqlite_(?:master|schema)\b/i.test(sql)
}

function hasSnapshotCatalogName(row: Record<string, unknown>): boolean {
  for (const key of ['name', 'tbl_name', 'table', 'tableName']) {
    const value = row[key]
    if (typeof value === 'string' && isSnapshotInternalTable(value)) return true
  }
  return false
}

function filterSnapshotCatalogRows<T>(sql: string, rows: T[]): T[] {
  if (!isSqliteCatalogQuery(sql)) return rows
  return rows.filter((row) => !hasSnapshotCatalogName(row as Record<string, unknown>))
}

function replaceIdentifierOutsideLiterals(
  sql: string,
  identifier: string,
  replacement: string
): string {
  let out = ''
  let i = 0

  while (i < sql.length) {
    const ch = sql[i]
    const next = sql[i + 1]

    if (ch === "'") {
      const start = i
      i++
      while (i < sql.length) {
        if (sql[i] === "'" && sql[i + 1] === "'") {
          i += 2
          continue
        }
        if (sql[i] === "'") {
          i++
          break
        }
        i++
      }
      out += sql.slice(start, i)
      continue
    }

    if (ch === '"') {
      const start = i
      let value = ''
      i++
      while (i < sql.length) {
        if (sql[i] === '"' && sql[i + 1] === '"') {
          value += '"'
          i += 2
          continue
        }
        if (sql[i] === '"') {
          i++
          break
        }
        value += sql[i]
        i++
      }
      out += value === identifier ? quoteIdentifier(replacement) : sql.slice(start, i)
      continue
    }

    if (ch === '-' && next === '-') {
      const start = i
      i += 2
      while (i < sql.length && sql[i] !== '\n') i++
      out += sql.slice(start, i)
      continue
    }

    if (ch === '/' && next === '*') {
      const start = i
      i += 2
      while (i < sql.length && !(sql[i] === '*' && sql[i + 1] === '/')) i++
      i = Math.min(sql.length, i + 2)
      out += sql.slice(start, i)
      continue
    }

    if (/[A-Za-z_]/.test(ch)) {
      const start = i
      i++
      while (i < sql.length && /[A-Za-z0-9_]/.test(sql[i])) i++
      const word = sql.slice(start, i)
      out += word === identifier ? replacement : word
      continue
    }

    out += ch
    i++
  }

  return out
}

function rewriteSQLTables(sql: string, tables: Map<string, string>): string {
  let rewritten = sql
  const names = [...tables.keys()].sort((a, b) => b.length - a.length)
  for (const name of names) {
    const snapshot = tables.get(name)!
    rewritten = replaceIdentifierOutsideLiterals(rewritten, name, snapshot)
  }
  return rewritten
}

// -- Statement --

export class Statement<T = Record<string, SqlStorageValue>> {
  readonly source: string
  #sql: SqlStorageLike
  #db: Database

  constructor(sql: SqlStorageLike, db: Database, source: string) {
    this.#sql = sql
    this.#db = db
    // auto-add IF NOT EXISTS to CREATE TABLE/INDEX (shared sqlite in browser)
    this.source = source
      .replace(/CREATE\s+TABLE\s+(?!IF\s+NOT\s+EXISTS)/gi, 'CREATE TABLE IF NOT EXISTS ')
      .replace(/CREATE\s+INDEX\s+(?!IF\s+NOT\s+EXISTS)/gi, 'CREATE INDEX IF NOT EXISTS ')
      .replace(
        /CREATE\s+UNIQUE\s+INDEX\s+(?!IF\s+NOT\s+EXISTS)/gi,
        'CREATE UNIQUE INDEX IF NOT EXISTS '
      )
  }

  // intercept PRAGMAs that sql.js can't handle (wal2, etc.)
  #interceptPragma(): { intercepted: boolean; result?: SqlStorageCursor } {
    const upper = this.source.trimStart().toUpperCase()
    if (!upper.startsWith('PRAGMA')) return { intercepted: false }

    // PRAGMA journal_mode — always report wal2 (sql.js doesn't support WAL)
    if (/PRAGMA\s+journal_mode\s*=/i.test(this.source)) {
      const value = this.source.split('=')[1]?.trim().replace(/['"]/g, '') || 'wal2'
      return {
        intercepted: true,
        result: {
          toArray: () => [{ journal_mode: value }],
          rowsRead: 1,
          rowsWritten: 0,
          columnNames: ['journal_mode'],
        },
      }
    }
    if (/PRAGMA\s+journal_mode\s*$/i.test(this.source)) {
      return {
        intercepted: true,
        result: {
          toArray: () => [{ journal_mode: 'wal2' }],
          rowsRead: 1,
          rowsWritten: 0,
          columnNames: ['journal_mode'],
        },
      }
    }
    return { intercepted: false }
  }

  // resolve named params (@key) → positional (?), or return sql + values as-is
  #resolveParams(params: unknown[]): { sql: string; values: SqlStorageValue[] } {
    const serialized = serializeSqliteParams(params)
    // detect named parameter marker: single non-array object
    const first = serialized[0] as any
    if (
      serialized.length === 1 &&
      first !== null &&
      typeof first === 'object' &&
      !Array.isArray(first) &&
      !(first instanceof ArrayBuffer) &&
      !(first instanceof Uint8Array)
    ) {
      return convertNamedParams(this.source, first as Record<string, unknown>)
    }
    return { sql: this.source, values: serialized }
  }

  run(...params: unknown[]): RunResult {
    if (!this.#db.open) {
      throw new SqliteError('The database connection is not open', 'SQLITE_MISUSE')
    }
    const pragma = this.#interceptPragma()
    if (pragma.intercepted) return { changes: 0, lastInsertRowid: 0 }

    const upper = this.source.trimStart().toUpperCase()
    const isTxCmd =
      upper.startsWith('BEGIN') ||
      upper.startsWith('COMMIT') ||
      upper.startsWith('ROLLBACK') ||
      upper === 'END' ||
      upper.startsWith('END ') ||
      upper.startsWith('SAVEPOINT') ||
      upper.startsWith('RELEASE ')
    const resolved = this.#resolveParams(params)
    const sql = this.#db._rewriteForSnapshot(resolved.sql)
    const values = resolved.values
    const cursor =
      isTxCmd && values.length === 0
        ? this.#db._execTransactionAware(sql, this.#sql)
        : execSql(this.#sql, sql, values, 'statement.run')
    return {
      changes: cursor.rowsWritten,
      lastInsertRowid: 0,
    }
  }

  get(...params: unknown[]): T | undefined {
    if (!this.#db.open) {
      throw new SqliteError('The database connection is not open', 'SQLITE_MISUSE')
    }
    const pragma = this.#interceptPragma()
    if (pragma.intercepted && pragma.result) {
      return pragma.result.toArray()[0] as T
    }

    const resolved = this.#resolveParams(params)
    const sql = this.#db._rewriteForSnapshot(resolved.sql)
    const values = resolved.values
    const cursor =
      maybeZeroIntrospectionCursor(this.#sql, sql, values) ??
      execSql(this.#sql, sql, values, 'statement.get')
    const rows = filterSnapshotCatalogRows(sql, cursor.toArray())
    return (rows[0] as T) ?? undefined
  }

  all(...params: unknown[]): T[] {
    if (!this.#db.open) {
      throw new SqliteError('The database connection is not open', 'SQLITE_MISUSE')
    }
    const pragma = this.#interceptPragma()
    if (pragma.intercepted && pragma.result) {
      return pragma.result.toArray() as T[]
    }

    const resolved = this.#resolveParams(params)
    const sql = this.#db._rewriteForSnapshot(resolved.sql)
    const values = resolved.values
    const cursor =
      maybeZeroIntrospectionCursor(this.#sql, sql, values) ??
      execSql(this.#sql, sql, values, 'statement.all')
    return filterSnapshotCatalogRows(sql, cursor.toArray()) as T[]
  }

  iterate(...params: unknown[]): IterableIterator<T> {
    // eagerly fetch all rows - DO sqlite doesn't support streaming
    const rows = this.all(...params)
    let index = 0
    return {
      next(): IteratorResult<T> {
        if (index < rows.length) {
          return { value: rows[index++], done: false }
        }
        return { value: undefined as unknown as T, done: true }
      },
      [Symbol.iterator]() {
        return this
      },
    }
  }

  /** no-op for compatibility — DO sqlite doesn't need bigint toggle */
  safeIntegers(_toggle?: boolean): this {
    return this
  }

  /** no-op for compatibility — scan status not available in DO */
  scanStatus(): undefined {
    return undefined
  }

  /** no-op for compatibility */
  scanStatusV2(): unknown[] {
    return []
  }

  /** no-op for compatibility */
  scanStatusReset(): void {}

  /** columns() returns column name metadata */
  columns(): Array<{ name: string; column: string | null; table: string | null }> {
    // execute a dummy query to get column names
    const cursor = execSql(
      this.#sql,
      this.#db._rewriteForSnapshot(this.source),
      [],
      'statement.columns'
    )
    return cursor.columnNames.map((name) => ({
      name,
      column: null,
      table: null,
    }))
  }
}

// -- TransactionFunction --

type TransactionFunction<F extends (...args: unknown[]) => unknown> = F & {
  deferred: F
  immediate: F
  exclusive: F
}

// -- Database --

export class Database {
  readonly name: string
  #sql: SqlStorageLike
  #open: boolean
  #inTransaction: boolean
  #snapshotTables: Map<string, string> | null
  #snapshotPrefix: string
  #snapshotCounter: number
  #connectionRole: SqliteConnectionRole

  constructor(sqlOrFilename: SqlStorageLike | string, _options?: { readonly?: boolean }) {
    if (typeof sqlOrFilename === 'string') {
      // when used as a bundler alias for @rocicorp/zero-sqlite3,
      // zero-cache passes a file path. look up DO storage from globalThis.
      const storage = (globalThis as any).__orez_do_sqlite as SqlStorageLike | undefined
      if (!storage) {
        throw new SqliteError(
          'sqlite shim: no DO storage on globalThis.__orez_do_sqlite. ' +
            'register DO storage before importing zero-cache.',
          'SQLITE_ERROR'
        )
      }
      this.#sql = storage
      this.name = sqlOrFilename
    } else {
      this.#sql = sqlOrFilename
      this.name = ':do-storage:'
    }
    this.#open = true
    this.#inTransaction = false
    this.#snapshotTables = null
    this.#snapshotPrefix = createSnapshotPrefix()
    this.#snapshotCounter = 0
    this.#connectionRole = currentConnectionRole()
    activeSnapshotPrefixes.add(this.#snapshotPrefix)
    cleanupInactiveSnapshotTables(this.#sql)

    // expose storage for StatementRunner to access transactionSync
    ;(this as any).__orez_sql = this.#sql
  }

  get open(): boolean {
    return this.#open
  }

  get inTransaction(): boolean {
    return this.#inTransaction
  }

  // transaction nesting: converts nested BEGIN to SAVEPOINT
  // uses shared counter on SqlStorageLike to handle multiple Database instances sharing one sql.js db
  //
  // CF DO SQLite rejects raw BEGIN/COMMIT/ROLLBACK/SAVEPOINT statements —
  // it requires state.storage.transactionSync() instead. when transactionSync
  // is available, all transaction control statements become no-ops here.
  // DO handles atomicity via its own write coalescing mechanism.
  _execTransactionAware(sql: string, sqlStorage: SqlStorageLike): SqlStorageCursor {
    const upper = sql.trimStart().toUpperCase()
    const shared = sqlStorage as any
    const noopCursor: SqlStorageCursor = {
      toArray: () => [],
      rowsRead: 0,
      rowsWritten: 0,
      columnNames: [],
    }

    // CF DO: all transaction control is no-op (DO coalesces writes automatically)
    if (sqlStorage.transactionSync) {
      if (upper.startsWith('SAVEPOINT') || upper.startsWith('RELEASE ')) {
        return noopCursor
      }
      if (upper.startsWith('BEGIN CONCURRENT')) {
        if (this.#connectionRole === 'replica-writer') {
          shared.__txDepth = (shared.__txDepth || 0) + 1
        } else {
          this.#beginSnapshot()
        }
        this.#inTransaction = true
        return noopCursor
      }
      if (upper.startsWith('BEGIN')) {
        shared.__txDepth = (shared.__txDepth || 0) + 1
        this.#inTransaction = true
        return noopCursor
      }
      if (
        upper.startsWith('COMMIT') ||
        upper === 'END' ||
        upper.startsWith('END ') ||
        upper.startsWith('ROLLBACK')
      ) {
        this.#dropSnapshot()
        shared.__txDepth = Math.max(0, (shared.__txDepth || 0) - 1)
        if (shared.__txDepth === 0) this.#inTransaction = false
        return noopCursor
      }
      // non-tx statement inside a "transaction" — just execute normally
      return execSql(sqlStorage, sql, [], 'transaction.do-statement')
    }

    // non-DO path: real BEGIN/COMMIT/ROLLBACK with SAVEPOINT nesting
    if (upper.startsWith('BEGIN')) {
      shared.__txDepth = (shared.__txDepth || 0) + 1
      if (shared.__txDepth > 1) {
        return execSql(
          sqlStorage,
          `SAVEPOINT _nested_${shared.__txDepth}`,
          [],
          'transaction.savepoint'
        )
      }
      this.#inTransaction = true
      return execSql(sqlStorage, sql, [], 'transaction.begin')
    }

    if (upper.startsWith('COMMIT') || upper === 'END' || upper.startsWith('END ')) {
      if ((shared.__txDepth || 0) > 1) {
        const result = execSql(
          sqlStorage,
          `RELEASE SAVEPOINT _nested_${shared.__txDepth}`,
          [],
          'transaction.release'
        )
        shared.__txDepth--
        return result
      }
      shared.__txDepth = 0
      this.#inTransaction = false
      return execSql(sqlStorage, sql, [], 'transaction.commit')
    }

    if (upper.startsWith('ROLLBACK')) {
      if ((shared.__txDepth || 0) > 1) {
        const result = execSql(
          sqlStorage,
          `ROLLBACK TO SAVEPOINT _nested_${shared.__txDepth}`,
          [],
          'transaction.rollback-savepoint'
        )
        shared.__txDepth--
        return result
      }
      shared.__txDepth = 0
      this.#inTransaction = false
      return execSql(sqlStorage, sql, [], 'transaction.rollback')
    }

    return execSql(sqlStorage, sql, [], 'transaction.statement')
  }

  #beginSnapshot(): void {
    this.#dropSnapshot()
    const prefix = `${this.#snapshotPrefix}_${++this.#snapshotCounter}`
    const tables = new Map<string, string>()
    const rows = execSql(
      this.#sql,
      `SELECT name FROM sqlite_master
         WHERE type = 'table'
         ORDER BY name`,
      [],
      'snapshot.table-list'
    ).toArray()

    for (const row of rows) {
      const name = String(row.name ?? '')
      if (!shouldSnapshotTable(name)) continue
      const snapshot = snapshotTableName(prefix, name)
      execSql(
        this.#sql,
        `CREATE TABLE ${quoteIdentifier(snapshot)} AS SELECT * FROM ${quoteIdentifier(name)}`,
        [],
        'snapshot.create'
      )
      tables.set(name, snapshot)
    }

    this.#snapshotTables = tables
  }

  #dropSnapshot(): void {
    const tables = this.#snapshotTables
    if (!tables) return
    this.#snapshotTables = null
    for (const snapshot of tables.values()) {
      try {
        execSql(
          this.#sql,
          `DROP TABLE IF EXISTS ${quoteIdentifier(snapshot)}`,
          [],
          'snapshot.drop'
        )
      } catch {}
    }
  }

  _rewriteForSnapshot(sql: string): string {
    if (!this.#snapshotTables) return sql
    return rewriteSQLTables(sql, this.#snapshotTables)
  }

  /** prepare a statement */
  prepare<T = Record<string, SqlStorageValue>>(sql: string): Statement<T> {
    if (!this.#open) {
      throw new SqliteError('The database connection is not open', 'SQLITE_MISUSE')
    }
    return new Statement<T>(this.#sql, this, sql)
  }

  /**
   * execute pragma statements.
   *
   * DO sqlite supports a subset of pragmas. we handle known ones and
   * return sensible defaults for the rest. the `simple` option returns
   * just the value instead of an array of objects.
   */
  pragma(source: string, options?: { simple?: boolean }): unknown {
    if (!this.#open) {
      throw new SqliteError('The database connection is not open', 'SQLITE_MISUSE')
    }

    const trimmed = source.trim().toLowerCase()

    // skip optimize pragma (not supported / can corrupt)
    if (trimmed.startsWith('optimize')) {
      return options?.simple ? undefined : []
    }

    // return sensible defaults for pragmas that DO SQLite may not support
    // but that zero-cache's zqlite Database constructor expects
    const pragmaDefaults: Record<string, unknown> = {
      page_size: [{ page_size: 4096 }],
      freelist_count: [{ freelist_count: 0 }],
      auto_vacuum: [{ auto_vacuum: 0 }],
      page_count: [{ page_count: 0 }],
      journal_mode: [{ journal_mode: 'wal2' }],
      wal_checkpoint: [{ busy: 0, log: 0, checkpointed: 0 }],
    }

    // parse pragma name and value
    const eqIndex = source.indexOf('=')
    const isSet = eqIndex !== -1

    if (isSet) {
      // intercept journal_mode set — sql.js doesn't support wal/wal2, fake it
      const pragmaName = source.substring(0, eqIndex).trim().toLowerCase()
      const pragmaValue = source
        .substring(eqIndex + 1)
        .trim()
        .replace(/['"]/g, '')
      if (pragmaName === 'journal_mode') {
        return options?.simple ? pragmaValue : [{ journal_mode: pragmaValue }]
      }
      if (
        pragmaName === 'synchronous' ||
        pragmaName === 'busy_timeout' ||
        pragmaName === 'analysis_limit'
      ) {
        return options?.simple ? undefined : []
      }

      // setting a pragma - execute it and return result
      try {
        const cursor = execSql(this.#sql, `PRAGMA ${source}`, [], 'pragma.set')
        const rows = cursor.toArray()
        return options?.simple ? rows[0]?.[Object.keys(rows[0] ?? {})[0]] : rows
      } catch {
        // many pragmas are no-ops in DO sqlite - swallow errors
        return options?.simple ? undefined : []
      }
    }

    // reading a pragma — check defaults first for pragmas we intercept
    const pragmaName = trimmed.split(/[\s(]/)[0]
    const defaultVal = pragmaDefaults[pragmaName]
    if (defaultVal) {
      if (options?.simple) {
        const firstRow = (defaultVal as any[])[0]
        return firstRow ? firstRow[Object.keys(firstRow)[0]] : undefined
      }
      return defaultVal
    }

    // try real execution for unknown pragmas
    try {
      const cursor = execSql(this.#sql, `PRAGMA ${source}`, [], 'pragma.get')
      const rows = cursor.toArray()
      if (rows.length > 0) {
        if (options?.simple) {
          const firstKey = Object.keys(rows[0])[0]
          return rows[0][firstKey]
        }
        return rows
      }
    } catch {
      // sql.js may not support this pragma
    }
    if (defaultVal) {
      if (options?.simple) {
        const arr = defaultVal as Record<string, unknown>[]
        const firstKey = Object.keys(arr[0])[0]
        return arr[0][firstKey]
      }
      return defaultVal
    }
    return options?.simple ? undefined : []
  }

  /**
   * execute one or more sql statements.
   * does not return results — used for DDL and multi-statement strings.
   */
  exec(source: string): this {
    if (!this.#open) {
      throw new SqliteError('The database connection is not open', 'SQLITE_MISUSE')
    }

    // split on semicolons to handle multi-statement strings
    // (DO sqlite exec only takes single statements)
    const statements = source
      .split(';')
      .map((s) => s.trim())
      .filter((s) => s.length > 0)

    for (const stmt of statements) {
      // route transaction control through _execTransactionAware
      // so CF DO's transactionSync short-circuit applies
      const upper = stmt.trimStart().toUpperCase()
      const isTxCmd =
        upper.startsWith('BEGIN') ||
        upper.startsWith('COMMIT') ||
        upper.startsWith('ROLLBACK') ||
        upper === 'END' ||
        upper.startsWith('END ') ||
        upper.startsWith('SAVEPOINT') ||
        upper.startsWith('RELEASE ')
      if (isTxCmd) {
        this._execTransactionAware(stmt, this.#sql)
      } else {
        execSql(this.#sql, this._rewriteForSnapshot(stmt), [], 'database.exec')
      }
    }

    return this
  }

  /**
   * create a transaction wrapper function.
   *
   * returns a function that, when called, wraps `fn` in BEGIN/COMMIT.
   * if `fn` throws, issues ROLLBACK instead.
   *
   * the returned function also has `.deferred()`, `.immediate()`, and
   * `.exclusive()` variants.
   */
  transaction<F extends (...args: unknown[]) => unknown>(fn: F): TransactionFunction<F> {
    const self = this

    const wrapInTransaction: F = ((...args: unknown[]) => {
      // handle nested transactions — just run fn
      if (self.#inTransaction) {
        return fn(...args)
      }

      // DO SQLite requires transactionSync() — raw BEGIN/COMMIT is rejected.
      // fall back to raw SQL only if transactionSync is unavailable.
      if (self.#sql.transactionSync) {
        return self.#sql.transactionSync(() => {
          self.#inTransaction = true
          try {
            return fn(...args)
          } finally {
            self.#inTransaction = false
          }
        })
      }

      // fallback for non-DO environments (tests with bedrock-sqlite)
      execSql(self.#sql, 'BEGIN', [], 'database.transaction.begin')
      self.#inTransaction = true
      try {
        const result = fn(...args)
        execSql(self.#sql, 'COMMIT', [], 'database.transaction.commit')
        self.#inTransaction = false
        return result
      } catch (err) {
        try {
          execSql(self.#sql, 'ROLLBACK', [], 'database.transaction.rollback')
        } catch {
          // swallow rollback errors
        }
        self.#inTransaction = false
        throw err
      }
    }) as F

    // all variants use the same transactionSync wrapper on DO
    const txn = wrapInTransaction as TransactionFunction<F>
    txn.deferred = wrapInTransaction
    txn.immediate = wrapInTransaction
    txn.exclusive = wrapInTransaction

    return txn
  }

  /** close the database connection */
  close(): this {
    this.#dropSnapshot()
    activeSnapshotPrefixes.delete(this.#snapshotPrefix)
    this.#open = false
    return this
  }

  /** no-op for compatibility — unsafe mode not needed in DO */
  unsafeMode(_unsafe?: boolean): this {
    return this
  }

  /** no-op for compatibility */
  defaultSafeIntegers(_toggle?: boolean): this {
    return this
  }
}

// -- StatementRunner --
// matches zero-cache's db/statements.js StatementRunner interface

export class StatementRunner {
  db: Database
  #stmtCache = new Map<string, Statement[]>()
  /** DO SqlStorage for transactionSync — set when Database wraps DO storage */
  #storage: SqlStorageLike | null = null

  constructor(db: Database) {
    this.db = db
    // extract the SqlStorageLike from the Database for transactionSync access.
    // the Database stores it privately, so we pass it via a well-known property.
    this.#storage = (db as any).__orez_sql ?? null
  }

  #getStatement(sql: string): Statement {
    const cached = this.#stmtCache.get(sql)
    if (cached && cached.length > 0) {
      return cached.pop()!
    }
    return this.db.prepare(sql)
  }

  #returnStatement(sql: string, stmt: Statement): void {
    let arr = this.#stmtCache.get(sql)
    if (!arr) {
      arr = []
      this.#stmtCache.set(sql, arr)
    }
    arr.push(stmt)
  }

  run(sql: string, ...args: unknown[]): RunResult {
    const stmt = this.#getStatement(sql)
    try {
      return stmt.run(...args)
    } finally {
      this.#returnStatement(sql, stmt)
    }
  }

  get(sql: string, ...args: unknown[]): unknown {
    const stmt = this.#getStatement(sql)
    try {
      return stmt.get(...args)
    } finally {
      this.#returnStatement(sql, stmt)
    }
  }

  all(sql: string, ...args: unknown[]): unknown[] {
    const stmt = this.#getStatement(sql)
    try {
      return stmt.all(...args)
    } finally {
      this.#returnStatement(sql, stmt)
    }
  }

  // -- transaction methods --
  // DO SQLite rejects raw BEGIN/COMMIT SQL. when transactionSync is
  // available, begin() starts a transactionSync block and commit()
  // lets it complete. for non-DO environments, falls back to raw SQL.

  #txnResult: RunResult = { changes: 0, lastInsertRowid: 0 }

  begin(): RunResult {
    // on DO, transactionSync is closure-based so begin/commit are
    // effectively no-ops — the actual transaction wrapping happens
    // at the Database.transaction() level or implicitly per-statement.
    // we try raw SQL first and swallow the DO rejection.
    if (this.#storage?.transactionSync) {
      return this.#txnResult
    }
    return this.run('BEGIN')
  }

  beginConcurrent(): RunResult {
    if (this.#storage?.transactionSync) {
      return this.run('BEGIN CONCURRENT')
    }
    return this.begin()
  }

  beginImmediate(): RunResult {
    if (this.#storage?.transactionSync) {
      return this.#txnResult
    }
    return this.run('BEGIN IMMEDIATE')
  }

  commit(): RunResult {
    if (this.#storage?.transactionSync) {
      return this.run('COMMIT')
    }
    return this.run('COMMIT')
  }

  rollback(): RunResult {
    if (this.#storage?.transactionSync) {
      return this.run('ROLLBACK')
    }
    return this.run('ROLLBACK')
  }
}

// -- default export --
// matches @rocicorp/zero-sqlite3's default export: the Database class.
// zero-cache's zqlite/src/db.js does:
//   import SQLite3Database, { SqliteError } from "@rocicorp/zero-sqlite3"
//   new SQLite3Database(path, options)

export default Database
