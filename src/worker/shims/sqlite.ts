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
function serializeSqliteParams(params: unknown[]): SqlStorageValue[] {
  // named parameters: .run({key: value}) → pass values in order
  // sql.js handles this natively, but DO SqlStorage doesn't
  // detect named params: single object arg with non-array, non-buffer
  if (params.length === 1 && params[0] !== null && typeof params[0] === 'object'
    && !Array.isArray(params[0]) && !(params[0] instanceof ArrayBuffer)
    && !(params[0] instanceof Uint8Array)) {
    // return the object as-is for sql.js named parameter binding
    return params as any
  }

  return params.map((p) => {
    if (p === null || p === undefined) return null
    if (typeof p === 'string' || typeof p === 'number') return p
    if (typeof p === 'boolean') return p ? 1 : 0
    if (typeof p === 'bigint') return Number(p)
    if (p instanceof ArrayBuffer || p instanceof Uint8Array) return p as any
    // objects (including Date, arrays) → JSON string
    if (typeof p === 'object') return JSON.stringify(p)
    return String(p)
  })
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
      .replace(/CREATE\s+UNIQUE\s+INDEX\s+(?!IF\s+NOT\s+EXISTS)/gi, 'CREATE UNIQUE INDEX IF NOT EXISTS ')
  }

  run(...params: unknown[]): RunResult {
    if (!this.#db.open) {
      throw new SqliteError('The database connection is not open', 'SQLITE_MISUSE')
    }
    // use transaction-aware execution for BEGIN/COMMIT/ROLLBACK
    const upper = this.source.trimStart().toUpperCase()
    const isTxCmd = upper.startsWith('BEGIN') || upper.startsWith('COMMIT') || upper.startsWith('ROLLBACK') || upper === 'END' || upper.startsWith('END ')
    const serialized = serializeSqliteParams(params)
    const cursor = isTxCmd && serialized.length === 0
      ? this.#db._execTransactionAware(this.source, this.#sql)
      : this.#sql.exec(this.source, ...serialized)
    return {
      changes: cursor.rowsWritten,
      lastInsertRowid: 0,
    }
  }

  get(...params: unknown[]): T | undefined {
    if (!this.#db.open) {
      throw new SqliteError('The database connection is not open', 'SQLITE_MISUSE')
    }
    const cursor = this.#sql.exec(this.source, ...serializeSqliteParams(params))
    const rows = cursor.toArray()
    return (rows[0] as T) ?? undefined
  }

  all(...params: unknown[]): T[] {
    if (!this.#db.open) {
      throw new SqliteError('The database connection is not open', 'SQLITE_MISUSE')
    }
    const cursor = this.#sql.exec(this.source, ...serializeSqliteParams(params))
    return cursor.toArray() as T[]
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
    const cursor = this.#sql.exec(this.source)
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
  #txDepth: number = 0

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
  _execTransactionAware(sql: string, sqlStorage: SqlStorageLike): SqlStorageCursor {
    const upper = sql.trimStart().toUpperCase()
    const shared = sqlStorage as any

    if (upper.startsWith('BEGIN')) {
      shared.__txDepth = (shared.__txDepth || 0) + 1
      if (shared.__txDepth > 1) {
        return sqlStorage.exec(`SAVEPOINT _nested_${shared.__txDepth}`)
      }
      this.#inTransaction = true
      return sqlStorage.exec(sql)
    }

    if (upper.startsWith('COMMIT') || upper === 'END' || upper.startsWith('END ')) {
      if ((shared.__txDepth || 0) > 1) {
        const result = sqlStorage.exec(`RELEASE SAVEPOINT _nested_${shared.__txDepth}`)
        shared.__txDepth--
        return result
      }
      shared.__txDepth = 0
      this.#inTransaction = false
      return sqlStorage.exec(sql)
    }

    if (upper.startsWith('ROLLBACK')) {
      if ((shared.__txDepth || 0) > 1) {
        const result = sqlStorage.exec(`ROLLBACK TO SAVEPOINT _nested_${shared.__txDepth}`)
        shared.__txDepth--
        return result
      }
      shared.__txDepth = 0
      this.#inTransaction = false
      return sqlStorage.exec(sql)
    }

    return sqlStorage.exec(sql)
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
      journal_mode: [{ journal_mode: 'wal' }],
      wal_checkpoint: [{ busy: 0, log: 0, checkpointed: 0 }],
    }

    // parse pragma name and value
    const eqIndex = source.indexOf('=')
    const isSet = eqIndex !== -1

    if (isSet) {
      // setting a pragma - execute it and return result
      try {
        const cursor = this.#sql.exec(`PRAGMA ${source}`)
        const rows = cursor.toArray()
        return options?.simple ? rows[0]?.[Object.keys(rows[0] ?? {})[0]] : rows
      } catch {
        // many pragmas are no-ops in DO sqlite - swallow errors
        return options?.simple ? undefined : []
      }
    }

    // reading a pragma
    try {
      const cursor = this.#sql.exec(`PRAGMA ${source}`)
      const rows = cursor.toArray()
      if (rows.length > 0) {
        if (options?.simple) {
          const firstKey = Object.keys(rows[0])[0]
          return rows[0][firstKey]
        }
        return rows
      }
      // empty result — fall through to defaults
    } catch {
      // DO SQLite may not support this pragma — fall through to defaults
    }

    // return known defaults or empty
    const pragmaName = trimmed.split(/[\s(]/)[0]
    const defaultVal = pragmaDefaults[pragmaName]
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
      this.#sql.exec(stmt)
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
      self.#sql.exec('BEGIN')
      self.#inTransaction = true
      try {
        const result = fn(...args)
        self.#sql.exec('COMMIT')
        self.#inTransaction = false
        return result
      } catch (err) {
        try {
          self.#sql.exec('ROLLBACK')
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
      return this.#txnResult
    }
    return this.run('COMMIT')
  }

  rollback(): RunResult {
    if (this.#storage?.transactionSync) {
      return this.#txnResult
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
