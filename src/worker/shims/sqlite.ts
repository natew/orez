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

// -- Statement --

export class Statement<T = Record<string, SqlStorageValue>> {
  readonly source: string
  #sql: SqlStorageLike
  #db: Database

  constructor(sql: SqlStorageLike, db: Database, source: string) {
    this.#sql = sql
    this.#db = db
    this.source = source
  }

  run(...params: unknown[]): RunResult {
    if (!this.#db.open) {
      throw new SqliteError('The database connection is not open', 'SQLITE_MISUSE')
    }
    const cursor = this.#sql.exec(this.source, ...(params as SqlStorageValue[]))
    return {
      changes: cursor.rowsWritten,
      lastInsertRowid: 0,
    }
  }

  get(...params: unknown[]): T | undefined {
    if (!this.#db.open) {
      throw new SqliteError('The database connection is not open', 'SQLITE_MISUSE')
    }
    const cursor = this.#sql.exec(this.source, ...(params as SqlStorageValue[]))
    const rows = cursor.toArray()
    return (rows[0] as T) ?? undefined
  }

  all(...params: unknown[]): T[] {
    if (!this.#db.open) {
      throw new SqliteError('The database connection is not open', 'SQLITE_MISUSE')
    }
    const cursor = this.#sql.exec(this.source, ...(params as SqlStorageValue[]))
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

  constructor(sqlOrFilename: SqlStorageLike | string, _options?: { readonly?: boolean }) {
    if (typeof sqlOrFilename === 'string') {
      throw new SqliteError(
        'Database constructor requires a SqlStorageLike instance in DO environment',
        'SQLITE_ERROR'
      )
    }
    this.#sql = sqlOrFilename
    this.name = ':do-storage:'
    this.#open = true
    this.#inTransaction = false
  }

  get open(): boolean {
    return this.#open
  }

  get inTransaction(): boolean {
    return this.#inTransaction
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
      if (options?.simple) {
        if (rows.length === 0) return undefined
        const firstKey = Object.keys(rows[0])[0]
        return rows[0][firstKey]
      }
      return rows
    } catch {
      return options?.simple ? undefined : []
    }
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

    const wrapWithBegin = (beginStmt: string): F => {
      const wrapped = ((...args: unknown[]) => {
        // handle nested transactions - if already in a transaction, just run fn
        if (self.#inTransaction) {
          return fn(...args)
        }

        self.#sql.exec(beginStmt)
        self.#inTransaction = true
        try {
          const result = fn(...args)
          self.#sql.exec('COMMIT')
          self.#inTransaction = false
          return result
        } catch (err) {
          // attempt rollback, but don't mask original error
          try {
            self.#sql.exec('ROLLBACK')
          } catch {
            // swallow rollback errors
          }
          self.#inTransaction = false
          throw err
        }
      }) as F
      return wrapped
    }

    const txn = wrapWithBegin('BEGIN') as TransactionFunction<F>
    txn.deferred = wrapWithBegin('BEGIN DEFERRED')
    txn.immediate = wrapWithBegin('BEGIN IMMEDIATE')
    txn.exclusive = wrapWithBegin('BEGIN EXCLUSIVE')

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

  constructor(db: Database) {
    this.db = db
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

  begin(): RunResult {
    return this.run('BEGIN')
  }

  beginConcurrent(): RunResult {
    // DO sqlite doesn't support BEGIN CONCURRENT — map to regular BEGIN
    return this.run('BEGIN')
  }

  beginImmediate(): RunResult {
    return this.run('BEGIN IMMEDIATE')
  }

  commit(): RunResult {
    return this.run('COMMIT')
  }

  rollback(): RunResult {
    return this.run('ROLLBACK')
  }
}
