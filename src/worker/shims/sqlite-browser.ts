/**
 * sql.js adapter for the sqlite shim.
 *
 * wraps a sql.js Database instance to implement the SqlStorageLike
 * interface that the existing sqlite shim expects. this lets zero-cache
 * use sql.js (SQLite compiled to WASM) in browser Web Workers.
 *
 * usage:
 *   import initSqlJs from 'sql.js'
 *   import { createSqlJsStorage } from 'orez/worker/shims/sqlite-browser'
 *
 *   const SQL = await initSqlJs()
 *   const db = new SQL.Database()
 *   const storage = createSqlJsStorage(db)
 *   globalThis.__orez_do_sqlite = storage
 */

import type { SqlStorageLike, SqlStorageCursor, SqlStorageValue } from './sqlite.js'

// sql.js Database interface (minimal, to avoid hard dependency)
interface SqlJsDatabase {
  run(sql: string, params?: unknown[]): void
  exec(
    sql: string,
    params?: unknown[]
  ): Array<{
    columns: string[]
    values: unknown[][]
  }>
  prepare(sql: string): SqlJsStatement
  getRowsModified(): number
  close(): void
}

interface SqlJsStatement {
  bind(params?: unknown[]): boolean
  step(): boolean
  getAsObject(params?: object): Record<string, unknown>
  getColumnNames(): string[]
  free(): boolean
  reset(): void
}

/**
 * create a SqlStorageLike adapter around a sql.js Database.
 *
 * the returned object can be set on `globalThis.__orez_do_sqlite`
 * or passed directly to the sqlite shim's Database constructor.
 */
export function createSqlJsStorage(sqlJsDb: SqlJsDatabase): SqlStorageLike {
  return {
    exec(query: string, ...bindings: SqlStorageValue[]): SqlStorageCursor {
      const stmt = sqlJsDb.prepare(query)
      try {
        if (bindings.length > 0) {
          // named parameters: single object arg → pass object with prefixed keys to sql.js
          if (
            bindings.length === 1 &&
            bindings[0] !== null &&
            typeof bindings[0] === 'object' &&
            !Array.isArray(bindings[0]) &&
            !(bindings[0] instanceof ArrayBuffer)
          ) {
            // sql.js expects keys with $/:/@  prefix for named params
            // better-sqlite3 accepts keys without prefix — add @ prefix
            const obj = bindings[0] as Record<string, unknown>
            const prefixed: Record<string, unknown> = {}
            for (const [k, v] of Object.entries(obj)) {
              // add @ prefix if not already prefixed
              const key =
                k.startsWith('$') || k.startsWith(':') || k.startsWith('@') ? k : `@${k}`
              prefixed[key] = v
            }
            stmt.bind(prefixed as any)
          } else {
            stmt.bind(bindings as unknown[])
          }
        }

        const rows: Record<string, SqlStorageValue>[] = []
        let columnNames: string[] = []

        while (stmt.step()) {
          const row = stmt.getAsObject() as Record<string, SqlStorageValue>
          if (columnNames.length === 0) {
            columnNames = stmt.getColumnNames()
          }
          rows.push(row)
        }

        // rowsWritten only meaningful for DML
        const rowsWritten = sqlJsDb.getRowsModified()

        return {
          toArray: () => rows,
          rowsRead: rows.length,
          rowsWritten,
          columnNames,
        }
      } finally {
        stmt.free()
      }
    },

    // sql.js transaction handling with nested transaction support
    transactionSync<T>(fn: () => T): T {
      // check if already in a transaction by trying BEGIN
      let inTransaction = false
      try {
        sqlJsDb.run('BEGIN')
      } catch {
        // already in a transaction — use savepoint instead
        inTransaction = true
      }

      if (inTransaction) {
        const sp = `sp_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`
        sqlJsDb.run(`SAVEPOINT "${sp}"`)
        try {
          const result = fn()
          sqlJsDb.run(`RELEASE SAVEPOINT "${sp}"`)
          return result
        } catch (err) {
          try {
            sqlJsDb.run(`ROLLBACK TO SAVEPOINT "${sp}"`)
          } catch {}
          throw err
        }
      }

      try {
        const result = fn()
        sqlJsDb.run('COMMIT')
        return result
      } catch (err) {
        try {
          sqlJsDb.run('ROLLBACK')
        } catch {}
        throw err
      }
    },
  }
}

/**
 * create an in-memory sql.js-like storage for environments where
 * sql.js isn't available. uses a minimal Map-based implementation
 * that handles basic CREATE TABLE / INSERT / SELECT / UPDATE / DELETE.
 *
 * NOTE: this is a very limited stub. for production use, prefer
 * a real sql.js instance. this stub exists so the browser embed
 * can start without sql.js for basic testing.
 */
export function createInMemoryStorage(): SqlStorageLike {
  // use sql.js if available on globalThis (consumer may have loaded it)
  const sqlJsDb = (globalThis as any).__orez_sqljs_db
  if (sqlJsDb) {
    return createSqlJsStorage(sqlJsDb)
  }

  // minimal stub — zero-cache's schema migrations will likely fail
  // but this allows the embed to start for basic PGlite-only use cases
  console.warn(
    '[orez] no sql.js database available. sqlite operations will fail. ' +
      'set globalThis.__orez_sqljs_db or pass a sql.js Database to the embed.'
  )

  return {
    exec(_query: string, ..._bindings: SqlStorageValue[]): SqlStorageCursor {
      return {
        toArray: () => [],
        rowsRead: 0,
        rowsWritten: 0,
        columnNames: [],
      }
    },
  }
}
