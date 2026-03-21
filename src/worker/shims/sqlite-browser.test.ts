import { describe, it, expect } from 'vitest'

import { createSqlJsStorage, createInMemoryStorage } from './sqlite-browser.js'
import { Database, StatementRunner } from './sqlite.js'

// mock sql.js Database for testing
function createMockSqlJsDb() {
  const tables = new Map<string, { columns: string[]; rows: Record<string, unknown>[] }>()
  let rowsModified = 0

  return {
    run(sql: string, params?: unknown[]) {
      // minimal DML support for testing
      const upper = sql.trim().toUpperCase()
      if (
        upper.startsWith('BEGIN') ||
        upper.startsWith('COMMIT') ||
        upper.startsWith('ROLLBACK')
      ) {
        return
      }
      if (upper.startsWith('CREATE TABLE')) {
        const match = sql.match(/CREATE TABLE\s+(?:IF NOT EXISTS\s+)?(\w+)\s*\((.+)\)/i)
        if (match) {
          const name = match[1]
          const cols = match[2].split(',').map((c) => c.trim().split(/\s/)[0])
          if (!tables.has(name)) {
            tables.set(name, { columns: cols, rows: [] })
          }
        }
        return
      }
      if (upper.startsWith('INSERT')) {
        const match = sql.match(/INSERT INTO\s+(\w+)/i)
        if (match) {
          const table = tables.get(match[1])
          if (table && params) {
            const row: Record<string, unknown> = {}
            table.columns.forEach((col, i) => {
              row[col] = params[i] ?? null
            })
            table.rows.push(row)
            rowsModified = 1
          }
        }
        return
      }
    },

    exec(
      sql: string,
      params?: unknown[]
    ): Array<{ columns: string[]; values: unknown[][] }> {
      const upper = sql.trim().toUpperCase()
      if (upper.startsWith('SELECT')) {
        const match = sql.match(/FROM\s+(\w+)/i)
        if (match) {
          const table = tables.get(match[1])
          if (table) {
            return [
              {
                columns: table.columns,
                values: table.rows.map((r) => table.columns.map((c) => r[c])),
              },
            ]
          }
        }
      }
      // for non-SELECT, delegate to run
      this.run(sql, params)
      return []
    },

    prepare(sql: string) {
      const self = this
      let boundParams: unknown[] | undefined
      let resultIndex = 0
      let results: Array<{ columns: string[]; values: unknown[][] }> = []
      let stepped = false

      return {
        bind(params?: unknown[]) {
          boundParams = params
          // for DML (INSERT etc), just run it
          const upper = sql.trim().toUpperCase()
          if (
            upper.startsWith('INSERT') ||
            upper.startsWith('CREATE') ||
            upper.startsWith('BEGIN') ||
            upper.startsWith('COMMIT') ||
            upper.startsWith('ROLLBACK')
          ) {
            self.run(sql, params)
            results = []
          } else {
            results = self.exec(sql, params)
          }
          resultIndex = 0
          stepped = false
          return true
        },
        step() {
          if (!stepped && !results.length) {
            results = self.exec(sql, boundParams)
            stepped = true
          }
          if (results.length === 0 || results[0].values.length <= resultIndex) {
            return false
          }
          return true
        },
        getAsObject() {
          const r = results[0]
          if (!r || resultIndex >= r.values.length) return {}
          const row: Record<string, unknown> = {}
          r.columns.forEach((col, i) => {
            row[col] = r.values[resultIndex][i]
          })
          resultIndex++
          return row
        },
        getColumnNames() {
          return results[0]?.columns || []
        },
        free() {
          return true
        },
        reset() {
          resultIndex = 0
        },
      }
    },

    getRowsModified() {
      return rowsModified
    },

    close() {},
  }
}

describe('createSqlJsStorage', () => {
  it('creates a SqlStorageLike from sql.js Database', () => {
    const sqlJs = createMockSqlJsDb()
    const storage = createSqlJsStorage(sqlJs as any)
    expect(storage).toBeDefined()
    expect(storage.exec).toBeInstanceOf(Function)
  })

  it('exec runs queries and returns cursor', () => {
    const sqlJs = createMockSqlJsDb()
    const storage = createSqlJsStorage(sqlJs as any)

    // insert directly via run (not through storage) to set up test data
    sqlJs.run('CREATE TABLE test (id, name)')
    sqlJs.run('INSERT INTO test VALUES (?, ?)', [1, 'hello'])

    const cursor = storage.exec('SELECT * FROM test')
    const rows = cursor.toArray()
    expect(rows.length).toBe(1)
    expect(rows[0]).toHaveProperty('id')
    expect(rows[0]).toHaveProperty('name')
  })

  it('transactionSync wraps in BEGIN/COMMIT', () => {
    const sqlJs = createMockSqlJsDb()
    const storage = createSqlJsStorage(sqlJs as any)

    sqlJs.run('CREATE TABLE t (v)')

    storage.transactionSync!(() => {
      sqlJs.run('INSERT INTO t VALUES (?)', [42])
    })

    const cursor = storage.exec('SELECT * FROM t')
    expect(cursor.toArray().length).toBe(1)
  })

  it('transactionSync rolls back on error', () => {
    const sqlJs = createMockSqlJsDb()
    const storage = createSqlJsStorage(sqlJs as any)

    expect(() => {
      storage.transactionSync!(() => {
        throw new Error('abort')
      })
    }).toThrow('abort')
  })

  it('works with sqlite shim Database', () => {
    const sqlJs = createMockSqlJsDb()
    const storage = createSqlJsStorage(sqlJs as any)

    // set on globalThis for the Database constructor
    const prev = (globalThis as any).__orez_do_sqlite
    ;(globalThis as any).__orez_do_sqlite = storage
    try {
      const db = new Database(':browser-sqlite:')
      expect(db.open).toBe(true)
      expect(db.name).toBe(':browser-sqlite:')
    } finally {
      if (prev) (globalThis as any).__orez_do_sqlite = prev
      else delete (globalThis as any).__orez_do_sqlite
    }
  })
})

describe('createInMemoryStorage', () => {
  it('creates a stub storage with exec', () => {
    const prev = (globalThis as any).__orez_sqljs_db
    delete (globalThis as any).__orez_sqljs_db
    try {
      const storage = createInMemoryStorage()
      expect(storage).toBeDefined()
      const cursor = storage.exec('SELECT 1')
      expect(cursor.toArray()).toEqual([])
    } finally {
      if (prev) (globalThis as any).__orez_sqljs_db = prev
    }
  })

  it('uses globalThis.__orez_sqljs_db if available', () => {
    const sqlJs = createMockSqlJsDb()
    ;(globalThis as any).__orez_sqljs_db = sqlJs
    try {
      const storage = createInMemoryStorage()
      // just verify it uses the sqlJs db (not the stub)
      expect(storage.transactionSync).toBeInstanceOf(Function)
    } finally {
      delete (globalThis as any).__orez_sqljs_db
    }
  })
})
