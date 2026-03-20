/**
 * sqlite shim tests.
 *
 * uses a mock SqlStorageLike backed by better-sqlite3 to validate that our
 * shim correctly bridges between the better-sqlite3 api and DO SqlStorage.
 */

// @ts-expect-error - CJS module
import BedrockSqlite from 'bedrock-sqlite'
const BetterSqlite3 = BedrockSqlite.Database
import { describe, it, expect, beforeEach, afterEach } from 'vitest'

import {
  Database,
  Statement,
  StatementRunner,
  SqliteError,
  type SqlStorageLike,
  type SqlStorageCursor,
  type SqlStorageValue,
} from './sqlite.js'

// -- mock SqlStorageLike backed by better-sqlite3 --

function createMockSqlStorage(): SqlStorageLike {
  const nativeDb = new BetterSqlite3(':memory:')

  return {
    exec(query: string, ...bindings: SqlStorageValue[]): SqlStorageCursor {
      const trimmed = query.trim()

      // handle statements that don't return rows
      const isWrite =
        /^(INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|BEGIN|COMMIT|ROLLBACK|PRAGMA|SAVEPOINT|RELEASE|VACUUM)/i.test(
          trimmed
        )

      if (isWrite && !trimmed.toUpperCase().startsWith('PRAGMA')) {
        const stmt = nativeDb.prepare(query)
        const result = stmt.run(...bindings)
        return {
          toArray: () => [],
          get rowsRead() {
            return 0
          },
          get rowsWritten() {
            return result.changes
          },
          get columnNames() {
            return []
          },
        }
      }

      // for pragmas — some are SET (contain =), some are GET
      if (trimmed.toUpperCase().startsWith('PRAGMA')) {
        try {
          const stmt = nativeDb.prepare(query)
          const rows = stmt.all(...bindings)
          return {
            toArray: () => rows as Record<string, SqlStorageValue>[],
            get rowsRead() {
              return rows.length
            },
            get rowsWritten() {
              return 0
            },
            get columnNames() {
              if (rows.length > 0) return Object.keys(rows[0] as object)
              return []
            },
          }
        } catch {
          // pragma that modifies state (e.g., journal_mode = WAL)
          nativeDb.pragma(query.replace(/^PRAGMA\s+/i, ''))
          return {
            toArray: () => [],
            get rowsRead() {
              return 0
            },
            get rowsWritten() {
              return 0
            },
            get columnNames() {
              return []
            },
          }
        }
      }

      // select / other read queries
      const stmt = nativeDb.prepare(query)
      const rows = stmt.all(...bindings)
      const columns = stmt.columns().map((c: { name: string }) => c.name)
      return {
        toArray: () => rows as Record<string, SqlStorageValue>[],
        get rowsRead() {
          return rows.length
        },
        get rowsWritten() {
          return 0
        },
        get columnNames() {
          return columns
        },
      }
    },
    // expose native db for cleanup
    _nativeDb: nativeDb,
  } as SqlStorageLike & { _nativeDb: typeof nativeDb }
}

// -- tests --

describe('SqliteError', () => {
  it('creates error with message and code', () => {
    const err = new SqliteError('table not found', 'SQLITE_ERROR')
    expect(err).toBeInstanceOf(Error)
    expect(err).toBeInstanceOf(SqliteError)
    expect(err.message).toBe('table not found')
    expect(err.code).toBe('SQLITE_ERROR')
    expect(err.name).toBe('SqliteError')
  })

  it('captures stack trace', () => {
    const err = new SqliteError('test', 'SQLITE_MISUSE')
    expect(err.stack).toBeDefined()
    expect(err.stack).toContain('SqliteError')
  })
})

describe('Database', () => {
  let mock: SqlStorageLike & { _nativeDb: any }
  let db: Database

  beforeEach(() => {
    mock = createMockSqlStorage() as SqlStorageLike & { _nativeDb: any }
    db = new Database(mock)
  })

  afterEach(() => {
    mock._nativeDb.close()
  })

  it('constructs with SqlStorageLike', () => {
    expect(db.open).toBe(true)
    expect(db.name).toBe(':do-storage:')
    expect(db.inTransaction).toBe(false)
  })

  it('throws when constructed with a string', () => {
    expect(() => new Database('/path/to/db' as unknown as SqlStorageLike)).toThrow(
      'requires a SqlStorageLike instance'
    )
  })

  it('close sets open to false', () => {
    expect(db.open).toBe(true)
    const ret = db.close()
    expect(db.open).toBe(false)
    expect(ret).toBe(db) // chainable
  })

  it('unsafeMode is a no-op that returns this', () => {
    expect(db.unsafeMode(true)).toBe(db)
    expect(db.unsafeMode(false)).toBe(db)
    expect(db.unsafeMode()).toBe(db)
  })

  it('defaultSafeIntegers is a no-op that returns this', () => {
    expect(db.defaultSafeIntegers(true)).toBe(db)
  })
})

describe('Database.exec', () => {
  let mock: SqlStorageLike & { _nativeDb: any }
  let db: Database

  beforeEach(() => {
    mock = createMockSqlStorage() as SqlStorageLike & { _nativeDb: any }
    db = new Database(mock)
  })

  afterEach(() => {
    mock._nativeDb.close()
  })

  it('executes single statement', () => {
    const ret = db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY)')
    expect(ret).toBe(db) // chainable
  })

  it('executes multiple statements separated by semicolons', () => {
    db.exec(
      'CREATE TABLE t (id INTEGER PRIMARY KEY); CREATE TABLE t2 (id INTEGER PRIMARY KEY)'
    )
    // verify both tables exist
    const r1 = db
      .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='t'")
      .get()
    const r2 = db
      .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='t2'")
      .get()
    expect(r1).toEqual({ name: 't' })
    expect(r2).toEqual({ name: 't2' })
  })

  it('throws on closed database', () => {
    db.close()
    expect(() => db.exec('SELECT 1')).toThrow('not open')
  })
})

describe('Database.prepare / Statement', () => {
  let mock: SqlStorageLike & { _nativeDb: any }
  let db: Database

  beforeEach(() => {
    mock = createMockSqlStorage() as SqlStorageLike & { _nativeDb: any }
    db = new Database(mock)
    db.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)')
  })

  afterEach(() => {
    mock._nativeDb.close()
  })

  it('prepare returns a Statement', () => {
    const stmt = db.prepare('SELECT 1 as x')
    expect(stmt).toBeInstanceOf(Statement)
    expect(stmt.source).toBe('SELECT 1 as x')
  })

  it('throws prepare on closed db', () => {
    db.close()
    expect(() => db.prepare('SELECT 1')).toThrow('not open')
  })

  it('run executes write and returns RunResult', () => {
    const result = db
      .prepare('INSERT INTO items (name, val) VALUES (?, ?)')
      .run('foo', 42)
    expect(result).toHaveProperty('changes')
    expect(result).toHaveProperty('lastInsertRowid')
    expect(result.changes).toBe(1)
  })

  it('get returns first row or undefined', () => {
    db.prepare('INSERT INTO items (name, val) VALUES (?, ?)').run('a', 1)
    db.prepare('INSERT INTO items (name, val) VALUES (?, ?)').run('b', 2)

    const row = db.prepare('SELECT * FROM items WHERE name = ?').get('a')
    expect(row).toEqual({ id: 1, name: 'a', val: 1 })

    const missing = db.prepare('SELECT * FROM items WHERE name = ?').get('zzz')
    expect(missing).toBeUndefined()
  })

  it('all returns all rows', () => {
    db.prepare('INSERT INTO items (name, val) VALUES (?, ?)').run('x', 10)
    db.prepare('INSERT INTO items (name, val) VALUES (?, ?)').run('y', 20)

    const rows = db.prepare('SELECT * FROM items ORDER BY id').all()
    expect(rows).toEqual([
      { id: 1, name: 'x', val: 10 },
      { id: 2, name: 'y', val: 20 },
    ])
  })

  it('all returns empty array when no rows', () => {
    const rows = db.prepare('SELECT * FROM items').all()
    expect(rows).toEqual([])
  })

  it('iterate yields rows lazily', () => {
    db.prepare('INSERT INTO items (name, val) VALUES (?, ?)').run('a', 1)
    db.prepare('INSERT INTO items (name, val) VALUES (?, ?)').run('b', 2)

    const iter = db.prepare('SELECT * FROM items ORDER BY id').iterate()
    const results: unknown[] = []
    for (const row of iter) {
      results.push(row)
    }
    expect(results).toEqual([
      { id: 1, name: 'a', val: 1 },
      { id: 2, name: 'b', val: 2 },
    ])
  })

  it('safeIntegers is a no-op returning this', () => {
    const stmt = db.prepare('SELECT 1 as x')
    expect(stmt.safeIntegers(true)).toBe(stmt)
    expect(stmt.safeIntegers()).toBe(stmt)
  })

  it('statement methods throw on closed db', () => {
    const stmt = db.prepare('SELECT 1 as x')
    db.close()
    expect(() => stmt.run()).toThrow('not open')
    expect(() => stmt.get()).toThrow('not open')
    expect(() => stmt.all()).toThrow('not open')
  })
})

describe('Database.pragma', () => {
  let mock: SqlStorageLike & { _nativeDb: any }
  let db: Database

  beforeEach(() => {
    mock = createMockSqlStorage() as SqlStorageLike & { _nativeDb: any }
    db = new Database(mock)
  })

  afterEach(() => {
    mock._nativeDb.close()
  })

  it('reads a pragma and returns array of objects', () => {
    const result = db.pragma('journal_mode')
    expect(Array.isArray(result)).toBe(true)
    expect((result as unknown[]).length).toBeGreaterThan(0)
  })

  it('reads pragma with simple option', () => {
    const result = db.pragma('journal_mode', { simple: true })
    // should return a scalar value, not an array
    expect(Array.isArray(result)).toBe(false)
    expect(typeof result).toBe('string')
  })

  it('sets a pragma', () => {
    const result = db.pragma('cache_size = 2000')
    // set pragmas return the new value or empty
    expect(Array.isArray(result)).toBe(true)
  })

  it('skips optimize pragma', () => {
    const result = db.pragma('optimize')
    expect(result).toEqual([])

    const simple = db.pragma('optimize', { simple: true })
    expect(simple).toBeUndefined()
  })

  it('throws on closed db', () => {
    db.close()
    expect(() => db.pragma('journal_mode')).toThrow('not open')
  })
})

describe('Database.transaction', () => {
  let mock: SqlStorageLike & { _nativeDb: any }
  let db: Database

  beforeEach(() => {
    mock = createMockSqlStorage() as SqlStorageLike & { _nativeDb: any }
    db = new Database(mock)
    db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY)')
  })

  afterEach(() => {
    mock._nativeDb.close()
  })

  it('commits on success', () => {
    const txn = db.transaction(() => {
      db.prepare('INSERT INTO t VALUES (1)').run()
      db.prepare('INSERT INTO t VALUES (2)').run()
    })

    txn()

    const rows = db.prepare('SELECT * FROM t').all()
    expect(rows).toEqual([{ id: 1 }, { id: 2 }])
  })

  it('rolls back on error', () => {
    const txn = db.transaction(() => {
      db.prepare('INSERT INTO t VALUES (1)').run()
      throw new Error('boom')
    })

    expect(() => txn()).toThrow('boom')

    const rows = db.prepare('SELECT * FROM t').all()
    expect(rows).toEqual([])
  })

  it('inTransaction reflects state', () => {
    expect(db.inTransaction).toBe(false)

    db.transaction(() => {
      expect(db.inTransaction).toBe(true)
    })()

    expect(db.inTransaction).toBe(false)
  })

  it('inTransaction is false after rollback', () => {
    try {
      db.transaction(() => {
        throw new Error('fail')
      })()
    } catch {}

    expect(db.inTransaction).toBe(false)
  })

  it('has deferred/immediate/exclusive variants', () => {
    const txn = db.transaction(() => {
      db.prepare('INSERT INTO t VALUES (99)').run()
    })

    expect(typeof txn.deferred).toBe('function')
    expect(typeof txn.immediate).toBe('function')
    expect(typeof txn.exclusive).toBe('function')
  })

  it('immediate variant works', () => {
    const txn = db.transaction(() => {
      db.prepare('INSERT INTO t VALUES (42)').run()
    })

    txn.immediate()

    const rows = db.prepare('SELECT * FROM t').all()
    expect(rows).toEqual([{ id: 42 }])
  })

  it('nested transaction calls run fn without extra BEGIN', () => {
    const inner = db.transaction(() => {
      db.prepare('INSERT INTO t VALUES (2)').run()
    })

    const outer = db.transaction(() => {
      db.prepare('INSERT INTO t VALUES (1)').run()
      inner() // should NOT issue another BEGIN
      db.prepare('INSERT INTO t VALUES (3)').run()
    })

    outer()

    const rows = db.prepare('SELECT * FROM t ORDER BY id').all()
    expect(rows).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }])
  })

  it('passes arguments through to wrapped function', () => {
    const txn = db.transaction((id: unknown, val: unknown) => {
      db.prepare('INSERT INTO t VALUES (?)').run(id)
      return val
    })

    const result = txn(7, 'hello')
    expect(result).toBe('hello')
    expect(db.prepare('SELECT * FROM t').all()).toEqual([{ id: 7 }])
  })
})

describe('StatementRunner', () => {
  let mock: SqlStorageLike & { _nativeDb: any }
  let db: Database
  let runner: StatementRunner

  beforeEach(() => {
    mock = createMockSqlStorage() as SqlStorageLike & { _nativeDb: any }
    db = new Database(mock)
    db.exec('CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)')
    runner = new StatementRunner(db)
  })

  afterEach(() => {
    mock._nativeDb.close()
  })

  it('run inserts data', () => {
    const result = runner.run('INSERT INTO foo (name) VALUES (?)', 'bar')
    expect(result).toHaveProperty('changes')
    expect(result.changes).toBe(1)
  })

  it('get returns single row', () => {
    runner.run('INSERT INTO foo (name) VALUES (?)', 'baz')
    const row = runner.get('SELECT * FROM foo WHERE name = ?', 'baz')
    expect(row).toEqual({ id: 1, name: 'baz' })
  })

  it('get returns undefined for no match', () => {
    const row = runner.get('SELECT * FROM foo WHERE name = ?', 'nope')
    expect(row).toBeUndefined()
  })

  it('all returns all rows', () => {
    runner.run('INSERT INTO foo (name) VALUES (?)', 'a')
    runner.run('INSERT INTO foo (name) VALUES (?)', 'b')
    const rows = runner.all('SELECT * FROM foo ORDER BY id')
    expect(rows).toEqual([
      { id: 1, name: 'a' },
      { id: 2, name: 'b' },
    ])
  })

  it('begin/commit transaction', () => {
    runner.begin()
    runner.run('INSERT INTO foo (name) VALUES (?)', 'x')
    runner.run('INSERT INTO foo (name) VALUES (?)', 'y')
    runner.commit()

    expect(runner.all('SELECT * FROM foo ORDER BY id')).toEqual([
      { id: 1, name: 'x' },
      { id: 2, name: 'y' },
    ])
  })

  it('begin/rollback discards changes', () => {
    runner.begin()
    runner.run('INSERT INTO foo (name) VALUES (?)', 'x')
    runner.rollback()

    expect(runner.all('SELECT * FROM foo')).toEqual([])
  })

  it('beginConcurrent maps to regular BEGIN', () => {
    runner.beginConcurrent()
    runner.run('INSERT INTO foo (name) VALUES (?)', 'concurrent')
    runner.commit()

    expect(runner.all('SELECT * FROM foo')).toEqual([{ id: 1, name: 'concurrent' }])
  })

  it('beginImmediate works', () => {
    runner.beginImmediate()
    runner.run('INSERT INTO foo (name) VALUES (?)', 'immediate')
    runner.commit()

    expect(runner.all('SELECT * FROM foo')).toEqual([{ id: 1, name: 'immediate' }])
  })

  it('caches prepared statements', () => {
    // run same SQL multiple times — statements should be reused
    runner.run('INSERT INTO foo (name) VALUES (?)', 'a')
    runner.run('INSERT INTO foo (name) VALUES (?)', 'b')
    runner.run('INSERT INTO foo (name) VALUES (?)', 'c')

    expect(runner.all('SELECT count(*) as c FROM foo')).toEqual([{ c: 3 }])
  })
})

describe('StatementRunner: zero-cache replicator pattern', () => {
  let mock: SqlStorageLike & { _nativeDb: any }
  let db: Database
  let runner: StatementRunner

  beforeEach(() => {
    mock = createMockSqlStorage() as SqlStorageLike & { _nativeDb: any }
    db = new Database(mock)
    db.exec(`
      CREATE TABLE issues (
        issueID INTEGER PRIMARY KEY,
        title TEXT,
        _0_version TEXT
      )
    `)
    runner = new StatementRunner(db)
  })

  afterEach(() => {
    mock._nativeDb.close()
  })

  it('simulates zero-cache batch processing', () => {
    // batch 1
    runner.begin()
    runner.run(
      'INSERT INTO issues (issueID, title, _0_version) VALUES (?, ?, ?)',
      1,
      'bug',
      '01'
    )
    runner.run(
      'INSERT INTO issues (issueID, title, _0_version) VALUES (?, ?, ?)',
      2,
      'feat',
      '01'
    )
    runner.commit()

    expect(runner.all('SELECT * FROM issues ORDER BY issueID')).toEqual([
      { issueID: 1, title: 'bug', _0_version: '01' },
      { issueID: 2, title: 'feat', _0_version: '01' },
    ])

    // batch 2
    runner.begin()
    runner.run(
      'INSERT OR REPLACE INTO issues (issueID, title, _0_version) VALUES (?, ?, ?)',
      1,
      'bug fix',
      '02'
    )
    runner.commit()

    expect(runner.get('SELECT title FROM issues WHERE issueID = ?', 1)).toEqual({
      title: 'bug fix',
    })
  })

  it('simulates rollback on conflict', () => {
    runner.begin()
    runner.run(
      'INSERT INTO issues (issueID, title, _0_version) VALUES (?, ?, ?)',
      1,
      'ok',
      '01'
    )
    runner.commit()

    runner.begin()
    try {
      runner.run(
        'INSERT INTO issues (issueID, title, _0_version) VALUES (?, ?, ?)',
        1,
        'dupe',
        '02'
      )
    } catch {
      runner.rollback()
    }

    // original row should still be there
    expect(runner.get('SELECT title FROM issues WHERE issueID = ?', 1)).toEqual({
      title: 'ok',
    })
  })
})
