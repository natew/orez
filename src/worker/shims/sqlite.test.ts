/**
 * sqlite shim tests.
 *
 * uses a mock SqlStorageLike backed by better-sqlite3 to validate that our
 * shim correctly bridges between the better-sqlite3 api and DO SqlStorage.
 */

// @ts-expect-error - CJS module
import BedrockSqlite from 'bedrock-sqlite'
const BetterSqlite3 = BedrockSqlite.Database
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import {
  registerCFInstanceRuntime,
  releaseCFInstanceRuntime,
  sqlitePathForCFInstance,
  type CFInstanceRuntime,
} from '../cf-instance-runtime.js'
import { sweepCFInstanceSqliteHandles } from '../embed-generation.js'
import {
  Database,
  cleanupInactiveSnapshotTablesForCFInstance,
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
    db.close()
    mock._nativeDb.close()
  })

  it('constructs with SqlStorageLike', () => {
    expect(db.open).toBe(true)
    expect(db.name).toBe(':do-storage:')
    expect(db.inTransaction).toBe(false)
  })

  it('rejects a string path without an explicit instance route', () => {
    expect(() => new Database('/path/to/db' as unknown as SqlStorageLike)).toThrow(
      'unroutable zero-cache path'
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

// embed restart contract: string-path construction (the zero-cache alias
// path) registers the handle so a later embed generation can reclaim it.
// object construction (app DOs) must never be registered or swept.
describe('Database embed handle registry', () => {
  let mock: SqlStorageLike & { _nativeDb: any }
  let runtime: CFInstanceRuntime
  let sqlitePath: string

  beforeEach(() => {
    mock = createMockSqlStorage() as SqlStorageLike & { _nativeDb: any }
    runtime = registerCFInstanceRuntime({
      doSqlite: mock,
      env: {},
      instanceId: 'sqlite-registry-test',
      pgPassword: '',
      pgUser: 'user',
    })
    sqlitePath = sqlitePathForCFInstance(runtime.instanceId)
  })

  afterEach(() => {
    sweepCFInstanceSqliteHandles(runtime)
    releaseCFInstanceRuntime(runtime)
    mock._nativeDb.close()
  })

  it('registers string-path handles and deregisters on close', () => {
    const db = new Database(sqlitePath)
    expect(runtime.sqliteHandles.has(db)).toBe(true)
    db.close()
    expect(runtime.sqliteHandles.has(db)).toBe(false)
    expect(runtime.sqliteHandles.size).toBe(0)
  })

  it('does not register object-storage handles (app DO usage)', () => {
    const db = new Database(mock)
    expect(runtime.sqliteHandles.size).toBe(0)
    db.close()
  })

  it('sweep closes leaked handles from a dead generation', () => {
    const leaked = new Database(sqlitePath)
    expect(leaked.open).toBe(true)
    expect(sweepCFInstanceSqliteHandles(runtime)).toBe(1)
    expect(leaked.open).toBe(false)
    expect(runtime.sqliteHandles.size).toBe(0)
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
    db.close()
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
    db.close()
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

  it('no-ops DO-unsupported setup pragma sets', () => {
    const originalExec = mock.exec.bind(mock)
    const forbiddenPragmas: string[] = []
    mock.exec = (query: string, ...bindings: SqlStorageValue[]) => {
      if (/^PRAGMA\s+(synchronous|busy_timeout|analysis_limit)\s*=/i.test(query.trim())) {
        forbiddenPragmas.push(query)
        throw new Error(`not authorized: SQLITE_AUTH: ${query}`)
      }
      return originalExec(query, ...bindings)
    }

    expect(db.pragma('synchronous = NORMAL')).toEqual([])
    expect(db.pragma('busy_timeout = 5000')).toEqual([])
    expect(db.pragma('analysis_limit = 400')).toEqual([])
    expect(db.pragma('synchronous = NORMAL', { simple: true })).toBeUndefined()
    expect(forbiddenPragmas).toEqual([])
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

describe('DO snapshot transactions', () => {
  let mock: SqlStorageLike & { _nativeDb: any; transactionSync: <T>(fn: () => T) => T }
  let live: Database
  let runtime: CFInstanceRuntime
  let snapshot: Database

  beforeEach(() => {
    mock = createMockSqlStorage() as SqlStorageLike & {
      _nativeDb: any
      transactionSync: <T>(fn: () => T) => T
    }
    mock.transactionSync = (fn) => fn()
    mock.sync = vi.fn(async () => {})
    // `live` models the replica-writer — in production it is the only connection
    // that mutates the live replica tables, and copy-on-write snapshots only
    // freeze a table when the replica-writer is about to write to it.
    runtime = registerCFInstanceRuntime({
      doSqlite: mock,
      env: {},
      instanceId: 'sqlite-snapshot-test',
      pgPassword: '',
      pgUser: 'user',
    })
    live = new Database(
      `${sqlitePathForCFInstance(runtime.instanceId)}?orezRole=replica-writer`
    )
    snapshot = new Database(sqlitePathForCFInstance(runtime.instanceId))
    live.exec('CREATE TABLE todo (id TEXT PRIMARY KEY, title TEXT, _0_version TEXT)')
    live.prepare('INSERT INTO todo VALUES (?, ?, ?)').run('1', 'old', '01')
  })

  afterEach(() => {
    live.close()
    snapshot.close()
    releaseCFInstanceRuntime(runtime)
    mock._nativeDb.close()
  })

  it('keeps BEGIN CONCURRENT reads stable until rollback', () => {
    snapshot.prepare('BEGIN CONCURRENT').run()
    expect(snapshot.prepare('SELECT title FROM todo WHERE id = ?').get('1')).toEqual({
      title: 'old',
    })

    live
      .prepare('UPDATE todo SET title = ?, _0_version = ? WHERE id = ?')
      .run('new', '02', '1')

    expect(live.prepare('SELECT title FROM todo WHERE id = ?').get('1')).toEqual({
      title: 'new',
    })
    expect(snapshot.prepare('SELECT title FROM todo WHERE id = ?').get('1')).toEqual({
      title: 'old',
    })

    snapshot.prepare('ROLLBACK').run()
    expect(snapshot.prepare('SELECT title FROM todo WHERE id = ?').get('1')).toEqual({
      title: 'new',
    })
  })

  it('does not copy any table until the writer mutates it (copy-on-write)', () => {
    // the whole point of COW: opening a reader snapshot and reading tables that
    // the writer never touches must NOT create any physical copies. the prior
    // eager-copy emulation copied every table here, scanning index-less copies
    // and amplifying replica reads ~50x. reads must hit the live indexed table.
    snapshot.prepare('BEGIN CONCURRENT').run()
    expect(snapshot.prepare('SELECT title FROM todo WHERE id = ?').get('1')).toEqual({
      title: 'old',
    })
    expect(
      mock
        .exec("SELECT count(*) c FROM sqlite_master WHERE name LIKE '_orez_snapshot_%'")
        .toArray()
    ).toEqual([{ c: 0 }])

    // an UNRELATED writer write (different table) still triggers no copy of todo.
    live.exec('CREATE TABLE other (id TEXT PRIMARY KEY, v TEXT)')
    live.prepare('INSERT INTO other VALUES (?, ?)').run('1', 'x')
    expect(
      mock.exec("SELECT count(*) c FROM sqlite_master WHERE name LIKE '%_todo'").toArray()
    ).toEqual([{ c: 0 }])

    snapshot.prepare('ROLLBACK').run()
  })

  it('copies indexes onto the snapshot so frozen reads stay indexed', () => {
    live.exec('CREATE INDEX todo_title_idx ON todo (title)')
    snapshot.prepare('BEGIN CONCURRENT').run()
    live.prepare('UPDATE todo SET title = ? WHERE id = ?').run('new', '1')

    const snap = mock
      .exec("SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE '%_todo'")
      .toArray()
      .map((r) => String(r.name))
    expect(snap).toHaveLength(1)
    const idxCount = mock
      .exec(
        `SELECT count(*) c FROM sqlite_master WHERE type = 'index' AND tbl_name = ?`,
        snap[0]
      )
      .toArray()
    // copy carries both the pk index and the secondary index of the source.
    expect(Number((idxCount[0] as { c: number }).c)).toBeGreaterThanOrEqual(1)
    expect(snapshot.prepare('SELECT title FROM todo WHERE id = ?').get('1')).toEqual({
      title: 'old',
    })
    snapshot.prepare('ROLLBACK').run()
  })

  it('hides snapshot tables from sqlite catalog queries', () => {
    snapshot.prepare('BEGIN CONCURRENT').run()
    live.prepare('UPDATE todo SET title = ? WHERE id = ?').run('new', '1')

    expect(
      live
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
        .all()
    ).toEqual([{ name: 'todo' }])
  })

  it('does not copy-on-write sqlite internal tables', () => {
    live.exec('CREATE TABLE seq (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT)')
    live.exec('CREATE TABLE _cf_KV (key TEXT PRIMARY KEY, value TEXT)')

    snapshot.prepare('BEGIN CONCURRENT').run()
    // writer mutates internal tables while a reader snapshot is open — these are
    // managed by the engine and must never be frozen into a snapshot copy.
    live.prepare('INSERT INTO seq (title) VALUES (?)').run('row')
    live.prepare('INSERT INTO _cf_KV (key, value) VALUES (?, ?)').run('k', 'v')

    const snapshotTables = mock
      .exec("SELECT name FROM sqlite_master WHERE name LIKE '_orez_snapshot_%'")
      .toArray()
      .map((row) => String(row.name))
    expect(snapshotTables.some((name) => name.endsWith('_sqlite_sequence'))).toBe(false)
    expect(snapshotTables.some((name) => name.endsWith('__cf_KV'))).toBe(false)
  })

  it('does not rewrite table names inside string literals during snapshots', () => {
    live.exec('CREATE TABLE log (id TEXT PRIMARY KEY, table_name TEXT, _0_version TEXT)')
    live.prepare('INSERT INTO log VALUES (?, ?, ?)').run('1', 'todo', '01')
    live.prepare('INSERT INTO log VALUES (?, ?, ?)').run('2', '"todo"', '02')

    snapshot.prepare('BEGIN CONCURRENT').run()
    // force a copy-on-write of `log` (writer deletes a later-id row) so the
    // reader's subsequent SELECTs route to the snapshot copy and exercise the
    // literal-aware table-name rewrite.
    live.prepare('DELETE FROM log WHERE id = ?').run('1')

    expect(
      snapshot.prepare("SELECT table_name FROM log WHERE table_name = 'todo'").get()
    ).toEqual({ table_name: 'todo' })
    expect(
      snapshot.prepare('SELECT table_name FROM log WHERE table_name = ?').get('"todo"')
    ).toEqual({ table_name: '"todo"' })
  })

  it('cleans inactive snapshot tables in bounded flushed batches', async () => {
    mock.exec('CREATE TABLE _orez_snapshot_1_todo AS SELECT * FROM todo')
    const insert = mock._nativeDb.prepare(
      'INSERT INTO _orez_snapshot_1_todo VALUES (?, ?, ?)'
    )
    for (let i = 0; i < 600; i++) insert.run(String(i + 2), `row ${i}`, '02')
    expect(
      mock
        .exec("SELECT name FROM sqlite_master WHERE name = '_orez_snapshot_1_todo'")
        .toArray()
    ).toEqual([{ name: '_orez_snapshot_1_todo' }])

    await cleanupInactiveSnapshotTablesForCFInstance(runtime)

    expect(
      mock
        .exec("SELECT name FROM sqlite_master WHERE name = '_orez_snapshot_1_todo'")
        .toArray()
    ).toEqual([])
    expect(mock.sync).toHaveBeenCalledTimes(4)
  })

  it('does not remove another open connection snapshot', async () => {
    snapshot.prepare('BEGIN CONCURRENT').run()
    // first writer touch on todo copies it into snapshot's namespace (lazy COW).
    live
      .prepare('UPDATE todo SET title = ?, _0_version = ? WHERE id = ?')
      .run('new', '02', '1')
    const snapshotTables = mock
      .exec("SELECT name FROM sqlite_master WHERE name LIKE '_orez_snapshot_%'")
      .toArray()
    expect(snapshotTables).toHaveLength(1)

    await cleanupInactiveSnapshotTablesForCFInstance(runtime)

    // opening another connection must not drop the still-open snapshot's copy.
    const other = new Database(sqlitePathForCFInstance(runtime.instanceId))
    try {
      expect(snapshot.prepare('SELECT title FROM todo WHERE id = ?').get('1')).toEqual({
        title: 'old',
      })
    } finally {
      other.close()
    }
  })

  it('persists BEGIN CONCURRENT writes for the zero-cache replica writer', () => {
    const writer = new Database(
      `${sqlitePathForCFInstance(runtime.instanceId)}?orezRole=replica-writer`
    )

    writer.prepare('BEGIN CONCURRENT').run()
    writer.prepare('INSERT INTO todo VALUES (?, ?, ?)').run('2', 'writer row', '02')
    writer.prepare('COMMIT').run()

    expect(live.prepare('SELECT title FROM todo WHERE id = ?').get('2')).toEqual({
      title: 'writer row',
    })
    expect(
      live
        .prepare("SELECT name FROM sqlite_master WHERE name LIKE '_orez_snapshot_%'")
        .all()
    ).toEqual([])
    writer.close()
  })

  it('does not send raw transaction control SQL to DO storage', () => {
    const originalExec = mock.exec.bind(mock)
    const rawTransactionStatements: string[] = []
    mock.exec = (query: string, ...bindings: SqlStorageValue[]) => {
      if (/^(BEGIN|COMMIT|ROLLBACK|END|SAVEPOINT|RELEASE)\b/i.test(query.trim())) {
        rawTransactionStatements.push(query)
        throw new Error(`raw transaction statement reached DO storage: ${query}`)
      }
      return originalExec(query, ...bindings)
    }

    live.prepare('BEGIN').run()
    live.prepare('SAVEPOINT zero_schema_migration').run()
    live.prepare('RELEASE zero_schema_migration').run()
    live.prepare('COMMIT').run()
    live.exec('SAVEPOINT zero_exec_migration; RELEASE zero_exec_migration')

    expect(rawTransactionStatements).toEqual([])
  })

  it('answers Zero schema introspection without dynamic pragma joins', () => {
    live.exec(`
      CREATE TABLE user_items (
        id TEXT PRIMARY KEY,
        owner TEXT NOT NULL,
        title TEXT DEFAULT 'Untitled'
      );
      CREATE TABLE _cf_KV (key TEXT PRIMARY KEY, value TEXT);
      CREATE UNIQUE INDEX user_items_owner_title_idx ON user_items (owner, title);
    `)

    const originalExec = mock.exec.bind(mock)
    mock.exec = (query: string, ...bindings: SqlStorageValue[]) => {
      if (
        query.includes('pragma_table_info(m.name)') ||
        query.includes('pragma_index_list(idx.tbl_name)') ||
        query.includes('pragma_index_info(idx.name)') ||
        query.includes('pragma_index_xinfo(idx.name)') ||
        query.includes('"_cf_KV"')
      ) {
        throw new Error(`not authorized: SQLITE_AUTH: ${query}`)
      }
      return originalExec(query, ...bindings)
    }

    const tableInfoQuery = `
      SELECT
        m.name as "table",
        p.name as name,
        p.type as type,
        p."notnull" as "notNull",
        p.dflt_value as "dflt",
        p.pk as keyPos
      FROM sqlite_master as m
      LEFT JOIN pragma_table_info(m.name) as p
      WHERE m.type = 'table'
      AND m.name NOT LIKE 'sqlite_%'
      AND m.name NOT LIKE '_zero.%'
      AND m.name NOT LIKE '_litestream_%'
      `
    const columns = live.prepare(tableInfoQuery).all()
    const schemaColumns = live
      .prepare(
        tableInfoQuery.replace('FROM sqlite_master as m', 'FROM sqlite_schema as m')
      )
      .all()

    for (const result of [columns, schemaColumns]) {
      expect(result).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ table: 'todo', name: 'id', keyPos: 1 }),
          expect.objectContaining({
            table: 'user_items',
            name: 'owner',
            type: 'TEXT',
            notNull: 1,
          }),
        ])
      )
      expect(result.some((row) => row.table === '_cf_KV')).toBe(false)
    }

    const indexInfoQuery = `SELECT
         idx.name as indexName,
         idx.tbl_name as tableName,
         info."unique" as "unique",
         col.name as column,
         CASE WHEN col.desc = 0 THEN 'ASC' ELSE 'DESC' END as dir
      FROM sqlite_master as idx
       JOIN pragma_index_list(idx.tbl_name) AS info ON info.name = idx.name
       JOIN pragma_index_xinfo(idx.name) as col
       WHERE idx.type = 'index' AND
             col.key = 1 AND
             idx.tbl_name NOT LIKE '_zero.%'
       ORDER BY idx.name, col.seqno ASC`
    const indexes = live.prepare(indexInfoQuery).all()
    const schemaIndexes = live
      .prepare(
        indexInfoQuery.replace('FROM sqlite_master as idx', 'FROM sqlite_schema as idx')
      )
      .all()

    for (const result of [indexes, schemaIndexes]) {
      expect(result).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            indexName: 'user_items_owner_title_idx',
            tableName: 'user_items',
            unique: 1,
            column: 'owner',
            dir: 'ASC',
          }),
          expect.objectContaining({
            indexName: 'user_items_owner_title_idx',
            tableName: 'user_items',
            unique: 1,
            column: 'title',
            dir: 'ASC',
          }),
        ])
      )
    }

    const uniqueIndexes = live
      .prepare(
        `SELECT idx.name, json_group_array(col.name) as columnsJSON
      FROM sqlite_master as idx
      JOIN pragma_index_list(idx.tbl_name) AS info ON info.name = idx.name
      JOIN pragma_index_info(idx.name) as col
      WHERE idx.tbl_name = ? AND
            idx.type = 'index' AND
            info."unique" != 0
      GROUP BY idx.name
      ORDER BY idx.name`
      )
      .all('user_items')

    expect(uniqueIndexes).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: 'user_items_owner_title_idx',
          columnsJSON: JSON.stringify(['owner', 'title']),
        }),
      ])
    )
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
