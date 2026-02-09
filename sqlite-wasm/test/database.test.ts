import { existsSync, mkdirSync, rmSync } from 'node:fs'
import { resolve } from 'node:path'

import { describe, it, expect, beforeAll, beforeEach, afterEach, afterAll } from 'vitest'

// skip all tests if wasm binary not built yet
const wasmPath = resolve(__dirname, '..', 'dist', 'sqlite3.wasm')
const wasmBuilt = existsSync(wasmPath)

const describeIfBuilt = wasmBuilt ? describe : describe.skip

// lazy import to avoid crash when wasm not built
let Database: any
if (wasmBuilt) {
  const { createRequire } = await import('node:module')
  const require = createRequire(import.meta.url)
  const mod = require(resolve(__dirname, '..', 'dist', 'sqlite3.js'))
  Database = mod.Database
}

const TEST_DIR = resolve(__dirname, '..', '.test-data')
let dbCounter = 0

function testDb(): string {
  return resolve(TEST_DIR, `test-${++dbCounter}-${Date.now()}.db`)
}

beforeAll(() => {
  if (wasmBuilt) mkdirSync(TEST_DIR, { recursive: true })
})

afterAll(() => {
  if (wasmBuilt && existsSync(TEST_DIR)) {
    rmSync(TEST_DIR, { recursive: true, force: true })
  }
})

describeIfBuilt('Database', () => {
  it('opens and closes a database', () => {
    const db = new Database(testDb())
    expect(db.open).toBe(true)
    db.close()
    expect(db.open).toBe(false)
  })

  it('creates in-memory database with :memory:', () => {
    const db = new Database(':memory:')
    expect(db.open).toBe(true)
    db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)')
    db.exec("INSERT INTO t VALUES (1, 'hello')")
    const row = db.prepare('SELECT * FROM t WHERE id = ?').get(1)
    expect(row).toEqual({ id: 1, val: 'hello' })
    db.close()
  })

  it('throws on invalid path with fileMustExist', () => {
    expect(
      () => new Database('/nonexistent/path/db.sqlite', { fileMustExist: true })
    ).toThrow()
  })
})

describeIfBuilt('exec', () => {
  it('executes multiple statements', () => {
    const db = new Database(':memory:')
    db.exec(`
      CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);
      INSERT INTO t VALUES (1, 'alice');
      INSERT INTO t VALUES (2, 'bob');
    `)
    const rows = db.prepare('SELECT * FROM t ORDER BY id').all()
    expect(rows).toEqual([
      { id: 1, name: 'alice' },
      { id: 2, name: 'bob' },
    ])
    db.close()
  })

  it('throws on invalid SQL', () => {
    const db = new Database(':memory:')
    expect(() => db.exec('INVALID SQL')).toThrow()
    db.close()
  })
})

describeIfBuilt('Statement.run', () => {
  it('returns changes and lastInsertRowid', () => {
    const db = new Database(':memory:')
    db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)')
    const result = db.prepare('INSERT INTO t VALUES (?, ?)').run(1, 'hello')
    expect(result.changes).toBe(1)
    expect(result.lastInsertRowid).toBe(1)
    db.close()
  })

  it('supports positional params', () => {
    const db = new Database(':memory:')
    db.exec('CREATE TABLE t (a INTEGER, b TEXT, c REAL)')
    db.prepare('INSERT INTO t VALUES (?, ?, ?)').run(42, 'test', 3.14)
    const row = db.prepare('SELECT * FROM t').get()
    expect(row.a).toBe(42)
    expect(row.b).toBe('test')
    expect(row.c).toBeCloseTo(3.14)
    db.close()
  })

  it('supports named params', () => {
    const db = new Database(':memory:')
    db.exec('CREATE TABLE t (id INTEGER, name TEXT)')
    db.prepare('INSERT INTO t VALUES (@id, @name)').run({ id: 1, name: 'alice' })
    const row = db.prepare('SELECT * FROM t').get()
    expect(row).toEqual({ id: 1, name: 'alice' })
    db.close()
  })

  it('supports named params with $ prefix', () => {
    const db = new Database(':memory:')
    db.exec('CREATE TABLE t (id INTEGER, name TEXT)')
    db.prepare('INSERT INTO t VALUES ($id, $name)').run({ id: 2, name: 'bob' })
    const row = db.prepare('SELECT * FROM t').get()
    expect(row).toEqual({ id: 2, name: 'bob' })
    db.close()
  })

  it('supports null and blob values', () => {
    const db = new Database(':memory:')
    db.exec('CREATE TABLE t (a BLOB, b INTEGER)')
    const buf = Buffer.from([1, 2, 3])
    db.prepare('INSERT INTO t VALUES (?, ?)').run(buf, null)
    const row = db.prepare('SELECT * FROM t').get()
    expect(Buffer.isBuffer(row.a) || row.a instanceof Uint8Array).toBe(true)
    expect(row.b).toBe(null)
    db.close()
  })
})

describeIfBuilt('Statement.get', () => {
  it('returns single row or undefined', () => {
    const db = new Database(':memory:')
    db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)')
    db.exec("INSERT INTO t VALUES (1, 'a')")
    expect(db.prepare('SELECT * FROM t WHERE id = ?').get(1)).toEqual({ id: 1, val: 'a' })
    expect(db.prepare('SELECT * FROM t WHERE id = ?').get(999)).toBeUndefined()
    db.close()
  })
})

describeIfBuilt('Statement.all', () => {
  it('returns all rows', () => {
    const db = new Database(':memory:')
    db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY)')
    db.exec('INSERT INTO t VALUES (1), (2), (3)')
    const rows = db.prepare('SELECT * FROM t ORDER BY id').all()
    expect(rows).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }])
    db.close()
  })
})

describeIfBuilt('Statement.iterate', () => {
  it('yields rows one at a time', () => {
    const db = new Database(':memory:')
    db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY)')
    db.exec('INSERT INTO t VALUES (1), (2), (3)')
    const ids: number[] = []
    for (const row of db.prepare('SELECT * FROM t ORDER BY id').iterate()) {
      ids.push(row.id)
    }
    expect(ids).toEqual([1, 2, 3])
    db.close()
  })
})

describeIfBuilt('Statement.pluck', () => {
  it('returns first column value only', () => {
    const db = new Database(':memory:')
    db.exec('CREATE TABLE t (id INTEGER, name TEXT)')
    db.exec("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")
    const names = db.prepare('SELECT name FROM t ORDER BY id').pluck().all()
    expect(names).toEqual(['alice', 'bob'])
    db.close()
  })
})

describeIfBuilt('Statement.raw', () => {
  it('returns arrays instead of objects', () => {
    const db = new Database(':memory:')
    db.exec('CREATE TABLE t (id INTEGER, name TEXT)')
    db.exec("INSERT INTO t VALUES (1, 'alice')")
    const row = db.prepare('SELECT * FROM t').raw().get()
    expect(row).toEqual([1, 'alice'])
    db.close()
  })
})

describeIfBuilt('Statement.expand', () => {
  it('returns objects keyed by table name', () => {
    const db = new Database(':memory:')
    db.exec('CREATE TABLE t (id INTEGER, name TEXT)')
    db.exec("INSERT INTO t VALUES (1, 'alice')")
    const row = db.prepare('SELECT * FROM t').expand().get()
    expect(row).toEqual({ t: { id: 1, name: 'alice' } })
    db.close()
  })
})

describeIfBuilt('Statement.columns', () => {
  it('returns column metadata', () => {
    const db = new Database(':memory:')
    db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)')
    const cols = db.prepare('SELECT * FROM t').columns()
    expect(cols).toHaveLength(2)
    expect(cols[0].name).toBe('id')
    expect(cols[1].name).toBe('name')
    db.close()
  })
})

describeIfBuilt('Statement reuse', () => {
  it('can run the same statement multiple times', () => {
    const db = new Database(':memory:')
    db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)')
    const stmt = db.prepare('INSERT INTO t VALUES (?, ?)')
    stmt.run(1, 'a')
    stmt.run(2, 'b')
    stmt.run(3, 'c')
    const rows = db.prepare('SELECT * FROM t ORDER BY id').all()
    expect(rows).toHaveLength(3)
    db.close()
  })
})

describeIfBuilt('transaction', () => {
  it('commits on success', () => {
    const db = new Database(':memory:')
    db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY)')
    const insert = db.transaction((ids: number[]) => {
      const stmt = db.prepare('INSERT INTO t VALUES (?)')
      for (const id of ids) stmt.run(id)
    })
    insert([1, 2, 3])
    expect(db.prepare('SELECT count(*) as c FROM t').get().c).toBe(3)
    db.close()
  })

  it('rolls back on error', () => {
    const db = new Database(':memory:')
    db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY)')
    const insert = db.transaction((ids: number[]) => {
      const stmt = db.prepare('INSERT INTO t VALUES (?)')
      for (const id of ids) stmt.run(id)
      throw new Error('abort')
    })
    expect(() => insert([1, 2, 3])).toThrow('abort')
    expect(db.prepare('SELECT count(*) as c FROM t').get().c).toBe(0)
    db.close()
  })

  it('inTransaction reflects transaction state', () => {
    const db = new Database(':memory:')
    expect(db.inTransaction).toBe(false)
    db.exec('BEGIN')
    expect(db.inTransaction).toBe(true)
    db.exec('COMMIT')
    expect(db.inTransaction).toBe(false)
    db.close()
  })
})

describeIfBuilt('pragma', () => {
  it('returns pragma values', () => {
    const db = new Database(':memory:')
    const result = db.pragma('journal_mode')
    expect(result).toBeDefined()
    expect(Array.isArray(result) ? result[0].journal_mode : result).toBeDefined()
    db.close()
  })

  it('sets pragma values', () => {
    const db = new Database(':memory:')
    db.pragma('cache_size = 5000')
    const result = db.pragma('cache_size', { simple: true })
    expect(result).toBe(5000)
    db.close()
  })

  it('returns simple pragma value', () => {
    const db = new Database(':memory:')
    const mode = db.pragma('journal_mode', { simple: true })
    expect(typeof mode).toBe('string')
    db.close()
  })
})

describeIfBuilt('custom functions', () => {
  it('registers scalar function', () => {
    const db = new Database(':memory:')
    db.function('double_it', (x: number) => x * 2)
    const result = db.prepare('SELECT double_it(21) as val').get()
    expect(result.val).toBe(42)
    db.close()
  })

  it('handles string return', () => {
    const db = new Database(':memory:')
    db.function('greet', (name: string) => `hello ${name}`)
    const result = db.prepare("SELECT greet('world') as val").get()
    expect(result.val).toBe('hello world')
    db.close()
  })

  it('handles null return', () => {
    const db = new Database(':memory:')
    db.function('nullable', () => null)
    const result = db.prepare('SELECT nullable() as val').get()
    expect(result.val).toBe(null)
    db.close()
  })
})

describeIfBuilt('aggregate functions', () => {
  it('registers aggregate function', () => {
    const db = new Database(':memory:')
    db.exec('CREATE TABLE t (val INTEGER)')
    db.exec('INSERT INTO t VALUES (1), (2), (3), (4), (5)')
    db.aggregate('sum_squares', {
      start: 0,
      step: (acc: number, val: number) => acc + val * val,
    })
    const result = db.prepare('SELECT sum_squares(val) as ss FROM t').get()
    expect(result.ss).toBe(55) // 1+4+9+16+25
    db.close()
  })
})

describeIfBuilt('file-based database', () => {
  it('persists data across connections', () => {
    const dbPath = testDb()
    const db1 = new Database(dbPath)
    db1.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)')
    db1.exec("INSERT INTO t VALUES (1, 'persisted')")
    db1.close()

    const db2 = new Database(dbPath)
    const row = db2.prepare('SELECT * FROM t WHERE id = 1').get()
    expect(row.val).toBe('persisted')
    db2.close()
  })
})

describeIfBuilt('WAL mode', () => {
  it('can set journal_mode to wal', () => {
    const db = new Database(testDb())
    const result = db.pragma('journal_mode = wal', { simple: true })
    expect(result).toBe('wal')
    db.close()
  })
})

describeIfBuilt('BEGIN CONCURRENT', () => {
  it('supports BEGIN CONCURRENT statement', () => {
    const dbPath = testDb()
    const db = new Database(dbPath)
    // wal2 is required for BEGIN CONCURRENT in bedrock sqlite
    db.pragma('journal_mode = wal2')
    db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)')
    db.exec("INSERT INTO t VALUES (1, 'initial')")

    // BEGIN CONCURRENT should not throw
    db.exec('BEGIN CONCURRENT')
    expect(db.inTransaction).toBe(true)
    db.exec("INSERT INTO t VALUES (2, 'concurrent')")
    db.exec('COMMIT')
    expect(db.inTransaction).toBe(false)

    const rows = db.prepare('SELECT * FROM t ORDER BY id').all()
    expect(rows).toHaveLength(2)
    expect(rows[1].val).toBe('concurrent')
    db.close()
  })
})

describeIfBuilt('WAL2 mode', () => {
  it('can set journal_mode to wal2', () => {
    const db = new Database(testDb())
    const result = db.pragma('journal_mode = wal2', { simple: true })
    expect(result).toBe('wal2')
    db.close()
  })

  it('WAL2 persists after reopen', () => {
    const dbPath = testDb()
    const db1 = new Database(dbPath)
    db1.pragma('journal_mode = wal2')
    db1.exec('CREATE TABLE t (id INTEGER PRIMARY KEY)')
    db1.exec('INSERT INTO t VALUES (1)')
    db1.close()

    const db2 = new Database(dbPath)
    const mode = db2.pragma('journal_mode', { simple: true })
    expect(mode).toBe('wal2')
    const rows = db2.prepare('SELECT * FROM t').all()
    expect(rows).toHaveLength(1)
    db2.close()
  })
})

describeIfBuilt('FTS5', () => {
  it('full-text search works', () => {
    const db = new Database(':memory:')
    db.exec(`
      CREATE VIRTUAL TABLE docs USING fts5(title, body);
      INSERT INTO docs VALUES ('hello world', 'this is a test document');
      INSERT INTO docs VALUES ('goodbye world', 'another test document');
    `)
    const results = db.prepare("SELECT * FROM docs WHERE docs MATCH 'hello'").all()
    expect(results).toHaveLength(1)
    expect(results[0].title).toBe('hello world')
    db.close()
  })
})

describeIfBuilt('JSON functions', () => {
  it('json_extract works', () => {
    const db = new Database(':memory:')
    const result = db.prepare("SELECT json_extract('{\"a\": 42}', '$.a') as val").get()
    expect(result.val).toBe(42)
    db.close()
  })
})

describeIfBuilt('data types', () => {
  it('handles bigint values', () => {
    const db = new Database(':memory:')
    db.exec('CREATE TABLE t (big INTEGER)')
    const big = 9007199254740993n // larger than MAX_SAFE_INTEGER
    db.prepare('INSERT INTO t VALUES (?)').run(big)
    const row = db.prepare('SELECT big FROM t').get()
    expect(row.big).toBe(big)
    db.close()
  })

  it('handles various types', () => {
    const db = new Database(':memory:')
    db.exec('CREATE TABLE t (i INTEGER, r REAL, t TEXT, b BLOB, n)')
    db.prepare('INSERT INTO t VALUES (?, ?, ?, ?, ?)').run(
      42,
      3.14,
      'text',
      Buffer.from([1, 2]),
      null
    )
    const row = db.prepare('SELECT * FROM t').get()
    expect(row.i).toBe(42)
    expect(row.r).toBeCloseTo(3.14)
    expect(row.t).toBe('text')
    expect(row.n).toBe(null)
    db.close()
  })
})
