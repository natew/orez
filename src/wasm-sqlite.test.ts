/**
 * wasm sqlite compatibility tests.
 *
 * validates that bedrock-sqlite (wasm) behaves identically to native
 * @rocicorp/zero-sqlite3 for the patterns zero-cache uses:
 * - statement caching with begin/commit
 * - BEGIN CONCURRENT
 * - unsafeMode
 * - WAL/WAL2 journal modes
 * - pragma support
 *
 * adapted from zero mono: packages/zero-cache/src/db/statements.test.ts,
 * packages/zero-cache/src/db/begin-concurrent.test.ts
 */

import { existsSync, mkdirSync, unlinkSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { resolve } from 'node:path'

// import bedrock-sqlite directly (our wasm build)
// @ts-expect-error - CJS module
import { Database } from 'bedrock-sqlite'
import { describe, test, expect, beforeEach, afterEach } from 'vitest'

// helper: temp db file
function tmpDbPath(name: string): string {
  const dir = resolve(tmpdir(), 'orez-test')
  mkdirSync(dir, { recursive: true })
  return resolve(dir, `${name}-${Date.now()}-${Math.random().toString(36).slice(2)}.db`)
}

function deleteLiteDB(path: string) {
  for (const suffix of ['', '-wal', '-wal2', '-shm', '-journal']) {
    try {
      if (existsSync(path + suffix)) unlinkSync(path + suffix)
    } catch {}
  }
}

// minimal statement cache matching zero-cache's StatementCache
class StatementCache {
  #cache = new Map<string, any[]>()
  #db: any
  #size = 0

  constructor(db: any) {
    this.#db = db
  }

  get size() {
    return this.#size
  }

  get(sql: string) {
    const statements = this.#cache.get(sql)
    if (statements && statements.length > 0) {
      const stmt = statements.pop()!
      this.#size--
      if (statements.length === 0) this.#cache.delete(sql)
      return { sql, statement: stmt }
    }
    return { sql, statement: this.#db.prepare(sql) }
  }

  use(sql: string, cb: (cached: any) => any) {
    const stmt = this.get(sql)
    try {
      return cb(stmt)
    } finally {
      this.return(stmt)
    }
  }

  return(stmt: any) {
    if (!this.#cache.has(stmt.sql)) this.#cache.set(stmt.sql, [])
    this.#cache.get(stmt.sql)!.push(stmt.statement)
    this.#size++
  }
}

// minimal StatementRunner matching zero-cache's
class StatementRunner {
  db: any
  statementCache: StatementCache

  constructor(db: any) {
    this.db = db
    this.statementCache = new StatementCache(db)
  }

  run(sql: string, ...args: any[]) {
    return this.statementCache.use(sql, (c) => c.statement.run(...args))
  }

  get(sql: string, ...args: any[]) {
    return this.statementCache.use(sql, (c) => c.statement.get(...args))
  }

  all(sql: string, ...args: any[]) {
    return this.statementCache.use(sql, (c) => c.statement.all(...args))
  }

  begin() {
    return this.run('BEGIN')
  }

  beginConcurrent() {
    return this.run('BEGIN CONCURRENT')
  }

  beginImmediate() {
    return this.run('BEGIN IMMEDIATE')
  }

  commit() {
    return this.run('COMMIT')
  }

  rollback() {
    return this.run('ROLLBACK')
  }
}

describe('wasm-sqlite: statement caching', () => {
  let db: any
  let runner: StatementRunner

  beforeEach(() => {
    db = new Database(':memory:')
    db.exec('CREATE TABLE foo(id INT PRIMARY KEY)')
    runner = new StatementRunner(db)
  })

  afterEach(() => {
    db?.close()
  })

  test('statement caching with insert and select', () => {
    expect(runner.statementCache.size).toBe(0)
    runner.run('INSERT INTO foo(id) VALUES(?)', 123)
    expect(runner.all('SELECT * FROM foo')).toEqual([{ id: 123 }])
    expect(runner.statementCache.size).toBe(2)

    runner.run('INSERT INTO foo(id) VALUES(?)', 456)
    expect(runner.all('SELECT * FROM foo')).toEqual([{ id: 123 }, { id: 456 }])
    // same statements reused
    expect(runner.statementCache.size).toBe(2)
  })

  test('begin/commit with cached statements', () => {
    runner.begin()
    runner.run('INSERT INTO foo(id) VALUES(?)', 1)
    runner.run('INSERT INTO foo(id) VALUES(?)', 2)
    runner.commit()

    expect(runner.all('SELECT * FROM foo')).toEqual([{ id: 1 }, { id: 2 }])
  })

  test('begin/rollback with cached statements', () => {
    runner.begin()
    runner.run('INSERT INTO foo(id) VALUES(?)', 1)
    runner.run('INSERT INTO foo(id) VALUES(?)', 2)
    runner.rollback()

    expect(runner.all('SELECT * FROM foo')).toEqual([])
  })

  test('multiple transactions with cached statements', () => {
    for (let i = 0; i < 10; i++) {
      runner.begin()
      runner.run('INSERT INTO foo(id) VALUES(?)', i)
      runner.commit()
    }

    const rows = runner.all('SELECT * FROM foo')
    expect(rows).toHaveLength(10)
  })

  test('interleaved reads and writes within transaction', () => {
    runner.begin()
    runner.run('INSERT INTO foo(id) VALUES(?)', 1)
    expect(runner.get('SELECT count(*) as c FROM foo')).toEqual({ c: 1 })
    runner.run('INSERT INTO foo(id) VALUES(?)', 2)
    expect(runner.get('SELECT count(*) as c FROM foo')).toEqual({ c: 2 })
    runner.commit()

    expect(runner.all('SELECT * FROM foo')).toEqual([{ id: 1 }, { id: 2 }])
  })
})

describe('wasm-sqlite: BEGIN CONCURRENT', () => {
  let dbPath: string

  beforeEach(() => {
    dbPath = tmpDbPath('begin-concurrent')
    const conn = new Database(dbPath)
    conn.pragma('journal_mode = WAL')
    conn.pragma('synchronous = NORMAL')
    conn.exec('CREATE TABLE foo(id INTEGER PRIMARY KEY)')
    conn.close()
  })

  afterEach(() => {
    deleteLiteDB(dbPath)
  })

  test('independent concurrent actions before commit', () => {
    const conn1 = new Database(dbPath)
    conn1.pragma('journal_mode = WAL')
    conn1.prepare('BEGIN CONCURRENT').run()

    const conn2 = new Database(dbPath)
    conn2.pragma('journal_mode = WAL')
    conn2.prepare('BEGIN CONCURRENT').run()

    conn1.prepare('INSERT INTO foo(id) VALUES(1)').run()
    expect(conn1.prepare('SELECT * FROM foo').all()).toEqual([{ id: 1 }])

    conn2.prepare('INSERT INTO foo(id) VALUES(2)').run()
    expect(conn2.prepare('SELECT * FROM foo').all()).toEqual([{ id: 2 }])

    conn1.prepare('COMMIT').run()
    conn2.prepare('ROLLBACK').run()

    conn1.close()
    conn2.close()
  })

  test('begin concurrent is deferred', () => {
    const conn1 = new Database(dbPath)
    conn1.pragma('journal_mode = WAL')
    conn1.prepare('BEGIN CONCURRENT').run()

    const conn2 = new Database(dbPath)
    conn2.pragma('journal_mode = WAL')
    conn2.prepare('BEGIN CONCURRENT').run()

    conn1.prepare('INSERT INTO foo(id) VALUES(1)').run()
    conn1.prepare('COMMIT').run()

    expect(conn1.prepare('SELECT * FROM foo').all()).toEqual([{ id: 1 }])

    // conn2's transaction starts here (deferred), sees conn1's commit
    conn2.prepare('INSERT INTO foo(id) VALUES(2)').run()
    expect(conn2.prepare('SELECT * FROM foo').all()).toEqual([{ id: 1 }, { id: 2 }])

    conn2.prepare('ROLLBACK').run()
    conn1.close()
    conn2.close()
  })

  test('simulate immediate - concurrent isolation', () => {
    const conn1 = new Database(dbPath)
    conn1.pragma('journal_mode = WAL')
    conn1.prepare('BEGIN CONCURRENT').run()
    // force transaction start
    expect(conn1.prepare('SELECT * FROM foo').all()).toEqual([])

    const conn2 = new Database(dbPath)
    conn2.pragma('journal_mode = WAL')
    conn2.prepare('BEGIN CONCURRENT').run()
    expect(conn2.prepare('SELECT * FROM foo').all()).toEqual([])

    conn1.prepare('INSERT INTO foo(id) VALUES(1)').run()
    conn1.prepare('COMMIT').run()

    expect(conn1.prepare('SELECT * FROM foo').all()).toEqual([{ id: 1 }])

    // conn2 should NOT see conn1's commit (snapshot isolation)
    conn2.prepare('INSERT INTO foo(id) VALUES(2)').run()
    expect(conn2.prepare('SELECT * FROM foo').all()).toEqual([{ id: 2 }])

    conn2.prepare('ROLLBACK').run()
    conn1.close()
    conn2.close()
  })

  test('begin concurrent with savepoints', () => {
    const conn1 = new Database(dbPath)
    conn1.pragma('journal_mode = WAL')
    conn1.prepare('BEGIN CONCURRENT').run()
    expect(conn1.prepare('SELECT * FROM foo').all()).toEqual([])

    const conn2 = new Database(dbPath)
    conn2.pragma('journal_mode = WAL')
    conn2.prepare('BEGIN CONCURRENT').run()
    expect(conn2.prepare('SELECT * FROM foo').all()).toEqual([])

    conn1.prepare('INSERT INTO foo(id) VALUES(1)').run()
    conn1.prepare('COMMIT').run()

    expect(conn1.prepare('SELECT * FROM foo').all()).toEqual([{ id: 1 }])

    conn2.prepare('SAVEPOINT foobar').run()
    conn2.prepare('INSERT INTO foo(id) VALUES(2)').run()
    expect(conn2.prepare('SELECT * FROM foo').all()).toEqual([{ id: 2 }])

    conn2.prepare('ROLLBACK TO foobar').run()
    expect(conn2.prepare('SELECT * FROM foo').all()).toEqual([])

    conn2.prepare('ROLLBACK').run()
    conn1.close()
    conn2.close()
  })
})

describe('wasm-sqlite: unsafeMode', () => {
  let db: any

  beforeEach(() => {
    db = new Database(':memory:')
    db.exec('CREATE TABLE foo(id INT PRIMARY KEY, val TEXT)')
  })

  afterEach(() => {
    db?.close()
  })

  test('unsafeMode exists and is chainable', () => {
    const ret = db.unsafeMode(true)
    expect(ret).toBe(db)
    db.unsafeMode(false)
  })

  test('commit works with cached statements after unsafeMode', () => {
    db.unsafeMode(true)
    db.pragma('journal_mode = OFF')
    db.pragma('synchronous = OFF')

    const insert = db.prepare('INSERT INTO foo(id, val) VALUES(?, ?)')
    insert.run(1, 'a')
    insert.run(2, 'b')

    const rows = db.prepare('SELECT * FROM foo').all()
    expect(rows).toEqual([
      { id: 1, val: 'a' },
      { id: 2, val: 'b' },
    ])

    db.unsafeMode(false)
  })
})

describe('wasm-sqlite: WAL modes', () => {
  let dbPath: string

  beforeEach(() => {
    dbPath = tmpDbPath('wal-modes')
  })

  afterEach(() => {
    deleteLiteDB(dbPath)
  })

  test('WAL mode works', () => {
    const db = new Database(dbPath)
    const result = db.pragma('journal_mode = WAL')
    expect(result).toEqual([{ journal_mode: 'wal' }])
    db.exec('CREATE TABLE t(id INT)')
    db.exec('INSERT INTO t VALUES(1)')
    expect(db.prepare('SELECT * FROM t').all()).toEqual([{ id: 1 }])
    db.close()
  })

  test('WAL2 mode works', () => {
    const db = new Database(dbPath)
    const result = db.pragma('journal_mode = WAL2')
    expect(result).toEqual([{ journal_mode: 'wal2' }])
    db.exec('CREATE TABLE t(id INT)')
    db.exec('INSERT INTO t VALUES(1)')
    expect(db.prepare('SELECT * FROM t').all()).toEqual([{ id: 1 }])
    db.close()
  })

  test('DELETE mode works', () => {
    const db = new Database(dbPath)
    db.pragma('journal_mode = DELETE')
    db.exec('CREATE TABLE t(id INT)')
    db.exec('INSERT INTO t VALUES(1)')
    expect(db.prepare('SELECT * FROM t').all()).toEqual([{ id: 1 }])
    db.close()
  })
})

describe('wasm-sqlite: pragmas', () => {
  test('busy_timeout', () => {
    const db = new Database(':memory:')
    db.pragma('busy_timeout = 30000')
    const result = db.pragma('busy_timeout')
    expect(result).toEqual([{ timeout: 30000 }])
    db.close()
  })

  test('synchronous', () => {
    const db = new Database(':memory:')
    db.pragma('synchronous = NORMAL')
    const result = db.pragma('synchronous')
    // 1 = NORMAL
    expect(result[0].synchronous).toBe(1)
    db.close()
  })

  test('foreign_keys', () => {
    const db = new Database(':memory:')
    db.pragma('foreign_keys = OFF')
    expect(db.pragma('foreign_keys')).toEqual([{ foreign_keys: 0 }])
    db.close()
  })
})

describe('wasm-sqlite: transaction() helper', () => {
  test('transaction commits on success', () => {
    const db = new Database(':memory:')
    db.exec('CREATE TABLE t(id INT)')

    db.transaction(() => {
      db.prepare('INSERT INTO t VALUES(1)').run()
      db.prepare('INSERT INTO t VALUES(2)').run()
    })()

    expect(db.prepare('SELECT * FROM t').all()).toEqual([{ id: 1 }, { id: 2 }])
    db.close()
  })

  test('transaction rolls back on error', () => {
    const db = new Database(':memory:')
    db.exec('CREATE TABLE t(id INT PRIMARY KEY)')

    try {
      db.transaction(() => {
        db.prepare('INSERT INTO t VALUES(1)').run()
        db.prepare('INSERT INTO t VALUES(1)').run() // duplicate
      })()
    } catch {}

    expect(db.prepare('SELECT * FROM t').all()).toEqual([])
    db.close()
  })
})

describe('wasm-sqlite: scanStatusV2', () => {
  test('scanStatusV2 exists on statements', () => {
    const db = new Database(':memory:')
    db.exec('CREATE TABLE t(id INT)')
    const stmt = db.prepare('SELECT * FROM t')
    expect(typeof stmt.scanStatusV2).toBe('function')
    db.close()
  })
})

describe('wasm-sqlite: zero-cache replicator pattern', () => {
  let dbPath: string

  beforeEach(() => {
    dbPath = tmpDbPath('replicator-pattern')
  })

  afterEach(() => {
    deleteLiteDB(dbPath)
  })

  test('replicator: unsafeMode + journal_mode OFF + vacuum', () => {
    // mirrors zero-cache replicator.js connect() vacuum path
    const db = new Database(dbPath)
    db.pragma('journal_mode = WAL')
    db.exec('CREATE TABLE "_zero.changeLog"(id INT)')
    db.exec('INSERT INTO "_zero.changeLog" VALUES(1)')

    db.unsafeMode(true)
    db.pragma('journal_mode = OFF')
    db.exec('DELETE FROM "_zero.changeLog"')
    db.exec('VACUUM')
    db.unsafeMode(false)

    db.pragma('journal_mode = WAL2')
    db.pragma('busy_timeout = 30000')

    expect(db.prepare('SELECT count(*) as c FROM "_zero.changeLog"').get()).toEqual({
      c: 0,
    })
    db.close()
  })

  test('replicator: begin concurrent + statement cache + commit', () => {
    // mirrors the change-processor transaction flow
    const db = new Database(dbPath)
    db.pragma('journal_mode = WAL2')
    db.pragma('busy_timeout = 30000')
    db.exec(`
      CREATE TABLE issues(
        issueID INTEGER PRIMARY KEY,
        title TEXT,
        _0_version TEXT
      )
    `)

    const runner = new StatementRunner(db)

    // simulate processing a replication message batch
    runner.beginConcurrent()
    runner.run(
      'INSERT INTO issues(issueID, title, _0_version) VALUES(?, ?, ?)',
      1,
      'bug',
      '01'
    )
    runner.run(
      'INSERT INTO issues(issueID, title, _0_version) VALUES(?, ?, ?)',
      2,
      'feat',
      '01'
    )
    runner.commit()

    expect(runner.all('SELECT * FROM issues')).toEqual([
      { issueID: 1, title: 'bug', _0_version: '01' },
      { issueID: 2, title: 'feat', _0_version: '01' },
    ])

    // second batch
    runner.beginConcurrent()
    runner.run(
      'INSERT OR REPLACE INTO issues(issueID, title, _0_version) VALUES(?, ?, ?)',
      1,
      'bug fix',
      '02'
    )
    runner.commit()

    expect(runner.get('SELECT title FROM issues WHERE issueID = ?', 1)).toEqual({
      title: 'bug fix',
    })

    db.close()
  })
})
