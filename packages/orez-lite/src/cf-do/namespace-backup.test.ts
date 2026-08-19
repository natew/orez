// @ts-expect-error - CJS module
import BedrockSqlite from 'bedrock-sqlite'
import { describe, expect, it } from 'vitest'

import {
  createNamespaceBackupManager,
  type NamespaceBackupBucket,
  type NamespaceBackupStatement,
} from './namespace-backup.js'

function stream(text: string): ReadableStream<Uint8Array> {
  const bytes = new TextEncoder().encode(text)
  return new ReadableStream({
    start(controller) {
      controller.enqueue(bytes)
      controller.close()
    },
  })
}

function bucketWith(key: string, dump: string) {
  let reads = 0
  const bucket: NamespaceBackupBucket = {
    async get(requestedKey) {
      if (requestedKey !== key) return null
      reads++
      return {
        body: stream(dump),
        async json() {
          return JSON.parse(dump)
        },
      }
    },
    async createMultipartUpload() {
      throw new Error('not used')
    },
    async put() {},
    async list() {
      return { objects: [] }
    },
    async delete() {},
  }
  return { bucket, reads: () => reads }
}

function dump(lines: unknown[]): string {
  return `${lines.map((line) => JSON.stringify(line)).join('\n')}\n`
}

function writableBucket() {
  const pointers = new Map<string, string>()
  const bucket: NamespaceBackupBucket = {
    async get(key) {
      const value = pointers.get(key)
      return value
        ? {
            body: stream(value),
            async json() {
              return JSON.parse(value)
            },
          }
        : null
    },
    async createMultipartUpload(key) {
      const parts = new Map<number, Uint8Array>()
      return {
        async uploadPart(partNumber, value) {
          parts.set(partNumber, value)
          return { partNumber }
        },
        async complete() {
          const ordered = [...parts.entries()]
            .sort(([left], [right]) => left - right)
            .map(([, value]) => value)
          const bytes = new Uint8Array(
            ordered.reduce((total, value) => total + value.byteLength, 0)
          )
          let offset = 0
          for (const value of ordered) {
            bytes.set(value, offset)
            offset += value.byteLength
          }
          pointers.set(key, new TextDecoder().decode(bytes))
        },
        async abort() {},
      }
    },
    async put(key, value) {
      pointers.set(key, value)
    },
    async list() {
      return { objects: [] }
    },
    async delete() {},
  }
  return { bucket, pointers }
}

const BetterSqlite3 = BedrockSqlite.Database

// every export read runs in one session. these tests exercise the scan itself,
// so the session is a pass-through over the same query callback.
function backupManager<Env>(options: any) {
  return createNamespaceBackupManager<Env>({
    readSession: (env: any, namespace: string, work: any) =>
      work((sql: string, params: readonly unknown[] = []) =>
        options.query(env, namespace, sql, params)
      ),
    ...options,
  })
}

function realSqliteManager(
  db: InstanceType<typeof BetterSqlite3>,
  bucket: NamespaceBackupBucket,
  batchSizes: number[] = [],
  metrics?: { queries: string[]; rowsRead: number }
) {
  return backupManager({
    format: 'test-v3',
    markerTable: '_test_backup_meta',
    excludedTables: ['_test_backup_meta'],
    files: () => bucket,
    query: async (_env, _namespace, sql, params) => {
      metrics?.queries.push(sql)
      const statement = db.prepare(sql)
      if (statement.reader) {
        const rows = statement.all(...params)
        if (metrics) metrics.rowsRead += rows.length
        return rows
      }
      statement.run(...params)
      return []
    },
    batch: async (_env, _namespace, statements) => {
      batchSizes.push(statements.length)
      db.exec('BEGIN')
      try {
        for (const statement of statements) {
          db.prepare(statement.sql).run(...(statement.params ?? []))
        }
        db.exec('COMMIT')
      } catch (error) {
        db.exec('ROLLBACK')
        throw error
      }
    },
    listNamespaces: async () => ['singleton'],
  })
}

describe('namespace backup export', () => {
  it('records per-table row counts in the durable summary without another scan', async () => {
    const stored = writableBucket()
    const manager = backupManager({
      format: 'test-v3',
      markerTable: '_test_backup_meta',
      files: () => stored.bucket,
      query: async (_env, _namespace, sql, params) => {
        if (sql.includes('SELECT write_seq')) return [{ write_seq: 9 }]
        if (sql.includes('sqlite_master')) {
          return [
            { name: 'empty', sql: 'CREATE TABLE empty (id TEXT)', type: 'table' },
            { name: 'message', sql: 'CREATE TABLE message (id TEXT)', type: 'table' },
          ]
        }
        if (sql.includes('FROM "empty"')) return []
        if (sql.includes('FROM "message"')) {
          return Number(params[0]) === 0 ? [{ __orez_backup_rowid: 1, id: 'one' }] : []
        }
        throw new Error(`unexpected export query: ${sql}`)
      },
      batch: async () => {},
      listNamespaces: async () => ['singleton'],
    })

    const summary = await manager.exportNamespace({}, 'singleton')

    expect(summary).toMatchObject({
      marker: 9,
      tables: 2,
      rows: 1,
      tableRows: { empty: 0, message: 1 },
    })
    expect(
      JSON.parse(stored.pointers.get('backups/singleton/latest.json') ?? '{}').tableRows
    ).toEqual({ empty: 0, message: 1 })
  })

  it('pages a WITHOUT ROWID table by its composite primary key', async () => {
    const stored = writableBucket()
    const rows = Array.from({ length: 201 }, (_, index) => ({
      tenant: 'tenant-one',
      id: `message-${String(index).padStart(3, '0')}`,
      body: `body-${index}`,
    }))
    const pageQueries: Array<{ sql: string; params: readonly unknown[] }> = []
    const manager = backupManager({
      format: 'test-v3',
      markerTable: '_test_backup_meta',
      files: () => stored.bucket,
      query: async (_env, _namespace, sql, params) => {
        if (sql.includes('SELECT write_seq')) return [{ write_seq: 4 }]
        if (sql.includes('sqlite_master')) {
          return [
            {
              name: 'message',
              sql: 'CREATE TABLE message (tenant TEXT, id TEXT, body TEXT, PRIMARY KEY (tenant, id)) WITHOUT ROWID',
              type: 'table',
            },
          ]
        }
        if (sql.startsWith('PRAGMA table_info')) {
          return [
            { name: 'tenant', pk: 1 },
            { name: 'id', pk: 2 },
            { name: 'body', pk: 0 },
          ]
        }
        if (sql.includes('FROM "message"')) {
          pageQueries.push({ sql, params })
          return params.length === 1 ? rows.slice(0, 200) : rows.slice(200)
        }
        throw new Error(`unexpected export query: ${sql}`)
      },
      batch: async () => {},
      listNamespaces: async () => ['singleton'],
    })

    const summary = await manager.exportNamespace({}, 'singleton')

    expect(summary).toMatchObject({
      marker: 4,
      tables: 1,
      rows: 201,
      tableRows: { message: 201 },
    })
    expect(pageQueries).toEqual([
      {
        sql: 'SELECT * FROM "message" ORDER BY "tenant", "id" LIMIT ?',
        params: [200],
      },
      {
        sql: 'SELECT * FROM "message" WHERE ("tenant", "id") > (?, ?) ORDER BY "tenant", "id" LIMIT ?',
        params: ['tenant-one', 'message-199', 1000],
      },
    ])
  })

  it('pages every composite WITHOUT ROWID key against real SQLite', async () => {
    const db = new BetterSqlite3(':memory:')
    const stored = writableBucket()
    db.exec(
      'CREATE TABLE message (tenant TEXT, id TEXT, body TEXT, PRIMARY KEY (tenant, id)) WITHOUT ROWID'
    )
    db.exec('CREATE TABLE _test_backup_meta (id INTEGER PRIMARY KEY, write_seq INTEGER)')
    db.exec('INSERT INTO _test_backup_meta VALUES (1, 7)')
    const expected = Array.from({ length: 1_201 }, (_, index) => ({
      tenant: `tenant-${String(Math.floor(index / 300)).padStart(2, '0')}`,
      id: `message-${String(index % 300).padStart(3, '0')}`,
      body: `body-${index}`,
    })).sort((left, right) =>
      left.tenant === right.tenant
        ? left.id.localeCompare(right.id)
        : left.tenant.localeCompare(right.tenant)
    )
    const insert = db.prepare('INSERT INTO message VALUES (?, ?, ?)')
    db.exec('BEGIN')
    for (const row of expected) insert.run(row.tenant, row.id, row.body)
    db.exec('COMMIT')

    const summary = await realSqliteManager(db, stored.bucket).exportNamespace(
      {},
      'singleton'
    )
    const exported = (stored.pointers.get(summary.key) ?? '')
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line))
      .filter((entry) => entry.kind === 'rows' && entry.table === 'message')
      .flatMap((entry) => entry.rows)

    expect(summary).toMatchObject({ marker: 7, rows: 1_201 })
    expect(exported).toEqual(expected)
    db.close()
  })
})

describe('namespace backup export consistency', () => {
  it('dumps a state that some transaction actually produced', async () => {
    const db = new BetterSqlite3(':memory:')
    db.exec(
      'CREATE TABLE account (id TEXT PRIMARY KEY, balance INTEGER NOT NULL);' +
        'CREATE TABLE ledger (id TEXT PRIMARY KEY, account_id TEXT NOT NULL REFERENCES account(id), amount INTEGER NOT NULL)'
    )
    db.exec("INSERT INTO account VALUES ('a1', 0)")

    // The durable object admits no write session while a read session is open
    // (worker.ts canAdmitApplicationSqlSession). A scan that owns one session
    // for its whole length therefore cannot observe half of a deposit; a scan
    // that opens a session per statement can.
    let readersOpen = 0
    const queuedWrites: Array<() => void> = []
    const admitWrites = () => {
      while (readersOpen === 0 && queuedWrites.length > 0) queuedWrites.shift()!()
    }
    let depositRequested = false
    const requestDepositOnce = (sql: string) => {
      if (depositRequested || !sql.includes('FROM "ledger"')) return
      depositRequested = true
      // one transaction, so sum(ledger.amount) always equals account.balance
      queuedWrites.push(() => {
        db.exec('BEGIN')
        db.exec("UPDATE account SET balance = balance + 100 WHERE id = 'a1'")
        db.exec("INSERT INTO ledger VALUES ('l1', 'a1', 100)")
        db.exec('COMMIT')
      })
      admitWrites()
    }
    const run = (sql: string, params: readonly unknown[]) => {
      requestDepositOnce(sql)
      const statement = db.prepare(sql)
      const rows = statement.reader
        ? statement.all(...params)
        : (statement.run(...params), [])
      admitWrites()
      return rows
    }

    const stored = writableBucket()
    const manager = createNamespaceBackupManager({
      format: 'test-v3',
      markerTable: '_test_backup_meta',
      excludedTables: ['_test_backup_meta'],
      files: () => stored.bucket,
      query: async (_env, _namespace, sql, params) => run(sql, params),
      readSession: async (_env, _namespace, work) => {
        readersOpen++
        try {
          return await work(async (sql, params = []) => run(sql, params))
        } finally {
          readersOpen--
          admitWrites()
        }
      },
      batch: async () => {},
      listNamespaces: async () => ['singleton'],
    })

    await manager.exportNamespace({}, 'singleton')

    const pointer = JSON.parse(
      stored.pointers.get('backups/singleton/latest.json') ?? '{}'
    )
    const dumped = (stored.pointers.get(String(pointer.key)) ?? '')
      .split('\n')
      .filter(Boolean)
      .map((line) => JSON.parse(line))
      .filter((line) => line.kind === 'rows')
    const rowsOf = (table: string) =>
      dumped.filter((line) => line.table === table).flatMap((line) => line.rows)
    const balance = Number(rowsOf('account').find((row: any) => row.id === 'a1')?.balance)
    const ledgerTotal = rowsOf('ledger').reduce(
      (total: number, row: any) => total + Number(row.amount),
      0
    )

    expect(depositRequested).toBe(true)
    expect(ledgerTotal).toBe(balance)
    // the deposit is delayed by the scan, never dropped
    expect(db.prepare("SELECT balance FROM account WHERE id = 'a1'").get()).toEqual({
      balance: 100,
    })
  })
})

describe('namespace backup restore', () => {
  it('validates the complete dump before mutating the namespace', async () => {
    const key = 'backups/singleton/truncated.ndjson'
    const stored = bucketWith(
      key,
      dump([
        {
          kind: 'header',
          format: 'test-v3',
          ns: 'singleton',
          orderedTables: true,
        },
        {
          kind: 'table',
          name: 'message',
          sql: 'CREATE TABLE message (id TEXT PRIMARY KEY)',
          indexes: [],
        },
      ])
    )
    const queries: string[] = []
    const batches: NamespaceBackupStatement[][] = []
    const manager = backupManager({
      format: 'test-v3',
      markerTable: '_test_backup_meta',
      files: () => stored.bucket,
      query: async (_env, _namespace, sql) => {
        queries.push(sql)
        return []
      },
      batch: async (_env, _namespace, statements) => {
        batches.push([...statements])
      },
      listNamespaces: async () => ['singleton'],
    })

    await expect(manager.importNamespace({}, 'singleton', key)).rejects.toThrow(
      'backup is truncated'
    )
    expect(stored.reads()).toBe(1)
    expect(queries).toEqual([])
    expect(batches).toEqual([])
  })

  it('streams ordered rows in bounded batches and caches insert SQL by shape', async () => {
    const key = 'backups/singleton/1.ndjson'
    const rows = Array.from({ length: 401 }, (_, index) => ({
      id: `message-${index}`,
      body: `body-${index}`,
    }))
    const stored = bucketWith(
      key,
      dump([
        {
          kind: 'header',
          format: 'test-v3',
          ns: 'source',
          orderedTables: true,
        },
        {
          kind: 'table',
          name: 'message',
          sql: 'CREATE TABLE message (id TEXT PRIMARY KEY, body TEXT)',
          indexes: ['CREATE INDEX message_body ON message (body)'],
        },
        { kind: 'rows', table: 'message', rows },
        { kind: 'footer', tables: 1, rows: rows.length },
      ])
    )
    const batches: NamespaceBackupStatement[][] = []
    const manager = backupManager({
      format: 'test-v3',
      markerTable: '_test_backup_meta',
      files: () => stored.bucket,
      query: async (_env, _namespace, sql) => {
        if (sql.startsWith('SELECT COUNT(*)')) return [{ n: rows.length }]
        return []
      },
      batch: async (_env, _namespace, statements) => {
        batches.push([...statements])
      },
      listNamespaces: async () => ['singleton'],
    })

    const summary = await manager.importNamespace({}, 'singleton', key)

    expect(stored.reads()).toBe(2)
    expect(summary).toMatchObject({
      sourceNs: 'source',
      tables: 1,
      rows: 401,
      counts: { message: 401 },
    })
    const inserts = batches
      .flat()
      .filter((statement) => statement.sql.startsWith('INSERT INTO "message"'))
    expect(inserts).toHaveLength(401)
    expect(new Set(inserts.map((statement) => statement.sql))).toHaveLength(1)
    expect(batches.some((batch) => batch.length === 400)).toBe(true)
    expect(
      batches
        .flat()
        .find(
          (statement) => statement.sql === 'CREATE INDEX message_body ON message (body)'
        )
    ).toBeDefined()
  })

  it('drops current-only dependent tables before restoring an older dump', async () => {
    const key = 'backups/singleton/older.ndjson'
    const stored = bucketWith(
      key,
      dump([
        {
          kind: 'header',
          format: 'test-v3',
          ns: 'source',
          orderedTables: true,
        },
        {
          kind: 'table',
          name: 'message',
          sql: 'CREATE TABLE message (id TEXT PRIMARY KEY)',
          indexes: [],
        },
        { kind: 'rows', table: 'message', rows: [{ id: 'one' }] },
        { kind: 'footer', tables: 1, rows: 1 },
      ])
    )
    const batches: NamespaceBackupStatement[][] = []
    const manager = backupManager({
      format: 'test-v3',
      markerTable: '_test_backup_meta',
      files: () => stored.bucket,
      query: async (_env, _namespace, sql) => {
        if (sql.startsWith('SELECT name, sql FROM sqlite_master')) {
          return [
            { name: 'message', sql: 'CREATE TABLE message (id TEXT PRIMARY KEY)' },
            {
              name: 'messageReaction',
              sql: `CREATE TABLE messageReaction (
                id TEXT DEFAULT 'REFERENCES ignored',
                messageId TEXT REFERENCES "Message"(id)
                /* REFERENCES ignored_too */
              )`,
            },
          ]
        }
        if (sql.startsWith('SELECT COUNT(*)')) return [{ n: 1 }]
        return []
      },
      batch: async (_env, _namespace, statements) => {
        batches.push([...statements])
      },
      listNamespaces: async () => ['singleton'],
    })

    await manager.importNamespace({}, 'singleton', key)

    const drops = batches
      .flat()
      .map((statement) => statement.sql)
      .filter((sql) => sql.startsWith('DROP TABLE'))
    expect(drops).toEqual([
      'DROP TABLE IF EXISTS "messageReaction"',
      'DROP TABLE IF EXISTS "message"',
    ])
  })

  it('drops reverse-FK current tables in bounded batches against real SQLite', async () => {
    const key = 'backups/singleton/real-older.ndjson'
    const stored = bucketWith(
      key,
      dump([
        {
          kind: 'header',
          format: 'test-v3',
          ns: 'source',
          orderedTables: true,
        },
        {
          kind: 'table',
          name: 'message',
          sql: 'CREATE TABLE message (id TEXT PRIMARY KEY)',
          indexes: [],
        },
        { kind: 'rows', table: 'message', rows: [{ id: 'restored' }] },
        { kind: 'footer', tables: 1, rows: 1 },
      ])
    )
    const db = new BetterSqlite3(':memory:')
    db.exec('PRAGMA foreign_keys = ON')
    db.exec('CREATE TABLE message (id TEXT PRIMARY KEY)')
    db.exec(
      'CREATE TABLE messageReaction (id TEXT PRIMARY KEY, messageId TEXT NOT NULL REFERENCES message(id))'
    )
    db.exec("INSERT INTO message VALUES ('current')")
    db.exec("INSERT INTO messageReaction VALUES ('reaction', 'current')")
    for (let index = 0; index < 41; index++) {
      db.exec(`CREATE TABLE "current_only_${String(index).padStart(3, '0')}" (id TEXT)`)
    }
    const batchSizes: number[] = []
    const metrics = { queries: [] as string[], rowsRead: 0 }
    const totalChanges = () =>
      Number(db.prepare('SELECT total_changes() AS value').get().value)
    const changesBeforeRestore = totalChanges()

    const summary = await realSqliteManager(
      db,
      stored.bucket,
      batchSizes,
      metrics
    ).importNamespace({}, 'singleton', key)
    const restoreChanges = totalChanges() - changesBeforeRestore

    expect(summary).toMatchObject({ rows: 1, counts: { message: 1 } })
    expect(db.prepare('SELECT * FROM message').all()).toEqual([{ id: 'restored' }])
    expect(
      db
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
        .all()
    ).toEqual([{ name: 'message' }])
    expect(metrics.queries).toHaveLength(8)
    expect(
      metrics.queries.filter((sql) => sql.startsWith('PRAGMA foreign_key_list'))
    ).toHaveLength(0)
    expect(metrics.rowsRead).toBe(44)
    expect(restoreChanges).toBe(1)
    expect(batchSizes).toHaveLength(4)
    expect(Math.max(...batchSizes)).toBeLessThanOrEqual(40)
    db.close()
  })

  it('accepts explicitly retained legacy formats', async () => {
    const key = 'backups/singleton/legacy.ndjson'
    const stored = bucketWith(
      key,
      dump([
        { kind: 'header', format: 'test-v2', ns: 'source' },
        {
          kind: 'table',
          name: 'message',
          sql: 'CREATE TABLE message (id TEXT PRIMARY KEY)',
          indexes: [],
        },
        { kind: 'rows', table: 'message', rows: [{ id: 'one' }] },
        { kind: 'footer', tables: 1, rows: 1 },
      ])
    )
    const manager = backupManager({
      format: 'test-v3',
      acceptedFormats: ['test-v2'],
      markerTable: '_test_backup_meta',
      files: () => stored.bucket,
      query: async (_env, _namespace, sql) => {
        if (sql.startsWith('SELECT COUNT(*)')) return [{ n: 1 }]
        return []
      },
      batch: async () => {},
      listNamespaces: async () => ['singleton'],
    })

    await expect(manager.importNamespace({}, 'singleton', key)).resolves.toMatchObject({
      sourceNs: 'source',
      rows: 1,
    })
  })
})
