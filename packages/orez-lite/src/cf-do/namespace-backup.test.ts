import { sha256 } from '@noble/hashes/sha2.js'
// @ts-expect-error - CJS module
import BedrockSqlite from 'bedrock-sqlite'
import { describe, expect, it, vi } from 'vitest'

import { ApplicationSqlSessionPreemptedError } from './application-sql.js'
import {
  createNamespaceBackupManager,
  type NamespaceBackupBucket,
  type NamespaceBackupStatement,
} from './namespace-backup.js'

async function exportedSummary(
  result: ReturnType<ReturnType<typeof createNamespaceBackupManager>['exportNamespace']>
) {
  const value = await result
  expect(value.outcome).toBe('exported')
  if (value.outcome !== 'exported') throw new Error('expected an exported backup')
  return value.summary
}

function stream(text: string): ReadableStream<Uint8Array> {
  const bytes = new TextEncoder().encode(text)
  return new ReadableStream({
    start(controller) {
      controller.enqueue(bytes)
      controller.close()
    },
  })
}

function bucketWith(key: string, dump: string, etags: readonly string[] = []) {
  let reads = 0
  const bucket: NamespaceBackupBucket = {
    async get(requestedKey) {
      if (requestedKey !== key) return null
      reads++
      return {
        body: stream(dump),
        etag: etags[reads - 1],
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
  const copied = lines.map((line) => ({ ...(line as Record<string, unknown>) }))
  const footerIndex = copied.findIndex((line) => line.kind === 'footer')
  const format = copied.find((line) => line.kind === 'header')?.format
  if (footerIndex !== -1 && format !== 'test-v2') {
    const payload = `${copied
      .slice(0, footerIndex)
      .map((line) => JSON.stringify(line))
      .join('\n')}\n`
    copied[footerIndex] = {
      ...copied[footerIndex],
      sha256: Array.from(sha256(new TextEncoder().encode(payload)), (byte) =>
        byte.toString(16).padStart(2, '0')
      ).join(''),
    }
  }
  return `${copied.map((line) => JSON.stringify(line)).join('\n')}\n`
}

function unsignedDump(lines: unknown[]): string {
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

    const summary = await exportedSummary(manager.exportNamespace({}, 'singleton'))

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

  it('does not publish a backup after its background read session is preempted', async () => {
    const stored = writableBucket()
    const query = async (_env: unknown, _namespace: string, sql: string) => {
      if (sql.includes('SELECT write_seq')) return [{ write_seq: 9 }]
      if (sql.includes('sqlite_master')) {
        return [{ name: 'message', sql: 'CREATE TABLE message (id TEXT)', type: 'table' }]
      }
      if (sql.includes('FROM "message"')) return []
      throw new Error(`unexpected export query: ${sql}`)
    }
    const manager = backupManager({
      format: 'test-v3',
      markerTable: '_test_backup_meta',
      excludedTables: ['_test_backup_meta'],
      files: () => stored.bucket,
      query,
      readSession: async (env: unknown, namespace: string, work: any) => {
        await work((sql: string, params: readonly unknown[] = []) =>
          query(env, namespace, sql, params)
        )
        throw new ApplicationSqlSessionPreemptedError()
      },
      batch: async () => {},
      listNamespaces: async () => ['singleton'],
    })

    await expect(manager.exportNamespace({}, 'singleton')).resolves.toEqual({
      outcome: 'preempted',
      namespace: 'singleton',
    })
    expect(stored.pointers.has('backups/singleton/latest.json')).toBe(false)
  })

  it('still rejects a genuine export failure', async () => {
    const stored = writableBucket()
    const manager = backupManager({
      format: 'test-v3',
      markerTable: '_test_backup_meta',
      files: () => stored.bucket,
      query: async () => [],
      readSession: async () => {
        throw new Error('R2 unavailable')
      },
      batch: async () => {},
      listNamespaces: async () => ['singleton'],
    })

    await expect(manager.exportNamespace({}, 'singleton')).rejects.toThrow(
      'R2 unavailable'
    )
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

    const summary = await exportedSummary(manager.exportNamespace({}, 'singleton'))

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

    const summary = await exportedSummary(
      realSqliteManager(db, stored.bucket).exportNamespace({}, 'singleton')
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
        'CREATE TABLE ledger (id TEXT PRIMARY KEY, account_id TEXT NOT NULL REFERENCES account(id), amount INTEGER NOT NULL);' +
        'CREATE TABLE _test_backup_meta (id INTEGER PRIMARY KEY, write_seq INTEGER)'
    )
    db.exec('INSERT INTO _test_backup_meta VALUES (1, 1)')
    db.exec("INSERT INTO account VALUES ('a1', 0)")

    // The same scan at its default chunk size, where the whole namespace fits
    // in one chunk. A deposit that arrives during it is admitted the moment
    // that chunk's session closes, so the dump has to hold either both halves
    // of the transaction or neither; a scan that opens a session per statement
    // holds one of them.
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
        db.exec('UPDATE _test_backup_meta SET write_seq = write_seq + 1 WHERE id = 1')
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

/**
 * The durable object's application-SQL admission rules, as worker.ts implements
 * them, over a real SQLite database.
 *
 * A read session keeps its turn until it closes, and an arriving writer does
 * not queue behind it: `[APPLICATION_SQL_ACQUIRE]` drops every active
 * background reader out of the reader set before admitting the write, so that
 * reader's next statement or its commit reports the session preempted. A
 * session that actually mutated bumps `write_seq` on commit, the way
 * `applicationSqlDidCommit` does.
 */
function durableObject(
  db: InstanceType<typeof BetterSqlite3>,
  onRead: (sql: string) => void = () => {}
) {
  const readers = new Set<{ preempted: boolean }>()
  const queuedWrites: Array<() => void> = []
  let sessions = 0
  let openReaders = 0
  let uploadsWithSessionOpen = 0

  const run = (sql: string, params: readonly unknown[] = []) => {
    const statement = db.prepare(sql)
    return statement.reader
      ? (statement.all(...params) as Record<string, any>[])
      : (statement.run(...params), [] as Record<string, any>[])
  }

  const commitWrite = (mutate: () => void, mutates: boolean) => {
    for (const reader of readers) reader.preempted = true
    readers.clear()
    db.exec('BEGIN')
    try {
      mutate()
      db.exec('COMMIT')
    } catch (error) {
      db.exec('ROLLBACK')
      throw error
    }
    if (mutates) {
      db.exec('UPDATE _test_backup_meta SET write_seq = write_seq + 1 WHERE id = 1')
    }
  }

  const admitQueued = () => {
    while (openReaders === 0 && queuedWrites.length > 0) queuedWrites.shift()!()
  }

  return {
    sessions: () => sessions,
    uploadsWithSessionOpen: () => uploadsWithSessionOpen,
    /** admitted immediately, preempting whatever background reader is open */
    writeNow(mutate: () => void, mutates = true) {
      commitWrite(mutate, mutates)
    },
    /** admitted the moment the open session closes, so it lands between chunks */
    writeBetweenChunks(mutate: () => void) {
      queuedWrites.push(() => commitWrite(mutate, true))
    },
    files(bucket: NamespaceBackupBucket): NamespaceBackupBucket {
      return {
        ...bucket,
        createMultipartUpload: async (key: string) => {
          const upload = await bucket.createMultipartUpload(key)
          return {
            ...upload,
            uploadPart: (partNumber: number, value: Uint8Array) => {
              if (openReaders > 0) uploadsWithSessionOpen++
              return upload.uploadPart(partNumber, value)
            },
          }
        },
      }
    },
    async readSession<Value>(
      _env: unknown,
      _namespace: string,
      work: (
        query: (
          sql: string,
          params?: readonly unknown[]
        ) => Promise<Record<string, any>[]>
      ) => Promise<Value>
    ): Promise<Value> {
      const session = { preempted: false }
      sessions++
      readers.add(session)
      openReaders++
      try {
        const value = await work(async (sql, params = []) => {
          if (session.preempted) throw new ApplicationSqlSessionPreemptedError()
          onRead(sql)
          if (session.preempted) throw new ApplicationSqlSessionPreemptedError()
          return run(sql, params)
        })
        // a session preempted after its last read still fails at commit
        if (session.preempted) throw new ApplicationSqlSessionPreemptedError()
        return value
      } finally {
        readers.delete(session)
        openReaders--
        admitQueued()
      }
    },
    query: async (
      _env: unknown,
      _namespace: string,
      sql: string,
      params: readonly unknown[]
    ) => run(sql, params),
  }
}

/** account/ledger joined by a foreign key, so a torn dump is visibly torn. */
function ledgerDatabase(ledgerRows: number) {
  const db = new BetterSqlite3(':memory:')
  db.exec(
    'CREATE TABLE account (id TEXT PRIMARY KEY, balance INTEGER NOT NULL);' +
      'CREATE TABLE ledger (id TEXT PRIMARY KEY, account_id TEXT NOT NULL REFERENCES account(id), amount INTEGER NOT NULL);' +
      'CREATE TABLE _test_backup_meta (id INTEGER PRIMARY KEY, write_seq INTEGER)'
  )
  db.exec('INSERT INTO _test_backup_meta VALUES (1, 1)')
  db.exec("INSERT INTO account VALUES ('a1', 0)")
  const insert = db.prepare('INSERT INTO ledger VALUES (?, ?, ?)')
  db.exec('BEGIN')
  for (let index = 0; index < ledgerRows; index++) {
    insert.run(`seed-${String(index).padStart(4, '0')}`, 'a1', 0)
  }
  db.exec('COMMIT')
  return db
}

/** one transaction, so sum(ledger.amount) always equals account.balance */
const deposit =
  (db: InstanceType<typeof BetterSqlite3>, id: string, amount: number) => () => {
    db.exec(`UPDATE account SET balance = balance + ${amount} WHERE id = 'a1'`)
    db.exec(`INSERT INTO ledger VALUES ('${id}', 'a1', ${amount})`)
  }

function dumped(stored: ReturnType<typeof writableBucket>) {
  const pointer = JSON.parse(stored.pointers.get('backups/singleton/latest.json') ?? '{}')
  const lines = (stored.pointers.get(String(pointer.key)) ?? '')
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line))
  const rowsOf = (table: string) =>
    lines
      .filter((line) => line.kind === 'rows' && line.table === table)
      .flatMap((line) => line.rows as Record<string, any>[])
  return {
    pointer,
    published: lines.length > 0,
    rowsOf,
    balance: () => Number(rowsOf('account').find((row) => row.id === 'a1')?.balance),
    ledgerTotal: () =>
      rowsOf('ledger').reduce((total, row) => total + Number(row.amount), 0),
  }
}

/** every `orez_backup` event the manager emitted while `work` ran. */
async function backupEvents<Value>(work: () => Promise<Value>) {
  const events: Record<string, any>[] = []
  const log = vi.spyOn(console, 'log').mockImplementation((line: unknown) => {
    try {
      const parsed = JSON.parse(String(line))
      if (parsed?.event === 'orez_backup') events.push(parsed)
    } catch {
      // not one of ours
    }
  })
  let value: Value
  try {
    value = await work()
  } finally {
    log.mockRestore()
  }
  return { value, events }
}

describe('namespace backup export under a live writer', () => {
  const scenario = (
    db: InstanceType<typeof BetterSqlite3>,
    onRead: (object: ReturnType<typeof durableObject>, sql: string) => void,
    managerOptions: Record<string, unknown> = {}
  ) => {
    const stored = writableBucket()
    let multipartUploads = 0
    let object!: ReturnType<typeof durableObject>
    object = durableObject(db, (sql) => onRead(object, sql))
    const files = object.files({
      ...stored.bucket,
      createMultipartUpload: (key: string) => {
        multipartUploads++
        return stored.bucket.createMultipartUpload(key)
      },
    })
    const manager = createNamespaceBackupManager<unknown>({
      format: 'test-v3',
      markerTable: '_test_backup_meta',
      excludedTables: ['_test_backup_meta'],
      // one chunk per page, so a passing scan cannot be one session by accident
      scanChunkBytes: 1,
      files: () => files,
      query: object.query,
      readSession: (env, namespace, work) => object.readSession(env, namespace, work),
      batch: async () => {},
      listNamespaces: async () => ['singleton'],
      ...managerOptions,
    })
    return { manager, object, stored, multipartUploads: () => multipartUploads }
  }

  /**
   * The failure this replaces: the export owned one read session for its whole
   * length, so any writer admitted during it killed the export outright. On the
   * production control plane that was every attempt for six hours. A run in
   * which no writer ever arrives proves nothing, so this one admits a real
   * interactive transaction in the middle of the scan.
   */
  it('completes while an interactive writer is admitted mid-scan', async () => {
    const db = ledgerDatabase(600)
    let deposits = 0
    const { manager, object, stored } = scenario(db, (self, sql) => {
      if (deposits > 0 || !sql.includes('FROM "ledger"')) return
      deposits++
      self.writeNow(deposit(db, 'l1', 100))
    })

    const { value, events } = await backupEvents(() =>
      manager.exportNamespace({}, 'singleton')
    )

    expect(deposits).toBe(1)
    expect(value.outcome).toBe('exported')
    const dump = dumped(stored)
    expect(dump.published).toBe(true)
    // the dump is one state the database actually had
    expect(dump.ledgerTotal()).toBe(dump.balance())
    expect(dump.rowsOf('ledger')).toHaveLength(601)
    // and the scan never held the database across an upload
    expect(object.sessions()).toBeGreaterThan(1)
    expect(object.uploadsWithSessionOpen()).toBe(0)
    expect(events.filter((event) => event.outcome === 'torn')).not.toHaveLength(0)
    // the writer was never made to wait: its deposit is live before the export
    // returns, not replayed after it
    expect(db.prepare("SELECT balance FROM account WHERE id = 'a1'").get()).toEqual({
      balance: 100,
    })
    db.close()
  })

  /**
   * The shape this replaces, reproduced: one session over the whole scan, no
   * chunk to re-read and no scan to retry. One writer ends the export, which is
   * what the production control plane did roughly twenty times in a row while
   * every quiet project namespace exported normally.
   */
  it('loses the whole export to one writer when the scan is a single session', async () => {
    const db = ledgerDatabase(600)
    let deposits = 0
    const { manager, object, stored } = scenario(
      db,
      (self, sql) => {
        if (deposits > 0 || !sql.includes('FROM "ledger"')) return
        deposits++
        self.writeNow(deposit(db, 'l1', 100))
      },
      { scanChunkBytes: Number.MAX_SAFE_INTEGER, chunkAttempts: 1, scanAttempts: 1 }
    )

    const value = await manager.exportNamespace({}, 'singleton')

    expect(deposits).toBe(1)
    expect(value).toEqual({ outcome: 'preempted', namespace: 'singleton' })
    expect(stored.pointers.get('backups/singleton/latest.json')).toBeUndefined()
    // the schema read plus the one session that was supposed to carry the scan
    expect(object.sessions()).toBe(2)
    db.close()
  })

  it('restarts rather than publishing a dump torn by a write between chunks', async () => {
    const db = ledgerDatabase(600)
    let deposits = 0
    const { manager, object, stored, multipartUploads } = scenario(db, (self, sql) => {
      if (deposits > 0 || !sql.includes('FROM "ledger"')) return
      deposits++
      // queued behind the open session, so it lands after the chunk closes and
      // preempts nothing. only the marker can catch it.
      self.writeBetweenChunks(deposit(db, 'l1', 100))
    })

    const { value, events } = await backupEvents(() =>
      manager.exportNamespace({}, 'singleton')
    )

    expect(value.outcome).toBe('exported')
    const torn = events.filter((event) => event.outcome === 'torn')
    expect(torn).toHaveLength(1)
    expect(torn[0]).toMatchObject({ marker: 1, observedMarker: 2 })
    expect(multipartUploads()).toBe(2)
    const dump = dumped(stored)
    expect(dump.ledgerTotal()).toBe(dump.balance())
    expect(dump.balance()).toBe(100)
    expect(dump.pointer.marker).toBe(2)
    expect(object.uploadsWithSessionOpen()).toBe(0)
    db.close()
  })

  /**
   * The control that shows the marker fence is doing the work: the same write,
   * with no scan left to retry with, abandons the export instead of publishing
   * the half-old dump the pages already collected would have made.
   */
  it('publishes nothing when the only scan it is allowed is torn', async () => {
    const db = ledgerDatabase(600)
    let deposits = 0
    const { manager, stored } = scenario(
      db,
      (self, sql) => {
        if (deposits > 0 || !sql.includes('FROM "ledger"')) return
        deposits++
        self.writeBetweenChunks(deposit(db, 'l1', 100))
      },
      { scanAttempts: 1 }
    )

    const value = await manager.exportNamespace({}, 'singleton')

    expect(value).toEqual({ outcome: 'preempted', namespace: 'singleton' })
    expect(stored.pointers.get('backups/singleton/latest.json')).toBeUndefined()
    db.close()
  })

  it('re-reads only the interrupted chunk when the writer commits no change', async () => {
    const db = ledgerDatabase(600)
    let interruptions = 0
    const { manager, object, stored, multipartUploads } = scenario(db, (self, sql) => {
      if (interruptions > 0 || !sql.includes('FROM "ledger"')) return
      interruptions++
      // admitted, took the turn, changed nothing: `applicationSqlDidCommit`
      // does not bump the marker for a session that never mutated
      self.writeNow(() => {}, false)
    })

    const { value, events } = await backupEvents(() =>
      manager.exportNamespace({}, 'singleton')
    )

    expect(interruptions).toBe(1)
    expect(value.outcome).toBe('exported')
    expect(events.filter((event) => event.outcome === 'torn')).toHaveLength(0)
    // one scan, one multipart upload: the interruption cost a page, not a dump
    expect(multipartUploads()).toBe(1)
    expect(dumped(stored).rowsOf('ledger')).toHaveLength(600)
    expect(object.uploadsWithSessionOpen()).toBe(0)
    db.close()
  })

  /**
   * The reason a writer used to win every race: the scan awaited each multipart
   * upload with the database still held, so the window an arriving writer could
   * land in was the whole export rather than its reads. Uploads are started
   * between chunks and only awaited at the end, so pages keep being read while
   * R2 is still working.
   */
  it('keeps reading while its uploads are still outstanding', async () => {
    const db = ledgerDatabase(600)
    let outstanding = 0
    let readsWhileUploading = 0
    let uploads = 0
    const stored = writableBucket()
    const object = durableObject(db, () => {
      if (outstanding > 0) readsWhileUploading++
    })
    const manager = createNamespaceBackupManager<unknown>({
      format: 'test-v3',
      markerTable: '_test_backup_meta',
      excludedTables: ['_test_backup_meta'],
      scanChunkBytes: 1,
      // small parts so the scan produces many uploads before it finishes
      partBytes: 2048,
      maxInflightParts: 64,
      files: () => ({
        ...stored.bucket,
        createMultipartUpload: async (key: string) => {
          const upload = await stored.bucket.createMultipartUpload(key)
          return {
            ...upload,
            uploadPart: async (partNumber: number, value: Uint8Array) => {
              uploads++
              outstanding++
              await new Promise((resolve) => setTimeout(resolve, 20))
              outstanding--
              return upload.uploadPart(partNumber, value)
            },
          }
        },
      }),
      query: object.query,
      readSession: (env, namespace, work) => object.readSession(env, namespace, work),
      batch: async () => {},
      listNamespaces: async () => ['singleton'],
    })

    const value = await manager.exportNamespace({}, 'singleton')

    expect(value.outcome).toBe('exported')
    expect(uploads).toBeGreaterThan(1)
    // a scan that awaited each upload with the database held reads nothing here
    expect(readsWhileUploading).toBeGreaterThan(0)
    expect(dumped(stored).rowsOf('ledger')).toHaveLength(600)
    db.close()
  })

  it('gives up after a bounded number of scans instead of spinning', async () => {
    const db = ledgerDatabase(600)
    let deposits = 0
    const { manager, stored, multipartUploads } = scenario(db, (self, sql) => {
      if (!sql.includes('FROM "ledger"')) return
      deposits++
      self.writeBetweenChunks(deposit(db, `l${deposits}`, 1))
    })

    const { value, events } = await backupEvents(() =>
      manager.exportNamespace({}, 'singleton')
    )

    expect(value).toEqual({ outcome: 'preempted', namespace: 'singleton' })
    expect(multipartUploads()).toBe(3)
    expect(events.filter((event) => event.outcome === 'torn')).toHaveLength(3)
    expect(events.at(-1)).toMatchObject({
      phase: 'export',
      outcome: 'preempted',
      torn: 3,
    })
    expect(stored.pointers.get('backups/singleton/latest.json')).toBeUndefined()
    db.close()
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

  it('rejects a digest mismatch before reading or writing the target namespace', async () => {
    const key = 'backups/singleton/tampered.ndjson'
    const valid = dump([
      { kind: 'header', format: 'test-v3', ns: 'source', orderedTables: true },
      {
        kind: 'table',
        name: 'message',
        sql: 'CREATE TABLE message (id TEXT PRIMARY KEY)',
        indexes: [],
      },
      { kind: 'rows', table: 'message', rows: [{ id: 'one' }] },
      { kind: 'footer', tables: 1, rows: 1 },
    ])
    const stored = bucketWith(key, valid.replace('"one"', '"tampered"'))
    const query = vi.fn(async () => [])
    const batch = vi.fn(async () => {})
    const manager = backupManager({
      format: 'test-v3',
      markerTable: '_test_backup_meta',
      files: () => stored.bucket,
      query,
      batch,
      listNamespaces: async () => ['singleton'],
    })

    await expect(manager.importNamespace({}, 'singleton', key)).rejects.toThrow(
      'sha256 digest mismatch'
    )
    expect(query).not.toHaveBeenCalled()
    expect(batch).not.toHaveBeenCalled()
  })

  it('requires an explicit override before replacing any live application table', async () => {
    const key = 'backups/singleton/fresh-only.ndjson'
    const stored = bucketWith(
      key,
      dump([
        { kind: 'header', format: 'test-v3', ns: 'source', orderedTables: true },
        {
          kind: 'table',
          name: 'message',
          sql: 'CREATE TABLE message (id TEXT PRIMARY KEY)',
          indexes: [],
        },
        { kind: 'footer', tables: 1, rows: 0 },
      ])
    )
    const beforeImport = vi.fn(async () => {})
    const batch = vi.fn(async () => {})
    const manager = backupManager({
      format: 'test-v3',
      markerTable: '_test_backup_meta',
      files: () => stored.bucket,
      query: async (_env, _namespace, sql) =>
        sql.startsWith('SELECT name, sql FROM sqlite_master')
          ? [{ name: 'message', sql: 'CREATE TABLE message (id TEXT PRIMARY KEY)' }]
          : [],
      beforeImport,
      batch,
      listNamespaces: async () => ['singleton'],
    })

    await expect(manager.importNamespace({}, 'singleton', key)).rejects.toThrow(
      'restore target is not empty'
    )
    expect(beforeImport).not.toHaveBeenCalled()
    expect(batch).not.toHaveBeenCalled()
  })

  it('rejects an object replaced after validation before starting the restore', async () => {
    const key = 'backups/singleton/replaced.ndjson'
    const stored = bucketWith(
      key,
      dump([
        { kind: 'header', format: 'test-v3', ns: 'source', orderedTables: true },
        {
          kind: 'table',
          name: 'message',
          sql: 'CREATE TABLE message (id TEXT PRIMARY KEY)',
          indexes: [],
        },
        { kind: 'footer', tables: 1, rows: 0 },
      ]),
      ['validated-etag', 'replacement-etag']
    )
    const beforeImport = vi.fn(async () => {})
    const batch = vi.fn(async () => {})
    const manager = backupManager({
      format: 'test-v3',
      markerTable: '_test_backup_meta',
      files: () => stored.bucket,
      query: async () => [],
      beforeImport,
      batch,
      listNamespaces: async () => ['singleton'],
    })

    await expect(manager.importNamespace({}, 'singleton', key)).rejects.toThrow(
      'backup object changed between validation and restore'
    )
    expect(beforeImport).not.toHaveBeenCalled()
    expect(batch).not.toHaveBeenCalled()
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

    await manager.importNamespace({}, 'singleton', key, { allowNonEmpty: true })

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
    ).importNamespace({}, 'singleton', key, { allowNonEmpty: true })
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

  it('accepts pre-integrity backups that use the current custom format', async () => {
    const key = 'backups/singleton/custom-current-legacy.ndjson'
    const stored = bucketWith(
      key,
      unsignedDump([
        { kind: 'header', format: 'test-v3', ns: 'source' },
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

  it('requires a digest when the header advertises sha256 integrity', async () => {
    const key = 'backups/singleton/missing-advertised-digest.ndjson'
    const stored = bucketWith(
      key,
      unsignedDump([
        {
          kind: 'header',
          format: 'test-v3',
          integrity: 'sha256',
          ns: 'source',
        },
        { kind: 'footer', tables: 0, rows: 0 },
      ])
    )
    const query = vi.fn(async () => [])
    const manager = backupManager({
      format: 'test-v3',
      markerTable: '_test_backup_meta',
      files: () => stored.bucket,
      query,
      batch: async () => {},
      listNamespaces: async () => ['singleton'],
    })

    await expect(manager.importNamespace({}, 'singleton', key)).rejects.toThrow(
      'backup footer is missing its sha256 digest'
    )
    expect(query).not.toHaveBeenCalled()
  })
})
