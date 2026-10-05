import { sha256 } from '@noble/hashes/sha2.js'
// @ts-expect-error - CJS module
import BedrockSqlite from 'bedrock-sqlite'
import { describe, expect, it, vi } from 'vitest'

import {
  createNamespaceBackupManager,
  isNamespaceBackupTableExcluded,
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

function sqliteSnapshotCallbacks(
  db: InstanceType<typeof BetterSqlite3>,
  onRead: (sql: string) => void = () => {}
) {
  return {
    snapshot: async (
      _env: unknown,
      _namespace: string,
      options: { markerTable: string; excludedTables: readonly string[] }
    ) => {
      db.exec('BEGIN')
      try {
        const schema = db
          .prepare(
            "SELECT name, sql, type, tbl_name FROM sqlite_master WHERE type IN ('table', 'index') AND sql IS NOT NULL ORDER BY name"
          )
          .all()
          .filter(
            (row: any) =>
              !isNamespaceBackupTableExcluded(
                row.name,
                new Set(options.excludedTables)
              ) &&
              !isNamespaceBackupTableExcluded(
                row.tbl_name,
                new Set(options.excludedTables)
              )
          )
        const tables = schema
          .filter((row: any) => row.type === 'table')
          .map((row: any) => row.name)
        const id = crypto.randomUUID()
        const columns: Record<string, string[]> = {}
        for (const name of tables) {
          columns[name] = db
            .prepare(`SELECT * FROM "${name.replaceAll('"', '""')}" LIMIT 0`)
            .columns()
            .map((column: any) => column.name)
          const projection = columns[name]!.map(
            (column, index) => `"${column.replaceAll('"', '""')}" AS c${index}`
          ).join(', ')
          db.exec(
            `CREATE TABLE "_orez_bk_${id}_${name.replaceAll('"', '""')}" AS SELECT ${projection} FROM "${name.replaceAll('"', '""')}"`
          )
        }
        const marker =
          Number(
            db
              .prepare(`SELECT write_seq FROM "${options.markerTable}" WHERE id = 1`)
              .get()?.write_seq
          ) || 0
        db.exec('COMMIT')
        return {
          id,
          marker,
          tables,
          schema,
          columns,
          lease: {
            async readPage(table: string, afterRowid: number, limit: number) {
              const sql = `SELECT rowid AS __orez_backup_rowid, * FROM "_orez_bk_${id}_${table.replaceAll('"', '""')}" WHERE rowid > ? ORDER BY rowid LIMIT ?`
              onRead(sql)
              return db.prepare(sql).all(afterRowid, limit)
            },
            [Symbol.dispose]() {},
          },
        }
      } catch (error) {
        db.exec('ROLLBACK')
        throw error
      }
    },
    dropSnapshot: async () => {
      for (const { name } of db
        .prepare(
          "SELECT name FROM sqlite_master WHERE type = 'table' AND name GLOB '_orez_bk_*'"
        )
        .all())
        db.exec(`DROP TABLE "${name.replaceAll('"', '""')}"`)
    },
  }
}

function realSqliteManager(
  db: InstanceType<typeof BetterSqlite3>,
  bucket: NamespaceBackupBucket,
  batchSizes: number[] = [],
  metrics?: { queries: string[]; rowsRead: number }
) {
  return createNamespaceBackupManager({
    ...sqliteSnapshotCallbacks(db),
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
  it('records empty and populated table counts in the durable summary', async () => {
    const db = ledgerDatabase(1)
    db.exec('CREATE TABLE empty (id TEXT)')
    const stored = writableBucket()
    const summary = await exportedSummary(
      realSqliteManager(db, stored.bucket).exportNamespace({}, 'singleton')
    )
    expect(summary.tableRows).toEqual({ account: 1, ledger: 1, empty: 0 })
    expect(
      db.prepare("SELECT name FROM sqlite_master WHERE name GLOB '_orez_bk_*'").all()
    ).toEqual([])
    db.close()
  })

  it('preserves rowid names and cursor-shaped source columns', async () => {
    const db = ledgerDatabase(0)
    db.exec(
      'CREATE TABLE shadow (rowid INTEGER PRIMARY KEY, _rowid_ TEXT, oid TEXT, __orez_backup_rowid TEXT) WITHOUT ROWID'
    )
    db.exec(
      "INSERT INTO shadow VALUES (-2, 'a', 'b', 'c'), (0, 'd', 'e', 'f'), (3, 'g', 'h', 'i')"
    )
    const stored = writableBucket()
    await exportedSummary(
      realSqliteManager(db, stored.bucket).exportNamespace({}, 'singleton')
    )
    expect(dumped(stored).rowsOf('shadow')).toEqual(
      db.prepare('SELECT * FROM shadow ORDER BY rowid').all()
    )
    db.close()
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

// writes use real SQLite transactions; the worker suite owns admission semantics.
function durableObject(db: InstanceType<typeof BetterSqlite3>) {
  return {
    writeNow(mutate: () => void, mutates = true) {
      db.exec('BEGIN')
      try {
        mutate()
        db.exec('COMMIT')
      } catch (error) {
        db.exec('ROLLBACK')
        throw error
      }
      if (mutates)
        db.exec('UPDATE _test_backup_meta SET write_seq = write_seq + 1 WHERE id = 1')
    },
    query: async (
      _env: unknown,
      _namespace: string,
      sql: string,
      params: readonly unknown[]
    ) => {
      const statement = db.prepare(sql)
      return statement.reader ? statement.all(...params) : (statement.run(...params), [])
    },
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
    object = durableObject(db)
    const files = {
      ...stored.bucket,
      createMultipartUpload: (key: string) => {
        multipartUploads++
        return stored.bucket.createMultipartUpload(key)
      },
    }
    const manager = createNamespaceBackupManager<unknown>({
      ...sqliteSnapshotCallbacks(db, (sql) => onRead(object, sql)),
      format: 'test-v3',
      markerTable: '_test_backup_meta',
      excludedTables: ['_test_backup_meta'],
      // one chunk per page exercises cursor advancement across serialization chunks
      scanChunkBytes: 1,
      files: () => files,
      query: object.query,
      batch: async () => {},
      listNamespaces: async () => ['singleton'],
      ...managerOptions,
    })
    return { manager, object, stored, multipartUploads: () => multipartUploads }
  }

  it('exports the pre-write snapshot and carries the committed write in the next export', async () => {
    const db = ledgerDatabase(600)
    let deposits = 0
    const { manager, stored } = scenario(db, (self, sql) => {
      if (deposits || !/FROM "_orez_bk_[^"]+_ledger"/.test(sql)) return
      deposits++
      self.writeNow(deposit(db, 'new-deposit', 100))
    })
    await exportedSummary(manager.exportNamespace({}, 'singleton'))
    expect(deposits).toBe(1)
    const first = dumped(stored)
    expect(first.pointer.marker).toBe(1)
    expect(first.balance()).toBe(0)
    expect(first.rowsOf('ledger')).toHaveLength(600)
    await exportedSummary(manager.exportNamespace({}, 'singleton'))
    const second = dumped(stored)
    expect(second.pointer.marker).toBe(2)
    expect(second.balance()).toBe(100)
    expect(second.ledgerTotal()).toBe(100)
    expect(second.rowsOf('ledger')).toHaveLength(601)
    db.close()
  })

  it('completes while an interactive writer is admitted mid-scan', async () => {
    const db = ledgerDatabase(600)
    let deposits = 0
    const { manager, stored } = scenario(db, (self, sql) => {
      if (deposits > 0 || !/FROM "_orez_bk_[^"]+_ledger"/.test(sql)) return
      deposits++
      self.writeNow(deposit(db, 'l1', 100))
    })

    const { value } = await backupEvents(() => manager.exportNamespace({}, 'singleton'))

    expect(deposits).toBe(1)
    expect(value.outcome).toBe('exported')
    const dump = dumped(stored)
    expect(dump.published).toBe(true)
    // the dump is one state the database actually had
    expect(dump.ledgerTotal()).toBe(dump.balance())
    expect(dump.rowsOf('ledger')).toHaveLength(600)
    // the writer was never made to wait: its deposit is live before the export
    // returns, not replayed after it
    expect(db.prepare("SELECT balance FROM account WHERE id = 'a1'").get()).toEqual({
      balance: 100,
    })
    db.close()
  })

  it('completes an immutable snapshot while a writer changes the source during every page', async () => {
    const db = ledgerDatabase(2200)
    let deposits = 0
    const { manager, stored } = scenario(db, (self, sql) => {
      if (!/FROM "_orez_bk_/.test(sql)) return
      deposits++
      self.writeNow(deposit(db, `writer-${deposits}`, 1))
    })
    try {
      const value = await manager.exportNamespace({}, 'singleton')
      expect(value.outcome).toBe('exported')
      expect(deposits).toBeGreaterThan(3)
      const dump = dumped(stored)
      expect(dump.published).toBe(true)
      expect(dump.balance()).toBe(0)
      expect(dump.ledgerTotal()).toBe(0)
      expect(dump.rowsOf('ledger')).toHaveLength(2200)
      expect(db.prepare("SELECT balance FROM account WHERE id = 'a1'").get()).toEqual({
        balance: deposits,
      })
    } finally {
      db.close()
    }
  })

  it('exports once while an admitted writer commits no change', async () => {
    const db = ledgerDatabase(600)
    let interruptions = 0
    const { manager, stored, multipartUploads } = scenario(db, (self, sql) => {
      if (interruptions > 0 || !/FROM "_orez_bk_[^"]+_ledger"/.test(sql)) return
      interruptions++
      // admitted, took the turn, changed nothing: `applicationSqlDidCommit`
      // does not bump the marker for a session that never mutated
      self.writeNow(() => {}, false)
    })

    const { value } = await backupEvents(() => manager.exportNamespace({}, 'singleton'))

    expect(interruptions).toBe(1)
    expect(value.outcome).toBe('exported')
    // one scan and one multipart upload
    expect(multipartUploads()).toBe(1)
    expect(dumped(stored).rowsOf('ledger')).toHaveLength(600)
    db.close()
  })

  it('settles uploads and aborts without publishing when a snapshot table disappears', async () => {
    const db = ledgerDatabase(2200)
    const stored = writableBucket()
    let outstanding = 0
    let aborted = false
    let completed = false
    let missing = false
    let releaseUploads!: () => void
    let signalMissing!: () => void
    const uploadBarrier = new Promise<void>((resolve) => {
      releaseUploads = resolve
    })
    const tableMissing = new Promise<void>((resolve) => {
      signalMissing = resolve
    })
    const manager = createNamespaceBackupManager<unknown>({
      ...sqliteSnapshotCallbacks(db, (sql) => {
        if (missing || outstanding === 0) return
        const table = /FROM ("_orez_bk_[^"]+")/.exec(sql)?.[1]
        if (!table) throw new Error('expected a snapshot page')
        db.exec(`DROP TABLE ${table}`)
        missing = true
        signalMissing()
      }),
      format: 'test-v3',
      markerTable: '_test_backup_meta',
      excludedTables: ['_test_backup_meta'],
      scanChunkBytes: 1,
      partBytes: 2048,
      maxInflightParts: 64,
      files: () => ({
        ...stored.bucket,
        createMultipartUpload: async (key) => {
          const upload = await stored.bucket.createMultipartUpload(key)
          return {
            async uploadPart(partNumber, value) {
              outstanding++
              await uploadBarrier
              try {
                return await upload.uploadPart(partNumber, value)
              } finally {
                outstanding--
              }
            },
            async complete(parts) {
              completed = true
              return upload.complete(parts)
            },
            async abort() {
              expect(outstanding).toBe(0)
              aborted = true
              return upload.abort()
            },
          }
        },
      }),
      query: durableObject(db).query,
      batch: async () => {},
      listNamespaces: async () => ['singleton'],
    })
    const result = manager.exportNamespace({}, 'singleton')
    const rejected = expect(result).rejects.toThrow(/no such table/)
    try {
      await tableMissing
      expect(outstanding).toBeGreaterThan(0)
      expect(aborted).toBe(false)
      releaseUploads()
      await rejected
      expect(aborted).toBe(true)
      expect(completed).toBe(false)
      expect(stored.pointers.size).toBe(0)
      expect(
        db.prepare("SELECT name FROM sqlite_master WHERE name GLOB '_orez_bk_*'").all()
      ).toEqual([])
    } finally {
      releaseUploads()
      db.close()
    }
  })

  it('keeps reading while its uploads are still outstanding', async () => {
    const db = ledgerDatabase(600)
    let outstanding = 0
    let readsWhileUploading = 0
    let uploads = 0
    const stored = writableBucket()
    const object = durableObject(db)
    const manager = createNamespaceBackupManager<unknown>({
      ...sqliteSnapshotCallbacks(db, () => {
        if (outstanding > 0) readsWhileUploading++
      }),
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

  it('finishes one snapshot despite a commit between every chunk', async () => {
    const db = ledgerDatabase(600)
    let deposits = 0
    const { manager, stored, multipartUploads } = scenario(db, (self, sql) => {
      if (!/FROM "_orez_bk_[^"]+_ledger"/.test(sql)) return
      deposits++
      self.writeNow(deposit(db, `l${deposits}`, 1))
    })

    const { value } = await backupEvents(() => manager.exportNamespace({}, 'singleton'))

    expect(value.outcome).toBe('exported')
    expect(deposits).toBeGreaterThan(1)
    expect(multipartUploads()).toBe(1)
    expect(dumped(stored).pointer.marker).toBe(1)
    expect(dumped(stored).balance()).toBe(0)
    expect(dumped(stored).rowsOf('ledger')).toHaveLength(600)
    db.close()
  })
})

describe('namespace backup restore', () => {
  it('skips snapshot tables and rows in a restore input', async () => {
    const db = ledgerDatabase(0)
    const key = 'backups/snapshot-input.ndjson'
    const stored = bucketWith(
      key,
      dump([
        { kind: 'header', format: 'test-v3', ns: 'source', orderedTables: true },
        {
          kind: 'table',
          name: '_orez_bk_private',
          sql: 'CREATE TABLE _orez_bk_private (id INTEGER)',
          indexes: [],
        },
        { kind: 'rows', table: '_orez_bk_private', rows: [{ id: 1 }] },
        { kind: 'footer', rows: 1 },
      ])
    )
    const summary = await realSqliteManager(db, stored.bucket).importNamespace(
      {},
      'target',
      key,
      { allowNonEmpty: true }
    )
    expect(summary.rows).toBe(0)
    expect(
      db.prepare("SELECT name FROM sqlite_master WHERE name GLOB '_orez_bk_*'").all()
    ).toEqual([])
    db.close()
  })

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
    const manager = createNamespaceBackupManager({
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
    const manager = createNamespaceBackupManager({
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
    const manager = createNamespaceBackupManager({
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
    const manager = createNamespaceBackupManager({
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
    const manager = createNamespaceBackupManager({
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
    const manager = createNamespaceBackupManager({
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

  it('drops captured tables before the cdc registry when an older backup replaces a live object', async () => {
    const key = 'backups/singleton/older-cdc.ndjson'
    const stored = bucketWith(
      key,
      dump([
        { kind: 'header', format: 'test-v3', ns: 'source' },
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
    db.exec(
      'CREATE TABLE _orez_cdc_tables (physical_table TEXT PRIMARY KEY, table_name TEXT NOT NULL)'
    )
    db.exec(
      'CREATE TABLE _orez_cdc_buffer (seq INTEGER PRIMARY KEY, table_name TEXT, op TEXT)'
    )
    db.exec(
      `CREATE TRIGGER "_orez_cdc_message_insert" AFTER INSERT ON message BEGIN
        INSERT INTO "_orez_cdc_buffer" (table_name, op) VALUES ('message', 'INSERT');
      END`
    )
    db.exec("INSERT INTO _orez_cdc_tables VALUES ('message', 'public.message')")
    db.exec("INSERT INTO message VALUES ('current')")
    db.exec("INSERT INTO messageReaction VALUES ('reaction', 'current')")
    const drops: string[] = []
    const manager = createNamespaceBackupManager({
      ...sqliteSnapshotCallbacks(db),
      format: 'test-v3',
      markerTable: '_test_backup_meta',
      excludedTables: ['_test_backup_meta'],
      files: () => stored.bucket,
      query: async (_env, _namespace, sql, params) => {
        const statement = db.prepare(sql)
        if (statement.reader) return statement.all(...params)
        statement.run(...params)
        return []
      },
      batch: async (_env, _namespace, statements) => {
        db.exec('BEGIN')
        try {
          for (const statement of statements) {
            const target = /^DROP TABLE IF EXISTS "([^"]+)"$/.exec(statement.sql)?.[1]
            if (target) drops.push(statement.sql)
            if (
              target &&
              target !== '_orez_cdc_tables' &&
              target !== '_orez_cdc_buffer' &&
              db
                .prepare(
                  "SELECT 1 AS ok FROM sqlite_master WHERE type = 'trigger' AND tbl_name = ? AND name GLOB '_orez_cdc_*'"
                )
                .get(target) &&
              !db
                .prepare(
                  "SELECT 1 AS ok FROM sqlite_master WHERE type = 'table' AND name = '_orez_cdc_tables'"
                )
                .get()
            ) {
              throw new Error('no such table: _orez_cdc_tables')
            }
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

    await manager.importNamespace({}, 'singleton', key, { allowNonEmpty: true })

    const messageDrop = drops.indexOf('DROP TABLE IF EXISTS "message"')
    const registryDrop = drops.indexOf('DROP TABLE IF EXISTS "_orez_cdc_tables"')
    const bufferDrop = drops.indexOf('DROP TABLE IF EXISTS "_orez_cdc_buffer"')
    expect(messageDrop).toBeGreaterThanOrEqual(0)
    expect(messageDrop).toBeLessThan(registryDrop)
    expect(messageDrop).toBeLessThan(bufferDrop)
    expect(db.prepare('SELECT id FROM message').all()).toEqual([{ id: 'restored' }])
    expect(
      db
        .prepare(
          "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE '\\_orez\\_cdc\\_%' ESCAPE '\\'"
        )
        .all()
    ).toEqual([])
    db.close()
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
    const manager = createNamespaceBackupManager({
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
    const manager = createNamespaceBackupManager({
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
    const manager = createNamespaceBackupManager({
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
