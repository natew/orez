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
    async createMultipartUpload() {
      return {
        async uploadPart(partNumber, value) {
          return { partNumber, value }
        },
        async complete() {},
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

describe('namespace backup export', () => {
  it('records per-table row counts in the durable summary without another scan', async () => {
    const stored = writableBucket()
    const manager = createNamespaceBackupManager({
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
        if (sql.startsWith('PRAGMA foreign_key_list')) return []
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
    const manager = createNamespaceBackupManager({
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
        if (sql.startsWith('PRAGMA foreign_key_list')) return []
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
})
