import { describe, expect, it, vi } from 'vitest'

vi.mock('cloudflare:workers', () => ({
  DurableObject: class {},
}))

class FakeResult {
  constructor(private readonly rows: Array<Record<string, unknown>> = []) {}

  one() {
    return this.rows[0]
  }

  toArray() {
    return this.rows
  }
}

class FakeSql {
  tables = new Map<string, Set<string>>()
  alters: string[] = []

  exec(sql: string, ...params: unknown[]) {
    if (sql.startsWith('CREATE TABLE IF NOT EXISTS _zero_schema_tables')) {
      return new FakeResult()
    }

    if (sql.startsWith('SELECT name FROM sqlite_master')) {
      const tableName = String(params[0])
      return new FakeResult(this.tables.has(tableName) ? [{ name: tableName }] : [])
    }

    if (sql.startsWith('CREATE TABLE IF NOT EXISTS')) {
      const tableName = sql.match(/CREATE TABLE IF NOT EXISTS "([^"]+)"/)?.[1]
      if (!tableName) throw new Error(`missing table name: ${sql}`)
      if (this.tables.has(tableName)) return new FakeResult()
      const columns = [...sql.matchAll(/"([^"]+)" (?:TEXT|REAL|INTEGER)/g)].map(
        (match) => match[1]
      )
      this.tables.set(tableName, new Set(columns))
      return new FakeResult()
    }

    if (sql.startsWith('PRAGMA table_info')) {
      const tableName = sql.match(/PRAGMA table_info\("([^"]+)"\)/)?.[1]
      if (!tableName) throw new Error(`missing pragma table name: ${sql}`)
      return new FakeResult(
        [...(this.tables.get(tableName) ?? [])].map((name) => ({ name }))
      )
    }

    if (sql.startsWith('ALTER TABLE')) {
      this.alters.push(sql)
      const match = sql.match(/ALTER TABLE "([^"]+)" ADD COLUMN "([^"]+)"/)
      if (!match) throw new Error(`unsupported alter: ${sql}`)
      const [, tableName, columnName] = match
      this.tables.get(tableName)?.add(columnName)
      return new FakeResult()
    }

    if (sql.startsWith('INSERT OR REPLACE INTO _zero_schema_tables')) {
      return new FakeResult()
    }

    throw new Error(`unexpected fake sql: ${sql}`)
  }
}

describe('ZeroDO schema table maintenance', () => {
  it('adds newly declared columns to existing tables', async () => {
    const { ZeroDO } = await import('./worker.js')
    const sql = new FakeSql()
    const zero = Object.create(ZeroDO.prototype) as any
    zero.sql = sql
    zero.schemaTables = new Set()
    zero.tableSchemas = new Map()

    zero.ensureSchemaTables({
      tables: {
        post: {
          primaryKey: ['id'],
          columns: {
            id: { type: 'string' },
            caption: { type: 'string' },
          },
        },
      },
    })

    zero.ensureSchemaTables({
      tables: {
        post: {
          primaryKey: ['id'],
          columns: {
            id: { type: 'string' },
            caption: { type: 'string' },
            syncMarker: { type: 'string' },
          },
        },
      },
    })

    expect([...sql.tables.get('post')!]).toEqual(['id', 'caption', 'syncMarker'])
    expect(sql.alters).toEqual(['ALTER TABLE "post" ADD COLUMN "syncMarker" TEXT'])
  })
})
