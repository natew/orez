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

class FakeChangeSql {
  changeState = 0
  pendingSeq = 0
  pending: Array<{
    id: number
    transaction_id: string
    table_name: string
    op: string
    row_data: string | null
    old_data: string | null
  }> = []
  changes: Array<{
    watermark: number
    table_name: string
    op: string
    row_data: string | null
    old_data: string | null
  }> = []

  exec(sql: string, ...params: unknown[]) {
    if (sql.startsWith('CREATE TABLE IF NOT EXISTS _zero_changes')) {
      return new FakeResult()
    }
    if (sql.startsWith('CREATE TABLE IF NOT EXISTS "_zero_change_state"')) {
      return new FakeResult()
    }
    if (sql.startsWith('CREATE TABLE IF NOT EXISTS _zero_pending_changes')) {
      return new FakeResult()
    }
    if (sql.startsWith('INSERT OR IGNORE INTO "_zero_change_state"')) {
      return new FakeResult()
    }
    if (sql.startsWith('UPDATE "_zero_change_state" SET last_value = ?')) {
      this.changeState = Number(params[0])
      return new FakeResult()
    }
    if (sql.startsWith('SELECT last_value FROM "_zero_change_state"')) {
      return new FakeResult([{ last_value: this.changeState }])
    }
    if (
      sql.startsWith('SELECT COALESCE(MAX(watermark), 0) AS watermark FROM _zero_changes')
    ) {
      return new FakeResult([
        {
          watermark: Math.max(0, ...this.changes.map((change) => change.watermark)),
        },
      ])
    }
    if (sql.includes('sqlite_master') && sql.includes('%zero_watermark%')) {
      return new FakeResult()
    }
    if (sql.startsWith('INSERT INTO _zero_pending_changes')) {
      const [transaction_id, table_name, op, row_data, old_data] = params
      this.pending.push({
        id: ++this.pendingSeq,
        transaction_id: String(transaction_id),
        table_name: String(table_name),
        op: String(op),
        row_data: row_data === null ? null : String(row_data),
        old_data: old_data === null ? null : String(old_data),
      })
      return new FakeResult()
    }
    if (
      sql.startsWith(
        'SELECT id, table_name, op, row_data, old_data FROM _zero_pending_changes'
      )
    ) {
      const transactionID = String(params[0])
      return new FakeResult(
        this.pending
          .filter((change) => change.transaction_id === transactionID)
          .sort((a, b) => a.id - b.id)
      )
    }
    if (sql.startsWith('DELETE FROM _zero_pending_changes')) {
      const transactionID = String(params[0])
      const deleted = this.pending.filter(
        (change) => change.transaction_id === transactionID
      )
      this.pending = this.pending.filter(
        (change) => change.transaction_id !== transactionID
      )
      return new FakeResult(deleted.map(() => ({ deleted: 1 })))
    }
    if (sql.startsWith('INSERT INTO _zero_changes')) {
      const [watermark, table_name, op, row_data, old_data] = params
      this.changes.push({
        watermark: Number(watermark),
        table_name: String(table_name),
        op: String(op),
        row_data: row_data === null ? null : String(row_data),
        old_data: old_data === null ? null : String(old_data),
      })
      return new FakeResult()
    }
    if (
      sql.startsWith(
        'SELECT watermark, table_name, op, row_data, old_data FROM _zero_changes'
      )
    ) {
      const watermark = Number(params[0])
      return new FakeResult(
        this.changes
          .filter((change) => change.watermark > watermark)
          .sort((a, b) => a.watermark - b.watermark)
      )
    }

    throw new Error(`unexpected fake change sql: ${sql}`)
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

  it('keeps transaction-tracked changes hidden until commit', async () => {
    const { ZeroDO } = await import('./worker.js')
    const { DurableWatermarkState } = await import('./watermark.js')
    const sql = new FakeChangeSql()
    const zero = Object.create(ZeroDO.prototype) as any
    zero.sql = sql
    zero.watermarks = new DurableWatermarkState(sql)

    zero.appendTrackedChange(
      'zero_0.clients',
      'UPDATE',
      { clientID: 'client-a', lastMutationID: 1 },
      null,
      'tx-a'
    )

    expect(zero.readChangesSince(0)).toEqual([])

    zero.appendTrackedChange('todo', 'INSERT', { id: 'todo-a' }, null)
    expect(zero.readChangesSince(0).map((change: any) => change.tableName)).toEqual([
      'todo',
    ])

    expect(zero.commitPendingTrackedChanges('tx-a')).toBe(1)
    expect(zero.readChangesSince(0).map((change: any) => change.tableName)).toEqual([
      'todo',
      'zero_0.clients',
    ])
    expect(zero.readChangesSince(1).map((change: any) => change.tableName)).toEqual([
      'zero_0.clients',
    ])
  })

  it('drops transaction-tracked changes on rollback', async () => {
    const { ZeroDO } = await import('./worker.js')
    const { DurableWatermarkState } = await import('./watermark.js')
    const sql = new FakeChangeSql()
    const zero = Object.create(ZeroDO.prototype) as any
    zero.sql = sql
    zero.watermarks = new DurableWatermarkState(sql)

    zero.appendTrackedChange(
      'zero_0.clients',
      'UPDATE',
      { clientID: 'client-a' },
      null,
      'tx-a'
    )

    expect(zero.deletePendingTrackedChanges('tx-a')).toBe(1)
    expect(zero.commitPendingTrackedChanges('tx-a')).toBe(0)
    expect(zero.readChangesSince(0)).toEqual([])
  })
})
