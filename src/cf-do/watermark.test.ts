import { describe, expect, it } from 'vitest'

import { DurableWatermarkState } from './watermark.js'

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
  changes: Array<{ watermark: number }> = []
  state = 0
  sequence = { name: '_orez___zero_watermark', last_value: 1, is_called: 0 }

  exec(sql: string, ...params: unknown[]) {
    if (sql.startsWith('CREATE TABLE IF NOT EXISTS')) return new FakeResult()

    if (sql.startsWith('SELECT last_value FROM "_zero_change_state"')) {
      return new FakeResult([{ last_value: this.state }])
    }

    if (sql.startsWith('INSERT OR IGNORE INTO "_zero_change_state"')) {
      return new FakeResult()
    }

    if (sql.startsWith('UPDATE "_zero_change_state" SET last_value = ?')) {
      this.state = Number(params[0])
      return new FakeResult()
    }

    if (
      sql.startsWith('SELECT COALESCE(MAX(watermark), 0) AS watermark FROM _zero_changes')
    ) {
      const watermark = Math.max(0, ...this.changes.map((change) => change.watermark))
      return new FakeResult([{ watermark }])
    }

    if (sql.includes('sqlite_master') && sql.includes('%zero_watermark%')) {
      return new FakeResult([{ name: this.sequence.name }])
    }

    if (sql.startsWith('SELECT last_value, is_called FROM "_orez___zero_watermark"')) {
      return new FakeResult([
        {
          last_value: this.sequence.last_value,
          is_called: this.sequence.is_called,
        },
      ])
    }

    if (sql.startsWith('INSERT OR IGNORE INTO "_orez___zero_watermark"')) {
      return new FakeResult()
    }

    if (sql.startsWith('UPDATE "_orez___zero_watermark" SET last_value = ?')) {
      this.sequence.last_value = Number(params[0])
      this.sequence.is_called = 1
      return new FakeResult()
    }

    throw new Error(`unexpected fake sql: ${sql}`)
  }
}

describe('DurableWatermarkState', () => {
  it('does not reuse watermarks after consumed changes are purged', () => {
    const sql = new FakeSql()
    const watermarks = new DurableWatermarkState(sql)

    expect(watermarks.current()).toBe(0)

    const first = watermarks.next()
    sql.changes.push({ watermark: first })
    watermarks.mark(first)
    expect(first).toBe(1)

    sql.changes = []

    const second = watermarks.next()
    sql.changes.push({ watermark: second })
    watermarks.mark(second)

    expect(second).toBe(2)
    expect(sql.sequence).toMatchObject({ last_value: 2, is_called: 1 })
  })

  it('synchronizes from existing change rows and sequence state', () => {
    const sql = new FakeSql()
    sql.changes.push({ watermark: 7 })
    const watermarks = new DurableWatermarkState(sql)

    expect(watermarks.current()).toBe(7)
    expect(sql.state).toBe(7)
    expect(sql.sequence).toMatchObject({ last_value: 7, is_called: 1 })
  })
})
