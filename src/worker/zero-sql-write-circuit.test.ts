import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import {
  type DurableSqlCursor,
  type DurableSqlStorage,
  installZeroSqlWriteCircuitBreaker,
} from './zero-sql-write-circuit.js'

// a DurableObject SqlStorage stand-in: the circuit meter table is backed by a
// real JS object (so the breaker's SELECT/UPDATE round-trips work), and any
// other (guarded) statement returns a test-controlled rowsWritten — letting us
// drive the windowing math without inserting millions of real rows.
class FakeSql implements DurableSqlStorage {
  table: string
  state = {
    window_start: 0,
    rows_in_window: 0,
    first_over_at: 0,
    tripped_at: 0,
    last_statement: '',
  }
  rowsForNextMutation = 0
  tableCreated = false

  constructor(table = '_orez_write_circuit') {
    this.table = table
  }

  private cursor(
    rows: Array<Record<string, unknown>>,
    rowsWritten = 0
  ): DurableSqlCursor {
    return { one: () => rows[0], rowsWritten }
  }

  exec(sql: string, ...params: unknown[]): DurableSqlCursor {
    if (sql.startsWith('CREATE TABLE IF NOT EXISTS ' + this.table)) {
      this.tableCreated = true
      return this.cursor([])
    }
    if (sql.startsWith('INSERT OR IGNORE INTO ' + this.table)) return this.cursor([])
    if (sql.startsWith('SELECT window_start') && sql.includes(this.table)) {
      return this.cursor([{ ...this.state }])
    }
    if (sql.startsWith('UPDATE ' + this.table)) {
      this.state.window_start = Number(params[0])
      this.state.rows_in_window = Number(params[1])
      this.state.first_over_at = Number(params[2])
      this.state.tripped_at = Number(params[3])
      this.state.last_statement = String(params[4])
      return this.cursor([])
    }
    // a guarded mutation (e.g. INSERT INTO data ...): a real DO would write rows
    return this.cursor([], this.rowsForNextMutation)
  }
}

// write `rows` through the wrapped exec as a single guarded mutation.
function write(sql: FakeSql, rows: number) {
  sql.rowsForNextMutation = rows
  sql.exec('insert into app_data values (1)')
}

const T0 = 1_700_000_000_000

describe('installZeroSqlWriteCircuitBreaker', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(T0)
  })
  afterEach(() => {
    vi.useRealTimers()
  })

  it('is idempotent and lazily creates the meter table on first write', () => {
    const sql = new FakeSql()
    installZeroSqlWriteCircuitBreaker(sql)
    const wrapped = sql.exec
    installZeroSqlWriteCircuitBreaker(sql) // second install is a no-op
    expect(sql.exec).toBe(wrapped)
    expect(sql.tableCreated).toBe(false) // not created until a metered write runs
    write(sql, 10)
    expect(sql.tableCreated).toBe(true)
  })

  it('passes through writes under the soft cap without tripping', () => {
    const sql = new FakeSql()
    installZeroSqlWriteCircuitBreaker(sql)
    expect(() => write(sql, 1_000)).not.toThrow()
    expect(sql.state.rows_in_window).toBe(1_000)
    expect(sql.state.tripped_at).toBe(0)
    expect(sql.state.first_over_at).toBe(0)
  })

  it('never meters reads or the breaker table itself', () => {
    const sql = new FakeSql()
    installZeroSqlWriteCircuitBreaker(sql)
    sql.rowsForNextMutation = 5_000_000
    sql.exec('select * from app_data') // read: not a mutation, not metered
    expect(sql.state.rows_in_window).toBe(0)
  })

  it('trips instantly past the hard cap and then refuses all writes', () => {
    const sql = new FakeSql()
    installZeroSqlWriteCircuitBreaker(sql)
    // the write that crosses the hard cap is recorded, then rejected
    expect(() => write(sql, 10_000_001)).toThrow(/circuit breaker tripped/)
    expect(sql.state.tripped_at).toBe(T0)
    // subsequent writes are refused up-front
    expect(() => write(sql, 1)).toThrow(/refusing SQL write/)
    // reads still pass
    expect(() => sql.exec('select 1')).not.toThrow()
  })

  it('trips only after the rate stays over the soft cap for the sustained window', () => {
    const sql = new FakeSql()
    installZeroSqlWriteCircuitBreaker(sql)
    // each 60s window stays just over the 2M soft cap; firstOverAt persists
    // across the resets, so the 180s sustained timer accrues.
    write(sql, 2_500_000) // T0: over soft, starts the sustained timer
    expect(sql.state.first_over_at).toBe(T0)
    expect(sql.state.tripped_at).toBe(0)

    vi.setSystemTime(T0 + 61_000)
    write(sql, 2_500_000)
    expect(sql.state.tripped_at).toBe(0)

    vi.setSystemTime(T0 + 122_000)
    write(sql, 2_500_000)
    expect(sql.state.tripped_at).toBe(0)

    vi.setSystemTime(T0 + 183_000) // 183s >= 180s sustained -> trip
    expect(() => write(sql, 2_500_000)).toThrow(/circuit breaker tripped/)
    expect(sql.state.tripped_at).toBe(T0 + 183_000)
  })

  it('resets the window so a steady moderate write rate never trips', () => {
    const sql = new FakeSql()
    installZeroSqlWriteCircuitBreaker(sql)
    for (let i = 0; i < 10; i++) {
      vi.setSystemTime(T0 + i * 61_000)
      write(sql, 1_000_000) // under the 2M soft cap each window
      expect(sql.state.rows_in_window).toBe(1_000_000) // window reset each time
      expect(sql.state.tripped_at).toBe(0)
    }
  })

  it('honours a consumer table name and log prefix (soot config)', () => {
    const sql = new FakeSql('_soot_write_circuit')
    installZeroSqlWriteCircuitBreaker(sql, {
      table: '_soot_write_circuit',
      logPrefix: '[soot]',
    })
    expect(() => write(sql, 10_000_001)).toThrow(
      /\[soot\] ZeroSqlDO write circuit breaker/
    )
  })
})
