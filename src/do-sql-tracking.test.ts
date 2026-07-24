import { describe, expect, it } from 'vitest'

import {
  isSqlMutation,
  isSqlRowMutation,
  RollingRowWriteBudget,
  trackSqlCursorRowsWritten,
  trackedChangeRow,
  WriteBudgetExceededError,
} from './do-sql-tracking.js'

describe('trackedChangeRow', () => {
  it('keeps only table columns and strips internal returning expressions', () => {
    expect(
      trackedChangeRow(
        {
          id: 't1',
          body: 'hello',
          __orez_returning_1: 'HELLO',
          extra: 'client-visible only',
        },
        { rowColumns: ['id', 'body'] }
      )
    ).toEqual({ id: 't1', body: 'hello' })
  })
})

describe('RollingRowWriteBudget', () => {
  it('counts a true rolling window with an injected deterministic clock', () => {
    let now = 1_000
    const meter = new RollingRowWriteBudget({
      budgetRows: 100,
      windowMs: 5_000,
      now: () => now,
    })
    meter.record(40)
    now = 4_000
    meter.record(50)
    expect(meter.status().windowRows).toBe(90)
    now = 7_001
    expect(meter.status().windowRows).toBe(50)
    meter.record(50)
    expect(meter.status().windowRows).toBe(100)
  })

  it('trips above the budget, stays sticky, and reopens with cleared pace', () => {
    let now = 10
    const meter = new RollingRowWriteBudget({
      budgetRows: 10,
      windowMs: 1_000,
      now: () => now,
    })
    meter.record(10)
    expect(() => meter.record(1)).toThrow(WriteBudgetExceededError)
    now = 2_000
    // The live window has fully decayed by now, so a sticky rejection has to
    // report the trip's own count rather than re-reading the rolling meter.
    expect(() => meter.record(1)).toThrow('row write budget exceeded: 11/10 rows')
    expect(meter.status()).toMatchObject({
      windowRows: 0,
      budget: 10,
      tripped: true,
      trippedAt: 10,
      trippedWindowRows: 11,
      trippedBudget: 10,
    })
    expect(meter.reopen()).toMatchObject({
      windowRows: 0,
      tripped: false,
      trippedWindowRows: null,
    })
    expect(() => meter.record(1)).not.toThrow()
  })

  it('round-trips a persisted trip so a restored object still reports its count', () => {
    let now = 10
    const tripped = new RollingRowWriteBudget({
      budgetRows: 10,
      windowMs: 1_000,
      now: () => now,
    })
    tripped.record(10)
    expect(() => tripped.record(5)).toThrow(WriteBudgetExceededError)
    const persisted = JSON.parse(JSON.stringify(tripped.trip()))
    expect(persisted).toEqual({ at: 10, windowRows: 15, budget: 10, windowMs: 1_000 })

    const restored = new RollingRowWriteBudget({
      budgetRows: 10,
      windowMs: 1_000,
      now: () => 900_000,
    })
    restored.restoreTrip(persisted)
    expect(restored.status()).toMatchObject({
      windowRows: 0,
      tripped: true,
      trippedAt: 10,
      trippedWindowRows: 15,
    })
    expect(() => restored.assertOpen()).toThrow('row write budget exceeded: 15/10 rows')
  })

  it('restores a legacy bare-timestamp trip as unrecorded rather than zero rows', () => {
    const meter = new RollingRowWriteBudget({
      budgetRows: 10,
      windowMs: 1_000,
      now: () => 5_000,
    })
    meter.restoreTrip(4_000)
    expect(meter.status()).toMatchObject({
      windowRows: 0,
      tripped: true,
      trippedAt: 4_000,
      trippedWindowRows: null,
      trippedBudget: 10,
    })
    expect(() => meter.assertOpen()).toThrow(
      'row write budget exceeded: unrecorded/10 rows'
    )
  })
})

describe('isSqlMutation', () => {
  it('keeps reads open and recognizes direct and CTE writes', () => {
    expect(isSqlMutation('select 1')).toBe(false)
    expect(isSqlMutation('WITH x AS (SELECT 1) SELECT * FROM x')).toBe(false)
    expect(isSqlMutation('CREATE TABLE x (id INTEGER)')).toBe(true)
    expect(isSqlMutation('/* migration */\nINSERT INTO t VALUES (1)')).toBe(true)
    expect(isSqlMutation('SELECT 1; -- next\nDELETE FROM t')).toBe(true)
    expect(isSqlMutation('WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x')).toBe(true)
  })

  it('distinguishes row DML from schema and maintenance writes', () => {
    expect(isSqlRowMutation('/* write */ UPDATE t SET value = 1')).toBe(true)
    expect(isSqlRowMutation('WITH x AS (SELECT 1) DELETE FROM t')).toBe(true)
    expect(isSqlRowMutation('CREATE TABLE t (id INTEGER)')).toBe(false)
    expect(isSqlRowMutation('VACUUM')).toBe(false)
  })
})

describe('trackSqlCursorRowsWritten', () => {
  it('records billing rows that appear only while a RETURNING cursor is drained', () => {
    const rows = [{ id: 1 }, { id: 2 }, { id: 3 }]
    let index = 0
    const cursor = {
      rowsWritten: 0,
      next() {
        if (index >= rows.length) return { done: true, value: undefined }
        this.rowsWritten += 4 // base row + three index rows billed by CF
        return { done: false, value: rows[index++] }
      },
      toArray() {
        const out = []
        for (;;) {
          const item = this.next()
          if (item.done) return out
          out.push(item.value)
        }
      },
    }
    const deltas: number[] = []
    const tracked = trackSqlCursorRowsWritten(cursor, (delta) => deltas.push(delta))
    expect(tracked.rowsWritten).toBe(0)
    expect(tracked.toArray()).toEqual(rows)
    expect(deltas).toEqual([12])
  })

  it('records immediate rows once and monotonic deltas from next/raw iteration', () => {
    const cursor = {
      rowsWritten: 2,
      next() {
        this.rowsWritten = 5
        return { done: true, value: undefined }
      },
      raw() {
        return {
          next: () => {
            this.rowsWritten = 9
            return { done: true, value: undefined }
          },
        }
      },
    }
    const deltas: number[] = []
    const tracked = trackSqlCursorRowsWritten(cursor, (delta) => deltas.push(delta))
    tracked.next()
    tracked.raw().next()
    expect(deltas).toEqual([2, 3, 4])
  })
})
