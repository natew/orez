import { describe, expect, it } from 'vitest'

import {
  isSqlMutation,
  RollingRowWriteBudget,
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
    expect(() => meter.record(1)).toThrow(WriteBudgetExceededError)
    expect(meter.status()).toMatchObject({
      windowRows: 0,
      budget: 10,
      tripped: true,
      trippedAt: 10,
    })
    expect(meter.reopen()).toMatchObject({ windowRows: 0, tripped: false })
    expect(() => meter.record(1)).not.toThrow()
  })

  it('restores a persisted sticky trip without synthesizing write rows', () => {
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
    })
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
})
