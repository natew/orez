import { describe, expect, test } from 'bun:test'

import {
  IngestCircuitBreaker,
  retryDelayMs,
  shouldRetryDelegatedPush,
  trackBillableCursorRows,
} from './src/write-safeguards.ts'

describe('IngestCircuitBreaker', () => {
  test('trips on rolling rows, backs off exponentially, and recovers', () => {
    let now = 1_000
    const breaker = new IngestCircuitBreaker({
      budgetRows: 10,
      windowMs: 1_000,
      initialBackoffMs: 100,
      maxBackoffMs: 400,
      now: () => now,
    })
    breaker.record(6)
    expect(() => breaker.record(5)).toThrow('ingestBudgetExceeded')
    expect(breaker.status()).toMatchObject({
      windowRows: 11,
      retryAfterMs: 100,
      consecutiveTrips: 1,
    })
    now += 100
    expect(() => breaker.record(1)).toThrow('ingestBudgetExceeded')
    expect(breaker.status().retryAfterMs).toBe(200)
    now += 1_001
    breaker.recovered()
    expect(() => breaker.record(1)).not.toThrow()
  })

  test('uses the same cooldown for a non-advancing cursor signature', () => {
    let now = 0
    const breaker = new IngestCircuitBreaker({
      budgetRows: 100,
      windowMs: 1_000,
      initialBackoffMs: 25,
      maxBackoffMs: 100,
      now: () => now,
    })
    expect(() => breaker.trip('ingestCursorStalled')).toThrow('ingestCursorStalled')
    now = 25
    expect(() => breaker.trip('ingestCursorStalled')).toThrow('ingestCursorStalled')
    expect(breaker.status().retryAfterMs).toBe(50)
  })

  test('restores persisted cooldown state without synthetic row samples', () => {
    const breaker = new IngestCircuitBreaker({
      budgetRows: 100,
      windowMs: 1_000,
      initialBackoffMs: 25,
      maxBackoffMs: 100,
      now: () => 50,
    })
    breaker.restore('ingestCursorStalled', 75, 3)
    expect(breaker.status()).toMatchObject({
      reason: 'ingestCursorStalled',
      retryAfterMs: 25,
      consecutiveTrips: 3,
      windowRows: 0,
    })
  })
})

test('delegated push retries are bounded and exponentially capped', () => {
  expect(retryDelayMs(1, 100, 250)).toBe(100)
  expect(retryDelayMs(3, 100, 250)).toBe(250)
  expect(shouldRetryDelegatedPush(null, 1, 3)).toBe(true)
  expect(shouldRetryDelegatedPush(503, 2, 3)).toBe(true)
  expect(shouldRetryDelegatedPush(503, 3, 3)).toBe(false)
  expect(shouldRetryDelegatedPush(400, 1, 3)).toBe(false)
})

test('billable cursor tracking captures rows that appear during raw iteration', () => {
  const cursor = {
    rowsWritten: 0,
    raw() {
      let done = false
      return {
        next: () => {
          if (done) return { done: true }
          done = true
          this.rowsWritten = 7
          return { done: false, value: [1] }
        },
      }
    },
  }
  const deltas = []
  const tracked = trackBillableCursorRows(cursor, (rows) => deltas.push(rows))
  const raw = tracked.raw()
  raw.next()
  raw.next()
  expect(deltas).toEqual([7])
})
