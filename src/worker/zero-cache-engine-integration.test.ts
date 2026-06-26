// real-SQLite integration for the lifted DO engine: backs the circuit breaker +
// replica-repair functions with an actual node:sqlite database through a
// DO-SqlStorage adapter, so the lifted SQL (CREATE/INSERT/SELECT/UPDATE/DROP,
// sqlite_master scans, the circuit meter) is exercised end to end, not mocked.
import { DatabaseSync } from 'node:sqlite'

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import {
  dropReplicaTables,
  repairPartialReplicaInit,
  resetReplicaIfTableSetChanged,
} from './zero-cache-replica-repair.js'
import { installZeroSqlWriteCircuitBreaker } from './zero-sql-write-circuit.js'

import type { DurableSqlStorage } from './zero-sql-write-circuit.js'

type Db = InstanceType<typeof DatabaseSync>

// minimal Cloudflare DO SqlStorage shape over a real node:sqlite handle:
// exec(sql, ...params) -> cursor with one()/toArray()/rowsWritten.
function doSqlAdapter(db: Db) {
  return {
    exec(sql: string, ...params: unknown[]) {
      const stmt = db.prepare(sql)
      if (/^\s*select/i.test(sql)) {
        const rows = stmt.all(...(params as never[])) as Array<Record<string, unknown>>
        return { one: () => rows[0], toArray: () => rows, rowsWritten: 0 }
      }
      const info = stmt.run(...(params as never[]))
      return {
        one: () => undefined,
        toArray: () => [],
        rowsWritten: Number(info.changes ?? 0),
      }
    },
  }
}

describe('circuit breaker over real sqlite', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(1_700_000_000_000)
  })
  afterEach(() => vi.useRealTimers())

  it('creates its meter table, meters real writes, and trips on the hard cap', () => {
    const db = new DatabaseSync(':memory:')
    db.exec('CREATE TABLE app (id INTEGER PRIMARY KEY, v TEXT)')
    const sql = doSqlAdapter(db) as unknown as DurableSqlStorage
    // tiny caps so a handful of real rows trips it
    installZeroSqlWriteCircuitBreaker(sql, {
      table: '_wc',
      rowsPerWindow: 3,
      hardRowsPerWindow: 5,
      logPrefix: '[itest]',
    })

    // the meter table is created on first metered write, and real rows count
    sql.exec('INSERT INTO app (v) VALUES (?)', 'a')
    const meter = db
      .prepare('SELECT rows_in_window, tripped_at FROM _wc WHERE id = 1')
      .get() as {
      rows_in_window: number
      tripped_at: number
    }
    expect(meter.rows_in_window).toBe(1)
    expect(meter.tripped_at).toBe(0)

    // cross the hard cap (5) within the window -> trips and refuses writes
    expect(() => {
      for (let i = 0; i < 20; i++) sql.exec('INSERT INTO app (v) VALUES (?)', 'x')
    }).toThrow(/\[itest\] ZeroSqlDO write circuit breaker tripped/)
    expect(
      (
        db.prepare('SELECT tripped_at FROM _wc WHERE id = 1').get() as {
          tripped_at: number
        }
      ).tripped_at
    ).toBeGreaterThan(0)
    // reads still pass after the trip
    expect(() => sql.exec('SELECT COUNT(*) FROM app')).not.toThrow()
    // and writes stay refused
    expect(() => sql.exec('INSERT INTO app (v) VALUES (?)', 'z')).toThrow(
      /refusing SQL write/
    )
  })
})

describe('replica repair over real sqlite', () => {
  function seedReplica(db: Db) {
    db.exec('CREATE TABLE "_zero.replicationConfig" (k TEXT)')
    db.exec('CREATE TABLE "user" (id INTEGER)')
    db.exec('CREATE TABLE project (id INTEGER)')
  }

  it('dropReplicaTables removes app + _zero tables and leaves sqlite internals', () => {
    const db = new DatabaseSync(':memory:')
    seedReplica(db)
    const sql = doSqlAdapter(db) as never
    const dropped = dropReplicaTables(sql)
    expect(dropped).toBe(3)
    const left = (
      db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all() as Array<{
        name: string
      }>
    ).map((r) => r.name)
    expect(left.filter((n) => !n.startsWith('sqlite_'))).toEqual([])
  })

  it('repairPartialReplicaInit wipes a config-present / versionHistory-missing replica', () => {
    const db = new DatabaseSync(':memory:')
    seedReplica(db) // replicationConfig present, no _zero.versionHistory table
    const sql = doSqlAdapter(db) as never
    repairPartialReplicaInit(sql, { logPrefix: '[itest]' })
    const tables = (
      db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all() as Array<{
        name: string
      }>
    ).map((r) => r.name)
    expect(tables.filter((n) => !n.startsWith('sqlite_'))).toEqual([])
  })

  it('repairPartialReplicaInit leaves a cleanly-initialized replica intact', () => {
    const db = new DatabaseSync(':memory:')
    seedReplica(db)
    db.exec('CREATE TABLE "_zero.versionHistory" (v INTEGER)')
    db.exec('INSERT INTO "_zero.versionHistory" (v) VALUES (1)')
    const sql = doSqlAdapter(db) as never
    repairPartialReplicaInit(sql, { logPrefix: '[itest]' })
    const tables = (
      db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all() as Array<{
        name: string
      }>
    ).map((r) => r.name)
    expect(tables).toContain('user')
    expect(tables).toContain('project')
  })

  it('resetReplicaIfTableSetChanged wipes on a changed tag and persists the new one', async () => {
    const db = new DatabaseSync(':memory:')
    seedReplica(db)
    const sql = doSqlAdapter(db) as never
    const kv = new Map<string, unknown>()
    const storage = {
      get: async (k: string) => kv.get(k),
      put: async (k: string, v: unknown) => {
        kv.set(k, v)
      },
    }
    await resetReplicaIfTableSetChanged(sql, storage, {
      schemaVersion: 'v1',
      tables: ['user', 'project'],
      tagKey: '__tag',
    })
    const tables = (
      db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all() as Array<{
        name: string
      }>
    ).map((r) => r.name)
    expect(tables.filter((n) => !n.startsWith('sqlite_'))).toEqual([])
    expect(kv.get('__tag')).toBe(JSON.stringify(['v1', ['project', 'user']]))
  })
})
