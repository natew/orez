import { describe, expect, it, vi } from 'vitest'

import {
  doSqliteStorage,
  doSqliteStorageIncarnation,
  installDoForbiddenSqliteGuard,
  isDoForbiddenSqlite,
} from './zero-cache-do-sqlite.js'

describe('isDoForbiddenSqlite', () => {
  it('flags the storage-engine statements the DO rejects', () => {
    for (const sql of [
      'VACUUM',
      '  vacuum ',
      'ATTACH DATABASE x AS y',
      'PRAGMA journal_mode=WAL',
      'PRAGMA wal_checkpoint(TRUNCATE)',
      'pragma  synchronous = NORMAL',
      'PRAGMA cache_size = -2000',
    ]) {
      expect(isDoForbiddenSqlite(sql), sql).toBe(true)
    }
  })

  it('lets real reads, writes, and benign pragmas through', () => {
    for (const sql of [
      'SELECT 1',
      'INSERT INTO t VALUES (1)',
      'UPDATE t SET v = 1',
      'PRAGMA table_info(t)',
      'PRAGMA foreign_keys = ON',
      'CREATE TABLE t (id INTEGER)',
    ]) {
      expect(isDoForbiddenSqlite(sql), sql).toBe(false)
    }
    expect(isDoForbiddenSqlite(123)).toBe(false)
  })
})

describe('installDoForbiddenSqliteGuard', () => {
  it('no-ops forbidden statements and passes everything else to the raw exec', () => {
    const raw = vi.fn((_sql: string, ..._p: unknown[]) => ({ real: true }))
    const sql: { exec: typeof raw; [k: string]: unknown } = { exec: raw }
    installDoForbiddenSqliteGuard(sql as never)

    const noop = sql.exec('PRAGMA journal_mode=WAL') as { toArray(): unknown[] }
    expect(noop.toArray()).toEqual([])
    expect(raw).not.toHaveBeenCalled()

    const real = sql.exec('SELECT 1')
    expect(real).toEqual({ real: true })
    expect(raw).toHaveBeenCalledWith('SELECT 1')
  })

  it('is idempotent (a second install does not double-wrap)', () => {
    const sql: { exec: (s: string) => unknown; [k: string]: unknown } = {
      exec: (s: string) => s,
    }
    installDoForbiddenSqliteGuard(sql as never)
    const wrapped = sql.exec
    installDoForbiddenSqliteGuard(sql as never)
    expect(sql.exec).toBe(wrapped)
  })
})

describe('doSqliteStorage', () => {
  it('no-ops forbidden statements and binds storage methods', async () => {
    const calls: string[] = []
    const ctx = {
      storage: {
        sql: {
          exec: (sql: string) => {
            calls.push(sql)
            return { rows: [sql] }
          },
        },
        sync: async function (this: unknown) {},
        transactionSync: function (this: unknown, fn: () => unknown) {
          return fn()
        },
      },
    }
    const wrapped = doSqliteStorage(ctx as never)
    expect((wrapped.exec('VACUUM') as { toArray(): unknown[] }).toArray()).toEqual([])
    expect(calls).toEqual([]) // VACUUM never reached the raw exec
    expect(wrapped.exec('SELECT 2')).toEqual({ rows: ['SELECT 2'] })
    expect(typeof wrapped.sync).toBe('function')
    await expect(wrapped.sync!()).resolves.toBeUndefined()
    expect(typeof wrapped.transactionSync).toBe('function')
    expect(wrapped.transactionSync!(() => 'ran')).toBe('ran')
  })

  it('leaves optional storage methods undefined when the platform lacks them', () => {
    const ctx = { storage: { sql: { exec: (s: string) => s } } }
    const wrapped = doSqliteStorage(ctx as never)
    expect(wrapped.sync).toBeUndefined()
    expect(wrapped.transactionSync).toBeUndefined()
  })

  it('carries a stable identity for one Durable Object incarnation', () => {
    const firstCtx = { storage: { sql: { exec: (s: string) => s } } }
    const secondCtx = { storage: { sql: { exec: (s: string) => s } } }
    const first = doSqliteStorage(firstCtx as never)
    const firstAgain = doSqliteStorage(firstCtx as never)
    const second = doSqliteStorage(secondCtx as never)

    expect(firstAgain).not.toBe(first)
    expect(doSqliteStorageIncarnation(firstAgain)).toBe(doSqliteStorageIncarnation(first))
    expect(doSqliteStorageIncarnation(second)).not.toBe(doSqliteStorageIncarnation(first))
  })
})
