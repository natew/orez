import { describe, expect, it, vi } from 'vitest'

import {
  type BackendExec,
  type ReplicaKvStorage,
  type ReplicaSqlResult,
  type ReplicaSqlStorage,
  clearChangeStreamerStateIfReplicaUninitialized,
  dropReplicaTables,
  healNullReplicaRank,
  repairPartialReplicaInit,
  resetReplicaIfChangeLogPoisoned,
  resetReplicaIfTableSetChanged,
} from './zero-cache-replica-repair.js'

function result(rows: Array<Record<string, unknown>>): ReplicaSqlResult {
  return { toArray: () => rows }
}

// a replica SQLite stand-in: a configurable set of table names + a versionHistory
// flag, answering the exact queries the repair functions issue and recording DROPs.
class FakeReplicaSql implements ReplicaSqlStorage {
  dropped: string[] = []
  versionHistoryThrows = false
  hasVersionHistoryRow = false

  constructor(public tables: string[] = []) {}

  exec(sql: string): ReplicaSqlResult {
    if (sql === "SELECT name FROM sqlite_master WHERE type='table'") {
      return result(this.tables.map((name) => ({ name })))
    }
    if (sql.includes("name='_zero.replicationConfig'")) {
      return result(this.tables.includes('_zero.replicationConfig') ? [{ ok: 1 }] : [])
    }
    if (sql.startsWith('SELECT 1 FROM "_zero.versionHistory"')) {
      if (this.versionHistoryThrows)
        throw new Error('no such table: _zero.versionHistory')
      return result(this.hasVersionHistoryRow ? [{ ok: 1 }] : [])
    }
    if (sql.startsWith('DROP TABLE IF EXISTS')) {
      const name = sql
        .slice(sql.indexOf('"') + 1, sql.lastIndexOf('"'))
        .replaceAll('""', '"')
      this.dropped.push(name)
      this.tables = this.tables.filter((t) => t !== name)
    }
    return result([])
  }
}

class FakeKv implements ReplicaKvStorage {
  map = new Map<string, unknown>()
  async get(key: string) {
    return this.map.get(key)
  }
  async put(key: string, value: unknown) {
    this.map.set(key, value)
    return undefined
  }
}

describe('dropReplicaTables', () => {
  it('drops every non-internal table, skipping sqlite_ and _cf_', () => {
    const sql = new FakeReplicaSql([
      'sqlite_sequence',
      '_cf_KV',
      '_cf_METADATA',
      '_zero.replicationConfig',
      'user',
      'project',
    ])
    expect(dropReplicaTables(sql)).toBe(3)
    expect(sql.dropped).toEqual(['_zero.replicationConfig', 'user', 'project'])
  })

  it('quotes embedded double-quotes in table names', () => {
    const sql = new FakeReplicaSql(['we"ird'])
    dropReplicaTables(sql)
    expect(sql.dropped).toEqual(['we"ird'])
  })
})

describe('resetReplicaIfTableSetChanged', () => {
  const base = { schemaVersion: 'v1', tables: ['user', 'project'], tagKey: '__tag' }

  it('wipes + tags when there is no baseline tag', async () => {
    const sql = new FakeReplicaSql(['user', 'project'])
    const kv = new FakeKv()
    await resetReplicaIfTableSetChanged(sql, kv, base)
    expect(sql.dropped.length).toBe(2)
    expect(await kv.get('__tag')).toBe(JSON.stringify(['v1', ['project', 'user']]))
  })

  it('does not wipe when the tag is unchanged', async () => {
    const sql = new FakeReplicaSql(['user', 'project'])
    const kv = new FakeKv()
    await kv.put('__tag', JSON.stringify(['v1', ['project', 'user']]))
    await resetReplicaIfTableSetChanged(sql, kv, base)
    expect(sql.dropped).toEqual([])
  })

  it('wipes when the schema version changes (column-only edit, same tables)', async () => {
    const sql = new FakeReplicaSql(['user', 'project'])
    const kv = new FakeKv()
    await kv.put('__tag', JSON.stringify(['v1', ['project', 'user']]))
    await resetReplicaIfTableSetChanged(sql, kv, { ...base, schemaVersion: 'v2' })
    expect(sql.dropped.length).toBe(2)
    expect(await kv.get('__tag')).toBe(JSON.stringify(['v2', ['project', 'user']]))
  })
})

describe('repairPartialReplicaInit', () => {
  it('is a no-op when the replica was never initialized', () => {
    const sql = new FakeReplicaSql(['user'])
    repairPartialReplicaInit(sql)
    expect(sql.dropped).toEqual([])
  })

  it('is a no-op when versionHistory has a row (clean init)', () => {
    const sql = new FakeReplicaSql(['_zero.replicationConfig', 'user'])
    sql.hasVersionHistoryRow = true
    repairPartialReplicaInit(sql)
    expect(sql.dropped).toEqual([])
  })

  it('wipes a half-initialized replica (config present, no versionHistory row)', () => {
    const sql = new FakeReplicaSql(['_zero.replicationConfig', 'user', 'project'])
    sql.hasVersionHistoryRow = false
    repairPartialReplicaInit(sql)
    expect(sql.dropped).toEqual(['_zero.replicationConfig', 'user', 'project'])
  })

  it('wipes when the versionHistory table is missing entirely (throws)', () => {
    const sql = new FakeReplicaSql(['_zero.replicationConfig', 'user'])
    sql.versionHistoryThrows = true
    repairPartialReplicaInit(sql)
    expect(sql.dropped).toEqual(['_zero.replicationConfig', 'user'])
  })
})

describe('resetReplicaIfChangeLogPoisoned', () => {
  const opts = { appId: 'zero' }

  it('is a no-op when the replica is not initialized', async () => {
    const sql = new FakeReplicaSql(['user'])
    const backend = vi.fn()
    await resetReplicaIfChangeLogPoisoned(sql, backend as unknown as BackendExec, opts)
    expect(backend).not.toHaveBeenCalled()
    expect(sql.dropped).toEqual([])
  })

  it('surfaces a backend list error without wiping', async () => {
    const sql = new FakeReplicaSql(['_zero.replicationConfig', 'user'])
    const backend: BackendExec = async () => ({ error: 'boom' })
    await resetReplicaIfChangeLogPoisoned(sql, backend, opts)
    expect(sql.dropped).toEqual([])
  })

  it('does not wipe when the changeLog scan is clean', async () => {
    const sql = new FakeReplicaSql(['_zero.replicationConfig', 'user'])
    const backend: BackendExec = async (q) => {
      if (q.includes('name LIKE $1')) return { rows: [{ name: 'zero_0/cdc_changeLog' }] }
      return { rows: [] } // scan: no uncommitted group
    }
    await resetReplicaIfChangeLogPoisoned(sql, backend, opts)
    expect(sql.dropped).toEqual([])
  })

  it('wipes the replica when a poisoned (uncommitted) tx group is found', async () => {
    const sql = new FakeReplicaSql(['_zero.replicationConfig', 'user', 'project'])
    const backend: BackendExec = async (q) => {
      if (q.includes('name LIKE $1')) return { rows: [{ name: 'zero_0/cdc_changeLog' }] }
      return { rows: [{ watermark: 42 }] } // scan: a group with no commit
    }
    await resetReplicaIfChangeLogPoisoned(sql, backend, opts)
    expect(sql.dropped).toEqual(['_zero.replicationConfig', 'user', 'project'])
  })
})

describe('clearChangeStreamerStateIfReplicaUninitialized', () => {
  const opts = { appId: 'zero' }

  it('is a no-op when the replica IS initialized', async () => {
    const sql = new FakeReplicaSql(['_zero.replicationConfig'])
    const backend = vi.fn()
    await clearChangeStreamerStateIfReplicaUninitialized(
      sql,
      backend as unknown as BackendExec,
      opts
    )
    expect(backend).not.toHaveBeenCalled()
  })

  it('drops the surviving cdc state tables in the backend when uninitialized', async () => {
    const sql = new FakeReplicaSql(['user']) // no _zero.replicationConfig
    const calls: string[] = []
    const backend: BackendExec = async (q) => {
      calls.push(q)
      if (q.includes('name LIKE $1')) {
        return {
          rows: [
            { name: 'zero_0/cdc_changeLog' },
            { name: 'zero_0/cdc_replicationState' },
          ],
        }
      }
      return {}
    }
    await clearChangeStreamerStateIfReplicaUninitialized(sql, backend, opts)
    const drops = calls.filter((c) => c.startsWith('DROP TABLE'))
    expect(drops).toEqual([
      'DROP TABLE IF EXISTS "zero_0/cdc_changeLog"',
      'DROP TABLE IF EXISTS "zero_0/cdc_replicationState"',
    ])
  })
})

describe('healNullReplicaRank', () => {
  function backendWithReplicas(rows: Array<{ id: string; rank: number | null }>) {
    const updates: Array<{ rank: unknown; id: unknown }> = []
    const exec: BackendExec = async (sql, params) => {
      if (sql.includes('sqlite_master')) {
        return { rows: [{ name: 'soot_0_replicas' }] }
      }
      if (sql.startsWith('SELECT id FROM')) {
        return { rows: rows.filter((r) => r.rank === null).map((r) => ({ id: r.id })) }
      }
      if (sql.startsWith('UPDATE')) {
        updates.push({ rank: params?.[0], id: params?.[1] })
        const row = rows.find((r) => r.id === params?.[1])
        if (row) row.rank = Number(params?.[0])
        return { rows: [] }
      }
      return { rows: [] }
    }
    return { exec, updates, rows }
  }

  it('backfills distinct Date.now()-based ranks onto NULL-rank rows only', async () => {
    const backend = backendWithReplicas([
      { id: 'a', rank: null },
      { id: 'b', rank: 42 },
      { id: 'c', rank: null },
    ])
    await healNullReplicaRank(backend.exec, { appId: 'soot', nowMs: 1_000_000 })
    expect(backend.updates).toEqual([
      { rank: 1_000_000, id: 'a' },
      { rank: 1_000_001, id: 'c' },
    ])
    expect(backend.rows.find((r) => r.id === 'b')?.rank).toBe(42)
  })

  it('is a no-op when every rank is set', async () => {
    const backend = backendWithReplicas([{ id: 'a', rank: 7 }])
    await healNullReplicaRank(backend.exec, { appId: 'soot' })
    expect(backend.updates).toEqual([])
  })

  it('surfaces list errors without updating', async () => {
    const exec: BackendExec = async () => ({ error: 'boom' })
    await expect(
      healNullReplicaRank(exec, { appId: 'soot' })
    ).resolves.toBeUndefined()
  })
})
