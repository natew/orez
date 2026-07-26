// Real-SQLite integration for replica-repair functions through a minimal
// Durable Object SqlStorage adapter.
import { DatabaseSync } from 'node:sqlite'

import { describe, expect, it } from 'vitest'

import {
  dropReplicaTables,
  repairPartialReplicaInit,
  resetReplicaIfTableSetChanged,
} from './zero-cache-replica-repair.js'

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
