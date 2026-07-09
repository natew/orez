// delta correctness suite for the sync-server core (rewrite phase 2
// acceptance, plans/zero-server-rewrite.md): cursor-diff pulls, retention
// floor snapshot fallback, LMID watermark markers, interleaved churn, two
// tabs one client group. wire-level against handlePull/handlePush directly —
// the zero-client integration lives in the harness lanes
// (harness/src/{smoke,shapes,bench}.ts).
import { DatabaseSync } from 'node:sqlite'
import { beforeEach, describe, expect, test } from 'vitest'
import {
  MutationAppError,
  type SyncDb,
  SyncHttpError,
  type SyncTables,
  createSyncServer,
} from './sync-server'

function nodeSqliteDb(sqlite: DatabaseSync): SyncDb {
  return {
    exec(sql, params = []) {
      sqlite.prepare(sql).run(...(params as never[]))
    },
    all(sql, params = []) {
      return sqlite.prepare(sql).all(...(params as never[])) as Record<string, unknown>[]
    },
    transaction<T>(fn: () => T): T {
      sqlite.exec('BEGIN')
      try {
        const result = fn()
        sqlite.exec('COMMIT')
        return result
      } catch (error) {
        sqlite.exec('ROLLBACK')
        throw error
      }
    },
  }
}

const TABLES: SyncTables = {
  item: {
    columns: { id: 'string', label: 'string', rank: 'number', done: 'boolean', meta: 'json' },
    primaryKey: ['id'],
  },
}

// mutators: item.put upserts, item.del deletes, item.reject always app-errors
function mutate(tx: SyncDb, name: string, args: unknown, _ctx: { userID: string }) {
  const a = args as Record<string, unknown>
  if (name === 'item.put') {
    tx.exec(
      `INSERT INTO item (id, label, rank, done, meta) VALUES (?, ?, ?, ?, ?)
       ON CONFLICT (id) DO UPDATE SET label = excluded.label, rank = excluded.rank,
       done = excluded.done, meta = excluded.meta`,
      [a.id, a.label, a.rank, a.done ? 1 : 0, a.meta === null ? null : JSON.stringify(a.meta)]
    )
    return
  }
  if (name === 'item.del') {
    tx.exec(`DELETE FROM item WHERE id = ?`, [a.id])
    return
  }
  if (name === 'item.reject') {
    // writes first so the rollback path is exercised, then rejects
    tx.exec(`INSERT INTO item (id, label, rank, done) VALUES ('rejected', 'x', 0, 0)`)
    throw new MutationAppError('nope')
  }
  throw new Error(`unknown mutator ${name}`)
}

function setup(config?: { retainChanges?: number; visible?: boolean }) {
  const sqlite = new DatabaseSync(':memory:')
  const db = nodeSqliteDb(sqlite)
  db.exec(`CREATE TABLE item (id TEXT PRIMARY KEY, label TEXT NOT NULL,
    rank REAL NOT NULL, done INTEGER NOT NULL, meta TEXT)`)
  // seed before createSyncServer: seed rows stay out of the change log
  db.exec(`INSERT INTO item (id, label, rank, done, meta)
           VALUES ('seed1', 'first', 1.5, 0, '{"tag":"a"}')`)
  const sync = createSyncServer({
    db,
    tables: TABLES,
    mutate,
    retainChanges: config?.retainChanges,
    visible: config?.visible
      ? (table) => ({ sql: `SELECT * FROM "${table}" WHERE done = 0`, params: [] })
      : undefined,
  })
  return { db, sync }
}

let nextMutationID: Record<string, number>
beforeEach(() => {
  nextMutationID = {}
})

function push(
  sync: ReturnType<typeof createSyncServer>,
  name: string,
  args: unknown,
  opts?: { clientID?: string; group?: string; userID?: string; id?: number }
) {
  const clientID = opts?.clientID ?? 'c1'
  const id = opts?.id ?? (nextMutationID[clientID] = (nextMutationID[clientID] ?? 0) + 1)
  return sync.handlePush(
    {
      clientGroupID: opts?.group ?? 'g1',
      mutations: [{ type: 'custom', id, clientID, name, args: [args], timestamp: 0 }],
      pushVersion: 1,
      requestID: 'r',
    },
    opts?.userID ?? 'u1'
  )
}

function pull(
  sync: ReturnType<typeof createSyncServer>,
  cookie: number | null,
  opts?: { clientID?: string; group?: string; userID?: string }
) {
  return sync.handlePull(
    {
      clientID: opts?.clientID ?? 'c1',
      clientGroupID: opts?.group ?? 'g1',
      cookie,
    },
    opts?.userID ?? 'u1'
  )
}

type Patch = { op: string; tableName?: string; value?: Record<string, unknown>; id?: Record<string, unknown> }

function patchOf(response: unknown): Patch[] {
  const rowsPatch = (response as { rowsPatch?: Patch[] }).rowsPatch
  expect(rowsPatch).toBeDefined()
  return rowsPatch!
}

describe('snapshot and unchanged', () => {
  test('fresh pull is a clear+puts snapshot with typed values', () => {
    const { sync } = setup()
    const response = pull(sync, null)
    const patch = patchOf(response)
    expect(patch[0]).toEqual({ op: 'clear' })
    expect(patch[1]).toEqual({
      op: 'put',
      tableName: 'item',
      value: { id: 'seed1', label: 'first', rank: 1.5, done: false, meta: { tag: 'a' } },
    })
    expect((response as { cookie: number }).cookie).toBe(0)
  })

  test('same-cookie pull is unchanged', () => {
    const { sync } = setup()
    const { cookie } = pull(sync, null) as { cookie: number }
    expect(pull(sync, cookie)).toEqual({ cookie, unchanged: true })
  })

  test('future cookie is a 409', () => {
    const { sync } = setup()
    expect(() => pull(sync, 99)).toThrowError(SyncHttpError)
    try {
      pull(sync, 99)
    } catch (error) {
      expect((error as SyncHttpError).status).toBe(409)
    }
  })
})

describe('cursor diffs', () => {
  test('insert arrives as a put diff without clear, floats exact', () => {
    const { sync } = setup()
    const { cookie } = pull(sync, null) as { cookie: number }
    const rank = 0.1 + 0.2 // 0.30000000000000004 — 17 significant digits
    push(sync, 'item.put', { id: 'i2', label: 'two', rank, done: true, meta: [1, 'x'] })
    const response = pull(sync, cookie)
    const patch = patchOf(response)
    expect(patch.some((op) => op.op === 'clear')).toBe(false)
    const put = patch.find((op) => op.op === 'put')!
    expect(put.value).toEqual({ id: 'i2', label: 'two', rank, done: true, meta: [1, 'x'] })
    expect(put.value!.rank).toBe(rank) // exact, not sqlite json's 15-digit form
  })

  test('update arrives as a put of only the touched row', () => {
    const { sync } = setup()
    push(sync, 'item.put', { id: 'i2', label: 'two', rank: 2, done: false, meta: null })
    const { cookie } = pull(sync, null) as { cookie: number }
    push(sync, 'item.put', { id: 'i2', label: 'renamed', rank: 2, done: false, meta: null })
    const patch = patchOf(pull(sync, cookie))
    const puts = patch.filter((op) => op.op === 'put')
    expect(puts).toHaveLength(1)
    expect(puts[0]!.value).toMatchObject({ id: 'i2', label: 'renamed' })
  })

  test('delete arrives as a del with the primary key', () => {
    const { sync } = setup()
    push(sync, 'item.put', { id: 'i2', label: 'two', rank: 2, done: false, meta: null })
    const { cookie } = pull(sync, null) as { cookie: number }
    push(sync, 'item.del', { id: 'i2' })
    const patch = patchOf(pull(sync, cookie))
    expect(patch).toEqual([{ op: 'del', tableName: 'item', id: { id: 'i2' } }])
  })

  test('delete then recreate between pulls collapses to a put', () => {
    const { sync } = setup()
    const { cookie } = pull(sync, null) as { cookie: number }
    push(sync, 'item.del', { id: 'seed1' })
    push(sync, 'item.put', { id: 'seed1', label: 'reborn', rank: 9, done: false, meta: null })
    const patch = patchOf(pull(sync, cookie))
    expect(patch).toEqual([
      {
        op: 'put',
        tableName: 'item',
        value: { id: 'seed1', label: 'reborn', rank: 9, done: false, meta: null },
      },
    ])
  })

  test('insert then delete between pulls collapses to a del', () => {
    const { sync } = setup()
    const { cookie } = pull(sync, null) as { cookie: number }
    push(sync, 'item.put', { id: 'ephemeral', label: 'x', rank: 0, done: false, meta: null })
    push(sync, 'item.del', { id: 'ephemeral' })
    const patch = patchOf(pull(sync, cookie))
    expect(patch).toEqual([{ op: 'del', tableName: 'item', id: { id: 'ephemeral' } }])
  })

  test('upstream sql outside push advances the watermark via triggers', () => {
    const { db, sync } = setup()
    const { cookie } = pull(sync, null) as { cookie: number }
    db.exec(`UPDATE item SET label = 'edited behind zero' WHERE id = 'seed1'`)
    const response = pull(sync, cookie)
    expect((response as { cookie: number }).cookie).toBeGreaterThan(cookie)
    const patch = patchOf(response)
    expect(patch).toEqual([
      {
        op: 'put',
        tableName: 'item',
        value: { id: 'seed1', label: 'edited behind zero', rank: 1.5, done: false, meta: { tag: 'a' } },
      },
    ])
  })

  test('pk-changing update dels the old pk and puts the new one', () => {
    const { db, sync } = setup()
    const { cookie } = pull(sync, null) as { cookie: number }
    db.exec(`UPDATE item SET id = 'seed1-renamed' WHERE id = 'seed1'`)
    const patch = patchOf(pull(sync, cookie))
    expect(patch).toContainEqual({ op: 'del', tableName: 'item', id: { id: 'seed1' } })
    expect(patch).toContainEqual({
      op: 'put',
      tableName: 'item',
      value: { id: 'seed1-renamed', label: 'first', rank: 1.5, done: false, meta: { tag: 'a' } },
    })
  })
})

describe('push semantics', () => {
  test('app error advances the LMID and the watermark but changes no rows', () => {
    const { db, sync } = setup()
    const { cookie } = pull(sync, null) as { cookie: number }
    const response = push(sync, 'item.reject', {})
    expect(response.pushResponse.mutations[0]!.result).toEqual({ error: 'app', details: 'nope' })
    expect(db.all(`SELECT * FROM item WHERE id = 'rejected'`)).toHaveLength(0)
    // the lmid marker makes the next pull non-unchanged so recovery settles
    const next = pull(sync, cookie) as {
      cookie: number
      lastMutationIDChanges: Record<string, number>
    }
    expect(next.cookie).toBeGreaterThan(cookie)
    expect(next.lastMutationIDChanges.c1).toBe(1)
    expect(patchOf(next)).toEqual([])
  })

  test('replayed mutation acks idempotently without re-executing', () => {
    const { db, sync } = setup()
    push(sync, 'item.put', { id: 'i2', label: 'once', rank: 1, done: false, meta: null }, { id: 1 })
    push(sync, 'item.put', { id: 'i2', label: 'twice?', rank: 1, done: false, meta: null }, { id: 1 })
    expect(db.all(`SELECT label FROM item WHERE id = 'i2'`)).toEqual([{ label: 'once' }])
  })

  test('out-of-order mutation id is a 400', () => {
    const { sync } = setup()
    expect(() => push(sync, 'item.put', { id: 'x' }, { id: 5 })).toThrowError(/skips lmid/)
  })

  test('two tabs in one client group settle through lastMutationIDChanges', () => {
    const { sync } = setup()
    push(sync, 'item.put', { id: 'a', label: 'a', rank: 0, done: false, meta: null }, { clientID: 'tab1' })
    push(sync, 'item.put', { id: 'b', label: 'b', rank: 0, done: false, meta: null }, { clientID: 'tab2' })
    const response = pull(sync, null, { clientID: 'tab1' }) as {
      lastMutationIDChanges: Record<string, number>
    }
    expect(response.lastMutationIDChanges).toEqual({ tab1: 1, tab2: 1 })
  })

  test('a client group claimed by one user rejects another', () => {
    const { sync } = setup()
    pull(sync, null, { userID: 'u1' })
    expect(() => pull(sync, null, { userID: 'intruder' })).toThrowError(/different user/)
  })
})

describe('retention floor', () => {
  test('cookie below the pruned floor falls back to snapshot; recent cookies still diff', () => {
    const { sync } = setup({ retainChanges: 2 })
    const ancient = (pull(sync, null) as { cookie: number }).cookie
    for (let i = 0; i < 6; i++) {
      push(sync, 'item.put', { id: `i${i}`, label: `l${i}`, rank: i, done: false, meta: null })
    }
    const recent = (pull(sync, null, { clientID: 'c2' }) as { cookie: number }).cookie
    push(sync, 'item.put', { id: 'last', label: 'last', rank: 99, done: false, meta: null })

    const stale = patchOf(pull(sync, ancient))
    expect(stale[0]).toEqual({ op: 'clear' }) // snapshot fallback
    expect(stale.filter((op) => op.op === 'put').length).toBeGreaterThanOrEqual(8)

    const fresh = patchOf(pull(sync, recent, { clientID: 'c2' }))
    expect(fresh.some((op) => op.op === 'clear')).toBe(false) // still a diff
    expect(fresh).toEqual([
      {
        op: 'put',
        tableName: 'item',
        value: { id: 'last', label: 'last', rank: 99, done: false, meta: null },
      },
    ])
  })
})

describe('per-user visibility', () => {
  test('visible() configs always snapshot, filtered per user', () => {
    const { sync } = setup({ visible: true })
    push(sync, 'item.put', { id: 'hidden', label: 'done item', rank: 0, done: true, meta: null })
    const { cookie } = pull(sync, null) as { cookie: number }
    push(sync, 'item.put', { id: 'shown', label: 'open item', rank: 0, done: false, meta: null })
    const patch = patchOf(pull(sync, cookie))
    expect(patch[0]).toEqual({ op: 'clear' }) // never a diff with visibility filtering
    const ids = patch.filter((op) => op.op === 'put').map((op) => op.value!.id)
    expect(ids).toContain('shown')
    expect(ids).not.toContain('hidden')
  })
})

describe('interleaved churn converges', () => {
  test('two pulling clients with interleaved pushes and upstream sql end equal', () => {
    const { db, sync } = setup()
    // client states: apply patches like the zero client store would
    const stores: Record<string, Map<string, Record<string, unknown>>> = {}
    const cookies: Record<string, number | null> = { c1: null, c2: null }
    const applyPull = (clientID: 'c1' | 'c2') => {
      const response = pull(sync, cookies[clientID]!, { clientID }) as {
        cookie: number
        unchanged?: true
        rowsPatch?: Patch[]
      }
      cookies[clientID] = response.cookie
      if (response.unchanged) return
      const store = (stores[clientID] ??= new Map())
      for (const op of response.rowsPatch!) {
        if (op.op === 'clear') store.clear()
        else if (op.op === 'put') store.set(String(op.value!.id), op.value!)
        else if (op.op === 'del') store.delete(String(op.id!.id))
      }
    }

    applyPull('c1')
    for (let round = 0; round < 20; round++) {
      push(sync, 'item.put', {
        id: `r${round % 7}`,
        label: `round ${round}`,
        rank: round + 0.1,
        done: round % 2 === 1,
        meta: round % 3 === 0 ? { round } : null,
      })
      if (round % 4 === 0) db.exec(`DELETE FROM item WHERE id = 'r${(round + 3) % 7}'`)
      if (round % 5 === 2) applyPull('c1')
      if (round % 3 === 1) applyPull('c2')
    }
    applyPull('c1')
    applyPull('c2')

    const oracle = new Map(
      (
        pull(sync, null, { clientID: 'c3' }) as { rowsPatch: Patch[] }
      ).rowsPatch
        .filter((op) => op.op === 'put')
        .map((op) => [String(op.value!.id), op.value!] as const)
    )
    for (const clientID of ['c1', 'c2'] as const) {
      expect(Object.fromEntries(stores[clientID]!)).toEqual(Object.fromEntries(oracle))
    }
  })
})
