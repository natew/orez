import { PGlite } from '@electric-sql/pglite'
import { describe, it, expect, beforeEach, afterEach } from 'vitest'

import {
  installChangeTracking,
  installTriggersOnShardTables,
  purgeConsumedChanges,
  getChangesSince,
  getCurrentWatermark,
} from './change-tracker'

describe('change-tracker', () => {
  let db: PGlite

  beforeEach(async () => {
    db = new PGlite()
    await db.waitReady
    await db.exec(`
      CREATE TABLE public.items (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        value INTEGER
      )
    `)
    await installChangeTracking(db)
  })

  afterEach(async () => {
    await db.close()
  })

  it('captures INSERT', async () => {
    await db.exec(`INSERT INTO public.items (name, value) VALUES ('a', 1)`)

    const changes = await getChangesSince(db, 0)
    expect(changes).toHaveLength(1)
    expect(changes[0].op).toBe('INSERT')
    expect(changes[0].table_name).toBe('public.items')
    expect(changes[0].row_data).toMatchObject({ name: 'a', value: 1 })
    expect(changes[0].old_data).toBeNull()
  })

  it('captures UPDATE with old + new data', async () => {
    await db.exec(`INSERT INTO public.items (name, value) VALUES ('a', 1)`)
    await db.exec(`UPDATE public.items SET value = 99 WHERE name = 'a'`)

    const changes = await getChangesSince(db, 0)
    expect(changes).toHaveLength(2)
    expect(changes[1].op).toBe('UPDATE')
    expect(changes[1].row_data).toMatchObject({ value: 99 })
    expect(changes[1].old_data).toMatchObject({ value: 1 })
  })

  it('captures DELETE with old data', async () => {
    await db.exec(`INSERT INTO public.items (name, value) VALUES ('a', 1)`)
    await db.exec(`DELETE FROM public.items WHERE name = 'a'`)

    const changes = await getChangesSince(db, 0)
    expect(changes).toHaveLength(2)
    expect(changes[1].op).toBe('DELETE')
    expect(changes[1].old_data).toMatchObject({ name: 'a', value: 1 })
    expect(changes[1].row_data).toBeNull()
  })

  it('watermarks increase monotonically', async () => {
    for (let i = 0; i < 5; i++) {
      await db.exec(`INSERT INTO public.items (name, value) VALUES ('item${i}', ${i})`)
    }

    const changes = await getChangesSince(db, 0)
    expect(changes).toHaveLength(5)
    for (let i = 1; i < changes.length; i++) {
      expect(changes[i].watermark).toBeGreaterThan(changes[i - 1].watermark)
    }
  })

  it('getChangesSince filters by watermark', async () => {
    await db.exec(`INSERT INTO public.items (name, value) VALUES ('a', 1)`)
    await db.exec(`INSERT INTO public.items (name, value) VALUES ('b', 2)`)

    const all = await getChangesSince(db, 0)
    const afterFirst = await getChangesSince(db, all[0].watermark)

    expect(afterFirst).toHaveLength(1)
    expect(afterFirst[0].row_data).toMatchObject({ name: 'b' })
  })

  it('respects limit', async () => {
    for (let i = 0; i < 10; i++) {
      await db.exec(`INSERT INTO public.items (name, value) VALUES ('x', ${i})`)
    }

    const limited = await getChangesSince(db, 0, 3)
    expect(limited).toHaveLength(3)
  })

  it('getCurrentWatermark returns 0 before any inserts', async () => {
    const wm = await getCurrentWatermark(db)
    expect(wm).toBe(0)
  })

  it('getCurrentWatermark advances', async () => {
    // first insert consumes the initial sequence value
    await db.exec(`INSERT INTO public.items (name, value) VALUES ('x', 1)`)
    const before = await getCurrentWatermark(db)
    expect(before).toBeGreaterThan(0)
    await db.exec(`INSERT INTO public.items (name, value) VALUES ('y', 2)`)
    const after = await getCurrentWatermark(db)
    expect(after).toBeGreaterThan(before)
  })

  it('tracks multiple tables', async () => {
    await db.exec(`CREATE TABLE public.other (id SERIAL PRIMARY KEY, label TEXT)`)
    await installChangeTracking(db) // reinstall picks up new table

    await db.exec(`INSERT INTO public.items (name, value) VALUES ('a', 1)`)
    await db.exec(`INSERT INTO public.other (label) VALUES ('b')`)

    const changes = await getChangesSince(db, 0)
    const tables = new Set(changes.map((c) => c.table_name))
    expect(tables).toContain('public.items')
    expect(tables).toContain('public.other')
  })

  it('handles rapid inserts (50 rows)', async () => {
    for (let i = 0; i < 50; i++) {
      await db.exec(`INSERT INTO public.items (name, value) VALUES ('r${i}', ${i})`)
    }

    const changes = await getChangesSince(db, 0)
    expect(changes).toHaveLength(50)

    for (let i = 1; i < changes.length; i++) {
      expect(changes[i].watermark).toBeGreaterThan(changes[i - 1].watermark)
    }
  })

  it('does not track internal _zero_ tables', async () => {
    await db.exec(`INSERT INTO public.items (name, value) VALUES ('x', 1)`)

    const changes = await getChangesSince(db, 0)
    // only the items insert, not the _zero_changes insert that the trigger itself caused
    const internalChanges = changes.filter((c) => c.table_name.startsWith('_zero_'))
    expect(internalChanges).toHaveLength(0)
  })

  it('handles NULL column values', async () => {
    await db.exec(`INSERT INTO public.items (name, value) VALUES ('nulltest', NULL)`)

    const changes = await getChangesSince(db, 0)
    expect(changes[0].row_data).toMatchObject({ name: 'nulltest', value: null })
  })

  it('handles multi-row update', async () => {
    await db.exec(
      `INSERT INTO public.items (name, value) VALUES ('a', 1), ('b', 2), ('c', 3)`
    )
    await db.exec(`UPDATE public.items SET value = value * 10`)

    const changes = await getChangesSince(db, 0)
    const updates = changes.filter((c) => c.op === 'UPDATE')
    expect(updates).toHaveLength(3)
  })

  it('preserves change ordering across mixed operations', async () => {
    await db.exec(`INSERT INTO public.items (name, value) VALUES ('a', 1)`)
    await db.exec(`UPDATE public.items SET value = 2 WHERE name = 'a'`)
    await db.exec(`INSERT INTO public.items (name, value) VALUES ('b', 3)`)
    await db.exec(`DELETE FROM public.items WHERE name = 'a'`)

    const changes = await getChangesSince(db, 0)
    const ops = changes.map((c) => c.op)
    expect(ops).toEqual(['INSERT', 'UPDATE', 'INSERT', 'DELETE'])
  })

  it('tracks tables with special characters in names', async () => {
    await db.exec(`CREATE TABLE public."my""table" (id SERIAL PRIMARY KEY, val TEXT)`)
    await installChangeTracking(db)

    await db.exec(`INSERT INTO public."my""table" (val) VALUES ('works')`)

    const changes = await getChangesSince(db, 0)
    const special = changes.filter((c) => c.table_name === 'public.my"table')
    expect(special).toHaveLength(1)
    expect(special[0].op).toBe('INSERT')
    expect(special[0].row_data).toMatchObject({ val: 'works' })
  })
})

describe('shard table tracking', () => {
  let db: PGlite

  beforeEach(async () => {
    db = new PGlite()
    await db.waitReady
    await installChangeTracking(db)
  })

  afterEach(async () => {
    await db.close()
  })

  it('only tracks clients table in shard schemas, not replicas/mutations', async () => {
    // zero-cache creates shard schemas like chat_0 with clients, replicas, mutations.
    // only clients needs tracking — replicas/mutations changes crash zero-cache
    // with "Unknown table chat_0.replicas" because they aren't in zero's schema.
    await db.exec(`
      CREATE SCHEMA chat_0;
      CREATE TABLE chat_0.clients (
        "clientGroupID" TEXT NOT NULL,
        "clientID" TEXT NOT NULL,
        "lastMutationID" BIGINT,
        "userID" TEXT,
        PRIMARY KEY ("clientGroupID", "clientID")
      );
      CREATE TABLE chat_0.replicas (
        id TEXT PRIMARY KEY,
        version TEXT,
        cookie TEXT
      );
      CREATE TABLE chat_0.mutations (
        id TEXT PRIMARY KEY,
        "clientID" TEXT,
        name TEXT,
        args JSONB
      );
    `)

    await installTriggersOnShardTables(db)

    // insert into all three tables
    await db.exec(
      `INSERT INTO chat_0.clients ("clientGroupID", "clientID", "lastMutationID") VALUES ('cg1', 'c1', 1)`
    )
    await db.exec(`INSERT INTO chat_0.replicas (id, version) VALUES ('r1', 'v1')`)
    await db.exec(
      `INSERT INTO chat_0.mutations (id, "clientID", name) VALUES ('m1', 'c1', 'sendMessage')`
    )

    const changes = await getChangesSince(db, 0)
    const tables = changes.map((c) => c.table_name)

    // only clients should be tracked
    expect(tables).toContain('chat_0.clients')
    expect(tables).not.toContain('chat_0.replicas')
    expect(tables).not.toContain('chat_0.mutations')
  })

  it('purges consumed changes to prevent OOM', async () => {
    // _zero_changes accumulates forever in 0.0.37. with wasm pglite,
    // this eventually causes OOM. we need a purge mechanism.
    await db.exec(`
      CREATE TABLE public.items (id SERIAL PRIMARY KEY, val TEXT)
    `)
    await installChangeTracking(db)

    // insert some data
    for (let i = 0; i < 10; i++) {
      await db.exec(`INSERT INTO public.items (val) VALUES ('item${i}')`)
    }

    const changes = await getChangesSince(db, 0)
    expect(changes).toHaveLength(10)
    const lastWatermark = changes[changes.length - 1].watermark

    // purge consumed changes up to the watermark we've processed
    await purgeConsumedChanges(db, lastWatermark)

    // after purge, no changes before that watermark should remain
    const remaining = await getChangesSince(db, 0)
    expect(remaining).toHaveLength(0)
  })

  it('tracks tables created after initial installChangeTracking', async () => {
    // simulate zero-cache creating shard schema AFTER replication starts.
    // in production, zero-cache creates chat_0 schema + clients table
    // after the replication connection is already established.
    // the change tracker must pick up these new tables.
    await db.exec(`
      CREATE SCHEMA chat_0;
      CREATE TABLE chat_0.clients (
        "clientGroupID" TEXT NOT NULL,
        "clientID" TEXT NOT NULL,
        "lastMutationID" BIGINT,
        PRIMARY KEY ("clientGroupID", "clientID")
      );
    `)

    // re-running installTriggersOnShardTables should pick up new tables
    await installTriggersOnShardTables(db)

    await db.exec(
      `INSERT INTO chat_0.clients ("clientGroupID", "clientID", "lastMutationID") VALUES ('cg1', 'c1', 1)`
    )

    const changes = await getChangesSince(db, 0)
    expect(changes).toHaveLength(1)
    expect(changes[0].table_name).toBe('chat_0.clients')
  })
})
