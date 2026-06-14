import { PGlite } from '@electric-sql/pglite'
import { describe, it, expect, beforeEach, afterEach } from 'vitest'

import { usePublicationsEnv } from '../test-env'
import {
  clearStreamedBatches,
  confirmStreamedBatches,
  installChangeTracking,
  installTriggersOnShardTables,
  resetShardSchemaCache,
  purgeConsumedChanges,
  getChangesSince,
  getCurrentWatermark,
  getStreamResumeWatermark,
  recordStreamedBatch,
} from './change-tracker'

usePublicationsEnv(undefined)

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

  it('respects empty configured publication (tracks no public tables)', async () => {
    const prev = process.env.ZERO_APP_PUBLICATIONS
    process.env.ZERO_APP_PUBLICATIONS = 'zero_scope'
    try {
      await db.exec(`CREATE PUBLICATION "zero_scope"`)
      await installChangeTracking(db) // reinstall picks up publication scope
      await db.exec(`TRUNCATE _orez._zero_changes`)

      await db.exec(`INSERT INTO public.items (name, value) VALUES ('x', 1)`)
      const changes = await getChangesSince(db, 0)
      expect(changes).toHaveLength(0)
    } finally {
      if (prev === undefined) delete process.env.ZERO_APP_PUBLICATIONS
      else process.env.ZERO_APP_PUBLICATIONS = prev
    }
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

  it('stream-resume floor covers rows written while the streamer was down', async () => {
    // streamed + purged: rows 1-2 delivered by a previous streamer life
    await db.exec(`INSERT INTO public.items (name, value) VALUES ('a', 1)`)
    await db.exec(`INSERT INTO public.items (name, value) VALUES ('b', 2)`)
    const streamed = await getChangesSince(db, 0)
    await purgeConsumedChanges(db, streamed[streamed.length - 1].watermark)

    // written while no streamer was alive (DO teardown / page reload gap)
    await db.exec(`INSERT INTO public.items (name, value) VALUES ('c', 3)`)
    await db.exec(`INSERT INTO public.items (name, value) VALUES ('d', 4)`)

    const resume = await getStreamResumeWatermark(db)
    const pending = await getChangesSince(db, resume)
    expect(pending.map((c) => c.row_data?.name)).toEqual(['c', 'd'])
  })

  it('stream-resume floor equals current watermark when nothing is pending', async () => {
    await db.exec(`INSERT INTO public.items (name, value) VALUES ('a', 1)`)
    const streamed = await getChangesSince(db, 0)
    await purgeConsumedChanges(db, streamed[streamed.length - 1].watermark)

    expect(await getStreamResumeWatermark(db)).toBe(await getCurrentWatermark(db))
    expect(await getChangesSince(db, await getStreamResumeWatermark(db))).toHaveLength(0)
  })

  it('streamed rows are retained until the consumer confirms the batch', async () => {
    await db.exec(`INSERT INTO public.items (name, value) VALUES ('a', 1)`)
    await db.exec(`INSERT INTO public.items (name, value) VALUES ('b', 2)`)
    const changes = await getChangesSince(db, 0)
    const batchEnd = changes[changes.length - 1].watermark

    // streamed but not confirmed: a consumer death mid-store must re-stream
    await recordStreamedBatch(db, 1000n, batchEnd)
    expect(await getChangesSince(db, 0)).toHaveLength(2)
    expect(await getStreamResumeWatermark(db)).toBe(changes[0].watermark - 1)

    // a confirmation below the batch lsn purges nothing
    expect(await confirmStreamedBatches(db, 999n)).toBe(0)
    expect(await getChangesSince(db, 0)).toHaveLength(2)

    // confirming the batch purges exactly its rows + mapping
    expect(await confirmStreamedBatches(db, 1000n)).toBe(2)
    expect(await getChangesSince(db, 0)).toHaveLength(0)
    expect(await getStreamResumeWatermark(db)).toBe(await getCurrentWatermark(db))
  })

  it('confirmation purges every batch at or below the confirmed lsn', async () => {
    await db.exec(`INSERT INTO public.items (name, value) VALUES ('a', 1)`)
    const first = await getChangesSince(db, 0)
    await recordStreamedBatch(db, 1000n, first[first.length - 1].watermark)

    await db.exec(`INSERT INTO public.items (name, value) VALUES ('b', 2)`)
    await db.exec(`INSERT INTO public.items (name, value) VALUES ('c', 3)`)
    const rest = await getChangesSince(db, first[first.length - 1].watermark)
    await recordStreamedBatch(db, 2000n, rest[rest.length - 1].watermark)

    // an ack for the second batch (storer commits in order) covers the first
    expect(await confirmStreamedBatches(db, 2000n)).toBe(3)
    expect(await getChangesSince(db, 0)).toHaveLength(0)
  })

  it('reconnect: confirmed batches purge from the resume lsn, unconfirmed rows re-stream', async () => {
    // batch 1 streamed + committed by the consumer (it resumes past it)
    await db.exec(`INSERT INTO public.items (name, value) VALUES ('a', 1)`)
    const committed = await getChangesSince(db, 0)
    await recordStreamedBatch(db, 1000n, committed[committed.length - 1].watermark)

    // batch 2 streamed but the consumer died before storing it
    await db.exec(`INSERT INTO public.items (name, value) VALUES ('b', 2)`)
    const lost = await getChangesSince(db, committed[committed.length - 1].watermark)
    await recordStreamedBatch(db, 2000n, lost[lost.length - 1].watermark)

    // reconnect arrives with START_REPLICATION at lsn 1001 (= 1000 + 1):
    // the reconciliation confirms batch 1 and clears the stale mapping so
    // batch 2's rows re-stream under fresh lsns.
    await confirmStreamedBatches(db, 1001n - 1n)
    await clearStreamedBatches(db)

    const resume = await getStreamResumeWatermark(db)
    const pending = await getChangesSince(db, resume)
    expect(pending.map((c) => c.row_data?.name)).toEqual(['b'])
  })
})

describe('shard table tracking', () => {
  let db: PGlite

  beforeEach(async () => {
    resetShardSchemaCache()
    db = new PGlite()
    await db.waitReady
    await installChangeTracking(db)
  })

  afterEach(async () => {
    await db.close()
  })

  it('only tracks mutation-confirmation tables in shard schemas', async () => {
    // zero-cache creates shard schemas like chat_0 with clients, replicas, mutations.
    // clients advance lmid and mutations carry server results; replicas stays internal.
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

    // only mutation-confirmation tables should be tracked
    expect(tables).toContain('chat_0.clients')
    expect(tables).toContain('chat_0.mutations')
    expect(tables).not.toContain('chat_0.replicas')
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

  it('tracks shard tables created after the schema was scanned', async () => {
    await db.exec(`CREATE SCHEMA chat_0`)

    // first scan sees the schema but no internal tables yet.
    await installTriggersOnShardTables(db)

    await db.exec(`
      CREATE TABLE chat_0.clients (
        "clientGroupID" TEXT NOT NULL,
        "clientID" TEXT NOT NULL,
        "lastMutationID" BIGINT,
        PRIMARY KEY ("clientGroupID", "clientID")
      );
      CREATE TABLE chat_0.mutations (
        "clientGroupID" TEXT NOT NULL,
        "clientID" TEXT NOT NULL,
        "mutationID" BIGINT NOT NULL,
        result JSONB,
        PRIMARY KEY ("clientGroupID", "clientID", "mutationID")
      );
    `)

    await installTriggersOnShardTables(db)

    await db.exec(
      `INSERT INTO chat_0.clients ("clientGroupID", "clientID", "lastMutationID") VALUES ('cg1', 'c1', 1)`
    )
    await db.exec(
      `INSERT INTO chat_0.mutations ("clientGroupID", "clientID", "mutationID", result) VALUES ('cg1', 'c1', 1, '{}')`
    )

    const changes = await getChangesSince(db, 0)
    expect(changes.map((change) => change.table_name).sort()).toEqual([
      'chat_0.clients',
      'chat_0.mutations',
    ])
  })

  // regression: zero-cache's startup slot cleanup runs
  //   SELECT slot, pg_drop_replication_slot(slot) FROM <slots> JOIN replicas WHERE <stale>
  // inside ensureSchemaMigrated's transaction. PGlite's real pg_drop builtin
  // errors on our fake slots ("replication slot does not exist"), aborting the
  // migration and crashing the change-streamer (exit 255). proxy rewrites route
  // that call to _orez._drop_zero_slot — a side-effecting DELETE that drops only
  // the selected (stale) rows and never errors on a missing slot.
  it('drops only stale slots via _orez._drop_zero_slot without erroring', async () => {
    await db.exec(`CREATE SCHEMA IF NOT EXISTS chat_0`)
    await db.exec(`CREATE TABLE chat_0.replicas (id TEXT PRIMARY KEY, slot TEXT)`)
    await db.exec(`INSERT INTO _orez._zero_replication_slots (slot_name, active) VALUES
      ('chat_0_a', false),  -- stale: inactive, no replica  -> dropped
      ('chat_0_b', false),  -- inactive but owned by a replica -> kept
      ('chat_0_c', true)    -- active -> kept
    `)
    await db.exec(`INSERT INTO chat_0.replicas (id, slot) VALUES ('r1', 'chat_0_b')`)

    // exact shape zero-cache sends, post-rewrite (pg_drop_replication_slot -> _orez._drop_zero_slot)
    const dropped = await db.query<{ slot: string }>(
      `SELECT slot_name as slot, _orez._drop_zero_slot(slot_name)
         FROM _orez._zero_replication_slots
         LEFT JOIN chat_0.replicas replica on slot_name = replica.slot
        WHERE slot_name LIKE $1 AND NOT active AND replica.id IS NULL`,
      ['chat_0_%']
    )
    expect(dropped.rows.map((r) => r.slot)).toEqual(['chat_0_a'])

    const remaining = await db.query<{ slot_name: string }>(
      `SELECT slot_name FROM _orez._zero_replication_slots ORDER BY slot_name`
    )
    expect(remaining.rows.map((r) => r.slot_name)).toEqual(['chat_0_b', 'chat_0_c'])

    // dropping an already-gone slot is a no-op, never an error
    const again = await db.query<{ dropped: string }>(
      `SELECT _orez._drop_zero_slot('chat_0_a') as dropped`
    )
    expect(again.rows[0].dropped).toBe('chat_0_a')
  })
})
