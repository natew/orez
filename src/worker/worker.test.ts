import { PGlite } from '@electric-sql/pglite'
import { describe, it, expect, beforeEach, afterEach } from 'vitest'

import { createOrezWorker } from './index'

import type { OrezWorker } from './types'

describe('orez/worker', () => {
  let worker: OrezWorker

  beforeEach(async () => {
    worker = await createOrezWorker({
      pgliteOptions: { dataDir: 'memory://' },
    })
  })

  afterEach(async () => {
    await worker.close()
  })

  it('creates worker with pgliteOptions', () => {
    expect(worker.db).toBeDefined()
    expect(worker.ownsInstance).toBe(true)
  })

  it('creates worker with pre-existing PGlite', async () => {
    const pglite = new PGlite()
    await pglite.waitReady
    const w = await createOrezWorker({ pglite })
    expect(w.db).toBe(pglite)
    expect(w.ownsInstance).toBe(false)
    await w.close()
    // pglite should still be open since worker doesn't own it
    expect(pglite.closed).toBe(false)
    await pglite.close()
  })

  it('throws without pglite or pgliteOptions', async () => {
    await expect(createOrezWorker({})).rejects.toThrow(
      'provide either pglite or pgliteOptions'
    )
  })

  it('exec and query work', async () => {
    await worker.exec(`
      CREATE TABLE public.items (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
      )
    `)
    await worker.installChangeTracking()
    await worker.query('INSERT INTO public.items (name) VALUES ($1)', ['hello'])
    const result = await worker.query<{ id: number; name: string }>(
      'SELECT * FROM public.items'
    )
    expect(result.rows).toHaveLength(1)
    expect(result.rows[0].name).toBe('hello')
  })

  it('change tracking captures mutations', async () => {
    await worker.exec(`
      CREATE TABLE public.things (
        id TEXT PRIMARY KEY,
        val INTEGER
      )
    `)
    // reinstall after creating table so triggers are on the new table
    await worker.installChangeTracking()

    await worker.exec(`INSERT INTO public.things VALUES ('a', 1)`)
    await worker.exec(`UPDATE public.things SET val = 2 WHERE id = 'a'`)
    await worker.exec(`DELETE FROM public.things WHERE id = 'a'`)

    const changes = await worker.getChangesSince(0)
    expect(changes).toHaveLength(3)
    expect(changes[0].op).toBe('INSERT')
    expect(changes[0].table_name).toBe('public.things')
    expect(changes[0].row_data).toMatchObject({ id: 'a', val: 1 })
    expect(changes[1].op).toBe('UPDATE')
    expect(changes[1].row_data).toMatchObject({ id: 'a', val: 2 })
    expect(changes[1].old_data).toMatchObject({ id: 'a', val: 1 })
    expect(changes[2].op).toBe('DELETE')
    expect(changes[2].old_data).toMatchObject({ id: 'a', val: 2 })
  })

  it('watermark tracking works', async () => {
    await worker.exec(`
      CREATE TABLE public.wm_test (id TEXT PRIMARY KEY)
    `)
    await worker.installChangeTracking()

    const wm0 = await worker.getCurrentWatermark()
    expect(wm0).toBe(0)

    await worker.exec(`INSERT INTO public.wm_test VALUES ('x')`)
    const wm1 = await worker.getCurrentWatermark()
    expect(wm1).toBeGreaterThan(0)

    await worker.exec(`INSERT INTO public.wm_test VALUES ('y')`)
    const wm2 = await worker.getCurrentWatermark()
    expect(wm2).toBeGreaterThan(wm1)

    // getChangesSince with wm1 should only return the second insert
    const changes = await worker.getChangesSince(wm1)
    expect(changes).toHaveLength(1)
    expect(changes[0].row_data).toMatchObject({ id: 'y' })
  })

  it('purgeChanges removes old entries', async () => {
    await worker.exec(`CREATE TABLE public.purge_test (id TEXT PRIMARY KEY)`)
    await worker.installChangeTracking()

    await worker.exec(`INSERT INTO public.purge_test VALUES ('a')`)
    await worker.exec(`INSERT INTO public.purge_test VALUES ('b')`)
    await worker.exec(`INSERT INTO public.purge_test VALUES ('c')`)

    const allChanges = await worker.getChangesSince(0)
    expect(allChanges).toHaveLength(3)

    // purge up to second change
    const purged = await worker.purgeChanges(allChanges[1].watermark)
    expect(purged).toBe(2)

    // only third change remains
    const remaining = await worker.getChangesSince(0)
    expect(remaining).toHaveLength(1)
    expect(remaining[0].row_data).toMatchObject({ id: 'c' })
  })

  it('close shuts down owned instance', async () => {
    const w = await createOrezWorker({ pgliteOptions: { dataDir: 'memory://' } })
    expect(w.db.closed).toBe(false)
    await w.close()
    expect(w.db.closed).toBe(true)
  })
})
