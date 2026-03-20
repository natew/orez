/**
 * integration test for orez/worker.
 *
 * tests the full pipeline available without zero-cache:
 * PGlite → change tracking → replication encoding → InProcessWriter
 *
 * mirrors the existing integration test patterns but uses the worker
 * API instead of startZeroLite(). validates that the worker entry
 * point produces the same replication stream that zero-cache expects.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'

import { createOrezWorker } from './index'
import { InProcessWriter } from '../replication/handler.js'
import { resetReplicationState, signalReplicationChange } from '../replication/handler.js'

import type { OrezWorker } from './types'

// extract pgoutput message types from CopyData(XLogData(...)) buffers
function extractPayloadTypes(buf: Uint8Array): number[] {
  const types: number[] = []
  let pos = 0
  while (pos < buf.length) {
    if (buf[pos] !== 0x64) break // CopyData
    const view = new DataView(buf.buffer, buf.byteOffset + pos + 1)
    const len = view.getInt32(0)
    // XLogData starts at pos+5, payload type at pos+5+1+8+8+8 = pos+30
    if (pos + 30 < buf.length) {
      types.push(buf[pos + 30])
    }
    pos += 1 + len
  }
  return types
}

describe('orez/worker integration', { timeout: 30000 }, () => {
  let worker: OrezWorker

  beforeEach(async () => {
    resetReplicationState()
    worker = await createOrezWorker({
      pgliteOptions: { dataDir: 'memory://' },
    })
    // create test table
    await worker.exec(`
      CREATE TABLE public.foo (
        id TEXT PRIMARY KEY,
        value TEXT,
        num INTEGER
      )
    `)
    // reinstall triggers after table creation
    await worker.installChangeTracking()
  })

  afterEach(async () => {
    resetReplicationState()
    await worker.close()
  })

  it('change tracking captures insert/update/delete cycle', async () => {
    await worker.query('INSERT INTO foo (id, value, num) VALUES ($1, $2, $3)', [
      'row1',
      'hello',
      42,
    ])
    await worker.query('UPDATE foo SET value = $1 WHERE id = $2', ['updated', 'row1'])
    await worker.query('DELETE FROM foo WHERE id = $1', ['row1'])

    const changes = await worker.getChangesSince(0)
    expect(changes).toHaveLength(3)
    expect(changes[0].op).toBe('INSERT')
    expect(changes[0].row_data).toMatchObject({ id: 'row1', value: 'hello', num: 42 })
    expect(changes[1].op).toBe('UPDATE')
    expect(changes[1].row_data).toMatchObject({ id: 'row1', value: 'updated' })
    expect(changes[1].old_data).toMatchObject({ id: 'row1', value: 'hello' })
    expect(changes[2].op).toBe('DELETE')
    expect(changes[2].old_data).toMatchObject({ id: 'row1', value: 'updated' })
  })

  it('InProcessWriter receives pgoutput stream from replication', async () => {
    const received: Uint8Array[] = []
    const writer = new InProcessWriter((data) => received.push(new Uint8Array(data)))

    // start replication in background
    const replPromise = worker.startReplication(writer)

    // wait for handler to set up
    await new Promise((r) => setTimeout(r, 200))

    // insert data
    await worker.query('INSERT INTO foo (id, value, num) VALUES ($1, $2, $3)', [
      'streamed-1',
      'live',
      99,
    ])
    signalReplicationChange()

    // wait for replication to deliver
    await new Promise((r) => setTimeout(r, 1500))

    // close writer to stop replication
    writer.close()
    await replPromise.catch(() => {}) // may reject on close

    // should have received pgoutput messages
    expect(received.length).toBeGreaterThan(0)

    // extract message types from all received buffers
    const allTypes = received.flatMap(extractPayloadTypes)

    // first message is CopyBothResponse (0x57), then pgoutput messages
    // check we got the core pgoutput types
    expect(allTypes).toContain(0x42) // BEGIN
    expect(allTypes).toContain(0x52) // RELATION
    expect(allTypes).toContain(0x49) // INSERT
    expect(allTypes).toContain(0x43) // COMMIT
  })

  it('watermarks advance monotonically', async () => {
    const watermarks: number[] = []

    for (let i = 0; i < 5; i++) {
      await worker.query('INSERT INTO foo (id, value, num) VALUES ($1, $2, $3)', [
        `wm-${i}`,
        `val-${i}`,
        i,
      ])
      watermarks.push(await worker.getCurrentWatermark())
    }

    // each watermark should be strictly greater than the previous
    for (let i = 1; i < watermarks.length; i++) {
      expect(watermarks[i]).toBeGreaterThan(watermarks[i - 1])
    }

    // getChangesSince with earlier watermark returns only newer changes
    const midpoint = watermarks[2]
    const laterChanges = await worker.getChangesSince(midpoint)
    expect(laterChanges).toHaveLength(2)
    expect(laterChanges[0].row_data).toMatchObject({ id: 'wm-3' })
    expect(laterChanges[1].row_data).toMatchObject({ id: 'wm-4' })
  })

  it('purge removes consumed changes', async () => {
    for (let i = 0; i < 10; i++) {
      await worker.query('INSERT INTO foo (id, value, num) VALUES ($1, $2, $3)', [
        `purge-${i}`,
        'x',
        i,
      ])
    }

    const allChanges = await worker.getChangesSince(0)
    expect(allChanges).toHaveLength(10)

    // purge first 7
    const purged = await worker.purgeChanges(allChanges[6].watermark)
    expect(purged).toBe(7)

    // only 3 remain
    const remaining = await worker.getChangesSince(0)
    expect(remaining).toHaveLength(3)
  })

  it('multiple tables each get tracked', async () => {
    await worker.exec(`
      CREATE TABLE public.bar (
        id TEXT PRIMARY KEY,
        foo_id TEXT
      )
    `)
    await worker.installChangeTracking()

    await worker.query('INSERT INTO foo VALUES ($1, $2, $3)', ['f1', 'a', 1])
    await worker.query('INSERT INTO bar VALUES ($1, $2)', ['b1', 'f1'])

    const changes = await worker.getChangesSince(0)
    expect(changes).toHaveLength(2)
    expect(changes[0].table_name).toBe('public.foo')
    expect(changes[1].table_name).toBe('public.bar')
  })

  it('replication stream contains correct data for multiple operations', async () => {
    const received: Uint8Array[] = []
    const writer = new InProcessWriter((data) => received.push(new Uint8Array(data)))

    const replPromise = worker.startReplication(writer)
    await new Promise((r) => setTimeout(r, 200))

    // insert
    await worker.query('INSERT INTO foo VALUES ($1, $2, $3)', ['r1', 'initial', 1])
    signalReplicationChange()
    await new Promise((r) => setTimeout(r, 800))

    // update
    await worker.query('UPDATE foo SET value = $1 WHERE id = $2', ['modified', 'r1'])
    signalReplicationChange()
    await new Promise((r) => setTimeout(r, 800))

    // delete
    await worker.query('DELETE FROM foo WHERE id = $1', ['r1'])
    signalReplicationChange()
    await new Promise((r) => setTimeout(r, 800))

    writer.close()
    await replPromise.catch(() => {})

    const allTypes = received.flatMap(extractPayloadTypes)

    // should have all three operation types
    expect(allTypes).toContain(0x49) // INSERT
    expect(allTypes).toContain(0x55) // UPDATE
    expect(allTypes).toContain(0x44) // DELETE

    // should have BEGIN/COMMIT pairs
    const begins = allTypes.filter((t) => t === 0x42).length
    const commits = allTypes.filter((t) => t === 0x43).length
    expect(begins).toBe(commits)
    expect(begins).toBeGreaterThanOrEqual(1)
  })
})
