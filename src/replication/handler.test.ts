import { PGlite } from '@electric-sql/pglite'
import { describe, it, expect, beforeEach, afterEach } from 'vitest'

import { Mutex } from '../mutex'
import { installChangeTracking } from './change-tracker'
import {
  handleReplicationQuery,
  handleStartReplication,
  resetReplicationState,
  signalReplicationChange,
  type ReplicationWriter,
} from './handler'

// parse wire protocol RowDescription+DataRow response into columns/values
function parseResponse(buf: Uint8Array): { columns: string[]; values: string[] } | null {
  if (buf[0] !== 0x54) return null // RowDescription

  const dv = new DataView(buf.buffer, buf.byteOffset)
  let pos = 7
  const numFields = dv.getInt16(5)
  const columns: string[] = []
  for (let i = 0; i < numFields; i++) {
    let end = pos
    while (buf[end] !== 0) end++
    columns.push(new TextDecoder().decode(buf.subarray(pos, end)))
    pos = end + 1 + 4 + 2 + 4 + 2 + 4 + 2
  }

  if (buf[pos] !== 0x44) return { columns, values: [] }
  pos += 7
  const values: string[] = []
  for (let i = 0; i < numFields; i++) {
    const len = dv.getInt32(pos)
    pos += 4
    values.push(new TextDecoder().decode(buf.subarray(pos, pos + len)))
    pos += len
  }

  return { columns, values }
}

describe('handleReplicationQuery', () => {
  let db: PGlite

  beforeEach(async () => {
    db = new PGlite()
    await db.waitReady
    await installChangeTracking(db)
  })

  afterEach(async () => {
    await db.close()
  })

  it('IDENTIFY_SYSTEM returns system info', async () => {
    const res = await handleReplicationQuery('IDENTIFY_SYSTEM', db)
    expect(res).not.toBeNull()

    const parsed = parseResponse(res!)
    expect(parsed!.columns).toEqual(['systemid', 'timeline', 'xlogpos', 'dbname'])
    expect(parsed!.values[0]).toBe('1234567890')
    expect(parsed!.values[1]).toBe('1')
    expect(parsed!.values[3]).toBe('postgres')
    // xlogpos should be a valid LSN format
    expect(parsed!.values[2]).toMatch(/^[0-9A-F]+\/[0-9A-F]+$/)
  })

  it('CREATE_REPLICATION_SLOT persists and returns slot info', async () => {
    const res = await handleReplicationQuery(
      'CREATE_REPLICATION_SLOT "test_slot" TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT',
      db
    )

    const parsed = parseResponse(res!)
    expect(parsed!.values[0]).toBe('test_slot')
    expect(parsed!.values[3]).toBe('pgoutput')

    const slots = await db.query<{ slot_name: string }>(
      `SELECT slot_name FROM _orez._zero_replication_slots WHERE slot_name = 'test_slot'`
    )
    expect(slots.rows).toHaveLength(1)
  })

  it('DROP_REPLICATION_SLOT removes slot', async () => {
    await handleReplicationQuery(
      'CREATE_REPLICATION_SLOT "drop_me" TEMPORARY LOGICAL pgoutput',
      db
    )
    await handleReplicationQuery('DROP_REPLICATION_SLOT "drop_me"', db)

    const slots = await db.query<{ count: string }>(
      `SELECT count(*) as count FROM _orez._zero_replication_slots WHERE slot_name = 'drop_me'`
    )
    expect(Number(slots.rows[0].count)).toBe(0)
  })

  it('wal_level query returns logical', async () => {
    const res = await handleReplicationQuery(
      "SELECT current_setting('wal_level'), version()",
      db
    )
    expect(res).not.toBeNull()
    const parsed = parseResponse(res!)
    expect(parsed!.values[0]).toBe('logical')
  })

  it('ALTER ROLE returns success', async () => {
    const res = await handleReplicationQuery('ALTER ROLE user REPLICATION', db)
    expect(res).not.toBeNull()
    // should contain CommandComplete
    expect(res![0]).toBe(0x43) // 'C'
  })

  it('returns null for unknown queries', async () => {
    expect(await handleReplicationQuery('SELECT 1', db)).toBeNull()
  })
})

describe('handleStartReplication', () => {
  let db: PGlite
  let replicationPromise: Promise<void>
  const testMutex = new Mutex()

  beforeEach(async () => {
    resetReplicationState()
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
    // closing db causes poll loop to exit with 'closed' error
    await db.close()
    // wake handler from idle sleep so it hits the closed db and exits
    signalReplicationChange()
    // wait for the replication promise to settle
    await replicationPromise?.catch(() => {})
  })

  function createWriter() {
    const written: Uint8Array[] = []
    const writer: ReplicationWriter = {
      write(data: Uint8Array) {
        written.push(new Uint8Array(data))
      },
    }
    return { written, writer }
  }

  // extract all pgoutput message types from a (possibly batched) buffer.
  // each CopyData frame: 0x64 + int32(len) + payload
  // XLogData payload: 0x77 + 24 bytes header + actual message type byte
  function extractPayloadTypes(buf: Uint8Array): number[] {
    const types: number[] = []
    const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength)
    let pos = 0
    while (pos < buf.length) {
      if (buf[pos] !== 0x64) break // not CopyData
      const len = dv.getInt32(pos + 1)
      if (buf[pos + 5] === 0x77 && pos + 30 < buf.length) {
        types.push(buf[pos + 30])
      }
      pos += 1 + len
    }
    return types
  }

  it('sends CopyBothResponse first', async () => {
    const { written, writer } = createWriter()

    replicationPromise = handleStartReplication(
      'START_REPLICATION SLOT "s" LOGICAL 0/0',
      writer,
      db,
      testMutex
    )

    await new Promise((r) => setTimeout(r, 200))

    expect(written.length).toBeGreaterThan(0)
    expect(written[0][0]).toBe(0x57) // 'W' CopyBothResponse
  })

  it('sends keepalives', async () => {
    const { written, writer } = createWriter()

    replicationPromise = handleStartReplication(
      'START_REPLICATION SLOT "s" LOGICAL 0/0',
      writer,
      db,
      testMutex
    )

    await new Promise((r) => setTimeout(r, 700))

    const keepalives = written.filter((msg) => msg[0] === 0x64 && msg[5] === 0x6b)
    expect(keepalives.length).toBeGreaterThan(0)
  })

  it('streams INSERT as BEGIN+RELATION+INSERT+COMMIT', async () => {
    const { written, writer } = createWriter()

    replicationPromise = handleStartReplication(
      'START_REPLICATION SLOT "s" LOGICAL 0/0',
      writer,
      db,
      testMutex
    )

    await new Promise((r) => setTimeout(r, 100))
    await db.exec(`INSERT INTO public.items (name, value) VALUES ('streamed', 123)`)
    signalReplicationChange()
    await new Promise((r) => setTimeout(r, 700))

    const types = written.flatMap(extractPayloadTypes)

    expect(types).toContain(0x42) // BEGIN
    expect(types).toContain(0x52) // RELATION
    expect(types).toContain(0x49) // INSERT
    expect(types).toContain(0x43) // COMMIT

    // order: BEGIN before RELATION before INSERT before COMMIT
    const beginIdx = types.indexOf(0x42)
    const relIdx = types.indexOf(0x52)
    const insIdx = types.indexOf(0x49)
    const comIdx = types.indexOf(0x43)
    expect(beginIdx).toBeLessThan(relIdx)
    expect(relIdx).toBeLessThan(insIdx)
    expect(insIdx).toBeLessThan(comIdx)
  })

  it('streams UPDATE and DELETE operations', async () => {
    const { written, writer } = createWriter()

    replicationPromise = handleStartReplication(
      'START_REPLICATION SLOT "s" LOGICAL 0/0',
      writer,
      db,
      testMutex
    )

    await new Promise((r) => setTimeout(r, 100))

    await db.exec(`INSERT INTO public.items (name, value) VALUES ('mut', 1)`)
    signalReplicationChange()
    await new Promise((r) => setTimeout(r, 700))

    await db.exec(`UPDATE public.items SET value = 2 WHERE name = 'mut'`)
    signalReplicationChange()
    await new Promise((r) => setTimeout(r, 700))

    await db.exec(`DELETE FROM public.items WHERE name = 'mut'`)
    signalReplicationChange()
    await new Promise((r) => setTimeout(r, 700))

    const types = written.flatMap(extractPayloadTypes)
    expect(types).toContain(0x49) // INSERT
    expect(types).toContain(0x55) // UPDATE
    expect(types).toContain(0x44) // DELETE
  }, 10_000)

  it('only sends RELATION once per table', async () => {
    const { written, writer } = createWriter()

    replicationPromise = handleStartReplication(
      'START_REPLICATION SLOT "s" LOGICAL 0/0',
      writer,
      db,
      testMutex
    )

    await new Promise((r) => setTimeout(r, 100))

    await db.exec(`INSERT INTO public.items (name, value) VALUES ('a', 1)`)
    signalReplicationChange()
    await new Promise((r) => setTimeout(r, 700))

    await db.exec(`INSERT INTO public.items (name, value) VALUES ('b', 2)`)
    signalReplicationChange()
    await new Promise((r) => setTimeout(r, 700))

    const types = written.flatMap(extractPayloadTypes)
    const relationCount = types.filter((t) => t === 0x52).length
    expect(relationCount).toBe(1)
  }, 10_000)

  it('sends RELATION for each distinct table', async () => {
    await db.exec(`CREATE TABLE public.other (id SERIAL PRIMARY KEY, label TEXT)`)
    await installChangeTracking(db)

    const { written, writer } = createWriter()

    replicationPromise = handleStartReplication(
      'START_REPLICATION SLOT "s" LOGICAL 0/0',
      writer,
      db,
      testMutex
    )

    await new Promise((r) => setTimeout(r, 100))

    await db.exec(`INSERT INTO public.items (name, value) VALUES ('a', 1)`)
    await db.exec(`INSERT INTO public.other (label) VALUES ('b')`)
    signalReplicationChange()
    await new Promise((r) => setTimeout(r, 700))

    const types = written.flatMap(extractPayloadTypes)
    const relationCount = types.filter((t) => t === 0x52).length
    expect(relationCount).toBe(2)
  })

  it('handles rapid sequential inserts', async () => {
    const { written, writer } = createWriter()

    replicationPromise = handleStartReplication(
      'START_REPLICATION SLOT "s" LOGICAL 0/0',
      writer,
      db,
      testMutex
    )

    await new Promise((r) => setTimeout(r, 100))

    for (let i = 0; i < 20; i++) {
      await db.exec(`INSERT INTO public.items (name, value) VALUES ('r${i}', ${i})`)
    }
    signalReplicationChange()

    // wait for handler to process
    await new Promise((r) => setTimeout(r, 1500))

    const inserts = written.flatMap(extractPayloadTypes).filter((t) => t === 0x49)
    expect(inserts.length).toBe(20)
  }, 10_000)

  it('each transaction has matching BEGIN and COMMIT', async () => {
    const { written, writer } = createWriter()

    replicationPromise = handleStartReplication(
      'START_REPLICATION SLOT "s" LOGICAL 0/0',
      writer,
      db,
      testMutex
    )

    await new Promise((r) => setTimeout(r, 100))

    await db.exec(`INSERT INTO public.items (name, value) VALUES ('tx1', 1)`)
    signalReplicationChange()
    await new Promise((r) => setTimeout(r, 700))

    await db.exec(`INSERT INTO public.items (name, value) VALUES ('tx2', 2)`)
    signalReplicationChange()
    await new Promise((r) => setTimeout(r, 700))

    const types = written.flatMap(extractPayloadTypes)
    const begins = types.filter((t) => t === 0x42).length
    const commits = types.filter((t) => t === 0x43).length
    expect(begins).toBe(commits)
    expect(begins).toBeGreaterThanOrEqual(1)
  }, 10_000)
})

describe('InProcessWriter', () => {
  it('routes data to callback', async () => {
    const { InProcessWriter } = await import('./handler')
    const received: Uint8Array[] = []
    const writer = new InProcessWriter((data) => received.push(data))

    const msg = new Uint8Array([1, 2, 3])
    writer.write(msg)
    expect(received).toHaveLength(1)
    expect(received[0]).toEqual(msg)
    expect(writer.closed).toBe(false)
  })

  it('stops delivering after close', async () => {
    const { InProcessWriter } = await import('./handler')
    const received: Uint8Array[] = []
    const writer = new InProcessWriter((data) => received.push(data))

    writer.write(new Uint8Array([1]))
    writer.close()
    writer.write(new Uint8Array([2]))

    expect(received).toHaveLength(1)
    expect(writer.closed).toBe(true)
  })

  it('implements ReplicationWriter interface', async () => {
    const { InProcessWriter } = await import('./handler')
    const writer = new InProcessWriter(() => {})

    // type check: can assign to ReplicationWriter
    const rw: ReplicationWriter = writer
    expect(rw.write).toBeDefined()
    expect(rw.closed).toBe(false)
  })
})
