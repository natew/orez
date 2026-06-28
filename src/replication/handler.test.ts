import { PGlite } from '@electric-sql/pglite'
import { describe, it, expect, beforeEach, afterEach } from 'vitest'

import { Mutex } from '../mutex'
import { usePublicationsEnv } from '../test-env'
import { installChangeTracking } from './change-tracker'
import {
  createReplicationFeedbackParser,
  extractStartLsn,
  handleReplicationQuery,
  handleStartReplication,
  lsnFromString,
  REPLICATION_BATCH_SIZE,
  resetReplicationState,
  signalReplicationChange,
  type ReplicationWriter,
} from './handler'

usePublicationsEnv(undefined)

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

  function countCopyDataFrames(buf: Uint8Array): number {
    const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength)
    let pos = 0
    let count = 0
    while (pos < buf.length) {
      if (buf[pos] !== 0x64) return count
      const len = dv.getInt32(pos + 1)
      pos += 1 + len
      count++
    }
    return count
  }

  function readCString(buf: Uint8Array, start: number): [string, number] {
    let end = start
    while (buf[end] !== 0) end++
    return [new TextDecoder().decode(buf.subarray(start, end)), end + 1]
  }

  function decodeTupleTexts(payload: Uint8Array, start: number): string[] {
    const dv = new DataView(payload.buffer, payload.byteOffset, payload.byteLength)
    let pos = start
    const count = dv.getInt16(pos)
    pos += 2
    const values: string[] = []
    for (let i = 0; i < count; i++) {
      const tag = payload[pos++]
      if (tag === 0x6e) {
        values.push('')
        continue
      }
      expect(tag).toBe(0x74)
      const len = dv.getInt32(pos)
      pos += 4
      values.push(new TextDecoder().decode(payload.subarray(pos, pos + len)))
      pos += len
    }
    return values
  }

  function decodeInsertTransactions(chunks: Uint8Array[]): string[][] {
    const relations = new Map<number, string[]>()
    const transactions: string[][] = []
    let current: string[] | null = null

    for (const chunk of chunks) {
      if (chunk[0] !== 0x64) continue
      const dv = new DataView(chunk.buffer, chunk.byteOffset, chunk.byteLength)
      let pos = 0
      while (pos < chunk.length && chunk[pos] === 0x64) {
        const len = dv.getInt32(pos + 1)
        const frameEnd = pos + 1 + len
        if (chunk[pos + 5] !== 0x77) {
          pos = frameEnd
          continue
        }
        const payload = chunk.subarray(pos + 30, frameEnd)
        const tag = payload[0]
        const payloadView = new DataView(
          payload.buffer,
          payload.byteOffset,
          payload.byteLength
        )

        if (tag === 0x42) {
          current = []
        } else if (tag === 0x43) {
          if (current) transactions.push(current)
          current = null
        } else if (tag === 0x52) {
          const tableOid = payloadView.getInt32(1)
          let p = 5
          const [, afterSchema] = readCString(payload, p)
          p = afterSchema
          const [, afterTable] = readCString(payload, p)
          p = afterTable
          p++ // replica identity
          const colCount = payloadView.getInt16(p)
          p += 2
          const columns: string[] = []
          for (let i = 0; i < colCount; i++) {
            p++ // flags
            const [name, next] = readCString(payload, p)
            columns.push(name)
            p = next + 8 // type oid + type mod
          }
          relations.set(tableOid, columns)
        } else if (tag === 0x49) {
          const tableOid = payloadView.getInt32(1)
          const columns = relations.get(tableOid) ?? []
          const values = decodeTupleTexts(payload, 6)
          const name = values[columns.indexOf('name')]
          if (name && current) current.push(name)
        }

        pos = frameEnd
      }
    }

    return transactions.filter((tx) => tx.length > 0)
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

  it('closes a stale unconfirmed stream so zero-cache can reconnect', async () => {
    const prevTimeout = process.env.OREZ_REPLICATION_UNCONFIRMED_RECONNECT_MS
    process.env.OREZ_REPLICATION_UNCONFIRMED_RECONNECT_MS = '100'
    const written: Uint8Array[] = []
    let closed = false
    const writer: ReplicationWriter = {
      write(data: Uint8Array) {
        if (!closed) written.push(new Uint8Array(data))
      },
      get closed() {
        return closed
      },
      close() {
        closed = true
      },
    }

    try {
      replicationPromise = handleStartReplication(
        'START_REPLICATION SLOT "s" LOGICAL 0/0',
        writer,
        db,
        testMutex
      )

      await new Promise((r) => setTimeout(r, 100))
      await db.exec(`INSERT INTO public.items (name, value) VALUES ('stale', 123)`)
      signalReplicationChange()

      const streamedDeadline = Date.now() + 3000
      while (Date.now() < streamedDeadline) {
        const types = written.flatMap(extractPayloadTypes)
        if (types.includes(0x43)) break
        await new Promise((r) => setTimeout(r, 25))
      }
      expect(written.flatMap(extractPayloadTypes)).toContain(0x43)

      const closeDeadline = Date.now() + 3000
      while (!closed && Date.now() < closeDeadline) {
        signalReplicationChange()
        await new Promise((r) => setTimeout(r, 25))
      }

      expect(closed).toBe(true)
      await replicationPromise
    } finally {
      if (prevTimeout === undefined) {
        delete process.env.OREZ_REPLICATION_UNCONFIRMED_RECONNECT_MS
      } else {
        process.env.OREZ_REPLICATION_UNCONFIRMED_RECONNECT_MS = prevTimeout
      }
    }
  })

  it('writes one CopyData frame per socket chunk', async () => {
    const { written, writer } = createWriter()

    replicationPromise = handleStartReplication(
      'START_REPLICATION SLOT "s" LOGICAL 0/0',
      writer,
      db,
      testMutex
    )

    await new Promise((r) => setTimeout(r, 100))
    await db.exec(`INSERT INTO public.items (name, value) VALUES ('chunked', 123)`)
    signalReplicationChange()
    await new Promise((r) => setTimeout(r, 700))

    const copyDataWrites = written.filter((msg) => msg[0] === 0x64)
    expect(copyDataWrites.length).toBeGreaterThanOrEqual(4)
    for (const msg of copyDataWrites) {
      expect(countCopyDataFrames(msg)).toBe(1)
    }
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

  it('streams large change bursts as bounded transactions', async () => {
    const { written, writer } = createWriter()
    const total = REPLICATION_BATCH_SIZE + 5

    replicationPromise = handleStartReplication(
      'START_REPLICATION SLOT "s" LOGICAL 0/0',
      writer,
      db,
      testMutex
    )

    await new Promise((r) => setTimeout(r, 100))
    await db.exec(`
      INSERT INTO public.items (name, value)
      SELECT 'bulk-' || n::text, n
      FROM generate_series(1, ${total}) AS g(n)
    `)
    signalReplicationChange()

    let types: number[] = []
    const deadline = Date.now() + 15_000
    while (Date.now() < deadline) {
      types = written.flatMap(extractPayloadTypes)
      const inserts = types.filter((t) => t === 0x49).length
      const begins = types.filter((t) => t === 0x42).length
      if (inserts === total && begins >= 2) break
      await new Promise((r) => setTimeout(r, 100))
    }

    const insertTransactions = decodeInsertTransactions(written)
    const orderedNames = insertTransactions.flat()

    expect(orderedNames).toEqual(Array.from({ length: total }, (_, i) => `bulk-${i + 1}`))
    expect(insertTransactions.map((tx) => tx.length)).toEqual([REPLICATION_BATCH_SIZE, 5])
    expect(types.filter((t) => t === 0x43).length).toBe(
      types.filter((t) => t === 0x42).length
    )
  }, 20_000)

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

describe('lsnFromString', () => {
  it('parses 0/0 to 0n', () => {
    expect(lsnFromString('0/0')).toBe(0n)
  })

  it('parses simple LSN', () => {
    expect(lsnFromString('0/1000000')).toBe(0x1000000n)
  })

  it('combines high and low halves', () => {
    expect(lsnFromString('1/0')).toBe(0x100000000n)
    expect(lsnFromString('1/1')).toBe(0x100000001n)
    expect(lsnFromString('A/B')).toBe(0xa0000000bn)
  })

  it('is case-insensitive', () => {
    expect(lsnFromString('0/ff')).toBe(0xffn)
    expect(lsnFromString('0/FF')).toBe(0xffn)
  })

  it('tolerates surrounding whitespace', () => {
    expect(lsnFromString('  0/100  ')).toBe(0x100n)
  })

  it('returns null for malformed input', () => {
    expect(lsnFromString('0')).toBeNull()
    expect(lsnFromString('0/')).toBeNull()
    expect(lsnFromString('/0')).toBeNull()
    expect(lsnFromString('xyz')).toBeNull()
    expect(lsnFromString('')).toBeNull()
  })
})

describe('extractStartLsn', () => {
  it('extracts from a basic START_REPLICATION query', () => {
    expect(extractStartLsn('START_REPLICATION SLOT "zero" LOGICAL 0/01000300')).toBe(
      0x1000300n
    )
  })

  it('handles trailing options', () => {
    expect(
      extractStartLsn(
        `START_REPLICATION SLOT "zero" LOGICAL 0/01000300 (proto_version '4', publication_names 'orez_zero_public')`
      )
    ).toBe(0x1000300n)
  })

  it('handles 0/0 (fresh slot)', () => {
    expect(extractStartLsn('START_REPLICATION SLOT "zero" LOGICAL 0/0')).toBe(0n)
  })

  it('handles quoted LSN', () => {
    expect(extractStartLsn(`START_REPLICATION SLOT "zero" LOGICAL '0/01000300'`)).toBe(
      0x1000300n
    )
  })

  it('is case-insensitive on the keyword', () => {
    expect(extractStartLsn('start_replication slot "z" logical 0/abc')).toBe(0xabcn)
  })

  it('returns null when no LSN is present', () => {
    expect(extractStartLsn('START_REPLICATION SLOT "z"')).toBeNull()
    expect(extractStartLsn('IDENTIFY_SYSTEM')).toBeNull()
  })
})

describe('createReplicationFeedbackParser', () => {
  // build a CopyData frame wrapping a standby status update ('r' message:
  // written(8) + flushed(8) + applied(8) + clock(8) + replyRequested(1))
  function standbyStatusFrame(flushedLsn: bigint): Uint8Array {
    const payload = new Uint8Array(34)
    const view = new DataView(payload.buffer)
    payload[0] = 0x72 // 'r'
    view.setBigUint64(1, flushedLsn)
    view.setBigUint64(9, flushedLsn)
    view.setBigUint64(17, flushedLsn)
    view.setBigUint64(25, 0n)
    payload[33] = 0
    const frame = new Uint8Array(1 + 4 + payload.length)
    frame[0] = 0x64 // CopyData
    new DataView(frame.buffer).setInt32(1, 4 + payload.length)
    frame.set(payload, 5)
    return frame
  }

  it('extracts the flushed lsn from a standby status update', () => {
    const seen: bigint[] = []
    const parse = createReplicationFeedbackParser((lsn) => seen.push(lsn))
    parse(standbyStatusFrame(0x1000300n))
    expect(seen).toEqual([0x1000300n])
  })

  it('handles coalesced and fragmented frames', () => {
    const seen: bigint[] = []
    const parse = createReplicationFeedbackParser((lsn) => seen.push(lsn))
    const a = standbyStatusFrame(100n)
    const b = standbyStatusFrame(200n)
    const coalesced = new Uint8Array(a.length + b.length)
    coalesced.set(a)
    coalesced.set(b, a.length)
    // split mid-frame
    parse(coalesced.subarray(0, a.length + 7))
    expect(seen).toEqual([100n])
    parse(coalesced.subarray(a.length + 7))
    expect(seen).toEqual([100n, 200n])
  })

  it('ignores keepalive zero lsns and non-CopyData frames', () => {
    const seen: bigint[] = []
    const parse = createReplicationFeedbackParser((lsn) => seen.push(lsn))
    parse(standbyStatusFrame(0n))
    // CopyDone frame
    const copyDone = new Uint8Array([0x63, 0, 0, 0, 4])
    parse(copyDone)
    expect(seen).toEqual([])
  })
})
