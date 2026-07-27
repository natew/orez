/**
 * tcp-level integration test for the full replication stack.
 *
 * connects to pg-proxy over tcp, speaks the pg wire protocol,
 * runs the replication handshake, and verifies streamed changes
 * match what a real pg consumer expects.
 *
 * this catches integration bugs (socket handling, framing, auth,
 * query routing) that unit tests on individual components miss.
 */

import { createConnection, type Socket } from 'node:net'

import { PGlite } from '@electric-sql/pglite'
import { describe, it, expect, beforeEach, afterEach } from 'vitest'

import { getConfig } from '../config'
import { startPgProxy } from '../pg-proxy'
import { usePublicationsEnv } from '../test-env'
import { installChangeTracking } from './change-tracker'
import { signalReplicationChange, resetReplicationState } from './handler'

import type { Server, AddressInfo } from 'node:net'

usePublicationsEnv(undefined)

// --- pgoutput decoder (validates against pg protocol spec) ---

interface DecodedMessage {
  type: string
  raw: Uint8Array
}

interface BeginMessage extends DecodedMessage {
  type: 'Begin'
  lsn: bigint
  timestamp: bigint
  xid: number
}

interface CommitMessage extends DecodedMessage {
  type: 'Commit'
  flags: number
  lsn: bigint
  endLsn: bigint
  timestamp: bigint
}

interface RelationColumn {
  flags: number
  name: string
  typeOid: number
  typeMod: number
}

interface RelationMessage extends DecodedMessage {
  type: 'Relation'
  tableOid: number
  schema: string
  tableName: string
  replicaIdentity: number
  columns: RelationColumn[]
}

interface InsertMessage extends DecodedMessage {
  type: 'Insert'
  tableOid: number
  tupleData: TupleData
}

interface UpdateMessage extends DecodedMessage {
  type: 'Update'
  tableOid: number
  oldTupleData?: TupleData
  newTupleData: TupleData
}

interface DeleteMessage extends DecodedMessage {
  type: 'Delete'
  tableOid: number
  keyTupleData: TupleData
}

interface TupleData {
  columns: Array<{ type: 'null' | 'text'; value: string | null }>
}

interface KeepaliveMessage extends DecodedMessage {
  type: 'Keepalive'
  walEnd: bigint
  timestamp: bigint
  replyRequested: boolean
}

type PgOutputMessage =
  | BeginMessage
  | CommitMessage
  | RelationMessage
  | InsertMessage
  | UpdateMessage
  | DeleteMessage
  | KeepaliveMessage

function r16(buf: Uint8Array, off: number) {
  return new DataView(buf.buffer, buf.byteOffset).getInt16(off)
}
function r32(buf: Uint8Array, off: number) {
  return new DataView(buf.buffer, buf.byteOffset).getInt32(off)
}
function r64(buf: Uint8Array, off: number) {
  return new DataView(buf.buffer, buf.byteOffset).getBigInt64(off)
}
function rCStr(buf: Uint8Array, off: number): [string, number] {
  let end = off
  while (end < buf.length && buf[end] !== 0) end++
  return [new TextDecoder().decode(buf.subarray(off, end)), end + 1]
}

function decodeTupleData(buf: Uint8Array, off: number): [TupleData, number] {
  const numCols = r16(buf, off)
  off += 2
  const columns: TupleData['columns'] = []
  for (let i = 0; i < numCols; i++) {
    const colType = buf[off++]
    if (colType === 0x6e) {
      // 'n' null
      columns.push({ type: 'null', value: null })
    } else if (colType === 0x74) {
      // 't' text
      const len = r32(buf, off)
      off += 4
      const value = new TextDecoder().decode(buf.subarray(off, off + len))
      off += len
      columns.push({ type: 'text', value })
    } else {
      throw new Error(`unknown tuple column type: 0x${colType.toString(16)}`)
    }
  }
  return [{ columns }, off]
}

function decodePgOutput(data: Uint8Array): PgOutputMessage {
  const msgType = data[0]

  switch (msgType) {
    case 0x42: {
      // Begin
      return {
        type: 'Begin',
        raw: data,
        lsn: r64(data, 1),
        timestamp: r64(data, 9),
        xid: r32(data, 17),
      }
    }
    case 0x43: {
      // Commit
      return {
        type: 'Commit',
        raw: data,
        flags: data[1],
        lsn: r64(data, 2),
        endLsn: r64(data, 10),
        timestamp: r64(data, 18),
      }
    }
    case 0x52: {
      // Relation
      const tableOid = r32(data, 1)
      let pos = 5
      const [schema, p1] = rCStr(data, pos)
      pos = p1
      const [tableName, p2] = rCStr(data, pos)
      pos = p2
      const replicaIdentity = data[pos++]
      const numCols = r16(data, pos)
      pos += 2
      const columns: RelationColumn[] = []
      for (let i = 0; i < numCols; i++) {
        const flags = data[pos++]
        const [name, np] = rCStr(data, pos)
        pos = np
        const typeOid = r32(data, pos)
        pos += 4
        const typeMod = r32(data, pos)
        pos += 4
        columns.push({ flags, name, typeOid, typeMod })
      }
      return {
        type: 'Relation',
        raw: data,
        tableOid,
        schema,
        tableName,
        replicaIdentity,
        columns,
      }
    }
    case 0x49: {
      // Insert
      const tableOid = r32(data, 1)
      const marker = data[5] // should be 'N'
      if (marker !== 0x4e)
        throw new Error(`insert: expected 'N' marker, got 0x${marker.toString(16)}`)
      const [tupleData] = decodeTupleData(data, 6)
      return { type: 'Insert', raw: data, tableOid, tupleData }
    }
    case 0x55: {
      // Update
      const tableOid = r32(data, 1)
      let pos = 5
      let oldTupleData: TupleData | undefined
      if (data[pos] === 0x4f) {
        // 'O' old tuple
        pos++
        const [old, np] = decodeTupleData(data, pos)
        oldTupleData = old
        pos = np
      }
      if (data[pos] !== 0x4e) throw new Error(`update: expected 'N' marker at ${pos}`)
      pos++
      const [newTupleData] = decodeTupleData(data, pos)
      return { type: 'Update', raw: data, tableOid, oldTupleData, newTupleData }
    }
    case 0x44: {
      // Delete
      const tableOid = r32(data, 1)
      const marker = data[5]
      if (marker !== 0x4b && marker !== 0x4f)
        throw new Error(
          `delete: expected 'K' or 'O' marker, got 0x${marker.toString(16)}`
        )
      const [keyTupleData] = decodeTupleData(data, 6)
      return { type: 'Delete', raw: data, tableOid, keyTupleData }
    }
    default:
      throw new Error(`unknown pgoutput message type: 0x${msgType.toString(16)}`)
  }
}

// decode a CopyData frame, returning either an XLogData payload or a Keepalive
function decodeCopyData(frame: Uint8Array): PgOutputMessage | KeepaliveMessage | null {
  if (frame[0] !== 0x64) return null // not CopyData
  const innerType = frame[5]
  if (innerType === 0x77) {
    // XLogData: walStart(8) + walEnd(8) + timestamp(8) + data
    const payload = frame.subarray(30)
    return decodePgOutput(payload)
  }
  if (innerType === 0x6b) {
    // Keepalive
    return {
      type: 'Keepalive',
      raw: frame,
      walEnd: r64(frame, 6),
      timestamp: r64(frame, 14),
      replyRequested: frame[22] === 1,
    }
  }
  return null
}

// --- minimal pg wire protocol client ---

function buildStartupMessage(params: Record<string, string>): Buffer {
  const pairs: Buffer[] = []
  for (const [k, v] of Object.entries(params)) {
    pairs.push(Buffer.from(`${k}\0${v}\0`, 'utf8'))
  }
  pairs.push(Buffer.from('\0', 'utf8'))

  const bodyLen = pairs.reduce((s, b) => s + b.length, 0)
  const buf = Buffer.alloc(4 + 4 + bodyLen)
  buf.writeInt32BE(4 + 4 + bodyLen, 0) // length
  buf.writeInt32BE(196608, 4) // protocol version 3.0
  let pos = 8
  for (const p of pairs) {
    p.copy(buf, pos)
    pos += p.length
  }
  return buf
}

function buildPasswordMessage(password: string): Buffer {
  const pwBuf = Buffer.from(password + '\0', 'utf8')
  const buf = Buffer.alloc(1 + 4 + pwBuf.length)
  buf[0] = 0x70 // 'p'
  buf.writeInt32BE(4 + pwBuf.length, 1)
  pwBuf.copy(buf, 5)
  return buf
}

function buildQuery(sql: string): Buffer {
  const sqlBuf = Buffer.from(sql + '\0', 'utf8')
  const buf = Buffer.alloc(1 + 4 + sqlBuf.length)
  buf[0] = 0x51 // 'Q'
  buf.writeInt32BE(4 + sqlBuf.length, 1)
  sqlBuf.copy(buf, 5)
  return buf
}

interface PgMessage {
  type: number
  data: Buffer
}

// reads exactly one PG message from a buffer, returns [message, remainingBuffer]
function parseMessage(buf: Buffer): [PgMessage | null, Buffer] {
  if (buf.length < 5) return [null, buf]
  const type = buf[0]
  const len = buf.readInt32BE(1)
  const totalLen = 1 + len
  if (buf.length < totalLen) return [null, buf]
  return [{ type, data: buf.subarray(0, totalLen) }, buf.subarray(totalLen)]
}

// higher-level client that connects, authenticates, and can send queries
class TestPgClient {
  private socket!: Socket
  private buffer = Buffer.alloc(0)
  private waiters: Array<(msg: PgMessage) => void> = []
  private messages: PgMessage[] = []
  port: number

  constructor(port: number) {
    this.port = port
  }

  async connect(opts: {
    user: string
    password: string
    database: string
    replication?: boolean
  }): Promise<void> {
    this.socket = createConnection({ port: this.port, host: '127.0.0.1' })

    await new Promise<void>((resolve, reject) => {
      this.socket.once('connect', resolve)
      this.socket.once('error', reject)
    })

    this.socket.on('data', (chunk: Buffer) => {
      this.buffer = Buffer.concat([this.buffer, chunk])
      this.drain()
    })

    const params: Record<string, string> = {
      user: opts.user,
      database: opts.database,
    }
    if (opts.replication) {
      params.replication = 'database'
    }

    this.socket.write(buildStartupMessage(params))

    // wait for auth request
    const authReq = await this.nextMessage()
    expect(authReq.type).toBe(0x52) // 'R' Authentication

    const authType = authReq.data.readInt32BE(5)
    if (authType === 3) {
      // cleartext password
      this.socket.write(buildPasswordMessage(opts.password))
      const authOk = await this.nextMessage()
      expect(authOk.type).toBe(0x52)
      expect(authOk.data.readInt32BE(5)).toBe(0) // AuthenticationOk
    }

    // consume parameter status + backend key data + ready for query
    while (true) {
      const msg = await this.nextMessage()
      if (msg.type === 0x5a) break // ReadyForQuery
    }
  }

  // send simple query and collect all response messages until ReadyForQuery
  async query(sql: string): Promise<PgMessage[]> {
    this.socket.write(buildQuery(sql))
    const responses: PgMessage[] = []
    while (true) {
      const msg = await this.nextMessage()
      responses.push(msg)
      if (msg.type === 0x5a) break // ReadyForQuery
    }
    return responses
  }

  // send START_REPLICATION and return CopyBothResponse, then collect stream messages
  async startReplication(query: string): Promise<void> {
    this.socket.write(buildQuery(query))
  }

  // collect streaming messages for a duration
  async collectStream(durationMs: number): Promise<PgMessage[]> {
    const collected: PgMessage[] = []
    const deadline = Date.now() + durationMs
    while (Date.now() < deadline) {
      try {
        const msg = await this.nextMessage(Math.max(50, deadline - Date.now()))
        collected.push(msg)
      } catch {
        // timeout, keep going
      }
    }
    return collected
  }

  // send raw data to inject into connection (e.g. for data connection)
  sendRaw(data: Buffer) {
    this.socket.write(data)
  }

  close() {
    this.socket?.destroy()
  }

  private drain() {
    while (true) {
      const [msg, remaining] = parseMessage(this.buffer)
      if (!msg) break
      this.buffer = remaining
      const waiter = this.waiters.shift()
      if (waiter) {
        waiter(msg)
      } else {
        this.messages.push(msg)
      }
    }
  }

  private nextMessage(timeoutMs = 5000): Promise<PgMessage> {
    const queued = this.messages.shift()
    if (queued) return Promise.resolve(queued)

    return new Promise<PgMessage>((resolve, reject) => {
      const waiter = (msg: PgMessage) => {
        clearTimeout(timer)
        resolve(msg)
      }
      const timer = setTimeout(() => {
        const idx = this.waiters.indexOf(waiter)
        if (idx >= 0) this.waiters.splice(idx, 1)
        reject(new Error(`timeout waiting for message (${timeoutMs}ms)`))
      }, timeoutMs)

      this.waiters.push(waiter)
    })
  }
}

// --- tests ---

describe('tcp replication', () => {
  let db: PGlite
  let server: Server
  let port: number

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

    // publication for zero-cache
    await db.exec(`CREATE PUBLICATION zero_takeout FOR ALL TABLES`)

    await installChangeTracking(db)

    const config = {
      ...getConfig(),
      pgPort: 0, // random port
    }
    server = await startPgProxy(db, config)
    port = (server.address() as AddressInfo).port
  })

  afterEach(async () => {
    server?.close()
    await db?.close()
  })

  it('accepts connection and authenticates', async () => {
    const client = new TestPgClient(port)
    await client.connect({
      user: 'user',
      password: 'password',
      database: 'postgres',
    })
    client.close()
  })

  it('rejects wrong password', async () => {
    const client = new TestPgClient(port)
    await expect(
      client.connect({
        user: 'user',
        password: 'wrong',
        database: 'postgres',
      })
    ).rejects.toThrow()
    client.close()
  })

  it('handles IDENTIFY_SYSTEM over tcp', async () => {
    const client = new TestPgClient(port)
    await client.connect({
      user: 'user',
      password: 'password',
      database: 'postgres',
      replication: true,
    })

    const response = await client.query('IDENTIFY_SYSTEM')
    // should have RowDescription + DataRow + CommandComplete + ReadyForQuery
    const types = response.map((m) => m.type)
    expect(types).toContain(0x54) // RowDescription
    expect(types).toContain(0x44) // DataRow
    expect(types).toContain(0x43) // CommandComplete
    expect(types).toContain(0x5a) // ReadyForQuery

    client.close()
  })

  it('handles CREATE_REPLICATION_SLOT over tcp', async () => {
    const client = new TestPgClient(port)
    await client.connect({
      user: 'user',
      password: 'password',
      database: 'postgres',
      replication: true,
    })

    const response = await client.query(
      'CREATE_REPLICATION_SLOT "tcp_test" TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT'
    )
    const types = response.map((m) => m.type)
    expect(types).toContain(0x54) // RowDescription
    expect(types).toContain(0x44) // DataRow

    client.close()
  })

  it('handles rapid inserts over tcp', async () => {
    const replClient = new TestPgClient(port)
    await replClient.connect({
      user: 'user',
      password: 'password',
      database: 'postgres',
      replication: true,
    })
    await replClient.query(
      'CREATE_REPLICATION_SLOT "rapid_test" TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT'
    )
    await replClient.startReplication(
      "START_REPLICATION SLOT \"rapid_test\" LOGICAL 0/0 (proto_version '1', publication_names 'zero_takeout')"
    )

    await replClient.collectStream(200)

    const count = 15
    for (let i = 0; i < count; i++) {
      await db.exec(`INSERT INTO public.items (name, value) VALUES ('rapid${i}', ${i})`)
    }
    signalReplicationChange()

    // poll until all inserts arrive
    const allDecoded: PgOutputMessage[] = []
    const deadline = Date.now() + 5000
    while (Date.now() < deadline) {
      const stream = await replClient.collectStream(500)
      for (const msg of stream) {
        if (msg.type === 0x64) {
          const result = decodeCopyData(new Uint8Array(msg.data))
          if (result) allDecoded.push(result)
        }
      }
      const inserts = allDecoded.filter((m) => m.type === 'Insert')
      if (inserts.length >= count) break
      signalReplicationChange()
    }

    const inserts = allDecoded.filter((m) => m.type === 'Insert')
    expect(inserts.length).toBe(count)

    replClient.close()
  }, 15_000)

  it('regular (non-replication) queries work over tcp', async () => {
    const client = new TestPgClient(port)
    await client.connect({
      user: 'user',
      password: 'password',
      database: 'postgres',
    })

    // insert via tcp
    await client.query(`INSERT INTO public.items (name, value) VALUES ('tcp_direct', 77)`)

    // select back
    const response = await client.query(
      `SELECT name, value FROM public.items WHERE name = 'tcp_direct'`
    )
    const dataRow = response.find((m) => m.type === 0x44) // DataRow
    expect(dataRow).toBeDefined()

    client.close()
  })
})
