/**
 * tcp proxy that makes pglite speak postgresql wire protocol.
 *
 * handles the postgresql wire protocol directly using raw tcp sockets,
 * avoiding pg-gateway's Duplex.toWeb() which deadlocks under concurrent
 * connections with large responses.
 *
 * regular connections: forwarded to pglite via execProtocolRaw()
 * replication connections: intercepted, replication protocol faked
 *
 * each "database" (postgres, zero_cvr, zero_cdb) maps to its own pglite
 * instance with independent transaction context, preventing cross-database
 * query interleaving that causes CVR concurrent modification errors.
 */

import { createServer, type Server, type Socket } from 'node:net'

import { log } from './log.js'
import { Mutex } from './mutex.js'
import { handleReplicationQuery, handleStartReplication } from './replication/handler.js'

import type { ZeroLiteConfig } from './config.js'
import type { PGliteInstances } from './pglite-manager.js'
import type { PGlite } from '@electric-sql/pglite'

// clean version string: strip emscripten compiler info that breaks pg_restore/pg_dump
const PG_VERSION_STRING =
  "'PostgreSQL 16.4 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 12.2.0, 64-bit'"

// query rewrites: make pglite look like real postgres with logical replication
const QUERY_REWRITES: Array<{ match: RegExp; replace: string }> = [
  // version() — return a standard-looking version string instead of the emscripten one
  {
    match: /\bversion\(\)/gi,
    replace: PG_VERSION_STRING,
  },
  // wal_level check
  {
    match: /current_setting\s*\(\s*'wal_level'\s*\)/gi,
    replace: "'logical'::text",
  },
  // strip READ ONLY from BEGIN (pglite is single-session, no read-only transactions)
  {
    match: /\bREAD\s+ONLY\b/gi,
    replace: '',
  },
  // strip ISOLATION LEVEL from any query (pglite is single-session, isolation is meaningless)
  // catches: SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, BEGIN ISOLATION LEVEL SERIALIZABLE, etc.
  {
    match:
      /\bISOLATION\s+LEVEL\s+(SERIALIZABLE|REPEATABLE\s+READ|READ\s+COMMITTED|READ\s+UNCOMMITTED)\b/gi,
    replace: '',
  },
  // strip bare SET TRANSACTION (after ISOLATION LEVEL is removed, this becomes a no-op statement)
  {
    match: /\bSET\s+TRANSACTION\s*;/gi,
    replace: ';',
  },
  // redirect pg_replication_slots to our fake table
  {
    match: /\bpg_replication_slots\b/g,
    replace: 'public._zero_replication_slots',
  },
]

// parameter status messages sent during connection handshake
// pg_restore and other tools read these to determine server capabilities
const SERVER_PARAMS: [string, string][] = [
  ['server_version', '16.4'],
  ['server_encoding', 'UTF8'],
  ['client_encoding', 'UTF8'],
  ['DateStyle', 'ISO, MDY'],
  ['integer_datetimes', 'on'],
  ['standard_conforming_strings', 'on'],
  ['TimeZone', 'UTC'],
  ['IntervalStyle', 'postgres'],
]

// queries to intercept and return no-op success (synthetic SET response)
// pglite rejects SET TRANSACTION if any query (e.g. SET search_path) ran first
const NOOP_QUERY_PATTERNS: RegExp[] = [/^\s*SET\s+TRANSACTION\b/i, /^\s*SET\s+SESSION\b/i]

// ── wire protocol helpers ──

function buildParameterStatus(name: string, value: string): Uint8Array {
  const encoder = new TextEncoder()
  const nameBytes = encoder.encode(name)
  const valueBytes = encoder.encode(value)
  const len = 4 + nameBytes.length + 1 + valueBytes.length + 1
  const buf = new Uint8Array(1 + len)
  buf[0] = 0x53 // 'S'
  new DataView(buf.buffer).setInt32(1, len)
  let pos = 5
  buf.set(nameBytes, pos)
  pos += nameBytes.length
  buf[pos++] = 0
  buf.set(valueBytes, pos)
  pos += valueBytes.length
  buf[pos] = 0
  return buf
}

function buildAuthOk(): Uint8Array {
  const buf = new Uint8Array(9)
  buf[0] = 0x52 // 'R' AuthenticationOk
  new DataView(buf.buffer).setInt32(1, 8)
  new DataView(buf.buffer).setInt32(5, 0) // auth ok
  return buf
}

function buildAuthCleartextPassword(): Uint8Array {
  const buf = new Uint8Array(9)
  buf[0] = 0x52 // 'R'
  new DataView(buf.buffer).setInt32(1, 8)
  new DataView(buf.buffer).setInt32(5, 3) // cleartext password
  return buf
}

function buildBackendKeyData(): Uint8Array {
  const buf = new Uint8Array(13)
  buf[0] = 0x4b // 'K'
  new DataView(buf.buffer).setInt32(1, 12)
  new DataView(buf.buffer).setInt32(5, process.pid)
  new DataView(buf.buffer).setInt32(9, 0)
  return buf
}

function buildReadyForQuery(status: number = 0x49): Uint8Array {
  const buf = new Uint8Array(6)
  buf[0] = 0x5a // 'Z'
  new DataView(buf.buffer).setInt32(1, 5)
  buf[5] = status // 'I' = idle
  return buf
}

function buildErrorResponse(message: string): Uint8Array {
  const encoder = new TextEncoder()
  const msgBytes = encoder.encode(message)
  // S(ERROR) + C(code) + M(message) + terminator
  const sField = new Uint8Array([0x53, ...encoder.encode('ERROR'), 0])
  const cField = new Uint8Array([0x43, ...encoder.encode('08006'), 0])
  const mField = new Uint8Array([0x4d, ...msgBytes, 0])
  const terminator = new Uint8Array([0])
  const bodyLen = 4 + sField.length + cField.length + mField.length + terminator.length
  const buf = new Uint8Array(1 + bodyLen)
  buf[0] = 0x45 // 'E'
  new DataView(buf.buffer).setInt32(1, bodyLen)
  let pos = 5
  buf.set(sField, pos)
  pos += sField.length
  buf.set(cField, pos)
  pos += cField.length
  buf.set(mField, pos)
  pos += mField.length
  buf.set(terminator, pos)
  return buf
}

// ── query helpers ──

function extractParseQuery(data: Uint8Array): string | null {
  if (data[0] !== 0x50) return null
  let offset = 5
  while (offset < data.length && data[offset] !== 0) offset++
  offset++
  const queryStart = offset
  while (offset < data.length && data[offset] !== 0) offset++
  return new TextDecoder().decode(data.subarray(queryStart, offset))
}

function rebuildParseMessage(data: Uint8Array, newQuery: string): Uint8Array {
  let offset = 5
  while (offset < data.length && data[offset] !== 0) offset++
  const nameEnd = offset + 1
  const nameBytes = data.subarray(5, nameEnd)

  offset = nameEnd
  while (offset < data.length && data[offset] !== 0) offset++
  offset++

  const suffix = data.subarray(offset)
  const encoder = new TextEncoder()
  const queryBytes = encoder.encode(newQuery)

  const totalLen = 4 + nameBytes.length + queryBytes.length + 1 + suffix.length
  const result = new Uint8Array(1 + totalLen)
  const dv = new DataView(result.buffer)
  result[0] = 0x50
  dv.setInt32(1, totalLen)
  let pos = 5
  result.set(nameBytes, pos)
  pos += nameBytes.length
  result.set(queryBytes, pos)
  pos += queryBytes.length
  result[pos++] = 0
  result.set(suffix, pos)
  return result
}

function rebuildSimpleQuery(newQuery: string): Uint8Array {
  const encoder = new TextEncoder()
  const queryBytes = encoder.encode(newQuery + '\0')
  const buf = new Uint8Array(5 + queryBytes.length)
  buf[0] = 0x51
  new DataView(buf.buffer).setInt32(1, 4 + queryBytes.length)
  buf.set(queryBytes, 5)
  return buf
}

function interceptQuery(data: Uint8Array): Uint8Array {
  const msgType = data[0]

  if (msgType === 0x51) {
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
    const len = view.getInt32(1)
    let query = new TextDecoder().decode(data.subarray(5, 1 + len - 1)).replace(/\0$/, '')

    let modified = false
    for (const rw of QUERY_REWRITES) {
      if (rw.match.test(query)) {
        query = query.replace(rw.match, rw.replace)
        modified = true
        rw.match.lastIndex = 0
      }
      rw.match.lastIndex = 0
    }

    if (modified) {
      return rebuildSimpleQuery(query)
    }
  } else if (msgType === 0x50) {
    const query = extractParseQuery(data)
    if (query) {
      let newQuery = query
      let modified = false
      for (const rw of QUERY_REWRITES) {
        if (rw.match.test(newQuery)) {
          newQuery = newQuery.replace(rw.match, rw.replace)
          modified = true
          rw.match.lastIndex = 0
        }
        rw.match.lastIndex = 0
      }
      if (modified) {
        return rebuildParseMessage(data, newQuery)
      }
    }
  }

  return data
}

function isNoopQuery(data: Uint8Array): boolean {
  let query: string | null = null
  if (data[0] === 0x51) {
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
    const len = view.getInt32(1)
    query = new TextDecoder().decode(data.subarray(5, 1 + len - 1)).replace(/\0$/, '')
  } else if (data[0] === 0x50) {
    query = extractParseQuery(data)
  }
  if (!query) return false
  return NOOP_QUERY_PATTERNS.some((p) => p.test(query!))
}

function buildSetCompleteResponse(): Uint8Array {
  const encoder = new TextEncoder()
  const tag = encoder.encode('SET\0')
  const cc = new Uint8Array(1 + 4 + tag.length)
  cc[0] = 0x43
  new DataView(cc.buffer).setInt32(1, 4 + tag.length)
  cc.set(tag, 5)

  const rfq = new Uint8Array(6)
  rfq[0] = 0x5a
  new DataView(rfq.buffer).setInt32(1, 5)
  rfq[5] = 0x54 // 'T' = in transaction

  const result = new Uint8Array(cc.length + rfq.length)
  result.set(cc, 0)
  result.set(rfq, cc.length)
  return result
}

function buildParseCompleteResponse(): Uint8Array {
  const pc = new Uint8Array(5)
  pc[0] = 0x31 // ParseComplete
  new DataView(pc.buffer).setInt32(1, 4)
  return pc
}

function stripReadyForQuery(data: Uint8Array): Uint8Array {
  if (data.length === 0) return data

  const parts: Uint8Array[] = []
  let offset = 0
  while (offset < data.length) {
    const msgType = data[offset]
    if (offset + 5 > data.length) break
    const msgLen = new DataView(data.buffer, data.byteOffset + offset + 1).getInt32(0)
    const totalLen = 1 + msgLen

    if (msgType !== 0x5a) {
      parts.push(data.subarray(offset, offset + totalLen))
    }

    offset += totalLen
  }

  if (parts.length === 0) return new Uint8Array(0)
  if (parts.length === 1) return parts[0]

  const total = parts.reduce((sum, p) => sum + p.length, 0)
  const result = new Uint8Array(total)
  let pos = 0
  for (const p of parts) {
    result.set(p, pos)
    pos += p.length
  }
  return result
}

// ── socket write with backpressure ──

function socketWrite(socket: Socket, data: Uint8Array): Promise<void> {
  if (data.length === 0 || socket.destroyed) return Promise.resolve()
  return new Promise<void>((resolve, reject) => {
    const ok = socket.write(data as any, (err) => (err ? reject(err) : resolve()))
    // if buffer is full, the callback still fires when flushed
    if (!ok) void 0
  })
}

// ── startup handshake ──

// parse startup message from raw bytes.
// handles SSLRequest (8 bytes, code 80877103) and StartupMessage.
function parseStartupMessage(buf: Buffer): {
  isSSL: boolean
  params: Record<string, string>
} {
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength)
  const len = dv.getInt32(0)
  const code = dv.getInt32(4)

  // SSL request: length=8, code=80877103
  if (len === 8 && code === 80877103) {
    return { isSSL: true, params: {} }
  }

  // startup message: length, protocol(196608=3.0), then key=value pairs
  const params: Record<string, string> = {}
  let offset = 8
  while (offset < len) {
    const keyStart = offset
    while (offset < buf.length && buf[offset] !== 0) offset++
    const key = buf.subarray(keyStart, offset).toString()
    offset++
    if (!key) break // double-null = end of params
    const valStart = offset
    while (offset < buf.length && buf[offset] !== 0) offset++
    params[key] = buf.subarray(valStart, offset).toString()
    offset++
  }

  return { isSSL: false, params }
}

// read exactly `n` bytes from socket
function readBytes(socket: Socket, n: number): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    let collected = Buffer.alloc(0)

    const onData = (chunk: Buffer) => {
      collected = Buffer.concat([collected, chunk])
      if (collected.length >= n) {
        socket.removeListener('data', onData)
        socket.removeListener('error', onError)
        socket.removeListener('close', onClose)
        socket.pause()
        resolve(collected)
      }
    }
    const onError = (err: Error) => {
      socket.removeListener('data', onData)
      socket.removeListener('close', onClose)
      reject(err)
    }
    const onClose = () => {
      socket.removeListener('data', onData)
      socket.removeListener('error', onError)
      reject(new Error('socket closed'))
    }

    socket.on('data', onData)
    socket.on('error', onError)
    socket.on('close', onClose)
    socket.resume()
  })
}

// perform the startup handshake (SSL negotiation, auth, parameter status)
async function performHandshake(
  socket: Socket,
  config: ZeroLiteConfig
): Promise<{ params: Record<string, string> }> {
  // read initial message length (first 4 bytes)
  let buf = await readBytes(socket, 8)

  // check for SSL request
  const startup = parseStartupMessage(buf)
  if (startup.isSSL) {
    // reject SSL, client will reconnect without it
    socket.write(Buffer.from('N'))
    buf = await readBytes(socket, 8)
  }

  // now we have startup message header - read the rest if needed
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength)
  const msgLen = dv.getInt32(0)
  if (buf.length < msgLen) {
    const rest = await readBytes(socket, msgLen - buf.length)
    buf = Buffer.concat([buf, rest])
  }

  const { params } = parseStartupMessage(buf)

  // request cleartext password
  socket.write(buildAuthCleartextPassword())

  // read password message: type(1) + len(4) + password + null
  const pwBuf = await readBytes(socket, 5)
  const pwDv = new DataView(pwBuf.buffer, pwBuf.byteOffset, pwBuf.byteLength)
  const pwLen = pwDv.getInt32(1)
  let fullPwBuf = pwBuf
  if (fullPwBuf.length < 1 + pwLen) {
    const rest = await readBytes(socket, 1 + pwLen - fullPwBuf.length)
    fullPwBuf = Buffer.concat([fullPwBuf, rest])
  }
  const password = fullPwBuf.subarray(5, 1 + pwLen - 1).toString()

  // validate credentials
  if (params.user !== config.pgUser || password !== config.pgPassword) {
    socket.write(buildErrorResponse('authentication failed'))
    socket.write(buildReadyForQuery())
    socket.destroy()
    throw new Error('auth failed')
  }

  // auth ok
  socket.write(buildAuthOk())

  // send parameter status messages
  for (const [name, value] of SERVER_PARAMS) {
    socket.write(buildParameterStatus(name, value))
  }

  // backend key data
  socket.write(buildBackendKeyData())

  // ready for query
  socket.write(buildReadyForQuery())

  return { params }
}

// ── message loop ──

// process messages from a connected, authenticated client.
// uses callback-based 'data' events instead of async iterators
// for reliable behavior across runtimes (node.js, bun).
function messageLoop(
  socket: Socket,
  db: PGlite,
  mutex: Mutex,
  isReplicationConnection: boolean,
  replicationDb: PGlite,
  replicationMutex: Mutex
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    let buffer: Buffer = Buffer.alloc(0)
    let processing = false

    async function processBuffer() {
      if (processing) return
      processing = true
      socket.pause()

      try {
        while (buffer.length >= 5) {
          const msgType = buffer[0]
          const dv = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength)
          const msgLen = dv.getInt32(1)
          const totalLen = 1 + msgLen

          if (buffer.length < totalLen) break // need more data

          // copy message out before modifying buffer
          const message = new Uint8Array(
            buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + totalLen)
          )
          buffer = buffer.subarray(totalLen)

          // handle Terminate message
          if (msgType === 0x58) {
            resolve()
            return
          }

          // handle replication connections
          if (isReplicationConnection) {
            await handleReplicationMsg(message, socket, replicationDb, replicationMutex)
            continue
          }

          // handle regular messages
          await handleRegularMessage(message, socket, db, mutex)
        }
      } catch (err) {
        reject(err)
        return
      }

      processing = false
      socket.resume()
    }

    socket.on('data', (chunk: Buffer) => {
      buffer = buffer.length > 0 ? Buffer.concat([buffer, chunk]) : chunk
      processBuffer()
    })

    socket.on('end', () => resolve())
    socket.on('error', (err) => reject(err))
    socket.on('close', () => resolve())

    socket.resume()
  })
}

async function handleRegularMessage(
  data: Uint8Array,
  socket: Socket,
  db: PGlite,
  mutex: Mutex
): Promise<void> {
  // check for no-op queries
  if (isNoopQuery(data)) {
    if (data[0] === 0x51) {
      await socketWrite(socket, buildSetCompleteResponse())
      return
    } else if (data[0] === 0x50) {
      await socketWrite(socket, buildParseCompleteResponse())
      return
    }
  }

  // intercept and rewrite queries
  data = interceptQuery(data)

  // serialize pglite access
  await mutex.acquire()
  let result: Uint8Array
  try {
    result = await db.execProtocolRaw(data, { throwOnError: false })
  } catch (err) {
    mutex.release()
    throw err
  }

  // strip ReadyForQuery from non-Sync/non-SimpleQuery responses
  if (data[0] !== 0x53 && data[0] !== 0x51) {
    result = stripReadyForQuery(result)
  }

  mutex.release()

  // write response directly to socket
  await socketWrite(socket, result)
}

async function handleReplicationMsg(
  data: Uint8Array,
  socket: Socket,
  db: PGlite,
  mutex: Mutex
): Promise<void> {
  if (data[0] !== 0x51) return

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
  const len = view.getInt32(1)
  const query = new TextDecoder().decode(data.subarray(5, 1 + len - 1)).replace(/\0$/, '')
  const upper = query.trim().toUpperCase()

  log.debug.proxy(`repl query: ${query.slice(0, 200)}`)

  if (upper.startsWith('START_REPLICATION')) {
    const writer = {
      write(chunk: Uint8Array) {
        if (!socket.destroyed) {
          socket.write(chunk)
        }
      },
    }

    // drain incoming standby status updates
    socket.on('data', (_chunk: Buffer) => {})
    socket.on('close', () => socket.destroy())

    // this runs indefinitely until the socket closes
    await handleStartReplication(query, writer, db, mutex).catch((err) => {
      log.debug.proxy(`replication stream ended: ${err}`)
    })
    return
  }

  // handle replication queries + fallthrough to pglite
  await mutex.acquire()
  try {
    const response = await handleReplicationQuery(query, db)
    if (response) {
      await socketWrite(socket, response)
      return
    }

    // apply query rewrites before forwarding
    data = interceptQuery(data)

    const result = await db.execProtocolRaw(data, { throwOnError: false })
    await socketWrite(socket, result)
  } finally {
    mutex.release()
  }
}

// ── main entry point ──

export async function startPgProxy(
  dbInput: PGlite | PGliteInstances,
  config: ZeroLiteConfig
): Promise<Server> {
  // normalize input: single PGlite instance = use it for all databases (backwards compat for tests)
  const instances: PGliteInstances =
    'postgres' in dbInput
      ? (dbInput as PGliteInstances)
      : { postgres: dbInput as PGlite, cvr: dbInput as PGlite, cdb: dbInput as PGlite }

  // per-instance mutexes for serializing pglite access
  const mutexes = {
    postgres: new Mutex(),
    cvr: new Mutex(),
    cdb: new Mutex(),
  }
  function getDbContext(dbName: string): { db: PGlite; mutex: Mutex } {
    if (dbName === 'zero_cvr') return { db: instances.cvr, mutex: mutexes.cvr }
    if (dbName === 'zero_cdb') return { db: instances.cdb, mutex: mutexes.cdb }
    return { db: instances.postgres, mutex: mutexes.postgres }
  }

  const server = createServer(async (socket: Socket) => {
    socket.setKeepAlive(true, 30000)
    socket.setTimeout(0)
    socket.setNoDelay(true)

    let dbName = 'postgres'
    let isReplicationConnection = false

    try {
      // perform startup handshake
      const { params } = await performHandshake(socket, config)

      dbName = params.database || 'postgres'
      isReplicationConnection = params.replication === 'database'

      log.debug.proxy(
        `connection: db=${dbName} user=${params.user} replication=${params.replication || 'none'}`
      )

      const { db } = getDbContext(dbName)
      await db.waitReady

      // clean up pglite transaction state when client disconnects
      socket.on('close', async () => {
        const { db: closeDb, mutex: closeMutex } = getDbContext(dbName)
        await closeMutex.acquire()
        try {
          await closeDb.exec('ROLLBACK')
        } catch {
          // no transaction to rollback
        } finally {
          closeMutex.release()
        }
      })

      // enter message processing loop
      const { db: msgDb, mutex: msgMutex } = getDbContext(dbName)
      await messageLoop(
        socket,
        msgDb,
        msgMutex,
        isReplicationConnection,
        instances.postgres,
        mutexes.postgres
      )
    } catch (err) {
      // connection error during handshake or message loop
      if (!socket.destroyed) {
        socket.destroy()
      }
    }
  })

  return new Promise((resolve, reject) => {
    server.listen(config.pgPort, '127.0.0.1', () => {
      log.debug.proxy(`listening on port ${config.pgPort}`)
      resolve(server)
    })
    server.on('error', reject)
  })
}
