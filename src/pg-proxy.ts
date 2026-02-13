/**
 * tcp proxy that makes pglite speak postgresql wire protocol.
 *
 * uses pg-gateway to handle protocol lifecycle for regular connections,
 * and directly handles the raw socket for replication connections.
 *
 * regular connections: forwarded to pglite via execProtocolRaw()
 * replication connections: intercepted, replication protocol faked
 *
 * each "database" (postgres, zero_cvr, zero_cdb) maps to its own pglite
 * instance with independent transaction context, preventing cross-database
 * query interleaving that causes CVR concurrent modification errors.
 */

import { createServer, type Server, type Socket } from 'node:net'

import { fromNodeSocket } from 'pg-gateway/node'

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
  ['server_encoding', 'UTF8'],
  ['client_encoding', 'UTF8'],
  ['DateStyle', 'ISO, MDY'],
  ['integer_datetimes', 'on'],
  ['standard_conforming_strings', 'on'],
  ['TimeZone', 'UTC'],
  ['IntervalStyle', 'postgres'],
]

// build a ParameterStatus wire protocol message (type 'S', 0x53)
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

// queries to intercept and return no-op success (synthetic SET response)
// pglite rejects SET TRANSACTION if any query (e.g. SET search_path) ran first
const NOOP_QUERY_PATTERNS: RegExp[] = [/^\s*SET\s+TRANSACTION\b/i, /^\s*SET\s+SESSION\b/i]

/**
 * extract query text from a Parse message (0x50).
 */
function extractParseQuery(data: Uint8Array): string | null {
  if (data[0] !== 0x50) return null
  let offset = 5
  while (offset < data.length && data[offset] !== 0) offset++
  offset++
  const queryStart = offset
  while (offset < data.length && data[offset] !== 0) offset++
  return new TextDecoder().decode(data.subarray(queryStart, offset))
}

/**
 * rebuild a Parse message with a modified query string.
 */
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

/**
 * rebuild a Simple Query message with a modified query string.
 */
function rebuildSimpleQuery(newQuery: string): Uint8Array {
  const encoder = new TextEncoder()
  const queryBytes = encoder.encode(newQuery + '\0')
  const buf = new Uint8Array(5 + queryBytes.length)
  buf[0] = 0x51
  new DataView(buf.buffer).setInt32(1, 4 + queryBytes.length)
  buf.set(queryBytes, 5)
  return buf
}

/**
 * intercept and rewrite query messages to make pglite look like real postgres.
 */
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

/**
 * check if a query should be intercepted as a no-op.
 */
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

/**
 * build a synthetic "SET" command complete response.
 */
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

/**
 * build a synthetic ParseComplete response for extended protocol no-ops.
 */
function buildParseCompleteResponse(): Uint8Array {
  const pc = new Uint8Array(5)
  pc[0] = 0x31 // ParseComplete
  new DataView(pc.buffer).setInt32(1, 4)
  return pc
}

/** read a big-endian int32 from a Uint8Array at the given offset */
function readInt32BE(data: Uint8Array, offset: number): number {
  return (
    ((data[offset] << 24) >>> 0) +
    (data[offset + 1] << 16) +
    (data[offset + 2] << 8) +
    data[offset + 3]
  )
}

/**
 * strip ReadyForQuery messages from a response buffer.
 */
function stripReadyForQuery(data: Uint8Array): Uint8Array {
  if (data.length === 0) return data

  const parts: Uint8Array[] = []
  let offset = 0
  while (offset < data.length) {
    const msgType = data[offset]
    if (offset + 5 > data.length) break
    const msgLen = readInt32BE(data, offset + 1)
    const totalLen = 1 + msgLen

    if (totalLen <= 0 || offset + totalLen > data.length) break

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

  // helper to get instance + mutex for a database name
  function getDbContext(dbName: string): { db: PGlite; mutex: Mutex } {
    if (dbName === 'zero_cvr') return { db: instances.cvr, mutex: mutexes.cvr }
    if (dbName === 'zero_cdb') return { db: instances.cdb, mutex: mutexes.cdb }
    return { db: instances.postgres, mutex: mutexes.postgres }
  }

  const server = createServer(async (socket: Socket) => {
    // prevent idle timeouts from killing connections
    socket.setKeepAlive(true, 30000)
    socket.setTimeout(0)

    let dbName = 'postgres'
    let isReplicationConnection = false

    // clean up pglite transaction state when a client disconnects
    socket.on('close', async () => {
      try {
        const { db, mutex } = getDbContext(dbName)
        await mutex.acquire()
        try {
          await db.exec('ROLLBACK')
        } catch {
          // no transaction to rollback, or db is closed
        } finally {
          mutex.release()
        }
      } catch {
        // instance may have been replaced during reset, ignore
      }
    })

    try {
      const connection = await fromNodeSocket(socket, {
        serverVersion: '16.4',
        auth: {
          method: 'password',
          getClearTextPassword() {
            return config.pgPassword
          },
          validateCredentials(credentials: {
            username: string
            password: string
            clearTextPassword: string
          }) {
            return (
              credentials.password === credentials.clearTextPassword &&
              credentials.username === config.pgUser
            )
          },
        },

        // send ParameterStatus messages that standard postgres tools expect
        // pg-gateway sends server_version via the serverVersion option above,
        // but tools like pg_restore also need encoding, datestyle, etc.
        onAuthenticated() {
          for (const [name, value] of SERVER_PARAMS) {
            socket.write(buildParameterStatus(name, value))
          }
        },

        async onStartup(state) {
          const params = state.clientParams
          if (params?.replication === 'database') {
            isReplicationConnection = true
          }
          dbName = params?.database || 'postgres'
          log.debug.proxy(
            `connection: db=${dbName} user=${params?.user} replication=${params?.replication || 'none'}`
          )
          const { db } = getDbContext(dbName)
          await db.waitReady
        },

        async onMessage(data, state) {
          if (!state.isAuthenticated) return

          // handle replication connections (always go to postgres instance)
          if (isReplicationConnection) {
            if (data[0] === 0x51) {
              const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
              const len = view.getInt32(1)
              const query = new TextDecoder()
                .decode(data.subarray(5, 1 + len - 1))
                .replace(/\0$/, '')
              log.debug.proxy(`repl query: ${query.slice(0, 200)}`)
            }
            return handleReplicationMessage(
              data,
              socket,
              instances.postgres,
              mutexes.postgres,
              connection
            )
          }

          // check for no-op queries
          if (isNoopQuery(data)) {
            if (data[0] === 0x51) {
              return buildSetCompleteResponse()
            } else if (data[0] === 0x50) {
              return buildParseCompleteResponse()
            }
          }

          // intercept and rewrite queries
          data = interceptQuery(data)

          // message-level locking on the connection's pglite instance
          const { db, mutex } = getDbContext(dbName)
          await mutex.acquire()

          let result: Uint8Array
          try {
            result = await db.execProtocolRaw(data, {
              throwOnError: false,
            })
          } catch (err) {
            mutex.release()
            throw err
          }

          // strip ReadyForQuery from non-Sync/non-SimpleQuery responses
          if (data[0] !== 0x53 && data[0] !== 0x51) {
            result = stripReadyForQuery(result)
          }

          mutex.release()
          return result
        },
      })
    } catch (err) {
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

async function handleReplicationMessage(
  data: Uint8Array,
  socket: Socket,
  db: PGlite,
  mutex: Mutex,
  connection: Awaited<ReturnType<typeof fromNodeSocket>>
): Promise<Uint8Array | undefined> {
  if (data[0] !== 0x51) return undefined

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
  const len = view.getInt32(1)
  const query = new TextDecoder().decode(data.subarray(5, 1 + len - 1)).replace(/\0$/, '')
  const upper = query.trim().toUpperCase()

  // check if this is a START_REPLICATION command
  if (upper.startsWith('START_REPLICATION')) {
    await connection.detach()

    const writer = {
      write(chunk: Uint8Array) {
        if (!socket.destroyed) {
          socket.write(chunk)
        }
      },
    }

    // drain incoming standby status updates
    socket.on('data', (_chunk: Buffer) => {})

    socket.on('close', () => {
      socket.destroy()
    })

    handleStartReplication(query, writer, db, mutex).catch((err) => {
      log.debug.proxy(`replication stream ended: ${err}`)
    })
    return undefined
  }

  // handle replication queries + fallthrough to pglite, all under mutex
  await mutex.acquire()
  try {
    const response = await handleReplicationQuery(query, db)
    if (response) return response

    // apply query rewrites before forwarding
    data = interceptQuery(data)

    // fall through to pglite for unrecognized queries
    return await db.execProtocolRaw(data, {
      throwOnError: false,
    })
  } finally {
    mutex.release()
  }
}
