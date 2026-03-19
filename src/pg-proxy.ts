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
import {
  handleReplicationQuery,
  handleStartReplication,
  signalReplicationChange,
} from './replication/handler.js'

import type { ZeroLiteConfig } from './config.js'
import type { PGliteInstances } from './pglite-manager.js'
import type { PGlite } from '@electric-sql/pglite'

// shared encoder/decoder instances
const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

// schema query cache: identical information_schema/catalog queries from multiple
// zero-cache clients are deduplicated. first query executes, all others get cached result.
interface CachedQueryResult {
  result: Uint8Array
  expiresAt: number
}
const schemaQueryCache = new Map<string, CachedQueryResult>()
const schemaQueryInFlight = new Map<string, Promise<Uint8Array>>()
const SCHEMA_CACHE_TTL_MS = 30_000

// performance tracking
const proxyStats = { totalWaitMs: 0, totalExecMs: 0, count: 0, batches: 0 }

function isCacheableQuery(query: string): boolean {
  const q = query.trimStart().toLowerCase()
  return (
    (q.includes('information_schema.') ||
      q.includes('pg_catalog.') ||
      q.includes('pg_tables') ||
      q.includes('pg_namespace') ||
      q.includes('pg_class') ||
      q.includes('pg_attribute') ||
      q.includes('pg_type') ||
      q.includes('pg_publication')) &&
    !q.startsWith('insert') &&
    !q.startsWith('update') &&
    !q.startsWith('delete') &&
    !q.startsWith('create') &&
    !q.startsWith('alter') &&
    !q.startsWith('drop')
  )
}

function extractQueryText(data: Uint8Array): string | null {
  if (data[0] === 0x51) {
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
    const len = view.getInt32(1)
    return textDecoder.decode(data.subarray(5, 1 + len - 1)).replace(/\0$/, '')
  }
  if (data[0] === 0x50) {
    return extractParseQuery(data)
  }
  return null
}

function invalidateSchemaCache() {
  schemaQueryCache.clear()
}

// abort previous replication handler when a new one starts
let abortPreviousReplication: (() => void) | null = null

// clean version string: strip emscripten compiler info that breaks pg_restore/pg_dump
const PG_VERSION_STRING =
  "'PostgreSQL 17.4 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 12.2.0, 64-bit'"

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
  // redirect pg_replication_slots to our fake table in _orez schema
  {
    match: /\bpg_replication_slots\b/g,
    replace: '_orez._zero_replication_slots',
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
  const encoder = textEncoder
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

// ping queries (SELECT 1, SELECT 2, etc.) — respond synthetically to avoid
// mutex contention during zero-cache connection warmup
const PING_QUERY_RE = /^\s*SELECT\s+(\d+)\s*$/i

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
  return textDecoder.decode(data.subarray(queryStart, offset))
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
  const encoder = textEncoder
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
  const encoder = textEncoder
  const queryBytes = encoder.encode(newQuery + '\0')
  const buf = new Uint8Array(5 + queryBytes.length)
  buf[0] = 0x51
  new DataView(buf.buffer).setInt32(1, 4 + queryBytes.length)
  buf.set(queryBytes, 5)
  return buf
}

// apply all rewrites in one pass, using replace directly (no separate test)
function applyRewrites(query: string): string {
  let result = query
  for (const rw of QUERY_REWRITES) {
    rw.match.lastIndex = 0
    result = result.replace(rw.match, rw.replace)
  }
  return result
}

/**
 * intercept and rewrite query messages to make pglite look like real postgres.
 */
function interceptQuery(data: Uint8Array): Uint8Array {
  const msgType = data[0]

  if (msgType === 0x51) {
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
    const len = view.getInt32(1)
    const original = textDecoder.decode(data.subarray(5, 1 + len - 1)).replace(/\0$/, '')
    const rewritten = applyRewrites(original)
    if (rewritten !== original) {
      return rebuildSimpleQuery(rewritten)
    }
  } else if (msgType === 0x50) {
    const original = extractParseQuery(data)
    if (original) {
      let rewritten = applyRewrites(original)
      // for extended protocol, noop queries must be rewritten to a harmless query
      // (can't return synthetic responses because they're part of a pipeline batch)
      if (NOOP_QUERY_PATTERNS.some((p) => p.test(rewritten))) {
        rewritten = 'SELECT 1'
      }
      if (rewritten !== original) {
        return rebuildParseMessage(data, rewritten)
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
    query = textDecoder.decode(data.subarray(5, 1 + len - 1)).replace(/\0$/, '')
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
  const encoder = textEncoder
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
 * build a synthetic response for SELECT <n> (ping queries).
 * returns RowDescription + DataRow + CommandComplete + ReadyForQuery
 * without touching PGlite or the mutex.
 */
function buildSelectIntResponse(val: string): Uint8Array {
  const enc = textEncoder
  const parts: Uint8Array[] = []

  // RowDescription: 1 column named "?column?" type int4 (oid 23)
  const colName = enc.encode('?column?\0')
  const rdLen = 4 + 2 + colName.length + 4 + 2 + 4 + 2 + 4 + 2
  const rd = new Uint8Array(1 + rdLen)
  const rdv = new DataView(rd.buffer)
  rd[0] = 0x54
  rdv.setInt32(1, rdLen)
  rdv.setInt16(5, 1)
  rd.set(colName, 7)
  let p = 7 + colName.length
  rdv.setInt32(p, 0)
  p += 4 // tableOid
  rdv.setInt16(p, 0)
  p += 2 // colAttr
  rdv.setInt32(p, 23)
  p += 4 // typeOid (int4)
  rdv.setInt16(p, 4)
  p += 2 // typeLen
  rdv.setInt32(p, -1)
  p += 4 // typeMod
  rdv.setInt16(p, 0) // format (text)
  parts.push(rd)

  // DataRow: 1 column with the value
  const valBytes = enc.encode(val)
  const drLen = 4 + 2 + 4 + valBytes.length
  const dr = new Uint8Array(1 + drLen)
  const drv = new DataView(dr.buffer)
  dr[0] = 0x44
  drv.setInt32(1, drLen)
  drv.setInt16(5, 1)
  drv.setInt32(7, valBytes.length)
  dr.set(valBytes, 11)
  parts.push(dr)

  // CommandComplete
  const tag = enc.encode('SELECT 1\0')
  const cc = new Uint8Array(1 + 4 + tag.length)
  cc[0] = 0x43
  new DataView(cc.buffer).setInt32(1, 4 + tag.length)
  cc.set(tag, 5)
  parts.push(cc)

  // ReadyForQuery
  const rfq = new Uint8Array(6)
  rfq[0] = 0x5a
  new DataView(rfq.buffer).setInt32(1, 5)
  rfq[5] = 0x49 // 'I' idle
  parts.push(rfq)

  const total = parts.reduce((s, p) => s + p.length, 0)
  const result = new Uint8Array(total)
  let off = 0
  for (const part of parts) {
    result.set(part, off)
    off += part.length
  }
  return result
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
 * extract ReadyForQuery status byte from a response.
 * returns the status: 'I' (0x49) idle, 'T' (0x54) in transaction, 'E' (0x45) error.
 * returns null if no ReadyForQuery found.
 */
function getReadyForQueryStatus(data: Uint8Array): number | null {
  let offset = 0
  let lastStatus: number | null = null
  while (offset < data.length) {
    if (offset + 5 > data.length) break
    const msgLen = readInt32BE(data, offset + 1)
    const totalLen = 1 + msgLen
    if (totalLen <= 0 || offset + totalLen > data.length) break
    if (data[offset] === 0x5a && totalLen >= 6) {
      lastStatus = data[offset + 5]
    }
    offset += totalLen
  }
  return lastStatus
}

/**
 * per-instance transaction state tracking.
 * pglite is single-connection: if one TCP "connection" leaves an aborted transaction,
 * it pollutes ALL other connections sharing the same pglite instance.
 * track which socket owns the current transaction so we can auto-ROLLBACK when a
 * DIFFERENT connection encounters the stale aborted state, while still letting the
 * ORIGINAL connection handle its own errors (e.g. ROLLBACK TO SAVEPOINT).
 */
interface PgLiteTxState {
  status: number // 0x49='I' idle, 0x54='T' in-transaction, 0x45='E' aborted
  owner: Socket | null // the socket that started the current transaction
}

// pglite warnings to suppress (benign, but noisy)
// 25001: "there is already a transaction in progress"
// 25P01: "there is no transaction in progress"
// 55000: "wal_level is insufficient to publish logical changes"
//        pglite internally tries to create a publication for change streaming, but embedded
//        pglite doesn't support wal_level=logical (server-level postgres config). the
//        change-streamer still works because it falls back to polling.
const SUPPRESS_NOTICE_CODES = new Set(['25001', '25P01', '55000'])

/**
 * extract SQLSTATE code from a NoticeResponse message.
 * returns null if not a NoticeResponse or code not found.
 */
function extractNoticeCode(
  data: Uint8Array,
  offset: number,
  totalLen: number
): string | null {
  if (data[offset] !== 0x4e) return null // not a NoticeResponse

  let pos = offset + 5 // skip type byte + length
  const end = offset + totalLen

  while (pos < end) {
    const fieldType = data[pos++]
    if (fieldType === 0) break // terminator

    // find null-terminated string
    const strStart = pos
    while (pos < end && data[pos] !== 0) pos++
    if (pos >= end) break

    if (fieldType === 0x43) {
      // 'C' = SQLSTATE code
      return textDecoder.decode(data.subarray(strStart, pos))
    }
    pos++ // skip null terminator
  }
  return null
}

/**
 * single-pass response message filter. strips ReadyForQuery messages (when
 * stripRfq=true) and benign transaction state warnings in one scan.
 */
function stripResponseMessages(data: Uint8Array, stripRfq: boolean): Uint8Array {
  if (data.length === 0) return data

  const parts: Uint8Array[] = []
  let offset = 0
  let stripped = false

  while (offset < data.length) {
    const msgType = data[offset]
    if (offset + 5 > data.length) break
    const msgLen = readInt32BE(data, offset + 1)
    const totalLen = 1 + msgLen

    if (totalLen <= 0 || offset + totalLen > data.length) break

    // strip ReadyForQuery (0x5a) when requested
    if (stripRfq && msgType === 0x5a) {
      stripped = true
    }
    // strip benign transaction state notices
    else {
      const code = extractNoticeCode(data, offset, totalLen)
      if (code && SUPPRESS_NOTICE_CODES.has(code)) {
        stripped = true
      } else {
        parts.push(data.subarray(offset, offset + totalLen))
      }
    }

    offset += totalLen
  }

  if (!stripped) return data
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

  // per-instance transaction state: tracks which socket owns the current transaction
  // so we can auto-ROLLBACK stale aborted transactions from other connections
  const txStates: Record<string, PgLiteTxState> = {
    postgres: { status: 0x49, owner: null },
    cvr: { status: 0x49, owner: null },
    cdb: { status: 0x49, owner: null },
  }

  // helper to get instance + mutex + tx state for a database name
  function getDbContext(dbName: string): {
    db: PGlite
    mutex: Mutex
    txState: PgLiteTxState
  } {
    if (dbName === 'zero_cvr')
      return { db: instances.cvr, mutex: mutexes.cvr, txState: txStates.cvr }
    if (dbName === 'zero_cdb')
      return { db: instances.cdb, mutex: mutexes.cdb, txState: txStates.cdb }
    return { db: instances.postgres, mutex: mutexes.postgres, txState: txStates.postgres }
  }

  // signal replication handler after writes complete.
  // 8ms trailing-edge debounce gives the PushProcessor time to read, parse,
  // and confirm the mutation response before the replication handler streams
  // the same change. pg_notify alone is too slow (PGlite notification delivery
  // is delayed by mutex contention). with real postgres, WAL→logical replication
  // is naturally slower than the response path so this race never occurs.
  // tested: setImmediate fails, 4ms fails, 8ms passes, pg_notify-only too slow.
  let signalTimer: ReturnType<typeof setTimeout> | null = null
  function signalWrite() {
    if (signalTimer) clearTimeout(signalTimer)
    signalTimer = setTimeout(() => {
      signalTimer = null
      signalReplicationChange()
    }, 8)
  }

  // pg-gateway uses Node WebStream adapters internally. when zero-cache
  // closes connections during startup, the WebStream write() throws EPIPE
  // as an unhandled promise rejection that escapes socket error handlers.
  // catch these globally while the proxy is running.
  const suppressSocketErrors = (err: unknown) => {
    const code = (err as NodeJS.ErrnoException)?.code
    if (code === 'EPIPE' || code === 'ECONNRESET') return
    const msg = err instanceof Error ? err.message : String(err)
    if (msg.includes('ended by the other party')) return
    // re-throw non-socket errors
    throw err
  }
  process.on('uncaughtException', suppressSocketErrors)
  process.on('unhandledRejection', suppressSocketErrors)

  const server = createServer(async (socket: Socket) => {
    // also catch at socket level for errors that don't escape to process
    socket.on('error', (err: NodeJS.ErrnoException) => {
      if (err.code === 'EPIPE' || err.code === 'ECONNRESET') return
      log.proxy(`socket error: ${err.message}`)
    })
    // prevent idle timeouts from killing connections
    socket.setKeepAlive(true, 30000)
    socket.setTimeout(0)
    // disable Nagle's algorithm — send every response immediately.
    // critical for wire protocol where each message is a complete unit.
    socket.setNoDelay(true)

    let dbName = 'postgres'
    let isReplicationConnection = false
    // track extended protocol writes (Parse with INSERT/UPDATE/DELETE/COPY/TRUNCATE)
    // so we can signal replication on Sync (0x53) after the pipeline completes
    let extWritePending = false
    // hold mutex across entire extended protocol pipeline (Parse→Sync).
    // prevents other connections from interleaving and corrupting PGlite's
    // unnamed portal/statement state during the pipeline.
    let pipelineMutexHeld = false
    // clean up pglite transaction state when a client disconnects
    socket.on('close', async () => {
      // replication sockets don't own a transaction — skip ROLLBACK
      if (isReplicationConnection) return
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
        serverVersion: '17.4',
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
              const query = textDecoder
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

          const msgType = data[0]
          const { db, mutex, txState } = getDbContext(dbName)

          // extended protocol pipeline: hold mutex across Parse→Sync to prevent
          // other connections from interleaving and corrupting unnamed portal state.
          // 0x50=Parse, 0x42=Bind, 0x44=Describe, 0x45=Execute, 0x43=Close, 0x48=Flush
          const isExtendedMsg =
            msgType === 0x50 ||
            msgType === 0x42 ||
            msgType === 0x44 ||
            msgType === 0x45 ||
            msgType === 0x43 ||
            msgType === 0x48
          const isSyncInPipeline = msgType === 0x53 && pipelineMutexHeld

          if (isExtendedMsg || isSyncInPipeline) {
            // acquire mutex on first message of pipeline
            if (!pipelineMutexHeld) {
              const t0 = performance.now()
              await mutex.acquire()
              proxyStats.totalWaitMs += performance.now() - t0
              pipelineMutexHeld = true
              // auto-rollback stale transactions from other connections
              if (txState.status === 0x45 && txState.owner !== socket) {
                try {
                  await db.exec('ROLLBACK')
                } catch {}
                txState.status = 0x49
                txState.owner = null
              }
            }

            // detect extended protocol writes for replication signaling
            if (dbName === 'postgres' && msgType === 0x50) {
              const q = extractParseQuery(data)?.trimStart().toLowerCase()
              if (q && /^(insert|update|delete|copy|truncate)/.test(q)) {
                extWritePending = true
                log.debug.proxy(`ext-write: detected ${q.slice(0, 40)}`)
              }
            }

            // apply query rewrites
            data = interceptQuery(data)

            const t1 = performance.now()
            let result: Uint8Array
            try {
              result = await db.execProtocolRaw(data, { syncToFs: false })
            } catch (err) {
              mutex.release()
              pipelineMutexHeld = false
              throw err
            }
            const t2 = performance.now()
            proxyStats.totalExecMs += t2 - t1
            proxyStats.count++

            // update transaction state
            const rfqStatus = getReadyForQueryStatus(result)
            if (rfqStatus !== null) {
              txState.status = rfqStatus
              txState.owner = rfqStatus === 0x49 ? null : socket
            }

            // release mutex on Sync (end of pipeline)
            if (msgType === 0x53) {
              mutex.release()
              pipelineMutexHeld = false
              proxyStats.batches++

              // signal replication handler on postgres writes
              if (dbName === 'postgres' && extWritePending) {
                extWritePending = false
                signalWrite()
              }
            } else {
              // strip ReadyForQuery from non-Sync pipeline messages
              result = stripResponseMessages(result, true)
            }

            if (proxyStats.count % 200 === 0) {
              log.debug.proxy(
                `perf: ${proxyStats.count} ops (${proxyStats.batches} batches) | mutex ${proxyStats.totalWaitMs.toFixed(0)}ms | pglite ${proxyStats.totalExecMs.toFixed(0)}ms`
              )
            }

            return result
          }

          // Simple Query (0x51) or standalone Sync — per-message mutex

          // fast-path for ping queries (SELECT 1, SELECT 2, etc.)
          // zero-cache fires these in parallel during warmup — bypass mutex entirely
          if (msgType === 0x51) {
            const queryText = extractQueryText(data)
            if (queryText) {
              const pingMatch = queryText.match(PING_QUERY_RE)
              if (pingMatch) {
                return buildSelectIntResponse(pingMatch[1])
              }
            }
          }

          // check for no-op queries (only SimpleQuery has queries worth intercepting)
          if (isNoopQuery(data)) {
            if (msgType === 0x51) {
              return buildSetCompleteResponse()
            }
          }

          // intercept and rewrite queries
          data = interceptQuery(data)

          // cache Simple Query (0x51) schema queries
          const isSimpleQuery = msgType === 0x51
          const queryText = isSimpleQuery ? extractQueryText(data) : null
          const cacheable = queryText && isCacheableQuery(queryText)
          if (cacheable) {
            const cached = schemaQueryCache.get(queryText!)
            if (cached && Date.now() < cached.expiresAt) {
              return stripResponseMessages(cached.result, false)
            }
            const inflight = schemaQueryInFlight.get(queryText!)
            if (inflight) {
              return stripResponseMessages(await inflight, false)
            }
          }

          const execute = async (): Promise<Uint8Array> => {
            const t0 = performance.now()
            await mutex.acquire()
            if (txState.status === 0x45 && txState.owner !== socket) {
              try {
                await db.exec('ROLLBACK')
              } catch {}
              txState.status = 0x49
              txState.owner = null
            }
            const t1 = performance.now()
            let result: Uint8Array
            try {
              result = await db.execProtocolRaw(data, { syncToFs: false })
            } catch (err) {
              mutex.release()
              throw err
            }
            const rfqStatus = getReadyForQueryStatus(result)
            if (rfqStatus !== null) {
              txState.status = rfqStatus
              txState.owner = rfqStatus === 0x49 ? null : socket
            }
            const t2 = performance.now()
            mutex.release()
            proxyStats.totalWaitMs += t1 - t0
            proxyStats.totalExecMs += t2 - t1
            proxyStats.count++
            if (proxyStats.count % 200 === 0) {
              log.debug.proxy(
                `perf: ${proxyStats.count} ops (${proxyStats.batches} batches) | mutex ${proxyStats.totalWaitMs.toFixed(0)}ms | pglite ${proxyStats.totalExecMs.toFixed(0)}ms`
              )
            }
            return result
          }

          let result: Uint8Array
          if (cacheable) {
            const promise = execute()
            schemaQueryInFlight.set(queryText!, promise)
            try {
              result = await promise
              schemaQueryCache.set(queryText!, {
                result,
                expiresAt: Date.now() + SCHEMA_CACHE_TTL_MS,
              })
            } finally {
              schemaQueryInFlight.delete(queryText!)
            }
          } else {
            result = await execute()
            if (isSimpleQuery && queryText) {
              const q = queryText.trimStart().toLowerCase()
              if (
                q.startsWith('create') ||
                q.startsWith('alter') ||
                q.startsWith('drop')
              ) {
                invalidateSchemaCache()
              }
            }
          }

          const stripRfq = msgType !== 0x53 && msgType !== 0x51
          result = stripResponseMessages(result, stripRfq)

          // signal replication handler on postgres writes for instant sync
          if (dbName === 'postgres' && isSimpleQuery && queryText) {
            const q = queryText.trimStart().toLowerCase()
            if (
              q.startsWith('insert') ||
              q.startsWith('update') ||
              q.startsWith('delete') ||
              q.startsWith('copy') ||
              q.startsWith('truncate')
            ) {
              signalReplicationChange()
            }
          }

          return result
        },
      })
    } catch (err) {
      if (!socket.destroyed) {
        socket.destroy()
      }
    }
  })

  server.on('close', () => {
    process.removeListener('uncaughtException', suppressSocketErrors)
    process.removeListener('unhandledRejection', suppressSocketErrors)
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
  const query = textDecoder.decode(data.subarray(5, 1 + len - 1)).replace(/\0$/, '')
  const upper = query.trim().toUpperCase()

  // check if this is a START_REPLICATION command
  if (upper.startsWith('START_REPLICATION')) {
    await connection.detach()

    // abort any previous replication handler to prevent zombies
    if (abortPreviousReplication) {
      log.proxy('aborting previous replication handler')
      abortPreviousReplication()
    }

    let aborted = false
    const writer = {
      write(chunk: Uint8Array) {
        if (!socket.destroyed && !aborted) {
          socket.write(chunk)
        }
      },
      get closed() {
        return socket.destroyed || aborted
      },
    }

    const abort = () => {
      aborted = true
      // use end() instead of destroy() to flush any pending writes.
      // the first handler may have just written 1MB+ of WAL data that
      // hasn't been fully flushed to the network. destroy() would discard
      // buffered data, causing zero-cache to receive truncated/corrupt
      // WAL messages which breaks its internal state.
      if (!socket.destroyed) {
        socket.end()
      }
    }
    abortPreviousReplication = abort

    // drain incoming standby status updates
    socket.on('data', (_chunk: Buffer) => {})

    socket.on('close', abort)

    handleStartReplication(query, writer, db, mutex).catch((err) => {
      log.proxy(`replication stream ended: ${err}`)
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
    const result = await db.execProtocolRaw(data, {
      throwOnError: false,
    })
    return stripResponseMessages(result, false)
  } finally {
    mutex.release()
  }
}
