/**
 * browser proxy that makes pglite speak postgresql wire protocol.
 *
 * browser port of pg-proxy.ts — uses pg-gateway's web DuplexStream
 * instead of TCP sockets. accepts MessagePort connections from zero-cache.
 *
 * regular connections: forwarded to pglite via execProtocolRaw()
 * replication connections: intercepted, replication protocol faked
 *
 * each "database" (postgres, zero_cvr, zero_cdb) maps to its own pglite
 * instance with independent transaction context, preventing cross-database
 * query interleaving that causes CVR concurrent modification errors.
 */

import { PostgresConnection, type DuplexStream } from 'pg-gateway'

import { log } from './log.js'
import { Mutex } from './mutex.js'
import {
  handleReplicationQuery,
  handleStartReplication,
  signalReplicationChange,
} from './replication/handler.js'

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

// query classification helpers — operate on pre-normalized (trimmed+lowercased) query strings
const SCHEMA_QUERY_MARKERS = [
  'information_schema.',
  'pg_catalog.',
  'pg_tables',
  'pg_namespace',
  'pg_class',
  'pg_attribute',
  'pg_type',
  'pg_publication',
]
const WRITE_PREFIXES = ['insert', 'update', 'delete', 'copy', 'truncate']
const DDL_PREFIXES = ['create', 'alter', 'drop']
const MUTATING_PREFIXES = [...WRITE_PREFIXES, ...DDL_PREFIXES]

function isCacheableNormalized(q: string): boolean {
  // fast-fail: mutating queries are never cacheable
  for (const p of MUTATING_PREFIXES) {
    if (q.startsWith(p)) return false
  }
  // check if it touches schema/catalog tables
  for (const marker of SCHEMA_QUERY_MARKERS) {
    if (q.includes(marker)) return true
  }
  return false
}

function isWriteNormalized(q: string): boolean {
  for (const p of WRITE_PREFIXES) {
    if (q.startsWith(p)) return true
  }
  return false
}

function isDDLNormalized(q: string): boolean {
  for (const p of DDL_PREFIXES) {
    if (q.startsWith(p)) return true
  }
  return false
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
 * pglite is single-connection: if one connection leaves an aborted transaction,
 * it pollutes ALL other connections sharing the same pglite instance.
 * track which connection owns the current transaction so we can auto-ROLLBACK when a
 * DIFFERENT connection encounters the stale aborted state, while still letting the
 * ORIGINAL connection handle its own errors (e.g. ROLLBACK TO SAVEPOINT).
 */
interface PgLiteTxState {
  status: number // 0x49='I' idle, 0x54='T' in-transaction, 0x45='E' aborted
  owner: object | null // opaque connection identity token
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

/**
 * create a DuplexStream<Uint8Array> from a MessagePort.
 * readable receives Uint8Array messages from the port.
 * writable sends Uint8Array messages via the port.
 */
function messagePortToDuplexWithInject(port: MessagePort): {
  duplex: DuplexStream<Uint8Array>
  rawWrite: (data: Uint8Array) => void
  injectMessage: (data: Uint8Array) => void
} {
  let readController: ReadableStreamDefaultController<Uint8Array>
  let msgCount = 0
  const readable = new ReadableStream<Uint8Array>({
    start(controller) {
      readController = controller
      port.onmessage = (ev: MessageEvent) => {
        msgCount++
        if (ev.data instanceof ArrayBuffer) {
          controller.enqueue(new Uint8Array(ev.data))
        } else if (ev.data instanceof Uint8Array) {
          controller.enqueue(ev.data)
        }
      }
    },
    cancel() {
      port.close()
    },
  })

  const writable = new WritableStream<Uint8Array>({
    write(chunk) {
      const buf = chunk.buffer.slice(
        chunk.byteOffset,
        chunk.byteOffset + chunk.byteLength
      ) as ArrayBuffer
      port.postMessage(buf, [buf])
    },
    close() {
      port.close()
    },
  })

  const rawWrite = (data: Uint8Array) => {
    const buf = data.buffer.slice(
      data.byteOffset,
      data.byteOffset + data.byteLength
    ) as ArrayBuffer
    port.postMessage(buf, [buf])
  }

  const injectMessage = (data: Uint8Array) => {
    if (readController) {
      readController.enqueue(data)
    }
  }

  return { duplex: { readable, writable }, rawWrite, injectMessage }
}

function messagePortToDuplex(port: MessagePort): {
  duplex: DuplexStream<Uint8Array>
  rawWrite: (data: Uint8Array) => void
} {
  let msgCount = 0
  const readable = new ReadableStream<Uint8Array>({
    start(controller) {
      port.onmessage = (ev: MessageEvent) => {
        msgCount++
        if (msgCount <= 3) {
          console.debug(`[pg-proxy-duplex] msg#${msgCount} type=${typeof ev.data} isAB=${ev.data instanceof ArrayBuffer} isU8=${ev.data instanceof Uint8Array} len=${ev.data?.byteLength ?? ev.data?.length ?? '?'}`)
        }
        if (ev.data instanceof ArrayBuffer) {
          controller.enqueue(new Uint8Array(ev.data))
        } else if (ev.data instanceof Uint8Array) {
          controller.enqueue(ev.data)
        } else {
          console.warn(`[pg-proxy-duplex] unexpected data type:`, typeof ev.data, ev.data)
        }
      }
    },
    cancel() {
      port.close()
    },
  })

  let writeCount = 0
  const writable = new WritableStream<Uint8Array>({
    write(chunk) {
      writeCount++
      if (writeCount <= 3) {
        console.debug(`[pg-proxy-duplex] write#${writeCount} len=${chunk.byteLength}`)
      }
      // transfer the ArrayBuffer for zero-copy
      const buf = chunk.buffer.slice(
        chunk.byteOffset,
        chunk.byteOffset + chunk.byteLength
      ) as ArrayBuffer
      port.postMessage(buf, [buf])
    },
    close() {
      port.close()
    },
  })

  // raw write function for injecting data outside of pg-gateway's stream
  // (e.g. parameter status messages during onAuthenticated)
  const rawWrite = (data: Uint8Array) => {
    const buf = data.buffer.slice(
      data.byteOffset,
      data.byteOffset + data.byteLength
    ) as ArrayBuffer
    port.postMessage(buf, [buf])
  }

  return { duplex: { readable, writable }, rawWrite }
}

export interface BrowserProxy {
  handleConnection(port: MessagePort): void
  close(): void
}

export async function createBrowserProxy(
  dbInput: PGlite | PGliteInstances,
  config: { pgPassword: string; pgUser: string; pgPort?: number; logLevel?: string }
): Promise<BrowserProxy> {
  // normalize input: single PGlite instance = use it for all databases (backwards compat for tests)
  const instances: PGliteInstances =
    'postgres' in dbInput
      ? (dbInput as PGliteInstances)
      : { postgres: dbInput as PGlite, cvr: dbInput as PGlite, cdb: dbInput as PGlite }

  // per-instance mutexes for serializing pglite access.
  // when all instances are the same object (single-db mode), share one mutex
  // to prevent concurrent protocol messages on the same pglite instance.
  const sharedInstance =
    instances.postgres === instances.cvr && instances.postgres === instances.cdb
  const pgMutex = new Mutex()
  const mutexes = {
    postgres: pgMutex,
    cvr: sharedInstance ? pgMutex : new Mutex(),
    cdb: sharedInstance ? pgMutex : new Mutex(),
  }

  // per-instance transaction state: tracks which connection owns the current transaction
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

  // signal replication handler after extended protocol writes complete.
  // 8ms leading-edge debounce: fires exactly 8ms after the FIRST write,
  // subsequent writes within that window are batched (handler polls all
  // changes at once). gives the PushProcessor time to confirm the mutation
  // before replication streams the same change to zero-cache.
  let signalTimer: ReturnType<typeof setTimeout> | null = null
  function signalWrite() {
    if (signalTimer) return
    signalTimer = setTimeout(() => {
      signalTimer = null
      signalReplicationChange()
    }, 8)
  }

  let closed = false

  function handleConnection(port: MessagePort) {
    if (closed) {
      port.close()
      return
    }

    port.start()

    // peek at the first message to detect replication connections.
    // replication connections bypass pg-gateway entirely and are handled
    // with raw MessagePort communication — matching orez-node where
    // handleReplicationMessage writes directly to the TCP socket.
    let firstMessage = true
    const origOnMessage = port.onmessage
    port.onmessage = (ev: MessageEvent) => {
      if (!firstMessage) return // handled by pg-gateway or raw handler
      firstMessage = false

      const data = ev.data instanceof ArrayBuffer ? new Uint8Array(ev.data) : ev.data
      if (!(data instanceof Uint8Array) || data.length < 8) {
        // not a valid startup message, let pg-gateway handle it
        port.onmessage = origOnMessage
        handleRegularConnection(port, ev)
        return
      }

      // parse startup message params
      const params = parseStartupParams(data)
      const dbName = params.database || 'postgres'
      const isRepl = params.replication === 'database'
      console.debug(`[pg-proxy] connection: db=${dbName} repl=${isRepl}`)
      // handle ALL connections with raw MessagePort (bypass pg-gateway).
      // pg-gateway's WritableStream doesn't reliably flush in browser Web Workers.
      handleRawConnection(port, data, params, getDbContext(dbName), isRepl)
    }
  }

  /** parse startup message key-value params */
  function parseStartupParams(data: Uint8Array): Record<string, string> {
    const params: Record<string, string> = {}
    // skip: int32 length + int32 protocol version = 8 bytes
    let pos = 8
    while (pos < data.length - 1) {
      const keyStart = pos
      while (pos < data.length && data[pos] !== 0) pos++
      if (pos >= data.length) break
      const key = textDecoder.decode(data.subarray(keyStart, pos))
      pos++ // skip null
      const valStart = pos
      while (pos < data.length && data[pos] !== 0) pos++
      const val = textDecoder.decode(data.subarray(valStart, pos))
      pos++ // skip null
      if (key) params[key] = val
    }
    return params
  }

  /** handle ANY connection with raw MessagePort (no pg-gateway) */
  function handleRawConnection(
    port: MessagePort,
    startupData: Uint8Array,
    params: Record<string, string>,
    ctx: { db: PGlite; mutex: Mutex; txState: PgLiteTxState },
    isReplicationConnection: boolean
  ) {
    const { db, mutex, txState } = ctx
    const connId = {}
    const dbName = params.database || 'postgres'
    let connClosed = false

    const write = (data: Uint8Array) => {
      if (connClosed) return
      const buf = data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) as ArrayBuffer
      port.postMessage(buf, [buf])
    }

    // step 1: send AuthenticationClearTextPassword (R, type=3) — ask for password
    const authRequest = new Uint8Array([0x52, 0, 0, 0, 8, 0, 0, 0, 3])
    write(authRequest)

    // step 2: wait for Password message (p), then send AuthOk + params
    port.onmessage = (ev: MessageEvent) => {
      const data2 = ev.data instanceof ArrayBuffer ? new Uint8Array(ev.data) : ev.data as Uint8Array
      if (!data2 || data2[0] !== 0x70) {
        console.warn('[pg-proxy-repl-raw] expected password message, got type=0x' + data2?.[0]?.toString(16))
      }

      // send AuthenticationOk (R, type=0)
      const authOk = new Uint8Array([0x52, 0, 0, 0, 8, 0, 0, 0, 0])
      write(authOk)

      // send ParameterStatus messages
      for (const [name, value] of SERVER_PARAMS) {
        write(buildParameterStatus(name, value))
      }

      // send BackendKeyData (K) — fake pid + secret
      const bkd = new Uint8Array(13)
      bkd[0] = 0x4b // K
      new DataView(bkd.buffer).setInt32(1, 12)
      new DataView(bkd.buffer).setInt32(5, 1) // pid
      new DataView(bkd.buffer).setInt32(9, 0) // secret
      write(bkd)

      // send ReadyForQuery (Z) — idle
      const rfq = new Uint8Array(6)
      rfq[0] = 0x5a
      new DataView(rfq.buffer).setInt32(1, 5)
      rfq[5] = 0x49 // I = idle
      write(rfq)

      console.debug('[pg-proxy-repl-raw] auth complete, ready for queries')

      // step 3: handle subsequent messages (queries, replication commands)
      installQueryHandler()
    }

    let pipelineMutexHeld = false
    let extWritePending = false

    function installQueryHandler() {
    port.onmessage = async (ev: MessageEvent) => {
      if (connClosed) return
      let data = ev.data instanceof ArrayBuffer ? new Uint8Array(ev.data) : ev.data as Uint8Array
      if (!data || !(data instanceof Uint8Array)) return

      const msgType = data[0]

      // replication connection: handle replication commands
      if (isReplicationConnection && msgType === 0x51) {
        const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
        const len = view.getInt32(1)
        const query = textDecoder.decode(data.subarray(5, 1 + len - 1)).replace(/\0$/, '')
        const upper = query.trim().toUpperCase()

        if (upper.startsWith('START_REPLICATION')) {
          if (abortPreviousReplication) abortPreviousReplication()
          let aborted = false
          const writer = {
            write(chunk: Uint8Array) {
              if (!connClosed && !aborted) {
                try { write(chunk) } catch { aborted = true }
              }
            },
            get closed() { return connClosed || aborted },
          }
          abortPreviousReplication = () => { aborted = true; connClosed = true; port.close() }
          port.onmessage = () => {}
          handleStartReplication(query, writer, db, mutex).catch(() => {})
          return
        }

        // replication queries (IDENTIFY_SYSTEM, CREATE/DROP SLOT)
        await mutex.acquire()
        try {
          const response = await handleReplicationQuery(query, db)
          if (response) { write(response); return }
          data = interceptQuery(data)
          const result = await db.execProtocolRaw(data, { syncToFs: false })
          write(result)
        } finally {
          mutex.release()
        }
        return
      }

      // regular query handling (SimpleQuery or extended protocol)

      // extended protocol pipeline: Parse(0x50), Bind(0x42), Describe(0x44),
      // Execute(0x45), Close(0x43), Flush(0x48)
      const isExtendedMsg = msgType === 0x50 || msgType === 0x42 ||
        msgType === 0x44 || msgType === 0x45 || msgType === 0x43 || msgType === 0x48
      const isSyncInPipeline = msgType === 0x53 && pipelineMutexHeld

      if (isExtendedMsg || isSyncInPipeline) {
        if (!pipelineMutexHeld) {
          await mutex.acquire()
          pipelineMutexHeld = true
          // auto-rollback stale transactions
          if (txState.status === 0x45 && txState.owner !== connId) {
            try { await db.exec('ROLLBACK') } catch {}
            txState.status = 0x49
            txState.owner = null
          }
        }

        // detect writes for replication signaling
        if (dbName === 'postgres' && msgType === 0x50) {
          const q = extractParseQuery(data)?.trimStart().toLowerCase()
          if (q && /^(insert|update|delete|copy|truncate)/.test(q)) {
            extWritePending = true
          }
        }

        data = interceptQuery(data)
        let result: Uint8Array
        try {
          result = await db.execProtocolRaw(data, { syncToFs: false })
        } catch (err) {
          mutex.release()
          pipelineMutexHeld = false
          return // silently drop on error
        }

        // update transaction state
        const rfqStatus = getReadyForQueryStatus(result)
        if (rfqStatus !== null) {
          txState.status = rfqStatus
          txState.owner = rfqStatus === 0x49 ? null : connId
        }

        // release mutex on Sync
        if (msgType === 0x53) {
          mutex.release()
          pipelineMutexHeld = false
          if (dbName === 'postgres' && extWritePending) {
            extWritePending = false
            signalWrite()
          }
        } else {
          // strip ReadyForQuery from non-Sync messages
          result = stripResponseMessages(result, true)
        }

        write(result)
        return
      }

      // SimpleQuery (0x51) or standalone Sync
      if (msgType === 0x51) {
        const queryText = extractQueryText(data)
        // ping fast-path
        if (queryText) {
          const pingMatch = queryText.match(PING_QUERY_RE)
          if (pingMatch) { write(buildSelectIntResponse(pingMatch[1])); return }
        }
        if (isNoopQuery(data)) { write(buildSetCompleteResponse()); return }
      }

      data = interceptQuery(data)
      await mutex.acquire()
      try {
        if (txState.status === 0x45 && txState.owner !== connId) {
          try { await db.exec('ROLLBACK') } catch {}
          txState.status = 0x49; txState.owner = null
        }
        const result = await db.execProtocolRaw(data, { syncToFs: false })
        const rfqStatus = getReadyForQueryStatus(result)
        if (rfqStatus !== null) {
          txState.status = rfqStatus
          txState.owner = rfqStatus === 0x49 ? null : connId
        }
        // signal writes
        if (dbName === 'postgres' && msgType === 0x51) {
          const qn = extractQueryText(data)?.trimStart().toLowerCase()
          if (qn && isWriteNormalized(qn)) signalReplicationChange()
        }
        write(result)
      } finally {
        mutex.release()
      }
    }
    } // end installQueryHandler
  }

  function handleRegularConnection(port: MessagePort, firstEvent: MessageEvent) {
    // create duplex AFTER we know it's not a replication connection.
    // the first message (startup) needs to be re-injected into the readable stream.
    const { duplex, rawWrite, injectMessage } = messagePortToDuplexWithInject(port)
    // re-inject the startup message that we consumed for detection
    if (firstEvent.data instanceof ArrayBuffer) {
      injectMessage(new Uint8Array(firstEvent.data))
    } else if (firstEvent.data instanceof Uint8Array) {
      injectMessage(firstEvent.data)
    }

    // opaque identity token for this connection (used for tx state ownership)
    const connId = {}

    let dbName = 'postgres'
    let isReplicationConnection = false
    // track extended protocol writes (Parse with INSERT/UPDATE/DELETE/COPY/TRUNCATE)
    // so we can signal replication on Sync (0x53) after the pipeline completes
    let extWritePending = false
    // hold mutex across entire extended protocol pipeline (Parse→Sync).
    // prevents other connections from interleaving and corrupting PGlite's
    // unnamed portal/statement state during the pipeline.
    let pipelineMutexHeld = false
    // connection closed flag
    let connClosed = false

    // clean up pglite transaction state when the connection ends
    const cleanup = async () => {
      if (connClosed) return
      connClosed = true
      // replication connections don't own a transaction — skip ROLLBACK
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
    }

    try {
      let connection!: PostgresConnection
      connection = new PostgresConnection(duplex, {
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
        // write directly to the port since pg-gateway owns the writable stream
        onAuthenticated() {
          console.debug(`[pg-proxy-conn] authenticated db=${dbName}`)
          for (const [name, value] of SERVER_PARAMS) {
            rawWrite(buildParameterStatus(name, value))
          }
        },

        async onStartup(state) {
          const params = state.clientParams
          if (params?.replication === 'database') {
            isReplicationConnection = true
          }
          dbName = params?.database || 'postgres'
          console.debug(`[pg-proxy-conn] startup: db=${dbName} user=${params?.user} repl=${params?.replication || 'none'}`)
          const { db } = getDbContext(dbName)
          await db.waitReady
        },

        async onMessage(data, state) {
          if (!state.isAuthenticated) {
            console.debug(`[pg-proxy-conn] msg before auth, type=0x${data[0].toString(16)}`)
            return
          }
          console.debug(`[pg-proxy-conn] msg db=${dbName} type=0x${data[0].toString(16)} len=${data.length}`)

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
            return handleReplicationMessageBrowser(
              data,
              rawWrite,
              () => connClosed,
              () => {
                connClosed = true
                port.close()
              },
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
              if (txState.status === 0x45 && txState.owner !== connId) {
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
              txState.owner = rfqStatus === 0x49 ? null : connId
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

          // normalize query once for all classification checks
          const isSimpleQuery = msgType === 0x51
          const queryText = isSimpleQuery ? extractQueryText(data) : null
          const queryNorm = queryText ? queryText.trimStart().toLowerCase() : null
          const cacheable = queryNorm && isCacheableNormalized(queryNorm)

          // cache Simple Query schema queries
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
            if (txState.status === 0x45 && txState.owner !== connId) {
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
              txState.owner = rfqStatus === 0x49 ? null : connId
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
            if (queryNorm && isDDLNormalized(queryNorm)) {
              invalidateSchemaCache()
            }
          }

          const stripRfq = msgType !== 0x53 && msgType !== 0x51
          result = stripResponseMessages(result, stripRfq)

          // signal replication handler on postgres writes for instant sync
          if (dbName === 'postgres' && queryNorm && isWriteNormalized(queryNorm)) {
            signalReplicationChange()
          }

          return result
        },
      })

      // when the pg-gateway connection's readable stream ends (port closed),
      // run cleanup. the PostgresConnection constructor starts init() which
      // reads from duplex.readable — when the port closes, the readable ends
      // and init() resolves, but there's no explicit "close" callback.
      // we rely on the readable stream ending to trigger cleanup.
      // the readable's cancel() calls port.close(), but if the port is closed
      // externally, the readable controller will error/close and init resolves.
      void (async () => {
        // wait for the connection to finish processing
        // PostgresConnection.init() returns when the readable stream ends
        try {
          // small delay to allow init() to start (constructor kicks it off synchronously)
          await new Promise((r) => setTimeout(r, 0))
          // poll until the connection is detached or the port signals close
          // since MessagePort has no 'close' event, we detect when
          // the connection's internal processing ends
        } catch {
          // ignore
        }
        cleanup()
      })()
    } catch {
      cleanup()
    }
  }

  return {
    handleConnection,
    close() {
      closed = true
      if (signalTimer) {
        clearTimeout(signalTimer)
        signalTimer = null
      }
    },
  }
}

async function handleReplicationMessageBrowser(
  data: Uint8Array,
  rawWrite: (data: Uint8Array) => void,
  isClosed: () => boolean,
  closeConn: () => void,
  db: PGlite,
  mutex: Mutex,
  connection: PostgresConnection
): Promise<Uint8Array | undefined> {
  console.debug(`[pg-proxy-repl] ENTRY type=0x${data[0].toString(16)} len=${data.length}`)

  // for non-SimpleQuery messages (extended protocol), execute against PGlite directly.
  if (data[0] !== 0x51) {
    console.debug(`[pg-proxy-repl] ext protocol msg type=0x${data[0].toString(16)} len=${data.length}`)
    await mutex.acquire()
    try {
      const result = await db.execProtocolRaw(data, { syncToFs: false })
      console.debug(`[pg-proxy-repl] ext protocol result len=${result.length}`)
      return result
    } finally {
      mutex.release()
    }
  }

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
        if (!isClosed() && !aborted) {
          try {
            rawWrite(chunk)
          } catch {
            // port may have closed between our check and write
            aborted = true
          }
        }
      },
      get closed() {
        return isClosed() || aborted
      },
    }

    const abort = () => {
      aborted = true
      closeConn()
    }
    abortPreviousReplication = abort

    handleStartReplication(query, writer, db, mutex).catch((err) => {
      log.proxy(`replication stream ended: ${err}`)
    })
    return undefined
  }

  // handle replication queries + fallthrough to pglite, all under mutex
  console.debug(`[pg-proxy-repl] query: ${query.slice(0, 100)}`)
  console.debug(`[pg-proxy-repl] acquiring mutex...`)
  await mutex.acquire()
  console.debug(`[pg-proxy-repl] mutex acquired, testing db access...`)
  try {
    const testResult = await db.query('SELECT 1 as test')
    console.debug(`[pg-proxy-repl] db.query works: ${JSON.stringify(testResult.rows)}`)
    const response = await handleReplicationQuery(query, db)
    console.debug(`[pg-proxy-repl] handleReplicationQuery result: ${response ? 'bytes(' + response.length + ')' : 'null'}`)
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
