// @ts-nocheck
/**
 * DoBackend: a PGlite-compatible adapter that forwards SQL to Cloudflare Durable Objects.
 *
 * Translates PG wire protocol messages → SQL → DO HTTP API → PG wire protocol responses.
 *
 * Handles PG transactions transparently: BEGIN/COMMIT/ROLLBACK are intercepted
 * and managed with in-memory write buffering. Writes are flushed to the DO
 * atomically via ctx.storage.transaction() on COMMIT.
 */

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

// ── PG wire protocol constants ────────────────────────────────────────────

const FT_QUERY = 0x51
const FT_PARSE = 0x50
const FT_BIND = 0x42
const FT_DESCRIBE = 0x44
const FT_EXECUTE = 0x45
const FT_SYNC = 0x53
const FT_CLOSE = 0x43
const FT_TERMINATE = 0x58
const FT_FLUSH = 0x48

const STATUS_IDLE = 0x49
const PG_TYPE_TEXT = 25
const PG_TYPE_INT4 = 23
const PG_TYPE_INT8 = 20
const PG_TYPE_BOOL = 16
const PG_TYPE_FLOAT8 = 701
const PG_TYPE_VARCHAR = 1043
const PG_TYPE_JSON = 114
const PG_TYPE_NUMERIC = 1700
const PG_TYPE_TIMESTAMP = 1114
const PG_TYPE_BYTEA = 17
const PG_TYPE_INT2 = 21

// ── Utilities ─────────────────────────────────────────────────────────────

function concat(...parts: Uint8Array[]): Uint8Array {
  let total = 0
  for (const p of parts) total += p.length
  const result = new Uint8Array(total)
  let offset = 0
  for (const p of parts) {
    result.set(p, offset)
    offset += p.length
  }
  return result
}

function i16(v: number, buf = new ArrayBuffer(2)): Uint8Array {
  new DataView(buf).setInt16(0, v)
  return new Uint8Array(buf)
}

function i32(v: number, buf = new ArrayBuffer(4)): Uint8Array {
  new DataView(buf).setInt32(0, v)
  return new Uint8Array(buf)
}

function int4(v: number, buf = new ArrayBuffer(4)): Uint8Array {
  new DataView(buf).setInt32(0, v)
  return new Uint8Array(buf)
}

function uint4(v: number, buf = new ArrayBuffer(4)): Uint8Array {
  new DataView(buf).setUint32(0, v)
  return new Uint8Array(buf)
}

function cstr(s: string): Uint8Array {
  const encoded = textEncoder.encode(s)
  const result = new Uint8Array(encoded.length + 1)
  result.set(encoded)
  return result
}

function msg(ty: number, payload: Uint8Array): Uint8Array {
  const out = new Uint8Array(1 + 4 + payload.length)
  out[0] = ty
  new DataView(out.buffer, 1, 4).setUint32(0, 4 + payload.length)
  out.set(payload, 5)
  return out
}

const zero4 = new Uint8Array(4)
const zero2 = new Uint8Array(2)

// ── PG response builders ─────────────────────────────────────────────────

function buildRowDescription(fields: { name: string; oid?: number }[]): Uint8Array {
  if (fields.length === 0) return buildNoData()
  const colParts: Uint8Array[] = []
  for (const f of fields) {
    colParts.push(cstr(f.name))
    const col = new Uint8Array(18)
    const v = new DataView(col.buffer)
    v.setUint32(0, 0)
    v.setInt16(4, 0)
    v.setUint32(6, f.oid ?? PG_TYPE_TEXT)
    v.setInt16(10, -1)
    v.setInt32(12, -1)
    v.setInt16(16, 0)
    colParts.push(col)
  }
  return msg(0x54, concat(i16(fields.length), ...colParts))
}

function buildDataRow(row: Record<string, unknown>, fields: string[]): Uint8Array {
  const colParts: Uint8Array[] = []
  for (const name of fields) {
    const val = row[name]
    if (val === null || val === undefined) {
      colParts.push(int4(-1))
    } else {
      const str = typeof val === 'object' ? JSON.stringify(val) : String(val)
      const encoded = textEncoder.encode(str)
      colParts.push(concat(uint4(encoded.length), encoded))
    }
  }
  return msg(0x44, concat(i16(fields.length), ...colParts))
}

function buildCommandComplete(tag: string): Uint8Array {
  return msg(0x43, cstr(tag))
}

function buildReadyForQuery(status: number = STATUS_IDLE): Uint8Array {
  return msg(0x5a, new Uint8Array([status]))
}

function buildErrorResponse(message: string): Uint8Array {
  return msg(
    0x45,
    concat(
      cstr('S'),
      cstr('ERROR'),
      cstr('C'),
      cstr('XX000'),
      cstr('M'),
      cstr(message),
      new Uint8Array([0])
    )
  )
}

function buildParseComplete(): Uint8Array {
  return msg(0x31, zero4)
}
function buildBindComplete(): Uint8Array {
  return msg(0x32, zero4)
}
function buildCloseComplete(): Uint8Array {
  return msg(0x33, zero4)
}
function buildNoData(): Uint8Array {
  return msg(0x6e, zero4)
}
function buildParameterDescription(oids: number[]): Uint8Array {
  return msg(0x74, concat(i16(oids.length), ...oids.map(uint4)))
}
function buildParameterStatus(name: string, value: string): Uint8Array {
  return msg(0x53, concat(cstr(name), cstr(value)))
}
function buildNotificationResponse(
  pid: number,
  channel: string,
  payload: string
): Uint8Array {
  return msg(0x41, concat(uint4(pid), cstr(channel), cstr(payload)))
}

// ── PG message parsers ────────────────────────────────────────────────────

function extractQueryText(data: Uint8Array): string | null {
  if (data[0] !== 0x51) return null
  const len = new DataView(data.buffer, data.byteOffset, data.byteLength).getInt32(1)
  return textDecoder.decode(data.subarray(5, 1 + len - 1)).replace(/\0$/, '')
}

function extractParseQuery(data: Uint8Array): string | null {
  if (data[0] !== 0x50) return null
  let offset = 5
  while (offset < data.length && data[offset] !== 0) offset++
  offset++
  const qStart = offset
  while (offset < data.length && data[offset] !== 0) offset++
  return textDecoder.decode(data.subarray(qStart, offset))
}

function extractParseStatementName(data: Uint8Array): string {
  let offset = 5
  const start = offset
  while (offset < data.length && data[offset] !== 0) offset++
  return textDecoder.decode(data.subarray(start, offset))
}

function extractBindStatementName(data: Uint8Array): string {
  let offset = 5
  while (offset < data.length && data[offset] !== 0) offset++
  offset++
  const start = offset
  while (offset < data.length && data[offset] !== 0) offset++
  return textDecoder.decode(data.subarray(start, offset))
}

function extractBindParams(data: Uint8Array): any[] {
  const params: any[] = []
  let offset = 5
  while (offset < data.length && data[offset] !== 0) offset++
  offset++
  while (offset < data.length && data[offset] !== 0) offset++
  offset++
  if (offset + 2 > data.length) return params
  const nfc = new DataView(data.buffer, data.byteOffset + offset, 2).getInt16(0)
  offset += 2 + nfc * 2
  if (offset + 2 > data.length) return params
  const np = new DataView(data.buffer, data.byteOffset + offset, 2).getInt16(0)
  offset += 2
  for (let i = 0; i < np; i++) {
    if (offset + 4 > data.length) break
    const plen = new DataView(data.buffer, data.byteOffset + offset, 4).getInt32(0)
    offset += 4
    if (plen === -1) {
      params.push(null)
      continue
    }
    const str = textDecoder.decode(data.subarray(offset, offset + plen))
    offset += plen
    params.push(
      /^-?\d+(\.\d+)?$/.test(str)
        ? str.includes('.')
          ? parseFloat(str)
          : Number(str)
        : str
    )
  }
  return params
}

function extractDescribeType(data: Uint8Array): 'S' | 'P' {
  return data[5] === 0x53 ? 'S' : 'P'
}
function extractDescribeName(data: Uint8Array): string {
  const start = 6
  let off = start
  while (off < data.length && data[off] !== 0) off++
  return textDecoder.decode(data.subarray(start, off))
}

// ── Catalog query interception ────────────────────────────────────────────

function isCatalogQuery(sql: string): boolean {
  const n = sql.replace(/\s+/g, ' ').trim().toLowerCase()
  if (n.includes('current_setting(')) return true
  if (n.includes('pg_advisory_xact_lock') || n.includes('pg_advisory_lock')) return true
  if (
    n.startsWith('select') &&
    (n.includes('information_schema.') ||
      n.includes('pg_catalog.') ||
      n.includes('pg_tables') ||
      n.includes('pg_namespace') ||
      n.includes('pg_type') ||
      n.includes('pg_class') ||
      n.includes('pg_attribute') ||
      n.includes('pg_stat_') ||
      n.includes('pg_index') ||
      n.includes('pg_depend') ||
      n.includes('pg_database') ||
      n.includes('pg_sequence') ||
      n.includes('pg_description') ||
      n.includes('pg_constraint') ||
      n.includes('pg_inherits') ||
      n.includes('pg_cast') ||
      n.includes('pg_opfamily') ||
      n.includes('pg_am ') ||
      n.includes('pg_operator') ||
      n.includes('pg_aggregate') ||
      n.includes('pg_language') ||
      n.includes('pg_extension') ||
      n.includes('pg_foreign_data') ||
      n.includes('pg_foreign_server') ||
      n.includes('pg_range') ||
      n.includes('pg_enum') ||
      n.includes('pg_rewrite') ||
      n.includes('pg_proc') ||
      n.includes('pg_roles') ||
      n.includes('pg_user ') ||
      n.includes('pg_authid') ||
      n.includes('pg_settings') ||
      n.includes('pg_collation') ||
      n.includes('pg_trigger') ||
      n.includes('pg_get_expr') ||
      n.includes('pg_get_functiondef') ||
      n.includes('pg_get_constraintdef') ||
      n.includes('pg_describe_object') ||
      n.includes('has_') ||
      n.includes('obj_description') ||
      n.includes('format_type') ||
      /\bfrom\s+pg_\w+/i.test(n))
  )
    return true
  if (
    n.includes('information_schema.') &&
    (n.includes('schemata') ||
      n.includes('views') ||
      n.includes('view_') ||
      n.includes('_pg_') ||
      n.includes('table_privileges') ||
      n.includes('column_udt_usage') ||
      n.includes('routine_') ||
      n.includes('parameters') ||
      n.includes('check_constraints') ||
      n.includes('referential_constraints') ||
      n.includes('key_column_usage'))
  )
    return true
  return false
}

// ── SQL rewriting ─────────────────────────────────────────────────────────

function rewriteSQL(sql: string): string {
  let result = sql.trim()

  // Strip PG type casts
  result = result.replace(/::\w+(\[\])?\b/g, '')

  // Schema-qualified names: "schema"."table" or schema.table → flat
  result = result.replace(/"(\w+)"\s*\.\s*"(\w+)"/g, '"$1_$2"')
  result = result.replace(/_orez\._zero_changes\b/g, '_zero_changes')
  result = result.replace(
    /_orez\._zero_replication_slots\b/g,
    '_orez__zero_replication_slots'
  )
  result = result.replace(/(\b)_orez\.(\w+)/g, '$1_orez__$2')
  // _zero schema references (quoted and unquoted)
  result = result.replace(/(\b)_zero\.(\w+)/g, '$1_zero_$2')
  // Quoted schema.table identifiers: "_zero.tableMetadata" → "_zero_tableMetadata"
  result = result.replace(/"(_zero|_orez)\.(\w+)"/g, '"$1_$2"')

  // nextval → 1
  result = result.replace(/nextval\s*\([^)]*\)/gi, '1')

  // CREATE SEQUENCE → CREATE TABLE IF NOT EXISTS
  if (/^\s*create\s+sequence\s+/i.test(result)) {
    const m = /create\s+sequence\s+(\S+)/i.exec(result)
    if (m)
      result = `CREATE TABLE IF NOT EXISTS _${m[1].replace(/"/g, '')}_seq (val INTEGER DEFAULT 1, dummy INTEGER PRIMARY KEY DEFAULT 1)`
  }

  // Skipped PG features
  if (/^\s*create\s+(or\s+replace\s+)?(function|trigger)\s+/i.test(result)) return ''
  if (/^\s*create\s+database\s/i.test(result)) return ''
  if (/^\s*cluster\s+/i.test(result)) return ''
  if (/^\s*(grant|revoke)\s+/i.test(result)) return ''
  if (/^\s*alter\s+default\s+privileges/i.test(result)) return ''
  if (/^\s*comment\s+on\s+/i.test(result)) return ''
  if (/^\s*(create|alter|drop)\s+publication\s+/i.test(result)) return ''
  if (/^\s*alter\s+table\s+.+replica\s+identity/i.test(result)) return ''
  // PG-specific ALTER COLUMN (SQLite doesn't support)
  if (/^\s*alter\s+table\s+.+alter\s+column\s+/i.test(result)) return ''

  // Handle multi-statement SQL beginning with CREATE SCHEMA (remove that line)
  result = result.replace(/^\s*create\s+schema\s+[^;]+;\s*/i, '')
  // CLOSE cursor — pg-specific
  if (/^\s*close\s+/i.test(result)) return ''

  // DDL schema flattening
  if (/^\s*(create|alter|drop)\s+(table|index|view|schema|sequence)\s+/i.test(result)) {
    result = result.replace(/(\w+)\.(\w+)/g, '$1_$2')
  }
  // PG type → SQLite type mapping for DDL
  result = result.replace(/\bTIMESTAMPTZ\b/g, 'TEXT')
  result = result.replace(/\bTIMESTAMP\b/ig, 'TEXT')
  result = result.replace(/\bJSONB\b/g, 'TEXT')
  result = result.replace(/\bDOUBLE\s+PRECISION\b/g, 'REAL')
  result = result.replace(/\bBOOLEAN\b/gi, 'INTEGER')
  result = result.replace(/\bBOOL\b/g, 'INTEGER')
  result = result.replace(/\bBIGINT\b/g, 'INTEGER')
  result = result.replace(/\bSMALLINT\b/g, 'INTEGER')
  result = result.replace(/\bSERIAL\b/g, 'INTEGER')
  result = result.replace(/\bBIGSERIAL\b/g, 'INTEGER')
  result = result.replace(/\bBYTEA\b/g, 'BLOB')
  // now() → CURRENT_TIMESTAMP (PG function not in SQLite)
  result = result.replace(/\bnow\s*\(\s*\)/gi, "CURRENT_TIMESTAMP")
  // true/false → 1/0 for DEFAULT and CHECK contexts
  result = result.replace(/\bdefault\s+true\b/gi, 'DEFAULT 1')
  result = result.replace(/\bdefault\s+false\b/gi, 'DEFAULT 0')
  // Strip CONSTRAINT name prefix (PG syntax, SQLite wants bare constraint)
  result = result.replace(/\bconstraint\s+"?\w+"?\s+(primary\s+key|unique|foreign\s+key|check)\b/gi, '$1')
  // ON CONFLICT DO NOTHING → INSERT OR IGNORE (if no conflict target)
  if (/\bon\s+conflict\s+do\s+nothing\b/i.test(result)) {
    result = result.replace(/^\s*insert\s+into\s+/i, 'INSERT OR IGNORE INTO ')
    result = result.replace(/\bon\s+conflict\s+do\s+nothing\b/gi, '')
  }

  // DEALLOCATE / DISCARD / RESET
  if (/^(deallocate|discard|reset\s+all)/i.test(result)) return ''
  // LISTEN / UNLISTEN
  if (/^(listen|unlisten)/i.test(result)) return ''
  // SHOW
  if (/^show\s+/i.test(result)) return ''
  // SET (LOCAL/SESSION/CONSTRAINTS/etc.) → skip (but allow SET that targets zero_0 tables)
  if (/^set\s/i.test(result) && !/^set\s+"zero_0"\./i.test(result)) return ''

  return result
}

// ── DoBackend class ───────────────────────────────────────────────────────

export class DoBackend {
  readonly waitReady: Promise<void>
  ready = false
  closed = false
  private doUrl: string
  private dbName: string
  private httpClient: HttpClient
  private preparedStatements = new Map<string, { sql: string }>()
  private sqlToExecute: { sql: string; params: any[] } | null = null

  // Transaction state
  private inTransaction = false
  private txnBuffer: string[] = []
  private txnReadOnly = false

  constructor(doUrl: string, dbName: string = 'postgres') {
    this.doUrl = doUrl.replace(/\/+$/, '')
    this.dbName = dbName
    this.httpClient = new HttpClient()
    this.waitReady = this.init()
  }

  private async init() {
    try {
      await this.httpClient.post(
        `${this.doUrl}/exec?db=${encodeURIComponent(this.dbName)}`,
        JSON.stringify({ sql: 'SELECT 1' })
      )
    } catch {}
    this.ready = true
  }

  async close(): Promise<void> {
    this.closed = true
  }

  async execProtocolRaw(
    message: Uint8Array,
    options?: { syncToFs?: boolean; throwOnError?: boolean }
  ): Promise<Uint8Array> {
    const msgType = message[0]
    try {
      switch (msgType) {
        case FT_QUERY:
          return await this.handleSimpleQuery(message)
        case FT_PARSE:
          return this.handleParse(message)
        case FT_BIND:
          return this.handleBind(message)
        case FT_DESCRIBE:
          return this.handleDescribe(message)
        case FT_EXECUTE:
          return await this.handleExecute(message)
        case FT_SYNC:
          return this.handleSync()
        case FT_CLOSE:
          return buildCloseComplete()
        case FT_FLUSH:
          return new Uint8Array(0)
        case FT_TERMINATE:
          return new Uint8Array(0)
        default:
          return new Uint8Array(0)
      }
    } catch (err: any) {
      if (options?.throwOnError !== false) throw err
      return buildErrorResponse(err.message || String(err))
    }
  }

  // ── Transaction-aware query handling ──────────────────────────────────────

  private async handleSimpleQuery(data: Uint8Array): Promise<Uint8Array> {
    const sql = extractQueryText(data)
    if (!sql) return concat(buildCommandComplete('OK'), buildReadyForQuery())

    const normalized = sql.trimStart().toLowerCase()

    // BEGIN / START TRANSACTION
    if (
      normalized === 'begin' ||
      normalized === 'begin;' ||
      normalized === 'begin work' ||
      normalized === 'begin transaction' ||
      normalized === 'start transaction'
    ) {
      this.inTransaction = true
      this.txnBuffer = []
      this.txnReadOnly = false
      return concat(buildCommandComplete('BEGIN'), buildReadyForQuery())
    }

    // COMMIT / END
    if (
      normalized === 'commit' ||
      normalized === 'commit;' ||
      normalized === 'commit work' ||
      normalized === 'end' ||
      normalized === 'end;'
    ) {
      if (this.inTransaction && this.txnBuffer.length > 0) {
        await this.flushTransactionBuffer()
      }
      this.inTransaction = false
      this.txnBuffer = []
      return concat(buildCommandComplete('COMMIT'), buildReadyForQuery())
    }

    // ROLLBACK / ABORT
    if (
      normalized === 'rollback' ||
      normalized === 'rollback;' ||
      normalized === 'rollback work' ||
      normalized === 'abort' ||
      normalized === 'abort;'
    ) {
      this.inTransaction = false
      this.txnBuffer = []
      return concat(buildCommandComplete('ROLLBACK'), buildReadyForQuery())
    }

    // SET (local) — skip
    if (normalized.startsWith('set '))
      return concat(buildCommandComplete('SET'), buildReadyForQuery())
    if (normalized.startsWith('show '))
      return concat(buildCommandComplete('SHOW'), buildReadyForQuery())
    if (normalized === 'show' || normalized === 'show;')
      return concat(buildCommandComplete('SHOW'), buildReadyForQuery())

    // SAVEPOINT — skip
    if (
      normalized.startsWith('savepoint ') ||
      normalized.startsWith('release savepoint') ||
      normalized.startsWith('release ') ||
      normalized.startsWith('rollback to savepoint') ||
      normalized.startsWith('rollback to ')
    ) {
      return concat(buildCommandComplete('SAVEPOINT'), buildReadyForQuery())
    }

    // DEALLOCATE, DISCARD, RESET → skip
    if (/^(deallocate|discard|reset)\b/.test(normalized)) {
      return concat(buildCommandComplete('OK'), buildReadyForQuery())
    }

    // LOCK TABLE → skip
    if (normalized.startsWith('lock table') || normalized.startsWith('lock ')) {
      return concat(buildCommandComplete('LOCK TABLE'), buildReadyForQuery())
    }

    // Prepare query
    const rewritten = rewriteSQL(sql)
    if (rewritten === '' || rewritten.startsWith('--'))
      return concat(buildCommandComplete('OK'), buildReadyForQuery())

    // Catalog queries — check before forwarding
    if (isCatalogQuery(rewritten)) {
      const result = this.handleCatalogQuery(rewritten)
      return this.buildSelectResponse(result.rows, result.fields)
    }

    // SELECT reads — execute immediately even in transaction
    const isWrite = this.isWriteQuery(rewritten)
    const isDDL = this.isDDLQuery(rewritten)

    if (this.inTransaction && (isWrite || isDDL)) {
      // Buffer writes until COMMIT
      this.txnBuffer.push(rewritten)
      if (isDDL) return concat(buildCommandComplete('CREATE TABLE'), buildReadyForQuery())
      const isInsert = /^\s*insert\b/i.test(rewritten)
      const isUpdate = /^\s*update\b/i.test(rewritten)
      const isDelete = /^\s*delete\b/i.test(rewritten)
      const tag = isInsert
        ? 'INSERT 0 1'
        : isUpdate
          ? 'UPDATE 1'
          : isDelete
            ? 'DELETE 1'
            : 'OK'
      return concat(buildCommandComplete(tag), buildReadyForQuery())
    }

    // Execute SQL
    try {
      const rows = await this.doExec(rewritten)
      return this.buildSQLResponse(rewritten, rows)
    } catch (err: any) {
      return concat(buildErrorResponse(err.message), buildReadyForQuery())
    }
  }

  private async flushTransactionBuffer(): Promise<void> {
    if (this.txnBuffer.length === 0) return
    await this.doBatchExec(this.txnBuffer)
    this.txnBuffer = []
  }

  private isWriteQuery(sql: string): boolean {
    return /^\s*(insert|update|delete|upsert|merge|truncate|copy)\b/i.test(sql)
  }

  private isDDLQuery(sql: string): boolean {
    return /^\s*(create|alter|drop|grant|revoke)\s+(table|index|view|schema|sequence|function|trigger|publication)/i.test(
      sql
    )
  }

  // ── Extended protocol handlers ──────────────────────────────────────────

  private handleParse(data: Uint8Array): Uint8Array {
    const sql = extractParseQuery(data)
    const stmtName = extractParseStatementName(data)
    if (sql) {
      const rewritten = rewriteSQL(sql)
      if (rewritten && !rewritten.startsWith('--') && !isCatalogQuery(rewritten)) {
        this.preparedStatements.set(stmtName, { sql: rewritten })
      }
    }
    return buildParseComplete()
  }

  private handleBind(data: Uint8Array): Uint8Array {
    const stmtName = extractBindStatementName(data)
    const params = extractBindParams(data)
    const stmt = this.preparedStatements.get(stmtName)
    if (stmt) (stmt as any)._params = params
    return buildBindComplete()
  }

  private async handleExecute(_data: Uint8Array): Promise<Uint8Array> {
    let stmt: any
    for (const [, s] of this.preparedStatements) {
      if ((s as any)._params !== undefined) {
        stmt = s
        break
      }
    }
    if (!stmt || !stmt.sql?.trim()) return new Uint8Array(0)

    const params = stmt._params || []
    delete stmt._params
    const sql = this.inlineParams(stmt.sql, params)

    const normalized = sql.trimStart().toLowerCase()

    // Handle transaction markers in extended protocol
    if (normalized === 'begin' || normalized.startsWith('begin ')) {
      this.inTransaction = true
      this.txnBuffer = []
      this.txnReadOnly = false
      return new Uint8Array(0)
    }
    if (normalized === 'commit' || normalized.startsWith('commit ')) {
      if (this.inTransaction && this.txnBuffer.length > 0)
        await this.flushTransactionBuffer()
      this.inTransaction = false
      this.txnBuffer = []
      return buildCommandComplete('COMMIT')
    }
    if (
      normalized === 'rollback' ||
      normalized.startsWith('rollback ') ||
      normalized === 'abort'
    ) {
      this.inTransaction = false
      this.txnBuffer = []
      return buildCommandComplete('ROLLBACK')
    }

    this.sqlToExecute = { sql, params }

    try {
      const rows = await this.doExec(sql)
      if (rows.length > 0) {
        const fns = Object.keys(rows[0])
        return concat(
          buildRowDescription(fns.map((n) => ({ name: n }))),
          ...rows.map((r) => buildDataRow(r, fns)),
          buildCommandComplete(`SELECT ${rows.length}`)
        )
      }
      const isSelect = /^\s*select\b/i.test(sql) || /^\s*with\b/i.test(sql)
      return buildCommandComplete(isSelect ? 'SELECT 0' : 'OK')
    } catch (err: any) {
      return buildErrorResponse(err.message)
    }
  }

  private handleSync(): Uint8Array {
    this.sqlToExecute = null
    return buildReadyForQuery()
  }

  private handleDescribe(data: Uint8Array): Uint8Array {
    const stmt = this.preparedStatements.get(extractDescribeName(data))
    if (stmt && stmt.paramOIDs?.length) return buildParameterDescription(stmt.paramOIDs!)
    return buildNoData()
  }

  // ── High-level API ──────────────────────────────────────────────────────

  async exec(sql: string): Promise<any[]> {
    const rewritten = rewriteSQL(sql)
    if (!rewritten) return []
    if (isCatalogQuery(rewritten)) return []
    return this.doExec(rewritten)
  }

  async query<T = Record<string, unknown>>(
    sql: string,
    _params?: any[]
  ): Promise<{ rows: T[] }> {
    const rewritten = rewriteSQL(sql)
    if (!rewritten) return { rows: [] }
    if (isCatalogQuery(rewritten)) return { rows: [] }
    const rows = await this.doExec(rewritten)
    return { rows: rows as T[] }
  }

  // ── Internal helpers ─────────────────────────────────────────────────────

  private async doExec(sql: string): Promise<Record<string, unknown>[]> {
    if (!sql.trim()) return []
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        const resp = await this.httpClient.post(
          `${this.doUrl}/exec?db=${encodeURIComponent(this.dbName)}`,
          JSON.stringify({ sql }),
          { 'Content-Type': 'application/json' }
        )
        const result = JSON.parse(resp)
        return result.rows ?? result ?? []
      } catch {}
    }

    return []
  }

  private async doBatchExec(statements: string[]): Promise<void> {
    await this.httpClient.post(
      `${this.doUrl}/batch?db=${encodeURIComponent(this.dbName)}`,
      JSON.stringify({ statements }),
      { 'Content-Type': 'application/json' }
    )
  }

  private inlineParams(sql: string, params: any[]): string {
    let result = sql
    for (let i = params.length; i >= 1; i--) {
      const val = params[i - 1]
      const esc =
        val === null
          ? 'NULL'
          : typeof val === 'string'
            ? `'${val.replace(/'/g, "''")}'`
            : String(val)
      result = result.replace(new RegExp(`\\$${i}\\b`, 'g'), esc)
    }
    return result
  }

  private buildSQLResponse(
    originalSql: string,
    rows: Record<string, unknown>[]
  ): Uint8Array {
    const isSelect = /^\s*select\b/i.test(originalSql) || /^\s*with\b/i.test(originalSql)
    if (rows.length > 0) {
      const fns = Object.keys(rows[0])
      const tag = isSelect
        ? `SELECT ${rows.length}`
        : /^\s*insert\b/i.test(originalSql)
          ? 'INSERT 0 1'
          : /^\s*update\b/i.test(originalSql)
            ? 'UPDATE 1'
            : /^\s*delete\b/i.test(originalSql)
              ? 'DELETE 1'
              : 'OK'
      return concat(
        buildRowDescription(fns.map((n) => ({ name: n }))),
        ...rows.map((r) => buildDataRow(r, fns)),
        buildCommandComplete(tag),
        buildReadyForQuery()
      )
    }
    const tag = isSelect
      ? 'SELECT 0'
      : /^\s*insert\b/i.test(originalSql)
        ? 'INSERT 0 0'
        : /^\s*update\b/i.test(originalSql)
          ? 'UPDATE 0'
          : /^\s*delete\b/i.test(originalSql)
            ? 'DELETE 0'
            : 'OK'
    return concat(buildCommandComplete(tag), buildReadyForQuery())
  }

  private buildSelectResponse(
    rows: Record<string, unknown>[],
    fields: { name: string; oid?: number }[]
  ): Uint8Array {
    const fns = fields.map((f) => f.name)
    if (rows.length === 0)
      return concat(
        buildRowDescription(fields),
        buildCommandComplete('SELECT 0'),
        buildReadyForQuery()
      )
    return concat(
      buildRowDescription(fields),
      ...rows.map((r) => buildDataRow(r, fns)),
      buildCommandComplete(`SELECT ${rows.length}`),
      buildReadyForQuery()
    )
  }

  private handleCatalogQuery(sql: string): {
    rows: Record<string, unknown>[]
    fields: { name: string; oid?: number }[]
  } {
    const n = sql.replace(/\s+/g, ' ').trim().toLowerCase()

    // current_setting('server_version') etc.
    const csMatch = /current_setting\s*\(\s*['"]([^'"]+)['"]\s*\)/.exec(n)
    if (csMatch) {
      const vals: Record<string, string> = {
        server_version: '16.0',
        server_encoding: 'UTF8',
        client_encoding: 'UTF8',
        standard_conforming_strings: 'on',
        TimeZone: 'UTC',
        integer_datetimes: 'on',
        IntervalStyle: 'postgres',
        DateStyle: 'ISO, MDY',
        lc_messages: 'en_US.UTF-8',
        lc_monetary: 'en_US.UTF-8',
        lc_numeric: 'en_US.UTF-8',
        lc_time: 'en_US.UTF-8',
      }
      return {
        rows: [{ current_setting: vals[csMatch[1]] ?? '' }],
        fields: [{ name: 'current_setting' }],
      }
    }

    return { rows: [], fields: [] }
  }
}

class HttpClient {
  async post(
    url: string,
    body: string,
    headers?: Record<string, string>
  ): Promise<string> {
    const resp = await fetch(url, {
      method: 'POST',
      headers: headers ?? { 'Content-Type': 'application/json' },
      body,
    })
    if (!resp.ok) {
      const text = await resp.text().catch(() => '')
      throw new Error(`HTTP ${resp.status}: ${text.slice(0, 200)}`)
    }
    return resp.text()
  }
}
