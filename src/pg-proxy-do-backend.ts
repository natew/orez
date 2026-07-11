// @ts-nocheck
import { deparseSync, loadModule, parseSync } from 'pgsql-parser'

import { TX_MANIFEST_DDL, TX_MANIFEST_TABLE } from './cf-do/tx-journal.js'
import { RETURNING_INTERNAL_PREFIX } from './do-sql-tracking.js'
import {
  expandDelete,
  FkCascadeRegistry,
  recordAlterTableForeignKeys,
  recordCreateTableForeignKeys,
  type FkChild,
} from './fk-cascade.js'
import { Mutex } from './mutex.js'
import {
  foldCountMarkerResult,
  transformCountedDeleteCte,
} from './pg-sqlite-compiler/passes/dml-cte.js'
import { signalReplicationChange } from './replication/handler.js'
import {
  markSQLiteKeywordIdentifiers,
  restoreSQLiteKeywordIdentifierMarkers,
} from './sqlite-keyword-identifiers.js'

/**
 * DoBackend: a PGlite-compatible adapter that forwards SQL to Cloudflare Durable Objects.
 *
 * Translates PG wire protocol messages → SQL → DO HTTP API → PG wire protocol responses.
 *
 * Handles PG transactions transparently: BEGIN/COMMIT/ROLLBACK are intercepted
 * and data writes are restored from table snapshots on ROLLBACK.
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
const STATUS_TRANSACTION = 0x54
const PG_TYPE_TEXT = 25
const PG_TYPE_INT4 = 23
const PG_TYPE_INT8 = 20
const PG_TYPE_BOOL = 16
const PG_TYPE_FLOAT8 = 701
const PG_TYPE_VARCHAR = 1043
const PG_TYPE_JSON = 114
const PG_TYPE_JSONB = 3802
const PG_TYPE_NUMERIC = 1700
const PG_TYPE_TIMESTAMP = 1114
const PG_TYPE_TIMESTAMPTZ = 1184
const PG_TYPE_BYTEA = 17
const PG_TYPE_INT2 = 21

type SqliteRow = Record<string, unknown>
interface ExecResult {
  rows: SqliteRow[]
  columns: string[]
  affectedRows?: number
}
interface CatalogResult {
  rows: Record<string, unknown>[]
  fields: { name: string; oid?: number }[]
}
interface PreparedStatement {
  sql: string
  originalSql?: string
  rewrittenStatements?: RewrittenStatement[]
  paramOIDs: number[]
  arrayParamNumbers?: Set<number>
  jsonParamNumbers?: Set<number>
  timestampParamNumbers?: Set<number>
  epochMillisParamNumbers?: Set<number>
  booleanParamNumbers?: Set<number>
  schemaColumns?: SchemaColumnMetadata[]
  schemaMetadataChanges?: SchemaMetadataChange[]
  publicationChanges?: PublicationChange[]
  fields?: { name: string; oid?: number }[]
  commandTag?: string
}
interface BoundPortal extends PreparedStatement {
  statementName: string
  params: any[]
}
interface SchemaColumnMetadata {
  table: string
  schema: string
  tableName: string
  column: string
  oid?: number
  typeOid?: number
  dataType?: string
  typtype?: string
  typname?: string
  elemTyptype?: string | null
  elemTypname?: string | null
  characterMaximumLength?: number | null
  numericPrecision?: number | null
  numericScale?: number | null
  notNull?: boolean
  primaryKey?: boolean
  unique?: boolean
}
type SchemaMetadata = Map<string, Map<string, SchemaColumnMetadata>>
interface PublicationTableRef {
  table: string
  schema: string
  tableName: string
}
interface ChangeTrackingMetadata {
  table: PublicationTableRef
  operation: 'INSERT' | 'UPDATE' | 'DELETE'
  returningSQL: string
  returnRows: boolean
  returningProjection?: ReturningProjection
}
interface ChangeTrackingRequest {
  tableName: string
  operation: ChangeTrackingMetadata['operation']
  returnRows: boolean
  rowColumns?: string[]
  transactionID?: string
}
type ReturningProjectionItem =
  | { kind: 'all' }
  | { kind: 'column'; source: string; name: string }
  | { kind: 'expression'; source: string; name: string }
interface ReturningProjection {
  items: ReturningProjectionItem[]
}
type SchemaMetadataChange =
  | { action: 'renameTable'; from: PublicationTableRef; to: PublicationTableRef }
  | { action: 'renameColumn'; table: PublicationTableRef; from: string; to: string }
interface PublicationChange {
  action: 'create' | 'drop' | 'add' | 'set' | 'remove'
  name: string
  allTables?: boolean
  schemas?: string[]
  tables?: PublicationTableRef[]
}
interface PublicationDefinition {
  name: string
  allTables: boolean
  schemas: Set<string>
  tables: Map<string, PublicationTableRef>
}
interface TriggerFunctionDefinition {
  name: string
  body: string
}
interface TransactionMetadataSnapshot {
  schemaMetadata: SchemaMetadata
  publications: Map<string, PublicationDefinition>
  skippedFunctionNames: Set<string>
  triggerFunctions: Map<string, TriggerFunctionDefinition>
}

// per-DoBackend cache. with ~20 concurrent DoBackend sessions sharing one 128MB
// DO isolate, 2048 entries each (each holding rewritten SQL + schema metadata)
// is needless heap — the distinct-statement working set per session during sync
// is small. 256 keeps the hot path cached while bounding aggregate footprint.
const MAX_REWRITE_CACHE_ENTRIES = 256
const METADATA_TABLE = '_orez_pg_metadata'
// how long reloadPublicationsIfEmpty waits between re-reads while publications
// stay empty — short enough that the first write after a concurrently-created
// publication picks it up quickly, long enough that a genuinely
// publication-less db doesn't re-query on every write.
const EMPTY_PUBLICATION_RELOAD_THROTTLE_MS = 1000

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

function i64(v: bigint, buf = new ArrayBuffer(8)): Uint8Array {
  new DataView(buf).setBigInt64(0, v)
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

function wireValue(
  row: Record<string, unknown>,
  field: { name: string; oid?: number }
): string {
  const val = row[field.name]
  if (field.oid === PG_TYPE_BOOL) {
    if (val === true || val === 1 || val === '1' || val === 't' || val === 'true')
      return 't'
    if (val === false || val === 0 || val === '0' || val === 'f' || val === 'false')
      return 'f'
  }
  if (isTimestampOid(field.oid)) return postgresTimestampText(val)
  if (field.oid === PG_TYPE_JSON || field.oid === PG_TYPE_JSONB) {
    return typeof val === 'string' ? val : JSON.stringify(val)
  }
  return typeof val === 'object' ? JSON.stringify(val) : String(val)
}

function buildDataRow(
  row: Record<string, unknown>,
  fields: { name: string; oid?: number }[]
): Uint8Array {
  const colParts: Uint8Array[] = []
  for (const field of fields) {
    const val = row[field.name]
    if (val === null || val === undefined) {
      colParts.push(int4(-1))
    } else {
      const str = wireValue(row, field)
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

/**
 * map a SQLite error message to the closest PostgreSQL SQLSTATE. zero-cache and
 * pg clients (e.g. @take-out/database's migrate) branch on the SQLSTATE code,
 * not the message — notably to treat "already exists" / "does not exist" DDL as
 * idempotent during migration replay. without this the DO backend reported
 * everything as XX000 (internal_error), so a re-applied `ADD COLUMN` aborted the
 * whole migration instead of being recorded as applied. codes mirror the ones
 * postgres returns for the equivalent failures.
 */
function sqlstateForSqliteError(message: string): string {
  const m = message.toLowerCase()
  // duplicate-object DDL (idempotent on replay)
  if (m.includes('duplicate column name')) return '42701' // duplicate_column
  if (/\btable\b[^]*already exists/.test(m)) return '42P07' // duplicate_table
  if (/already exists/.test(m)) return '42710' // duplicate_object (index/trigger/view/etc)
  // missing-object DDL (idempotent for DROP ... without IF EXISTS)
  if (m.includes('no such column')) return '42703' // undefined_column
  if (m.includes('no such table')) return '42P01' // undefined_table
  if (m.includes('no such index') || m.includes('no such trigger')) return '42704' // undefined_object
  if (m.includes('syntax error')) return '42601' // syntax_error
  if (m.includes('unique constraint failed')) return '23505' // unique_violation
  if (m.includes('not null constraint failed')) return '23502' // not_null_violation
  if (m.includes('foreign key constraint failed')) return '23503' // foreign_key_violation
  return 'XX000' // internal_error
}

function buildErrorResponse(message: string, sqlstate?: string): Uint8Array {
  const field = (code: string, value: string) =>
    concat(textEncoder.encode(code), cstr(value))
  return msg(
    0x45,
    concat(
      field('S', 'ERROR'),
      field('V', 'ERROR'),
      field('C', sqlstate ?? sqlstateForSqliteError(message)),
      field('M', message),
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
  return msg(0x74, concat(i16(oids.length), ...oids.map((oid) => uint4(oid))))
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

function extractParseParamOIDs(data: Uint8Array): number[] {
  let offset = 5
  while (offset < data.length && data[offset] !== 0) offset++
  offset++
  while (offset < data.length && data[offset] !== 0) offset++
  offset++
  if (offset + 2 > data.length) return []
  const count = new DataView(data.buffer, data.byteOffset + offset, 2).getInt16(0)
  offset += 2
  const oids: number[] = []
  for (let i = 0; i < count && offset + 4 <= data.length; i++) {
    oids.push(new DataView(data.buffer, data.byteOffset + offset, 4).getUint32(0))
    offset += 4
  }
  return oids
}

function extractBindStatementName(data: Uint8Array): string {
  let offset = 5
  while (offset < data.length && data[offset] !== 0) offset++
  offset++
  const start = offset
  while (offset < data.length && data[offset] !== 0) offset++
  return textDecoder.decode(data.subarray(start, offset))
}

function extractBindPortalName(data: Uint8Array): string {
  const start = 5
  let offset = start
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

function extractExecutePortalName(data: Uint8Array): string {
  const start = 5
  let offset = start
  while (offset < data.length && data[offset] !== 0) offset++
  return textDecoder.decode(data.subarray(start, offset))
}

function extractCloseType(data: Uint8Array): 'S' | 'P' {
  return data[5] === 0x53 ? 'S' : 'P'
}

function extractCloseName(data: Uint8Array): string {
  const start = 6
  let offset = start
  while (offset < data.length && data[offset] !== 0) offset++
  return textDecoder.decode(data.subarray(start, offset))
}

function stripTrailingSemicolon(sql: string): string {
  return sql.trim().replace(/;+\s*$/, '')
}

function isSelectLike(sql: string): boolean {
  return /^\s*(select|with)\b/i.test(sql)
}

function splitTopLevelComma(input: string): string[] {
  const parts: string[] = []
  let start = 0
  let depth = 0
  let quote: string | null = null
  for (let i = 0; i < input.length; i++) {
    const ch = input[i]
    if (quote) {
      if (ch === quote) {
        if (quote === "'" && input[i + 1] === "'") {
          i++
        } else if (quote === '"' && input[i + 1] === '"') {
          i++
        } else {
          quote = null
        }
      }
      continue
    }
    if (ch === "'" || ch === '"') {
      quote = ch
      continue
    }
    if (ch === '(') depth++
    else if (ch === ')') depth = Math.max(0, depth - 1)
    else if (ch === ',' && depth === 0) {
      parts.push(input.slice(start, i).trim())
      start = i + 1
    }
  }
  const tail = input.slice(start).trim()
  if (tail) parts.push(tail)
  return parts
}

function topLevelKeywordIndex(input: string, keyword: string): number {
  let depth = 0
  let quote: string | null = null
  const lower = input.toLowerCase()
  for (let i = 0; i < input.length; i++) {
    const ch = input[i]
    if (quote) {
      if (ch === quote) {
        if (quote === "'" && input[i + 1] === "'") {
          i++
        } else if (quote === '"' && input[i + 1] === '"') {
          i++
        } else {
          quote = null
        }
      }
      continue
    }
    if (ch === "'" || ch === '"') {
      quote = ch
      continue
    }
    if (ch === '(') depth++
    else if (ch === ')') depth = Math.max(0, depth - 1)
    else if (
      depth === 0 &&
      lower.startsWith(keyword, i) &&
      (i === 0 || /\W/.test(lower[i - 1])) &&
      (i + keyword.length >= lower.length || /\W/.test(lower[i + keyword.length]))
    ) {
      return i
    }
  }
  return -1
}

function extractTopLevelSelectList(sql: string): string | null {
  const normalized = stripTrailingSemicolon(sql)
  const selectIndex = topLevelKeywordIndex(normalized, 'select')
  if (selectIndex < 0) return null
  const fromIndex = topLevelKeywordIndex(normalized.slice(selectIndex + 6), 'from')
  if (fromIndex < 0) return normalized.slice(selectIndex + 6).trim()
  return normalized.slice(selectIndex + 6, selectIndex + 6 + fromIndex).trim()
}

function inferFieldName(expression: string): string {
  const trimmed = expression.trim()
  const quotedAlias = /\bas\s+"([^"]+)"\s*$/i.exec(trimmed)
  if (quotedAlias) return quotedAlias[1]
  const bareAlias = /\bas\s+([a-z_][\w$]*)\s*$/i.exec(trimmed)
  if (bareAlias) return bareAlias[1]
  const quotedTail = /"([^"]+)"\s*$/.exec(trimmed)
  if (quotedTail) return quotedTail[1]
  const dottedTail = /(?:^|\.)([a-z_][\w$]*)\s*$/i.exec(trimmed)
  if (dottedTail) return dottedTail[1]
  const fn = /^([a-z_][\w$]*)\s*\(/i.exec(trimmed)
  if (fn) return fn[1]
  return '?column?'
}

function inferFieldsFromSQL(sql: string): { name: string; oid?: number }[] {
  const returningIndex = topLevelKeywordIndex(sql, 'returning')
  const list =
    returningIndex >= 0
      ? stripTrailingSemicolon(sql.slice(returningIndex + 'returning'.length))
      : extractTopLevelSelectList(sql)
  if (!list || list === '*') return []
  return splitTopLevelComma(list).map((part) => ({ name: inferFieldName(part) }))
}

function columnRefTailName(value: any): string | null {
  const fields = value?.ColumnRef?.fields
  if (!Array.isArray(fields) || fields.length === 0) return null
  return stringValue(fields[fields.length - 1]) ?? null
}

function expressionOid(value: any): number | undefined {
  if (value?.TypeCast) {
    return (
      wireOidForTypeName(value.TypeCast.typeName) ?? expressionOid(value.TypeCast.arg)
    )
  }
  const node = value
  if (!node || typeof node !== 'object') return undefined
  if (node.SubLink?.subLinkType === 'EXISTS_SUBLINK') return PG_TYPE_BOOL
  if (node.FuncCall) {
    const name = functionName(node.FuncCall)
    if (name && JSON_PRODUCING_FUNCTIONS.has(name)) return PG_TYPE_JSON
  }
  if (node.CoalesceExpr) {
    for (const arg of node.CoalesceExpr.args ?? []) {
      const oid = expressionOid(arg)
      if (oid) return oid
    }
  }
  // json access operators `->` and `#>` return a json/jsonb VALUE in postgres
  // (vs `->>`/`#>>` which return text). sqlite's `->` returns the json text
  // representation (e.g. `"begin"` with quotes), so the column must carry a
  // json oid for the driver to JSON.parse it back into a value — otherwise
  // zero-cache's changeLog catchup reads `change->'tag'` as the literal
  // string `"begin"` and its `case "begin"` switch never matches, mis-tagging
  // every begin/commit as a `data` change and poisoning the change stream.
  if (node.A_Expr?.kind === 'AEXPR_OP') {
    const op = operatorName(node.A_Expr)
    if (op === '->' || op === '#>') return PG_TYPE_JSON
  }
  return undefined
}

function selectResultColumnMetadata(
  sql: string
): Map<string, { source?: string; oid?: number }> {
  const columns = new Map<string, { source?: string; oid?: number }>()
  try {
    const parsed = parseSync(stripTrailingSemicolon(sql.trim()))
    const select = parsed.stmts[0]?.stmt?.SelectStmt
    if (!select) return columns
    for (const targetNode of select.targetList ?? []) {
      const target = targetNode.ResTarget
      if (!target) continue
      const source = columnRefTailName(unwrapTypeCast(target.val))
      const func = unwrapTypeCast(target.val)?.FuncCall
      const funcSource = func ? functionDisplayName(func) : null
      const output = target.name ?? source ?? funcSource
      if (!output) continue
      columns.set(output, {
        ...(source ? { source } : null),
        ...(expressionOid(target.val) ? { oid: expressionOid(target.val) } : null),
      })
    }
  } catch {}
  return columns
}

function firstSourceTableFromSQL(sql: string): string | null {
  try {
    const parsed = parseSync(stripTrailingSemicolon(sql.trim()))
    return firstSourceTable(parsed.stmts[0]?.stmt)
  } catch {
    return null
  }
}

function setInferredParamOid(oids: number[], paramNumber: number, oid?: number): void {
  if (!oid || paramNumber <= 0) return
  while (oids.length < paramNumber) oids.push(0)
  if (!oids[paramNumber - 1]) oids[paramNumber - 1] = oid
}

function paramOidForTypeName(typeName: any): number | undefined {
  const metadata = pgTypeMetadataForTypeName(typeName)
  return metadata.typeOid ?? metadata.oid
}

function collectParamRefs(value: any, visit: (paramNumber: number) => void): void {
  if (!value || typeof value !== 'object') return
  if (Array.isArray(value)) {
    for (const item of value) collectParamRefs(item, visit)
    return
  }
  const paramNumber = value.ParamRef?.number
  if (paramNumber) visit(paramNumber)
  for (const child of Object.values(value)) collectParamRefs(child, visit)
}

function inferInsertParamOids(
  stmt: any,
  oids: number[],
  schemaMetadata: SchemaMetadata
): void {
  const table = flattenedRangeVarName(stmt.relation)
  const columns = stmt.cols
    ?.map((column: any) => column?.ResTarget?.name)
    .filter((name: unknown): name is string => typeof name === 'string')
  const valuesLists = stmt.selectStmt?.SelectStmt?.valuesLists
  if (!table || !columns?.length || !Array.isArray(valuesLists)) return

  const metadata = schemaMetadata.get(table)
  for (const valuesList of valuesLists) {
    const items = valuesList?.List?.items
    if (!Array.isArray(items)) continue
    for (let index = 0; index < Math.min(columns.length, items.length); index++) {
      const column =
        metadata?.get(columns[index]) ?? fallbackMetadataForColumnName(columns[index])
      collectParamRefs(items[index], (paramNumber) =>
        setInferredParamOid(oids, paramNumber, column?.oid)
      )
    }
  }

  for (const targetNode of stmt.onConflictClause?.targetList ?? []) {
    const target = targetNode.ResTarget
    if (!target?.name) continue
    const column =
      metadata?.get(target.name) ?? fallbackMetadataForColumnName(target.name)
    collectParamRefs(target.val, (paramNumber) =>
      setInferredParamOid(oids, paramNumber, column?.oid)
    )
  }
}

function inferUpdateParamOids(
  stmt: any,
  oids: number[],
  schemaMetadata: SchemaMetadata
): void {
  const table = flattenedRangeVarName(stmt.relation)
  if (!table) return
  const metadata = schemaMetadata.get(table)
  for (const targetNode of stmt.targetList ?? []) {
    const target = targetNode.ResTarget
    if (!target?.name) continue
    const column =
      metadata?.get(target.name) ?? fallbackMetadataForColumnName(target.name)
    collectParamRefs(target.val, (paramNumber) =>
      setInferredParamOid(oids, paramNumber, column?.oid)
    )
  }
}

function jsonInputParamOid(name: string | null, argIndex: number): number | undefined {
  switch (name) {
    case 'json_to_recordset':
    case 'json_populate_recordset':
    case 'json_each':
    case 'json_each_text':
    case 'json_array_elements':
    case 'json_array_elements_text':
    case 'json_array_length':
    case 'json_extract':
    case 'json_remove':
    case 'json_replace':
    case 'json_insert':
    case 'json_set':
    case 'json_patch':
    case 'json_type':
    case 'json_valid':
    case 'json':
      return argIndex === 0 ? PG_TYPE_JSON : undefined
    case 'jsonb_to_recordset':
    case 'jsonb_populate_recordset':
    case 'jsonb_array_elements':
    case 'jsonb_array_elements_text':
    case 'jsonb_array_length':
    case 'jsonb_set':
      return argIndex === 0 || (name === 'jsonb_set' && argIndex === 2)
        ? PG_TYPE_JSONB
        : undefined
    default:
      return undefined
  }
}

function columnOidForExpression(
  value: any,
  schemaMetadata: SchemaMetadata
): number | undefined {
  const fields = value?.ColumnRef?.fields
  if (!Array.isArray(fields) || fields.length === 0) return undefined
  const column = stringValue(fields[fields.length - 1])
  if (!column) return undefined

  if (fields.length >= 2) {
    const table = stringValue(fields[fields.length - 2])
    const metadata = table ? schemaMetadata.get(table)?.get(column) : undefined
    if (metadata) return metadata.typeOid ?? metadata.oid
  }

  let found: number | undefined
  for (const columns of schemaMetadata.values()) {
    const metadata = columns.get(column)
    if (!metadata) continue
    const oid = metadata.typeOid ?? metadata.oid
    if (found && found !== oid) return undefined
    found = oid
  }
  return found
}

function inferAExprParamOids(
  expr: any,
  oids: number[],
  schemaMetadata: SchemaMetadata,
  expectedOid?: number
): void {
  if (expr?.kind === 'AEXPR_OP_ANY' || expr?.kind === 'AEXPR_OP_ALL') {
    const elementOid = columnOidForExpression(expr.lexpr, schemaMetadata)
    inferExpressionParamOids(expr.lexpr, oids, schemaMetadata)
    inferExpressionParamOids(
      expr.rexpr,
      oids,
      schemaMetadata,
      elementOid ? arrayTypeOidForElementOid(elementOid) : undefined
    )
    return
  }

  const leftOid = columnOidForExpression(expr.lexpr, schemaMetadata)
  const rightOid = columnOidForExpression(expr.rexpr, schemaMetadata)
  inferExpressionParamOids(expr.lexpr, oids, schemaMetadata, rightOid ?? expectedOid)
  inferExpressionParamOids(expr.rexpr, oids, schemaMetadata, leftOid ?? expectedOid)
}

function inferExpressionParamOids(
  value: any,
  oids: number[],
  schemaMetadata: SchemaMetadata,
  expectedOid?: number
): void {
  if (!value || typeof value !== 'object') return
  if (Array.isArray(value)) {
    for (const item of value) inferExpressionParamOids(item, oids, schemaMetadata)
    return
  }

  const paramNumber = value.ParamRef?.number
  if (paramNumber) {
    setInferredParamOid(oids, paramNumber, expectedOid)
    return
  }

  if (value.TypeCast) {
    const castOid = paramOidForTypeName(value.TypeCast.typeName)
    inferExpressionParamOids(
      value.TypeCast.arg,
      oids,
      schemaMetadata,
      castOid === PG_TYPE_TEXT && expectedOid ? expectedOid : (castOid ?? expectedOid)
    )
    return
  }

  if (value.FuncCall) {
    const name = functionName(value.FuncCall)
    const args = value.FuncCall.args ?? []
    for (let index = 0; index < args.length; index++) {
      inferExpressionParamOids(
        args[index],
        oids,
        schemaMetadata,
        jsonInputParamOid(name, index) ?? expectedOid
      )
    }
    for (const [key, child] of Object.entries(value.FuncCall)) {
      if (key === 'args' || key === 'funcname') continue
      inferExpressionParamOids(child, oids, schemaMetadata)
    }
    return
  }

  if (value.A_Expr) {
    inferAExprParamOids(value.A_Expr, oids, schemaMetadata, expectedOid)
    return
  }

  if (value.CoalesceExpr) {
    for (const arg of value.CoalesceExpr.args ?? [])
      inferExpressionParamOids(arg, oids, schemaMetadata, expectedOid)
    return
  }

  if (value.CaseExpr) {
    inferExpressionParamOids(value.CaseExpr.arg, oids, schemaMetadata)
    for (const caseWhenNode of value.CaseExpr.args ?? []) {
      const caseWhen = caseWhenNode.CaseWhen
      if (!caseWhen) continue
      inferExpressionParamOids(caseWhen.expr, oids, schemaMetadata)
      inferExpressionParamOids(caseWhen.result, oids, schemaMetadata, expectedOid)
    }
    inferExpressionParamOids(value.CaseExpr.defresult, oids, schemaMetadata, expectedOid)
    return
  }

  for (const child of Object.values(value)) {
    inferExpressionParamOids(child, oids, schemaMetadata)
  }
}

function inferParamOidsForSQL(
  sql: string,
  paramOids: number[],
  schemaMetadata: SchemaMetadata
): number[] {
  const inferred = [...paramOids]
  try {
    const parsed = parseSync(stripTrailingSemicolon(sql.trim()))
    for (const rawStmt of parsed.stmts ?? []) {
      const stmt = rawStmt.stmt
      if (stmt?.InsertStmt)
        inferInsertParamOids(stmt.InsertStmt, inferred, schemaMetadata)
      else if (stmt?.UpdateStmt)
        inferUpdateParamOids(stmt.UpdateStmt, inferred, schemaMetadata)
      inferExpressionParamOids(stmt, inferred, schemaMetadata)
    }
  } catch {}
  return inferred
}

// ── Catalog query interception ────────────────────────────────────────────

function walkAst(node: any, visit: (node: any) => void): void {
  if (!node || typeof node !== 'object') return
  visit(node)
  if (Array.isArray(node)) {
    for (const item of node) walkAst(item, visit)
    return
  }
  for (const value of Object.values(node)) walkAst(value, visit)
}

function isCatalogRelation(rangeVar: any): boolean {
  const schema = String(rangeVar?.schemaname ?? '').toLowerCase()
  const rel = String(rangeVar?.relname ?? '').toLowerCase()
  if (schema === 'pg_catalog' || schema === 'information_schema') return true
  return rel.startsWith('pg_')
}

function isCatalogFunction(funcCall: any): boolean {
  const name = functionName(funcCall)
  if (!name) return false
  if (SQLITE_FUNCTION_BY_PG_FUNCTION.has(name)) return false
  if (name === 'current_setting') return true
  if (name.startsWith('pg_') || name.startsWith('has_')) return true
  return name === 'obj_description' || name === 'format_type'
}

function isCatalogStatement(stmt: any): boolean {
  let catalog = false
  walkAst(stmt, (node) => {
    if (catalog) return
    if (node.RangeVar && isCatalogRelation(node.RangeVar)) catalog = true
    else if (node.FuncCall && isCatalogFunction(node.FuncCall)) catalog = true
  })
  return catalog
}

function isCatalogQuery(sql: string): boolean {
  try {
    const parsed = parseSync(stripTrailingSemicolon(sql.trim()))
    return parsed.stmts.some((statement: any) => isCatalogStatement(statement.stmt))
  } catch {
    return false
  }
}

// ── SQL rewriting ─────────────────────────────────────────────────────────

interface RewrittenStatement {
  sql: string
  isDDL?: boolean
  isWrite?: boolean
  writeTable?: PublicationTableRef
  changeTracking?: ChangeTrackingMetadata
  usesPublishedSchemaFunction?: boolean
  arrayParamNumbers?: Set<number>
  jsonParamNumbers?: Set<number>
  epochMillisParamNumbers?: Set<number>
  schemaColumns?: SchemaColumnMetadata[]
  schemaMetadataChanges?: SchemaMetadataChange[]
  publicationChanges?: PublicationChange[]
  skipIfColumnExists?: { table: string; column: string }
  skipIfColumnMissing?: { table: string; column: string }
  skipIfTableEmpty?: { table: string }
  // FK cascade: child DELETE/UPDATE statements to execute (leaves-first) BEFORE
  // this parent DELETE, restoring ON DELETE CASCADE/SET NULL semantics the store
  // lost when FKs were stripped. each is a normal RewrittenStatement (already
  // flattened + change-tracked) run as its own bound exec, so the shared params
  // bind correctly and the deletion replicates like any other write.
  cascadeStatements?: RewrittenStatement[]
  // set when a CREATE TABLE contributed FK edges — triggers metadata persist +
  // rewrite-cache invalidation so later DELETEs pick up the cascade.
  fkEdges?: boolean
}

interface RewriteContext {
  skippedFunctionNames?: Set<string>
  triggerFunctions?: Map<string, TriggerFunctionDefinition>
  arrayParamNumbers?: Set<number>
  jsonParamNumbers?: Set<number>
  epochMillisParamNumbers?: Set<number>
  fkRegistry?: FkCascadeRegistry
  // set while rewriting expanded cascade children, so they don't re-expand.
  suppressFkCascade?: boolean
}

const SKIPPED_NODE_TYPES = new Set([
  'AlterDefaultPrivilegesStmt',
  'ClosePortalStmt',
  'ClusterStmt',
  'CommentStmt',
  'CreateEventTrigStmt',
  'CreateExtensionStmt',
  'CreateFunctionStmt',
  'CreatePublicationStmt',
  'CreateSchemaStmt',
  'CreateTrigStmt',
  'CreatedbStmt',
  'DeallocateStmt',
  'DiscardStmt',
  'DoStmt',
  'GrantStmt',
  'ListenStmt',
  'LockStmt',
  'NotifyStmt',
  'UnlistenStmt',
  'VariableSetStmt',
  'VariableShowStmt',
])

const SKIPPED_DROP_OBJECTS = new Set([
  'OBJECT_FUNCTION',
  'OBJECT_EVENT_TRIGGER',
  'OBJECT_PUBLICATION',
  'OBJECT_TRIGGER',
])
const UNSUPPORTED_INDEX_METHODS = new Set(['gin', 'gist', 'ivfflat', 'hnsw'])
const UNSUPPORTED_GENERATED_COLUMN_FUNCTIONS = new Set(['setweight', 'to_tsvector'])
const skippedFunctionNamesByTarget = new Map<string, Set<string>>()
const SQLITE_FUNCTION_BY_PG_FUNCTION = new Map([
  ['json_agg', 'json_group_array'],
  ['json_build_object', 'json_object'],
  ['json_object_agg', 'json_group_object'],
  ['jsonb_agg', 'json_group_array'],
  ['jsonb_array_elements', 'json_each'],
  ['jsonb_array_elements_text', 'json_each'],
  ['jsonb_array_length', 'json_array_length'],
  ['jsonb_build_object', 'json_object'],
  ['jsonb_set', 'json_set'],
  ['json_array_elements', 'json_each'],
  ['json_array_elements_text', 'json_each'],
  ['pg_column_size', 'length'],
])
const JSON_PRODUCING_FUNCTIONS = new Set([
  'json_agg',
  'json_build_object',
  'json_group_array',
  'json_group_object',
  'json_object',
  'json_object_agg',
  'jsonb_agg',
  'jsonb_build_object',
  'jsonb_set',
  'json_set',
])
const TRACKED_SHARD_TABLES = new Set(['clients', 'mutations'])
const SQLITE_TYPE_BY_PG_TYPE = new Map([
  ['bigint', 'integer'],
  ['bigserial', 'integer'],
  ['bool', 'integer'],
  ['boolean', 'integer'],
  ['bytea', 'blob'],
  ['float4', 'real'],
  ['float8', 'real'],
  ['int2', 'integer'],
  ['int4', 'integer'],
  ['int8', 'integer'],
  ['json', 'text'],
  ['jsonb', 'text'],
  ['numeric', 'real'],
  ['serial', 'integer'],
  ['serial4', 'integer'],
  ['serial8', 'integer'],
  ['timestamp', 'text'],
  ['timestamptz', 'text'],
  ['tsvector', 'text'],
  ['vector', 'text'],
])

function stringNode(value: string): any {
  return { String: { sval: value } }
}

function cloneAst<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T
}

function stringValue(node: any): string | null {
  return node?.String?.sval ?? null
}

function statementNodeType(stmt: any): string {
  return Object.keys(stmt)[0]
}

function quoteIdentifier(value: string): string {
  return `"${value.replace(/"/g, '""')}"`
}

function unquoteIdentifier(value: string): string {
  const trimmed = value.trim()
  if (trimmed.startsWith('"') && trimmed.endsWith('"')) {
    return trimmed.slice(1, -1).replace(/""/g, '"')
  }
  return trimmed
}

function splitQualifiedIdentifier(value: string): string[] {
  const parts: string[] = []
  let current = ''
  let quoted = false
  for (let i = 0; i < value.length; i++) {
    const ch = value[i]
    if (ch === '"') {
      current += ch
      if (quoted && value[i + 1] === '"') {
        current += value[++i]
      } else {
        quoted = !quoted
      }
      continue
    }
    if (ch === '.' && !quoted) {
      parts.push(current.trim())
      current = ''
      continue
    }
    current += ch
  }
  if (current.trim()) parts.push(current.trim())
  return parts
}

function flattenSchemaName(schema: string, name: string): string {
  if (schema === 'public' && name === 'migrations') return 'public_migrations'
  if (schema === 'public') return name
  if (schema === '_orez' && name === '_zero_changes') return '_zero_changes'
  if (schema === '_orez' && name === '_zero_replication_slots')
    return '_orez__zero_replication_slots'
  if (schema === '_orez') return `_orez__${name}`
  if (schema === '_zero') return `_zero_${name}`
  return `${schema}_${name}`
}

// canonical FK-registry key for a table: schema-qualified PG name (un-flattened),
// quoted. used for BOTH capture (CREATE TABLE child + parent) and lookup (DELETE
// target), so they always agree. expansion emits PG SQL under these names and
// each child re-enters rewriteParsedStatement, which flattens + tracks it exactly
// like a normal delete — so cascade tracking is identical to hand-written deletes.
function fkTableKey(ref: { schemaname?: string; relname: string }): string {
  return `${quoteIdentifier(ref.schemaname ?? 'public')}.${quoteIdentifier(ref.relname)}`
}

function flattenRangeVar(rangeVar: any): string {
  if (!rangeVar?.schemaname) return rangeVar?.relname
  const flattened = flattenSchemaName(rangeVar.schemaname, rangeVar.relname)
  rangeVar.relname = flattened
  delete rangeVar.schemaname
  return flattened
}

function flattenedRangeVarName(rangeVar: any): string | null {
  if (!rangeVar?.relname) return null
  return rangeVar.schemaname
    ? flattenSchemaName(rangeVar.schemaname, rangeVar.relname)
    : rangeVar.relname
}

function flattenSQLIdentifier(value: string): string {
  const parts = splitQualifiedIdentifier(value).map(unquoteIdentifier)
  if (parts.length >= 2) return quoteIdentifier(flattenSchemaName(parts[0], parts[1]))
  return quoteIdentifier(parts[0] ?? value)
}

function publicationTableRefForRangeVar(rangeVar: any): PublicationTableRef | null {
  if (!rangeVar?.relname) return null
  const schema = rangeVar.schemaname ?? 'public'
  const tableName = rangeVar.relname
  return {
    table: flattenSchemaName(schema, tableName),
    schema,
    tableName,
  }
}

function renamedPublicationTableRef(
  from: PublicationTableRef,
  tableName: string
): PublicationTableRef {
  return {
    table: flattenSchemaName(from.schema, tableName),
    schema: from.schema,
    tableName,
  }
}

function flattenColumnRef(columnRef: any): void {
  const fields = columnRef?.fields
  if (!Array.isArray(fields) || fields.length < 3) return
  const schema = stringValue(fields[0])
  const table = stringValue(fields[1])
  if (!schema || !table) return
  fields.splice(0, 2, stringNode(flattenSchemaName(schema, table)))
}

function rewriteColumnRefQualifier(node: any, from: string, to: string): void {
  if (!node || typeof node !== 'object') return
  if (Array.isArray(node)) {
    for (const item of node) rewriteColumnRefQualifier(item, from, to)
    return
  }
  const fields = node.ColumnRef?.fields
  if (Array.isArray(fields) && fields.length > 1 && stringValue(fields[0]) === from) {
    fields[0] = stringNode(to)
  }
  for (const child of Object.values(node)) {
    rewriteColumnRefQualifier(child, from, to)
  }
}

function functionName(funcCall: any): string | null {
  const parts = funcCall?.funcname
  if (!Array.isArray(parts) || parts.length === 0) return null
  return stringValue(parts[parts.length - 1])?.toLowerCase() ?? null
}

function functionDisplayName(funcCall: any): string | null {
  const parts = funcCall?.funcname
  if (!Array.isArray(parts) || parts.length === 0) return null
  return stringValue(parts[parts.length - 1])
}

function createFunctionName(stmt: any): string | null {
  return functionName({ funcname: stmt.funcname })
}

function createTriggerFunctionDefinition(stmt: any): TriggerFunctionDefinition | null {
  const name = createFunctionName(stmt)
  if (!name || typeNameBase(stmt.returnType) !== 'trigger') return null
  let language = ''
  let body = ''
  for (const option of stmt.options ?? []) {
    const def = option.DefElem
    if (!def) continue
    if (def.defname === 'language') language = stringValue(def.arg)?.toLowerCase() ?? ''
    if (def.defname === 'as') {
      const items = def.arg?.List?.items
      if (Array.isArray(items) && items[0]) body = stringValue(items[0]) ?? ''
    }
  }
  if (language !== 'plpgsql' || !body.trim()) return null
  return { name, body }
}

function intConst(value: number): any {
  return { A_Const: { ival: value === 0 ? {} : { ival: value } } }
}

function nullConst(): any {
  return { A_Const: { isnull: true } }
}

function stringConst(value: string): any {
  return { A_Const: { sval: { sval: value } } }
}

function columnRefNode(...names: string[]): any {
  return {
    ColumnRef: {
      fields: names.map(stringNode),
      location: -1,
    },
  }
}

function funcCallNode(name: string, args: any[] = []): any {
  return {
    FuncCall: {
      funcname: [stringNode(name)],
      args,
      funcformat: 'COERCE_EXPLICIT_CALL',
      location: -1,
    },
  }
}

function jsonPathForColumn(column: string): string {
  if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(column)) return `$.${column}`
  return `$.${JSON.stringify(column)}`
}

function equalityExpr(left: any, right: any): any {
  return {
    A_Expr: {
      kind: 'AEXPR_OP',
      name: [stringNode('=')],
      lexpr: left,
      rexpr: right,
      location: -1,
    },
  }
}

function numericLiteralValue(value: any): number | null {
  const literal = astLiteralValue(unwrapTypeCast(value))
  if (typeof literal !== 'number') return null
  return Number.isFinite(literal) ? literal : null
}

function isDivisionByMillis(value: any): boolean {
  const expr = unwrapTypeCast(value)?.A_Expr
  if (expr?.kind !== 'AEXPR_OP' || operatorName(expr) !== '/') return false
  return numericLiteralValue(expr.rexpr) === 1000
}

function startsWithExpr(func: any, context?: RewriteContext): any | null {
  const args = func.args ?? []
  if (args.length < 2) return null
  return equalityExpr(
    funcCallNode('instr', [rewriteNode(args[0], context), rewriteNode(args[1], context)]),
    intConst(1)
  )
}

function inExpr(left: any, values: any[]): any {
  return {
    A_Expr: {
      kind: 'AEXPR_IN',
      name: [stringNode('=')],
      lexpr: left,
      rexpr: { List: { items: values } },
      location: -1,
    },
  }
}

function operatorName(expr: any): string | null {
  return stringValue(expr?.name?.[0])
}

function isAsciiAlpha(ch: string): boolean {
  const code = ch.charCodeAt(0)
  return (code >= 65 && code <= 90) || (code >= 97 && code <= 122)
}

function isAsciiDigit(ch: string): boolean {
  const code = ch.charCodeAt(0)
  return code >= 48 && code <= 57
}

function isDollarQuoteTagStart(ch: string): boolean {
  return ch === '_' || isAsciiAlpha(ch)
}

function isDollarQuoteTagPart(ch: string): boolean {
  return ch === '_' || isAsciiAlpha(ch) || isAsciiDigit(ch)
}

function currentTimestampNode(): any {
  return { SQLValueFunction: { op: 'SVFOP_CURRENT_TIMESTAMP', typmod: -1 } }
}

function isTimestampOid(oid: number | undefined): boolean {
  return oid === PG_TYPE_TIMESTAMP || oid === PG_TYPE_TIMESTAMPTZ
}

function isBooleanOid(oid: number | undefined): boolean {
  return oid === PG_TYPE_BOOL
}

function isJsonOid(oid: number | undefined): boolean {
  return oid === PG_TYPE_JSON || oid === PG_TYPE_JSONB
}

function finiteNumberFromPlainString(value: string): number | null {
  const trimmed = value.trim()
  if (!trimmed) return null
  let start = 0
  if (trimmed[0] === '-' || trimmed[0] === '+') start = 1
  if (start === trimmed.length) return null

  let sawDigit = false
  let sawDot = false
  for (let i = start; i < trimmed.length; i++) {
    const ch = trimmed[i]
    if (isAsciiDigit(ch)) {
      sawDigit = true
      continue
    }
    if (ch === '.' && !sawDot) {
      sawDot = true
      continue
    }
    return null
  }
  if (!sawDigit) return null
  const number = Number(trimmed)
  return Number.isFinite(number) ? number : null
}

function timestampMillisValue(value: unknown): number | null {
  if (typeof value === 'number') return Number.isFinite(value) ? value : null
  if (typeof value === 'string') return finiteNumberFromPlainString(value)
  return null
}

function postgresTimestampTextFromDate(value: Date): string {
  const raw = value.toISOString()
  const withSpace = raw.replace('T', ' ')
  return withSpace.endsWith('Z') ? `${withSpace.slice(0, -1)}+00` : withSpace
}

function postgresTimestampText(value: unknown): string {
  const millis = timestampMillisValue(value)
  if (millis !== null) {
    const date = new Date(millis)
    if (Number.isFinite(date.getTime())) return postgresTimestampTextFromDate(date)
  }
  const raw = value instanceof Date ? value.toISOString() : String(value)
  const withSpace = raw.replace('T', ' ')
  return withSpace.endsWith('Z') ? `${withSpace.slice(0, -1)}+00` : withSpace
}

function postgresQueryBoolean(value: unknown): unknown {
  if (value === true || value === 1 || value === '1' || value === 't' || value === 'true')
    return true
  if (
    value === false ||
    value === 0 ||
    value === '0' ||
    value === 'f' ||
    value === 'false'
  )
    return false
  return value
}

function postgresQueryJson(value: unknown): unknown {
  if (typeof value !== 'string') return value
  try {
    return JSON.parse(value)
  } catch {
    return value
  }
}

function postgresQueryTimestamp(value: unknown): unknown {
  if (value instanceof Date) return value
  const millis = timestampMillisValue(value)
  const date = millis !== null ? new Date(millis) : new Date(String(value))
  return Number.isFinite(date.getTime()) ? date : value
}

function postgresQueryValue(value: unknown, oid: number | undefined): unknown {
  if (value === null || value === undefined) return value
  if (oid === PG_TYPE_BOOL) return postgresQueryBoolean(value)
  if (oid === PG_TYPE_JSON || oid === PG_TYPE_JSONB) return postgresQueryJson(value)
  if (isTimestampOid(oid)) return postgresQueryTimestamp(value)
  return value
}

function epochMillisParamValue(value: unknown): unknown {
  const millis = timestampMillisValue(value)
  if (millis !== null) return millis
  const raw = value instanceof Date ? value.toISOString() : String(value)
  const date = new Date(raw)
  return Number.isFinite(date.getTime()) ? date.getTime() : value
}

function paramNumbersForOids(
  oids: number[],
  matches: (oid: number | undefined) => boolean
): Set<number> {
  const numbers = new Set<number>()
  for (let index = 0; index < oids.length; index++) {
    if (matches(oids[index])) numbers.add(index + 1)
  }
  return numbers
}

function parsePgArrayLiteral(value: string): unknown[] | null {
  if (value[0] !== '{' || value[value.length - 1] !== '}') return null
  const items: unknown[] = []
  let i = 1
  while (i < value.length - 1) {
    if (value[i] === ',') {
      i++
      continue
    }

    if (value[i] === '"') {
      i++
      let out = ''
      while (i < value.length - 1) {
        const ch = value[i]
        if (ch === '\\') {
          if (i + 1 < value.length - 1) out += value[i + 1]
          i += 2
          continue
        }
        if (ch === '"') {
          i++
          break
        }
        out += ch
        i++
      }
      items.push(out)
      continue
    }

    const start = i
    while (i < value.length - 1 && value[i] !== ',') i++
    const token = value.slice(start, i)
    items.push(token === 'NULL' ? null : token)
  }
  return items
}

function pgArrayLiteralToJson(value: string): string | null {
  const items = parsePgArrayLiteral(value)
  return items ? JSON.stringify(items) : null
}

function tryParseJson(value: string): { ok: true; value: unknown } | { ok: false } {
  try {
    return { ok: true, value: JSON.parse(value) }
  } catch {
    return { ok: false }
  }
}

function pgArrayLiteralToJsonDocument(value: string): string | null {
  const items = parsePgArrayLiteral(value)
  if (!items) return null
  return JSON.stringify(
    items.map((item) => {
      if (typeof item !== 'string') return item
      const parsed = tryParseJson(item)
      return parsed.ok ? parsed.value : item
    })
  )
}

function sqliteJsonParamValue(value: unknown): unknown {
  if (Array.isArray(value)) return JSON.stringify(value)
  if (value && typeof value === 'object') return JSON.stringify(value)
  if (typeof value !== 'string') return value
  const parsed = tryParseJson(value)
  if (parsed.ok) return value
  return pgArrayLiteralToJsonDocument(value) ?? value
}

function sqliteJsonArrayExpr(value: any, context?: RewriteContext): any {
  const paramNumber = value?.ParamRef?.number
  if (paramNumber) context?.arrayParamNumbers?.add(paramNumber)

  const literal = value?.A_Const?.sval?.sval
  if (typeof literal === 'string') {
    const json = pgArrayLiteralToJson(literal)
    if (json) return { A_Const: { sval: { sval: json } } }
  }

  return value
}

const NON_LITERAL_ARRAY_VALUE = Symbol('non-literal-array-value')

function astLiteralValue(value: any): unknown | typeof NON_LITERAL_ARRAY_VALUE {
  const constValue = value?.A_Const
  if (constValue) {
    if (Object.hasOwn(constValue, 'isnull')) return null
    if (Object.hasOwn(constValue, 'sval')) return constValue.sval?.sval ?? ''
    if (Object.hasOwn(constValue, 'ival')) return constValue.ival?.ival ?? 0
    if (Object.hasOwn(constValue, 'fval')) {
      const raw = constValue.fval?.fval ?? ''
      const number = Number(raw)
      return Number.isFinite(number) ? number : raw
    }
    if (Object.hasOwn(constValue, 'boolval')) {
      return constValue.boolval?.boolval === true
    }
  }

  const arrayExpr = value?.A_ArrayExpr
  if (arrayExpr) return arrayExpressionValue(arrayExpr)

  return NON_LITERAL_ARRAY_VALUE
}

function arrayExpressionValue(
  arrayExpr: any
): unknown[] | typeof NON_LITERAL_ARRAY_VALUE {
  const values: unknown[] = []
  for (const element of arrayExpr.elements ?? []) {
    const value = astLiteralValue(element)
    if (value === NON_LITERAL_ARRAY_VALUE) return NON_LITERAL_ARRAY_VALUE
    values.push(value)
  }
  return values
}

function rewriteArrayExpression(arrayExpr: any): any | null {
  const value = arrayExpressionValue(arrayExpr)
  if (value === NON_LITERAL_ARRAY_VALUE) return null
  return stringConst(JSON.stringify(value))
}

function jsonEachArraySubquery(value: any): any {
  return {
    SelectStmt: {
      targetList: [
        {
          ResTarget: {
            val: columnRefNode('value'),
            location: -1,
          },
        },
      ],
      fromClause: [
        {
          RangeFunction: {
            functions: [
              {
                List: {
                  items: [
                    {
                      FuncCall: {
                        funcname: [stringNode('json_each')],
                        args: [value],
                        funcformat: 'COERCE_EXPLICIT_CALL',
                        location: -1,
                      },
                    },
                    {},
                  ],
                },
              },
            ],
          },
        },
      ],
      limitOption: 'LIMIT_OPTION_DEFAULT',
      op: 'SETOP_NONE',
    },
  }
}

function arrayAnySubLink(testexpr: any, arrayExpr: any): any {
  return {
    SubLink: {
      subLinkType: 'ANY_SUBLINK',
      testexpr,
      subselect: jsonEachArraySubquery(arrayExpr),
      location: -1,
    },
  }
}

function jsonEachRangeFunction(value: any, alias: string): any {
  return {
    RangeFunction: {
      functions: [
        {
          List: {
            items: [
              {
                FuncCall: {
                  funcname: [stringNode('json_each')],
                  args: [value],
                  funcformat: 'COERCE_EXPLICIT_CALL',
                  location: -1,
                },
              },
              {},
            ],
          },
        },
      ],
      alias: { aliasname: alias },
    },
  }
}

function jsonbExistsAnyKeySubLink(jsonExpr: any, arrayExpr: any): any {
  return {
    SubLink: {
      subLinkType: 'EXISTS_SUBLINK',
      subselect: {
        SelectStmt: {
          targetList: [
            {
              ResTarget: {
                val: intConst(1),
                location: -1,
              },
            },
          ],
          fromClause: [
            jsonEachRangeFunction(jsonExpr, 'obj'),
            jsonEachRangeFunction(arrayExpr, 'keys'),
          ],
          whereClause: equalityExpr(
            columnRefNode('obj', 'key'),
            columnRefNode('keys', 'value')
          ),
          limitOption: 'LIMIT_OPTION_DEFAULT',
          op: 'SETOP_NONE',
        },
      },
      location: -1,
    },
  }
}

function rewriteArrayComparisonExpr(expr: any, context?: RewriteContext): any | null {
  const op = operatorName(expr)
  if (expr?.kind === 'AEXPR_OP_ANY' && op === '=') {
    return arrayAnySubLink(expr.lexpr, sqliteJsonArrayExpr(expr.rexpr, context))
  }
  if (expr?.kind === 'AEXPR_OP_ALL' && op === '<>') {
    return {
      BoolExpr: {
        boolop: 'NOT_EXPR',
        args: [arrayAnySubLink(expr.lexpr, sqliteJsonArrayExpr(expr.rexpr, context))],
        location: -1,
      },
    }
  }
  return null
}

function rewriteJsonbExistenceExpr(expr: any, context?: RewriteContext): any | null {
  if (expr?.kind !== 'AEXPR_OP' || operatorName(expr) !== '?|') return null
  const jsonExpr = cloneAst(expr.lexpr)
  const arrayExpr = sqliteJsonArrayExpr(
    rewriteNode(cloneAst(expr.rexpr), context),
    context
  )
  return jsonbExistsAnyKeySubLink(jsonExpr, arrayExpr)
}

function rewriteLikeExpr(expr: any): any | null {
  if (expr?.kind !== 'AEXPR_LIKE') return null
  const op = operatorName(expr)
  if (op !== '~~' && op !== '!~~') return null

  const call = {
    FuncCall: {
      funcname: [stringNode('like')],
      args: [expr.rexpr, expr.lexpr, funcCallNode('char', [intConst(92)])],
      funcformat: 'COERCE_EXPLICIT_CALL',
      location: expr.location ?? -1,
    },
  }

  if (op !== '!~~') return call
  return {
    BoolExpr: {
      boolop: 'NOT_EXPR',
      args: [call],
      location: expr.location ?? -1,
    },
  }
}

function jsonMaybeParsedExpr(value: any): any {
  const probe = cloneAst(value)
  return {
    CaseExpr: {
      args: [
        {
          CaseWhen: {
            expr: {
              BoolExpr: {
                boolop: 'AND_EXPR',
                args: [
                  funcCallNode('json_valid', [cloneAst(value)]),
                  inExpr(
                    funcCallNode('substr', [
                      funcCallNode('ltrim', [cloneAst(value)]),
                      intConst(1),
                      intConst(1),
                    ]),
                    [stringConst('{'), stringConst('[')]
                  ),
                ],
                location: -1,
              },
            },
            result: funcCallNode('json', [cloneAst(value)]),
            location: -1,
          },
        },
      ],
      defresult: probe,
      location: -1,
    },
  }
}

function rewriteJsonFunctionArguments(funcCall: any, name: string): void {
  if (
    name === 'json_build_object' ||
    name === 'jsonb_build_object' ||
    name === 'json_object'
  ) {
    for (let i = 1; i < (funcCall.args?.length ?? 0); i += 2) {
      funcCall.args[i] = jsonMaybeParsedExpr(funcCall.args[i])
    }
  } else if (
    (name === 'json_object_agg' || name === 'json_group_object') &&
    funcCall.args?.[1]
  ) {
    funcCall.args[1] = jsonMaybeParsedExpr(funcCall.args[1])
  }
}

function markJsonInputParams(
  funcCall: any,
  name: string | null,
  context?: RewriteContext
): void {
  if (!context?.jsonParamNumbers) return
  const args = funcCall.args ?? []
  for (let index = 0; index < args.length; index++) {
    if (!jsonInputParamOid(name, index)) continue
    collectParamRefs(args[index], (paramNumber) =>
      context.jsonParamNumbers?.add(paramNumber)
    )
  }
}

function unwrapJsonEachArraySubLinkArg(arg: any): any | null {
  const subLink = arg?.SubLink
  if (subLink?.subLinkType !== 'ARRAY_SUBLINK') return null
  const select = subLink.subselect?.SelectStmt
  const targetList = select?.targetList ?? []
  const fromClause = select?.fromClause ?? []
  if (targetList.length !== 1 || fromClause.length !== 1) return null
  if (columnRefTailName(unwrapTypeCast(targetList[0]?.ResTarget?.val)) !== 'value')
    return null

  const func = fromClause[0]?.RangeFunction?.functions?.[0]?.List?.items?.[0]?.FuncCall
  const name = functionName(func)
  if (
    name !== 'json_each' &&
    name !== 'json_array_elements' &&
    name !== 'json_array_elements_text' &&
    name !== 'jsonb_array_elements' &&
    name !== 'jsonb_array_elements_text'
  ) {
    return null
  }
  return func.args?.[0] ?? null
}

function isDistinctOnClause(clause: any): boolean {
  return (
    Array.isArray(clause) && clause.some((item) => item && Object.keys(item).length > 0)
  )
}

function buildCopyOutResponse(columnCount: number, binary = false): Uint8Array {
  return msg(
    0x48,
    concat(
      new Uint8Array([binary ? 1 : 0]),
      i16(columnCount),
      ...Array.from({ length: columnCount }, () => i16(binary ? 1 : 0))
    )
  )
}

function buildCopyData(data: string): Uint8Array {
  return msg(0x64, textEncoder.encode(data))
}

function buildCopyDataBytes(data: Uint8Array): Uint8Array {
  return msg(0x64, data)
}

function buildCopyDone(): Uint8Array {
  return msg(0x63, new Uint8Array(0))
}

function sortByDefault(node: any): any {
  return {
    SortBy: {
      node,
      sortby_dir: 'SORTBY_DEFAULT',
      sortby_nulls: 'SORTBY_NULLS_DEFAULT',
      location: -1,
    },
  }
}

function outputNameForTarget(targetNode: any, index: number): string {
  const target = targetNode?.ResTarget
  if (target?.name) return target.name
  const fields = target?.val?.ColumnRef?.fields
  if (Array.isArray(fields)) {
    const name = stringValue(fields[fields.length - 1])
    if (name) return name
  }
  return `_orez_col_${index + 1}`
}

function rowToJsonObject(alias: string, columns: string[]): any {
  return funcCallNode(
    'json_object',
    columns.flatMap((column) => [stringConst(column), columnRefNode(alias, column)])
  )
}

function rowJsonShapesForSelect(stmt: any): Map<string, string[]> {
  const shapes = new Map<string, string[]>()
  for (const fromNode of stmt?.fromClause ?? []) {
    const subselect = fromNode.RangeSubselect
    const alias = subselect?.alias?.aliasname
    const targets = subselect?.subquery?.SelectStmt?.targetList
    if (!alias || !Array.isArray(targets)) continue
    shapes.set(alias, targets.map(outputNameForTarget))
  }
  return shapes
}

function rewriteRowToJsonValue(value: any, shapes: Map<string, string[]>): any {
  if (!value || typeof value !== 'object') return value
  if (Array.isArray(value))
    return value.map((item) => rewriteRowToJsonValue(item, shapes))

  const func = value.FuncCall
  if (func && functionName(func) === 'row_to_json') {
    const alias = columnRefTailName(func.args?.[0])
    const columns = alias ? shapes.get(alias) : null
    if (alias && columns?.length) return rowToJsonObject(alias, columns)
  }

  for (const key of Object.keys(value)) {
    value[key] = rewriteRowToJsonValue(value[key], shapes)
  }
  return value
}

function rewriteRowToJsonSelect(stmt: any): void {
  const shapes = rowJsonShapesForSelect(stmt)
  if (shapes.size === 0) return
  stmt.targetList = rewriteRowToJsonValue(stmt.targetList ?? [], shapes)
}

function rowNumberTarget(partitionClause: any[], orderClause?: any[]): any {
  return {
    ResTarget: {
      name: '_orez_rn',
      val: {
        FuncCall: {
          funcname: [stringNode('row_number')],
          over: {
            partitionClause,
            orderClause,
            frameOptions: 1058,
            location: -1,
          },
          funcformat: 'COERCE_EXPLICIT_CALL',
          location: -1,
        },
      },
      location: -1,
    },
  }
}

// maps an explicit select-list alias to the expression that defines it, but
// only when the alias is NOT just that same base column under its own name
// (e.g. `p.id AS id` stays a real column for sqlite to resolve). computed
// targets (`CASE … END AS match_rank`) and renames (`u.name AS authorName`)
// produce a name that exists ONLY as an output column.
function selectListAliasExpressions(targetList: any[]): Map<string, any> {
  const aliases = new Map<string, any>()
  for (const targetNode of targetList) {
    const target = targetNode?.ResTarget
    const name = target?.name
    if (!name || target.val == null) continue
    if (columnRefTailName(target.val) === name) continue
    aliases.set(name, target.val)
  }
  return aliases
}

// postgres lets DISTINCT ON / ORDER BY reference select-list aliases; sqlite
// cannot resolve a select-list alias inside that same select's window-function
// ORDER BY/PARTITION BY (the alias isn't a real column there). so before the
// window clauses move into the inner select, replace any single-field ColumnRef
// that names an alias with the alias's underlying expression.
function substituteSelectAliasesInClause(node: any, aliases: Map<string, any>): any {
  if (!node || typeof node !== 'object') return node
  if (Array.isArray(node))
    return node.map((item) => substituteSelectAliasesInClause(item, aliases))

  const fields = node.ColumnRef?.fields
  if (Array.isArray(fields) && fields.length === 1) {
    const name = stringValue(fields[0])
    if (name && aliases.has(name)) return cloneAst(aliases.get(name))
  }

  for (const key of Object.keys(node)) {
    node[key] = substituteSelectAliasesInClause(node[key], aliases)
  }
  return node
}

function rewriteDistinctOnSelect(stmt: any): any {
  if (!isDistinctOnClause(stmt?.distinctClause)) return stmt

  const alias = 'orez_distinct_on'
  const innerTargets = cloneAst(stmt.targetList ?? [])
  const outputNames = innerTargets.map(outputNameForTarget)
  innerTargets.forEach((targetNode: any, index: number) => {
    targetNode.ResTarget.name ??= outputNames[index]
  })

  const aliasExpressions = selectListAliasExpressions(innerTargets)
  const partitionClause = substituteSelectAliasesInClause(
    cloneAst(stmt.distinctClause),
    aliasExpressions
  )
  const orderClause = substituteSelectAliasesInClause(
    stmt.sortClause
      ? cloneAst(stmt.sortClause)
      : partitionClause.map((node: any) => sortByDefault(cloneAst(node))),
    aliasExpressions
  )

  const inner = {
    ...cloneAst(stmt),
    distinctClause: undefined,
    sortClause: undefined,
    targetList: [...innerTargets, rowNumberTarget(partitionClause, orderClause)],
  }

  return {
    targetList: outputNames.map((name: string) => ({
      ResTarget: {
        name: name.startsWith('_orez_col_') ? name : undefined,
        val: columnRefNode(alias, name),
        location: -1,
      },
    })),
    fromClause: [
      {
        RangeSubselect: {
          subquery: { SelectStmt: inner },
          alias: { aliasname: alias },
        },
      },
    ],
    whereClause: equalityExpr(columnRefNode('_orez_rn'), intConst(1)),
    limitOption: 'LIMIT_OPTION_DEFAULT',
    op: 'SETOP_NONE',
  }
}

function flattenSelectRangeVarQualifiers(stmt: any): void {
  const visitFromNode = (node: any) => {
    if (!node || typeof node !== 'object') return
    const rangeVar = node.RangeVar
    if (rangeVar?.schemaname && rangeVar.relname) {
      const from = rangeVar.alias?.aliasname ? null : rangeVar.relname
      const to = flattenRangeVar(rangeVar)
      if (from && to) rewriteColumnRefQualifier(stmt, from, to)
      return
    }
    const join = node.JoinExpr
    if (join) {
      visitFromNode(join.larg)
      visitFromNode(join.rarg)
    }
  }
  for (const fromNode of stmt?.fromClause ?? []) visitFromNode(fromNode)
}

function selectRangeVarNames(stmt: any): Set<string> {
  const names = new Set<string>()
  const visitFromNode = (node: any) => {
    if (!node || typeof node !== 'object') return
    const rangeVar = node.RangeVar
    if (rangeVar?.relname) {
      names.add(rangeVar.relname)
      if (rangeVar.alias?.aliasname) names.add(rangeVar.alias.aliasname)
      return
    }
    const join = node.JoinExpr
    if (join) {
      visitFromNode(join.larg)
      visitFromNode(join.rarg)
    }
  }
  for (const fromNode of stmt?.fromClause ?? []) visitFromNode(fromNode)
  return names
}

function rewritePgColumnSizeCompositeArgs(value: any, sourceNames: Set<string>): any {
  if (!value || typeof value !== 'object') return value
  if (Array.isArray(value))
    return value.map((item) => rewritePgColumnSizeCompositeArgs(item, sourceNames))
  if (value.FuncCall && functionName(value.FuncCall) === 'pg_column_size') {
    const arg = value.FuncCall.args?.[0]
    const fields = arg?.ColumnRef?.fields
    if (Array.isArray(fields) && fields.length === 1) {
      const name = stringValue(fields[0])
      if (name && sourceNames.has(name)) return intConst(0)
    } else if (Array.isArray(fields) && fields.length === 2) {
      const schema = stringValue(fields[0])
      const table = stringValue(fields[1])
      if (schema && table && sourceNames.has(flattenSchemaName(schema, table)))
        return intConst(0)
    }
  }
  for (const [key, child] of Object.entries(value)) {
    value[key] = rewritePgColumnSizeCompositeArgs(child, sourceNames)
  }
  return value
}

function normalizeSelectStmt(stmt: any): any {
  const normalized = rewriteDistinctOnSelect(stmt)
  flattenSelectRangeVarQualifiers(normalized)
  rewritePgColumnSizeCompositeArgs(normalized, selectRangeVarNames(normalized))
  rewriteRowToJsonSelect(normalized)
  delete normalized.lockingClause
  return normalized
}

function rewriteExtractFuncCall(func: any): any | null {
  if (functionName(func) !== 'extract') return null
  const part = String(func.args?.[0]?.A_Const?.sval?.sval ?? '').toLowerCase()
  const source = func.args?.[1]
  if (part !== 'epoch' || !source) return null
  return funcCallNode('strftime', [stringConst('%s'), source])
}

function rewriteJsonToRecordsetRangeFunction(rangeFunction: any): any | null {
  const functionList = rangeFunction?.functions?.[0]?.List?.items
  const func = functionList?.[0]?.FuncCall
  if (!func || functionName(func) !== 'json_to_recordset') return null
  const jsonArg = func.args?.[0]
  const columns = (rangeFunction.coldeflist ?? [])
    .map((node: any) => node.ColumnDef)
    .filter((node: any) => node?.colname)
  if (!jsonArg || columns.length === 0) return null

  return {
    RangeSubselect: {
      subquery: {
        SelectStmt: {
          targetList: columns.map((column: any) => ({
            ResTarget: {
              name: column.colname,
              val: funcCallNode('json_extract', [
                columnRefNode('value'),
                stringConst(jsonPathForColumn(column.colname)),
              ]),
              location: -1,
            },
          })),
          fromClause: [
            {
              RangeFunction: {
                functions: [
                  {
                    List: {
                      items: [funcCallNode('json_each', [cloneAst(jsonArg)]), {}],
                    },
                  },
                ],
              },
            },
          ],
          limitOption: 'LIMIT_OPTION_DEFAULT',
          op: 'SETOP_NONE',
        },
      },
      alias: rangeFunction.alias ?? { aliasname: 'json_to_recordset' },
    },
  }
}

function rewriteNode(value: any, context?: RewriteContext): any {
  if (!value || typeof value !== 'object') return value
  if (Array.isArray(value)) return value.map((item) => rewriteNode(item, context))

  if (value.SelectStmt) {
    value.SelectStmt = normalizeSelectStmt(value.SelectStmt)
  }
  if (value.A_ArrayExpr) {
    const rewritten = rewriteArrayExpression(value.A_ArrayExpr)
    if (rewritten) return rewritten
  }
  if (value.A_Expr) {
    const like = rewriteLikeExpr(value.A_Expr)
    if (like) return rewriteNode(like, context)
    const jsonbExists = rewriteJsonbExistenceExpr(value.A_Expr, context)
    if (jsonbExists) return rewriteNode(jsonbExists, context)
    const rewritten = rewriteArrayComparisonExpr(value.A_Expr, context)
    if (rewritten) return rewriteNode(rewritten, context)
  }
  if (value.RangeFunction) {
    const rewritten = rewriteJsonToRecordsetRangeFunction(value.RangeFunction)
    if (rewritten) return rewriteNode(rewritten, context)
  }
  if (value.RangeVar) {
    flattenRangeVar(value.RangeVar)
    return value
  }
  if (value.ColumnRef) {
    flattenColumnRef(value.ColumnRef)
    return value
  }
  if (value.TypeCast) {
    return rewriteNode(value.TypeCast.arg, context)
  }
  if (value.MinMaxExpr) {
    const name = value.MinMaxExpr.op === 'IS_LEAST' ? 'min' : 'max'
    return {
      FuncCall: {
        funcname: [stringNode(name)],
        args: rewriteNode(value.MinMaxExpr.args ?? [], context),
        funcformat: 'COERCE_EXPLICIT_CALL',
      },
    }
  }
  if (value.FuncCall) {
    const name = functionName(value.FuncCall)
    markJsonInputParams(value.FuncCall, name, context)
    if (
      (name === 'json_each' ||
        SQLITE_FUNCTION_BY_PG_FUNCTION.get(name ?? '') === 'json_each') &&
      value.FuncCall.args?.[0]
    ) {
      const unwrapped = unwrapJsonEachArraySubLinkArg(value.FuncCall.args[0])
      if (unwrapped) value.FuncCall.args[0] = unwrapped
    }
    if (name === 'extract') {
      const rewritten = rewriteExtractFuncCall(value.FuncCall)
      if (rewritten) return rewriteNode(rewritten, context)
    }
    if (name === 'md5' && value.FuncCall.args?.[0]) {
      return rewriteNode(value.FuncCall.args[0], context)
    }
    if (name === 'to_timestamp' && value.FuncCall.args?.[0]) {
      const arg = value.FuncCall.args[0]
      if (isDivisionByMillis(arg)) {
        collectParamRefs(arg, (paramNumber) =>
          context?.epochMillisParamNumbers?.add(paramNumber)
        )
      }
      return funcCallNode('datetime', [
        rewriteNode(arg, context),
        stringConst('unixepoch'),
      ])
    }
    if (name === 'timezone' && value.FuncCall.args?.[1]) {
      return rewriteNode(value.FuncCall.args[1], context)
    }
    if (name === 'starts_with') {
      const rewritten = startsWithExpr(value.FuncCall, context)
      if (rewritten) return rewritten
    }
    if (name) rewriteJsonFunctionArguments(value.FuncCall, name)
    const sqliteName = SQLITE_FUNCTION_BY_PG_FUNCTION.get(name ?? '')
    if (sqliteName) value.FuncCall.funcname = [stringNode(sqliteName)]
    if (name === 'now') return currentTimestampNode()
    if (name === 'nextval') return intConst(1)
    if (name === '_drop_zero_slot' || name === 'pg_drop_replication_slot') {
      // DO SQLite can't host orez's `_orez._drop_zero_slot` plpgsql stub (no
      // schema-qualified functions, no CREATE FUNCTION), so a call left intact
      // makes sqlite throw `near "(": syntax error`. zero-cache AWAITS this
      // orphan-slot cleanup on the initial-sync path (createReplicaAndSlot), so
      // the throw wedges the embed before it can signal `ready` (120s timeout →
      // every /sync dies). neutralize the call to its slot-name arg so the
      // cleanup SELECT parses; leftover orphan slot rows are harmless (a fresh
      // active slot is created separately; the pglite/node paths still run the
      // real DELETE the schema-qualified stub performs). this mirrors the
      // now/nextval neutralization above. see soot incident 2026-06-14.
      const slotArg = value.FuncCall.args?.[0]
      return slotArg ? rewriteNode(slotArg, context) : intConst(0)
    }
  }
  if (value.A_Const && Object.hasOwn(value.A_Const, 'boolval')) {
    return intConst(value.A_Const.boolval?.boolval ? 1 : 0)
  }

  for (const key of Object.keys(value)) {
    value[key] = rewriteNode(value[key], context)
  }
  return value
}

function typeNameBase(typeName: any): string | null {
  const names = typeName?.names
  if (!Array.isArray(names) || names.length === 0) return null
  return stringValue(names[names.length - 1])?.toLowerCase() ?? null
}

function typeNameTypmod(typeName: any, index: number): number | null {
  const value = typeName?.typmods?.[index]?.A_Const?.ival?.ival
  return typeof value === 'number' ? value : null
}

function arrayTypeOidForElementOid(oid: number): number {
  return ARRAY_TYPE_ROWS.find((row) => row.oid === oid)?.typarray ?? PG_TYPE_JSON
}

function basePgTypeMetadata(
  typeName: any
): Omit<SchemaColumnMetadata, 'table' | 'schema' | 'tableName' | 'column'> {
  const base = typeNameBase(typeName)
  switch (base) {
    case 'bool':
    case 'boolean':
      return {
        oid: PG_TYPE_BOOL,
        typeOid: PG_TYPE_BOOL,
        dataType: 'boolean',
        typtype: 'b',
        typname: 'bool',
      }
    case 'bytea':
      return {
        oid: PG_TYPE_BYTEA,
        typeOid: PG_TYPE_BYTEA,
        dataType: 'bytea',
        typtype: 'b',
        typname: 'bytea',
      }
    case 'float4':
      return {
        oid: PG_TYPE_FLOAT8,
        typeOid: 700,
        dataType: 'real',
        typtype: 'b',
        typname: 'float4',
      }
    case 'float8':
      return {
        oid: PG_TYPE_FLOAT8,
        typeOid: PG_TYPE_FLOAT8,
        dataType: 'double precision',
        typtype: 'b',
        typname: 'float8',
      }
    case 'int2':
      return {
        oid: PG_TYPE_INT2,
        typeOid: PG_TYPE_INT2,
        dataType: 'smallint',
        typtype: 'b',
        typname: 'int2',
      }
    case 'int4':
    case 'integer':
    case 'serial':
    case 'serial4':
      return {
        oid: PG_TYPE_INT4,
        typeOid: PG_TYPE_INT4,
        dataType: 'integer',
        typtype: 'b',
        typname: 'int4',
      }
    case 'bigint':
    case 'bigserial':
    case 'int8':
    case 'serial8':
      return {
        oid: PG_TYPE_INT8,
        typeOid: PG_TYPE_INT8,
        dataType: 'bigint',
        typtype: 'b',
        typname: 'int8',
      }
    case 'json':
      return {
        oid: PG_TYPE_JSON,
        typeOid: PG_TYPE_JSON,
        dataType: 'json',
        typtype: 'b',
        typname: 'json',
      }
    case 'jsonb':
      return {
        oid: PG_TYPE_JSONB,
        typeOid: PG_TYPE_JSONB,
        dataType: 'jsonb',
        typtype: 'b',
        typname: 'jsonb',
      }
    case 'numeric':
      return {
        oid: PG_TYPE_NUMERIC,
        typeOid: PG_TYPE_NUMERIC,
        dataType: 'numeric',
        typtype: 'b',
        typname: 'numeric',
        numericPrecision: typeNameTypmod(typeName, 0),
        numericScale: typeNameTypmod(typeName, 1),
      }
    case 'timestamp':
      return {
        oid: PG_TYPE_TIMESTAMP,
        typeOid: PG_TYPE_TIMESTAMP,
        dataType: 'timestamp without time zone',
        typtype: 'b',
        typname: 'timestamp',
      }
    case 'timestamptz':
      return {
        oid: PG_TYPE_TIMESTAMPTZ,
        typeOid: PG_TYPE_TIMESTAMPTZ,
        dataType: 'timestamp with time zone',
        typtype: 'b',
        typname: 'timestamptz',
      }
    case 'varchar':
    case 'character varying':
      return {
        oid: PG_TYPE_VARCHAR,
        typeOid: PG_TYPE_VARCHAR,
        dataType: 'character varying',
        typtype: 'b',
        typname: 'varchar',
        characterMaximumLength: typeNameTypmod(typeName, 0),
      }
    case 'bpchar':
    case 'char':
    case 'character':
      return {
        oid: PG_TYPE_VARCHAR,
        typeOid: 1042,
        dataType: 'character',
        typtype: 'b',
        typname: 'bpchar',
        characterMaximumLength: typeNameTypmod(typeName, 0),
      }
    case 'uuid':
      return {
        oid: PG_TYPE_TEXT,
        typeOid: 2950,
        dataType: 'uuid',
        typtype: 'b',
        typname: 'uuid',
      }
    case 'text':
    default:
      return {
        oid: PG_TYPE_TEXT,
        typeOid: PG_TYPE_TEXT,
        dataType: 'text',
        typtype: 'b',
        typname: base ?? 'text',
      }
  }
}

function pgTypeMetadataForTypeName(
  typeName: any
): Omit<SchemaColumnMetadata, 'table' | 'schema' | 'tableName' | 'column'> {
  const base = basePgTypeMetadata(typeName)
  if (!Array.isArray(typeName?.arrayBounds)) {
    return {
      ...base,
      elemTyptype: null,
      elemTypname: null,
    }
  }
  return {
    ...base,
    oid: PG_TYPE_JSON,
    typeOid: arrayTypeOidForElementOid(base.typeOid ?? PG_TYPE_TEXT),
    dataType: `${base.dataType ?? 'text'}[]`,
    typname: `_${base.typname ?? 'text'}`,
    elemTyptype: base.typtype ?? 'b',
    elemTypname: base.typname ?? 'text',
  }
}

function wireOidForTypeName(typeName: any): number | undefined {
  return pgTypeMetadataForTypeName(typeName).oid
}

function columnConstraintMetadata(columnDef: any): {
  notNull?: boolean
  primaryKey?: boolean
  unique?: boolean
} {
  const metadata: { notNull?: boolean; primaryKey?: boolean; unique?: boolean } = {}
  for (const constraint of columnDef?.constraints ?? []) {
    const type = constraint?.Constraint?.contype
    if (type === 'CONSTR_NOTNULL') metadata.notNull = true
    if (type === 'CONSTR_UNIQUE') metadata.unique = true
    if (type === 'CONSTR_PRIMARY') {
      metadata.notNull = true
      metadata.primaryKey = true
      metadata.unique = true
    }
  }
  return metadata
}

function schemaColumnForColumnDef(
  table: PublicationTableRef | null,
  columnDef: any
): SchemaColumnMetadata | null {
  if (!table || !columnDef?.colname) return null
  return {
    ...table,
    column: columnDef.colname,
    ...pgTypeMetadataForTypeName(columnDef.typeName),
    ...columnConstraintMetadata(columnDef),
  }
}

function schemaColumnForAlterColumnType(
  table: PublicationTableRef | null,
  cmd: any
): SchemaColumnMetadata | null {
  const columnDef = cmd?.def?.ColumnDef
  if (!table || !cmd?.name || !columnDef?.typeName) return null
  return {
    ...table,
    column: cmd.name,
    ...pgTypeMetadataForTypeName(columnDef.typeName),
  }
}

function schemaColumnsForCreateTable(stmt: any): SchemaColumnMetadata[] {
  const table = publicationTableRefForRangeVar(stmt.relation)
  return (stmt.tableElts ?? [])
    .map((tableElt: any) => schemaColumnForColumnDef(table, tableElt.ColumnDef))
    .filter(Boolean)
}

function setTypeName(typeName: any, sqliteType: string): void {
  typeName.names = [stringNode(sqliteType)]
  delete typeName.typmods
  delete typeName.arrayBounds
  typeName.typemod = -1
}

function normalizeColumnType(columnDef: any): void {
  const typeName = columnDef?.typeName
  const sqliteType = Array.isArray(typeName?.arrayBounds)
    ? 'text'
    : SQLITE_TYPE_BY_PG_TYPE.get(typeNameBase(typeName) ?? '')
  if (sqliteType) setTypeName(columnDef.typeName, sqliteType)
}

// pg SERIAL types auto-assign from a sequence. SQLite only auto-increments an
// INTEGER PRIMARY KEY, so a non-PK serial column (e.g. zero 1.6's replicas.rank
// BIGSERIAL) becomes a plain nullable integer and stays NULL on inserts that
// don't supply it — zero then reads it and throws "Expected bigint at rank.
// Got null". emulate the sequence with an AFTER INSERT trigger that fills the
// column with max()+1 when it's left NULL.
const SERIAL_TYPES = new Set([
  'serial',
  'serial2',
  'serial4',
  'serial8',
  'smallserial',
  'bigserial',
])

function serialColumnNames(createStmt: any): string[] {
  const names: string[] = []
  for (const elt of createStmt?.tableElts ?? []) {
    const col = elt?.ColumnDef
    if (!col?.colname) continue
    const base = typeNameBase(col.typeName)
    if (!base || !SERIAL_TYPES.has(base)) continue
    // an inline PRIMARY KEY serial becomes INTEGER PRIMARY KEY (rowid alias),
    // which SQLite already auto-increments — no trigger needed.
    const isPrimaryKey = (col.constraints ?? []).some(
      (c: any) => c?.Constraint?.contype === 'CONSTR_PRIMARY'
    )
    if (isPrimaryKey) continue
    names.push(col.colname)
  }
  return names
}

function serialTriggerStatements(table: string, columns: string[]): RewrittenStatement[] {
  return columns.map((col) => ({
    sql: `CREATE TRIGGER IF NOT EXISTS ${quoteIdentifier(`${table}_${col}_serial`)}
AFTER INSERT ON ${quoteIdentifier(table)}
FOR EACH ROW WHEN NEW.${quoteIdentifier(col)} IS NULL
BEGIN
  UPDATE ${quoteIdentifier(table)}
  SET ${quoteIdentifier(col)} = (SELECT coalesce(max(${quoteIdentifier(col)}), 0) + 1 FROM ${quoteIdentifier(table)})
  WHERE rowid = NEW.rowid;
END`,
    isDDL: true,
  }))
}

function isDefaultConstraint(constraint: any): boolean {
  return constraint?.Constraint?.contype === 'CONSTR_DEFAULT'
}

// functions that produce a fresh non-constant value and have no usable SQLite
// column-default form. on the DO replica these defaults are never exercised:
// zero-cache replicates full row values (every column is supplied), and zero's
// own replicas.id default is explicitly "for backwards compatibility" with each
// insert providing id. so we drop the default rather than translate it. checked
// recursively because zero wraps it, e.g. replace(gen_random_uuid()::text,…).
const NON_CONSTANT_DEFAULT_FUNCTIONS = new Set([
  'gen_random_uuid',
  'uuid_generate_v4',
  'md5',
])

function shouldDropFunctionDefault(constraint: any): boolean {
  if (!isDefaultConstraint(constraint)) return false
  return containsAnyFuncCall(
    constraint.Constraint.raw_expr,
    NON_CONSTANT_DEFAULT_FUNCTIONS
  )
}

function containsAnyFuncCall(value: any, names: Set<string>): boolean {
  if (!value || typeof value !== 'object') return false
  if (Array.isArray(value)) return value.some((item) => containsAnyFuncCall(item, names))
  if (value.FuncCall) {
    const name = functionName(value.FuncCall)
    if (name && names.has(name)) return true
  }
  return Object.values(value).some((child) => containsAnyFuncCall(child, names))
}

function normalizeColumnDef(columnDef: any, options?: { addedColumn?: boolean }): void {
  normalizeColumnType(columnDef)
  if (Array.isArray(columnDef.constraints)) {
    columnDef.constraints = columnDef.constraints.filter((constraint: any) => {
      const type = constraint?.Constraint?.contype
      if (type === 'CONSTR_FOREIGN') return false
      if (
        options?.addedColumn &&
        (type === 'CONSTR_NOTNULL' || type === 'CONSTR_PRIMARY')
      )
        return false
      if (shouldDropFunctionDefault(constraint)) return false
      if (
        type === 'CONSTR_GENERATED' &&
        containsAnyFuncCall(
          constraint.Constraint.raw_expr,
          UNSUPPORTED_GENERATED_COLUMN_FUNCTIONS
        )
      )
        return false
      return true
    })
  }
  rewriteNode(columnDef)
}

function isForeignKeyConstraint(tableElt: any): boolean {
  return tableElt?.Constraint?.contype === 'CONSTR_FOREIGN'
}

function normalizeCreateTable(stmt: any): void {
  flattenRangeVar(stmt.relation)
  stmt.if_not_exists = true
  stmt.relation.relpersistence = 'p'
  stmt.tableElts = (stmt.tableElts ?? []).filter(
    (tableElt: any) => !isForeignKeyConstraint(tableElt)
  )
  for (const tableElt of stmt.tableElts ?? []) {
    if (tableElt.ColumnDef) normalizeColumnDef(tableElt.ColumnDef)
  }
}

function normalizeCreateTableAs(stmt: any): void {
  if (stmt.into?.rel) {
    flattenRangeVar(stmt.into.rel)
    stmt.into.rel.relpersistence = 'p'
  }
  rewriteNode(stmt.query)
}

function rewriteInsertDefaults(stmt: any): void {
  const cols = stmt.cols
  const valuesLists = stmt.selectStmt?.SelectStmt?.valuesLists
  if (!Array.isArray(cols) || !Array.isArray(valuesLists) || cols.length === 0) return

  const lists = valuesLists
    .map((list: any) => list?.List?.items)
    .filter((items: any) => Array.isArray(items))
  if (lists.length !== valuesLists.length) return
  if (lists.some((items: any[]) => items.length !== cols.length)) return

  const dropIndexes = cols
    .map((_: any, index: number) => index)
    .filter((index: number) =>
      lists.every((items: any[]) => Object.hasOwn(items[index] ?? {}, 'SetToDefault'))
    )
  if (dropIndexes.length === 0 || dropIndexes.length === cols.length) return

  const drop = new Set(dropIndexes)
  stmt.cols = cols.filter((_: any, index: number) => !drop.has(index))
  for (const items of lists) {
    for (let index = items.length - 1; index >= 0; index--) {
      if (drop.has(index)) items.splice(index, 1)
    }
  }
}

function normalizeInsertSelectOnConflict(stmt: any): void {
  const select = stmt.selectStmt?.SelectStmt
  if (!stmt.onConflictClause || !select?.fromClause?.length || select.whereClause) return
  select.whereClause = intConst(1)
}

function normalizeInsert(stmt: any, context?: RewriteContext): void {
  const from =
    stmt.relation?.alias?.aliasname ??
    (stmt.relation?.schemaname ? stmt.relation.relname : null)
  const table = flattenRangeVar(stmt.relation)
  if (from && table) {
    if (stmt.relation?.alias?.aliasname) delete stmt.relation.alias
    rewriteColumnRefQualifier(stmt, from, table)
  }
  rewriteInsertDefaults(stmt)
  normalizeInsertSelectOnConflict(stmt)
  rewriteNode(stmt, context)
}

function firstSourceTable(value: any, cteNames = new Set<string>()): string | null {
  if (!value || typeof value !== 'object') return null
  if (Array.isArray(value)) {
    for (const item of value) {
      const table = firstSourceTable(item, cteNames)
      if (table) return table
    }
    return null
  }

  if (value.SelectStmt?.withClause?.ctes) {
    const nextCtes = new Set(cteNames)
    for (const cte of value.SelectStmt.withClause.ctes) {
      const name = cte.CommonTableExpr?.ctename
      if (name) nextCtes.add(name)
    }
    for (const cte of value.SelectStmt.withClause.ctes) {
      const table = firstSourceTable(cte.CommonTableExpr?.ctequery, nextCtes)
      if (table) return table
    }
    return firstSourceTable({ ...value.SelectStmt, withClause: undefined }, nextCtes)
  }

  if (value.RangeVar) {
    const table = flattenedRangeVarName(value.RangeVar)
    return table && !cteNames.has(table) ? table : null
  }

  for (const child of Object.values(value)) {
    const table = firstSourceTable(child, cteNames)
    if (table) return table
  }
  return null
}

function collectSourceTables(
  value: any,
  out: Set<string>,
  cteNames = new Set<string>()
): void {
  if (!value || typeof value !== 'object') return
  if (Array.isArray(value)) {
    for (const item of value) collectSourceTables(item, out, cteNames)
    return
  }

  if (value.SelectStmt?.withClause?.ctes) {
    const nextCtes = new Set(cteNames)
    for (const cte of value.SelectStmt.withClause.ctes) {
      const name = cte.CommonTableExpr?.ctename
      if (name) nextCtes.add(name)
    }
    for (const cte of value.SelectStmt.withClause.ctes) {
      collectSourceTables(cte.CommonTableExpr?.ctequery, out, nextCtes)
    }
    collectSourceTables({ ...value.SelectStmt, withClause: undefined }, out, nextCtes)
    return
  }

  if (value.RangeVar) {
    const table = flattenedRangeVarName(value.RangeVar)
    if (table && !cteNames.has(table)) out.add(table)
    return
  }

  for (const child of Object.values(value)) collectSourceTables(child, out, cteNames)
}

function sourceTablesFromSQL(sql: string): string[] {
  try {
    const parsed = parseSync(stripTrailingSemicolon(sql.trim()))
    const tables = new Set<string>()
    collectSourceTables(parsed.stmts[0]?.stmt, tables)
    return [...tables]
  } catch {
    return []
  }
}

function normalizeAlterTable(stmt: any): {
  skipIfColumnExistsByCmd: Map<any, RewrittenStatement['skipIfColumnExists']>
  skipIfColumnMissingByCmd: Map<any, RewrittenStatement['skipIfColumnMissing']>
  schemaColumnsByCmd: Map<any, SchemaColumnMetadata[]>
  metadataOnlySchemaColumns: SchemaColumnMetadata[]
  syntheticStatements: RewrittenStatement[]
} {
  const tableRef = publicationTableRefForRangeVar(stmt.relation)
  const table = flattenRangeVar(stmt.relation)
  const nextCmds: any[] = []
  const skipIfColumnExistsByCmd = new Map<any, RewrittenStatement['skipIfColumnExists']>()
  const skipIfColumnMissingByCmd = new Map<
    any,
    RewrittenStatement['skipIfColumnMissing']
  >()
  const schemaColumnsByCmd = new Map<any, SchemaColumnMetadata[]>()
  const metadataOnlySchemaColumns: SchemaColumnMetadata[] = []
  const syntheticStatements: RewrittenStatement[] = []

  for (const cmdNode of stmt.cmds ?? []) {
    const cmd = cmdNode.AlterTableCmd
    if (!cmd) continue
    if (cmd.subtype === 'AT_AddConstraint') {
      const constraint = cmd.def?.Constraint
      const contype = constraint?.contype
      if (
        table &&
        (contype === 'CONSTR_UNIQUE' || contype === 'CONSTR_PRIMARY') &&
        Array.isArray(constraint.keys)
      ) {
        const columns = constraint.keys.map(stringValue).filter(Boolean)
        if (columns.length) {
          const name =
            constraint.conname ||
            `${table}_${columns.join('_')}_${contype === 'CONSTR_PRIMARY' ? 'pkey' : 'key'}`
          syntheticStatements.push({
            sql: `CREATE UNIQUE INDEX IF NOT EXISTS ${quoteIdentifier(name)} ON ${quoteIdentifier(table)} (${columns.map(quoteIdentifier).join(', ')})`,
            isDDL: true,
          })
        }
      }
      continue
    }
    if (cmd.subtype === 'AT_DropConstraint') {
      if (cmd.name) {
        syntheticStatements.push({
          sql: `DROP INDEX IF EXISTS ${quoteIdentifier(cmd.name)}`,
          isDDL: true,
        })
      }
      continue
    }
    if (cmd.subtype === 'AT_AlterColumnType') {
      const schemaColumn = schemaColumnForAlterColumnType(tableRef, cmd)
      if (schemaColumn) metadataOnlySchemaColumns.push(schemaColumn)
      continue
    }
    if (
      cmd.subtype === 'AT_ReplicaIdentity' ||
      cmd.subtype?.startsWith('AT_AlterColumn') ||
      cmd.subtype === 'AT_ColumnDefault' ||
      cmd.subtype === 'AT_SetNotNull' ||
      cmd.subtype === 'AT_DropNotNull'
    ) {
      continue
    }
    if (cmd.subtype === 'AT_AddColumn' && cmd.def?.ColumnDef) {
      const column = cmd.def.ColumnDef.colname
      if (table && column) skipIfColumnExistsByCmd.set(cmdNode, { table, column })
      const schemaColumn = schemaColumnForColumnDef(tableRef, cmd.def.ColumnDef)
      if (schemaColumn) schemaColumnsByCmd.set(cmdNode, [schemaColumn])
      cmd.missing_ok = false
      normalizeColumnDef(cmd.def.ColumnDef, { addedColumn: true })
    }
    if (cmd.subtype === 'AT_DropColumn') {
      if (cmd.missing_ok && table && cmd.name)
        skipIfColumnMissingByCmd.set(cmdNode, { table, column: cmd.name })
      cmd.missing_ok = false
      delete cmd.behavior
    }
    nextCmds.push(cmdNode)
  }

  stmt.cmds = nextCmds
  return {
    skipIfColumnExistsByCmd,
    skipIfColumnMissingByCmd,
    schemaColumnsByCmd,
    metadataOnlySchemaColumns,
    syntheticStatements,
  }
}

function normalizeRename(stmt: any): { schemaMetadataChanges?: SchemaMetadataChange[] } {
  const relation = stmt.relation
  const table = publicationTableRefForRangeVar(relation)
  delete stmt.behavior
  if (!table) return {}

  if (stmt.renameType === 'OBJECT_TABLE' && stmt.newname) {
    const to = renamedPublicationTableRef(table, stmt.newname)
    flattenRangeVar(relation)
    if (to.table !== stmt.newname) stmt.newname = to.table
    return { schemaMetadataChanges: [{ action: 'renameTable', from: table, to }] }
  }

  if (stmt.renameType === 'OBJECT_COLUMN' && stmt.subname && stmt.newname) {
    flattenRangeVar(relation)
    return {
      schemaMetadataChanges: [
        {
          action: 'renameColumn',
          table,
          from: stmt.subname,
          to: stmt.newname,
        },
      ],
    }
  }

  flattenRangeVar(relation)
  return {}
}

function normalizeIndex(stmt: any): boolean {
  flattenRangeVar(stmt.relation)
  stmt.if_not_exists = true
  const method = stmt.accessMethod?.toLowerCase()
  if (method && UNSUPPORTED_INDEX_METHODS.has(method)) return false
  if (method === 'btree') delete stmt.accessMethod
  for (const param of stmt.indexParams ?? []) {
    delete param.IndexElem?.opclass
    delete param.IndexElem?.nulls_ordering
  }
  rewriteNode(stmt)
  return true
}

function containsFuncCall(value: any, name: string): boolean {
  if (!value || typeof value !== 'object') return false
  if (Array.isArray(value)) return value.some((item) => containsFuncCall(item, name))
  if (value.FuncCall && functionName(value.FuncCall) === name) return true
  return Object.values(value).some((child) => containsFuncCall(child, name))
}

function normalizeUpdate(
  stmt: any,
  context?: RewriteContext
): RewrittenStatement['skipIfTableEmpty'] | null {
  const targetAlias = stmt.relation?.alias?.aliasname
  const table = flattenRangeVar(stmt.relation)
  if (targetAlias && table) {
    delete stmt.relation.alias
    rewriteColumnRefQualifier(stmt, targetAlias, table)
  }
  const hasUnsupportedRegexpReplace = containsFuncCall(stmt, 'regexp_replace')
  rewriteNode(stmt, context)
  return hasUnsupportedRegexpReplace && table ? { table } : null
}

function normalizeDelete(
  stmt: any,
  context?: RewriteContext
): RewrittenStatement['skipIfTableEmpty'] | null {
  const table = flattenRangeVar(stmt.relation)
  const hasUnsupportedUsing =
    Array.isArray(stmt.usingClause) && stmt.usingClause.length > 0
  rewriteNode(stmt, context)
  return hasUnsupportedUsing && table ? { table } : null
}

function createDeleteStatementsForTruncate(
  version: number,
  stmt: any
): RewrittenStatement[] {
  return (stmt.relations ?? []).map((relationNode: any) => {
    const relation = cloneAst(relationNode.RangeVar)
    const table = publicationTableRefForRangeVar(relation)
    flattenRangeVar(relation)
    const deleteStmt = {
      DeleteStmt: { relation },
    }
    const changeTracking = changeTrackingForDML(
      version,
      deleteStmt,
      'DeleteStmt',
      table,
      'DELETE'
    )
    return {
      sql: deparseStatement(version, deleteStmt),
      isWrite: true,
      ...(changeTracking ? { changeTracking } : null),
    }
  })
}

function createSequenceTable(stmt: any): RewrittenStatement[] {
  const sequence = stmt.sequence
  const name = sequence.schemaname
    ? flattenSchemaName(sequence.schemaname, sequence.relname)
    : sequence.relname
  const table = quoteIdentifier(name)
  const schemaColumns: SchemaColumnMetadata[] = [
    {
      table: name,
      schema: sequence.schemaname ?? 'public',
      tableName: sequence.relname,
      column: 'last_value',
      oid: PG_TYPE_INT8,
      typeOid: PG_TYPE_INT8,
      dataType: 'bigint',
      typtype: 'b',
      typname: 'int8',
      elemTyptype: null,
      elemTypname: null,
    },
    {
      table: name,
      schema: sequence.schemaname ?? 'public',
      tableName: sequence.relname,
      column: 'is_called',
      oid: PG_TYPE_BOOL,
      typeOid: PG_TYPE_BOOL,
      dataType: 'boolean',
      typtype: 'b',
      typname: 'bool',
      elemTyptype: null,
      elemTypname: null,
    },
  ]
  return [
    {
      sql: `CREATE TABLE IF NOT EXISTS ${table} (dummy INTEGER PRIMARY KEY DEFAULT 1, last_value INTEGER NOT NULL DEFAULT 1, is_called INTEGER NOT NULL DEFAULT 0)`,
      isDDL: true,
      schemaColumns,
    },
    {
      sql: `INSERT OR IGNORE INTO ${table} (dummy, last_value, is_called) VALUES (1, 1, 0)`,
      isWrite: true,
    },
  ]
}

function publicationRefsFromObjects(objects: any[] | undefined): {
  tables: PublicationTableRef[]
  schemas: string[]
} {
  const tables: PublicationTableRef[] = []
  const schemas: string[] = []
  for (const object of objects ?? []) {
    const spec = object.PublicationObjSpec
    if (!spec) continue
    if (spec.pubobjtype === 'PUBLICATIONOBJ_TABLE') {
      const ref = publicationTableRefForRangeVar(spec.pubtable?.relation)
      if (ref) tables.push(ref)
    } else if (
      spec.pubobjtype === 'PUBLICATIONOBJ_TABLES_IN_SCHEMA' &&
      typeof spec.name === 'string'
    ) {
      schemas.push(spec.name)
    }
  }
  return { tables, schemas }
}

function createPublicationChange(stmt: any): PublicationChange {
  const refs = publicationRefsFromObjects(stmt.pubobjects)
  return {
    action: 'create',
    name: stmt.pubname,
    allTables: Boolean(stmt.for_all_tables),
    ...(refs.schemas.length ? { schemas: refs.schemas } : null),
    ...(refs.tables.length ? { tables: refs.tables } : null),
  }
}

function alterPublicationChange(stmt: any): PublicationChange {
  const refs = publicationRefsFromObjects(stmt.pubobjects)
  const action =
    stmt.action === 'AP_SetObjects'
      ? 'set'
      : stmt.action === 'AP_DropObjects'
        ? 'remove'
        : 'add'
  return {
    action,
    name: stmt.pubname,
    allTables: Boolean(stmt.for_all_tables),
    ...(refs.schemas.length ? { schemas: refs.schemas } : null),
    ...(refs.tables.length ? { tables: refs.tables } : null),
  }
}

function dropPublicationChanges(stmt: any): PublicationChange[] {
  return (stmt.objects ?? [])
    .map((object: any) => stringValue(object))
    .filter((name: unknown): name is string => typeof name === 'string')
    .map((name: string) => ({ action: 'drop', name }))
}

function rewriteSkippedFunctionInvocationSelect(
  stmt: any,
  context?: RewriteContext
): boolean {
  const skippedFunctionNames = context?.skippedFunctionNames
  if (!skippedFunctionNames?.size) return false
  if (stmt.fromClause || stmt.whereClause || stmt.groupClause || stmt.havingClause)
    return false
  if (stmt.sortClause || stmt.limitCount || stmt.withClause) return false

  const targetList = stmt.targetList
  if (!Array.isArray(targetList) || targetList.length !== 1) return false

  const target = targetList[0]?.ResTarget
  const funcCall = target?.val?.FuncCall
  if (!funcCall) return false

  const name = functionName(funcCall)
  if (!name || !skippedFunctionNames.has(name)) return false
  if (name === 'schema_specs') return false

  target.name ??= functionDisplayName(funcCall) ?? name
  target.val = nullConst()
  return true
}

function replaceSchemaSpecsFunctionCalls(sql: string): { sql: string; count: number } {
  let count = 0
  const replaced = sql.replace(
    /(?:(?:"(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_$]*)\s*\.\s*)?schema_specs\s*\(\s*\)/gi,
    () => {
      count++
      return '?'
    }
  )
  return { sql: replaced, count }
}

function deparseStatement(version: number, stmt: any): string {
  const quotedByMarker = markSQLiteKeywordIdentifiers(stmt)
  return stripTrailingSemicolon(
    restoreSQLiteKeywordIdentifierMarkers(
      deparseSync({ version, stmts: [{ stmt }] }),
      quotedByMarker
    ).trim()
  )
}

function starTarget(): any {
  return {
    ResTarget: {
      val: {
        ColumnRef: {
          fields: [{ A_Star: {} }],
          location: -1,
        },
      },
      location: -1,
    },
  }
}

function isStarTarget(target: any): boolean {
  const fields = target?.val?.ColumnRef?.fields
  if (!Array.isArray(fields) || fields.length === 0) return false
  return !!fields[fields.length - 1]?.A_Star
}

function returningExpressionName(target: any): string {
  if (target?.name) return target.name
  const source = columnRefTailName(unwrapTypeCast(target?.val))
  if (source) return source
  const func = unwrapTypeCast(target?.val)?.FuncCall
  const funcName = func ? functionDisplayName(func) : null
  return funcName ?? '?column?'
}

function returningProjectionForList(returningList: any[]): {
  projection: ReturningProjection
  extraTargets: any[]
} {
  const items: ReturningProjectionItem[] = []
  const extraTargets: any[] = []

  for (let index = 0; index < returningList.length; index++) {
    const targetNode = returningList[index]
    const target = targetNode?.ResTarget
    if (!target) continue
    if (isStarTarget(target)) {
      items.push({ kind: 'all' })
      continue
    }

    const source = columnRefTailName(unwrapTypeCast(target.val))
    const name = returningExpressionName(target)
    if (source) {
      items.push({ kind: 'column', source, name })
      continue
    }

    const internalName = `${RETURNING_INTERNAL_PREFIX}${index}`
    const extraTarget = cloneAst(targetNode)
    extraTarget.ResTarget.name = internalName
    items.push({ kind: 'expression', source: internalName, name })
    extraTargets.push(extraTarget)
  }

  return { projection: { items }, extraTargets }
}

function changeTrackingForDML(
  version: number,
  stmt: any,
  nodeType: string,
  table: PublicationTableRef | null,
  operation: ChangeTrackingMetadata['operation']
): ChangeTrackingMetadata | undefined {
  if (!table) return undefined
  const trackedStmt = cloneAst(stmt)
  const node = trackedStmt[nodeType]
  const originalReturningList = Array.isArray(node.returningList)
    ? node.returningList
    : []
  const returnRows = originalReturningList.length > 0
  const returning = returnRows
    ? returningProjectionForList(originalReturningList)
    : undefined
  node.returningList = returnRows
    ? [starTarget(), ...(returning?.extraTargets ?? [])]
    : [starTarget()]
  return {
    table,
    operation,
    returningSQL: deparseStatement(version, trackedStmt),
    returnRows,
    returningProjection: returning?.projection,
  }
}

function resultColumnNames(result: ExecResult): string[] {
  if (result.columns.length > 0) return result.columns
  return result.rows.length > 0 ? Object.keys(result.rows[0]) : []
}

function projectReturningResult(
  result: ExecResult,
  projection: ReturningProjection
): ExecResult {
  const sourceColumns = resultColumnNames(result)
  const visibleColumns: string[] = []

  for (const item of projection.items) {
    if (item.kind === 'all') {
      for (const column of sourceColumns) {
        if (!column.startsWith(RETURNING_INTERNAL_PREFIX)) visibleColumns.push(column)
      }
    } else {
      visibleColumns.push(item.name)
    }
  }

  const rows = result.rows.map((row) => {
    const projected: SqliteRow = {}
    for (const item of projection.items) {
      if (item.kind === 'all') {
        for (const column of sourceColumns) {
          if (!column.startsWith(RETURNING_INTERNAL_PREFIX))
            projected[column] = row[column]
        }
      } else {
        projected[item.name] = row[item.source]
      }
    }
    return projected
  })

  return { rows, columns: visibleColumns, affectedRows: result.affectedRows }
}

function isIdentifierChar(ch: string | undefined): boolean {
  if (!ch) return false
  return isAsciiAlpha(ch) || isAsciiDigit(ch) || ch === '_'
}

function stripLineComments(source: string): string {
  let out = ''
  let quote: "'" | '"' | null = null
  for (let i = 0; i < source.length; i++) {
    const ch = source[i]
    const next = source[i + 1]
    if (quote) {
      out += ch
      if (ch === quote) {
        if (quote === "'" && next === "'") {
          out += next
          i++
        } else {
          quote = null
        }
      }
      continue
    }
    if (ch === "'" || ch === '"') {
      quote = ch
      out += ch
      continue
    }
    if (ch === '-' && next === '-') {
      while (i < source.length && source[i] !== '\n') i++
      if (i < source.length) out += '\n'
      continue
    }
    out += ch
  }
  return out
}

function keywordMatchesAt(source: string, keyword: string, index: number): boolean {
  if (index < 0 || index + keyword.length > source.length) return false
  const before = source[index - 1]
  const after = source[index + keyword.length]
  if (isIdentifierChar(before) || isIdentifierChar(after)) return false
  return source.slice(index, index + keyword.length).toUpperCase() === keyword
}

function findKeywordOutsideQuotes(source: string, keyword: string, start = 0): number {
  const upperKeyword = keyword.toUpperCase()
  let quote: "'" | '"' | null = null
  for (let i = start; i < source.length; i++) {
    const ch = source[i]
    const next = source[i + 1]
    if (quote) {
      if (ch === quote) {
        if (quote === "'" && next === "'") i++
        else quote = null
      }
      continue
    }
    if (ch === "'" || ch === '"') {
      quote = ch
      continue
    }
    if (keywordMatchesAt(source, upperKeyword, i)) return i
  }
  return -1
}

function splitSqlStatements(source: string): string[] {
  const statements: string[] = []
  let start = 0
  let depth = 0
  let quote: "'" | '"' | null = null
  for (let i = 0; i < source.length; i++) {
    const ch = source[i]
    const next = source[i + 1]
    if (quote) {
      if (ch === quote) {
        if (quote === "'" && next === "'") i++
        else quote = null
      }
      continue
    }
    if (ch === "'" || ch === '"') {
      quote = ch
      continue
    }
    if (ch === '(') depth++
    else if (ch === ')' && depth > 0) depth--
    else if (ch === ';' && depth === 0) {
      const statement = source.slice(start, i).trim()
      if (statement) statements.push(statement)
      start = i + 1
    }
  }
  const last = source.slice(start).trim()
  if (last) statements.push(last)
  return statements
}

function selectWhereSQL(version: number, whereClause: any): string | null {
  const stmt = {
    SelectStmt: {
      targetList: [{ ResTarget: { val: intConst(1), location: -1 } }],
      whereClause: rewriteNode(cloneAst(whereClause)),
      limitOption: 'LIMIT_OPTION_DEFAULT',
      op: 'SETOP_NONE',
    },
  }
  const sql = deparseStatement(version, stmt)
  const whereIndex = findKeywordOutsideQuotes(sql, 'WHERE')
  if (whereIndex < 0) return null
  return sql.slice(whereIndex + 'WHERE'.length).trim()
}

function triggerConditionSQL(condition: string): string | null {
  try {
    const parsed = parseSync(`SELECT 1 WHERE ${condition}`)
    const whereClause = parsed.stmts[0]?.stmt?.SelectStmt?.whereClause
    if (!whereClause) return null
    return selectWhereSQL(parsed.version, whereClause)
  } catch {
    return null
  }
}

function compileSelectIntoNew(
  version: number,
  select: any,
  triggerTable: string,
  rowCondition?: string
): string | null {
  const rel = select.intoClause?.rel
  if (rel?.schemaname?.toLowerCase?.() !== 'new' || !rel.relname) return null
  const targetColumn = rel.relname
  const cloned = cloneAst(select)
  delete cloned.intoClause
  const selectSQL = deparseStatement(version, { SelectStmt: rewriteNode(cloned) })
  return rowUpdateSQL(triggerTable, targetColumn, `(${selectSQL})`, rowCondition)
}

function deparseExpressionSQL(version: number, expr: any): string | null {
  const sql = deparseStatement(version, {
    SelectStmt: {
      targetList: [{ ResTarget: { val: expr, location: -1 } }],
      limitOption: 'LIMIT_OPTION_DEFAULT',
      op: 'SETOP_NONE',
    },
  })
  const selectIndex = findKeywordOutsideQuotes(sql, 'SELECT')
  if (selectIndex < 0) return null
  return sql.slice(selectIndex + 'SELECT'.length).trim()
}

// expand a DELETE on `target` into its cascade child statements (leaves-first).
// expansion emits PG SQL under un-flattened names, then each child re-enters
// rewriteParsedStatement (suppressFkCascade so it doesn't re-expand) — so each
// returns a normal RewrittenStatement, flattened + change-tracked identically to
// a hand-written delete. `whereClause` is the parent's already-normalized clause
// (deparsed back to SQL, $N params intact); its $N bind to the parent's params
// at execute time. unconditional deletes (no WHERE) cascade every child row.
function buildCascadeStatements(
  version: number,
  target: string,
  whereClause: any,
  context: RewriteContext
): RewrittenStatement[] {
  const whereSql = whereClause ? deparseExpressionSQL(version, whereClause) : null
  const childContext: RewriteContext = { ...context, suppressFkCascade: true }
  const out: RewrittenStatement[] = []
  for (const childSql of expandDelete(target, whereSql, context.fkRegistry!)) {
    for (const raw of parseSync(childSql).stmts) {
      const rewritten = rewriteParsedStatement(version, raw, childContext)
      if (Array.isArray(rewritten))
        out.push(...rewritten.filter((s): s is RewrittenStatement => !!s))
      else if (rewritten) out.push(rewritten)
    }
  }
  return out
}

function parseExpressionTarget(source: string): { version: number; expr: any } | null {
  try {
    const parsed = parseSync(`SELECT ${source}`)
    const select = parsed.stmts[0]?.stmt?.SelectStmt
    const target = select?.targetList?.[0]?.ResTarget?.val
    if (!target || parsed.stmts.length !== 1 || select.targetList.length !== 1)
      return null
    return { version: parsed.version, expr: target }
  } catch {
    return null
  }
}

function splitTopLevelAssignment(
  statement: string
): { left: string; right: string } | null {
  let depth = 0
  let quote: "'" | '"' | null = null
  for (let i = 0; i < statement.length; i++) {
    const ch = statement[i]
    const next = statement[i + 1]
    if (quote) {
      if (ch === quote) {
        if (quote === "'" && next === "'") i++
        else quote = null
      }
      continue
    }
    if (ch === "'" || ch === '"') {
      quote = ch
      continue
    }
    if (ch === '(') {
      depth++
      continue
    }
    if (ch === ')' && depth > 0) {
      depth--
      continue
    }
    if (depth !== 0) continue
    if (ch === ':' && next === '=') {
      return {
        left: statement.slice(0, i).trim(),
        right: statement.slice(i + 2).trim(),
      }
    }
    if (ch === '=') {
      return {
        left: statement.slice(0, i).trim(),
        right: statement.slice(i + 1).trim(),
      }
    }
  }
  return null
}

function newAssignmentColumn(left: string): string | null {
  const parsed = parseExpressionTarget(left)
  const fields = parsed?.expr?.ColumnRef?.fields
  if (!Array.isArray(fields) || fields.length !== 2) return null
  if (stringValue(fields[0])?.toLowerCase() !== 'new') return null
  return stringValue(fields[1])
}

function rowUpdateSQL(
  table: string,
  column: string,
  expressionSQL: string,
  rowCondition?: string
): string {
  const condition = rowCondition ? ` AND (${rowCondition})` : ''
  return `UPDATE ${quoteIdentifier(table)} SET ${quoteIdentifier(column)} = ${expressionSQL} WHERE rowid = NEW.rowid${condition}`
}

function compileNewAssignment(
  statement: string,
  triggerTable: string,
  rowCondition?: string
): { sql: string; forceAfter: true } | null {
  const assignment = splitTopLevelAssignment(statement)
  if (!assignment?.left || !assignment.right) return null
  const column = newAssignmentColumn(assignment.left)
  if (!column) return null
  const parsed = parseExpressionTarget(assignment.right)
  if (!parsed) return null
  const expressionSQL = deparseExpressionSQL(
    parsed.version,
    rewriteNode(cloneAst(parsed.expr))
  )
  if (!expressionSQL) return null
  return {
    sql: rowUpdateSQL(triggerTable, column, expressionSQL, rowCondition),
    forceAfter: true,
  }
}

function compileTriggerBodyStatement(
  statement: string,
  triggerTable: string,
  rowCondition?: string
): { sql: string; forceAfter?: boolean } | null {
  const assignment = compileNewAssignment(statement, triggerTable, rowCondition)
  if (assignment) return assignment

  let parsed: ReturnType<typeof parseSync>
  try {
    parsed = parseSync(statement)
  } catch {
    return null
  }
  if (parsed.stmts.length !== 1) return null
  const stmt = parsed.stmts[0]?.stmt
  if (!stmt) return null
  if (stmt.SelectStmt?.intoClause) {
    const sql = compileSelectIntoNew(
      parsed.version,
      stmt.SelectStmt,
      triggerTable,
      rowCondition
    )
    return sql ? { sql, forceAfter: true } : null
  }
  if (rowCondition) return null
  const rewrittenStmt = cloneAst(stmt)
  if (rewrittenStmt.UpdateStmt) normalizeUpdate(rewrittenStmt.UpdateStmt)
  else rewriteNode(rewrittenStmt)
  return {
    sql: deparseStatement(parsed.version, rewrittenStmt),
  }
}

function compileTriggerStatementBlock(
  source: string,
  triggerTable: string,
  rowCondition?: string
): { statements: string[]; forceAfter?: boolean } | null {
  const statements: string[] = []
  let forceAfter = false
  for (const statement of splitSqlStatements(source)) {
    const upper = statement.trim().toUpperCase()
    if (!upper || upper === 'RETURN NEW' || upper === 'RETURN OLD') continue
    const compiled = compileTriggerBodyStatement(statement, triggerTable, rowCondition)
    if (!compiled) return null
    statements.push(compiled.sql)
    if (compiled.forceAfter) forceAfter = true
  }
  return statements.length ? { statements, forceAfter } : null
}

function compilePlpgsqlTriggerBody(
  body: string,
  triggerTable: string
): { when?: string; statements: string[]; forceAfter?: boolean } | null {
  const source = stripLineComments(body).trim()
  const begin = findKeywordOutsideQuotes(source, 'BEGIN')
  if (begin < 0) return null
  const ifStart = findKeywordOutsideQuotes(source, 'IF', begin + 'BEGIN'.length)
  let condition: string | undefined
  let statementsSource = ''
  if (ifStart >= 0) {
    const thenIndex = findKeywordOutsideQuotes(source, 'THEN', ifStart + 'IF'.length)
    const endIf = findKeywordOutsideQuotes(source, 'END IF', thenIndex + 'THEN'.length)
    if (thenIndex < 0 || endIf < 0) return null
    condition = source.slice(ifStart + 'IF'.length, thenIndex).trim()
    const elseIndex = findKeywordOutsideQuotes(source, 'ELSE', thenIndex + 'THEN'.length)
    if (elseIndex >= 0 && elseIndex < endIf) {
      const when = triggerConditionSQL(condition)
      if (!when) return null
      const thenBlock = compileTriggerStatementBlock(
        source.slice(thenIndex + 'THEN'.length, elseIndex).trim(),
        triggerTable,
        when
      )
      const elseBlock = compileTriggerStatementBlock(
        source.slice(elseIndex + 'ELSE'.length, endIf).trim(),
        triggerTable,
        `NOT (${when})`
      )
      if (!thenBlock || !elseBlock) return null
      return {
        statements: [...thenBlock.statements, ...elseBlock.statements],
        forceAfter: Boolean(thenBlock.forceAfter || elseBlock.forceAfter),
      }
    }
    statementsSource = source.slice(thenIndex + 'THEN'.length, endIf).trim()
  } else {
    const finalEnd = findKeywordOutsideQuotes(source, 'END', begin + 'BEGIN'.length)
    if (finalEnd < 0) return null
    statementsSource = source.slice(begin + 'BEGIN'.length, finalEnd).trim()
  }

  const when = condition ? triggerConditionSQL(condition) : undefined
  if (condition && !when) return null
  const block = compileTriggerStatementBlock(statementsSource, triggerTable)
  return block ? { when, ...block } : null
}

function sqliteTriggerEvents(events: number): string[] {
  const result: string[] = []
  if (events & 4) result.push('INSERT')
  if (events & 16) result.push('UPDATE')
  if (events & 8) result.push('DELETE')
  return result
}

function sqliteTriggerName(base: string, event: string, eventCount: number): string {
  return eventCount === 1 ? base : `${base}_${event.toLowerCase()}`
}

function threadReplyCountTriggerStatements(node: any): RewrittenStatement[] | null {
  const table = publicationTableRefForRangeVar(node.relation)
  if (!table || table.table !== 'message') return null
  const events = sqliteTriggerEvents(Number(node.events ?? 0))
  if (events.length === 0) return null

  return events.map((event) => {
    const row = event === 'DELETE' ? 'OLD' : 'NEW'
    const triggerName = sqliteTriggerName(node.trigname, event, events.length)
    return {
      sql: `CREATE TRIGGER IF NOT EXISTS ${quoteIdentifier(triggerName)}
AFTER ${event} ON ${quoteIdentifier(table.table)}
FOR EACH ROW
WHEN ${row}."threadId" IS NOT NULL
BEGIN
  UPDATE "thread"
  SET "replyCount" = min(11, (
    SELECT COUNT(*)
    FROM "message"
    WHERE "threadId" = ${row}."threadId"
      AND "deleted" = 0
      AND "type" IS DISTINCT FROM 'draft'
      AND "id" IS DISTINCT FROM (
        SELECT "messageId" FROM "thread" WHERE "id" = ${row}."threadId"
      )
  ))
  WHERE "id" = ${row}."threadId";
END`,
      isDDL: true,
    }
  })
}

function createTriggerStatements(
  node: any,
  context?: RewriteContext
): RewrittenStatement[] | null {
  const fnName = functionName({ funcname: node.funcname })
  const fn = fnName ? context?.triggerFunctions?.get(fnName) : undefined
  if (!fn) return null
  if (fnName === 'updatethreadreplycount') {
    return threadReplyCountTriggerStatements(node)
  }
  if (
    findKeywordOutsideQuotes(fn.body, 'TG_OP') >= 0 ||
    findKeywordOutsideQuotes(fn.body, 'TG_ARGV') >= 0 ||
    findKeywordOutsideQuotes(fn.body, 'TG_NAME') >= 0 ||
    findKeywordOutsideQuotes(fn.body, 'TG_TABLE_NAME') >= 0 ||
    findKeywordOutsideQuotes(fn.body, 'TG_WHEN') >= 0
  ) {
    return null
  }
  const table = publicationTableRefForRangeVar(node.relation)
  if (!table) return null
  const body = compilePlpgsqlTriggerBody(fn.body, table.table)
  if (!body) return null
  const events = sqliteTriggerEvents(Number(node.events ?? 0))
  if (events.length === 0) return null
  const timing = body.forceAfter ? 'AFTER' : node.timing === 2 ? 'BEFORE' : 'AFTER'
  return events.map((event) => {
    const triggerName = sqliteTriggerName(node.trigname, event, events.length)
    const when = body.when ? `\nWHEN ${body.when}` : ''
    const statements = body.statements.map((sql) => `  ${sql};`).join('\n')
    return {
      sql: `CREATE TRIGGER IF NOT EXISTS ${quoteIdentifier(triggerName)}
${timing} ${event} ON ${quoteIdentifier(table.table)}
FOR EACH ROW${when}
BEGIN
${statements}
END`,
      isDDL: true,
    }
  })
}

function dropTriggerStatements(node: any): RewrittenStatement[] | null {
  if (node.removeType !== 'OBJECT_TRIGGER') return null
  const statements: RewrittenStatement[] = []
  for (const object of node.objects ?? []) {
    const items = object.List?.items ?? []
    const trigger = stringValue(items[items.length - 1])
    if (!trigger) continue
    for (const name of [
      trigger,
      `${trigger}_insert`,
      `${trigger}_update`,
      `${trigger}_delete`,
    ]) {
      statements.push({
        sql: `DROP TRIGGER IF EXISTS ${quoteIdentifier(name)}`,
        isDDL: true,
      })
    }
  }
  return statements.length ? statements : null
}

function rewriteParsedStatement(
  version: number,
  rawStmt: any,
  context?: RewriteContext
): RewrittenStatement | RewrittenStatement[] | null {
  const stmt = rawStmt.stmt
  // counted-delete CTEs (zero's changeLog purge) restructure into a plain
  // DELETE before dispatch, so flattening/tracking/deparse treat them like
  // any other delete; doExecResult folds the marker rows back into a count.
  transformCountedDeleteCte(stmt)
  const nodeType = statementNodeType(stmt)
  const node = stmt[nodeType]

  if (nodeType === 'CreateFunctionStmt') {
    const name = createFunctionName(node)
    if (name) context?.skippedFunctionNames?.add(name)
    const triggerFunction = createTriggerFunctionDefinition(node)
    if (triggerFunction) context?.triggerFunctions?.set(name, triggerFunction)
    return null
  }
  if (nodeType === 'CreateTrigStmt') {
    return createTriggerStatements(node, context)
  }
  if (nodeType === 'CreatePublicationStmt') {
    return {
      sql: '',
      isDDL: true,
      publicationChanges: [createPublicationChange(node)],
    }
  }
  if (nodeType === 'AlterPublicationStmt') {
    return {
      sql: '',
      isDDL: true,
      publicationChanges: [alterPublicationChange(node)],
    }
  }
  if (nodeType === 'DropStmt' && node.removeType === 'OBJECT_PUBLICATION') {
    return {
      sql: '',
      isDDL: true,
      publicationChanges: dropPublicationChanges(node),
    }
  }
  if (nodeType === 'DropStmt' && node.removeType === 'OBJECT_FUNCTION') {
    for (const object of node.objects ?? []) {
      const items = object.List?.items ?? []
      const name = stringValue(items[items.length - 1])
      if (name) {
        context?.skippedFunctionNames?.delete(name.toLowerCase())
        context?.triggerFunctions?.delete(name.toLowerCase())
      }
    }
    return null
  }
  if (nodeType === 'DropStmt' && node.removeType === 'OBJECT_TRIGGER') {
    return dropTriggerStatements(node)
  }
  if (SKIPPED_NODE_TYPES.has(nodeType)) return null
  if (nodeType === 'DropStmt' && SKIPPED_DROP_OBJECTS.has(node.removeType)) return null
  if (nodeType === 'CreateSeqStmt') return createSequenceTable(node)
  if (nodeType === 'TruncateStmt') return createDeleteStatementsForTruncate(version, node)
  if (nodeType === 'IndexStmt' && !normalizeIndex(node)) return null

  let alterMetadata: ReturnType<typeof normalizeAlterTable> | null = null
  let schemaColumns: SchemaColumnMetadata[] = []
  let schemaMetadataChanges: SchemaMetadataChange[] = []
  let skipIfTableEmpty: RewrittenStatement['skipIfTableEmpty'] | null = null
  let changeTracking: ChangeTrackingMetadata | undefined
  let writeTable: PublicationTableRef | null = null
  let serialTriggers: RewrittenStatement[] = []
  let cascadeStatements: RewrittenStatement[] | undefined
  let fkEdgesAdded = false
  if (nodeType === 'AlterTableStmt') {
    // capture FK cascade/set-null edges before normalizeAlterTable drops the
    // ADD CONSTRAINT — drizzle emits FKs as a separate ALTER, not inline.
    if (context?.fkRegistry && !context.suppressFkCascade) {
      fkEdgesAdded = recordAlterTableForeignKeys(node, context.fkRegistry, fkTableKey) > 0
    }
    alterMetadata = normalizeAlterTable(node)
    if (!node.cmds?.length) {
      const statements: RewrittenStatement[] = []
      if (alterMetadata.metadataOnlySchemaColumns.length) {
        statements.push({
          sql: '',
          isDDL: true,
          schemaColumns: alterMetadata.metadataOnlySchemaColumns,
        })
      }
      statements.push(...alterMetadata.syntheticStatements)
      return statements.length ? statements : null
    }
  } else if (nodeType === 'CreateStmt') {
    schemaColumns = schemaColumnsForCreateTable(node)
    // capture FK cascade/set-null edges BEFORE normalizeCreateTable drops the
    // CONSTR_FOREIGN nodes. flattened keys (fkTableKey) match the DELETE lookup.
    if (context?.fkRegistry) {
      fkEdgesAdded =
        recordCreateTableForeignKeys(node, context.fkRegistry, fkTableKey) > 0
    }
    // capture serial columns before normalizeCreateTable rewrites the type to integer
    const serialCols = serialColumnNames(node)
    normalizeCreateTable(node)
    if (serialCols.length && node.relation?.relname) {
      serialTriggers = serialTriggerStatements(node.relation.relname, serialCols)
    }
  } else if (nodeType === 'CreateTableAsStmt') {
    normalizeCreateTableAs(node)
  } else if (nodeType === 'RenameStmt') {
    const normalized = normalizeRename(node)
    schemaMetadataChanges = normalized.schemaMetadataChanges ?? []
  } else if (nodeType === 'InsertStmt') {
    const table = publicationTableRefForRangeVar(node.relation)
    writeTable = table
    normalizeInsert(node, context)
    if (node.selectStmt?.SelectStmt?.withClause) {
      const sourceTable = firstSourceTable(node.selectStmt)
      if (sourceTable) skipIfTableEmpty = { table: sourceTable }
    }
    changeTracking = changeTrackingForDML(version, stmt, nodeType, table, 'INSERT')
  } else if (nodeType === 'UpdateStmt') {
    const table = publicationTableRefForRangeVar(node.relation)
    writeTable = table
    skipIfTableEmpty = normalizeUpdate(node, context)
    changeTracking = changeTrackingForDML(version, stmt, nodeType, table, 'UPDATE')
  } else if (nodeType === 'DeleteStmt') {
    const table = publicationTableRefForRangeVar(node.relation)
    writeTable = table
    // build the cascade from the ORIGINAL delete (before normalizeDelete mutates
    // node), so each child re-enters the rewrite and is flattened + translated
    // uniformly. suppressFkCascade guards against re-expanding the children.
    if (context?.fkRegistry?.hasEdges && !context.suppressFkCascade && node.relation) {
      const cascadeTarget = fkTableKey(node.relation)
      if (context.fkRegistry.childrenOf(cascadeTarget).length) {
        cascadeStatements = buildCascadeStatements(
          version,
          cascadeTarget,
          node.whereClause,
          context
        )
      }
    }
    skipIfTableEmpty = normalizeDelete(node, context)
    changeTracking = changeTrackingForDML(version, stmt, nodeType, table, 'DELETE')
  } else if (nodeType === 'SelectStmt') {
    rawStmt.stmt.SelectStmt = normalizeSelectStmt(node)
    if (!rewriteSkippedFunctionInvocationSelect(rawStmt.stmt.SelectStmt, context))
      rewriteNode(rawStmt.stmt.SelectStmt, context)
  } else if (nodeType === 'DropStmt') {
    delete node.behavior
    rewriteNode(node, context)
  } else {
    rewriteNode(node, context)
  }

  if (
    nodeType === 'AlterTableStmt' &&
    (node.cmds.length > 1 || alterMetadata?.syntheticStatements.length)
  ) {
    const statements: RewrittenStatement[] = []
    if (alterMetadata?.metadataOnlySchemaColumns.length) {
      statements.push({
        sql: '',
        isDDL: true,
        schemaColumns: alterMetadata.metadataOnlySchemaColumns,
      })
    }
    statements.push(...(alterMetadata?.syntheticStatements ?? []))
    statements.push(
      ...node.cmds.map((cmdNode: any) => {
        const singleStmt = {
          [nodeType]: {
            ...node,
            cmds: [cmdNode],
          },
        }
        const skipIfColumnExists = alterMetadata?.skipIfColumnExistsByCmd.get(cmdNode)
        const skipIfColumnMissing = alterMetadata?.skipIfColumnMissingByCmd.get(cmdNode)
        const cmdSchemaColumns = alterMetadata?.schemaColumnsByCmd.get(cmdNode)
        return {
          sql: deparseStatement(version, singleStmt),
          isDDL: true,
          ...(context?.arrayParamNumbers?.size
            ? { arrayParamNumbers: new Set(context.arrayParamNumbers) }
            : null),
          ...(context?.jsonParamNumbers?.size
            ? { jsonParamNumbers: new Set(context.jsonParamNumbers) }
            : null),
          ...(context?.epochMillisParamNumbers?.size
            ? {
                epochMillisParamNumbers: new Set(context.epochMillisParamNumbers),
              }
            : null),
          ...(cmdSchemaColumns?.length ? { schemaColumns: cmdSchemaColumns } : null),
          ...(skipIfColumnExists ? { skipIfColumnExists } : null),
          ...(skipIfColumnMissing ? { skipIfColumnMissing } : null),
        }
      })
    )
    return statements
  }

  const skipIfColumnExists =
    nodeType === 'AlterTableStmt'
      ? alterMetadata?.skipIfColumnExistsByCmd.get(node.cmds[0])
      : null
  const skipIfColumnMissing =
    nodeType === 'AlterTableStmt'
      ? alterMetadata?.skipIfColumnMissingByCmd.get(node.cmds[0])
      : null
  if (nodeType === 'AlterTableStmt') {
    schemaColumns = [
      ...(alterMetadata?.metadataOnlySchemaColumns ?? []),
      ...(alterMetadata?.schemaColumnsByCmd.get(node.cmds[0]) ?? []),
    ]
  }
  const rewritten = deparseStatement(version, stmt)
  const isDDL =
    nodeType === 'AlterTableStmt' ||
    nodeType === 'CreateStmt' ||
    nodeType === 'CreateTableAsStmt' ||
    nodeType === 'DropStmt' ||
    nodeType === 'IndexStmt' ||
    nodeType === 'RenameStmt'
  const isWrite =
    nodeType === 'DeleteStmt' || nodeType === 'InsertStmt' || nodeType === 'UpdateStmt'
  const usesPublishedSchemaFunction =
    context?.skippedFunctionNames?.has('schema_specs') &&
    containsFuncCall(stmt, 'schema_specs')
  const mainStatement: RewrittenStatement = {
    sql: rewritten,
    ...(isDDL ? { isDDL } : null),
    ...(isWrite ? { isWrite } : null),
    ...(writeTable ? { writeTable } : null),
    ...(changeTracking ? { changeTracking } : null),
    ...(usesPublishedSchemaFunction ? { usesPublishedSchemaFunction } : null),
    ...(context?.arrayParamNumbers?.size
      ? { arrayParamNumbers: new Set(context.arrayParamNumbers) }
      : null),
    ...(context?.jsonParamNumbers?.size
      ? { jsonParamNumbers: new Set(context.jsonParamNumbers) }
      : null),
    ...(context?.epochMillisParamNumbers?.size
      ? { epochMillisParamNumbers: new Set(context.epochMillisParamNumbers) }
      : null),
    ...(schemaColumns.length ? { schemaColumns } : null),
    ...(schemaMetadataChanges.length ? { schemaMetadataChanges } : null),
    ...(skipIfColumnExists ? { skipIfColumnExists } : null),
    ...(skipIfColumnMissing ? { skipIfColumnMissing } : null),
    ...(skipIfTableEmpty ? { skipIfTableEmpty } : null),
    ...(cascadeStatements?.length ? { cascadeStatements } : null),
    ...(fkEdgesAdded ? { fkEdges: true } : null),
  }
  return serialTriggers.length ? [mainStatement, ...serialTriggers] : mainStatement
}

function rewriteSQLStatements(
  sql: string,
  context?: RewriteContext
): RewrittenStatement[] {
  const trimmed = sql.trim()
  if (!trimmed) return []
  if (isCatalogQuery(trimmed)) return [{ sql: trimmed }]
  const parsed = parseSync(trimmed)
  return parsed.stmts
    .flatMap((stmt: any) => rewriteParsedStatement(parsed.version, stmt, context))
    .filter(Boolean)
}

function rewrittenSQLText(statements: RewrittenStatement[]): string {
  return statements
    .map((statement) => statement.sql)
    .filter((sql) => sql.trim())
    .join(';\n')
}

// pg DDL (e.g. a generated init.sql) → the exact /batch statements a SQL DO
// needs to have the schema "already applied" with full pg type metadata: the
// SQLite-native DDL plus the _orez_pg_metadata upserts that
// loadDurableMetadata() hydrates schemaMetadata from on boot. callers that
// apply DDL out-of-band at deploy time (no runtime parse) MUST apply both —
// DDL alone loses the pg column types, which silently downgrades binary COPY
// encoding to text (crashing typed consumers like zero-cache initial sync)
// and breaks typed result formatting.
export async function deployTimeSchemaBatchStatements(ddl: string): Promise<
  Array<{
    sql: string
    params?: string[]
    skipIfColumnExists?: { table: string; column: string }
    skipIfColumnMissing?: { table: string; column: string }
  }>
> {
  await loadModule()
  const statements: Array<{
    sql: string
    params?: string[]
    skipIfColumnExists?: { table: string; column: string }
    skipIfColumnMissing?: { table: string; column: string }
  }> = []
  const metadataRows: Array<[string, string, string, string]> = []
  for (const chunk of ddl.split('--> statement-breakpoint')) {
    const sql = chunk.trim()
    if (!sql) continue
    for (const statement of rewriteSQLStatements(sql)) {
      // carry the runtime-conditional DDL skips (ALTER TABLE ... ADD/DROP
      // COLUMN IF [NOT] EXISTS) — the /batch executor evaluates them against
      // the target's actual shape, which deploy time cannot know.
      if (statement.sql.trim())
        statements.push({
          sql: statement.sql,
          ...(statement.skipIfColumnExists
            ? { skipIfColumnExists: statement.skipIfColumnExists }
            : null),
          ...(statement.skipIfColumnMissing
            ? { skipIfColumnMissing: statement.skipIfColumnMissing }
            : null),
        })
      for (const column of statement.schemaColumns ?? []) {
        metadataRows.push([
          'schema-column',
          column.table,
          column.column,
          JSON.stringify(column),
        ])
      }
    }
  }
  if (metadataRows.length) {
    statements.push({ sql: metadataTableDDL() })
    // mirrors persistDurableMetadata: chunked multi-row upserts under the
    // Cloudflare DO SQLite host-param cap (4 cols × 20 rows).
    const CHUNK = 20
    for (let i = 0; i < metadataRows.length; i += CHUNK) {
      const rows = metadataRows.slice(i, i + CHUNK)
      statements.push({
        sql: `INSERT OR REPLACE INTO ${quoteIdentifier(METADATA_TABLE)} (kind, key, subkey, value) VALUES ${rows.map(() => '(?, ?, ?, ?)').join(', ')}`,
        params: rows.flat(),
      })
    }
  }
  return statements
}

function metadataTableDDL(): string {
  return `
      CREATE TABLE IF NOT EXISTS ${quoteIdentifier(METADATA_TABLE)} (
        kind TEXT NOT NULL,
        key TEXT NOT NULL,
        subkey TEXT NOT NULL DEFAULT '',
        value TEXT NOT NULL,
        PRIMARY KEY (kind, key, subkey)
      )
    `
}

function rewriteSQL(sql: string, context?: RewriteContext): string {
  return rewrittenSQLText(rewriteSQLStatements(sql, context))
}

interface TransactionAction {
  kind: 'begin' | 'commit' | 'rollback' | 'savepoint' | 'release' | 'rollback_to'
  name?: string
}

function transactionKind(sql: string): TransactionAction['kind'] | null {
  return transactionAction(sql)?.kind ?? null
}

function transactionAction(sql: string): TransactionAction | null {
  try {
    const parsed = parseSync(sql.trim())
    if (parsed.stmts.length !== 1) return null
    const stmt = parsed.stmts[0]?.stmt?.TransactionStmt
    if (!stmt) return null
    switch (stmt.kind) {
      case 'TRANS_STMT_BEGIN':
      case 'TRANS_STMT_START':
        return { kind: 'begin' }
      case 'TRANS_STMT_COMMIT':
        return { kind: 'commit' }
      case 'TRANS_STMT_ROLLBACK':
        return { kind: 'rollback' }
      case 'TRANS_STMT_SAVEPOINT':
        return { kind: 'savepoint', name: stmt.savepoint_name }
      case 'TRANS_STMT_RELEASE':
        return { kind: 'release', name: stmt.savepoint_name }
      case 'TRANS_STMT_ROLLBACK_TO':
        return { kind: 'rollback_to', name: stmt.savepoint_name }
      default:
        return null
    }
  } catch {
    return null
  }
}

function commandTagForNodeType(nodeType: string, node: any): string {
  switch (nodeType) {
    case 'AlterDefaultPrivilegesStmt':
      return 'ALTER DEFAULT PRIVILEGES'
    case 'AlterTableStmt':
      return 'ALTER TABLE'
    case 'AlterPublicationStmt':
      return 'ALTER PUBLICATION'
    case 'ClosePortalStmt':
      return 'CLOSE'
    case 'ClusterStmt':
      return 'CLUSTER'
    case 'CommentStmt':
      return 'COMMENT'
    case 'CreateEventTrigStmt':
      return 'CREATE EVENT TRIGGER'
    case 'CreateExtensionStmt':
      return 'CREATE EXTENSION'
    case 'CreateFunctionStmt':
      return 'CREATE FUNCTION'
    case 'CreatePublicationStmt':
      return 'CREATE PUBLICATION'
    case 'CreateSchemaStmt':
      return 'CREATE SCHEMA'
    case 'CreateTrigStmt':
      return 'CREATE TRIGGER'
    case 'CreatedbStmt':
      return 'CREATE DATABASE'
    case 'DeallocateStmt':
      return 'DEALLOCATE'
    case 'DiscardStmt':
      return 'DISCARD'
    case 'DoStmt':
      return 'DO'
    case 'DropStmt':
      switch (node?.removeType) {
        case 'OBJECT_EVENT_TRIGGER':
          return 'DROP EVENT TRIGGER'
        case 'OBJECT_FUNCTION':
          return 'DROP FUNCTION'
        case 'OBJECT_PUBLICATION':
          return 'DROP PUBLICATION'
        case 'OBJECT_TRIGGER':
          return 'DROP TRIGGER'
        default:
          return 'DROP'
      }
    case 'GrantStmt':
      return 'GRANT'
    case 'ListenStmt':
      return 'LISTEN'
    case 'LockStmt':
      return 'LOCK TABLE'
    case 'NotifyStmt':
      return 'NOTIFY'
    case 'RenameStmt':
      return 'ALTER TABLE'
    case 'UnlistenStmt':
      return 'UNLISTEN'
    case 'VariableSetStmt':
      return 'SET'
    case 'VariableShowStmt':
      return 'SHOW'
    default:
      return 'OK'
  }
}

function commandTagForSQL(sql: string): string {
  const txKind = transactionKind(sql)
  if (txKind === 'begin') return 'BEGIN'
  if (txKind === 'commit') return 'COMMIT'
  if (txKind === 'rollback') return 'ROLLBACK'
  if (txKind === 'savepoint') return 'SAVEPOINT'
  if (txKind === 'release') return 'RELEASE'
  if (txKind === 'rollback_to') return 'ROLLBACK'

  try {
    const parsed = parseSync(stripTrailingSemicolon(sql.trim()))
    const stmt = parsed.stmts[0]?.stmt
    if (!stmt) return 'OK'
    const nodeType = statementNodeType(stmt)
    return commandTagForNodeType(nodeType, stmt[nodeType])
  } catch {
    return 'OK'
  }
}

function copySelectSQL(sql: string): { sql: string; binary: boolean } | null {
  try {
    const parsed = parseSync(stripTrailingSemicolon(sql.trim()))
    if (parsed.stmts.length !== 1) return null
    const copy = parsed.stmts[0]?.stmt?.CopyStmt
    if (!copy?.query?.SelectStmt) return null
    const binary = (copy.options ?? []).some((option: any) => {
      const def = option.DefElem
      return (
        def?.defname?.toLowerCase?.() === 'format' &&
        stringValue(def.arg)?.toLowerCase() === 'binary'
      )
    })
    return {
      sql: deparseStatement(parsed.version, { SelectStmt: copy.query.SelectStmt }),
      binary,
    }
  } catch {
    return null
  }
}

function currentSettingValue(name: string): string {
  const vals: Record<string, string> = {
    client_encoding: 'UTF8',
    datestyle: 'ISO, MDY',
    integer_datetimes: 'on',
    intervalstyle: 'postgres',
    lc_messages: 'en_US.UTF-8',
    lc_monetary: 'en_US.UTF-8',
    lc_numeric: 'en_US.UTF-8',
    lc_time: 'en_US.UTF-8',
    max_replication_slots: '10',
    max_wal_senders: '10',
    server_encoding: 'UTF8',
    server_version: '16.0',
    server_version_num: '160000',
    standard_conforming_strings: 'on',
    timezone: 'UTC',
    wal_level: 'logical',
  }
  return vals[name.toLowerCase()] ?? ''
}

function unwrapTypeCast(value: any): any {
  let node = value
  while (node?.TypeCast) node = node.TypeCast.arg
  return node
}

function currentSettingArg(value: any): string | null {
  const node = unwrapTypeCast(value)
  if (!node?.FuncCall || functionName(node.FuncCall) !== 'current_setting') return null
  return node.FuncCall.args?.[0]?.A_Const?.sval?.sval ?? null
}

function catalogConstantValue(value: any): unknown {
  const constant = unwrapTypeCast(value)?.A_Const
  if (!constant) return undefined
  if (constant.isnull) return null
  if (constant.sval) return constant.sval.sval ?? ''
  if (constant.ival) return constant.ival.ival ?? 0
  if (constant.fval) return constant.fval.fval ?? ''
  if (Object.hasOwn(constant, 'boolval')) return constant.boolval?.boolval ? 't' : 'f'
  return undefined
}

function catalogCurrentSettingResult(sql: string): {
  rows: Record<string, unknown>[]
  fields: { name: string; oid?: number }[]
} | null {
  try {
    const parsed = parseSync(stripTrailingSemicolon(sql))
    if (parsed.stmts.length !== 1) return null
    const select = parsed.stmts[0]?.stmt?.SelectStmt
    if (!select?.targetList?.length) return null

    const row: Record<string, unknown> = {}
    const fields: { name: string; oid?: number }[] = []
    for (const targetNode of select.targetList) {
      const target = targetNode.ResTarget
      const setting = currentSettingArg(target?.val)
      const name = target.name ?? 'current_setting'
      if (setting) {
        row[name] = currentSettingValue(setting)
      } else if (target.name) {
        const value = catalogConstantValue(target.val)
        if (value === undefined) continue
        row[name] = value
      } else {
        continue
      }
      fields.push({ name })
    }
    return fields.length ? { rows: [row], fields } : null
  } catch {
    return null
  }
}

interface CatalogTargetField {
  name: string
  source: string | null
  value?: unknown
  oid?: number
}
interface SqliteTableInfo {
  name: string
  sql: string | null
}
interface SqliteColumnInfo {
  cid: number
  name: string
  type: string
  notnull: number
  dflt_value: string | null
  pk: number
}
interface PublicationTableInfo {
  name: string
  schema: string
  tableName: string
  columns: SqliteColumnInfo[]
  primaryKey: string[]
  indexes: PublicationIndexInfo[]
}
interface SqliteIndexInfo {
  seq: number
  name: string
  unique: number
  origin: string | null
  partial: number
}
interface SqliteIndexColumnInfo {
  seqno: number
  cid: number
  name: string | null
  desc: number
  key: number
}
interface PublicationIndexInfo {
  schema: string
  tableName: string
  name: string
  unique: boolean
  isPrimaryKey: boolean
  isReplicaIdentity: boolean
  isImmediate: boolean
  columns: Record<string, 'ASC' | 'DESC'>
}

function columnRefName(value: any): string | null {
  const fields = unwrapTypeCast(value)?.ColumnRef?.fields
  if (!Array.isArray(fields) || fields.length === 0) return null
  return stringValue(fields[fields.length - 1])?.toLowerCase() ?? null
}

function selectTargetFields(select: any): CatalogTargetField[] {
  const fields: CatalogTargetField[] = []
  for (const targetNode of select?.targetList ?? []) {
    const target = targetNode.ResTarget
    if (!target) continue

    const sourceName = columnRefTailName(unwrapTypeCast(target.val))
    const source = sourceName?.toLowerCase() ?? null
    const constant = catalogConstantValue(target.val)
    const func = unwrapTypeCast(target.val)?.FuncCall
    const funcSource = func ? functionDisplayName(func) : null
    const name = target.name ?? sourceName ?? funcSource ?? '?column?'
    const lowerSource = source ?? funcSource?.toLowerCase() ?? null
    const field: CatalogTargetField = {
      name,
      source: lowerSource,
      ...(constant !== undefined ? { value: constant } : null),
    }
    const oid = expressionOid(target.val)
    if (oid) {
      field.oid = oid
    } else if (lowerSource && PUBLICATION_BOOLEAN_FIELDS.has(lowerSource)) {
      field.oid = PG_TYPE_BOOL
    } else if (lowerSource === 'oid') {
      field.oid = PG_TYPE_INT8
    }
    fields.push(field)
  }
  return fields
}

function selectReferencesTable(select: any, tableName: string): boolean {
  const lowerName = tableName.toLowerCase()
  let found = false
  walkAst(select, (node) => {
    if (found) return
    const rangeVar = node.RangeVar
    if (!rangeVar) return
    const schema = String(rangeVar.schemaname ?? '').toLowerCase()
    const rel = String(rangeVar.relname ?? '').toLowerCase()
    if (
      (schema === 'pg_catalog' || schema === 'information_schema' || schema === '') &&
      rel === lowerName
    )
      found = true
  })
  return found
}

function stringConstValue(node: any): string | null {
  const value = unwrapTypeCast(node)?.A_Const?.sval?.sval
  return typeof value === 'string' ? value : null
}

function expressionReferencesColumn(node: any, columnName: string): boolean {
  return columnRefName(node) === columnName.toLowerCase()
}

function collectStringFilterValues(
  node: any,
  columnName: string,
  values: string[]
): void {
  if (!node || typeof node !== 'object') return
  const expr = node.A_Expr
  if (expr) {
    if (expr.kind === 'AEXPR_IN' && expressionReferencesColumn(expr.lexpr, columnName)) {
      for (const item of expr.rexpr?.List?.items ?? []) {
        const value = stringConstValue(item)
        if (value !== null) values.push(value)
      }
      return
    }

    if (
      expr.kind === 'AEXPR_OP' &&
      operatorName(expr) === '=' &&
      expressionReferencesColumn(expr.lexpr, columnName)
    ) {
      const value = stringConstValue(expr.rexpr)
      if (value !== null) values.push(value)
      return
    }
  }

  if (Array.isArray(node)) {
    for (const item of node) collectStringFilterValues(item, columnName, values)
    return
  }
  for (const value of Object.values(node))
    collectStringFilterValues(value, columnName, values)
}

function stringFilterValues(select: any, columnName: string): string[] {
  const values: string[] = []
  collectStringFilterValues(select, columnName, values)
  return [...new Set(values)]
}

function catalogValueForColumn(row: Record<string, unknown>, column: string): unknown {
  const lower = column.toLowerCase()
  if (Object.hasOwn(row, column)) return row[column]
  for (const [key, value] of Object.entries(row)) {
    if (key.toLowerCase() === lower) return value
  }
  return undefined
}

function catalogExpressionValue(
  node: any,
  row: Record<string, unknown>
): unknown | typeof NON_LITERAL_ARRAY_VALUE {
  const unwrapped = unwrapTypeCast(node)
  if (unwrapped?.ColumnRef) {
    const name = columnRefName(unwrapped)
    return name ? catalogValueForColumn(row, name) : undefined
  }
  if (unwrapped?.RowExpr) {
    const values: unknown[] = []
    for (const item of unwrapped.RowExpr.args ?? []) {
      const value = catalogExpressionValue(item, row)
      if (value === NON_LITERAL_ARRAY_VALUE) return value
      values.push(value)
    }
    return values
  }
  const literal = astLiteralValue(unwrapped)
  if (literal !== NON_LITERAL_ARRAY_VALUE) return literal
  return NON_LITERAL_ARRAY_VALUE
}

function catalogValuesEqual(left: unknown, right: unknown): boolean {
  if (Array.isArray(left) || Array.isArray(right)) {
    if (!Array.isArray(left) || !Array.isArray(right)) return false
    if (left.length !== right.length) return false
    return left.every((value, index) => catalogValuesEqual(value, right[index]))
  }
  return String(left) === String(right)
}

function sqlLike(value: string, pattern: string): boolean {
  const memo = new Map<string, boolean>()
  const match = (valueIndex: number, patternIndex: number): boolean => {
    const key = `${valueIndex}:${patternIndex}`
    const cached = memo.get(key)
    if (cached !== undefined) return cached
    let result: boolean
    if (patternIndex === pattern.length) {
      result = valueIndex === value.length
    } else if (pattern[patternIndex] === '\\' && patternIndex + 1 < pattern.length) {
      result =
        valueIndex < value.length &&
        value[valueIndex] === pattern[patternIndex + 1] &&
        match(valueIndex + 1, patternIndex + 2)
    } else if (pattern[patternIndex] === '%') {
      result =
        match(valueIndex, patternIndex + 1) ||
        (valueIndex < value.length && match(valueIndex + 1, patternIndex))
    } else if (pattern[patternIndex] === '_') {
      result = valueIndex < value.length && match(valueIndex + 1, patternIndex + 1)
    } else {
      result =
        valueIndex < value.length &&
        value[valueIndex] === pattern[patternIndex] &&
        match(valueIndex + 1, patternIndex + 1)
    }
    memo.set(key, result)
    return result
  }
  return match(0, 0)
}

function arrayValuesFromExpression(value: unknown): unknown[] {
  if (Array.isArray(value)) return value
  if (typeof value !== 'string') return []
  return parsePgArrayLiteral(value) ?? []
}

function catalogWhereMatches(node: any, row: Record<string, unknown>): boolean {
  if (!node) return true
  const bool = node.BoolExpr
  if (bool) {
    const args = bool.args ?? []
    if (bool.boolop === 'AND_EXPR')
      return args.every((arg: any) => catalogWhereMatches(arg, row))
    if (bool.boolop === 'OR_EXPR')
      return args.some((arg: any) => catalogWhereMatches(arg, row))
    if (bool.boolop === 'NOT_EXPR') return !catalogWhereMatches(args[0], row)
  }

  const expr = node.A_Expr
  if (!expr) return true

  const op = operatorName(expr)
  const left = catalogExpressionValue(expr.lexpr, row)
  if (left === NON_LITERAL_ARRAY_VALUE) return true

  if (expr.kind === 'AEXPR_IN') {
    const values = (expr.rexpr?.List?.items ?? [])
      .map((item: any) => catalogExpressionValue(item, row))
      .filter((value: unknown) => value !== NON_LITERAL_ARRAY_VALUE)
    const found = values.some((value: unknown) => catalogValuesEqual(left, value))
    return op === '<>' ? !found : found
  }

  const right = catalogExpressionValue(expr.rexpr, row)
  if (right === NON_LITERAL_ARRAY_VALUE) return true

  if (expr.kind === 'AEXPR_OP_ALL') {
    const values = arrayValuesFromExpression(right)
    if (op === '<>') return values.every((value) => !catalogValuesEqual(left, value))
    if (op === '=') return values.every((value) => catalogValuesEqual(left, value))
  }
  if (expr.kind === 'AEXPR_OP_ANY') {
    const values = arrayValuesFromExpression(right)
    if (op === '=') return values.some((value) => catalogValuesEqual(left, value))
    if (op === '<>') return values.some((value) => !catalogValuesEqual(left, value))
  }
  if (expr.kind === 'AEXPR_LIKE') {
    const matches = sqlLike(String(left ?? ''), String(right ?? ''))
    return op === '!~~' ? !matches : matches
  }
  if (expr.kind === 'AEXPR_OP') {
    if (op === '=') return catalogValuesEqual(left, right)
    if (op === '<>' || op === '!=') return !catalogValuesEqual(left, right)
  }
  return true
}

interface CatalogIntervalValue {
  intervalSeconds: number
}

function isCatalogIntervalValue(value: unknown): value is CatalogIntervalValue {
  return (
    !!value &&
    typeof value === 'object' &&
    typeof (value as CatalogIntervalValue).intervalSeconds === 'number'
  )
}

function parseIntervalSeconds(value: string): number | null {
  const trimmed = value.trim()
  const match = /^(-?\d+(?:\.\d+)?)\s*([a-z]*)$/i.exec(trimmed)
  if (!match) return null
  const amount = Number(match[1])
  if (!Number.isFinite(amount)) return null
  const unit = match[2].toLowerCase()
  if (
    unit === '' ||
    unit === 's' ||
    unit === 'sec' ||
    unit === 'second' ||
    unit === 'seconds'
  )
    return amount
  if (
    unit === 'ms' ||
    unit === 'msec' ||
    unit === 'millisecond' ||
    unit === 'milliseconds'
  )
    return amount / 1000
  if (unit === 'min' || unit === 'mins' || unit === 'minute' || unit === 'minutes')
    return amount * 60
  if (unit === 'h' || unit === 'hr' || unit === 'hour' || unit === 'hours')
    return amount * 60 * 60
  if (unit === 'd' || unit === 'day' || unit === 'days') return amount * 24 * 60 * 60
  return null
}

function catalogScalarValue(
  node: any,
  row: Record<string, unknown>
): unknown | typeof NON_LITERAL_ARRAY_VALUE {
  if (!node || typeof node !== 'object') return NON_LITERAL_ARRAY_VALUE

  if (node.TypeCast) {
    const castType = typeNameBase(node.TypeCast.typeName)
    const value = catalogScalarValue(node.TypeCast.arg, row)
    if (value === NON_LITERAL_ARRAY_VALUE) return value
    if (castType === 'interval') {
      const seconds = parseIntervalSeconds(String(value ?? ''))
      return seconds === null ? NON_LITERAL_ARRAY_VALUE : { intervalSeconds: seconds }
    }
    if (castType === 'int2' || castType === 'int4' || castType === 'int8') {
      const number = Number(value)
      return Number.isFinite(number) ? Math.trunc(number) : NON_LITERAL_ARRAY_VALUE
    }
    if (castType === 'float4' || castType === 'float8' || castType === 'numeric') {
      const number = Number(value)
      return Number.isFinite(number) ? number : NON_LITERAL_ARRAY_VALUE
    }
    if (castType === 'text' || castType === 'varchar' || castType === 'bpchar')
      return String(value ?? '')
    return value
  }

  if (node.ColumnRef) {
    const name = columnRefName(node)
    return name ? catalogValueForColumn(row, name) : NON_LITERAL_ARRAY_VALUE
  }

  const literal = astLiteralValue(node)
  if (literal !== NON_LITERAL_ARRAY_VALUE) return literal

  const expr = node.A_Expr
  if (expr?.kind === 'AEXPR_OP') {
    const op = operatorName(expr)
    const left = catalogScalarValue(expr.lexpr, row)
    const right = catalogScalarValue(expr.rexpr, row)
    if (left === NON_LITERAL_ARRAY_VALUE || right === NON_LITERAL_ARRAY_VALUE)
      return NON_LITERAL_ARRAY_VALUE
    if (op === '||') return String(left ?? '') + String(right ?? '')
    if (op === '*' || op === '+' || op === '-' || op === '/') {
      const l = Number(left)
      const r = Number(right)
      if (!Number.isFinite(l) || !Number.isFinite(r)) return NON_LITERAL_ARRAY_VALUE
      if (op === '*') return l * r
      if (op === '+') return l + r
      if (op === '-') return l - r
      return r === 0 ? NON_LITERAL_ARRAY_VALUE : l / r
    }
  }

  const func = node.FuncCall
  if (func && functionName(func) === 'extract') {
    const part = catalogScalarValue(func.args?.[0], row)
    const source = catalogScalarValue(func.args?.[1], row)
    if (String(part).toLowerCase() === 'epoch' && isCatalogIntervalValue(source)) {
      return source.intervalSeconds
    }
  }

  return NON_LITERAL_ARRAY_VALUE
}

function projectedCatalogResult(
  select: any,
  sourceRows: Record<string, unknown>[]
): CatalogResult {
  const fields = selectTargetFields(select)
  const targets = select?.targetList ?? []
  const rows = sourceRows
    .filter((row) => catalogWhereMatches(select?.whereClause, row))
    .map((row) =>
      Object.fromEntries(
        fields.map((field, index) => {
          const target = targets[index]?.ResTarget
          const value = target
            ? catalogScalarValue(target.val, row)
            : NON_LITERAL_ARRAY_VALUE
          if (value !== NON_LITERAL_ARRAY_VALUE && !isCatalogIntervalValue(value))
            return [field.name, value]
          if (Object.hasOwn(field, 'value')) return [field.name, field.value]
          if (field.source) return [field.name, catalogValueForColumn(row, field.source)]
          return [field.name, null]
        })
      )
    )

  return {
    rows,
    fields: fields.map((field) => {
      if (field.oid) return { name: field.name, oid: field.oid }
      const sample = rows.find(
        (row) => row[field.name] !== null && row[field.name] !== undefined
      )
      const value = sample?.[field.name]
      if (typeof value === 'boolean') return { name: field.name, oid: PG_TYPE_BOOL }
      if (typeof value === 'number')
        return {
          name: field.name,
          oid: PG_TYPE_FLOAT8,
        }
      return { name: field.name }
    }),
  }
}

const PG_SETTINGS_ROWS: Record<string, unknown>[] = [
  {
    name: 'wal_sender_timeout',
    setting: '60',
    unit: 's',
    vartype: 'integer',
    context: 'sighup',
    boot_val: '60',
    reset_val: '60',
    source: 'default',
    pending_restart: 'f',
  },
  {
    name: 'wal_level',
    setting: 'logical',
    unit: '',
    vartype: 'enum',
    context: 'postmaster',
    boot_val: 'logical',
    reset_val: 'logical',
    source: 'default',
    pending_restart: 'f',
  },
  {
    name: 'server_version_num',
    setting: '160000',
    unit: '',
    vartype: 'integer',
    context: 'internal',
    boot_val: '160000',
    reset_val: '160000',
    source: 'default',
    pending_restart: 'f',
  },
]

function pgSettingsResult(select: any): CatalogResult | null {
  if (!selectReferencesTable(select, 'pg_settings')) return null
  return projectedCatalogResult(select, PG_SETTINGS_ROWS)
}

const PUBLICATION_BOOLEAN_FIELDS = new Set([
  'pubinsert',
  'pubupdate',
  'pubdelete',
  'pubtruncate',
])
const ARRAY_TYPE_ROWS = [
  { oid: 16, typarray: 1000 },
  { oid: 20, typarray: 1016 },
  { oid: 21, typarray: 1005 },
  { oid: 23, typarray: 1007 },
  { oid: 25, typarray: 1009 },
  { oid: 114, typarray: 199 },
  { oid: 700, typarray: 1021 },
  { oid: 701, typarray: 1022 },
  { oid: 1043, typarray: 1015 },
  { oid: 1114, typarray: 1115 },
  { oid: PG_TYPE_TIMESTAMPTZ, typarray: 1185 },
  { oid: 1700, typarray: 1231 },
  { oid: PG_TYPE_JSONB, typarray: 3807 },
]

function publicationOid(name: string): number {
  let hash = 0
  for (let i = 0; i < name.length; i++) hash = (hash * 33 + name.charCodeAt(i)) >>> 0
  return 50_000 + (hash % 10_000_000)
}

function tableOid(name: string): number {
  return publicationOid(`table:${name}`)
}

function isSystemSqliteTable(name: string): boolean {
  return (
    name === '__miniflare_do_name' ||
    name.startsWith('sqlite_') ||
    name.startsWith('_orez_')
  )
}

function sqliteTypeBase(type: string): string {
  const lower = type.trim().toLowerCase()
  const paren = lower.indexOf('(')
  return paren >= 0 ? lower.slice(0, paren) : lower
}

function isLikelyJsonColumn(column: SqliteColumnInfo): boolean {
  const dflt = column.dflt_value?.trim()
  if (!dflt) return false
  return dflt.startsWith("'{}") || dflt.startsWith("'[]") || dflt.startsWith("'{")
}

function pgTypeForSqliteColumn(
  column: SqliteColumnInfo,
  metadata?: SchemaColumnMetadata
): string {
  if (metadata?.dataType) return metadata.dataType
  const type = sqliteTypeBase(column.type)
  if (isLikelyJsonColumn(column) && (type === 'text' || type === 'varchar'))
    return 'jsonb'
  if (type.includes('int')) return 'integer'
  if (type === 'real' || type === 'double' || type === 'float') return 'double precision'
  if (type === 'numeric' || type === 'decimal') return 'numeric'
  if (type === 'json' || type === 'jsonb') return 'jsonb'
  if (type === 'blob' || type === 'bytea') return 'bytea'
  if (type === 'varchar' || type === 'character varying') return 'character varying'
  if (type === 'timestamp' || type === 'timestamptz') return 'timestamp'
  return 'text'
}

function pgTypeOid(dataType: string): number {
  const lower = dataType.toLowerCase()
  if (lower.endsWith('[]')) {
    return arrayTypeOidForElementOid(pgTypeOid(lower.slice(0, -2)))
  }
  switch (lower) {
    case 'boolean':
      return PG_TYPE_BOOL
    case 'integer':
      return PG_TYPE_INT4
    case 'double precision':
      return PG_TYPE_FLOAT8
    case 'numeric':
      return PG_TYPE_NUMERIC
    case 'jsonb':
      return PG_TYPE_JSONB
    case 'bytea':
      return PG_TYPE_BYTEA
    case 'character varying':
      return PG_TYPE_VARCHAR
    case 'timestamp with time zone':
    case 'timestamptz':
      return PG_TYPE_TIMESTAMPTZ
    case 'timestamp without time zone':
    case 'timestamp':
      return PG_TYPE_TIMESTAMP
    default:
      return PG_TYPE_TEXT
  }
}

function pgDataTypeForWireOid(oid: number | undefined): string | null {
  switch (oid) {
    case PG_TYPE_BOOL:
      return 'boolean'
    case PG_TYPE_INT2:
    case PG_TYPE_INT4:
      return 'integer'
    case PG_TYPE_INT8:
      return 'bigint'
    case PG_TYPE_FLOAT8:
      return 'double precision'
    case PG_TYPE_NUMERIC:
      return 'numeric'
    case PG_TYPE_JSON:
    case PG_TYPE_JSONB:
      return 'jsonb'
    case PG_TYPE_BYTEA:
      return 'bytea'
    case PG_TYPE_VARCHAR:
      return 'character varying'
    case PG_TYPE_TIMESTAMP:
      return 'timestamp without time zone'
    case PG_TYPE_TIMESTAMPTZ:
      return 'timestamp with time zone'
    case PG_TYPE_TEXT:
      return 'text'
    default:
      return null
  }
}

function fallbackMetadataForColumnName(
  column: string
): Pick<
  SchemaColumnMetadata,
  'oid' | 'typeOid' | 'dataType' | 'typtype' | 'typname'
> | null {
  const lower = column.toLowerCase()
  if (lower === 'ddldetection') {
    return {
      oid: PG_TYPE_BOOL,
      typeOid: PG_TYPE_BOOL,
      dataType: 'boolean',
      typtype: 'b',
      typname: 'bool',
    }
  }
  if (
    lower === 'publications' ||
    lower === 'initialschema' ||
    lower === 'initialsynccontext' ||
    lower === 'subscribercontext' ||
    lower === 'permissions' ||
    lower === 'result' ||
    lower === 'metadata' ||
    lower === 'change' ||
    lower === 'precommit' ||
    lower === 'rowkey' ||
    lower === 'refcounts' ||
    lower === 'clientast' ||
    lower === 'queryargs' ||
    lower === 'clientschema' ||
    lower === 'backfill'
  ) {
    return {
      oid: PG_TYPE_JSON,
      typeOid: PG_TYPE_JSON,
      dataType: 'json',
      typtype: 'b',
      typname: 'json',
    }
  }
  return null
}

function parsePublicationList(value: unknown): string[] {
  if (Array.isArray(value)) return value.map(String)
  if (typeof value !== 'string') return []
  const trimmed = value.trim()
  if (!trimmed) return []
  try {
    const parsed = JSON.parse(trimmed)
    if (Array.isArray(parsed)) return parsed.map(String)
  } catch {}
  if (trimmed.startsWith('{') && trimmed.endsWith('}')) {
    return trimmed
      .slice(1, -1)
      .split(',')
      .map((part) => part.trim().replace(/^"|"$/g, ''))
      .filter(Boolean)
  }
  return []
}

function shardConfigInfoFromSqliteTable(
  table: string
): { appID: string; shardNum: string; schema: string } | null {
  const match = /_(\d+)_shardConfig$/.exec(table)
  if (!match) return null
  const appID = table.slice(0, match.index)
  if (!appID) return null
  return {
    appID,
    shardNum: match[1],
    schema: `${appID}_${match[1]}`,
  }
}

function metadataPublicationDefinition(
  appID: string,
  shardNum: string
): PublicationDefinition {
  const shardSchema = `${appID}_${shardNum}`
  const tables = new Map<string, PublicationTableRef>()
  for (const table of [
    {
      table: `${appID}_permissions`,
      schema: appID,
      tableName: 'permissions',
    },
    {
      table: `${shardSchema}_clients`,
      schema: shardSchema,
      tableName: 'clients',
    },
    {
      table: `${shardSchema}_mutations`,
      schema: shardSchema,
      tableName: 'mutations',
    },
  ]) {
    tables.set(table.table, table)
  }
  return {
    name: `_${appID}_metadata_${shardNum}`,
    allTables: false,
    schemas: new Set(),
    tables,
  }
}

function publicationCatalogValue(name: string, field: CatalogTargetField): unknown {
  if (Object.hasOwn(field, 'value')) return field.value
  switch (field.source) {
    case 'oid':
      return publicationOid(name)
    case 'pubname':
      return name
    case 'pubinsert':
    case 'pubupdate':
    case 'pubdelete':
    case 'pubtruncate':
      return 't'
    default:
      return null
  }
}

function catalogCurrentSettingResultFromSelect(select: any): CatalogResult | null {
  if (!select?.targetList?.length) return null

  const row: Record<string, unknown> = {}
  const fields: { name: string; oid?: number }[] = []
  for (const targetNode of select.targetList) {
    const target = targetNode.ResTarget
    const setting = currentSettingArg(target?.val)
    const name = target.name ?? 'current_setting'
    if (setting) {
      row[name] = currentSettingValue(setting)
    } else if (target.name) {
      const value = catalogConstantValue(target.val)
      if (value === undefined) continue
      row[name] = value
    } else {
      continue
    }
    fields.push({ name })
  }
  return fields.length ? { rows: [row], fields } : null
}

function pgPublicationResult(
  select: any,
  availablePublications: string[]
): CatalogResult | null {
  if (!selectReferencesTable(select, 'pg_publication')) return null
  const fields = selectTargetFields(select)
  if (fields.length === 0) return { rows: [], fields: [] }

  const requested = stringFilterValues(select, 'pubname')
  const names = requested.length
    ? requested.filter((name) => availablePublications.includes(name))
    : availablePublications
  const sorted = select.sortClause?.length ? [...names].sort() : names
  const rows = sorted.map((name) =>
    Object.fromEntries(
      fields.map((field) => [field.name, publicationCatalogValue(name, field)])
    )
  )
  return {
    rows,
    fields: fields.map((field) => ({ name: field.name, oid: field.oid })),
  }
}

function selectCallsFunction(select: any, name: string): boolean {
  let found = false
  walkAst(select, (node) => {
    if (found) return
    if (node.FuncCall && functionName(node.FuncCall) === name) found = true
  })
  return found
}

function advisoryLockResult(select: any): CatalogResult | null {
  if (!selectCallsFunction(select, 'pg_advisory_xact_lock')) return null
  const fields = selectTargetFields(select)
  const projectedFields = fields.length
    ? fields
    : [{ name: 'pg_advisory_xact_lock', source: 'pg_advisory_xact_lock' }]
  return {
    rows: [Object.fromEntries(projectedFields.map((field) => [field.name, null]))],
    fields: projectedFields.map((field) => ({ name: field.name, oid: field.oid })),
  }
}

function logicalEmitMessageResult(select: any): CatalogResult | null {
  if (!selectCallsFunction(select, 'pg_logical_emit_message')) return null
  const fields = selectTargetFields(select)
  const commitTimeMs = Date.now()
  const isCommitTimeField = (field: CatalogTargetField) =>
    field.name.toLowerCase() === 'committimems' || field.source === 'committimems'
  const isLsnField = (field: CatalogTargetField) =>
    field.name === 'lsn' || field.source === 'pg_logical_emit_message'
  return {
    rows: [
      Object.fromEntries(
        fields.map((field) => [
          field.name,
          isLsnField(field) ? '0/1' : isCommitTimeField(field) ? commitTimeMs : null,
        ])
      ),
    ],
    fields: fields.map((field) => ({
      name: field.name,
      oid: isCommitTimeField(field)
        ? PG_TYPE_FLOAT8
        : isLsnField(field)
          ? PG_TYPE_TEXT
          : field.oid,
    })),
  }
}

function emptyCatalogResultFromSelect(select: any): CatalogResult {
  const fields = selectTargetFields(select)
  return {
    rows: [],
    fields: fields.map((field) => ({ name: field.name, oid: field.oid })),
  }
}

function getSkippedFunctionNames(dbName: string, namespace: string): Set<string> {
  const key = `${namespace}\0${dbName}`
  let names = skippedFunctionNamesByTarget.get(key)
  if (!names) {
    names = new Set()
    skippedFunctionNamesByTarget.set(key, names)
  }
  return names
}

// ── DoBackend class ───────────────────────────────────────────────────────

export class DoBackend {
  ready = false
  closed = false
  private doUrl: string
  private dbName: string
  private httpClient: HttpClient
  private namespace: string
  private skippedFunctionNames: Set<string>
  private triggerFunctions: Map<string, TriggerFunctionDefinition>
  private schemaMetadata: SchemaMetadata
  private publications: Map<string, PublicationDefinition>
  // throttle for reloadPublicationsIfEmpty (self-heal of a stale-empty
  // publication cache). undefined until the first empty-publication write.
  private lastEmptyPublicationReloadAt: number | undefined
  private rewriteCache: Map<string, RewrittenStatement[]>
  private preparedStatements = new Map<string, PreparedStatement>()
  private portals = new Map<string, BoundPortal>()
  // catalog-query answers (pg_tables, information_schema, published-schema)
  // introspect every sqlite table via PRAGMAs. on a remote DO each PRAGMA is a
  // round-trip, and zero-cache fires several catalog queries per pg session —
  // uncached this serialized ~200 round-trips per query and starved /sync
  // connects past the client's connect budget. cache per instance, invalidated
  // on DDL / metadata / publication changes and on rollback.
  private publicationTableInfoCache: PublicationTableInfo[] | null = null
  private readyPromise: Promise<void> | null = null
  private operationMutex = new Mutex()

  // Transaction state. The Durable Object refuses raw SQL BEGIN/COMMIT/SAVEPOINT
  // (Cloudflare requires ctx.storage.transaction()), so PG-style multi-call
  // transactions can't ride on SQLite's own transaction machinery. Instead we
  // emulate rollback by snapshotting each table on its first in-tx write and
  // restoring it atomically through /batch on ROLLBACK. Atomicity of the
  // restore itself comes from the DO's /batch transaction.
  private inTransaction = false
  private txID: string | null = null
  private txSnapshot: TransactionMetadataSnapshot | null = null
  private txDataSnapshots = new Map<string, string | null>()
  private txSnapshotCounter = 0
  // pending persist while inside a transaction. flushed on commit so we don't
  // round-trip to durable storage after every DDL statement in a migration.
  private txMetadataDirty = false
  private txHasTrackedWrite = false
  // durable-metadata rows as last read/written (kind\0key\0subkey -> value).
  // persistDurableMetadata diffs against this and writes only changed rows —
  // a full INSERT OR REPLACE of the whole set on every dirty commit/rollback
  // is what let a crash-looping embed boot re-write ~700 identical
  // _orez_pg_metadata rows per cycle into the SQL DO until the write circuit
  // tripped (2026-07-09 prod incident). null until the first load/persist,
  // which falls back to a full write.
  private lastPersistedMetadata: Map<string, string> | null = null

  // identifies the client process generation owning this backend's
  // transactions in the durable tx journal. a process that knows its previous
  // generation is dead (e.g. the zero-cache embed at boot) recovers orphaned
  // transactions for its own owner id only, so it can never roll back another
  // live client's in-flight transaction.
  private txOwner: string

  constructor(
    doUrl: string,
    dbName: string = 'postgres',
    namespace = 'default',
    opts?: { fetch?: typeof fetch; txOwner?: string }
  ) {
    this.doUrl = doUrl.replace(/\/+$/, '')
    this.dbName = dbName
    this.namespace = namespace
    this.txOwner = opts?.txOwner || 'default'
    this.httpClient = new HttpClient(opts?.fetch)
    this.skippedFunctionNames = getSkippedFunctionNames(dbName, namespace)
    this.triggerFunctions = new Map()
    this.schemaMetadata = new Map()
    this.publications = new Map()
    this.rewriteCache = new Map()
    this.fkRegistry = new FkCascadeRegistry()
  }

  get waitReady(): Promise<void> {
    return this.ensureReady()
  }

  private ensureReady(): Promise<void> {
    if (this.ready) return Promise.resolve()
    if (!this.readyPromise) {
      this.readyPromise = this.init().catch((err) => {
        this.readyPromise = null
        throw err
      })
    }
    return this.readyPromise
  }

  private async init() {
    await loadModule()
    try {
      await this.httpClient.post(this.url('/exec'), JSON.stringify({ sql: 'SELECT 1' }))
    } catch {}
    if (this.dbName === 'postgres') await this.ensureChangeTrackingTables()
    await this.loadDurableMetadata()
    this.ready = true
  }

  private async ensureChangeTrackingTables(): Promise<void> {
    await this.doExecResult(
      "CREATE TABLE IF NOT EXISTS \"_zero_changes\" (watermark INTEGER PRIMARY KEY AUTOINCREMENT, table_name TEXT NOT NULL, op TEXT NOT NULL CHECK (op IN ('INSERT', 'UPDATE', 'DELETE')), row_data TEXT, old_data TEXT, created_at INTEGER NOT NULL DEFAULT (unixepoch()))"
    )
    await this.doExecResult(
      'CREATE TABLE IF NOT EXISTS "_zero_change_state" (id INTEGER PRIMARY KEY CHECK (id = 1), last_value INTEGER NOT NULL DEFAULT 0)'
    )
    await this.doExecResult(
      'INSERT OR IGNORE INTO "_zero_change_state" (id, last_value) VALUES (1, 0)'
    )
    await this.doExecResult(
      'CREATE TABLE IF NOT EXISTS "_orez___zero_watermark" (dummy INTEGER PRIMARY KEY DEFAULT 1, last_value INTEGER NOT NULL DEFAULT 1, is_called INTEGER NOT NULL DEFAULT 0)'
    )
    await this.doExecResult(
      'INSERT OR IGNORE INTO "_orez___zero_watermark" (dummy, last_value, is_called) VALUES (1, 1, 0)'
    )
    await this.doExecResult(
      "CREATE TABLE IF NOT EXISTS \"_orez__zero_replication_slots\" (slot_name TEXT PRIMARY KEY, restart_lsn TEXT NOT NULL DEFAULT '0/1000000', confirmed_flush_lsn TEXT NOT NULL DEFAULT '0/1000000', wal_status TEXT NOT NULL DEFAULT 'reserved', plugin TEXT NOT NULL DEFAULT 'pgoutput', slot_type TEXT NOT NULL DEFAULT 'logical', active INTEGER NOT NULL DEFAULT 0, active_pid INTEGER DEFAULT NULL, created_at INTEGER NOT NULL DEFAULT (unixepoch()))"
    )
    await this.doExecResult(
      'CREATE TABLE IF NOT EXISTS "_orez___zero_streamed_batches" (batch_lsn INTEGER PRIMARY KEY, batch_end INTEGER NOT NULL)'
    )
  }

  private async ensureMetadataTable(): Promise<void> {
    await this.doExecResult(metadataTableDDL())
  }

  private publicationToJSON(publication: PublicationDefinition): string {
    return JSON.stringify({
      name: publication.name,
      allTables: publication.allTables,
      schemas: [...publication.schemas],
      tables: [...publication.tables.entries()],
    })
  }

  private publicationFromJSON(value: string): PublicationDefinition | null {
    try {
      const parsed = JSON.parse(value)
      return {
        name: String(parsed.name),
        allTables: Boolean(parsed.allTables),
        schemas: new Set(Array.isArray(parsed.schemas) ? parsed.schemas.map(String) : []),
        tables: new Map(
          Array.isArray(parsed.tables)
            ? parsed.tables.map(([key, table]: any[]) => [
                String(key),
                {
                  table: String(table.table),
                  schema: String(table.schema),
                  tableName: String(table.tableName),
                },
              ])
            : []
        ),
      }
    } catch {
      return null
    }
  }

  private hydrateSchemaMetadataRow(
    tableName: string,
    columnName: string,
    value: string
  ): boolean {
    const metadata = JSON.parse(value) as SchemaColumnMetadata
    let table = this.schemaMetadata.get(tableName)
    if (!table) {
      table = new Map()
      this.schemaMetadata.set(tableName, table)
    }
    const changed = JSON.stringify(table.get(columnName)) !== value
    table.set(columnName, metadata)
    return changed
  }

  private async loadDurableMetadata(): Promise<void> {
    try {
      await this.ensureMetadataTable()
      const result = await this.doExecResult(
        `SELECT kind, key, subkey, value FROM ${quoteIdentifier(METADATA_TABLE)}`
      )
      this.lastPersistedMetadata = new Map()
      for (const row of result.rows) {
        const kind = String(row.kind ?? '')
        const key = String(row.key ?? '')
        const subkey = String(row.subkey ?? '')
        const value = String(row.value ?? '')
        this.lastPersistedMetadata.set(`${kind}\u0000${key}\u0000${subkey}`, value)
        if (kind === 'schema-column') {
          this.hydrateSchemaMetadataRow(key, subkey, value)
        } else if (kind === 'publication') {
          const publication = this.publicationFromJSON(value)
          if (publication) this.publications.set(key, publication)
        } else if (kind === 'fk_edge') {
          this.fkRegistry.add(key, JSON.parse(value) as FkChild)
        }
      }
      if (await this.repairShardMetadataPublications()) {
        await this.persistDurableMetadata()
      }
    } catch {}
  }

  // Schema migrations persist PostgreSQL type metadata directly into the
  // shared SQL DO, outside this backend instance. If this backend initialized
  // first, its one-time load cached an empty schema and requests then exposed
  // physical SQLite INTEGER/TEXT types to zero forever. Refresh before parsing
  // any request so prepared statements also get semantic parameter types. Do
  // not negative-cache an empty read: the migration may commit immediately
  // after it, which is the race this self-heal exists to close.
  private async reloadSchemaMetadataIfEmpty(force = false): Promise<void> {
    if (this.dbName !== 'postgres' || (!force && this.schemaMetadata.size > 0)) return
    try {
      await this.ensureMetadataTable()
      const result = await this.doExecResult(
        `SELECT kind, key, subkey, value FROM ${quoteIdentifier(METADATA_TABLE)} WHERE kind = 'schema-column'`
      )
      let changed = false
      const persisted = this.lastPersistedMetadata ?? new Map<string, string>()
      this.lastPersistedMetadata = persisted
      for (const row of result.rows) {
        const key = String(row.key ?? '')
        const subkey = String(row.subkey ?? '')
        const value = String(row.value ?? '')
        changed = this.hydrateSchemaMetadataRow(key, subkey, value) || changed
        persisted.set(`schema-column\u0000${key}\u0000${subkey}`, value)
      }
      if (changed) {
        if (this.txSnapshot) {
          // The rows were committed out-of-band, not by this transaction. A
          // later ROLLBACK must preserve them rather than restoring the stale
          // empty snapshot captured before the migration finished.
          this.txSnapshot.schemaMetadata = this.cloneSchemaMetadata(this.schemaMetadata)
        }
        this.rewriteCache.clear()
        this.publicationTableInfoCache = null
      }
    } catch {}
  }

  // self-heal a stale-empty publication cache. publications are durable
  // (_orez_pg_metadata) and SHARED across every DoBackend instance pointed at
  // one DO, but each instance only loads them once at init(). when this
  // backend's init() ran BEFORE another instance (e.g. the schema-migration pg
  // pool) created the publication, this.publications is empty and stays empty —
  // so a public write's change-capture is skipped (trackingForStatement) and the
  // row never reaches _zero_changes / the replica / a client. on CF this is the
  // per-project namespace's empty-fileTree bug: the project's /__soot_pg write
  // backend is constructed by a read/write that races provisioning's CREATE
  // PUBLICATION, caches zero publications, and never recovers. re-read just the
  // publication rows when empty so the first write after the publication exists
  // picks it up. throttled (a genuinely publication-less db must not re-query on
  // every write); cleared whenever a publication appears.
  private async reloadPublicationsIfEmpty(): Promise<void> {
    if (this.dbName !== 'postgres' || this.publications.size > 0) return
    const now = Date.now()
    if (
      this.lastEmptyPublicationReloadAt &&
      now - this.lastEmptyPublicationReloadAt < EMPTY_PUBLICATION_RELOAD_THROTTLE_MS
    ) {
      return
    }
    this.lastEmptyPublicationReloadAt = now
    try {
      await this.ensureMetadataTable()
      const result = await this.doExecResult(
        `SELECT key, value FROM ${quoteIdentifier(METADATA_TABLE)} WHERE kind = 'publication'`
      )
      for (const row of result.rows) {
        const publication = this.publicationFromJSON(String(row.value ?? ''))
        if (publication) this.publications.set(String(row.key ?? ''), publication)
      }
    } catch {}
  }

  private async repairShardMetadataPublications(): Promise<boolean> {
    let changed = false
    const tables = await this.listSqliteTables()
    for (const table of tables) {
      const shardConfig = shardConfigInfoFromSqliteTable(table.name)
      if (!shardConfig) continue

      let result: ExecResult
      try {
        result = await this.doExecResult(
          `SELECT publications FROM ${quoteIdentifier(table.name)} LIMIT 1`
        )
      } catch {
        continue
      }

      const publications = parsePublicationList(result.rows[0]?.publications)
      const metadataPublicationName = `_${shardConfig.appID}_metadata_${shardConfig.shardNum}`
      if (!publications.includes(metadataPublicationName)) continue
      if (this.publications.has(metadataPublicationName)) continue

      this.publications.set(
        metadataPublicationName,
        metadataPublicationDefinition(shardConfig.appID, shardConfig.shardNum)
      )
      changed = true
    }
    return changed
  }

  private async persistDurableMetadata(): Promise<void> {
    try {
      const rows: Array<[string, string, string, string]> = []
      for (const [tableName, columns] of this.schemaMetadata) {
        for (const [columnName, metadata] of columns) {
          rows.push(['schema-column', tableName, columnName, JSON.stringify(metadata)])
        }
      }
      for (const [name, publication] of this.publications) {
        rows.push(['publication', name, '', this.publicationToJSON(publication)])
      }
      // FK cascade edges — durable so the registry survives DO eviction (the
      // app tables persist and CREATE TABLE never re-runs to repopulate it).
      for (const [parentKey, child] of this.fkRegistry.entries()) {
        rows.push([
          'fk_edge',
          parentKey,
          `${child.table}|${child.columns.join(',')}`,
          JSON.stringify(child),
        ])
      }
      // write only rows that differ from what durable storage already holds
      // (seeded by loadDurableMetadata, maintained below). every persisted row
      // is a real DO rows-written cost even when the value is identical —
      // INSERT OR REPLACE always rewrites — and this method fires on every
      // dirty commit AND every rollback, so a crash-looping embed boot used to
      // rewrite the entire set (~700 rows on a real app schema) several times
      // per ~4s cycle into the SQL DO until the write circuit tripped and
      // blocked auth (2026-07-09 prod incident).
      const known = this.lastPersistedMetadata
      const changed =
        known === null
          ? rows
          : rows.filter(
              ([kind, key, subkey, value]) =>
                known.get(`${kind}\u0000${key}\u0000${subkey}`) !== value
            )
      if (changed.length === 0) return
      await this.ensureMetadataTable()
      // single multi-row INSERT OR REPLACE per chunk. previously this was one
      // HTTP roundtrip per row, which dominated boot when migrations touched
      // many columns. Cloudflare DO SQLite has a lower host-param cap than
      // stock SQLite; 4 cols × 20 rows keeps metadata persistence comfortably
      // below that limit.
      const CHUNK = 20
      const persisted = this.lastPersistedMetadata ?? new Map<string, string>()
      this.lastPersistedMetadata = persisted
      for (let i = 0; i < changed.length; i += CHUNK) {
        const chunk = changed.slice(i, i + CHUNK)
        const placeholders = chunk.map(() => '(?, ?, ?, ?)').join(', ')
        const params: string[] = []
        for (const row of chunk) params.push(...row)
        await this.doExecResult(
          `INSERT OR REPLACE INTO ${quoteIdentifier(METADATA_TABLE)} (kind, key, subkey, value) VALUES ${placeholders}`,
          params
        )
        // per-chunk so a mid-persist failure never marks unwritten rows as done
        for (const [kind, key, subkey, value] of chunk) {
          persisted.set(`${kind}\u0000${key}\u0000${subkey}`, value)
        }
      }
    } catch {}
  }

  private async runExclusive<T>(fn: () => Promise<T>): Promise<T> {
    await this.operationMutex.acquire()
    try {
      return await fn()
    } finally {
      this.operationMutex.release()
    }
  }

  async close(): Promise<void> {
    return this.runExclusive(async () => {
      if (this.inTransaction) {
        try {
          await this.rollbackTransaction()
        } catch {
          this.clearTransactionState()
        }
      }
      this.closed = true
    })
  }

  private readyForQuery(): Uint8Array {
    return buildReadyForQuery(this.inTransaction ? STATUS_TRANSACTION : STATUS_IDLE)
  }

  private cloneSchemaMetadata(source: SchemaMetadata): SchemaMetadata {
    const cloned: SchemaMetadata = new Map()
    for (const [table, columns] of source) {
      const columnMap = new Map<string, SchemaColumnMetadata>()
      for (const [column, metadata] of columns) {
        columnMap.set(column, { ...metadata })
      }
      cloned.set(table, columnMap)
    }
    return cloned
  }

  private clonePublications(
    source: Map<string, PublicationDefinition>
  ): Map<string, PublicationDefinition> {
    const cloned = new Map<string, PublicationDefinition>()
    for (const [name, publication] of source) {
      const tables = new Map<string, PublicationTableRef>()
      for (const [key, table] of publication.tables) {
        tables.set(key, { ...table })
      }
      cloned.set(name, {
        name: publication.name,
        allTables: publication.allTables,
        schemas: new Set(publication.schemas),
        tables,
      })
    }
    return cloned
  }

  private cloneTriggerFunctions(
    source: Map<string, TriggerFunctionDefinition>
  ): Map<string, TriggerFunctionDefinition> {
    const cloned = new Map<string, TriggerFunctionDefinition>()
    for (const [name, fn] of source) cloned.set(name, { ...fn })
    return cloned
  }

  private captureTransactionMetadataSnapshot(): TransactionMetadataSnapshot {
    return {
      schemaMetadata: this.cloneSchemaMetadata(this.schemaMetadata),
      publications: this.clonePublications(this.publications),
      skippedFunctionNames: new Set(this.skippedFunctionNames),
      triggerFunctions: this.cloneTriggerFunctions(this.triggerFunctions),
    }
  }

  private restoreTransactionMetadataSnapshot(
    snapshot: TransactionMetadataSnapshot
  ): void {
    this.schemaMetadata.clear()
    for (const [table, columns] of this.cloneSchemaMetadata(snapshot.schemaMetadata)) {
      this.schemaMetadata.set(table, columns)
    }
    this.publications.clear()
    for (const [name, publication] of this.clonePublications(snapshot.publications)) {
      this.publications.set(name, publication)
    }
    this.skippedFunctionNames.clear()
    for (const name of snapshot.skippedFunctionNames) this.skippedFunctionNames.add(name)
    this.triggerFunctions.clear()
    for (const [name, fn] of this.cloneTriggerFunctions(snapshot.triggerFunctions)) {
      this.triggerFunctions.set(name, fn)
    }
    // Cached rewrites may reference metadata that the rollback just invalidated.
    this.rewriteCache.clear()
    this.publicationTableInfoCache = null
  }

  private clearTransactionState(): void {
    this.inTransaction = false
    this.txID = null
    this.txSnapshot = null
    this.txDataSnapshots.clear()
    this.txSnapshotCounter = 0
    this.txMetadataDirty = false
    this.txHasTrackedWrite = false
  }

  private signalTrackedWrite(): void {
    if (this.inTransaction) {
      this.txHasTrackedWrite = true
      return
    }
    signalReplicationChange()
  }

  private newTransactionID(): string {
    const uuid = globalThis.crypto?.randomUUID?.()
    if (uuid) return uuid
    return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`
  }

  private async beginTransaction(): Promise<void> {
    if (this.inTransaction) return
    this.txID = this.newTransactionID()
    this.txSnapshot = this.captureTransactionMetadataSnapshot()
    this.inTransaction = true
  }

  private async commitTransaction(): Promise<void> {
    if (!this.inTransaction) {
      this.clearTransactionState()
      return
    }
    const shouldPersist = this.txMetadataDirty
    const shouldSignal = this.txHasTrackedWrite
    const txID = this.txID
    // ONE atomic commit point on the backend: promotes pending tracked
    // changes and clears the tx journal (snapshots + manifest) in a single
    // storage transaction. read-only transactions skip the round-trip.
    if (txID && (shouldSignal || this.txDataSnapshots.size > 0)) {
      await this.httpClient.post(
        this.url('/commit-tx'),
        JSON.stringify({ transactionID: txID }),
        { 'Content-Type': 'application/json' }
      )
    }
    this.clearTransactionState()
    if (shouldPersist) await this.persistDurableMetadata()
    if (shouldSignal) signalReplicationChange()
  }

  private async rollbackTransaction(): Promise<void> {
    if (!this.inTransaction) {
      this.clearTransactionState()
      return
    }
    const snapshot = this.txSnapshot
    const txID = this.txID
    try {
      // ONE atomic rollback on the backend: restores snapshotted tables from
      // the durable tx journal and discards pending tracked changes.
      if (txID && (this.txHasTrackedWrite || this.txDataSnapshots.size > 0)) {
        await this.httpClient.post(
          this.url('/rollback-tx'),
          JSON.stringify({ transactionID: txID }),
          { 'Content-Type': 'application/json' }
        )
      }
    } finally {
      if (snapshot) this.restoreTransactionMetadataSnapshot(snapshot)
      await this.persistDurableMetadata()
      this.clearTransactionState()
    }
  }

  async execProtocolRaw(
    message: Uint8Array,
    options?: { syncToFs?: boolean; throwOnError?: boolean }
  ): Promise<Uint8Array> {
    return this.runExclusive(() => this.execProtocolRawLocked(message, options))
  }

  private async execProtocolRawLocked(
    message: Uint8Array,
    options?: { syncToFs?: boolean; throwOnError?: boolean }
  ): Promise<Uint8Array> {
    if (!this.ready) await this.waitReady
    if (this.hasMultipleProtocolMessages(message)) {
      const responses: Uint8Array[] = []
      let offset = 0
      while (offset < message.length) {
        const length = this.protocolMessageLength(message, offset)
        if (!length) break
        responses.push(
          await this.execProtocolMessage(
            message.subarray(offset, offset + length),
            options
          )
        )
        offset += length
      }
      return concat(...responses)
    }
    return this.execProtocolMessage(message, options)
  }

  private protocolMessageLength(message: Uint8Array, offset: number): number | null {
    if (offset + 5 > message.length) return null
    const length = new DataView(
      message.buffer,
      message.byteOffset + offset + 1,
      message.byteLength - offset - 1
    ).getInt32(0)
    const total = 1 + length
    if (total <= 0 || offset + total > message.length) return null
    return total
  }

  private hasMultipleProtocolMessages(message: Uint8Array): boolean {
    const firstLength = this.protocolMessageLength(message, 0)
    return Boolean(firstLength && firstLength < message.length)
  }

  private async execProtocolMessage(
    message: Uint8Array,
    options?: { syncToFs?: boolean; throwOnError?: boolean }
  ): Promise<Uint8Array> {
    const msgType = message[0]
    try {
      switch (msgType) {
        case FT_QUERY:
          await this.reloadSchemaMetadataIfEmpty()
          return this.handleSimpleQuery(message)
        case FT_PARSE:
          await this.reloadSchemaMetadataIfEmpty()
          return this.handleParse(message)
        case FT_BIND:
          return this.handleBind(message)
        case FT_DESCRIBE:
          return this.handleDescribe(message)
        case FT_EXECUTE:
          return this.handleExecute(message)
        case FT_SYNC:
          return this.handleSync()
        case FT_CLOSE:
          return this.handleClose(message)
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
    if (!sql) return concat(buildCommandComplete('OK'), this.readyForQuery())

    const normalized = sql.trimStart().toLowerCase()
    const txAction = transactionAction(sql)

    if (txAction) {
      try {
        switch (txAction.kind) {
          case 'begin':
            await this.beginTransaction()
            break
          case 'commit':
            await this.commitTransaction()
            break
          case 'rollback':
            await this.rollbackTransaction()
            break
          // SAVEPOINT / RELEASE / ROLLBACK TO are no-ops: the DO refuses raw
          // SQL transactions, and we can't emulate nested rollback without
          // per-savepoint snapshots. ROLLBACK TO returning success when no
          // savepoint exists is wrong, but chat e2e doesn't exercise it.
          case 'savepoint':
          case 'release':
          case 'rollback_to':
            break
        }
      } catch (err: any) {
        return concat(buildErrorResponse(err.message), this.readyForQuery())
      }
      return concat(buildCommandComplete(commandTagForSQL(sql)), this.readyForQuery())
    }

    // SET (local) — skip
    if (normalized.startsWith('set '))
      return concat(buildCommandComplete('SET'), this.readyForQuery())
    if (normalized.startsWith('show '))
      return concat(buildCommandComplete('SHOW'), this.readyForQuery())
    if (normalized === 'show' || normalized === 'show;')
      return concat(buildCommandComplete('SHOW'), this.readyForQuery())

    // DEALLOCATE, DISCARD, RESET → skip
    if (/^(deallocate|discard|reset)\b/.test(normalized)) {
      return concat(buildCommandComplete('OK'), this.readyForQuery())
    }

    // LOCK TABLE → skip
    if (normalized.startsWith('lock table') || normalized.startsWith('lock ')) {
      return concat(buildCommandComplete('LOCK TABLE'), this.readyForQuery())
    }

    const copySelect = copySelectSQL(sql)
    if (copySelect) {
      const rewrittenCopySelect = this.rewriteSQL(copySelect.sql)
      const result = await this.doExecResult(rewrittenCopySelect)
      return this.buildCopyResponse(result, copySelect.sql, copySelect.binary)
    }

    // Prepare query
    const rewrittenStatements = this.rewriteSQLStatements(sql)
    const rewritten = rewrittenSQLText(rewrittenStatements)
    if (rewritten === '' || rewritten.startsWith('--')) {
      await this.applyStatementMetadata(rewrittenStatements)
      return concat(buildCommandComplete(commandTagForSQL(sql)), this.readyForQuery())
    }

    // Catalog queries — check before forwarding
    if (isCatalogQuery(rewritten)) {
      const results = await this.handleCatalogQueries(rewritten)
      return concat(
        ...results.map((result) => this.buildSelectResult(result)),
        this.readyForQuery()
      )
    }

    // Execute SQL
    try {
      const statement =
        rewrittenStatements.length === 1 ? rewrittenStatements[0] : undefined
      const result = statement
        ? await this.executeRewrittenStatement(statement)
        : this.inTransaction
          ? await this.executeRewrittenStatements(rewrittenStatements)
          : await this.doExecResult(rewritten)
      await this.applyStatementMetadata(rewrittenStatements)
      const tracking = statement ? this.trackingForStatement(statement) : undefined
      const visibleResult = this.visibleResultForTracking(result, tracking)
      return this.buildSQLResponse(sql, visibleResult.rows, visibleResult.columns, {
        affectedRows: visibleResult.affectedRows,
      })
    } catch (err: any) {
      return concat(buildErrorResponse(err.message), this.readyForQuery())
    }
  }

  private async applyStatementMetadata(statements: RewrittenStatement[]): Promise<void> {
    let changed = false
    for (const statement of statements) {
      for (const column of statement.schemaColumns ?? []) {
        let table = this.schemaMetadata.get(column.table)
        if (!table) {
          table = new Map()
          this.schemaMetadata.set(column.table, table)
        }
        table.set(column.column, column)
        changed = true
      }
      for (const change of statement.schemaMetadataChanges ?? []) {
        this.applySchemaMetadataChange(change)
        changed = true
      }
      for (const change of statement.publicationChanges ?? []) {
        this.applyPublicationChange(change)
        changed = true
      }
      if (statement.fkEdges) {
        // edges were already added to this.fkRegistry at rewrite time; persist
        // them and drop cached rewrites so DELETEs issued before this CREATE
        // TABLE now expand their cascade.
        changed = true
        this.rewriteCache.clear()
      }
    }
    if (changed) {
      this.publicationTableInfoCache = null
      // inside an explicit tx we batch the persist to commit time. chat's
      // migrations open one tx and run dozens of DDL statements; persisting
      // after every one was the hot path.
      if (this.inTransaction) this.txMetadataDirty = true
      else await this.persistDurableMetadata()
    }
  }

  private applySchemaMetadataChange(change: SchemaMetadataChange): void {
    if (change.action === 'renameTable') {
      const columns = this.schemaMetadata.get(change.from.table)
      if (columns) {
        const renamed = new Map<string, SchemaColumnMetadata>()
        for (const [name, column] of columns) {
          renamed.set(name, {
            ...column,
            table: change.to.table,
            schema: change.to.schema,
            tableName: change.to.tableName,
          })
        }
        this.schemaMetadata.delete(change.from.table)
        this.schemaMetadata.set(change.to.table, renamed)
      }
      for (const publication of this.publications.values()) {
        const ref = publication.tables.get(change.from.table)
        if (!ref) continue
        publication.tables.delete(change.from.table)
        publication.tables.set(change.to.table, change.to)
      }
      return
    }

    const columns = this.schemaMetadata.get(change.table.table)
    const column = columns?.get(change.from)
    if (!columns || !column) return
    columns.delete(change.from)
    columns.set(change.to, { ...column, column: change.to })
  }

  private applyPublicationChange(change: PublicationChange): void {
    if (change.action === 'drop') {
      this.publications.delete(change.name)
      return
    }

    let publication = this.publications.get(change.name)
    if (!publication || change.action === 'create' || change.action === 'set') {
      publication = {
        name: change.name,
        allTables: false,
        schemas: new Set(),
        tables: new Map(),
      }
      this.publications.set(change.name, publication)
    }

    if (change.action === 'set') {
      publication.allTables = false
      publication.schemas.clear()
      publication.tables.clear()
    }

    if (change.allTables) publication.allTables = true
    for (const schema of change.schemas ?? []) {
      if (change.action === 'remove') publication.schemas.delete(schema)
      else publication.schemas.add(schema)
    }
    for (const table of change.tables ?? []) {
      if (change.action === 'remove') publication.tables.delete(table.table)
      else publication.tables.set(table.table, table)
    }
  }

  publicationNames(): string[] {
    return [...this.publications.keys()]
  }

  private trackingForStatement(
    statement: RewrittenStatement
  ): ChangeTrackingMetadata | undefined {
    const tracking = statement.changeTracking
    if (!tracking || this.dbName !== 'postgres') return undefined
    const { table } = tracking
    if (table.schema === 'public') {
      const publications = this.publicationNames()
      if (!publications.length) return undefined
      return this.publicationsForTable(table, publications).length ? tracking : undefined
    }
    if (
      table.schema !== 'pg_catalog' &&
      table.schema !== 'information_schema' &&
      !table.schema.startsWith('pg_') &&
      !table.schema.startsWith('zero_') &&
      !table.schema.startsWith('_zero') &&
      !table.schema.includes('/') &&
      TRACKED_SHARD_TABLES.has(table.tableName)
    ) {
      return tracking
    }
    return undefined
  }

  private trackingRequest(tracking: ChangeTrackingMetadata): ChangeTrackingRequest {
    const rowColumns = this.schemaMetadata.get(tracking.table.table)
    return {
      tableName: `${tracking.table.schema}.${tracking.table.tableName}`,
      operation: tracking.operation,
      returnRows: tracking.returnRows,
      ...(rowColumns ? { rowColumns: [...rowColumns.keys()] } : null),
      ...(this.inTransaction ? { transactionID: this.currentTransactionID() } : null),
    }
  }

  private visibleResultForTracking(
    result: ExecResult,
    tracking: ChangeTrackingMetadata | undefined
  ): ExecResult {
    if (!tracking) return result
    if (!tracking.returnRows)
      return { rows: [], columns: [], affectedRows: result.affectedRows }
    if (tracking.returningProjection)
      return projectReturningResult(result, tracking.returningProjection)
    return result
  }

  private async currentPublishedSchema(publications = [...this.publications.keys()]) {
    const infos = await this.publicationTableInfos(publications)
    return {
      tables: infos.map((info) => this.publicationTableSpec(info, publications)),
      indexes: infos.flatMap((info) => info.indexes),
    }
  }

  private async materializePublishedSchemaFunctions(
    sql: string,
    statement?: RewrittenStatement,
    params: any[] = []
  ): Promise<{ sql: string; params: any[] }> {
    if (!statement?.usesPublishedSchemaFunction) return { sql, params }
    const replaced = replaceSchemaSpecsFunctionCalls(sql)
    if (replaced.count === 0) return { sql, params }
    const schema = await this.currentPublishedSchema()
    const value = JSON.stringify(schema)
    return {
      sql: replaced.sql,
      params: [...params, ...Array.from({ length: replaced.count }, () => value)],
    }
  }

  // ── Extended protocol handlers ──────────────────────────────────────────

  private handleParse(data: Uint8Array): Uint8Array {
    const sql = extractParseQuery(data)
    const stmtName = extractParseStatementName(data)
    if (sql) {
      const rewrittenStatements = this.rewriteSQLStatements(sql)
      const rewritten = rewrittenSQLText(rewrittenStatements)
      const paramOIDs = inferParamOidsForSQL(
        rewritten,
        inferParamOidsForSQL(sql, extractParseParamOIDs(data), this.schemaMetadata),
        this.schemaMetadata
      )
      const timestampParamNumbers = paramNumbersForOids(paramOIDs, isTimestampOid)
      const booleanParamNumbers = paramNumbersForOids(paramOIDs, isBooleanOid)
      const inferredJsonParamNumbers = paramNumbersForOids(paramOIDs, isJsonOid)
      if (rewritten && !rewritten.startsWith('--')) {
        const arrayParamNumbers = new Set<number>()
        const jsonParamNumbers = new Set<number>(inferredJsonParamNumbers)
        const epochMillisParamNumbers = new Set<number>()
        const schemaColumns: SchemaColumnMetadata[] = []
        const schemaMetadataChanges: SchemaMetadataChange[] = []
        const publicationChanges: PublicationChange[] = []
        for (const statement of rewrittenStatements) {
          for (const number of statement.arrayParamNumbers ?? []) {
            arrayParamNumbers.add(number)
          }
          for (const number of statement.jsonParamNumbers ?? []) {
            jsonParamNumbers.add(number)
          }
          for (const number of statement.epochMillisParamNumbers ?? []) {
            epochMillisParamNumbers.add(number)
          }
          schemaColumns.push(...(statement.schemaColumns ?? []))
          schemaMetadataChanges.push(...(statement.schemaMetadataChanges ?? []))
          publicationChanges.push(...(statement.publicationChanges ?? []))
        }
        this.preparedStatements.set(stmtName, {
          sql: rewritten,
          originalSql: sql,
          rewrittenStatements,
          paramOIDs,
          ...(arrayParamNumbers.size ? { arrayParamNumbers } : null),
          ...(jsonParamNumbers.size ? { jsonParamNumbers } : null),
          ...(timestampParamNumbers.size ? { timestampParamNumbers } : null),
          ...(epochMillisParamNumbers.size ? { epochMillisParamNumbers } : null),
          ...(booleanParamNumbers.size ? { booleanParamNumbers } : null),
          ...(schemaColumns.length ? { schemaColumns } : null),
          ...(schemaMetadataChanges.length ? { schemaMetadataChanges } : null),
          ...(publicationChanges.length ? { publicationChanges } : null),
        })
      } else {
        this.preparedStatements.set(stmtName, {
          sql: '',
          originalSql: sql,
          rewrittenStatements,
          paramOIDs,
          ...(inferredJsonParamNumbers.size
            ? { jsonParamNumbers: inferredJsonParamNumbers }
            : null),
          ...(timestampParamNumbers.size ? { timestampParamNumbers } : null),
          ...(rewrittenStatements.some(
            (statement) => statement.epochMillisParamNumbers?.size
          )
            ? {
                epochMillisParamNumbers: new Set(
                  rewrittenStatements.flatMap((statement) => [
                    ...(statement.epochMillisParamNumbers ?? []),
                  ])
                ),
              }
            : null),
          ...(booleanParamNumbers.size ? { booleanParamNumbers } : null),
          schemaColumns: rewrittenStatements.flatMap(
            (statement) => statement.schemaColumns ?? []
          ),
          schemaMetadataChanges: rewrittenStatements.flatMap(
            (statement) => statement.schemaMetadataChanges ?? []
          ),
          publicationChanges: rewrittenStatements.flatMap(
            (statement) => statement.publicationChanges ?? []
          ),
          commandTag: commandTagForSQL(sql),
        })
      }
    }
    return buildParseComplete()
  }

  private handleBind(data: Uint8Array): Uint8Array {
    const portalName = extractBindPortalName(data)
    const stmtName = extractBindStatementName(data)
    const params = extractBindParams(data)
    const stmt = this.preparedStatements.get(stmtName)
    if (!stmt)
      return buildErrorResponse(`prepared statement "${stmtName}" does not exist`)
    this.portals.set(portalName, { ...stmt, statementName: stmtName, params })
    return buildBindComplete()
  }

  private async handleExecute(data: Uint8Array): Promise<Uint8Array> {
    const portalName = extractExecutePortalName(data)
    const portal = this.portals.get(portalName)
    if (!portal) return buildErrorResponse(`portal "${portalName}" does not exist`)
    if (!portal.sql?.trim()) {
      await this.applyStatementMetadata([
        {
          sql: '',
          schemaColumns: portal.schemaColumns ?? [],
          schemaMetadataChanges: portal.schemaMetadataChanges ?? [],
          publicationChanges: portal.publicationChanges ?? [],
        },
      ])
      return buildCommandComplete(portal.commandTag ?? 'OK')
    }

    const sql = portal.sql

    const normalized = sql.trimStart().toLowerCase()
    const txAction = transactionAction(sql)

    if (txAction) {
      try {
        switch (txAction.kind) {
          case 'begin':
            await this.beginTransaction()
            break
          case 'commit':
            await this.commitTransaction()
            break
          case 'rollback':
            await this.rollbackTransaction()
            break
          case 'savepoint':
          case 'release':
          case 'rollback_to':
            break
        }
      } catch (err: any) {
        return buildErrorResponse(err.message)
      }
      return buildCommandComplete(commandTagForSQL(sql))
    }
    if (normalized.startsWith('set ')) return buildCommandComplete('SET')

    try {
      if (isCatalogQuery(sql)) {
        const catalogSql = this.inlineParams(
          sql,
          portal.params,
          portal.arrayParamNumbers,
          portal.jsonParamNumbers,
          portal.timestampParamNumbers,
          portal.epochMillisParamNumbers,
          portal.booleanParamNumbers
        )
        const result = await this.handleCatalogQuery(catalogSql)
        return concat(
          buildRowDescription(result.fields),
          ...result.rows.map((r) => buildDataRow(r, result.fields)),
          buildCommandComplete(`SELECT ${result.rows.length}`)
        )
      }

      const statement =
        portal.rewrittenStatements?.length === 1
          ? portal.rewrittenStatements[0]
          : undefined
      if (statement) await this.snapshotTransactionWrite(statement)
      const tracking = statement ? this.trackingForStatement(statement) : undefined
      const bound = this.sqliteBoundSQL(
        tracking?.returningSQL ?? sql,
        portal.params,
        portal.arrayParamNumbers,
        portal.jsonParamNumbers,
        portal.timestampParamNumbers,
        portal.epochMillisParamNumbers,
        portal.booleanParamNumbers
      )
      const exec = await this.materializePublishedSchemaFunctions(
        bound.sql,
        statement,
        bound.params
      )
      if (statement?.cascadeStatements?.length) {
        await this.runCascadeStatements(statement.cascadeStatements, portal)
      }
      const result = await this.doExecResult(
        exec.sql,
        exec.params,
        tracking ? this.trackingRequest(tracking) : undefined
      )
      if (portal.schemaColumns?.length) {
        await this.applyStatementMetadata([{ sql, schemaColumns: portal.schemaColumns }])
      }
      const visibleResult = this.visibleResultForTracking(result, tracking)
      const visibleRows = visibleResult.rows
      const visibleColumns = visibleResult.columns
      const fields = this.fieldsForResult(portal.originalSql ?? sql, {
        rows: visibleRows,
        columns: visibleColumns,
      })
      const affectedRows = visibleResult.affectedRows ?? result.rows.length
      const commandTag = isSelectLike(sql)
        ? `SELECT ${visibleRows.length}`
        : /^\s*insert\b/i.test(sql)
          ? `INSERT 0 ${affectedRows}`
          : /^\s*update\b/i.test(sql)
            ? `UPDATE ${affectedRows}`
            : /^\s*delete\b/i.test(sql)
              ? `DELETE ${affectedRows}`
              : 'OK'
      if (visibleRows.length > 0) {
        return concat(
          buildRowDescription(fields),
          ...visibleRows.map((r) => buildDataRow(r, fields)),
          buildCommandComplete(commandTag)
        )
      }
      const isSelect = /^\s*select\b/i.test(sql) || /^\s*with\b/i.test(sql)
      return concat(
        isSelect && fields.length > 0 ? buildRowDescription(fields) : new Uint8Array(0),
        buildCommandComplete(commandTag)
      )
    } catch (err: any) {
      return buildErrorResponse(err.message)
    }
  }

  private handleSync(): Uint8Array {
    return this.readyForQuery()
  }

  private async handleDescribe(data: Uint8Array): Promise<Uint8Array> {
    const describeType = extractDescribeType(data)
    const name = extractDescribeName(data)
    const target =
      describeType === 'P' ? this.portals.get(name) : this.preparedStatements.get(name)
    if (!target) return buildNoData()

    if (describeType === 'P') {
      const fields =
        target.fields ??
        (await this.describeFields(target.sql, target.originalSql ?? target.sql))
      target.fields = fields
      return fields.length > 0 ? buildRowDescription(fields) : buildNoData()
    }

    const fields = await this.describeFields(target.sql, target.originalSql ?? target.sql)
    target.fields = fields
    return concat(
      buildParameterDescription(target.paramOIDs),
      fields.length > 0 ? buildRowDescription(fields) : buildNoData()
    )
  }

  private handleClose(data: Uint8Array): Uint8Array {
    const closeType = extractCloseType(data)
    const name = extractCloseName(data)
    if (closeType === 'P') {
      this.portals.delete(name)
    } else {
      this.preparedStatements.delete(name)
      for (const [portalName, portal] of this.portals) {
        if (portal.statementName === name) this.portals.delete(portalName)
      }
    }
    return buildCloseComplete()
  }

  // ── High-level API ──────────────────────────────────────────────────────

  async exec(sql: string): Promise<any[]> {
    return this.runExclusive(() => this.execLocked(sql))
  }

  private async execLocked(sql: string): Promise<any[]> {
    if (!this.ready) await this.waitReady
    await this.reloadSchemaMetadataIfEmpty()
    if (await this.handleTransactionControl(sql)) return []
    const statements = this.rewriteSQLStatements(sql)
    const rewritten = rewrittenSQLText(statements)
    if (!rewritten) {
      await this.applyStatementMetadata(statements)
      return []
    }
    if (isCatalogQuery(rewritten)) return (await this.handleCatalogQuery(rewritten)).rows
    const statement = statements.length === 1 ? statements[0] : undefined
    if (statements.some((item) => item.usesPublishedSchemaFunction)) {
      const result = await this.executeRewrittenStatements(statements)
      await this.applyStatementMetadata(statements)
      const tracking = statement ? this.trackingForStatement(statement) : undefined
      // metadata: original SQL only — see normalizedHighLevelResult comment in query().
      return this.normalizedHighLevelResult(
        sql,
        this.visibleResultForTracking(result, tracking)
      ).rows
    }
    if (statement) await this.snapshotTransactionWrite(statement)
    if (statement?.cascadeStatements?.length) {
      await this.runCascadeStatements(statement.cascadeStatements, {})
    }
    const tracking = statement ? this.trackingForStatement(statement) : undefined
    const result = await this.doExecResult(
      tracking?.returningSQL ?? rewritten,
      undefined,
      tracking ? this.trackingRequest(tracking) : undefined
    )
    await this.applyStatementMetadata(statements)
    return this.normalizedHighLevelResult(
      sql,
      this.visibleResultForTracking(result, tracking)
    ).rows
  }

  private async handleTransactionControl(sql: string): Promise<boolean> {
    const action = transactionAction(sql)
    if (!action) return false
    switch (action.kind) {
      case 'begin':
        await this.beginTransaction()
        break
      case 'commit':
        await this.commitTransaction()
        break
      case 'rollback':
        await this.rollbackTransaction()
        break
      case 'savepoint':
      case 'release':
      case 'rollback_to':
        break
    }
    return true
  }

  async query<T = Record<string, unknown>>(
    sql: string,
    params?: any[]
  ): Promise<{ rows: T[] }> {
    return this.runExclusive(() => this.queryLocked(sql, params))
  }

  /**
   * Execute one caller request's ordered statements while holding the backend
   * operation lock for the entire request. This is required for clients that
   * express a Postgres transaction as separate BEGIN / statement / COMMIT
   * calls: transaction state belongs to this shared DoBackend instance, so
   * releasing the lock between those calls lets another request join, commit,
   * or roll back the transaction in progress.
   */
  async queryBatch<T = Record<string, unknown>>(
    statements: Array<{ sql: string; params?: any[] }>
  ): Promise<Array<{ rows: T[] }>> {
    return this.runExclusive(async () => {
      const results: Array<{ rows: T[] }> = []
      for (const statement of statements) {
        results.push(await this.queryLocked<T>(statement.sql, statement.params))
      }
      return results
    })
  }

  private async queryLocked<T = Record<string, unknown>>(
    sql: string,
    params?: any[]
  ): Promise<{ rows: T[] }> {
    if (!this.ready) await this.waitReady
    await this.reloadSchemaMetadataIfEmpty()
    if (await this.handleTransactionControl(sql)) return { rows: [] }
    const statements = this.rewriteSQLStatements(sql)
    const rewritten = rewrittenSQLText(statements)
    const paramOIDs = inferParamOidsForSQL(
      rewritten,
      inferParamOidsForSQL(sql, [], this.schemaMetadata),
      this.schemaMetadata
    )
    const timestampParamNumbers = paramNumbersForOids(paramOIDs, isTimestampOid)
    const booleanParamNumbers = paramNumbersForOids(paramOIDs, isBooleanOid)
    const inferredJsonParamNumbers = paramNumbersForOids(paramOIDs, isJsonOid)
    if (!rewritten) {
      await this.applyStatementMetadata(statements)
      return { rows: [] }
    }
    if (isCatalogQuery(rewritten)) {
      const catalogSql = this.inlineStatementParams(
        rewritten,
        params,
        statements,
        inferredJsonParamNumbers,
        timestampParamNumbers,
        new Set<number>(),
        booleanParamNumbers
      )
      return { rows: (await this.handleCatalogQuery(catalogSql)).rows as T[] }
    }
    const arrayParamNumbers = new Set<number>()
    const jsonParamNumbers = new Set<number>(inferredJsonParamNumbers)
    const epochMillisParamNumbers = new Set<number>()
    for (const statement of statements) {
      for (const number of statement.arrayParamNumbers ?? []) {
        arrayParamNumbers.add(number)
      }
      for (const number of statement.jsonParamNumbers ?? []) {
        jsonParamNumbers.add(number)
      }
      for (const number of statement.epochMillisParamNumbers ?? []) {
        epochMillisParamNumbers.add(number)
      }
    }
    const bound = this.sqliteBoundSQL(
      rewritten,
      params,
      arrayParamNumbers,
      jsonParamNumbers,
      timestampParamNumbers,
      epochMillisParamNumbers,
      booleanParamNumbers
    )
    const statement = statements.length === 1 ? statements[0] : undefined
    if (statement) await this.snapshotTransactionWrite(statement)
    // a public DML write whose publication cache is empty may have been built
    // before another backend instance created the publication — self-heal the
    // stale-empty cache so its change-capture isn't silently skipped.
    if (statement?.changeTracking?.table.schema === 'public') {
      await this.reloadPublicationsIfEmpty()
    }
    const tracking = statement ? this.trackingForStatement(statement) : undefined
    const execBound = tracking
      ? this.sqliteBoundSQL(
          tracking.returningSQL,
          params,
          arrayParamNumbers,
          jsonParamNumbers,
          timestampParamNumbers,
          epochMillisParamNumbers,
          booleanParamNumbers
        )
      : bound
    const exec = await this.materializePublishedSchemaFunctions(
      execBound.sql,
      statement,
      execBound.params
    )
    if (statement?.cascadeStatements?.length) {
      await this.runCascadeStatements(statement.cascadeStatements, {
        params,
        arrayParamNumbers,
        jsonParamNumbers,
        timestampParamNumbers,
        epochMillisParamNumbers,
        booleanParamNumbers,
      })
    }
    const result = await this.doExecResult(
      exec.sql,
      exec.params,
      tracking ? this.trackingRequest(tracking) : undefined
    )
    await this.applyStatementMetadata(statements)
    // metadata for the returned columns must be derived from the ORIGINAL SQL,
    // not the rewritten one: rewriteNode() strips every TypeCast node (so the
    // SQLite executor sees expressions in their PG-equivalent form), which
    // means `(row_to_json(t))::text AS zql_result` loses its `::text` cast in
    // the rewritten SQL and `expressionOid` then sees only `row_to_json(...)`
    // and reports the column as PG_TYPE_JSON. that triggers postgresQueryJson
    // → JSON.parse on the value, returning a JS object where the apex caller
    // expected a JSON-text string — zero's json-custom-numbers parser then
    // String()s the object to `[object Object]` and the permission read on a
    // server-side custom mutator throws "Unexpected 'o', expecting JSON value".
    return {
      rows: this.normalizedHighLevelResult(
        sql,
        this.visibleResultForTracking(result, tracking)
      ).rows as T[],
    }
  }

  // ── Internal helpers ─────────────────────────────────────────────────────

  private url(path: string): string {
    const qs = new URLSearchParams({
      db: this.dbName,
      ns: this.namespace,
    })
    return `${this.doUrl}${path}?${qs}`
  }

  private rewriteSQLStatements(sql: string): RewrittenStatement[] {
    const key = sql.trim()
    const cached = this.rewriteCache.get(key)
    if (cached) return cached

    const arrayParamNumbers = new Set<number>()
    const jsonParamNumbers = new Set<number>()
    const epochMillisParamNumbers = new Set<number>()
    const statements = rewriteSQLStatements(sql, {
      skippedFunctionNames: this.skippedFunctionNames,
      triggerFunctions: this.triggerFunctions,
      arrayParamNumbers,
      jsonParamNumbers,
      epochMillisParamNumbers,
      fkRegistry: this.fkRegistry,
    })
    if (this.canCacheRewrite(statements)) {
      this.rememberRewrite(key, statements)
    } else if (this.rewriteCache.size) {
      this.rewriteCache.clear()
    }
    return statements
  }

  private canCacheRewrite(statements: RewrittenStatement[]): boolean {
    return (
      statements.length > 0 &&
      statements.every(
        (statement) =>
          statement.sql.trim() &&
          !statement.isDDL &&
          !statement.usesPublishedSchemaFunction &&
          !statement.schemaColumns?.length &&
          !statement.schemaMetadataChanges?.length &&
          !statement.publicationChanges?.length
      )
    )
  }

  private rememberRewrite(key: string, statements: RewrittenStatement[]): void {
    if (!key) return
    if (this.rewriteCache.size >= MAX_REWRITE_CACHE_ENTRIES) {
      const firstKey = this.rewriteCache.keys().next().value
      if (firstKey) this.rewriteCache.delete(firstKey)
    }
    this.rewriteCache.set(key, statements)
  }

  private rewriteSQL(sql: string): string {
    const arrayParamNumbers = new Set<number>()
    const jsonParamNumbers = new Set<number>()
    const epochMillisParamNumbers = new Set<number>()
    return rewriteSQL(sql, {
      skippedFunctionNames: this.skippedFunctionNames,
      triggerFunctions: this.triggerFunctions,
      arrayParamNumbers,
      jsonParamNumbers,
      epochMillisParamNumbers,
      fkRegistry: this.fkRegistry,
    })
  }

  private normalizedHighLevelResult(sql: string, result: ExecResult): ExecResult {
    if (result.rows.length === 0) return result
    const fields = this.fieldsForResult(sql, result)
    if (fields.length === 0 || fields.every((field) => !field.oid)) return result
    const fieldByName = new Map(fields.map((field) => [field.name, field]))
    return {
      ...result,
      rows: result.rows.map((row) =>
        Object.fromEntries(
          Object.entries(row).map(([name, value]) => [
            name,
            postgresQueryValue(value, fieldByName.get(name)?.oid),
          ])
        )
      ),
    }
  }

  private inlineStatementParams(
    sql: string,
    params: any[] | undefined,
    statements: RewrittenStatement[],
    inferredJsonParamNumbers = new Set<number>(),
    timestampParamNumbers = new Set<number>(),
    epochMillisParamNumbers = new Set<number>(),
    booleanParamNumbers = new Set<number>()
  ): string {
    if (!params?.length) return sql
    const arrayParamNumbers = new Set<number>()
    const jsonParamNumbers = new Set<number>(inferredJsonParamNumbers)
    for (const statement of statements) {
      for (const number of statement.arrayParamNumbers ?? []) {
        arrayParamNumbers.add(number)
      }
      for (const number of statement.jsonParamNumbers ?? []) {
        jsonParamNumbers.add(number)
      }
      for (const number of statement.epochMillisParamNumbers ?? []) {
        epochMillisParamNumbers.add(number)
      }
    }
    return this.inlineParams(
      sql,
      params,
      arrayParamNumbers,
      jsonParamNumbers,
      timestampParamNumbers,
      epochMillisParamNumbers,
      booleanParamNumbers
    )
  }

  private async doExec(sql: string, params?: any[]): Promise<SqliteRow[]> {
    return (await this.doExecResult(sql, params)).rows
  }

  private async doExecResult(
    sql: string,
    params?: any[],
    track?: ChangeTrackingRequest
  ): Promise<ExecResult> {
    if (!sql.trim()) return { rows: [], columns: [] }
    const execSQL = sql
    let lastErr: unknown
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        const resp = await this.httpClient.post(
          this.url('/exec'),
          JSON.stringify({
            sql: execSQL,
            ...(params?.length ? { params } : null),
            ...(track ? { track } : null),
          }),
          { 'Content-Type': 'application/json' }
        )
        const result = JSON.parse(resp)
        const rows = (result.rows ?? result ?? []) as SqliteRow[]
        const columns =
          Array.isArray(result.columns) && result.columns.length > 0
            ? result.columns.map(String)
            : rows.length > 0
              ? Object.keys(rows[0])
              : []
        // counted-delete CTEs (zero's changeLog purge) rewrite to a DELETE
        // whose RETURNING alias is a self-describing count marker; fold the
        // marker rows back into the original single-count-row shape.
        const counted = track ? null : foldCountMarkerResult(rows.length, execSQL)
        if (counted) {
          return { ...counted, affectedRows: rows.length }
        }
        if (track) this.signalTrackedWrite()
        return { rows, columns, affectedRows: result.affectedRows }
      } catch (err) {
        lastErr = err
      }
    }

    throw lastErr instanceof Error ? lastErr : new Error(String(lastErr))
  }

  private currentTransactionID(): string {
    if (!this.inTransaction || !this.txID) {
      throw new Error('internal transaction state is missing a transaction id')
    }
    return this.txID
  }

  private transactionSnapshotName(txID: string, table: string): string {
    const safeTable = table.replace(/[^A-Za-z0-9_]/g, '_')
    return `_orez_tx_${txID}_${this.txSnapshotCounter++}_${safeTable}`
  }

  private async tableExistsInDo(table: string): Promise<boolean> {
    const result = await this.doExecResult(
      "SELECT 1 AS ok FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
      [table]
    )
    return result.rows.length > 0
  }

  private async snapshotTransactionTable(table: string): Promise<void> {
    if (!this.inTransaction || this.txDataSnapshots.has(table)) return
    if (table.startsWith('_orez_tx_')) return
    const txID = this.currentTransactionID()
    // skip the sqlite_master probe when we already have schema metadata for
    // the table — registration only happens after a successful CREATE, so its
    // presence is proof the table exists. saves one /exec on the first write
    // to each table per tx, which on chat's hot mutation paths matters.
    const exists = this.schemaMetadata.has(table) || (await this.tableExistsInDo(table))
    // snapshot + manifest row land in one atomic /batch, so a DO kill at any
    // point leaves either no trace or a journal entry recovery can roll back.
    // the DDL is idempotent and rides the same batch (no extra round-trip).
    const snapshot = exists ? this.transactionSnapshotName(txID, table) : null
    const statements: Array<{ sql: string; params?: any[] }> = [{ sql: TX_MANIFEST_DDL }]
    if (snapshot) {
      statements.push({
        sql: `CREATE TABLE ${quoteIdentifier(snapshot)} AS SELECT * FROM ${quoteIdentifier(table)}`,
      })
    }
    statements.push({
      sql: `INSERT INTO "${TX_MANIFEST_TABLE}" (tx_id, owner, original, snapshot) VALUES (?, ?, ?, ?)`,
      params: [txID, this.txOwner, table, snapshot],
    })
    await this.doRawBatch(statements)
    this.txDataSnapshots.set(table, snapshot)
  }

  private async snapshotTransactionWrite(statement: RewrittenStatement): Promise<void> {
    if (!this.inTransaction || !statement.isWrite) return
    const table = statement.writeTable?.table
    if (table) await this.snapshotTransactionTable(table)
  }

  private async doRawBatch(
    statements: Array<string | { sql: string; params?: any[] }>
  ): Promise<void> {
    const sqls = statements.filter((statement) =>
      (typeof statement === 'string' ? statement : statement.sql).trim()
    )
    if (sqls.length === 0) return
    await this.httpClient.post(this.url('/batch'), JSON.stringify({ statements: sqls }), {
      'Content-Type': 'application/json',
    })
  }

  /** run read statements in one /batch round-trip and return per-statement results. */
  private async doBatchResults(statements: string[]): Promise<ExecResult[]> {
    if (statements.length === 0) return []
    const resp = await this.httpClient.post(
      this.url('/batch'),
      JSON.stringify({ statements }),
      { 'Content-Type': 'application/json' }
    )
    const parsed = JSON.parse(resp)
    const results = Array.isArray(parsed.results) ? parsed.results : []
    return statements.map((_, index) => {
      const result = results[index] ?? {}
      const rows = (result.rows ?? []) as SqliteRow[]
      const columns =
        Array.isArray(result.columns) && result.columns.length > 0
          ? result.columns.map(String)
          : rows.length > 0
            ? Object.keys(rows[0])
            : []
      return { rows, columns, affectedRows: result.affectedRows }
    })
  }

  private async shouldSkipStatement(statement: RewrittenStatement): Promise<boolean> {
    if (
      statement.skipIfColumnExists &&
      (await this.columnExists(
        statement.skipIfColumnExists.table,
        statement.skipIfColumnExists.column
      ))
    ) {
      return true
    }
    if (
      statement.skipIfColumnMissing &&
      !(await this.columnExists(
        statement.skipIfColumnMissing.table,
        statement.skipIfColumnMissing.column
      ))
    ) {
      return true
    }
    if (
      statement.skipIfTableEmpty &&
      !(await this.tableHasRows(statement.skipIfTableEmpty.table))
    ) {
      return true
    }
    return false
  }

  private async executeRewrittenStatement(
    statement: RewrittenStatement
  ): Promise<ExecResult> {
    if (!statement.sql.trim()) return { rows: [], columns: [] }
    if (await this.shouldSkipStatement(statement)) return { rows: [], columns: [] }
    if (statement.isDDL) this.publicationTableInfoCache = null
    await this.snapshotTransactionWrite(statement)
    if (statement.cascadeStatements?.length) {
      await this.runCascadeStatements(statement.cascadeStatements, {})
    }
    const tracking = this.trackingForStatement(statement)
    const exec = await this.materializePublishedSchemaFunctions(
      tracking?.returningSQL ?? statement.sql,
      statement
    )
    return this.doExecResult(
      exec.sql,
      exec.params,
      tracking ? this.trackingRequest(tracking) : undefined
    )
  }

  private async executeRewrittenStatements(
    statements: RewrittenStatement[]
  ): Promise<ExecResult> {
    let result: ExecResult = { rows: [], columns: [] }
    for (const statement of statements) {
      result = await this.executeRewrittenStatement(statement)
    }
    return result
  }

  // run a parent DELETE's cascade children (leaves-first) as their own bound
  // execs BEFORE the parent. each child's returningSQL embeds the full parent
  // WHERE, so the SAME params bind unchanged (sqliteBoundSQL maps $N
  // positionally); the tracking request captures every deletion so it
  // replicates like any other write. publication gating matches the parent:
  // unpublished (e.g. private) child tables cascade in the store but don't
  // stream — correct, since clients don't see those rows anyway.
  private async runCascadeStatements(
    cascades: RewrittenStatement[],
    bind: {
      params?: any[]
      arrayParamNumbers?: Set<number>
      jsonParamNumbers?: Set<number>
      timestampParamNumbers?: Set<number>
      epochMillisParamNumbers?: Set<number>
      booleanParamNumbers?: Set<number>
    }
  ): Promise<void> {
    for (const child of cascades) {
      if (await this.shouldSkipStatement(child)) continue
      await this.snapshotTransactionWrite(child)
      const gated = this.trackingForStatement(child)
      const bound = this.sqliteBoundSQL(
        gated?.returningSQL ?? child.sql,
        bind.params,
        bind.arrayParamNumbers,
        bind.jsonParamNumbers,
        bind.timestampParamNumbers,
        bind.epochMillisParamNumbers,
        bind.booleanParamNumbers
      )
      await this.doExecResult(
        bound.sql,
        bound.params,
        gated ? this.trackingRequest(gated) : undefined
      )
    }
  }

  private async doBatchExec(statements: (RewrittenStatement | string)[]): Promise<void> {
    let sqls: Array<
      string | { sql: string; params?: any[]; track?: ChangeTrackingRequest }
    > = []
    const flush = async () => {
      if (sqls.length === 0) return
      const hasTrackedWrite = sqls.some(
        (statement) => typeof statement !== 'string' && Boolean(statement.track)
      )
      await this.httpClient.post(
        this.url('/batch'),
        JSON.stringify({ statements: sqls }),
        {
          'Content-Type': 'application/json',
        }
      )
      if (hasTrackedWrite) this.signalTrackedWrite()
      sqls = []
    }

    for (const statement of statements) {
      const item =
        typeof statement === 'string'
          ? ({ sql: statement } as RewrittenStatement)
          : statement
      if (!item.sql.trim()) continue
      if (item.isDDL) this.publicationTableInfoCache = null
      if (item.skipIfColumnExists || item.skipIfColumnMissing || item.skipIfTableEmpty) {
        await flush()
        if (
          item.skipIfColumnExists &&
          (await this.columnExists(
            item.skipIfColumnExists.table,
            item.skipIfColumnExists.column
          ))
        ) {
          continue
        }
        if (
          item.skipIfColumnMissing &&
          !(await this.columnExists(
            item.skipIfColumnMissing.table,
            item.skipIfColumnMissing.column
          ))
        ) {
          continue
        }
        if (
          item.skipIfTableEmpty &&
          !(await this.tableHasRows(item.skipIfTableEmpty.table))
        ) {
          continue
        }
      }
      const tracking = this.trackingForStatement(item)
      const exec = await this.materializePublishedSchemaFunctions(
        tracking?.returningSQL ?? item.sql,
        item
      )
      sqls.push(
        tracking
          ? {
              sql: exec.sql,
              ...(exec.params.length ? { params: exec.params } : null),
              track: this.trackingRequest(tracking),
            }
          : exec.params.length
            ? { sql: exec.sql, params: exec.params }
            : exec.sql
      )
    }

    await flush()
  }

  private async columnExists(table: string, column: string): Promise<boolean> {
    const result = await this.doExecResult(`PRAGMA table_info(${quoteIdentifier(table)})`)
    return result.rows.some((row) => row.name === column)
  }

  private async tableHasRows(table: string): Promise<boolean> {
    const result = await this.doExecResult(
      `SELECT 1 AS ok FROM ${quoteIdentifier(table)} LIMIT 1`
    )
    return result.rows.length > 0
  }

  private async listSqliteTables(): Promise<SqliteTableInfo[]> {
    const result = await this.doExecResult(
      "SELECT name, sql FROM sqlite_master WHERE type = 'table' ORDER BY name"
    )
    return result.rows
      .map((row) => ({
        name: String(row.name ?? ''),
        sql: row.sql === null || row.sql === undefined ? null : String(row.sql),
      }))
      .filter((table) => table.name)
  }

  private tableRefForSqliteTable(name: string): PublicationTableRef {
    const metadata = this.schemaMetadata.get(name)?.values().next().value
    if (metadata) {
      return {
        table: name,
        schema: metadata.schema,
        tableName: metadata.tableName,
      }
    }
    const zeroInternalRef = this.zeroInternalTableRefForSqliteTable(name)
    if (zeroInternalRef) return zeroInternalRef
    if (name === 'public_migrations') {
      return {
        table: name,
        schema: 'public',
        tableName: 'migrations',
      }
    }
    return {
      table: name,
      schema: 'public',
      tableName: name,
    }
  }

  private zeroInternalTableRefForSqliteTable(name: string): PublicationTableRef | null {
    for (const publicationName of this.publications.keys()) {
      const match = /^_(.+)_metadata_(\d+)$/.exec(publicationName)
      if (!match) continue
      const appID = match[1]
      const shardNum = match[2]
      const shardSchema = `${appID}_${shardNum}`
      if (name === `${appID}_permissions`) {
        return {
          table: name,
          schema: appID,
          tableName: 'permissions',
        }
      }
      for (const tableName of ['clients', 'mutations', 'replicas', 'shardConfig']) {
        if (name !== `${shardSchema}_${tableName}`) continue
        return {
          table: name,
          schema: shardSchema,
          tableName,
        }
      }
    }
    return null
  }

  private generatedIndexName(
    table: PublicationTableRef,
    columns: string[],
    kind: 'pkey' | 'key'
  ): string {
    return `${table.tableName}_${columns.join('_')}_${kind}`
  }

  private uniqueIndexName(name: string, usedNames: Set<string>): string {
    if (!usedNames.has(name)) {
      usedNames.add(name)
      return name
    }
    let suffix = 2
    while (usedNames.has(`${name}_${suffix}`)) suffix++
    const unique = `${name}_${suffix}`
    usedNames.add(unique)
    return unique
  }

  private sqliteIndexColumnsFromRows(
    xinfoRows: SqliteRow[]
  ): Record<string, 'ASC' | 'DESC'> {
    const rows = xinfoRows
      .map((row) => ({
        seqno: Number(row.seqno ?? 0),
        cid: Number(row.cid ?? -1),
        name: row.name === null || row.name === undefined ? null : String(row.name),
        desc: Number(row.desc ?? 0),
        key: Number(row.key ?? 0),
      }))
      .filter((row): row is SqliteIndexColumnInfo => row.key === 1 && row.name !== null)
      .sort((a, b) => a.seqno - b.seqno)

    const columns: Record<string, 'ASC' | 'DESC'> = {}
    for (const row of rows) columns[row.name] = row.desc === 1 ? 'DESC' : 'ASC'
    return columns
  }

  // a table with no PRIMARY KEY but a full, non-partial UNIQUE index is keyed
  // by that index — the shape soot's DDL generator emits for composite drizzle
  // primaryKey() tables (CREATE TABLE without a PK + CREATE UNIQUE INDEX
  // <table>_pkey), because a PK cannot be retrofitted onto an existing sqlite
  // table. real pg conveys this key via replica identity; without promoting it
  // here, zero's initial sync builds a keyless replica spec and its change
  // processor throws "Cannot replicate table without a PRIMARY KEY or UNIQUE
  // INDEX" on the first UPDATE (2026-07-10 soot prod outage).
  //
  // the <table>_pkey name IS the generator's primary-key convention, so it is
  // trusted outright: legacy tables created before NOT NULL reached their DDL
  // have nullable physical columns (and possibly no durable metadata), yet the
  // index still represents the app-level composite PK — requiring NOT NULL
  // there would silently skip exactly the tables this promotion exists to
  // heal. any OTHER unique index must cover only NOT NULL columns (a unique
  // index over nullable columns permits duplicate NULL rows and is not a row
  // identity); among those, narrowest wins, name as the tiebreak.
  private promotedUniqueIndexKey(
    sqliteTableName: string,
    columns: SqliteColumnInfo[],
    indexListRows: SqliteRow[],
    indexColumnsByName: Map<string, Record<string, 'ASC' | 'DESC'>>
  ): string[] {
    const columnNames = new Set(columns.map((column) => column.name))
    const notNull = new Set(
      columns
        .filter((column) => {
          const metadata = this.schemaMetadata.get(sqliteTableName)?.get(column.name)
          return metadata?.notNull || metadata?.primaryKey || column.notnull || column.pk
        })
        .map((column) => column.name)
    )
    const ref = this.tableRefForSqliteTable(sqliteTableName)
    const pkeyName = `${ref.tableName}_pkey`
    const candidates: Array<{ name: string; columns: string[] }> = []
    for (const row of indexListRows) {
      const name = String(row.name ?? '')
      if (!name || !Number(row.unique ?? 0) || Number(row.partial ?? 0)) continue
      const indexColumns = Object.keys(indexColumnsByName.get(name) ?? {})
      if (indexColumns.length === 0) continue
      if (!indexColumns.every((column) => columnNames.has(column))) continue
      if (name === pkeyName) return indexColumns
      if (!indexColumns.every((column) => notNull.has(column))) continue
      candidates.push({ name, columns: indexColumns })
    }
    if (candidates.length === 0) return []
    candidates.sort(
      (a, b) => a.columns.length - b.columns.length || a.name.localeCompare(b.name)
    )
    return candidates[0].columns
  }

  private tableIndexInfos(
    table: PublicationTableRef,
    columns: SqliteColumnInfo[],
    primaryKey: string[],
    indexListRows: SqliteRow[],
    indexColumnsByName: Map<string, Record<string, 'ASC' | 'DESC'>>
  ): PublicationIndexInfo[] {
    const usedNames = new Set<string>()
    const seenSignatures = new Set<string>()
    const indexes: PublicationIndexInfo[] = []
    const addIndex = (
      name: string,
      indexColumns: Record<string, 'ASC' | 'DESC'>,
      unique: boolean,
      isPrimaryKey: boolean
    ) => {
      if (Object.keys(indexColumns).length === 0) return
      const signature = [
        isPrimaryKey ? 'primary' : 'index',
        unique ? 'unique' : 'plain',
        ...Object.entries(indexColumns).map(
          ([column, direction]) => `${column}:${direction}`
        ),
      ].join('\0')
      if (seenSignatures.has(signature)) return
      seenSignatures.add(signature)
      indexes.push({
        schema: table.schema,
        tableName: table.tableName,
        name: this.uniqueIndexName(name, usedNames),
        unique,
        isPrimaryKey,
        isReplicaIdentity: false,
        isImmediate: true,
        columns: indexColumns,
      })
    }

    if (primaryKey.length > 0) {
      addIndex(
        this.generatedIndexName(table, primaryKey, 'pkey'),
        Object.fromEntries(primaryKey.map((column) => [column, 'ASC'])),
        true,
        true
      )
    }

    for (const column of columns) {
      const metadata = this.schemaMetadata.get(table.table)?.get(column.name)
      if (!metadata?.unique || primaryKey.includes(column.name)) continue
      addIndex(
        this.generatedIndexName(table, [column.name], 'key'),
        { [column.name]: 'ASC' },
        true,
        false
      )
    }

    const rawIndexes = indexListRows
      .map((row) => ({
        seq: Number(row.seq ?? 0),
        name: String(row.name ?? ''),
        unique: Number(row.unique ?? 0),
        origin:
          row.origin === null || row.origin === undefined ? null : String(row.origin),
        partial: Number(row.partial ?? 0),
      }))
      .filter((row): row is SqliteIndexInfo => Boolean(row.name))
      .sort((a, b) => a.seq - b.seq)

    for (const raw of rawIndexes) {
      if (raw.partial) continue
      const indexColumns = indexColumnsByName.get(raw.name) ?? {}
      const names = Object.keys(indexColumns)
      if (raw.origin === 'pk' && primaryKey.length > 0) continue
      const isPrimaryKey = raw.origin === 'pk'
      const name =
        raw.name.startsWith('sqlite_autoindex_') || isPrimaryKey
          ? this.generatedIndexName(table, names, isPrimaryKey ? 'pkey' : 'key')
          : raw.name
      addIndex(name, indexColumns, Boolean(raw.unique || isPrimaryKey), isPrimaryKey)
    }

    return indexes
  }

  private publicationContainsTable(
    publication: PublicationDefinition,
    table: PublicationTableInfo | PublicationTableRef
  ): boolean {
    const tableKey = 'table' in table ? table.table : table.name
    return (
      publication.allTables ||
      publication.schemas.has(table.schema) ||
      publication.tables.has(tableKey)
    )
  }

  private publicationsForTable(
    table: PublicationTableInfo | PublicationTableRef,
    requested: string[]
  ): string[] {
    return requested.filter((publicationName) => {
      const publication = this.publications.get(publicationName)
      return publication ? this.publicationContainsTable(publication, table) : false
    })
  }

  private async publicationTableInfos(
    publications?: string[]
  ): Promise<PublicationTableInfo[]> {
    // Schema migration can run through a second DoBackend instance after this
    // one has already cached an empty physical catalog. Do not let that
    // pre-provisioning observation make information_schema.columns report an
    // empty database forever; empty catalogs are cheap to re-check and become
    // stable as soon as the first table appears.
    if (!this.publicationTableInfoCache?.length) {
      this.publicationTableInfoCache = await this.loadPublicationTableInfos()
    }
    const requested = publications?.filter((name) => this.publications.has(name)) ?? []
    return this.publicationTableInfoCache.filter((info) => {
      if (requested.length > 0) {
        return requested.some((publicationName) =>
          this.publicationContainsTable(this.publications.get(publicationName)!, info)
        )
      }
      return !publications?.length
    })
  }

  private async loadPublicationTableInfos(): Promise<PublicationTableInfo[]> {
    const allTables = (await this.listSqliteTables()).filter(
      (table) => !isSystemSqliteTable(table.name)
    )
    // batch table_info + index_list for every table into ONE round-trip, then
    // every index_xinfo into a second — per-PRAGMA round-trips to the SQL DO
    // made each full scan cost seconds and starve concurrent sessions.
    const pragmaResults = await this.doBatchResults(
      allTables.flatMap((table) => [
        `PRAGMA table_info(${quoteIdentifier(table.name)})`,
        `PRAGMA index_list(${quoteIdentifier(table.name)})`,
      ])
    )
    const indexNames: string[] = []
    for (let i = 0; i < allTables.length; i++) {
      for (const row of pragmaResults[i * 2 + 1]?.rows ?? []) {
        const name = String(row.name ?? '')
        if (name && !Number(row.partial ?? 0)) indexNames.push(name)
      }
    }
    const xinfoResults = await this.doBatchResults(
      indexNames.map((name) => `PRAGMA index_xinfo(${quoteIdentifier(name)})`)
    )
    const indexColumnsByName = new Map<string, Record<string, 'ASC' | 'DESC'>>()
    for (let i = 0; i < indexNames.length; i++) {
      indexColumnsByName.set(
        indexNames[i],
        this.sqliteIndexColumnsFromRows(xinfoResults[i]?.rows ?? [])
      )
    }

    const infos: PublicationTableInfo[] = []
    for (let i = 0; i < allTables.length; i++) {
      const table = allTables[i]
      const ref = this.tableRefForSqliteTable(table.name)
      const columns = (pragmaResults[i * 2]?.rows ?? []).map((row) => ({
        cid: Number(row.cid ?? 0),
        name: String(row.name ?? ''),
        type: String(row.type ?? ''),
        notnull: Number(row.notnull ?? 0),
        dflt_value:
          row.dflt_value === null || row.dflt_value === undefined
            ? null
            : String(row.dflt_value),
        pk: Number(row.pk ?? 0),
      }))
      if (columns.length === 0) continue
      const metadataPrimaryKey = columns
        .filter(
          (column) => this.schemaMetadata.get(table.name)?.get(column.name)?.primaryKey
        )
        .map((column) => column.name)
      const sqlitePrimaryKey = columns
        .filter((column) => column.pk > 0)
        .sort((a, b) => a.pk - b.pk)
        .map((column) => column.name)
      const primaryKey =
        metadataPrimaryKey.length > 0
          ? metadataPrimaryKey
          : sqlitePrimaryKey.length > 0
            ? sqlitePrimaryKey
            : this.promotedUniqueIndexKey(
                table.name,
                columns,
                pragmaResults[i * 2 + 1]?.rows ?? [],
                indexColumnsByName
              )
      const info = {
        name: table.name,
        schema: ref.schema,
        tableName: ref.tableName,
        columns,
        primaryKey,
        indexes: [],
      }
      info.indexes = this.tableIndexInfos(
        ref,
        columns,
        info.primaryKey,
        pragmaResults[i * 2 + 1]?.rows ?? [],
        indexColumnsByName
      )
      infos.push(info)
    }
    return infos
  }

  private projectCatalogRow(
    fields: CatalogTargetField[],
    values: Record<string, unknown>
  ): Record<string, unknown> {
    return Object.fromEntries(
      fields.map((field) => [
        field.name,
        Object.hasOwn(field, 'value')
          ? field.value
          : (values[field.source ?? field.name] ?? values[field.name] ?? null),
      ])
    )
  }

  private async pgTablesResult(select: any): Promise<CatalogResult | null> {
    if (!selectReferencesTable(select, 'pg_tables')) return null
    const fields = selectTargetFields(select)
    const infos = await this.publicationTableInfos()
    return {
      rows: infos
        .map((info) => ({
          schemaname: info.schema,
          tablename: info.tableName,
          tableowner: 'user',
        }))
        .filter((row) => catalogWhereMatches(select.whereClause, row))
        .map((row) => this.projectCatalogRow(fields, row)),
      fields: fields.map((field) => ({ name: field.name, oid: field.oid })),
    }
  }

  private async informationSchemaColumnsResult(
    select: any
  ): Promise<CatalogResult | null> {
    if (!selectReferencesTable(select, 'columns')) return null
    const fields = selectTargetFields(select)
    const zeroServerSchemaQuery = ['schema', 'table', 'column', 'dataType'].every(
      (name) => fields.some((field) => field.name === name)
    )
    let rows: Record<string, unknown>[] = []
    let matchedColumns = new Set<string>()
    if (!zeroServerSchemaQuery) {
      for (let attempt = 0; attempt < 2; attempt++) {
        const infos = await this.publicationTableInfos()
        rows = []
        matchedColumns = new Set()
        for (const info of infos) {
          for (const column of info.columns) {
            const metadata = this.schemaMetadata.get(info.name)?.get(column.name)
            const dataType = pgTypeForSqliteColumn(column, metadata)
            const baseDataType = dataType.endsWith('[]')
              ? dataType.slice(0, -2)
              : dataType
            const row = {
              table_schema: info.schema,
              table_name: info.tableName,
              column_name: column.name,
              data_type: metadata?.elemTypname ? 'ARRAY' : baseDataType,
              udt_name:
                metadata?.typname ??
                (dataType.endsWith('[]') ? `_${baseDataType}` : baseDataType),
              character_maximum_length: metadata?.characterMaximumLength ?? null,
              numeric_precision: metadata?.numericPrecision ?? null,
              numeric_scale: metadata?.numericScale ?? null,
              typtype: metadata?.typtype ?? 'b',
              typname:
                metadata?.typname ??
                (dataType.endsWith('[]') ? `_${baseDataType}` : baseDataType),
              elemTyptype: metadata?.elemTyptype ?? null,
              elemTypname: metadata?.elemTypname ?? null,
              schema: info.schema,
              table: info.tableName,
              column: column.name,
              dataType: metadata?.elemTypname ? 'ARRAY' : baseDataType,
              length: metadata?.characterMaximumLength ?? null,
              precision: metadata?.numericPrecision ?? null,
              scale: metadata?.numericScale ?? null,
              typename:
                metadata?.typname ??
                (dataType.endsWith('[]') ? `_${baseDataType}` : baseDataType),
            }
            if (catalogWhereMatches(select.whereClause, row)) {
              rows.push(this.projectCatalogRow(fields, row))
              matchedColumns.add(
                `${info.schema}\u0000${info.tableName}\u0000${column.name}`
              )
            }
          }
        }
        if (rows.length > 0 || attempt === 1) break
        // A different backend can provision the app tables after this instance
        // cached only Zero's internal shard tables. One forced refresh on an
        // otherwise empty information-schema answer heals that non-empty stale
        // cache without turning every healthy catalog query into a full scan.
        this.publicationTableInfoCache = null
      }
    }
    // A large out-of-band migration can leave the PRAGMA-derived catalog
    // partially populated, which is more dangerous than an empty cache because
    // a zero-row retry never fires. Durable schema metadata is written in the
    // same SQL DO as the physical tables, so make it authoritative for column
    // introspection while still requiring each physical table to exist. This
    // keeps schema validation honest if stale metadata survives a real DROP.
    await this.reloadSchemaMetadataIfEmpty()
    const physicalTables = new Set(
      (await this.listSqliteTables()).map((table) => table.name)
    )
    for (const [tableName, columns] of this.schemaMetadata) {
      if (!physicalTables.has(tableName)) continue
      for (const [columnName, metadata] of columns) {
        const dataType = metadata.elemTypname ? 'ARRAY' : metadata.dataType
        const row = {
          table_schema: metadata.schema,
          table_name: metadata.tableName,
          column_name: columnName,
          data_type: dataType,
          udt_name: metadata.typname,
          character_maximum_length: metadata.characterMaximumLength ?? null,
          numeric_precision: metadata.numericPrecision ?? null,
          numeric_scale: metadata.numericScale ?? null,
          typtype: metadata.typtype,
          typname: metadata.typname,
          elemTyptype: metadata.elemTyptype ?? null,
          elemTypname: metadata.elemTypname ?? null,
          schema: metadata.schema,
          table: metadata.tableName,
          column: columnName,
          dataType,
          length: metadata.characterMaximumLength ?? null,
          precision: metadata.numericPrecision ?? null,
          scale: metadata.numericScale ?? null,
          typename: metadata.typname,
        }
        if (zeroServerSchemaQuery || catalogWhereMatches(select.whereClause, row)) {
          const key = `${metadata.schema}\u0000${metadata.tableName}\u0000${columnName}`
          if (matchedColumns.has(key)) continue
          rows.push(this.projectCatalogRow(fields, row))
          matchedColumns.add(key)
        }
      }
    }
    return {
      rows,
      fields: fields.map((field) => ({ name: field.name, oid: field.oid })),
    }
  }

  private async informationSchemaKeyColumnsResult(
    select: any
  ): Promise<CatalogResult | null> {
    const hasKeyCatalog =
      selectReferencesTable(select, 'table_constraints') ||
      selectReferencesTable(select, 'key_column_usage')
    if (!hasKeyCatalog) return null

    const selectedFields = selectTargetFields(select)
    const fields =
      selectedFields.length > 0
        ? selectedFields
        : [
            { name: 'kind', source: 'kind' },
            { name: 'table_schema', source: 'table_schema' },
            { name: 'table_name', source: 'table_name' },
            { name: 'column_name', source: 'column_name' },
            { name: 'data_type', source: 'data_type' },
            {
              name: 'ordinal_position',
              source: 'ordinal_position',
              oid: PG_TYPE_INT4,
            },
          ]
    const includeColumns = selectReferencesTable(select, 'columns')
    const infos = await this.publicationTableInfos()
    const rows: Record<string, unknown>[] = []

    for (const info of infos) {
      for (const columnName of info.primaryKey) {
        const column = info.columns.find((candidate) => candidate.name === columnName)
        const ordinal = column ? column.cid + 1 : info.primaryKey.indexOf(columnName) + 1
        rows.push(
          this.projectCatalogRow(fields, {
            kind: 'pk',
            table_schema: info.schema,
            table_name: info.tableName,
            column_name: columnName,
            data_type: null,
            ordinal_position: ordinal,
            constraint_name: `${info.tableName}_pkey`,
            constraint_type: 'PRIMARY KEY',
          })
        )
      }

      if (!includeColumns) continue
      for (const column of info.columns) {
        const metadata = this.schemaMetadata.get(info.name)?.get(column.name)
        const dataType = pgTypeForSqliteColumn(column, metadata)
        const baseDataType = dataType.endsWith('[]') ? dataType.slice(0, -2) : dataType
        rows.push(
          this.projectCatalogRow(fields, {
            kind: 'col',
            table_schema: info.schema,
            table_name: info.tableName,
            column_name: column.name,
            data_type: metadata?.elemTypname ? 'ARRAY' : baseDataType,
            ordinal_position: column.cid + 1,
            constraint_name: null,
            constraint_type: null,
          })
        )
      }
    }

    return {
      rows,
      fields: fields.map((field) => ({ name: field.name, oid: field.oid })),
    }
  }

  private async pgPublicationTablesResult(select: any): Promise<CatalogResult | null> {
    if (!selectReferencesTable(select, 'pg_publication_tables')) return null
    const fields = selectTargetFields(select)
    const requested = stringFilterValues(select, 'pubname')
    const publications = requested.length ? requested : [...this.publications.keys()]
    const infos = await this.publicationTableInfos(publications)
    const aggregatePublications = fields.some(
      (field) =>
        field.name === 'publications' ||
        field.source === 'json_object_agg' ||
        field.source === 'json_group_object'
    )

    if (aggregatePublications) {
      return {
        rows: infos.map((info) => {
          const tablePublications = this.publicationsForTable(info, publications)
          const publicationColumns = Object.fromEntries(
            tablePublications.map((publication) => [
              publication,
              info.columns.map((column) => column.name),
            ])
          )
          return this.projectCatalogRow(fields, {
            schemaname: info.schema,
            schema: info.schema,
            tablename: info.tableName,
            table: info.tableName,
            publications: publicationColumns,
            json_object_agg: publicationColumns,
            json_group_object: publicationColumns,
          })
        }),
        fields: fields.map((field) => ({ name: field.name, oid: field.oid })),
      }
    }

    const rows: Record<string, unknown>[] = []
    for (const publication of publications) {
      for (const info of infos) {
        if (!this.publicationsForTable(info, [publication]).length) continue
        rows.push(
          this.projectCatalogRow(fields, {
            pubname: publication,
            schemaname: info.schema,
            tablename: info.tableName,
            attnames: info.columns.map((column) => column.name),
            rowfilter: null,
          })
        )
      }
    }
    return {
      rows,
      fields: fields.map((field) => ({ name: field.name, oid: field.oid })),
    }
  }

  private publicationTableSpec(
    info: PublicationTableInfo,
    publications: string[]
  ): Record<string, unknown> {
    return {
      oid: tableOid(info.name),
      schema: info.schema,
      schemaOID: 2200,
      name: info.tableName,
      replicaIdentity: 'd',
      columns: Object.fromEntries(
        info.columns.map((column) => {
          const metadata = this.schemaMetadata.get(info.name)?.get(column.name)
          const dataType = pgTypeForSqliteColumn(column, metadata)
          return [
            column.name,
            {
              pos: column.cid + 1,
              dataType,
              pgTypeClass: metadata?.typtype ?? 'b',
              elemPgTypeClass: metadata?.elemTyptype ?? null,
              typeOID: metadata?.typeOid ?? pgTypeOid(dataType),
              characterMaximumLength: metadata?.characterMaximumLength ?? null,
              // primary-key columns are never-null by definition; legacy
              // tables whose key was promoted from a <table>_pkey unique
              // index carry nullable physical columns, and zero's
              // view-syncer refuses to sync a table whose key columns are
              // nullable on the replica (checkClientSchema).
              notNull: Boolean(
                metadata?.notNull ||
                metadata?.primaryKey ||
                column.notnull ||
                column.pk ||
                info.primaryKey.includes(column.name)
              ),
              dflt: null,
            },
          ]
        })
      ),
      primaryKey: info.primaryKey,
      publications: Object.fromEntries(
        this.publicationsForTable(info, publications).map((publication) => [
          publication,
          { rowFilter: null },
        ])
      ),
    }
  }

  private async publishedTablesResult(select: any): Promise<CatalogResult | null> {
    const fields = selectTargetFields(select)
    if (!fields.some((field) => field.name === 'tables')) return null
    if (!selectReferencesTable(select, 'pg_attribute')) return null
    const requested = stringFilterValues(select, 'pubname')
    const publications = requested.length ? requested : [...this.publications.keys()]
    const infos = await this.publicationTableInfos(publications)
    return {
      rows: [
        {
          tables: infos.map((info) => this.publicationTableSpec(info, publications)),
        },
      ],
      fields: [{ name: 'tables', oid: PG_TYPE_JSON }],
    }
  }

  private async publishedIndexesResult(select: any): Promise<CatalogResult | null> {
    const fields = selectTargetFields(select)
    if (!fields.some((field) => field.name === 'indexes')) return null
    if (!selectReferencesTable(select, 'pg_index')) return null
    const requested = stringFilterValues(select, 'pubname')
    const publications = requested.length ? requested : [...this.publications.keys()]
    const infos = await this.publicationTableInfos(publications)
    return {
      rows: [{ indexes: infos.flatMap((info) => info.indexes) }],
      fields: [{ name: 'indexes', oid: PG_TYPE_JSON }],
    }
  }

  private async publishedSchemaResult(select: any): Promise<CatalogResult | null> {
    const fields = selectTargetFields(select)
    if (!fields.some((field) => field.name === 'publishedSchema')) return null
    if (!selectReferencesTable(select, 'pg_publication_tables')) return null
    const requested = stringFilterValues(select, 'pubname')
    const publications = requested.length ? requested : [...this.publications.keys()]
    const infos = await this.publicationTableInfos(publications)
    return {
      rows: [
        {
          publishedSchema: {
            tables: infos.map((info) => this.publicationTableSpec(info, publications)),
            indexes: infos.flatMap((info) => info.indexes),
          },
        },
      ],
      fields: [{ name: 'publishedSchema', oid: PG_TYPE_JSON }],
    }
  }

  private pgTypeArrayResult(select: any): CatalogResult | null {
    if (!selectReferencesTable(select, 'pg_type')) return null
    const fields = selectTargetFields(select)
    if (
      !fields.some((field) => field.name === 'oid') ||
      !fields.some((field) => field.name === 'typarray')
    ) {
      return null
    }
    return {
      rows: ARRAY_TYPE_ROWS.map((row) => this.projectCatalogRow(fields, row)),
      fields: fields.map((field) => ({ name: field.name, oid: PG_TYPE_INT4 })),
    }
  }

  private sqlLiteral(
    val: unknown,
    options?: {
      pgArrayAsJson?: boolean
      pgJsonAsJson?: boolean
      pgTimestampAsText?: boolean
      pgEpochMillisAsNumber?: boolean
      pgBooleanAsInteger?: boolean
    }
  ): string {
    if (val === null || val === undefined) return 'NULL'
    if (options?.pgEpochMillisAsNumber) {
      const millis = epochMillisParamValue(val)
      if (typeof millis === 'number') return String(millis)
      val = millis
    }
    if (options?.pgTimestampAsText)
      return `'${postgresTimestampText(val).replace(/'/g, "''")}'`
    if (options?.pgJsonAsJson) {
      const json = sqliteJsonParamValue(val)
      if (typeof json === 'string') return `'${json.replace(/'/g, "''")}'`
      return String(json)
    }
    if (options?.pgBooleanAsInteger) {
      if (typeof val === 'boolean') return val ? '1' : '0'
      if (typeof val === 'string') {
        const lower = val.toLowerCase()
        if (lower === 'true' || lower === 't') return '1'
        if (lower === 'false' || lower === 'f') return '0'
      }
    }
    if (options?.pgArrayAsJson) {
      const json = Array.isArray(val)
        ? JSON.stringify(val)
        : typeof val === 'string'
          ? (pgArrayLiteralToJson(val) ?? val)
          : val && typeof val === 'object'
            ? JSON.stringify(val)
            : String(val)
      return `'${json.replace(/'/g, "''")}'`
    }
    if (val && typeof val === 'object')
      return `'${JSON.stringify(val).replace(/'/g, "''")}'`
    if (typeof val === 'string') return `'${val.replace(/'/g, "''")}'`
    return String(val)
  }

  private inlineParams(
    sql: string,
    params: any[],
    arrayParamNumbers = new Set<number>(),
    jsonParamNumbers = new Set<number>(),
    timestampParamNumbers = new Set<number>(),
    epochMillisParamNumbers = new Set<number>(),
    booleanParamNumbers = new Set<number>()
  ): string {
    let out = ''
    for (let i = 0; i < sql.length; ) {
      const ch = sql[i]
      const next = sql[i + 1]

      if (ch === "'") {
        const start = i
        i++
        while (i < sql.length) {
          if (sql[i] === "'" && sql[i + 1] === "'") {
            i += 2
            continue
          }
          if (sql[i++] === "'") break
        }
        out += sql.slice(start, i)
        continue
      }

      if (ch === '"') {
        const start = i
        i++
        while (i < sql.length) {
          if (sql[i] === '"' && sql[i + 1] === '"') {
            i += 2
            continue
          }
          if (sql[i++] === '"') break
        }
        out += sql.slice(start, i)
        continue
      }

      if (ch === '-' && next === '-') {
        const start = i
        i += 2
        while (i < sql.length && sql[i] !== '\n') i++
        out += sql.slice(start, i)
        continue
      }

      if (ch === '/' && next === '*') {
        const start = i
        i += 2
        while (i < sql.length && !(sql[i] === '*' && sql[i + 1] === '/')) i++
        i = Math.min(sql.length, i + 2)
        out += sql.slice(start, i)
        continue
      }

      if (ch === '$' && next && isDollarQuoteTagStart(next)) {
        const start = i
        i += 1
        while (i < sql.length && isDollarQuoteTagPart(sql[i])) i++
        if (sql[i] === '$') {
          const tag = sql.slice(start, i + 1)
          const end = sql.indexOf(tag, i + 1)
          if (end >= 0) {
            out += sql.slice(start, end + tag.length)
            i = end + tag.length
            continue
          }
        }
        out += sql.slice(start, i)
        continue
      }

      if (ch === '$' && next && next >= '1' && next <= '9') {
        let end = i + 2
        while (end < sql.length && sql[end] >= '0' && sql[end] <= '9') end++
        const number = Number(sql.slice(i + 1, end))
        const index = number - 1
        out +=
          index >= 0 && index < params.length
            ? this.sqlLiteral(params[index], {
                pgArrayAsJson: arrayParamNumbers.has(number),
                pgJsonAsJson: jsonParamNumbers.has(number),
                pgTimestampAsText: timestampParamNumbers.has(number),
                pgEpochMillisAsNumber: epochMillisParamNumbers.has(number),
                pgBooleanAsInteger: booleanParamNumbers.has(number),
              })
            : sql.slice(i, end)
        i = end
        continue
      }

      out += ch
      i++
    }
    return out
  }

  private sqliteParamValue(
    value: unknown,
    options?: {
      pgArrayAsJson?: boolean
      pgJsonAsJson?: boolean
      pgTimestampAsText?: boolean
      pgEpochMillisAsNumber?: boolean
      pgBooleanAsInteger?: boolean
    }
  ): unknown {
    if (value === null || value === undefined) return null
    if (options?.pgEpochMillisAsNumber) return epochMillisParamValue(value)
    if (options?.pgTimestampAsText) return postgresTimestampText(value)
    if (options?.pgJsonAsJson) return sqliteJsonParamValue(value)
    if (typeof value === 'boolean') return value ? 1 : 0
    if (options?.pgBooleanAsInteger && typeof value === 'string') {
      const lower = value.toLowerCase()
      if (lower === 'true' || lower === 't') return 1
      if (lower === 'false' || lower === 'f') return 0
    }
    if (!options?.pgArrayAsJson) {
      if (value && typeof value === 'object') return JSON.stringify(value)
      return value
    }
    if (Array.isArray(value)) return JSON.stringify(value)
    if (typeof value === 'string') return pgArrayLiteralToJson(value) ?? value
    if (value && typeof value === 'object') return JSON.stringify(value)
    return value
  }

  private sqliteBoundSQL(
    sql: string,
    params: any[] | undefined,
    arrayParamNumbers = new Set<number>(),
    jsonParamNumbers = new Set<number>(),
    timestampParamNumbers = new Set<number>(),
    epochMillisParamNumbers = new Set<number>(),
    booleanParamNumbers = new Set<number>()
  ): { sql: string; params: any[] } {
    if (!params?.length) return { sql, params: [] }
    let out = ''
    const bound: any[] = []
    for (let i = 0; i < sql.length; ) {
      const ch = sql[i]
      const next = sql[i + 1]

      if (ch === "'") {
        const start = i
        i++
        while (i < sql.length) {
          if (sql[i] === "'" && sql[i + 1] === "'") {
            i += 2
            continue
          }
          if (sql[i++] === "'") break
        }
        out += sql.slice(start, i)
        continue
      }

      if (ch === '"') {
        const start = i
        i++
        while (i < sql.length) {
          if (sql[i] === '"' && sql[i + 1] === '"') {
            i += 2
            continue
          }
          if (sql[i++] === '"') break
        }
        out += sql.slice(start, i)
        continue
      }

      if (ch === '-' && next === '-') {
        const start = i
        i += 2
        while (i < sql.length && sql[i] !== '\n') i++
        out += sql.slice(start, i)
        continue
      }

      if (ch === '/' && next === '*') {
        const start = i
        i += 2
        while (i < sql.length && !(sql[i] === '*' && sql[i + 1] === '/')) i++
        i = Math.min(sql.length, i + 2)
        out += sql.slice(start, i)
        continue
      }

      if (ch === '$' && next && isDollarQuoteTagStart(next)) {
        const start = i
        i += 1
        while (i < sql.length && isDollarQuoteTagPart(sql[i])) i++
        if (sql[i] === '$') {
          const tag = sql.slice(start, i + 1)
          const end = sql.indexOf(tag, i + 1)
          if (end >= 0) {
            out += sql.slice(start, end + tag.length)
            i = end + tag.length
            continue
          }
        }
        out += sql.slice(start, i)
        continue
      }

      if (ch === '$' && next && next >= '1' && next <= '9') {
        let end = i + 2
        while (end < sql.length && sql[end] >= '0' && sql[end] <= '9') end++
        const number = Number(sql.slice(i + 1, end))
        const index = number - 1
        if (index >= 0 && index < params.length) {
          out += '?'
          bound.push(
            this.sqliteParamValue(params[index], {
              pgArrayAsJson: arrayParamNumbers.has(number),
              pgJsonAsJson: jsonParamNumbers.has(number),
              pgTimestampAsText: timestampParamNumbers.has(number),
              pgEpochMillisAsNumber: epochMillisParamNumbers.has(number),
              pgBooleanAsInteger: booleanParamNumbers.has(number),
            })
          )
        } else {
          out += sql.slice(i, end)
        }
        i = end
        continue
      }

      out += ch
      i++
    }
    return { sql: out, params: bound }
  }

  private buildSQLResponse(
    originalSql: string,
    rows: Record<string, unknown>[],
    columns: string[] = [],
    options: { affectedRows?: number } = {}
  ): Uint8Array {
    const isSelect = /^\s*select\b/i.test(originalSql) || /^\s*with\b/i.test(originalSql)
    const fields = this.fieldsForResult(originalSql, { rows, columns })
    const affectedRows = options.affectedRows ?? rows.length
    if (rows.length > 0) {
      const tag = isSelect
        ? `SELECT ${rows.length}`
        : /^\s*insert\b/i.test(originalSql)
          ? `INSERT 0 ${affectedRows}`
          : /^\s*update\b/i.test(originalSql)
            ? `UPDATE ${affectedRows}`
            : /^\s*delete\b/i.test(originalSql)
              ? `DELETE ${affectedRows}`
              : 'OK'
      return concat(
        buildRowDescription(fields),
        ...rows.map((r) => buildDataRow(r, fields)),
        buildCommandComplete(tag),
        this.readyForQuery()
      )
    }
    const tag = isSelect
      ? 'SELECT 0'
      : /^\s*insert\b/i.test(originalSql)
        ? `INSERT 0 ${affectedRows}`
        : /^\s*update\b/i.test(originalSql)
          ? `UPDATE ${affectedRows}`
          : /^\s*delete\b/i.test(originalSql)
            ? `DELETE ${affectedRows}`
            : 'OK'
    return concat(
      isSelect && fields.length > 0 ? buildRowDescription(fields) : new Uint8Array(0),
      buildCommandComplete(tag),
      this.readyForQuery()
    )
  }

  private copyTextValue(
    column: string,
    val: unknown,
    field?: { name: string; oid?: number }
  ): string {
    if (val === null || val === undefined) return '\\N'
    if (field?.oid === PG_TYPE_BOOL) {
      if (val === true || val === 1 || val === '1' || val === 't' || val === 'true')
        return 't'
      if (val === false || val === 0 || val === '0' || val === 'f' || val === 'false')
        return 'f'
    }
    const formatted = isTimestampOid(field?.oid) ? postgresTimestampText(val) : val
    const str =
      typeof formatted === 'object' ? JSON.stringify(formatted) : String(formatted)
    let out = ''
    for (const ch of str) {
      switch (ch) {
        case '\\':
          out += '\\\\'
          break
        case '\t':
          out += '\\t'
          break
        case '\n':
          out += '\\n'
          break
        case '\r':
          out += '\\r'
          break
        default:
          out += ch
      }
    }
    return out
  }

  private copyBinaryValue(
    column: string,
    value: unknown,
    field?: { name: string; oid?: number }
  ): Uint8Array | null {
    if (value === null || value === undefined) return null
    const oid = field?.oid
    if (oid === PG_TYPE_BOOL)
      return new Uint8Array([value === true || value === 1 ? 1 : 0])
    if (oid === PG_TYPE_INT2) return i16(Number(value))
    if (oid === PG_TYPE_INT4) return i32(Number(value))
    if (oid === PG_TYPE_INT8) {
      try {
        return i64(typeof value === 'bigint' ? value : BigInt(value as any))
      } catch {
        // value is not a valid integer for this int8 column — e.g. a timestamp
        // string written into a bigint column, which sqlite's dynamic typing
        // accepts but real pg would reject. encode a best-effort int rather than
        // throw: a thrown error mid binary-COPY emits an ErrorResponse into the
        // copy-out stream, which the consumer's COPY parser cannot recover from
        // and hangs on — wedging zero-cache's entire initial sync (the embed
        // never reaches ready, every /sync gets 0 frames). 0 keeps the stream
        // well-formed so the rest of the snapshot completes.
        const n = Number(value)
        return i64(Number.isFinite(n) ? BigInt(Math.trunc(n)) : 0n)
      }
    }
    if (oid === PG_TYPE_FLOAT8) {
      const buf = new ArrayBuffer(8)
      new DataView(buf).setFloat64(0, Number(value))
      return new Uint8Array(buf)
    }
    if (oid === PG_TYPE_TIMESTAMP || oid === PG_TYPE_TIMESTAMPTZ) {
      const millis = timestampMillisValue(value)
      const finite = typeof millis === 'number' && Number.isFinite(millis) ? millis : 0
      return i64(BigInt(Math.round((finite - 946684800000) * 1000)))
    }
    if (oid === PG_TYPE_JSONB) {
      const json =
        typeof value === 'string' ? value : JSON.stringify(sqliteJsonParamValue(value))
      return concat(new Uint8Array([1]), textEncoder.encode(json))
    }
    if (oid === PG_TYPE_BYTEA && value instanceof Uint8Array) return value
    if (oid === PG_TYPE_JSON) {
      const json =
        typeof value === 'string' ? value : JSON.stringify(sqliteJsonParamValue(value))
      return textEncoder.encode(json)
    }
    // unknown type in a BINARY copy means schemaMetadata never learned this
    // column (real pg always knows its types). the text bytes emitted here
    // will crash any consumer decoding by declared type (e.g. int4
    // readInt32BE on "847"), so make the downgrade loud.
    if (oid === undefined && !this.warnedBinaryCopyTextFallback.has(column)) {
      this.warnedBinaryCopyTextFallback.add(column)
      console.warn(
        `[orez] binary COPY falling back to text encoding for column "${column}" — no pg type metadata; apply schema via deployTimeSchemaBatchStatements or through the backend`
      )
    }
    return textEncoder.encode(this.copyTextValue(column, value, field))
  }

  private warnedBinaryCopyTextFallback = new Set<string>()

  private copyBinaryRow(
    columns: string[],
    row: SqliteRow,
    fields: { name: string; oid?: number }[]
  ): Uint8Array {
    const parts: Uint8Array[] = [i16(columns.length)]
    for (const [index, column] of columns.entries()) {
      const value = this.copyBinaryValue(column, row[column], fields[index])
      if (value === null) {
        parts.push(i32(-1))
      } else {
        parts.push(i32(value.length), value)
      }
    }
    return concat(...parts)
  }

  private buildBinaryCopyResponse(
    result: ExecResult,
    columns: string[],
    fields: { name: string; oid?: number }[]
  ): Uint8Array {
    const header = concat(
      new Uint8Array([80, 71, 67, 79, 80, 89, 10, 255, 13, 10, 0]),
      i32(0),
      i32(0)
    )
    return concat(
      buildCopyOutResponse(columns.length, true),
      buildCopyDataBytes(header),
      ...result.rows.map((row) =>
        buildCopyDataBytes(this.copyBinaryRow(columns, row, fields))
      ),
      buildCopyDataBytes(i16(-1)),
      buildCopyDone(),
      buildCommandComplete(`COPY ${result.rows.length}`),
      this.readyForQuery()
    )
  }

  private buildCopyResponse(result: ExecResult, sql = '', binary = false): Uint8Array {
    const columns =
      result.columns.length > 0
        ? result.columns
        : result.rows.length > 0
          ? Object.keys(result.rows[0])
          : []
    const fields = columns.map((name) => ({
      name,
      oid: sql ? this.fieldMetadataForResultColumn(sql, name)?.oid : undefined,
    }))
    if (binary) return this.buildBinaryCopyResponse(result, columns, fields)
    return concat(
      buildCopyOutResponse(columns.length),
      ...result.rows.map((row) =>
        buildCopyData(
          `${columns.map((column, index) => this.copyTextValue(column, row[column], fields[index])).join('\t')}\n`
        )
      ),
      buildCopyDone(),
      buildCommandComplete(`COPY ${result.rows.length}`),
      this.readyForQuery()
    )
  }

  private fieldMetadataForResultColumn(
    sql: string,
    column: string
  ): SchemaColumnMetadata | undefined {
    const table = firstSourceTableFromSQL(sql)
    const sourceTables = sourceTablesFromSQL(sql)
    const resultColumn = selectResultColumnMetadata(sql).get(column)
    if (resultColumn?.oid) {
      return {
        table: '',
        schema: 'public',
        tableName: '',
        column,
        oid: resultColumn.oid,
      }
    }
    const source = resultColumn?.source ?? column
    if (sourceTables.length > 0) {
      let foundFromSource: SchemaColumnMetadata | undefined
      for (const sourceTable of sourceTables) {
        const metadata = this.schemaMetadata.get(sourceTable)?.get(source)
        if (!metadata) continue
        if (foundFromSource) return undefined
        foundFromSource = metadata
      }
      if (foundFromSource) return foundFromSource
    }
    if (table) {
      const metadata = this.schemaMetadata.get(table)?.get(source)
      if (metadata) return metadata
    }

    let found: SchemaColumnMetadata | undefined
    for (const columns of this.schemaMetadata.values()) {
      const metadata = columns.get(source)
      if (!metadata) continue
      if (found) return undefined
      found = metadata
    }
    if (found) return found

    const fallback = fallbackMetadataForColumnName(source)
    if (!fallback) return undefined
    return {
      table: '',
      schema: 'public',
      tableName: '',
      column,
      ...fallback,
    }
  }

  private fieldsForResult(
    sql: string,
    result: ExecResult
  ): { name: string; oid?: number }[] {
    if (result.columns.length > 0)
      return result.columns.map((name) => ({
        name,
        oid: this.fieldMetadataForResultColumn(sql, name)?.oid,
      }))
    if (result.rows.length > 0)
      return Object.keys(result.rows[0]).map((name) => ({
        name,
        oid: this.fieldMetadataForResultColumn(sql, name)?.oid,
      }))
    return inferFieldsFromSQL(sql)
  }

  private metadataFieldsForSQL(sql: string): { name: string; oid?: number }[] {
    const inferred = inferFieldsFromSQL(sql).map((field) => ({
      name: field.name,
      oid: field.oid ?? this.fieldMetadataForResultColumn(sql, field.name)?.oid,
    }))
    if (inferred.length > 0) return inferred

    const selectList = extractTopLevelSelectList(sql)
    if (!selectList) return []
    const hasStar = splitTopLevelComma(selectList).some((part) => {
      const trimmed = part.trim()
      return trimmed === '*' || /(?:^|[.\s])\*$/.test(trimmed)
    })
    if (!hasStar) return []

    const fields: { name: string; oid?: number }[] = []
    for (const table of sourceTablesFromSQL(sql)) {
      const columns = this.schemaMetadata.get(table)
      if (!columns) continue
      for (const column of columns.values()) {
        fields.push({ name: column.column, oid: column.oid })
      }
    }
    return fields
  }

  private async describeFields(
    sql: string,
    metadataSql = sql
  ): Promise<{ name: string; oid?: number }[]> {
    if (isCatalogQuery(sql)) return (await this.handleCatalogQuery(sql)).fields
    const metadataFields = this.metadataFieldsForSQL(metadataSql)
    if (/\$\d+/.test(sql) && metadataFields.length > 0) return metadataFields
    if (isSelectLike(sql)) {
      try {
        const result = await this.doExecResult(
          `SELECT * FROM (${stripTrailingSemicolon(sql)}) AS _orez_describe LIMIT 0`
        )
        const fields = this.fieldsForResult(metadataSql, result)
        if (fields.length > 0) return fields
      } catch {}
    }
    if (metadataFields.length > 0) return metadataFields
    return inferFieldsFromSQL(metadataSql)
  }

  private buildSelectResult(result: CatalogResult): Uint8Array {
    return concat(
      buildRowDescription(result.fields),
      ...result.rows.map((r) => buildDataRow(r, result.fields)),
      buildCommandComplete(`SELECT ${result.rows.length}`)
    )
  }

  private buildSelectResponse(
    rows: Record<string, unknown>[],
    fields: { name: string; oid?: number }[]
  ): Uint8Array {
    return concat(this.buildSelectResult({ rows, fields }), this.readyForQuery())
  }

  private async handleCatalogQueries(sql: string): Promise<CatalogResult[]> {
    try {
      const parsed = parseSync(stripTrailingSemicolon(sql.trim()))
      const results: CatalogResult[] = []
      for (const statement of parsed.stmts) {
        const select = statement.stmt?.SelectStmt
        if (!select) continue
        results.push(await this.handleCatalogSelect(select))
      }
      return results.length ? results : [{ rows: [], fields: [] }]
    } catch {
      const currentSettings = catalogCurrentSettingResult(sql)
      return [currentSettings ?? { rows: [], fields: [] }]
    }
  }

  private async handleCatalogQuery(sql: string): Promise<CatalogResult> {
    return (await this.handleCatalogQueries(sql))[0] ?? { rows: [], fields: [] }
  }

  private async handleCatalogSelect(select: any): Promise<CatalogResult> {
    // zero-cache's initial sync validates the publication via pg_publication /
    // pg_publication_tables. those answers come from in-memory this.publications,
    // loaded once at backend init. a backend instance constructed during early
    // embed boot (migrateOnly, before CREATE PUBLICATION persisted) caches an
    // empty set, so the catalog query reports "Found: []" → setupTablesAndReplication
    // throws "Unknown or invalid publications" → initial sync aborts → the embed
    // never reaches ready (120s timeout, /sync sends 0 frames). the write path
    // already self-heals (reloadPublicationsIfEmpty); the catalog-read path the
    // change-streamer uses did not. reload from durable _orez_pg_metadata before
    // answering so the publication created concurrently on another instance is seen.
    if (
      selectReferencesTable(select, 'pg_publication') ||
      selectReferencesTable(select, 'pg_publication_tables')
    ) {
      await this.reloadPublicationsIfEmpty()
    }
    return (
      catalogCurrentSettingResultFromSelect(select) ??
      (await this.informationSchemaKeyColumnsResult(select)) ??
      (await this.informationSchemaColumnsResult(select)) ??
      this.pgTypeArrayResult(select) ??
      (await this.publishedSchemaResult(select)) ??
      (await this.publishedTablesResult(select)) ??
      (await this.publishedIndexesResult(select)) ??
      (await this.pgPublicationTablesResult(select)) ??
      pgPublicationResult(select, this.publicationNames()) ??
      (await this.pgTablesResult(select)) ??
      pgSettingsResult(select) ??
      logicalEmitMessageResult(select) ??
      advisoryLockResult(select) ??
      emptyCatalogResultFromSelect(select)
    )
  }
}

class HttpClient {
  private fetcher: typeof fetch

  constructor(fetcher: typeof fetch = fetch) {
    this.fetcher = fetcher
  }

  async post(
    url: string,
    body: string,
    headers?: Record<string, string>
  ): Promise<string> {
    const resp = await this.fetcher(url, {
      method: 'POST',
      headers: headers ?? { 'Content-Type': 'application/json' },
      body,
    })
    if (!resp.ok) {
      const text = await resp.text().catch(() => '')
      throw new Error(`HTTP ${resp.status}: ${text.slice(0, 5000)}`)
    }
    return resp.text()
  }
}
