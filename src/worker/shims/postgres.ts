/**
 * postgres shim for cloudflare workers.
 *
 * wraps a PGlite instance to implement the `postgres` npm package API
 * that zero-cache uses. enables bundler aliasing so zero-cache talks to
 * PGlite instead of a real postgres server.
 *
 * usage with bundler alias:
 *   alias: { 'postgres': './src/worker/shims/postgres.js' }
 *
 * usage directly:
 *   import { createPostgresShim } from 'orez/worker/shims/postgres'
 *   const sql = createPostgresShim(pglite)
 *   const rows = await sql`SELECT * FROM users WHERE id = ${id}`
 */

import type { PGlite, Results, Transaction } from '@electric-sql/pglite'
import { PassThrough } from 'stream'

// -- PostgresError --

export class PostgresError extends Error {
  name = 'PostgresError' as const
  severity_local: string
  severity: string
  code: string
  position: string
  file: string
  line: string
  routine: string
  detail?: string
  hint?: string
  schema_name?: string
  table_name?: string
  column_name?: string
  constraint_name?: string
  query: string
  parameters: unknown[]

  constructor(info: {
    message?: string
    code?: string
    severity?: string
    detail?: string
    hint?: string
    [key: string]: unknown
  }) {
    super(info.message || 'postgres error')
    this.severity_local = (info.severity as string) || 'ERROR'
    this.severity = (info.severity as string) || 'ERROR'
    this.code = (info.code as string) || '00000'
    this.position = (info.position as string) || ''
    this.file = (info.file as string) || ''
    this.line = (info.line as string) || ''
    this.routine = (info.routine as string) || ''
    this.detail = info.detail as string | undefined
    this.hint = info.hint as string | undefined
    this.schema_name = info.schema_name as string | undefined
    this.table_name = info.table_name as string | undefined
    this.column_name = info.column_name as string | undefined
    this.constraint_name = info.constraint_name as string | undefined
    this.query = (info.query as string) || ''
    this.parameters = (info.parameters as unknown[]) || []
    Object.assign(this, info)
  }
}

// -- Identifier --
// returned by sql(string) for dynamic identifier escaping

class Identifier {
  value: string
  constructor(value: string) {
    this.value = escapeIdentifier(value)
  }
}

function escapeIdentifier(str: string): string {
  return '"' + str.replace(/"/g, '""').replace(/\./g, '"."') + '"'
}

// -- result array --
// creates an array of rows that also has metadata properties (count, command, columns, statement, state)
// matching the RowList type from postgres

interface ResultMeta {
  count: number
  command: string
  state: { status: string; pid: number; secret: number }
  statement: { name: string; string: string; types: number[]; columns: ColumnMeta[] }
  columns: ColumnMeta[]
}

interface ColumnMeta {
  name: string
  type: number
  table: number
  number: number
  parser?: ((raw: string) => unknown) | undefined
}

type ResultArray<T = Record<string, unknown>> = T[] & ResultMeta

function createResultArray<T extends Record<string, unknown>>(
  pgliteResult: Results<T>,
  queryString: string
): ResultArray<T> {
  const rows = pgliteResult.rows
  const columns: ColumnMeta[] = (pgliteResult.fields || []).map((f, i) => ({
    name: f.name,
    type: f.dataTypeID,
    table: 0,
    number: i,
  }))

  // create a proper array with rows as elements
  const result = [...rows] as ResultArray<T>

  // attach metadata
  const command = detectCommand(queryString)
  // for SELECT queries affectedRows is 0, use row count instead
  result.count =
    command === 'SELECT' || !pgliteResult.affectedRows
      ? rows.length
      : pgliteResult.affectedRows
  result.command = command
  result.state = { status: 'idle', pid: 0, secret: 0 }
  result.statement = {
    name: '',
    string: queryString,
    types: [],
    columns,
  }
  result.columns = columns

  return result
}

function detectCommand(sql: string): string {
  const trimmed = sql.trimStart().toUpperCase()
  if (trimmed.startsWith('SELECT')) return 'SELECT'
  if (trimmed.startsWith('INSERT')) return 'INSERT'
  if (trimmed.startsWith('UPDATE')) return 'UPDATE'
  if (trimmed.startsWith('DELETE')) return 'DELETE'
  if (trimmed.startsWith('CREATE')) return 'CREATE'
  if (trimmed.startsWith('DROP')) return 'DROP'
  if (trimmed.startsWith('ALTER')) return 'ALTER'
  return trimmed.split(/\s/)[0] || 'SELECT'
}

// -- multi-statement detection --
// detects if a query string contains multiple SQL statements.
// strips string literals and comments first to avoid false positives
// from semicolons inside quoted strings.

function hasMultipleStatements(sql: string): boolean {
  // strip dollar-quoted strings ($$ ... $$)
  let stripped = sql.replace(/(\$[a-zA-Z_]*\$)([\s\S]*?)\1/g, '')
  // strip string literals (single-quoted, with '' escape)
  stripped = stripped.replace(/'(?:[^']|'')*'/g, '')
  // strip double-quoted identifiers
  stripped = stripped.replace(/"(?:[^"]|"")*"/g, '')
  // strip -- line comments
  stripped = stripped.replace(/--[^\n]*/g, '')
  // strip /* block comments */
  stripped = stripped.replace(/\/\*[\s\S]*?\*\//g, '')

  // check if there are multiple non-empty statements
  const statements = stripped
    .split(';')
    .map((s) => s.trim())
    .filter((s) => s.length > 0)

  return statements.length > 1
}

// -- parameter serialization --
// convert js values to postgres-compatible parameter values

function serializeParam(value: unknown): unknown {
  if (value === null || value === undefined) return null
  if (value instanceof Identifier) return value // handled in template assembly
  if (typeof value === 'bigint') return value.toString()
  if (
    typeof value === 'object' &&
    !(value instanceof Date) &&
    !Array.isArray(value) &&
    !ArrayBuffer.isView(value)
  ) {
    return JSON.stringify(value)
  }
  return value
}

// -- template tag to parameterized query conversion --
// sql`SELECT * FROM foo WHERE id = ${id} AND name = ${name}`
// becomes: { text: 'SELECT * FROM foo WHERE id = $1 AND name = $2', params: [id, name] }

function buildQuery(
  strings: TemplateStringsArray,
  values: unknown[]
): { text: string; params: unknown[] } {
  const params: unknown[] = []
  let text = ''

  for (let i = 0; i < strings.length; i++) {
    text += strings[i]
    if (i < values.length) {
      const val = values[i]
      if (val instanceof Identifier) {
        // identifiers are inlined (already escaped)
        text += val.value
      } else if (val && typeof val === 'object' && '_isHelper' in val && (val as any)._isHelper) {
        // sql(object) helper — expand based on preceding SQL context
        const helper = val as { _isHelper: true; _data: Record<string, any>; toInsert: () => any; toUpdate: () => any }
        const before = text.trimEnd().toUpperCase()
        if (/\)\s*$/.test(before) || /SET\s*$/i.test(before) || /DO\s+UPDATE\s+SET\s*$/i.test(before)) {
          // UPDATE SET context: col1 = $1, col2 = $2
          const { set, params: updateParams } = helper.toUpdate()
          // rebase placeholder indices
          const rebasedSet = set.replace(/\$(\d+)/g, (_: string, n: string) => `$${params.length + Number(n)}`)
          text += rebasedSet
          params.push(...updateParams.map(serializeParam))
        } else {
          // INSERT context: (col1, col2) VALUES ($1, $2)
          const { columns, values: placeholders, params: insertParams } = helper.toInsert()
          // rebase placeholder indices
          const rebasedPlaceholders = placeholders.replace(/\$(\d+)/g, (_: string, n: string) => `$${params.length + Number(n)}`)
          text += `(${columns}) VALUES (${rebasedPlaceholders})`
          params.push(...insertParams.map(serializeParam))
        }
      } else if (val && typeof val === 'object' && '_isArrayExpansion' in val && (val as any)._isArrayExpansion) {
        // sql([1,2,3]) — expand for IN clauses: ($1, $2, $3)
        const arr = (val as any)._values as unknown[]
        const placeholders = arr.map((_, j) => `$${params.length + j + 1}`).join(', ')
        text += `(${placeholders})`
        params.push(...arr.map(serializeParam))
      } else if (Array.isArray(val)) {
        // raw array in template tag
        // check context: json_to_recordset etc. need JSON, everything else needs PG array
        const before = text.trimEnd()
        const needsJson = /json_to_record(?:set)?|json_(?:array|build|each|populate)\s*\(\s*$/i.test(before)
          || (/\(\s*$/.test(before) && /json/i.test(before.slice(Math.max(0, before.length - 40))))
        if (needsJson) {
          params.push(JSON.stringify(val))
          text += `$${params.length}::json`
        } else {
          // PostgreSQL array literal: {val1,val2,...}
          const pgArray = `{${val.map((v: any) => {
            if (v === null || v === undefined) return 'NULL'
            const s = String(v)
            // quote if contains special chars
            if (s.includes(',') || s.includes('"') || s.includes('{') || s.includes('}') || s.includes(' ')) {
              return `"${s.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`
            }
            return s
          }).join(',')}}`
          params.push(pgArray)
          text += `$${params.length}`
        }
      } else {
        const serialized = serializeParam(val)
        params.push(serialized)
        // add ::json cast for JSON values (PGlite needs explicit type for json_to_recordset etc.)
        const needsJsonCast = typeof serialized === 'string' && typeof val === 'object' && val !== null
          && (serialized.startsWith('[') || serialized.startsWith('{'))
        text += `$${params.length}${needsJsonCast ? '::json' : ''}`
      }
    }
  }

  return { text, params }
}

// -- pending query --
// wraps a promise to add .simple(), .readable(), .writable(), .execute(), .describe(), .values(), .raw()

function createPendingQuery<T>(
  promise: Promise<T>
): T extends any[]
  ? Promise<T> & PendingQueryModifiers
  : Promise<T> & PendingQueryModifiers {
  const pending = promise as any

  pending.simple = () => pending
  pending.execute = () => pending
  pending.cancel = () => {}

  pending.describe = () =>
    Promise.reject(new Error('describe() not supported in worker mode'))
  pending.values = () =>
    promise.then((rows: any) => {
      if (!Array.isArray(rows)) return []
      return rows.map((row: any) => Object.values(row))
    })
  pending.raw = () => Promise.reject(new Error('raw() not supported in worker mode'))

  pending.readable = () => {
    throw new Error('readable() not supported in worker mode')
  }
  pending.writable = () => {
    throw new Error('writable() not supported in worker mode')
  }

  pending.forEach = (cb: (row: any, result: any) => void) =>
    promise.then((rows: any) => {
      const result = { count: rows.length, command: rows.command || 'SELECT' }
      for (const row of rows) cb(row, result)
      return result
    })

  // cursor: returns async iterable yielding batches of rows
  pending.cursor = (batchSize: number = 100) => ({
    [Symbol.asyncIterator]() {
      let allRows: any[] | null = null
      let offset = 0
      return {
        async next() {
          if (!allRows) {
            const result = await promise
            allRows = Array.isArray(result) ? result : []
          }
          if (offset >= allRows.length) return { done: true as const, value: undefined }
          const batch = allRows.slice(offset, offset + batchSize)
          offset += batchSize
          return { done: false as const, value: batch }
        },
      }
    },
  })

  pending.stream = () => {
    throw new Error('stream() is deprecated, use forEach()')
  }

  return pending
}

interface PendingQueryModifiers {
  simple(): this
  readable(): never
  writable(): never
  execute(): this
  cancel(): void
  describe(): Promise<never>
  values(): Promise<never>
  raw(): Promise<never>
  forEach(cb: (row: any, result: any) => void): Promise<any>
  cursor(...args: unknown[]): never
}

// -- execute a query, routing multi-statement to exec() --
// PGlite.query() only handles single statements. multi-statement DDL
// (schema migrations, etc.) must use exec(). when params are present
// AND the query is multi-statement, we split and run each individually.

// intercept replication-related queries that PGlite can't handle natively.
// these are sent by zero-cache during initialization (wal_level check,
// replication slot management, etc.) and need fake responses.
async function interceptReplicationQuery(
  text: string,
  pglite: PGlite
): Promise<ResultArray<any> | null> {
  const upper = text.trimStart().toUpperCase()

  // wal_level check: zero-cache verifies logical replication is enabled
  if (upper.includes('WAL_LEVEL') && (upper.includes('CURRENT_SETTING') || upper.startsWith('SHOW'))) {
    if (upper.includes('VERSION')) {
      return fakeResult([{ walLevel: 'logical', version: '170004' }], text)
    }
    return fakeResult([{ walLevel: 'logical' }], text)
  }

  // CREATE_REPLICATION_SLOT: zero-cache creates a slot during initial sync
  if (upper.includes('CREATE_REPLICATION_SLOT')) {
    const match = text.match(/CREATE_REPLICATION_SLOT\s+(?:"([^"]+)"|'([^']+)'|(\S+))/i)
    const slotName = match?.[1] || match?.[2] || match?.[3] || 'zero_slot'
    const lsn = '0/1000100'
    try {
      await pglite.exec(`
        CREATE TABLE IF NOT EXISTS _orez._zero_replication_slots (
          slot_name TEXT PRIMARY KEY, restart_lsn TEXT,
          confirmed_flush_lsn TEXT, wal_status TEXT DEFAULT 'reserved'
        )
      `)
      await pglite.query(
        `INSERT INTO _orez._zero_replication_slots (slot_name, restart_lsn, confirmed_flush_lsn)
         VALUES ($1, $2, $2) ON CONFLICT (slot_name) DO UPDATE SET restart_lsn = $2`,
        [slotName, lsn]
      )
    } catch {}
    return fakeResult([{
      slot_name: slotName, consistent_point: lsn,
      snapshot_name: '00000003-00000001-1', output_plugin: 'pgoutput',
    }], text)
  }

  // DROP_REPLICATION_SLOT
  if (upper.startsWith('DROP_REPLICATION_SLOT')) {
    return fakeResult([], text, 'DROP_REPLICATION_SLOT')
  }

  // pg_replication_slots query
  if (upper.includes('PG_REPLICATION_SLOTS') && upper.includes('SELECT')) {
    try {
      const result = await pglite.query<any>(
        `SELECT slot_name, restart_lsn as "restartLSN", wal_status as "walStatus"
         FROM _orez._zero_replication_slots`
      )
      return createResultArray(result, text)
    } catch {
      return fakeResult([], text)
    }
  }

  // IDENTIFY_SYSTEM
  if (upper === 'IDENTIFY_SYSTEM' || upper === 'IDENTIFY_SYSTEM;') {
    return fakeResult([{
      systemid: '1234567890', timeline: '1',
      xlogpos: '0/1000100', dbname: 'template1',
    }], text)
  }

  // ALTER ROLE ... REPLICATION
  if (upper.startsWith('ALTER ROLE') && upper.includes('REPLICATION')) {
    return fakeResult([], text, 'ALTER ROLE')
  }

  // SET TRANSACTION / SET SESSION
  if (upper.startsWith('SET TRANSACTION') || upper.startsWith('SET SESSION')) {
    return fakeResult([], text, 'SET')
  }

  return null
}

function fakeResult(rows: Record<string, unknown>[], queryString: string, command?: string): ResultArray<any> {
  const columns: ColumnMeta[] = rows.length > 0
    ? Object.keys(rows[0]).map((name, i) => ({ name, type: 25, table: 0, number: i }))
    : []
  const result = [...rows] as ResultArray<any>
  result.count = rows.length
  result.command = command || detectCommand(queryString)
  result.state = { status: 'idle', pid: 0, secret: 0 }
  result.statement = { name: '', string: queryString, types: [], columns }
  result.columns = columns
  return result
}

async function executeQuery(
  executor: {
    query<T>(sql: string, params?: unknown[]): Promise<Results<T>>
    exec(sql: string): Promise<Array<Results>>
  },
  text: string,
  params: unknown[],
  pglite?: PGlite
): Promise<ResultArray<any>> {
  // intercept replication-related queries before they reach PGlite
  if (pglite) {
    const intercepted = await interceptReplicationQuery(text, pglite)
    if (intercepted) return intercepted
  }

  // make FK constraints DEFERRABLE so zero-cache's batched CVR writes work
  // (zero-cache flushes desires before queries in the same transaction)
  if (/FOREIGN\s+KEY/i.test(text) && /CREATE\s+TABLE/i.test(text) && !/DEFERRABLE/i.test(text)) {
    const before = text
    text = text.replace(
      /(ON\s+DELETE\s+CASCADE)/gi,
      '$1 DEFERRABLE INITIALLY DEFERRED'
    )
    if (before !== text) {
      console.log('[postgres-shim] added DEFERRABLE to FK constraint')
    }
  }

  const isMulti = hasMultipleStatements(text)

  if (!isMulti) {
    // single statement — use query() with params
    const r = await (params.length > 0
      ? executor.query(text, params)
      : executor.query(text))
    return createResultArray(r as Results<any>, text)
  }

  // multi-statement: ALWAYS split and run individually
  // PGliteWorker's execProtocol rejects multi-statement SQL, and exec() also
  // uses execProtocol internally. splitting upfront avoids all these issues.
  if (params.length === 0) {
    const stmts = splitStatements(text)
    let lastResult: Results<any> = { rows: [], fields: [], affectedRows: 0 } as any
    for (const stmt of stmts) {
      lastResult = (await executor.query(stmt)) as Results<any>
    }
    return createResultArray(lastResult, text)
  }

  // multi-statement WITH params — split and run each statement,
  // distributing $N params to the correct statement
  const statements = splitStatements(text)
  let lastResult: Results<any> = { rows: [], fields: [], affectedRows: 0 } as any

  for (const stmt of statements) {
    // find which $N params this statement references
    const paramRefs = [...stmt.matchAll(/\$(\d+)/g)].map((m) => Number(m[1]))

    if (paramRefs.length > 0) {
      // remap $N to $1, $2, ... for this statement's params
      const stmtParams = paramRefs.map((n) => params[n - 1])
      let remapped = stmt
      paramRefs.forEach((origN, i) => {
        remapped = remapped.replace(new RegExp(`\\$${origN}\\b`), `$${i + 1}`)
      })
      lastResult = (await executor.query(remapped, stmtParams)) as Results<any>
    } else {
      // no params in this statement — can use query() directly
      lastResult = (await executor.query(stmt)) as Results<any>
    }
  }

  return createResultArray(lastResult, text)
}

// split SQL into individual statements, respecting string literals
function splitStatements(sql: string): string[] {
  // strip string literals to find real semicolons
  const literals: string[] = []
  // dollar-quoted strings first ($$ ... $$ or $tag$ ... $tag$)
  let stripped = sql.replace(/(\$[a-zA-Z_]*\$)([\s\S]*?)\1/g, (match) => {
    literals.push(match)
    return `__LIT${literals.length - 1}__`
  })
  stripped = stripped.replace(/'(?:[^']|'')*'/g, (match) => {
    literals.push(match)
    return `__LIT${literals.length - 1}__`
  })
  stripped = stripped.replace(/"(?:[^"]|"")*"/g, (match) => {
    literals.push(match)
    return `__LIT${literals.length - 1}__`
  })
  // strip -- comments
  stripped = stripped.replace(/--[^\n]*/g, '')
  // strip /* block comments */
  stripped = stripped.replace(/\/\*[\s\S]*?\*\//g, '')

  // split on semicolons
  const parts = stripped
    .split(';')
    .map((s) => s.trim())
    .filter((s) => s.length > 0)

  // restore literals
  return parts.map((part) => part.replace(/__LIT(\d+)__/g, (_, i) => literals[Number(i)]))
}

// -- sql function factory for a given executor --
// used both for the top-level sql and for transaction sql

function createSqlFunction(
  executor: {
    query<T>(sql: string, params?: unknown[]): Promise<Results<T>>
    exec(sql: string): Promise<Array<Results>>
  },
  rootPglite?: PGlite
) {
  function sql(first: any, ...rest: any[]): any {
    // tagged template: sql`SELECT ...`
    if (first && Array.isArray(first.raw)) {
      const { text, params } = buildQuery(first as TemplateStringsArray, rest)
      const promise = executeQuery(executor, text, params, rootPglite)
      return createPendingQuery(promise)
    }

    // function call with string: sql('identifier') => Identifier
    if (typeof first === 'string' && rest.length === 0) {
      return new Identifier(first)
    }

    // sql(object) — helper for dynamic INSERT/UPDATE
    if (typeof first === 'object' && first !== null && !Array.isArray(first)) {
      return {
        _isHelper: true,
        _data: first,
        toInsert() {
          const keys = Object.keys(first)
          const columns = keys.map((k) => '"' + k.replace(/"/g, '""') + '"').join(', ')
          const placeholders = keys.map((_, i) => `$${i + 1}`).join(', ')
          return { columns, values: placeholders, params: keys.map((k) => first[k]) }
        },
        toUpdate() {
          const keys = Object.keys(first)
          const set = keys.map((k, i) => `"${k.replace(/"/g, '""')}" = $${i + 1}`).join(', ')
          return { set, params: keys.map((k) => first[k]) }
        },
      }
    }

    // sql(array) — parameter expansion for IN clauses
    // wrap in marker object so buildQuery knows to expand (not serialize as JSON)
    if (Array.isArray(first)) {
      return { _isArrayExpansion: true, _values: first }
    }

    throw new Error('postgres shim: unsupported sql() call')
  }

  return sql
}

// -- COPY TO STDOUT support --

function createCopyPendingQuery(copyQuery: string, executor: { query: (sql: string, params?: unknown[]) => Promise<Results<any>> }): any {
  // extract the query from COPY (SELECT ...) TO STDOUT or COPY table TO STDOUT
  let selectQuery: string
  const parenMatch = copyQuery.match(/COPY\s*\(([\s\S]+)\)\s*TO\s+STDOUT/i)
  if (parenMatch) {
    selectQuery = parenMatch[1].trim()
  } else {
    const tableMatch = copyQuery.match(
      /COPY\s+("(?:[^"]|"")*"(?:\."(?:[^"]|"")*")*|\S+)\s+TO\s+STDOUT/i,
    )
    selectQuery = tableMatch ? `SELECT * FROM ${tableMatch[1]}` : 'SELECT 1 WHERE false'
  }

  // returns a Node.js Readable stream (via PassThrough) compatible with pipeline()
  const readablePromise = (async () => {
    const result = await executor.query(selectQuery)
    const rows = result.rows as any[]
    const pt = new PassThrough()

    // write all rows as TSV-encoded COPY output, using Buffer for stream compatibility
    const encoder = typeof Buffer !== 'undefined' ? null : new TextEncoder()
    for (const row of rows) {
      const values = Object.values(row).map((v: any) => {
        if (v === null || v === undefined) return '\\N'
        if (typeof v === 'boolean') return v ? 't' : 'f'
        if (typeof v === 'object') return JSON.stringify(v).replace(/\\/g, '\\\\').replace(/\t/g, '\\t').replace(/\n/g, '\\n')
        return String(v).replace(/\\/g, '\\\\').replace(/\t/g, '\\t').replace(/\n/g, '\\n')
      })
      const line = values.join('\t') + '\n'
      const chunk = encoder ? encoder.encode(line) : Buffer.from(line)
      pt.push(chunk)
    }
    // signal end of stream
    pt.push(null)
    return pt
  })()

  const pending = readablePromise as any
  pending.execute = () => pending
  pending.simple = () => pending
  pending.cancel = () => {}
  pending.readable = () => readablePromise
  pending.writable = () => Promise.resolve(new PassThrough())
  pending.describe = () => Promise.reject(new Error('not supported'))
  pending.values = () => Promise.reject(new Error('not supported'))
  pending.raw = () => Promise.reject(new Error('not supported'))
  pending.forEach = () => Promise.reject(new Error('not supported'))
  pending.cursor = () => { throw new Error('not supported') }
  pending.stream = () => { throw new Error('not supported') }
  return pending
}

// -- type parsers --
// pre-populated parsers matching the postgres npm package's type registry.

function identity(x: string) { return x }
function parseFloat_(x: string) { return parseFloat(x) }
function parseInt_(x: string) { return parseInt(x, 10) }
function parseBool(x: string) { return x === 't' || x === 'true' }
function parseJSON(x: string) { try { return JSON.parse(x) } catch { return x } }

function makeArrayParser(elementParser: (x: string) => unknown) {
  const fn = (x: string) => {
    if (!x || x === '{}') return []
    const inner = x.slice(1, -1)
    return inner.split(',').map((v) => {
      if (v === 'NULL') return null
      return elementParser(v.replace(/^"|"$/g, ''))
    })
  }
  ;(fn as any).array = true
  return fn
}

function buildDefaultParsers(): Record<number, (value: string) => unknown> {
  const p: Record<number, any> = {}
  // scalar types
  p[16] = parseBool       // bool
  p[17] = identity        // bytea
  p[20] = identity        // int8 (bigint as string)
  p[21] = parseInt_       // int2
  p[23] = parseInt_       // int4
  p[25] = identity        // text
  p[26] = parseInt_       // oid
  p[114] = parseJSON      // json
  p[700] = parseFloat_    // float4
  p[701] = parseFloat_    // float8
  p[1042] = identity      // bpchar
  p[1043] = identity      // varchar
  p[1082] = identity      // date
  p[1083] = identity      // time
  p[1114] = identity      // timestamp
  p[1184] = identity      // timestamptz
  p[1266] = identity      // timetz
  p[1700] = identity      // numeric
  p[2950] = identity      // uuid
  p[3802] = parseJSON     // jsonb
  // array types
  p[1000] = makeArrayParser(parseBool)    // bool[]
  p[1005] = makeArrayParser(parseInt_)    // int2[]
  p[1007] = makeArrayParser(parseInt_)    // int4[]
  p[1009] = makeArrayParser(identity)     // text[]
  p[1016] = makeArrayParser(identity)     // int8[]
  p[1021] = makeArrayParser(parseFloat_)  // float4[]
  p[1022] = makeArrayParser(parseFloat_)  // float8[]
  p[1015] = makeArrayParser(identity)     // varchar[]
  p[1182] = makeArrayParser(identity)     // date[]
  p[1115] = makeArrayParser(identity)     // timestamp[]
  p[1185] = makeArrayParser(identity)     // timestamptz[]
  p[2951] = makeArrayParser(identity)     // uuid[]
  p[199] = makeArrayParser(parseJSON)     // json[]
  p[3807] = makeArrayParser(parseJSON)    // jsonb[]
  return p
}

// -- main export --

export interface PostgresShimOptions {
  max?: number
  max_lifetime?: number
  idle_timeout?: number
  fetch_types?: boolean
  ssl?: unknown
  onnotice?: (notice: unknown) => void
  connection?: Record<string, unknown>
  types?: Record<string, unknown>
}

export function createPostgresShim(pglite: PGlite, opts?: PostgresShimOptions) {
  const sqlFn = createSqlFunction(pglite, pglite)

  function sql(first: any, ...rest: any[]): any {
    return sqlFn(first, ...rest)
  }

  // sql.unsafe(queryString, params?) — raw SQL execution
  sql.unsafe = (queryString: string, params?: unknown[]) => {
    const upper = queryString.trimStart().toUpperCase()

    // COPY TO STDOUT — returns readable stream of rows
    if (upper.startsWith('COPY') && upper.includes('TO STDOUT')) {
      return createCopyPendingQuery(queryString, pglite)
    }

    // make FK constraints DEFERRABLE (zero-cache CVR creates tables via unsafe())
    if (/FOREIGN\s+KEY/i.test(queryString) && /CREATE\s+TABLE/i.test(queryString) && !/DEFERRABLE/i.test(queryString)) {
      queryString = queryString.replace(
        /(ON\s+DELETE\s+CASCADE)/gi,
        '$1 DEFERRABLE INITIALLY DEFERRED'
      )
    }

    const serializedParams = (params ?? []).map(serializeParam)

    // multi-statement with no params: split and run each individually
    // PGliteWorker's execProtocol rejects multi-statement, so always split upfront
    if (hasMultipleStatements(queryString) && serializedParams.length === 0) {
      const promise = (async () => {
        const intercepted = await interceptReplicationQuery(queryString, pglite)
        if (intercepted) return intercepted
        {
            const statements = splitStatements(queryString)
            const resultArrays = []
            for (const stmt of statements) {
              const r = await pglite.query(stmt)
              resultArrays.push(createResultArray(r as Results<any>, stmt))
            }
            const combined = resultArrays as any
            combined.count = resultArrays.length
            combined.command = 'SELECT'
            combined.state = { status: 'idle', pid: 0, secret: 0 }
            combined.statement = { name: '', string: queryString, types: [], columns: [] }
            combined.columns = []
            return combined
        }
      })()
      return createPendingQuery(promise)
    }

    const promise = executeQuery(pglite, queryString, serializedParams, pglite)
    return createPendingQuery(promise)
  }

  // sql.begin(options?, callback) — transactions
  sql.begin = async (
    optionsOrCb: string | ((tx: any) => any),
    maybeCb?: (tx: any) => any
  ) => {
    const cb = typeof optionsOrCb === 'function' ? optionsOrCb : maybeCb!
    // isolation level is ignored — PGlite is single-connection

    return pglite.transaction(async (tx: Transaction) => {
      // defer FK constraints so batched writes can insert in any order
      await tx.query('SET CONSTRAINTS ALL DEFERRED').catch(() => {})
      const txSql = createSqlFunction(tx)

      function txSqlFn(first: any, ...rest: any[]): any {
        return txSql(first, ...rest)
      }

      // add unsafe to transaction sql
      txSqlFn.unsafe = (queryString: string, params?: unknown[]) => {
        // COPY TO STDOUT — returns readable stream of rows
        const upper = queryString.trimStart().toUpperCase()
        if (upper.startsWith('COPY') && upper.includes('TO STDOUT')) {
          return createCopyPendingQuery(queryString, tx)
        }

        const serializedParams = (params ?? []).map(serializeParam)

        // multi-statement: split and run each (PGlite rejects multi-statement in execProtocol)
        if (hasMultipleStatements(queryString) && serializedParams.length === 0) {
          const promise = (async () => {
            const stmts = splitStatements(queryString)
            const resultArrays = []
            for (const stmt of stmts) {
              const r = await tx.query(stmt)
              resultArrays.push(createResultArray(r as Results<any>, stmt))
            }
            const combined = resultArrays as any
            combined.count = resultArrays.length
            combined.command = 'SELECT'
            combined.state = { status: 'idle', pid: 0, secret: 0 }
            combined.statement = { name: '', string: queryString, types: [], columns: [] }
            combined.columns = []
            return combined
          })()
          return createPendingQuery(promise)
        }

        const promise = executeQuery(tx, queryString, serializedParams, pglite)
        return createPendingQuery(promise)
      }

      // add begin (savepoint) to transaction sql
      txSqlFn.begin = sql.begin

      // savepoint(name?, fn) — runs fn(sql) inside a SAVEPOINT
      let _savepointIdx = 0
      txSqlFn.savepoint = async (nameOrFn: any, maybeFn?: any) => {
        const fn = typeof nameOrFn === 'function' ? nameOrFn : maybeFn
        const name = typeof nameOrFn === 'string' ? nameOrFn : `sp_${_savepointIdx++}`
        const spName = name.replace(/[^a-zA-Z0-9_]/g, '_')

        await tx.query(`SAVEPOINT "${spName}"`)
        try {
          const result = await fn(txSqlFn)
          await tx.query(`RELEASE SAVEPOINT "${spName}"`)
          return result
        } catch (err) {
          await tx.query(`ROLLBACK TO SAVEPOINT "${spName}"`).catch(() => {})
          throw err
        }
      }

      // add no-op end
      txSqlFn.end = async () => {}

      // add options
      txSqlFn.options = sql.options

      // add PostgresError
      txSqlFn.PostgresError = PostgresError

      const result = await cb(txSqlFn)
      // match postgres behavior: unwrap array results via Promise.all
      return Array.isArray(result) ? await Promise.all(result) : result
    })
  }

  // sql.end() — no-op (PGlite lifecycle managed elsewhere)
  sql.end = async (_opts?: { timeout?: number }) => {}

  // sql.close() — alias for end
  sql.close = sql.end

  // sql.options — connection metadata
  sql.options = {
    host: ['localhost'],
    port: [5432],
    database: 'pglite',
    user: 'pglite',
    max: opts?.max ?? 1,
    parsers: buildDefaultParsers(),
    fetch_types: opts?.fetch_types ?? true,
    connection: opts?.connection ?? {},
    ssl: opts?.ssl ?? false,
    types: opts?.types ?? {},
    transform: {
      undefined: undefined,
      column: { from: undefined, to: undefined },
      value: { from: undefined, to: undefined },
      row: { from: undefined, to: undefined },
    },
    serializers: {} as Record<number, (value: unknown) => unknown>,
  }

  // sql.PostgresError — error class
  sql.PostgresError = PostgresError

  // sql.CLOSE / sql.END — sentinel objects
  sql.CLOSE = {} as Record<string, never>
  sql.END = sql.CLOSE

  // sql.parameters — server parameters
  sql.parameters = {
    application_name: 'pglite-shim',
    server_version: '17.0',
  }

  // sql.types / sql.typed — type helpers
  sql.typed = (value: unknown, oid: number) => ({ value, type: oid })
  sql.types = sql.typed

  // sql.json — json parameter helper
  sql.json = (value: unknown) => JSON.stringify(value)

  // sql.array — array parameter helper
  sql.array = (value: unknown[], type?: number) => ({ value, type, array: true })

  // sql.listen — not supported
  sql.listen = () => {
    throw new Error('listen() not supported in worker mode')
  }

  // sql.notify — not supported
  sql.notify = () => {
    throw new Error('notify() not supported in worker mode')
  }

  // sql.subscribe — not supported
  sql.subscribe = () => {
    throw new Error('subscribe() not supported in worker mode')
  }

  // sql.reserve — not supported
  sql.reserve = () => {
    throw new Error('reserve() not supported in worker mode')
  }

  // sql.file — not supported
  sql.file = () => {
    throw new Error('file() not supported in worker mode')
  }

  // sql.largeObject — not supported
  sql.largeObject = () => {
    throw new Error('largeObject() not supported in worker mode')
  }

  return sql
}

// -- default export --
// matches the `postgres` package's default export shape:
//   import postgres from 'postgres'
//   const sql = postgres(url, options)
//
// when used as a bundler alias, zero-cache calls postgres(connectionURI, options).
// we intercept by reading the PGlite instance from globalThis.__orez_pglite.

function postgres(
  _urlOrOpts?: string | PostgresShimOptions,
  opts?: PostgresShimOptions
): ReturnType<typeof createPostgresShim> {
  const pglite = (globalThis as any).__orez_pglite as PGlite | undefined
  if (!pglite) {
    throw new Error(
      'postgres shim: no PGlite instance found on globalThis.__orez_pglite. ' +
        'register PGlite before importing modules that use postgres.'
    )
  }

  const resolvedOpts = typeof _urlOrOpts === 'object' ? _urlOrOpts : opts
  return createPostgresShim(pglite, resolvedOpts)
}

// attach PostgresError and BigInt to the default export (matches postgres package)
postgres.PostgresError = PostgresError
postgres.BigInt = {
  to: 20,
  from: [20],
  parse: (x: string) => globalThis.BigInt(x),
  serialize: (x: bigint) => x.toString(),
}

export default postgres
