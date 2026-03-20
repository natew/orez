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
  result.count = (command === 'SELECT' || !pgliteResult.affectedRows)
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

// -- parameter serialization --
// convert js values to postgres-compatible parameter values

function serializeParam(value: unknown): unknown {
  if (value === null || value === undefined) return null
  if (value instanceof Identifier) return value // handled in template assembly
  if (typeof value === 'bigint') return value.toString()
  if (typeof value === 'object' && !(value instanceof Date) && !Array.isArray(value) && !ArrayBuffer.isView(value)) {
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
      } else {
        params.push(serializeParam(val))
        text += `$${params.length}`
      }
    }
  }

  return { text, params }
}

// -- pending query --
// wraps a promise to add .simple(), .readable(), .writable(), .execute(), .describe(), .values(), .raw()

function createPendingQuery<T>(
  promise: Promise<T>
): T extends any[] ? Promise<T> & PendingQueryModifiers : Promise<T> & PendingQueryModifiers {
  const pending = promise as any

  pending.simple = () => pending
  pending.execute = () => pending
  pending.cancel = () => {}

  pending.describe = () =>
    Promise.reject(new Error('describe() not supported in worker mode'))
  pending.values = () =>
    Promise.reject(new Error('values() not supported in worker mode'))
  pending.raw = () =>
    Promise.reject(new Error('raw() not supported in worker mode'))

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

  pending.cursor = () => {
    throw new Error('cursor() not supported in worker mode')
  }

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

// -- sql function factory for a given executor --
// used both for the top-level sql and for transaction sql

function createSqlFunction(
  executor: {
    query<T>(sql: string, params?: unknown[]): Promise<Results<T>>
    exec(sql: string): Promise<Array<Results>>
  }
) {
  function sql(first: any, ...rest: any[]): any {
    // tagged template: sql`SELECT ...`
    if (first && Array.isArray(first.raw)) {
      const { text, params } = buildQuery(first as TemplateStringsArray, rest)
      const promise = executor
        .query(text, params)
        .then((r) => createResultArray(r as Results<any>, text))
      return createPendingQuery(promise)
    }

    // function call with string: sql('identifier') => Identifier
    if (typeof first === 'string' && rest.length === 0) {
      return new Identifier(first)
    }

    // function call with array of objects: sql(data, ...columns) => Helper for insert
    // not fully implemented — throw a clear error
    throw new Error(
      'postgres shim: helper forms (insert/update/dynamic values) are not yet supported'
    )
  }

  return sql
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
  const sqlFn = createSqlFunction(pglite)

  function sql(first: any, ...rest: any[]): any {
    return sqlFn(first, ...rest)
  }

  // sql.unsafe(queryString, params?) — raw SQL execution
  sql.unsafe = (queryString: string, params?: unknown[]) => {
    const serializedParams = params?.map(serializeParam)
    const promise = (
      serializedParams && serializedParams.length > 0
        ? pglite.query(queryString, serializedParams)
        : pglite.query(queryString)
    ).then((r) => createResultArray(r as Results<any>, queryString))
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
      const txSql = createSqlFunction(tx)

      function txSqlFn(first: any, ...rest: any[]): any {
        return txSql(first, ...rest)
      }

      // add unsafe to transaction sql
      txSqlFn.unsafe = (queryString: string, params?: unknown[]) => {
        const serializedParams = params?.map(serializeParam)
        const promise = (
          serializedParams && serializedParams.length > 0
            ? tx.query(queryString, serializedParams)
            : tx.query(queryString)
        ).then((r) => createResultArray(r as Results<any>, queryString))
        return createPendingQuery(promise)
      }

      // add begin (savepoint) to transaction sql
      txSqlFn.begin = sql.begin

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
    parsers: {} as Record<number, (value: string) => unknown>,
    fetch_types: opts?.fetch_types ?? false,
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
    server_version: '16.0',
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

function postgres(_urlOrOpts?: string | PostgresShimOptions, opts?: PostgresShimOptions): ReturnType<typeof createPostgresShim> {
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
