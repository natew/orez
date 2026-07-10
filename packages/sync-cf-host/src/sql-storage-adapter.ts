import type { MutatorSql, SyncSql } from './types.js'

type WireValue =
  | { kind: 'null' }
  | { kind: 'integer'; value: string }
  | { kind: 'real'; value: number }
  | { kind: 'text'; value: string }
  | { kind: 'blob'; value: number[] }

type WireRow = { columns: string[]; values: WireValue[] }

interface JsSyncDb {
  exec(sql: string, params: WireValue[]): void
  query(sql: string, params: WireValue[]): WireRow[]
}

export type CompiledQuery = { sql: string; params: WireValue[] }

export type AdapterStats = {
  execCalls: number
  queryCalls: number
  sqlMs: number
}

// Match transaction control only as the statement itself. CREATE TRIGGER uses
// `... BEGIN ... END` for its body, which is valid DO SQL and not a transaction.
const TX_SQL = /^\s*(BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE)(?=\s|;|$)/i
const NUMBERED_PARAMETER = /\?[0-9]+/
const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER)
const MIN_SAFE = BigInt(Number.MIN_SAFE_INTEGER)

export function assertHostSql(sql: string): void {
  if (TX_SQL.test(sql)) {
    throw new TypeError('transaction SQL is host-owned and forbidden')
  }
  if (NUMBERED_PARAMETER.test(sql)) {
    throw new TypeError('numbered parameters are forbidden; use positional ? bindings')
  }
}

function decodeBinding(value: WireValue): unknown {
  switch (value.kind) {
    case 'null':
      return null
    case 'integer': {
      const exact = BigInt(value.value)
      // SqlStorage does not accept bigint. Safe values can use Number; unsafe
      // values use decimal text and rely on the destination's INTEGER affinity.
      return exact >= MIN_SAFE && exact <= MAX_SAFE ? Number(exact) : value.value
    }
    case 'real':
    case 'text':
      return value.value
    case 'blob':
      return Uint8Array.from(value.value).buffer
  }
}

function encodeResult(value: unknown): WireValue {
  if (value === null) return { kind: 'null' }
  if (typeof value === 'number') {
    return Number.isInteger(value)
      ? { kind: 'integer', value: String(value) }
      : { kind: 'real', value }
  }
  if (typeof value === 'string') return { kind: 'text', value }
  if (value instanceof ArrayBuffer) {
    return { kind: 'blob', value: Array.from(new Uint8Array(value)) }
  }
  if (ArrayBuffer.isView(value)) {
    return {
      kind: 'blob',
      value: Array.from(new Uint8Array(value.buffer, value.byteOffset, value.byteLength)),
    }
  }
  throw new TypeError(`unsupported SqlStorage result: ${Object.prototype.toString.call(value)}`)
}

/**
 * The sole JS SQL adapter used by wasm. It is synchronous, consumes every
 * cursor before returning, accepts positional `?` only, and never starts or
 * finishes a transaction.
 */
export class SqlStorageSyncDb implements JsSyncDb {
  readonly stats: AdapterStats = { execCalls: 0, queryCalls: 0, sqlMs: 0 }

  constructor(private readonly sql: SqlStorage) {}

  resetStats(): void {
    this.stats.execCalls = 0
    this.stats.queryCalls = 0
    this.stats.sqlMs = 0
  }

  exec(sql: string, params: WireValue[]): void {
    assertHostSql(sql)
    const start = performance.now()
    this.sql.exec(sql, ...params.map(decodeBinding))
    this.stats.execCalls++
    this.stats.sqlMs += performance.now() - start
  }

  query(sql: string, params: WireValue[]): WireRow[] {
    assertHostSql(sql)
    const start = performance.now()
    const cursor = this.sql.exec(sql, ...params.map(decodeBinding))
    const columns = [...cursor.columnNames]
    // A SqlStorage cursor must be fully consumed before any await. Returning a
    // materialized array here makes that property structural.
    const rows = Array.from(cursor.raw()).map((values) => ({
      columns,
      values: values.map(encodeResult),
    }))
    this.stats.queryCalls++
    this.stats.sqlMs += performance.now() - start
    return rows
  }
}

/** Direct application SQL surface used for initialization and admin reads. */
export class SqlStorageDirect implements SyncSql {
  constructor(private readonly sql: SqlStorage) {}

  exec(sql: string, params: readonly unknown[] = []): void {
    assertHostSql(sql)
    this.sql.exec(sql, ...params)
  }

  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params: readonly unknown[] = [],
  ): Row[] {
    assertHostSql(sql)
    return this.sql.exec(sql, ...params).toArray() as Row[]
  }
}

/**
 * Async-compatible consumer mutator surface. Each method fully completes and
 * materializes its SqlStorage operation before its promise resolves, so an
 * existing `await tx.query(...)` cannot carry a cursor across an await.
 */
export class SqlStorageMutatorTransaction implements MutatorSql {
  constructor(
    private readonly direct: SqlStorageDirect,
    private readonly compileQuery: (ast: unknown) => CompiledQuery,
  ) {}

  async exec(sql: string, params: readonly unknown[] = []): Promise<void> {
    this.direct.exec(sql, params)
  }

  async query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params: readonly unknown[] = [],
  ): Promise<Row[]> {
    return this.direct.query<Row>(sql, params)
  }

  async queryAst<Row extends Record<string, unknown> = Record<string, unknown>>(
    ast: unknown,
  ): Promise<Row[]> {
    const compiled = this.compileQuery(ast)
    return this.direct.query<Row>(compiled.sql, compiled.params.map(decodeBinding))
  }
}
