import { trackBillableCursorRows } from 'orez-sync-executor/sql-billing'
import { encodeSqlValue } from 'orez-sync-executor/sqlite'

import { executeTransactionQueryPlan } from './transaction-query.js'

import type {
  CompiledTransactionQueryPlan,
  TransactionQueryBudget,
} from './transaction-query.js'
import type { SyncSql } from './types.js'
import type {
  ApplicationTransaction,
  ExecResult,
  SqlStatementMetadata,
} from 'orez-sync-executor'
import type { TransactionQueryFormat } from 'orez-sync-executor'
import type { SqlWireValue } from 'orez-sync-executor/sqlite'

type WireValue = SqlWireValue

type WireRow = { columns: string[]; values: WireValue[] }

interface JsSyncDb {
  exec(sql: string, params: WireValue[]): void
  query(sql: string, params: WireValue[]): WireRow[]
}

export type AdapterStats = {
  execCalls: number
  queryCalls: number
  sqlMs: number
}

export type SqlStorageFailureBinding = {
  index: number
  kind: WireValue['kind']
  value?: string | number | null
  bytes?: number
  truncated?: true
}

export type SqlStorageFailure = {
  operation: 'exec' | 'query'
  error: string
  sql: string
  sqlBytes: number
  sqlTruncated?: true
  paramCount: number
  params: SqlStorageFailureBinding[]
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

function decodeBinding(value: unknown, path = 'binding'): unknown {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError(`${path} must be a typed SQL value`)
  }
  const binding = value as { kind?: unknown; value?: unknown }
  const keys = Object.keys(binding)
  const assertValueShape = () => {
    if (keys.length !== 2 || !keys.includes('kind') || !keys.includes('value')) {
      throw new TypeError(`${path} must contain only kind and value`)
    }
  }
  switch (binding.kind) {
    case 'null':
      if (keys.length !== 1 || keys[0] !== 'kind') {
        throw new TypeError(`${path} null binding must contain only kind`)
      }
      return null
    case 'integer': {
      assertValueShape()
      if (typeof binding.value !== 'string') {
        throw new TypeError(`${path}.value must be an i64 decimal string`)
      }
      if (!/^-?\d+$/.test(binding.value)) {
        throw new TypeError(`${path}.value must be an i64 decimal string`)
      }
      let exact: bigint
      try {
        exact = BigInt(binding.value)
      } catch {
        throw new TypeError(`${path}.value must be an i64 decimal string`)
      }
      if (exact < -(1n << 63n) || exact > (1n << 63n) - 1n) {
        throw new TypeError(`${path}.value is outside the i64 range`)
      }
      // SqlStorage does not accept bigint. Safe values can use Number; unsafe
      // values use decimal text and rely on the destination's INTEGER affinity.
      return exact >= MIN_SAFE && exact <= MAX_SAFE ? Number(exact) : binding.value
    }
    case 'real': {
      assertValueShape()
      if (typeof binding.value !== 'number' || !Number.isFinite(binding.value)) {
        throw new TypeError(`${path}.value must be a finite number`)
      }
      return binding.value
    }
    case 'text': {
      assertValueShape()
      if (typeof binding.value !== 'string') {
        throw new TypeError(`${path}.value must be a string`)
      }
      return binding.value
    }
    case 'blob': {
      assertValueShape()
      if (
        !Array.isArray(binding.value) ||
        binding.value.some((byte) => !Number.isInteger(byte) || byte < 0 || byte > 255)
      ) {
        throw new TypeError(`${path}.value must be an array of bytes`)
      }
      return Uint8Array.from(binding.value).buffer
    }
    default:
      throw new TypeError(`${path}.kind must be null, integer, real, text, or blob`)
  }
}

export function decodeSqlParams(value: unknown): unknown[] {
  if (value === undefined) return []
  if (!Array.isArray(value)) throw new TypeError('params must be an array')
  return value.map((binding, index) => decodeBinding(binding, `params[${index}]`))
}

function encodeResult(value: unknown): WireValue {
  return encodeSqlValue(value)
}

const MAX_FAILURE_TEXT_BYTES = 256
const MAX_FAILURE_SQL_BYTES = 64 * 1024
const MAX_FAILURE_BINDINGS = 512
const failureTextEncoder = new TextEncoder()

function describeFailureBinding(
  value: WireValue,
  index: number
): SqlStorageFailureBinding {
  if (value.kind === 'null') return { index, kind: value.kind, value: null }
  if (value.kind === 'blob') return { index, kind: value.kind, bytes: value.value.length }
  if (value.kind !== 'text') return { index, kind: value.kind, value: value.value }
  const bytes = failureTextEncoder.encode(value.value).byteLength
  if (bytes <= MAX_FAILURE_TEXT_BYTES) {
    return { index, kind: value.kind, value: value.value, bytes }
  }
  return {
    index,
    kind: value.kind,
    value: `${value.value.slice(0, MAX_FAILURE_TEXT_BYTES / 2)}…${value.value.slice(-MAX_FAILURE_TEXT_BYTES / 2)}`,
    bytes,
    truncated: true,
  }
}

function describeSqlFailure(
  operation: SqlStorageFailure['operation'],
  error: unknown,
  sql: string,
  params: WireValue[]
): SqlStorageFailure {
  const sqlBytes = failureTextEncoder.encode(sql).byteLength
  const failure: SqlStorageFailure = {
    operation,
    error: String(error),
    sql:
      sqlBytes <= MAX_FAILURE_SQL_BYTES
        ? sql
        : `${sql.slice(0, MAX_FAILURE_SQL_BYTES / 2)}…${sql.slice(-MAX_FAILURE_SQL_BYTES / 2)}`,
    sqlBytes,
    paramCount: params.length,
    params: params.slice(0, MAX_FAILURE_BINDINGS).map(describeFailureBinding),
  }
  if (sqlBytes > MAX_FAILURE_SQL_BYTES) failure.sqlTruncated = true
  return failure
}

/**
 * The sole JS SQL adapter used by wasm. It is synchronous, consumes every
 * cursor before returning, accepts positional `?` only, and never starts or
 * finishes a transaction.
 */
export class SqlStorageSyncDb implements JsSyncDb {
  readonly stats: AdapterStats = { execCalls: 0, queryCalls: 0, sqlMs: 0 }

  constructor(
    private readonly sql: SqlStorage,
    private readonly recordRowsWritten: (rows: number) => void = () => {},
    private readonly recordRowsRead: (rows: number) => void = () => {},
    private readonly recordFailure: (failure: SqlStorageFailure) => void = () => {}
  ) {}

  resetStats(): void {
    this.stats.execCalls = 0
    this.stats.queryCalls = 0
    this.stats.sqlMs = 0
  }

  exec(sql: string, params: WireValue[]): void {
    assertHostSql(sql)
    const start = performance.now()
    try {
      trackBillableCursorRows(
        this.sql.exec(sql, ...params.map((value) => decodeBinding(value))),
        this.recordRowsWritten,
        this.recordRowsRead
      )
    } catch (error) {
      this.recordFailure(describeSqlFailure('exec', error, sql, params))
      throw error
    }
    this.stats.execCalls++
    this.stats.sqlMs += performance.now() - start
  }

  query(sql: string, params: WireValue[]): WireRow[] {
    assertHostSql(sql)
    const start = performance.now()
    let rows: WireRow[]
    try {
      const cursor = trackBillableCursorRows(
        this.sql.exec(sql, ...params.map((value) => decodeBinding(value))),
        this.recordRowsWritten,
        this.recordRowsRead
      )
      const columns = [...cursor.columnNames]
      // A SqlStorage cursor must be fully consumed before any await. Returning a
      // materialized array here makes that property structural.
      rows = Array.from(cursor.raw()).map((values) => ({
        columns,
        values: values.map(encodeResult),
      }))
    } catch (error) {
      this.recordFailure(describeSqlFailure('query', error, sql, params))
      throw error
    }
    this.stats.queryCalls++
    this.stats.sqlMs += performance.now() - start
    return rows
  }
}

/** Direct application SQL surface used for initialization and admin reads. */
export class SqlStorageDirect implements SyncSql {
  constructor(
    private readonly sql: SqlStorage,
    private readonly recordRowsWritten: (rows: number) => void = () => {},
    private readonly recordRowsRead: (rows: number) => void = () => {}
  ) {}

  exec(
    sql: string,
    params: readonly unknown[] = [],
    _metadata?: SqlStatementMetadata
  ): ExecResult {
    assertHostSql(sql)
    trackBillableCursorRows(
      this.sql.exec(sql, ...params),
      this.recordRowsWritten,
      this.recordRowsRead
    ).toArray()
    const changes = Number(
      trackBillableCursorRows(
        this.sql.exec('SELECT changes() AS changes'),
        this.recordRowsWritten,
        this.recordRowsRead
      ).one()?.changes ?? 0
    )
    return { changes }
  }

  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params: readonly unknown[] = []
  ): Row[] {
    assertHostSql(sql)
    return trackBillableCursorRows(
      this.sql.exec(sql, ...params),
      this.recordRowsWritten,
      this.recordRowsRead
    ).toArray() as Row[]
  }
}

/**
 * Async-compatible consumer mutator surface. Each method fully completes and
 * materializes its SqlStorage operation before its promise resolves, so an
 * existing `await tx.query(...)` cannot carry a cursor across an await.
 */
export class SqlStorageMutatorTransaction implements ApplicationTransaction {
  constructor(
    private readonly direct: SqlStorageDirect,
    private readonly compileQuery: (
      ast: unknown,
      format: TransactionQueryFormat
    ) => CompiledTransactionQueryPlan,
    private readonly queryBudget?: Partial<TransactionQueryBudget>
  ) {}

  async exec(
    sql: string,
    params: readonly unknown[] = [],
    metadata?: SqlStatementMetadata
  ): Promise<ExecResult> {
    return this.direct.exec(sql, params, metadata)
  }

  async query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params: readonly unknown[] = []
  ): Promise<Row[]> {
    return this.direct.query<Row>(sql, params)
  }

  async queryAst<Result = unknown>(
    ast: unknown,
    format: TransactionQueryFormat,
    queryName?: string
  ): Promise<Result> {
    const compiled = this.compileQuery(ast, format)
    return executeTransactionQueryPlan<Result>(
      compiled,
      (sql, params) => this.direct.query(sql, params),
      { queryName, budget: this.queryBudget }
    )
  }
}
