import { executeTransactionQueryPlan } from 'orez-sync-cf-host/transaction-query'

import type { MutatorSql, SyncSql } from './types.js'
import type { BedrockSqliteModule, Database, Statement } from 'bedrock-sqlite/browser'
import type { SQLiteExecResult, SqlStatementMetadata } from 'orez-sync-cf-host'
import type {
  CompiledTransactionQueryPlan,
  TransactionQueryBudget,
  TransactionQueryFormat,
} from 'orez-sync-cf-host/transaction-query'

export type WireValue =
  | { kind: 'null' }
  | { kind: 'integer'; value: string }
  | { kind: 'real'; value: number }
  | { kind: 'text'; value: string }
  | { kind: 'blob'; value: number[] }

export type WireRow = { columns: string[]; values: WireValue[] }

export interface JsSyncDb {
  exec(sql: string, params: WireValue[]): void
  query(sql: string, params: WireValue[]): WireRow[]
}

export type MemfsFile = { data: Uint8Array; size: number }
export type BedrockBrowserModule = BedrockSqliteModule & {
  _memfs: {
    files: Record<string, MemfsFile>
    fds: Record<number, { path: string; pos: number }>
    nextFd: number
  }
}

const TX_SQL = /^\s*(BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE)(?=\s|;|$)/i
const NUMBERED_PARAMETER = /\?[0-9]+/
const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER)
const MIN_SAFE = BigInt(Number.MIN_SAFE_INTEGER)

export function assertConsumerSql(sql: string): void {
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
      return exact >= MIN_SAFE && exact <= MAX_SAFE ? Number(exact) : exact
    }
    case 'real':
    case 'text':
      return value.value
    case 'blob':
      return Uint8Array.from(value.value)
  }
}

function encodeResult(value: unknown): WireValue {
  if (value === null) return { kind: 'null' }
  if (typeof value === 'bigint') return { kind: 'integer', value: value.toString() }
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
  throw new TypeError(
    `unsupported Bedrock SQLite result: ${Object.prototype.toString.call(value)}`
  )
}

class StatementCache {
  readonly #statements = new Map<string, Statement>()

  constructor(private readonly db: Database) {}

  get(sql: string): Statement {
    let statement = this.#statements.get(sql)
    if (!statement) {
      statement = this.db.prepare(sql)
      this.#statements.set(sql, statement)
    }
    return statement
  }
}

/** The synchronous wire-value database boundary consumed by sync-wasm. */
export class BedrockSyncDb implements JsSyncDb {
  readonly #statements: StatementCache

  constructor(db: Database) {
    this.#statements = new StatementCache(db)
  }

  exec(sql: string, params: WireValue[]): void {
    assertConsumerSql(sql)
    this.#statements.get(sql).run(params.map(decodeBinding))
  }

  query(sql: string, params: WireValue[]): WireRow[] {
    assertConsumerSql(sql)
    const statement = this.#statements.get(sql).raw()
    const columns = statement.columns().map((column) => column.name)
    return (statement.all(params.map(decodeBinding)) as unknown[][]).map((values) => ({
      columns,
      values: values.map(encodeResult),
    }))
  }
}

/** Synchronous application SQL used only while the host owns the operation. */
export class BedrockDirectSql implements SyncSql {
  constructor(private readonly db: Database) {}

  exec(
    sql: string,
    params: readonly unknown[] = [],
    _metadata?: SqlStatementMetadata
  ): SQLiteExecResult {
    assertConsumerSql(sql)
    if (params.length === 0) {
      this.db.exec(sql)
      const statement = this.db.prepare('SELECT changes() AS changes')
      try {
        const result = statement.get() as { changes: number }
        return { changes: result.changes }
      } finally {
        statement.finalize()
      }
    }
    const statement = this.db.prepare(sql)
    try {
      return { changes: statement.run([...params]).changes }
    } finally {
      statement.finalize()
    }
  }

  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params: readonly unknown[] = []
  ): Row[] {
    assertConsumerSql(sql)
    const statement = this.db.prepare(sql)
    try {
      return statement.all([...params]) as Row[]
    } finally {
      statement.finalize()
    }
  }
}

export class BedrockMutatorSql implements MutatorSql {
  constructor(
    private readonly direct: BedrockDirectSql,
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
  ): Promise<SQLiteExecResult> {
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
