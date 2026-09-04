import { executeTransactionQueryPlan } from 'orez-sync-cf-host/transaction-query'
import { encodeSqlValue } from 'orez-sync-executor/sqlite'

import type { SyncSql } from './types.js'
import type { BedrockSqliteModule, Database, Statement } from 'bedrock-sqlite/browser'
import type {
  CompiledTransactionQueryPlan,
  TransactionQueryBudget,
} from 'orez-sync-cf-host/transaction-query'
import type { TransactionQueryFormat } from 'orez-sync-executor'
import type {
  ApplicationTransaction,
  ExecResult as SQLiteExecResult,
  SqlStatementMetadata,
} from 'orez-sync-executor'
import type { SqlWireValue } from 'orez-sync-executor/sqlite'

export type WireValue = SqlWireValue

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
  return encodeSqlValue(value)
}

class StatementCache {
  readonly #statements = new Map<string, Statement>()

  constructor(private readonly db: Database) {}

  get(sql: string): Statement {
    let statement = this.#statements.get(sql)
    if (!statement) {
      statement = this.db.prepare(sql)
      if (typeof statement.safeIntegers === 'function') {
        statement.safeIntegers(true)
      }
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
    const statement = this.#statements.get(sql)
    const bindings = params.map(decodeBinding)
    if (typeof (statement as unknown as { values?: unknown }).values === 'function') {
      const columns =
        (statement as unknown as { columnNames?: string[] }).columnNames ?? []
      const rawRows = (
        statement as unknown as { values(params: unknown[]): unknown[][] }
      ).values(bindings)
      return rawRows.map((values) => ({
        columns,
        values: values.map(encodeResult),
      }))
    }
    const rawStatement = typeof statement.raw === 'function' ? statement.raw() : statement
    const columns =
      typeof rawStatement.columns === 'function'
        ? rawStatement.columns().map((column) => column.name)
        : []
    const rawRows = rawStatement.all(bindings) as unknown[][]
    return rawRows.map((values) => ({
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
        if (typeof statement.safeIntegers === 'function') {
          statement.safeIntegers(true)
        }
        const result = statement.get() as { changes: number | bigint }
        return { changes: Number(result.changes) }
      } finally {
        statement.finalize()
      }
    }
    const statement = this.db.prepare(sql)
    try {
      if (typeof statement.safeIntegers === 'function') {
        statement.safeIntegers(true)
      }
      const res = statement.run([...params]) as { changes?: number | bigint }
      return { changes: Number(res.changes ?? 0) }
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
      if (typeof statement.safeIntegers === 'function') {
        statement.safeIntegers(true)
      }
      return statement.all([...params]) as Row[]
    } finally {
      statement.finalize()
    }
  }
}

export class BedrockMutatorSql implements ApplicationTransaction {
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
