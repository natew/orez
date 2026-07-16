import { Database } from 'bun:sqlite'
import { deepStrictEqual } from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { inspect } from 'node:util'

import { PGlite } from '@electric-sql/pglite'

import {
  compile,
  extractZqlResult,
} from '../../node_modules/@rocicorp/zero/out/z2s/src/compiler.js'
import { formatPgInternalConvert } from '../../node_modules/@rocicorp/zero/out/z2s/src/sql.js'
import { getServerSchema } from '../../node_modules/@rocicorp/zero/out/zero-server/src/schema.js'
import {
  engine_compile_query,
  initSync,
} from '../../packages/sync-cf-host/src/generated/sync_wasm.js'
import { executeTransactionQueryPlan } from '../../packages/sync-cf-host/src/transaction-query.js'

type ColumnType = 'string' | 'number' | 'boolean' | 'json'

type ColumnSchema = {
  type: ColumnType
  serverName?: string
}

type TableSchema = {
  name?: string
  serverName?: string
  columns: Record<string, ColumnSchema>
  primaryKey: string[]
}

type Schema = {
  tables: Record<string, TableSchema>
}

type CorpusCase = {
  ns: string
  name: string
  user: string
  expect: string[]
  rootTable: string
  ast: unknown
  format: unknown
}

type Corpus = {
  version: number
  source: { commit: string; zero: string }
  counts: { cases: number; queries: number }
  schema: Schema
  seed: Record<string, string[]>
  cases: CorpusCase[]
}

type DatabasePair = {
  sqlite: Database
  postgres: PGlite
  serverSchema: unknown
}

const EXPECTED_CHAT_COMMIT = 'cc2d26fa24a88161231f3337c0e0cae9d43ae2d1'
const EXPECTED_ZERO_VERSION = '1.7.0'
const EXPECTED_CASES = 252
const EXPECTED_QUERIES = 123

const fixturePath = join(
  import.meta.dirname,
  '..',
  'corpus',
  'chat-transaction-query-v1.json'
)
const wasmPath = join(
  import.meta.dirname,
  '..',
  '..',
  'packages',
  'sync-cf-host',
  'src',
  'generated',
  'sync_wasm_bg.wasm'
)

const corpus = JSON.parse(readFileSync(fixturePath, 'utf8')) as Corpus

function quote(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`
}

function physicalTable(logicalName: string, table: TableSchema): string {
  return table.serverName ?? table.name ?? logicalName
}

function physicalColumn(logicalName: string, column: ColumnSchema): string {
  return column.serverName ?? logicalName
}

function sqliteType(type: ColumnType): string {
  switch (type) {
    case 'string':
    case 'json':
      return 'TEXT'
    case 'number':
      return 'REAL'
    case 'boolean':
      return 'INTEGER'
  }
}

function postgresType(type: ColumnType): string {
  switch (type) {
    case 'string':
      return 'TEXT'
    case 'number':
      return 'DOUBLE PRECISION'
    case 'boolean':
      return 'BOOLEAN'
    case 'json':
      return 'JSONB'
  }
}

function createTableSql(
  logicalName: string,
  table: TableSchema,
  typeName: (type: ColumnType) => string
): string {
  const columns = Object.entries(table.columns).map(
    ([logicalColumn, column]) =>
      `${quote(physicalColumn(logicalColumn, column))} ${typeName(column.type)}`
  )
  const primaryKey = table.primaryKey.map((column) => {
    const spec = table.columns[column]
    if (!spec) throw new Error(`unknown primary key ${logicalName}.${column}`)
    return quote(physicalColumn(column, spec))
  })
  columns.push(`PRIMARY KEY (${primaryKey.join(', ')})`)
  return `CREATE TABLE ${quote(physicalTable(logicalName, table))} (${columns.join(', ')})`
}

function schemaWithTableNames(schema: Schema): Schema {
  return {
    tables: Object.fromEntries(
      Object.entries(schema.tables).map(([name, table]) => [
        name,
        { ...table, name: table.name ?? name },
      ])
    ),
  }
}

function postgresValue(value: unknown, type: ColumnType): unknown {
  if (value === null) return null
  if (type === 'boolean') {
    if (value === 0 || value === 0n) return false
    if (value === 1 || value === 1n) return true
    throw new Error(`invalid SQLite boolean value ${inspect(value)}`)
  }
  if (type === 'json') {
    if (typeof value !== 'string') return JSON.stringify(value)
    JSON.parse(value)
    return value
  }
  return value
}

async function createDatabasePair(schema: Schema, seed: string[]): Promise<DatabasePair> {
  const sqlite = new Database(':memory:')
  const postgres = new PGlite()
  await postgres.waitReady

  for (const [logicalName, table] of Object.entries(schema.tables)) {
    sqlite.exec(createTableSql(logicalName, table, sqliteType))
    await postgres.exec(createTableSql(logicalName, table, postgresType))
  }
  for (const statement of seed) sqlite.exec(statement)

  for (const [logicalName, table] of Object.entries(schema.tables)) {
    const entries = Object.entries(table.columns)
    const selectColumns = entries
      .map(([logicalColumn, column]) => quote(physicalColumn(logicalColumn, column)))
      .join(', ')
    const rows = sqlite
      .query(`SELECT ${selectColumns} FROM ${quote(physicalTable(logicalName, table))}`)
      .all() as Record<string, unknown>[]
    if (rows.length === 0) continue

    const insertColumns = entries
      .map(([logicalColumn, column]) => quote(physicalColumn(logicalColumn, column)))
      .join(', ')
    const placeholders = entries.map((_, index) => `$${index + 1}`).join(', ')
    const insert = `INSERT INTO ${quote(physicalTable(logicalName, table))} (${insertColumns}) VALUES (${placeholders})`
    for (const row of rows) {
      const values = entries.map(([logicalColumn, column]) => {
        const name = physicalColumn(logicalColumn, column)
        return postgresValue(row[name], column.type)
      })
      await postgres.query(insert, values)
    }
  }

  const officialSchema = schemaWithTableNames(schema)
  const serverSchema = await getServerSchema(
    {
      query: async (sql: string, params: unknown[]) =>
        (await postgres.query<Record<string, unknown>>(sql, params)).rows,
    },
    officialSchema
  )
  return { sqlite, postgres, serverSchema }
}

async function runOfficial(
  pair: DatabasePair,
  schema: Schema,
  ast: unknown,
  format: unknown
): Promise<unknown> {
  const statement = compile(pair.serverSchema, schemaWithTableNames(schema), ast, format)
  const { text, values } = formatPgInternalConvert(statement)
  const result = await pair.postgres.query<Record<string, unknown>>(text, values)
  if (
    result.rows.length === 0 &&
    typeof format === 'object' &&
    format !== null &&
    (format as { singular?: unknown }).singular === true
  ) {
    return undefined
  }
  return extractZqlResult(result.rows)
}

function runSqlite(
  pair: DatabasePair,
  schema: Schema,
  ast: unknown,
  format: unknown,
  queryName: string
): unknown {
  const plan = engine_compile_query(schema, ast, format)
  return executeTransactionQueryPlan(
    plan,
    (sql, params) => pair.sqlite.query(sql).all(...params),
    { queryName, budget: { maxSelects: 10_000, maxRows: 100_000 } }
  )
}

function compare(label: string, official: unknown, sqlite: unknown): void {
  try {
    deepStrictEqual(sqlite, official)
  } catch (error) {
    throw new Error(
      `${label} diverged\nOfficial z2s/Postgres:\n${inspect(official, { depth: 12 })}\nOrez/SQLite:\n${inspect(sqlite, { depth: 12 })}\n${String(error)}`
    )
  }
}

async function runChatCorpus(): Promise<void> {
  if (
    corpus.version !== 1 ||
    corpus.source.commit !== EXPECTED_CHAT_COMMIT ||
    corpus.source.zero !== EXPECTED_ZERO_VERSION ||
    corpus.counts.cases !== EXPECTED_CASES ||
    corpus.counts.queries !== EXPECTED_QUERIES ||
    corpus.cases.length !== EXPECTED_CASES
  ) {
    throw new Error('Chat transaction query fixture provenance or count changed')
  }

  const pairs = new Map<string, DatabasePair>()
  try {
    for (const [namespace, seed] of Object.entries(corpus.seed)) {
      pairs.set(namespace, await createDatabasePair(corpus.schema, seed))
    }

    for (const [index, testCase] of corpus.cases.entries()) {
      const pair = pairs.get(testCase.ns)
      if (!pair) throw new Error(`missing database namespace ${testCase.ns}`)
      const official = await runOfficial(
        pair,
        corpus.schema,
        testCase.ast,
        testCase.format
      )
      const sqlite = runSqlite(
        pair,
        corpus.schema,
        testCase.ast,
        testCase.format,
        testCase.name
      )
      const label = `Chat case ${index + 1}/${corpus.cases.length} ${testCase.name} (${testCase.user}, ${testCase.ns})`
      compare(label, official, sqlite)
    }
  } finally {
    for (const pair of pairs.values()) {
      pair.sqlite.close()
      await pair.postgres.close()
    }
  }
}

async function runPatternAndNullCases(): Promise<void> {
  const schema: Schema = {
    tables: {
      person: {
        columns: {
          id: { type: 'string' },
          name: { type: 'string' },
        },
        primaryKey: ['id'],
      },
    },
  }
  const pair = await createDatabasePair(schema, [
    `INSERT INTO person (id, name) VALUES ('p1', 'Alice'), ('p2', 'alice'), ('p3', NULL)`,
  ])
  const cases = [
    { name: 'LIKE is case-sensitive', op: 'LIKE', value: 'A%' },
    { name: 'ILIKE is case-insensitive', op: 'ILIKE', value: 'A%' },
    { name: '= NULL uses three-valued logic', op: '=', value: null },
    { name: '!= NULL uses three-valued logic', op: '!=', value: null },
    { name: 'IS NULL is null-safe', op: 'IS', value: null },
    { name: 'IS NOT NULL is null-safe', op: 'IS NOT', value: null },
  ]
  try {
    for (const testCase of cases) {
      const ast = {
        table: 'person',
        where: {
          type: 'simple',
          op: testCase.op,
          left: { type: 'column', name: 'name' },
          right: { type: 'literal', value: testCase.value },
        },
        orderBy: [['id', 'asc']],
      }
      const format = { singular: false, relationships: {} }
      const official = await runOfficial(pair, schema, ast, format)
      const sqlite = runSqlite(pair, schema, ast, format, testCase.name)
      compare(testCase.name, official, sqlite)
    }
  } finally {
    pair.sqlite.close()
    await pair.postgres.close()
  }
}

initSync({ module: readFileSync(wasmPath) })
const started = performance.now()
await runChatCorpus()
await runPatternAndNullCases()
console.log(
  `[chat-transaction-query-diff] PASS: ${EXPECTED_CASES} harvested Chat cases across ${EXPECTED_QUERIES} queries plus 6 targeted semantics in ${Math.round(performance.now() - started)}ms`
)
