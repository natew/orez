import {
  asQueryInternals,
  consume,
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
  MemorySource,
  QueryDelegateBase,
  type CommitListener,
  type Source,
  type SourceChange,
} from '../zero/bindings.js'
import { createBuilder } from '../zero/zero.js'

import type {
  AST,
  Condition,
  HumanReadable,
  LocalSchemaMeta,
  LocalTableMeta,
  Query,
  Row,
  Schema,
  TableSchema,
  TypedView,
} from '../zero/zero.js'
import type { OneDataColumn, OneDataIndex } from './one-schema.js'

export type LocalWriteOp =
  | { kind: 'put'; table: string; key: string; row: Row }
  | { kind: 'delete'; table: string; key: string }

export type LocalPersistedColumnSnapshot = {
  kind: string
  optional: boolean
  default?: unknown
  unique?: true
}

export type LocalPersistedTableSnapshot = {
  columns: Record<string, LocalPersistedColumnSnapshot>
  primaryKey: readonly string[]
  indexes?: Readonly<Record<string, OneDataIndex>>
}

export type LocalPersistenceAdapter = {
  /** load persisted device tables, reconciling additive schema changes */
  open(
    tables: Readonly<Record<string, LocalTableMeta>>
  ): Promise<Readonly<Record<string, Row[]>>>
  /** commit one atomic batch of writes to persisted device tables */
  commit(ops: readonly LocalWriteOp[]): Promise<void>
  close?(): Promise<void>
  clear?(tables?: Readonly<Record<string, LocalTableMeta>>): Promise<void>
}

export type LocalSQLiteExecutor = {
  exec(sql: string): void | Promise<void>
  all(sql: string, params?: readonly unknown[]): Row[] | Promise<Row[]>
  run(sql: string, params?: readonly unknown[]): unknown | Promise<unknown>
}

export type LocalMutationResult = PromiseLike<void> & {
  readonly client: Promise<void>
}

type ColumnValueType<C> = C extends { customType: infer Custom }
  ? [Custom] extends [never]
    ? BaseColumnType<C>
    : unknown extends Custom
      ? BaseColumnType<C>
      : Custom extends null
        ? BaseColumnType<C>
        : Custom
  : BaseColumnType<C>

type BaseColumnType<C> = C extends { type: 'string' }
  ? string
  : C extends { type: 'number' }
    ? number
    : C extends { type: 'boolean' }
      ? boolean
      : C extends { type: 'json' }
        ? unknown
        : unknown

export type LocalRow<T extends TableSchema> = {
  [K in keyof T['columns']]: T['columns'][K] extends { optional: true }
    ? ColumnValueType<T['columns'][K]> | null
    : ColumnValueType<T['columns'][K]>
}

export type LocalInsertValue<T extends TableSchema> = {
  [K in keyof T['columns'] as T['columns'][K] extends { optional: true }
    ? never
    : K]: ColumnValueType<T['columns'][K]>
} & {
  [K in keyof T['columns'] as T['columns'][K] extends { optional: true } ? K : never]?:
    | ColumnValueType<T['columns'][K]>
    | null
    | undefined
}

export type LocalUpdateValue<T extends TableSchema> = {
  [K in keyof T['columns']]?: T['columns'][K] extends { optional: true }
    ? ColumnValueType<T['columns'][K]> | null | undefined
    : ColumnValueType<T['columns'][K]> | undefined
}

export type LocalDeleteID<T extends TableSchema> = {
  [K in keyof T['columns']]?: ColumnValueType<T['columns'][K]>
}

export type LocalTableMutator<T extends TableSchema = TableSchema> = {
  insert(row: LocalInsertValue<T>): LocalMutationResult
  upsert(row: LocalInsertValue<T>): LocalMutationResult
  update(row: LocalUpdateValue<T>): LocalMutationResult
  delete(key: LocalDeleteID<T>): LocalMutationResult
}

export type LocalMutators<S extends Schema = Schema> = {
  [K in keyof S['tables'] & string]: LocalTableMutator<S['tables'][K]>
}

export type LocalTxMutate<S extends Schema = Schema> = {
  [K in keyof S['tables'] & string]: {
    insert(row: LocalInsertValue<S['tables'][K]>): Promise<void>
    upsert(row: LocalInsertValue<S['tables'][K]>): Promise<void>
    update(row: LocalUpdateValue<S['tables'][K]>): Promise<void>
    delete(key: LocalDeleteID<S['tables'][K]>): Promise<void>
  }
}

export type LocalTransaction<S extends Schema = Schema> = {
  run<TTable extends keyof S['tables'] & string, TReturn>(
    query: Query<TTable, S, TReturn>
  ): Promise<HumanReadable<TReturn>>
  readonly mutate: LocalTxMutate<S>
}

export type LocalQueryClassification = 'local' | 'sync'

function collectConditionTables(condition: Condition | undefined, out: Set<string>) {
  if (!condition) return
  if (condition.type === 'correlatedSubquery') {
    collectAstTablesInto(condition.related.subquery, out)
    return
  }
  if (condition.type === 'and' || condition.type === 'or') {
    for (const child of condition.conditions) collectConditionTables(child, out)
  }
}

function collectAstTablesInto(ast: AST, out: Set<string>) {
  if (ast.table) out.add(ast.table)
  if (ast.where) collectConditionTables(ast.where, out)
  if (ast.related) {
    for (const rel of ast.related) {
      collectAstTablesInto(rel.subquery, out)
    }
  }
}

export function collectAstTables(ast: AST): Set<string> {
  const out = new Set<string>()
  collectAstTablesInto(ast, out)
  return out
}

export function classifyQueryAst(
  ast: AST,
  localTableNames: ReadonlySet<string>
): LocalQueryClassification {
  const tables = collectAstTables(ast)
  let hasLocal = false
  let hasSync = false
  for (const table of tables) {
    if (localTableNames.has(table)) hasLocal = true
    else hasSync = true
  }
  if (hasLocal && hasSync) {
    throw new Error(
      `[limesync] mixed local and synced query across [${[...tables].join(', ')}] is not supported in v1. ` +
        `Read the local table with useQuery and pass its id into an authorized named synced query.`
    )
  }
  return hasLocal ? 'local' : 'sync'
}

export function primaryKeyFor(
  table: LocalTableMeta,
  row: Record<string, unknown>
): string {
  const values = table.primaryKey.map((col) => {
    const val = row[col]
    if (val === undefined || val === null) {
      throw new Error(
        `[limesync] local table '${table.name}' requires primary key column '${col}'`
      )
    }
    return val
  })
  return JSON.stringify(values)
}

function cloneJsonValue(value: unknown, path: string): unknown {
  if (value === null) return null
  const type = typeof value
  if (type === 'string' || type === 'boolean') return value
  if (type === 'number') {
    if (!Number.isFinite(value)) {
      throw new Error(`[limesync] local column '${path}' cannot store non-finite number`)
    }
    return value
  }
  if (
    type === 'undefined' ||
    type === 'function' ||
    type === 'symbol' ||
    type === 'bigint'
  ) {
    throw new Error(`[limesync] local column '${path}' contains non-JSON value (${type})`)
  }
  if (Array.isArray(value)) {
    return value.map((item, idx) => cloneJsonValue(item, `${path}[${idx}]`))
  }
  if (
    Object.getPrototypeOf(value) !== Object.prototype &&
    Object.getPrototypeOf(value) !== null
  ) {
    // clone plain/structured data via JSON roundtrip check
    return structuredClone(value)
  }
  const copy: Record<string, unknown> = {}
  for (const [key, item] of Object.entries(value as Record<string, unknown>)) {
    if (item === undefined) continue
    copy[key] = cloneJsonValue(item, `${path}.${key}`)
  }
  return copy
}

function validateAndCloneColumnValue(
  tableName: string,
  colName: string,
  col: OneDataColumn,
  value: unknown
): unknown {
  if (value === null) {
    if (!col.optional) {
      throw new Error(
        `[limesync] local table '${tableName}' column '${colName}' is not optional`
      )
    }
    return null
  }
  switch (col.kind) {
    case 'string':
      if (typeof value !== 'string') {
        throw new Error(
          `[limesync] local table '${tableName}.${colName}' expects string, got ${typeof value}`
        )
      }
      return value
    case 'integer':
      if (typeof value !== 'number' || !Number.isInteger(value)) {
        throw new Error(
          `[limesync] local table '${tableName}.${colName}' expects integer, got ${String(value)}`
        )
      }
      return value
    case 'number':
    case 'timestamp':
      if (typeof value !== 'number' || !Number.isFinite(value)) {
        throw new Error(
          `[limesync] local table '${tableName}.${colName}' expects finite number, got ${String(value)}`
        )
      }
      return value
    case 'boolean':
      if (typeof value !== 'boolean') {
        throw new Error(
          `[limesync] local table '${tableName}.${colName}' expects boolean, got ${typeof value}`
        )
      }
      return value
    case 'json':
      return cloneJsonValue(value, `${tableName}.${colName}`)
  }
}

function cloneRow(table: LocalTableMeta, row: Record<string, unknown>): Row {
  const out: Record<string, unknown> = {}
  for (const [colName, col] of Object.entries(table.columns)) {
    const val = row[colName]
    out[colName] =
      val === undefined
        ? null
        : validateAndCloneColumnValue(table.name, colName, col, val)
  }
  return out as Row
}

function resolveDefault(
  tableName: string,
  colName: string,
  column: OneDataColumn
): unknown {
  if (column.default === 'now' && column.kind === 'timestamp') {
    return Date.now()
  }
  return validateAndCloneColumnValue(tableName, colName, column, column.default)
}

function isLiteralDefault(column: OneDataColumn): boolean {
  return column.default !== undefined && column.default !== 'now'
}

function normalizeInsertRow(table: LocalTableMeta, input: Record<string, unknown>): Row {
  if (!input || typeof input !== 'object') {
    throw new Error(`[limesync] local table '${table.name}' insert expects an object row`)
  }
  const out: Record<string, unknown> = {}
  for (const [colName, col] of Object.entries(table.columns)) {
    const raw = input[colName]
    if (raw !== undefined) {
      out[colName] = validateAndCloneColumnValue(table.name, colName, col, raw)
    } else if (col.default !== undefined) {
      out[colName] = resolveDefault(table.name, colName, col)
    } else if (col.optional) {
      out[colName] = null
    } else {
      throw new Error(
        `[limesync] local table '${table.name}' missing required column '${colName}'`
      )
    }
  }
  primaryKeyFor(table, out)
  return out as Row
}

function normalizeUpdateRow(
  table: LocalTableMeta,
  existing: Row,
  patch: Record<string, unknown>
): Row {
  if (!patch || typeof patch !== 'object') {
    throw new Error(
      `[limesync] local table '${table.name}' update expects an object patch`
    )
  }
  const out: Record<string, unknown> = cloneRow(table, existing)
  for (const [colName, col] of Object.entries(table.columns)) {
    if (!Object.hasOwn(patch, colName)) continue
    const raw = patch[colName]
    if (raw === undefined) continue
    out[colName] = validateAndCloneColumnValue(table.name, colName, col, raw)
  }
  primaryKeyFor(table, out)
  return out as Row
}

type SqlTriBool = boolean | null
type PartialWherePredicate = (row: Row) => boolean
type TriBoolPredicate = (row: Row) => SqlTriBool

type ParsedWhereLiteral = {
  value: string | number | boolean | null
  isRealLiteral: boolean
}

function parseWhereLiteral(raw: string): ParsedWhereLiteral {
  const trimmed = raw.trim()
  if (/^null$/i.test(trimmed)) return { value: null, isRealLiteral: false }
  if (/^true$/i.test(trimmed)) return { value: true, isRealLiteral: false }
  if (/^false$/i.test(trimmed)) return { value: false, isRealLiteral: false }
  if (/^-?\d+(?:\.\d+)?$/.test(trimmed)) {
    return { value: Number(trimmed), isRealLiteral: trimmed.includes('.') }
  }
  if (
    (trimmed.startsWith("'") && trimmed.endsWith("'")) ||
    (trimmed.startsWith('"') && trimmed.endsWith('"'))
  ) {
    const q = trimmed[0]!
    return { value: trimmed.slice(1, -1).replaceAll(q + q, q), isRealLiteral: false }
  }
  throw new Error(`unsupported literal '${raw}'`)
}

function resolveWhereColumnKey(
  table: LocalTableMeta,
  rawIdentifier: string
): string | null {
  const clean = rawIdentifier.trim().replace(/^["'`]|["'`]$/g, '')
  if (Object.hasOwn(table.columns, clean)) return clean
  for (const [colName, col] of Object.entries(table.columns)) {
    if (col.name === clean) return colName
  }
  return null
}

function formatSqliteRealAsText(num: number): string {
  return Number.isInteger(num) ? `${num}.0` : String(num)
}

function coerceSqliteValue(
  val: unknown,
  colKind: OneDataColumn['kind'],
  isRealLiteral = false
): string | number | null {
  if (val === null || val === undefined) return null
  // SQLite stores booleans as integer 0 / 1 before applying column affinity
  const normalized = typeof val === 'boolean' ? (val ? 1 : 0) : val
  if (colKind === 'string') {
    if (typeof normalized === 'string') return normalized
    if (typeof normalized === 'number') {
      return isRealLiteral ? formatSqliteRealAsText(normalized) : String(normalized)
    }
    return String(normalized)
  }
  if (
    colKind === 'integer' ||
    colKind === 'number' ||
    colKind === 'timestamp' ||
    colKind === 'boolean'
  ) {
    if (typeof normalized === 'number') return normalized
    if (typeof normalized === 'string') {
      const trimmed = normalized.trim()
      if (/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?$/i.test(trimmed)) {
        const num = Number(trimmed)
        if (Number.isFinite(num)) return num
      }
      return normalized
    }
  }
  return typeof normalized === 'string' || typeof normalized === 'number'
    ? normalized
    : String(normalized)
}

function sqliteTypeRank(val: string | number): number {
  return typeof val === 'number' ? 1 : 2
}

function compareWhereValue(
  rowVal: unknown,
  colKind: OneDataColumn['kind'],
  op: string,
  literal: ParsedWhereLiteral
): SqlTriBool {
  if (rowVal === null || rowVal === undefined || literal.value === null) return null
  const lhs = coerceSqliteValue(rowVal, colKind, false)
  const rhs = coerceSqliteValue(literal.value, colKind, literal.isRealLiteral)
  if (lhs === null || rhs === null) return null

  const lhsRank = sqliteTypeRank(lhs)
  const rhsRank = sqliteTypeRank(rhs)
  let cmp: number
  if (lhsRank !== rhsRank) {
    cmp = lhsRank - rhsRank
  } else if (lhs < rhs) {
    cmp = -1
  } else if (lhs > rhs) {
    cmp = 1
  } else {
    cmp = 0
  }

  switch (op) {
    case '=':
      return cmp === 0
    case '!=':
    case '<>':
      return cmp !== 0
    case '<':
      return cmp < 0
    case '<=':
      return cmp <= 0
    case '>':
      return cmp > 0
    case '>=':
      return cmp >= 0
    default:
      return null
  }
}

function sqlAnd(a: SqlTriBool, b: SqlTriBool): SqlTriBool {
  if (a === false || b === false) return false
  if (a === true && b === true) return true
  return null
}

function sqlOr(a: SqlTriBool, b: SqlTriBool): SqlTriBool {
  if (a === true || b === true) return true
  if (a === false && b === false) return false
  return null
}

function sqlNot(a: SqlTriBool): SqlTriBool {
  return a === null ? null : !a
}

function compilePartialWhere(
  table: LocalTableMeta,
  indexName: string,
  whereSql: string
): PartialWherePredicate {
  const src = whereSql.trim()
  let pos = 0

  const skipWs = () => {
    while (pos < src.length && /\s/.test(src[pos]!)) pos++
  }

  const peekWord = (): string => {
    skipWs()
    const match = /^[A-Za-z_][A-Za-z0-9_]*/.exec(src.slice(pos))
    return match ? match[0] : ''
  }

  const parseOr = (): TriBoolPredicate => {
    let left = parseAnd()
    while (true) {
      if (peekWord().toUpperCase() === 'OR') {
        skipWs()
        pos += 2
        const right = parseAnd()
        const prev = left
        left = (row) => sqlOr(prev(row), right(row))
        continue
      }
      break
    }
    return left
  }

  const parseAnd = (): TriBoolPredicate => {
    let left = parsePrimary()
    while (true) {
      if (peekWord().toUpperCase() === 'AND') {
        skipWs()
        pos += 3
        const right = parsePrimary()
        const prev = left
        left = (row) => sqlAnd(prev(row), right(row))
        continue
      }
      break
    }
    return left
  }

  const parsePrimary = (): TriBoolPredicate => {
    skipWs()
    if (src[pos] === '(') {
      pos++
      const inner = parseOr()
      skipWs()
      if (src[pos] !== ')') {
        throw new Error(`unclosed '(' in '${whereSql}'`)
      }
      pos++
      return inner
    }
    if (peekWord().toUpperCase() === 'NOT') {
      skipWs()
      pos += 3
      const inner = parsePrimary()
      return (row) => sqlNot(inner(row))
    }

    const colMatch = /^(?:"[^"]+"|`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)/.exec(src.slice(pos))
    if (!colMatch) {
      throw new Error(`expected column identifier at offset ${pos}`)
    }
    pos += colMatch[0].length
    const colKey = resolveWhereColumnKey(table, colMatch[0])
    if (!colKey) {
      throw new Error(`unknown column '${colMatch[0]}'`)
    }
    const colKind = table.columns[colKey]!.kind

    skipWs()
    const isNullMatch = /^IS\s+(NOT\s+)?NULL\b/i.exec(src.slice(pos))
    if (isNullMatch) {
      pos += isNullMatch[0].length
      const negated = Boolean(isNullMatch[1])
      return (row) => {
        const isNull = row[colKey] === null || row[colKey] === undefined
        return negated ? !isNull : isNull
      }
    }

    const opMatch = /^(<=|>=|!=|<>|=|<|>)/.exec(src.slice(pos))
    if (!opMatch) {
      throw new Error(`expected comparison operator at offset ${pos}`)
    }
    const op = opMatch[1]!
    pos += op.length
    skipWs()

    const litMatch =
      /^(?:'(?:''|[^'])*'|"(?:""|[^"])*"|-?\d+(?:\.\d+)?|true\b|false\b|null\b)/i.exec(
        src.slice(pos)
      )
    if (!litMatch) {
      throw new Error(`expected literal at offset ${pos}`)
    }
    pos += litMatch[0].length
    const literal = parseWhereLiteral(litMatch[0])
    return (row) => compareWhereValue(row[colKey], colKind, op, literal)
  }

  try {
    const predicate = parseOr()
    skipWs()
    if (pos !== src.length) {
      throw new Error(`unexpected token '${src.slice(pos)}'`)
    }
    // SQLite WHERE matches ONLY rows where the predicate evaluates to TRUE (not FALSE or NULL)
    return (row) => predicate(row) === true
  } catch (err) {
    throw new Error(
      `[limesync] unsupported partial unique index predicate on '${table.name}.${indexName}' (${whereSql}): ${err instanceof Error ? err.message : String(err)}`
    )
  }
}

function uniqueConstraintGroups(
  table: LocalTableMeta
): Array<{ label: string; columns: readonly string[]; where?: PartialWherePredicate }> {
  const groups: Array<{
    label: string
    columns: readonly string[]
    where?: PartialWherePredicate
  }> = []
  for (const [colName, col] of Object.entries(table.columns)) {
    if (col.unique) {
      groups.push({ label: `${table.name}.${colName}`, columns: [colName] })
    }
  }
  if (table.indexes) {
    for (const [indexName, idx] of Object.entries(table.indexes)) {
      if (idx.unique) {
        groups.push({
          label: `${table.name}.${indexName}`,
          columns: idx.columns,
          ...(idx.where
            ? { where: compilePartialWhere(table, indexName, idx.where) }
            : {}),
        })
      }
    }
  }
  return groups
}

function assertUniqueConstraints(table: LocalTableMeta, rows: Iterable<Row>) {
  const groups = uniqueConstraintGroups(table)
  if (groups.length === 0) return
  const seenSets = groups.map(() => new Set<string>())
  for (const row of rows) {
    for (let i = 0; i < groups.length; i++) {
      const group = groups[i]!
      if (group.where && !group.where(row)) continue
      const vals = group.columns.map((c) => row[c])
      // SQLite/SQL UNIQUE allows multiple NULLs
      if (vals.some((v) => v === null || v === undefined)) continue
      const key = JSON.stringify(vals)
      if (seenSets[i]!.has(key)) {
        throw new Error(
          `[limesync] unique constraint violated on '${group.label}': ${key}`
        )
      }
      seenSets[i]!.add(key)
    }
  }
}

function assertForeignKeys(
  tables: Readonly<Record<string, LocalTableMeta>>,
  getRowsForTable: (tableName: string) => ReadonlyMap<string, Row>,
  dirtyTables: ReadonlySet<string>
) {
  for (const tableName of dirtyTables) {
    const meta = tables[tableName]
    if (!meta) continue
    const fkCols = Object.entries(meta.columns).filter(([, col]) =>
      Boolean(col.references)
    )
    if (fkCols.length === 0) continue
    const sourceRows = getRowsForTable(tableName)
    for (const [colName, col] of fkCols) {
      const ref = col.references!
      const targetMeta = tables[ref.table]
      if (!targetMeta) {
        throw new Error(
          `[limesync] local foreign key '${tableName}.${colName}' references unknown local table '${ref.table}'`
        )
      }
      const targetRows = getRowsForTable(ref.table)
      const targetValues = new Set<string>()
      for (const targetRow of targetRows.values()) {
        const tv = targetRow[ref.column]
        if (tv !== null && tv !== undefined) targetValues.add(JSON.stringify(tv))
      }
      for (const row of sourceRows.values()) {
        const val = row[colName]
        if (val === null || val === undefined) continue
        if (!targetValues.has(JSON.stringify(val))) {
          throw new Error(
            `[limesync] foreign key constraint violated on '${tableName}.${colName}' -> '${ref.table}.${ref.column}': missing ${JSON.stringify(val)}`
          )
        }
      }
    }
  }
  // also verify no remaining referencing rows in non-dirty tables point to deleted rows in dirty tables
  for (const [sourceName, sourceMeta] of Object.entries(tables)) {
    if (dirtyTables.has(sourceName)) continue
    for (const [colName, col] of Object.entries(sourceMeta.columns)) {
      const ref = col.references
      if (!ref || !dirtyTables.has(ref.table)) continue
      const targetRows = getRowsForTable(ref.table)
      const targetValues = new Set<string>()
      for (const targetRow of targetRows.values()) {
        const tv = targetRow[ref.column]
        if (tv !== null && tv !== undefined) targetValues.add(JSON.stringify(tv))
      }
      for (const row of getRowsForTable(sourceName).values()) {
        const val = row[colName]
        if (val === null || val === undefined) continue
        if (!targetValues.has(JSON.stringify(val))) {
          throw new Error(
            `[limesync] foreign key constraint violated: '${sourceName}.${colName}' still references deleted '${ref.table}.${ref.column}' = ${JSON.stringify(val)}`
          )
        }
      }
    }
  }
}

function reconcileSchemaChange(
  tableName: string,
  previous: LocalPersistedTableSnapshot,
  next: LocalTableMeta,
  rows: Row[]
): { rows: Row[]; changed: boolean } {
  if (
    previous.primaryKey.length !== next.primaryKey.length ||
    previous.primaryKey.some((pk, idx) => pk !== next.primaryKey[idx])
  ) {
    throw new Error(
      `[limesync] incompatible local schema change on '${tableName}': primary key changed. Existing data was preserved.`
    )
  }
  const addedDefaults: Array<[string, OneDataColumn]> = []
  for (const [colName, nextCol] of Object.entries(next.columns)) {
    const prevCol = previous.columns[colName]
    if (!prevCol) {
      if (!nextCol.optional && !isLiteralDefault(nextCol)) {
        throw new Error(
          `[limesync] incompatible local schema change on '${tableName}.${colName}': new required column needs a literal default or .optional(). Existing data was preserved.`
        )
      }
      addedDefaults.push([colName, nextCol])
    } else {
      if (prevCol.kind !== nextCol.kind) {
        throw new Error(
          `[limesync] incompatible local schema change on '${tableName}.${colName}': column kind changed (${prevCol.kind} -> ${nextCol.kind}). Existing data was preserved.`
        )
      }
      if (prevCol.optional && !nextCol.optional) {
        throw new Error(
          `[limesync] incompatible local schema change on '${tableName}.${colName}': column nullability tightened from optional to required. Existing data was preserved.`
        )
      }
    }
  }

  let nextRows = rows
  let changed = false
  if (addedDefaults.length > 0) {
    changed = true
    nextRows = rows.map((row) => {
      const nextRow: Record<string, unknown> = { ...row }
      for (const [colName, col] of addedDefaults) {
        if (nextRow[colName] === undefined) {
          nextRow[colName] =
            col.default !== undefined ? resolveDefault(tableName, colName, col) : null
        }
      }
      return nextRow as Row
    })
  }

  // validate every reconciled row against the new schema + unique constraints before writing
  const validated = nextRows.map((row) => cloneRow(next, row))
  assertUniqueConstraints(next, validated)
  return { rows: validated, changed }
}

function snapshotOf(table: LocalTableMeta): LocalPersistedTableSnapshot {
  return {
    primaryKey: [...table.primaryKey],
    columns: Object.fromEntries(
      Object.entries(table.columns).map(([name, col]) => [
        name,
        {
          kind: col.kind,
          optional: col.optional,
          ...(col.default !== undefined ? { default: col.default } : {}),
          ...(col.unique ? { unique: true as const } : {}),
        },
      ])
    ),
    ...(table.indexes && Object.keys(table.indexes).length > 0
      ? { indexes: table.indexes }
      : {}),
  }
}

export function createMemoryLocalPersistence(): LocalPersistenceAdapter {
  const rowsByTable = new Map<string, Map<string, Row>>()
  const metaByTable = new Map<string, LocalPersistedTableSnapshot>()

  return {
    async open(tables) {
      const staged: Array<{
        name: string
        meta: LocalTableMeta
        rows: Row[]
      }> = []
      // reconcile ALL tables before mutating any state so an error on table 2 leaves table 1 untouched
      for (const [name, meta] of Object.entries(tables)) {
        if (meta.persistence !== 'device') continue
        const existingMap = rowsByTable.get(name)
        const existingMeta = metaByTable.get(name)
        const existingRows = existingMap
          ? [...existingMap.values()].map((r) => structuredClone(r))
          : []
        const reconciled = existingMeta
          ? reconcileSchemaChange(name, existingMeta, meta, existingRows).rows
          : existingRows.map((r) => cloneRow(meta, r))
        staged.push({ name, meta, rows: reconciled })
      }
      const out: Record<string, Row[]> = {}
      for (const { name, meta, rows } of staged) {
        metaByTable.set(name, snapshotOf(meta))
        const nextMap = new Map<string, Row>()
        for (const r of rows) nextMap.set(primaryKeyFor(meta, r), structuredClone(r))
        rowsByTable.set(name, nextMap)
        out[name] = rows
      }
      return out
    },
    async commit(ops) {
      const cloned = ops.map((op) =>
        op.kind === 'put' ? { ...op, row: structuredClone(op.row) } : op
      )
      for (const op of cloned) {
        let map = rowsByTable.get(op.table)
        if (!map) {
          map = new Map()
          rowsByTable.set(op.table, map)
        }
        if (op.kind === 'put') map.set(op.key, op.row)
        else map.delete(op.key)
      }
    },
    async clear(tables) {
      rowsByTable.clear()
      metaByTable.clear()
      if (tables) {
        for (const [name, meta] of Object.entries(tables)) {
          if (meta.persistence === 'device') {
            metaByTable.set(name, snapshotOf(meta))
            rowsByTable.set(name, new Map())
          }
        }
      }
    },
  }
}

const IDB_META_STORE = '__meta'
const IDB_ROWS_STORE = '__rows'
const IDB_REVISION_KEY = '__limesync_store_revision__'

export function createIndexedDBLocalPersistence(
  storageKey: string
): LocalPersistenceAdapter {
  const dbName = `limesync-local-${storageKey}`
  let dbPromise: Promise<IDBDatabase> | null = null
  let knownRevision = 0

  const openDb = (): Promise<IDBDatabase> => {
    if (dbPromise) return dbPromise
    const pending = new Promise<IDBDatabase>((resolve, reject) => {
      try {
        const req = indexedDB.open(dbName, 1)
        req.onupgradeneeded = () => {
          const db = req.result
          if (!db.objectStoreNames.contains(IDB_META_STORE)) {
            db.createObjectStore(IDB_META_STORE)
          }
          if (!db.objectStoreNames.contains(IDB_ROWS_STORE)) {
            db.createObjectStore(IDB_ROWS_STORE)
          }
        }
        req.onsuccess = () => resolve(req.result)
        req.onerror = () => {
          dbPromise = null
          reject(req.error ?? new Error(`Failed to open ${dbName}`))
        }
        req.onblocked = () => {
          dbPromise = null
          reject(new Error(`Opening ${dbName} was blocked`))
        }
      } catch (err) {
        dbPromise = null
        reject(err)
      }
    })
    dbPromise = pending
    return pending
  }

  return {
    async open(tables) {
      if (typeof indexedDB === 'undefined') {
        throw new Error(
          `[limesync] IndexedDB is not available for device persistence '${storageKey}'.`
        )
      }
      const db = await openDb()
      const deviceTables = Object.entries(tables).filter(
        ([, meta]) => meta.persistence === 'device'
      )

      return new Promise<Readonly<Record<string, Row[]>>>((resolve, reject) => {
        let settled = false
        const fail = (tx: IDBTransaction, err: unknown) => {
          if (settled) return
          settled = true
          try {
            tx.abort()
          } catch {}
          reject(err)
        }

        const tx = db.transaction([IDB_META_STORE, IDB_ROWS_STORE], 'readwrite')
        const metaStore = tx.objectStore(IDB_META_STORE)
        const rowsStore = tx.objectStore(IDB_ROWS_STORE)

        const out: Record<string, Row[]> = {}
        let nextRevision = 0

        tx.oncomplete = () => {
          if (settled) return
          settled = true
          knownRevision = nextRevision
          resolve(out)
        }
        tx.onerror = () => {
          if (settled) return
          settled = true
          reject(tx.error ?? new Error('IndexedDB transaction failed'))
        }
        tx.onabort = () => {
          if (settled) return
          settled = true
          reject(tx.error ?? new Error('IndexedDB transaction aborted'))
        }

        // read all keys, values, and table metadata via IDB callbacks inside the same active transaction
        let allRowKeys: IDBValidKey[] | null = null
        let allRowValues: unknown[] | null = null
        let allMetaKeys: IDBValidKey[] | null = null
        let allMetaValues: unknown[] | null = null

        const tryReconcileAndWrite = () => {
          if (
            allRowKeys === null ||
            allRowValues === null ||
            allMetaKeys === null ||
            allMetaValues === null
          ) {
            return
          }
          try {
            const metaByName = new Map<string, unknown>()
            for (let i = 0; i < allMetaKeys.length; i++) {
              metaByName.set(String(allMetaKeys[i]), allMetaValues[i])
            }
            const storedRev = metaByName.get(IDB_REVISION_KEY)
            nextRevision = typeof storedRev === 'number' ? storedRev : 0

            const rowsByTable = new Map<string, Row[]>()
            for (let i = 0; i < allRowKeys.length; i++) {
              const compositeKey = String(allRowKeys[i])
              const sep = compositeKey.indexOf('\0')
              if (sep < 0) continue
              const tableName = compositeKey.slice(0, sep)
              const list = rowsByTable.get(tableName) ?? []
              list.push(allRowValues[i] as Row)
              rowsByTable.set(tableName, list)
            }

            // validate & reconcile ALL tables before issuing ANY writes
            const stagedWrites: Array<{
              name: string
              meta: LocalTableMeta
              rows: Row[]
              changed: boolean
            }> = []
            for (const [name, meta] of deviceTables) {
              const prevMeta = metaByName.get(name) as
                | LocalPersistedTableSnapshot
                | undefined
              const existing = rowsByTable.get(name) ?? []
              const result = prevMeta
                ? reconcileSchemaChange(name, prevMeta, meta, existing)
                : { rows: existing.map((r) => cloneRow(meta, r)), changed: false }
              stagedWrites.push({
                name,
                meta,
                rows: result.rows,
                changed: result.changed,
              })
            }

            // all tables reconciled cleanly — write backfills first, then metadata
            let anyBackfilled = false
            for (const { name, meta, rows, changed } of stagedWrites) {
              if (changed) {
                anyBackfilled = true
                for (const row of rows) {
                  rowsStore.put(row, `${name}\0${primaryKeyFor(meta, row)}`)
                }
              }
              metaStore.put(snapshotOf(meta), name)
              out[name] = rows
            }
            if (anyBackfilled) {
              nextRevision = nextRevision + 1
              metaStore.put(nextRevision, IDB_REVISION_KEY)
            } else if (storedRev === undefined) {
              metaStore.put(nextRevision, IDB_REVISION_KEY)
            }
          } catch (err) {
            fail(tx, err)
          }
        }

        try {
          const keysReq = rowsStore.getAllKeys()
          keysReq.onsuccess = () => {
            allRowKeys = keysReq.result
            tryReconcileAndWrite()
          }
          keysReq.onerror = () => fail(tx, keysReq.error)

          const valsReq = rowsStore.getAll()
          valsReq.onsuccess = () => {
            allRowValues = valsReq.result
            tryReconcileAndWrite()
          }
          valsReq.onerror = () => fail(tx, valsReq.error)

          const metaKeysReq = metaStore.getAllKeys()
          metaKeysReq.onsuccess = () => {
            allMetaKeys = metaKeysReq.result
            tryReconcileAndWrite()
          }
          metaKeysReq.onerror = () => fail(tx, metaKeysReq.error)

          const metaValsReq = metaStore.getAll()
          metaValsReq.onsuccess = () => {
            allMetaValues = metaValsReq.result
            tryReconcileAndWrite()
          }
          metaValsReq.onerror = () => fail(tx, metaValsReq.error)
        } catch (err) {
          fail(tx, err)
        }
      })
    },

    async commit(ops) {
      if (ops.length === 0) return
      if (typeof indexedDB === 'undefined') {
        throw new Error(
          `[limesync] IndexedDB is not available for device persistence '${storageKey}'.`
        )
      }
      // pre-clone & validate entire batch up front before opening the IDB transaction
      const clonedOps = ops.map((op) =>
        op.kind === 'put' ? { ...op, row: structuredClone(op.row) } : op
      )
      const db = await openDb()

      return new Promise<void>((resolve, reject) => {
        let settled = false
        const fail = (tx: IDBTransaction, err: unknown) => {
          if (settled) return
          settled = true
          try {
            tx.abort()
          } catch {}
          reject(err)
        }

        const tx = db.transaction([IDB_META_STORE, IDB_ROWS_STORE], 'readwrite')
        const metaStore = tx.objectStore(IDB_META_STORE)
        const rowsStore = tx.objectStore(IDB_ROWS_STORE)
        const nextRevision = knownRevision + 1

        tx.oncomplete = () => {
          if (settled) return
          settled = true
          knownRevision = nextRevision
          resolve()
        }
        tx.onerror = () => {
          if (settled) return
          settled = true
          reject(tx.error ?? new Error('IndexedDB transaction failed'))
        }
        tx.onabort = () => {
          if (settled) return
          settled = true
          reject(tx.error ?? new Error('IndexedDB transaction aborted'))
        }

        try {
          const revReq = metaStore.get(IDB_REVISION_KEY)
          revReq.onsuccess = () => {
            try {
              const currentRev = typeof revReq.result === 'number' ? revReq.result : 0
              if (currentRev !== knownRevision) {
                fail(
                  tx,
                  new Error(
                    `[limesync] concurrent local store modification detected on '${dbName}' (expected revision ${knownRevision}, found ${currentRev}). Reopen the store before retrying.`
                  )
                )
                return
              }
              for (const op of clonedOps) {
                const compositeKey = `${op.table}\0${op.key}`
                if (op.kind === 'put') rowsStore.put(op.row, compositeKey)
                else rowsStore.delete(compositeKey)
              }
              metaStore.put(nextRevision, IDB_REVISION_KEY)
            } catch (err) {
              fail(tx, err)
            }
          }
          revReq.onerror = () => fail(tx, revReq.error)
        } catch (err) {
          fail(tx, err)
        }
      })
    },

    async close() {
      if (!dbPromise) return
      const pending = dbPromise
      dbPromise = null
      try {
        const db = await pending
        db.close()
      } catch {
        // already failed to open
      }
    },

    async clear(tables) {
      if (typeof indexedDB === 'undefined') return
      const db = await openDb()
      await new Promise<void>((resolve, reject) => {
        let settled = false
        const fail = (tx: IDBTransaction, err: unknown) => {
          if (settled) return
          settled = true
          try {
            tx.abort()
          } catch {}
          reject(err)
        }
        const tx = db.transaction([IDB_META_STORE, IDB_ROWS_STORE], 'readwrite')
        const metaStore = tx.objectStore(IDB_META_STORE)
        const rowsStore = tx.objectStore(IDB_ROWS_STORE)
        let nextRev = knownRevision + 1

        tx.oncomplete = () => {
          if (settled) return
          settled = true
          knownRevision = nextRev
          resolve()
        }
        tx.onerror = () => {
          if (settled) return
          settled = true
          reject(tx.error ?? new Error('IndexedDB clear transaction failed'))
        }
        tx.onabort = () => {
          if (settled) return
          settled = true
          reject(tx.error ?? new Error('IndexedDB clear transaction aborted'))
        }

        try {
          const revReq = metaStore.get(IDB_REVISION_KEY)
          revReq.onsuccess = () => {
            try {
              const currentRev = typeof revReq.result === 'number' ? revReq.result : 0
              nextRev = Math.max(currentRev, knownRevision) + 1
              rowsStore.clear()
              metaStore.clear()
              if (tables) {
                for (const [name, meta] of Object.entries(tables)) {
                  if (meta.persistence === 'device') {
                    metaStore.put(snapshotOf(meta), name)
                  }
                }
              }
              metaStore.put(nextRev, IDB_REVISION_KEY)
            } catch (err) {
              fail(tx, err)
            }
          }
          revReq.onerror = () => fail(tx, revReq.error)
        } catch (err) {
          fail(tx, err)
        }
      })
    },
  }
}

const SQLITE_REVISION_ROW = '__limesync_store_revision__'

export type SharedSQLiteLocalPersistence = LocalPersistenceAdapter & {
  /** serialize external SQLite transactions on the same connection */
  runExclusive<T>(work: () => Promise<T> | T): Promise<T>
}

/**
 * Shared-SQLite persistence adapter for native and Bun runtimes.
 * Stores local device tables inside the app's single SQLite database under
 * `limesync_local_meta` and `limesync_local_rows` so sync cache resets never
 * touch local rows and no secondary SQLite file or ATTACH is needed.
 */
export function createSQLiteLocalPersistence(
  db: LocalSQLiteExecutor
): SharedSQLiteLocalPersistence {
  let queue: Promise<unknown> = Promise.resolve()
  let knownRevision = 0

  const runExclusive = <T>(work: () => Promise<T> | T): Promise<T> => {
    const next = queue.then(work)
    queue = next.catch(() => {})
    return next
  }

  const ensureTables = async () => {
    await db.exec(
      `CREATE TABLE IF NOT EXISTS limesync_local_meta (table_name TEXT PRIMARY KEY, schema_json TEXT NOT NULL);
       CREATE TABLE IF NOT EXISTS limesync_local_rows (table_name TEXT NOT NULL, row_key TEXT NOT NULL, row_json TEXT NOT NULL, PRIMARY KEY (table_name, row_key));`
    )
  }

  const inExclusiveTransaction = async <T>(work: () => Promise<T>): Promise<T> => {
    await ensureTables()
    await db.exec('BEGIN IMMEDIATE')
    try {
      const value = await work()
      await db.exec('COMMIT')
      return value
    } catch (error) {
      try {
        await db.exec('ROLLBACK')
      } catch {}
      throw error
    }
  }

  return {
    runExclusive,

    open(tables) {
      return runExclusive(async () => {
        const { out, nextRevision } = await inExclusiveTransaction(async () => {
          const revRows = await db.all(
            'SELECT schema_json FROM limesync_local_meta WHERE table_name = ?',
            [SQLITE_REVISION_ROW]
          )
          const currentRevision = revRows[0]?.schema_json
            ? Number(JSON.parse(String(revRows[0].schema_json)))
            : 0
          const deviceTables = Object.entries(tables).filter(
            ([, meta]) => meta.persistence === 'device'
          )

          // read and reconcile ALL tables first so any failure rolls back cleanly
          const staged: Array<{
            name: string
            meta: LocalTableMeta
            rows: Row[]
            changed: boolean
          }> = []
          for (const [name, meta] of deviceTables) {
            const metaRows = await db.all(
              'SELECT schema_json FROM limesync_local_meta WHERE table_name = ?',
              [name]
            )
            const prevMeta = metaRows[0]?.schema_json
              ? (JSON.parse(
                  String(metaRows[0].schema_json)
                ) as LocalPersistedTableSnapshot)
              : undefined
            const storedRows = await db.all(
              'SELECT row_json FROM limesync_local_rows WHERE table_name = ?',
              [name]
            )
            const parsed = storedRows.map((r) => JSON.parse(String(r.row_json)) as Row)
            const result = prevMeta
              ? reconcileSchemaChange(name, prevMeta, meta, parsed)
              : { rows: parsed.map((r) => cloneRow(meta, r)), changed: false }
            staged.push({ name, meta, rows: result.rows, changed: result.changed })
          }

          // write backfilled rows FIRST, then update schema metadata inside the same transaction
          const out: Record<string, Row[]> = {}
          let anyBackfilled = false
          for (const { name, meta, rows, changed } of staged) {
            if (changed) {
              anyBackfilled = true
              for (const row of rows) {
                await db.run(
                  'INSERT OR REPLACE INTO limesync_local_rows (table_name, row_key, row_json) VALUES (?, ?, ?)',
                  [name, primaryKeyFor(meta, row), JSON.stringify(row)]
                )
              }
            }
            await db.run(
              'INSERT OR REPLACE INTO limesync_local_meta (table_name, schema_json) VALUES (?, ?)',
              [name, JSON.stringify(snapshotOf(meta))]
            )
            out[name] = rows
          }
          const nextRevision = anyBackfilled ? currentRevision + 1 : currentRevision
          if (anyBackfilled || revRows.length === 0) {
            await db.run(
              'INSERT OR REPLACE INTO limesync_local_meta (table_name, schema_json) VALUES (?, ?)',
              [SQLITE_REVISION_ROW, JSON.stringify(nextRevision)]
            )
          }
          return { out, nextRevision }
        })
        // publish revision ONLY after COMMIT succeeds
        knownRevision = nextRevision
        return out
      })
    },

    commit(ops) {
      if (ops.length === 0) return Promise.resolve()
      const serialized = ops.map((op) =>
        op.kind === 'put'
          ? {
              kind: 'put' as const,
              table: op.table,
              key: op.key,
              json: JSON.stringify(op.row),
            }
          : op
      )
      return runExclusive(async () => {
        const nextRevision = await inExclusiveTransaction(async () => {
          const revRows = await db.all(
            'SELECT schema_json FROM limesync_local_meta WHERE table_name = ?',
            [SQLITE_REVISION_ROW]
          )
          const currentRev = revRows[0]?.schema_json
            ? Number(JSON.parse(String(revRows[0].schema_json)))
            : 0
          if (currentRev !== knownRevision) {
            throw new Error(
              `[limesync] concurrent local SQLite modification detected (expected revision ${knownRevision}, found ${currentRev}). Reopen the store before retrying.`
            )
          }
          for (const op of serialized) {
            if (op.kind === 'put') {
              await db.run(
                'INSERT OR REPLACE INTO limesync_local_rows (table_name, row_key, row_json) VALUES (?, ?, ?)',
                [op.table, op.key, op.json]
              )
            } else {
              await db.run(
                'DELETE FROM limesync_local_rows WHERE table_name = ? AND row_key = ?',
                [op.table, op.key]
              )
            }
          }
          const nextRev = knownRevision + 1
          await db.run(
            'INSERT OR REPLACE INTO limesync_local_meta (table_name, schema_json) VALUES (?, ?)',
            [SQLITE_REVISION_ROW, JSON.stringify(nextRev)]
          )
          return nextRev
        })
        knownRevision = nextRevision
      })
    },

    clear(tables) {
      return runExclusive(async () => {
        const nextRevision = await inExclusiveTransaction(async () => {
          const revRows = await db.all(
            'SELECT schema_json FROM limesync_local_meta WHERE table_name = ?',
            [SQLITE_REVISION_ROW]
          )
          const currentRev = revRows[0]?.schema_json
            ? Number(JSON.parse(String(revRows[0].schema_json)))
            : 0
          const nextRev = Math.max(currentRev, knownRevision) + 1
          await db.exec(
            'DELETE FROM limesync_local_rows; DELETE FROM limesync_local_meta;'
          )
          if (tables) {
            for (const [name, meta] of Object.entries(tables)) {
              if (meta.persistence === 'device') {
                await db.run(
                  'INSERT OR REPLACE INTO limesync_local_meta (table_name, schema_json) VALUES (?, ?)',
                  [name, JSON.stringify(snapshotOf(meta))]
                )
              }
            }
          }
          await db.run(
            'INSERT OR REPLACE INTO limesync_local_meta (table_name, schema_json) VALUES (?, ?)',
            [SQLITE_REVISION_ROW, JSON.stringify(nextRev)]
          )
          return nextRev
        })
        knownRevision = nextRevision
      })
    },
  }
}

class LocalQueryDelegate extends QueryDelegateBase {
  readonly #sources: Map<string, MemorySource>
  readonly #commitListeners = new Set<CommitListener>()
  #ready: boolean

  constructor(sources: Map<string, MemorySource>, ready: boolean) {
    super()
    this.#sources = sources
    this.#ready = ready
  }

  get defaultQueryComplete(): boolean {
    return this.#ready
  }

  setReady(ready: boolean) {
    this.#ready = ready
  }

  getSource(name: string): Source | undefined {
    return this.#sources.get(name)
  }

  override onTransactionCommit(cb: CommitListener): () => void {
    this.#commitListeners.add(cb)
    return () => {
      this.#commitListeners.delete(cb)
    }
  }

  notifyCommit() {
    for (const cb of [...this.#commitListeners]) {
      try {
        cb()
      } catch (error) {
        console.error('[limesync] local query commit listener failed:', error)
      }
    }
  }
}

function makeLocalMutationResult(promise: Promise<void>): LocalMutationResult {
  const result = {
    client: promise,
    then: promise.then.bind(promise),
  } as LocalMutationResult
  return result
}

export class LocalDataStore<S extends Schema = Schema> {
  readonly schema: S
  readonly tables: Readonly<Record<string, LocalTableMeta>>
  readonly tableNames: ReadonlySet<string>
  readonly #hasDeviceTables: boolean
  readonly #sources = new Map<string, MemorySource>()
  readonly #rows = new Map<string, Map<string, Row>>()
  readonly #delegate: LocalQueryDelegate
  readonly #persistence: LocalPersistenceAdapter
  readonly #readyListeners = new Set<() => void>()
  readonly #queryBuilder: ReturnType<typeof createBuilder<S>>
  #ready = false
  #version = 0
  #openPromise: Promise<void> | null = null
  #opQueue: Promise<unknown> = Promise.resolve()
  #lifecycleEpoch = 0
  #closing = false
  #closed = false
  #closePromise: Promise<void> | null = null

  constructor(
    meta: LocalSchemaMeta<S>,
    persistence: LocalPersistenceAdapter = createMemoryLocalPersistence()
  ) {
    this.schema = meta.schema
    this.tables = meta.tables
    this.tableNames = new Set(Object.keys(meta.tables))
    this.#persistence = persistence

    this.#hasDeviceTables = Object.values(meta.tables).some(
      (t) => t.persistence === 'device'
    )
    this.#ready = !this.#hasDeviceTables

    for (const [name, tableSchema] of Object.entries(meta.schema.tables)) {
      this.#sources.set(
        name,
        new MemorySource(name, tableSchema.columns, tableSchema.primaryKey)
      )
      this.#rows.set(name, new Map())
    }

    this.#delegate = new LocalQueryDelegate(this.#sources, this.#ready)
    this.#queryBuilder = createBuilder(meta.schema)
  }

  get ready(): boolean {
    return this.#ready
  }

  get version(): number {
    return this.#version
  }

  get closed(): boolean {
    return this.#closing || this.#closed
  }

  get query() {
    return this.#queryBuilder
  }

  subscribeReady(listener: () => void): () => void {
    this.#readyListeners.add(listener)
    return () => {
      this.#readyListeners.delete(listener)
    }
  }

  #enqueue<T>(work: () => Promise<T>): Promise<T> {
    if (this.#closing || this.#closed) {
      return Promise.reject(new Error('[limesync] local data store is closed'))
    }
    const queued = this.#opQueue.then(async () => {
      // ops already accepted before close() must drain to completion; only
      // reject if the store has already finished closing.
      if (this.#closed) {
        throw new Error('[limesync] local data store is closed')
      }
      return work()
    })
    this.#opQueue = queued.catch(() => {})
    return queued
  }

  async #hydrateUnlocked(expectedEpoch: number): Promise<void> {
    if (this.#ready || !this.#hasDeviceTables) return
    if (this.#lifecycleEpoch !== expectedEpoch) return
    const loaded = await this.#persistence.open(this.tables)
    // discard stale hydration if close()/reopen() advanced the lifecycle epoch while open() was pending
    if (this.#lifecycleEpoch !== expectedEpoch) return
    // validate all loaded rows before mutating any live state so a partial failure never leaks
    const validatedByTable = new Map<string, Map<string, Row>>()
    for (const [tableName, meta] of Object.entries(this.tables)) {
      if (meta.persistence !== 'device') continue
      const rows = loaded[tableName] ?? []
      const nextMap = new Map<string, Row>()
      for (const row of rows) {
        const cloned = cloneRow(meta, row)
        const pk = primaryKeyFor(meta, cloned)
        nextMap.set(pk, cloned)
      }
      validatedByTable.set(tableName, nextMap)
    }

    // reconcile live device maps against freshly loaded disk state (handles
    // both initial hydration and reopen() after external updates or deletions)
    for (const [tableName, loadedMap] of validatedByTable) {
      const source = this.#sources.get(tableName)
      const liveMap = this.#rows.get(tableName)
      if (!source || !liveMap) continue

      for (const [pk, prevRow] of [...liveMap.entries()]) {
        if (!loadedMap.has(pk)) {
          liveMap.delete(pk)
          consume(source.push(makeSourceChangeRemove(prevRow)))
        }
      }
      for (const [pk, nextRow] of loadedMap) {
        const prevRow = liveMap.get(pk)
        if (!prevRow) {
          liveMap.set(pk, nextRow)
          consume(source.push(makeSourceChangeAdd(nextRow)))
        } else {
          liveMap.set(pk, nextRow)
          consume(source.push(makeSourceChangeEdit(nextRow, prevRow)))
        }
      }
    }
    this.#ready = true
    this.#version++
    this.#delegate.setReady(true)
    this.#delegate.notifyCommit()
    for (const listener of [...this.#readyListeners]) {
      try {
        listener()
      } catch (error) {
        console.error('[limesync] local ready listener failed:', error)
      }
    }
  }

  open(): Promise<void> {
    if (this.#closing || this.#closed) {
      return Promise.reject(new Error('[limesync] local data store is closed'))
    }
    if (this.#ready) return Promise.resolve()
    if (this.#openPromise) return this.#openPromise
    const epoch = this.#lifecycleEpoch
    const pending = this.#enqueue(() => this.#hydrateUnlocked(epoch)).catch((error) => {
      if (this.#openPromise === pending) {
        this.#openPromise = null
      }
      throw error
    })
    this.#openPromise = pending
    return pending
  }

  flushPersistence(): Promise<void> {
    return this.#opQueue.then(() => undefined)
  }

  materialize<TTable extends keyof S['tables'] & string, TReturn>(
    q: Query<TTable, S, TReturn>
  ): TypedView<HumanReadable<TReturn>> {
    const internals = asQueryInternals(q)
    classifyQueryAst(internals.ast, this.tableNames)
    return this.#delegate.materialize(q)
  }

  /** side-effect-free synchronous snapshot read for render-phase hooks */
  readSnapshot<TTable extends keyof S['tables'] & string, TReturn>(
    q: Query<TTable, S, TReturn>
  ): HumanReadable<TReturn> {
    const view = this.materialize(q)
    try {
      return view.data
    } finally {
      view.destroy()
    }
  }

  async run<TTable extends keyof S['tables'] & string, TReturn>(
    q: Query<TTable, S, TReturn>
  ): Promise<HumanReadable<TReturn>> {
    const internals = asQueryInternals(q)
    classifyQueryAst(internals.ast, this.tableNames)
    if (!this.#ready && this.#hasDeviceTables) {
      await this.open()
    }
    return this.#delegate.run(q)
  }

  #assertLocalTable(tableName: string): LocalTableMeta {
    const meta = this.tables[tableName]
    if (!meta) {
      throw new Error(`[limesync] unknown local table '${tableName}'`)
    }
    return meta
  }

  #buildStagedDelegate(stagedRows: Map<string, Map<string, Row>>): LocalQueryDelegate {
    const stagedSources = new Map<string, MemorySource>()
    for (const [name, tableSchema] of Object.entries(this.schema.tables)) {
      const src = new MemorySource(name, tableSchema.columns, tableSchema.primaryKey)
      const map = stagedRows.get(name) ?? this.#rows.get(name)!
      for (const row of map.values()) {
        consume(src.push(makeSourceChangeAdd(row)))
      }
      stagedSources.set(name, src)
    }
    return new LocalQueryDelegate(stagedSources, true)
  }

  transaction<T>(work: (tx: LocalTransaction<S>) => T | Promise<T>): Promise<T> {
    return this.#enqueue(async (): Promise<T> => {
      // ensure hydration completes before any transaction reads or writes
      if (!this.#ready && this.#hasDeviceTables) {
        await this.#hydrateUnlocked(this.#lifecycleEpoch)
      }

      const stagedRows = new Map<string, Map<string, Row>>()
      const dirtyTables = new Set<string>()
      const pendingChanges = new Map<string, Map<string, { prev?: Row; next?: Row }>>()

      const getTableMap = (tableName: string): Map<string, Row> => {
        let map = stagedRows.get(tableName)
        if (!map) {
          map = new Map(this.#rows.get(tableName)!)
          stagedRows.set(tableName, map)
        }
        return map
      }

      const getEffectiveMap = (tableName: string): ReadonlyMap<string, Row> =>
        stagedRows.get(tableName) ?? this.#rows.get(tableName)!

      const recordChange = (tableName: string, pk: string, next: Row | undefined) => {
        dirtyTables.add(tableName)
        let tableChanges = pendingChanges.get(tableName)
        if (!tableChanges) {
          tableChanges = new Map()
          pendingChanges.set(tableName, tableChanges)
        }
        const existing = tableChanges.get(pk)
        if (existing) {
          existing.next = next
        } else {
          tableChanges.set(pk, {
            prev: this.#rows.get(tableName)!.get(pk),
            next,
          })
        }
      }

      const deleteWithReferentialActions = (
        tableName: string,
        pk: string,
        visited = new Set<string>()
      ) => {
        const visitKey = `${tableName}\0${pk}`
        if (visited.has(visitKey)) return
        visited.add(visitKey)
        const map = getTableMap(tableName)
        const deletedRow = map.get(pk)
        if (!deletedRow) return
        map.delete(pk)
        recordChange(tableName, pk, undefined)

        // apply onDelete actions for any referencing local table
        for (const [childTable, childMeta] of Object.entries(this.tables)) {
          for (const [colName, col] of Object.entries(childMeta.columns)) {
            const ref = col.references
            if (!ref || ref.table !== tableName) continue
            const deletedVal = deletedRow[ref.column]
            if (deletedVal === null || deletedVal === undefined) continue
            const childMap = getTableMap(childTable)
            for (const [childPk, childRow] of [...childMap.entries()]) {
              if (!Object.is(childRow[colName], deletedVal)) continue
              const action = ref.onDelete ?? 'restrict'
              if (action === 'cascade') {
                deleteWithReferentialActions(childTable, childPk, visited)
              } else if (action === 'set null') {
                if (!col.optional) {
                  throw new Error(
                    `[limesync] cannot set null on required foreign key '${childTable}.${colName}'`
                  )
                }
                const updated = normalizeUpdateRow(childMeta, childRow, {
                  [colName]: null,
                })
                childMap.set(childPk, updated)
                recordChange(childTable, childPk, updated)
              } else {
                throw new Error(
                  `[limesync] cannot delete '${tableName}' (${pk}): referenced by '${childTable}.${colName}'`
                )
              }
            }
          }
        }
      }

      let active = true
      const ensureActive = () => {
        if (!active) {
          throw new Error('[limesync] local transaction is already closed')
        }
      }

      const mutate = Object.fromEntries(
        Object.keys(this.tables).map((tableName) => {
          const meta = this.#assertLocalTable(tableName)
          return [
            tableName,
            {
              insert: async (input: Record<string, unknown>) => {
                ensureActive()
                const row = normalizeInsertRow(meta, input)
                const pk = primaryKeyFor(meta, row)
                const map = getTableMap(tableName)
                if (map.has(pk)) {
                  throw new Error(
                    `[limesync] duplicate primary key in local table '${tableName}': ${pk}`
                  )
                }
                map.set(pk, row)
                recordChange(tableName, pk, row)
              },
              upsert: async (input: Record<string, unknown>) => {
                ensureActive()
                const pk = primaryKeyFor(meta, input)
                const map = getTableMap(tableName)
                const prev = map.get(pk)
                const row = prev
                  ? normalizeUpdateRow(meta, prev, input)
                  : normalizeInsertRow(meta, input)
                map.set(pk, row)
                recordChange(tableName, pk, row)
              },
              update: async (patch: Record<string, unknown>) => {
                ensureActive()
                const pk = primaryKeyFor(meta, patch)
                const map = getTableMap(tableName)
                const prev = map.get(pk)
                if (!prev) {
                  throw new Error(
                    `[limesync] cannot update missing row in local table '${tableName}': ${pk}`
                  )
                }
                const row = normalizeUpdateRow(meta, prev, patch)
                map.set(pk, row)
                recordChange(tableName, pk, row)
              },
              delete: async (key: Record<string, unknown>) => {
                ensureActive()
                const pk = primaryKeyFor(meta, key)
                deleteWithReferentialActions(tableName, pk)
              },
            },
          ]
        })
      ) as LocalTxMutate<S>

      const tx: LocalTransaction<S> = {
        run: async (q) => {
          ensureActive()
          const internals = asQueryInternals(q)
          classifyQueryAst(internals.ast, this.tableNames)
          if (dirtyTables.size === 0) return this.#delegate.run(q)
          return this.#buildStagedDelegate(stagedRows).run(q)
        },
        mutate,
      }

      let result: T
      try {
        result = await work(tx)
      } finally {
        active = false
      }

      // 1. Validate unique constraints and local foreign keys across the final staged state
      for (const tableName of dirtyTables) {
        const meta = this.tables[tableName]!
        assertUniqueConstraints(meta, getEffectiveMap(tableName).values())
      }
      if (dirtyTables.size > 0) {
        assertForeignKeys(this.tables, getEffectiveMap, dirtyTables)
      }

      // 2. Build prevalidated persistence ops and IVM changes WITHOUT mutating live state yet
      const persistOps: LocalWriteOp[] = []
      const liveEdits: Array<{
        tableName: string
        pk: string
        next?: Row
        change: SourceChange
      }> = []

      for (const [tableName, tableChanges] of pendingChanges) {
        const meta = this.tables[tableName]!
        const shouldPersist = meta.persistence === 'device'

        for (const [pk, { prev, next }] of tableChanges) {
          if (prev === undefined && next === undefined) continue
          if (prev === undefined && next !== undefined) {
            liveEdits.push({ tableName, pk, next, change: makeSourceChangeAdd(next) })
            if (shouldPersist) {
              persistOps.push({ kind: 'put', table: tableName, key: pk, row: next })
            }
          } else if (prev !== undefined && next === undefined) {
            liveEdits.push({
              tableName,
              pk,
              next: undefined,
              change: makeSourceChangeRemove(prev),
            })
            if (shouldPersist) {
              persistOps.push({ kind: 'delete', table: tableName, key: pk })
            }
          } else {
            liveEdits.push({
              tableName,
              pk,
              next: next!,
              change: makeSourceChangeEdit(next!, prev!),
            })
            if (shouldPersist) {
              persistOps.push({ kind: 'put', table: tableName, key: pk, row: next! })
            }
          }
        }
      }

      // 3. Durably commit device writes BEFORE publishing to live IVM sources
      if (persistOps.length > 0) {
        await this.#persistence.commit(persistOps)
      }

      // 4. Publish prevalidated changes atomically into live rows + IVM sources
      if (liveEdits.length > 0) {
        for (const edit of liveEdits) {
          const liveMap = this.#rows.get(edit.tableName)!
          const source = this.#sources.get(edit.tableName)!
          if (edit.next === undefined) liveMap.delete(edit.pk)
          else liveMap.set(edit.pk, edit.next)
          consume(source.push(edit.change))
        }
        this.#version++
        this.#delegate.notifyCommit()
      }

      return result
    })
  }

  createMutators(): LocalMutators<S> {
    const out: Record<string, LocalTableMutator<any>> = {}
    for (const tableName of Object.keys(this.tables)) {
      out[tableName] = {
        insert: (row: any) =>
          makeLocalMutationResult(
            this.transaction((tx) => (tx.mutate as any)[tableName].insert(row))
          ),
        upsert: (row: any) =>
          makeLocalMutationResult(
            this.transaction((tx) => (tx.mutate as any)[tableName].upsert(row))
          ),
        update: (row: any) =>
          makeLocalMutationResult(
            this.transaction((tx) => (tx.mutate as any)[tableName].update(row))
          ),
        delete: (key: any) =>
          makeLocalMutationResult(
            this.transaction((tx) => (tx.mutate as any)[tableName].delete(key))
          ),
      }
    }
    return out as LocalMutators<S>
  }

  clear(): Promise<void> {
    return this.#enqueue(async () => {
      // clear backing storage AND atomically reinstall per-table schema metadata
      // inside one persistence transaction so no peer can interleave in between.
      await this.#persistence.clear?.(this.tables)
      let mutated = false
      for (const [tableName, map] of this.#rows) {
        const source = this.#sources.get(tableName)!
        for (const [pk, row] of [...map.entries()]) {
          map.delete(pk)
          consume(source.push(makeSourceChangeRemove(row)))
          mutated = true
        }
      }
      this.#ready = true
      this.#openPromise = null
      this.#delegate.setReady(true)
      if (mutated) {
        this.#version++
        this.#delegate.notifyCommit()
      }
    })
  }

  /** reopen a closed or closing store (used when a suspended provider fiber reveals) */
  reopen(): Promise<void> {
    const previousClose = this.#closePromise
    this.#lifecycleEpoch++
    this.#closing = false
    this.#closed = false
    this.#closePromise = null
    this.#openPromise = null
    this.#ready = !this.#hasDeviceTables
    this.#delegate.setReady(this.#ready)
    if (previousClose) {
      this.#opQueue = this.#opQueue.then(() => previousClose.catch(() => {}))
    }
    return this.#hasDeviceTables ? this.open() : Promise.resolve()
  }

  close(): Promise<void> {
    if (this.#closePromise) return this.#closePromise
    const epoch = this.#lifecycleEpoch
    this.#closing = true
    this.#closePromise = (async () => {
      await this.#opQueue.catch(() => {})
      await this.#persistence.close?.()
      // only finalize closed state if reopen() has not started a newer epoch
      if (this.#lifecycleEpoch === epoch) {
        this.#closed = true
      }
    })()
    return this.#closePromise
  }
}
