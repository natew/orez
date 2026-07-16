export type TransactionQueryWireValue =
  | { kind: 'null' }
  | { kind: 'integer'; value: string }
  | { kind: 'real'; value: number }
  | { kind: 'text'; value: string }
  | { kind: 'blob'; value: number[] }

export type TransactionQueryBinding =
  | { kind: 'literal'; value: TransactionQueryWireValue }
  | { kind: 'parent_field'; field: string }

export type TransactionQueryColumnType = 'string' | 'number' | 'boolean' | 'json' | 'null'

export type TransactionQueryColumn = {
  name: string
  columnType: TransactionQueryColumnType
}

export type CompiledTransactionQueryRelationship = {
  name: string
  node: CompiledTransactionQueryNode
}

export type CompiledTransactionQueryNode = {
  table: string
  singular: boolean
  sql: string
  bindings: TransactionQueryBinding[]
  columns: TransactionQueryColumn[]
  relationships: CompiledTransactionQueryRelationship[]
}

export type CompiledTransactionQueryPlan = {
  rootTable: string
  planHash: string
  root: CompiledTransactionQueryNode
}

export type TransactionQueryBudget = {
  maxSelects: number
  maxRows: number
}

export type TransactionQueryExecutionOptions = {
  queryName?: string
  budget?: Partial<TransactionQueryBudget>
}

export type TransactionQueryFormat = {
  singular: boolean
  relationships: Readonly<Record<string, TransactionQueryFormat>>
}

export const DEFAULT_TRANSACTION_QUERY_BUDGET: TransactionQueryBudget = {
  maxSelects: 256,
  maxRows: 10_000,
}

export class TransactionQueryBudgetError extends Error {
  readonly code = 'transaction_query_budget_exceeded'

  constructor(
    readonly query: string,
    readonly selects: number,
    readonly rows: number,
    readonly maxSelects: number,
    readonly maxRows: number
  ) {
    super(
      `transaction_query_budget_exceeded: query=${query} selects=${selects} rows=${rows} maxSelects=${maxSelects} maxRows=${maxRows}`
    )
    this.name = 'TransactionQueryBudgetError'
  }
}

type MaterializedRow = Record<string, unknown>
type ExecuteSelect = (
  sql: string,
  params: readonly unknown[]
) => readonly MaterializedRow[]
type ExecuteSelectAsync = (
  sql: string,
  params: readonly unknown[]
) => readonly MaterializedRow[] | Promise<readonly MaterializedRow[]>

const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER)
const MIN_SAFE = BigInt(Number.MIN_SAFE_INTEGER)
const PLAN_HASH = /^[0-9a-f]{16}$/
const COLUMN_TYPES = new Set<TransactionQueryColumnType>([
  'string',
  'number',
  'boolean',
  'json',
  'null',
])

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function malformed(detail: string): TypeError {
  return new TypeError(`compiled transaction query plan ${detail}`)
}

function assertWireValue(value: unknown, path: string): TransactionQueryWireValue {
  if (!isObject(value) || typeof value.kind !== 'string') {
    throw malformed(`${path} has an invalid literal`)
  }
  switch (value.kind) {
    case 'null':
      return value as TransactionQueryWireValue
    case 'integer':
      if (typeof value.value !== 'string' || !/^-?[0-9]+$/.test(value.value)) {
        throw malformed(`${path} has an invalid integer literal`)
      }
      return value as TransactionQueryWireValue
    case 'real':
      if (typeof value.value !== 'number' || !Number.isFinite(value.value)) {
        throw malformed(`${path} has an invalid real literal`)
      }
      return value as TransactionQueryWireValue
    case 'text':
      if (typeof value.value !== 'string') {
        throw malformed(`${path} has an invalid text literal`)
      }
      return value as TransactionQueryWireValue
    case 'blob':
      if (
        !Array.isArray(value.value) ||
        value.value.some(
          (byte) => !Number.isInteger(byte) || Number(byte) < 0 || Number(byte) > 255
        )
      ) {
        throw malformed(`${path} has an invalid blob literal`)
      }
      return value as TransactionQueryWireValue
    default:
      throw malformed(`${path} has an unknown literal kind`)
  }
}

function assertNode(
  value: unknown,
  path: string,
  depth: number
): asserts value is CompiledTransactionQueryNode {
  if (depth > 64) throw malformed(`${path} exceeds the maximum relationship depth`)
  if (!isObject(value)) throw malformed(`${path} must be an object`)
  if (typeof value.table !== 'string' || value.table.length === 0) {
    throw malformed(`${path}.table must be a non-empty string`)
  }
  if (typeof value.singular !== 'boolean') {
    throw malformed(`${path}.singular must be a boolean`)
  }
  if (typeof value.sql !== 'string' || value.sql.length === 0) {
    throw malformed(`${path}.sql must be a non-empty string`)
  }
  if (!Array.isArray(value.bindings)) {
    throw malformed(`${path}.bindings must be an array`)
  }
  for (const [index, binding] of value.bindings.entries()) {
    if (!isObject(binding)) throw malformed(`${path}.bindings[${index}] is invalid`)
    if (binding.kind === 'literal') {
      assertWireValue(binding.value, `${path}.bindings[${index}]`)
    } else if (
      binding.kind !== 'parent_field' ||
      typeof binding.field !== 'string' ||
      binding.field.length === 0
    ) {
      throw malformed(`${path}.bindings[${index}] is invalid`)
    }
  }
  if (!Array.isArray(value.columns) || value.columns.length === 0) {
    throw malformed(`${path}.columns must be a non-empty array`)
  }
  const names = new Set<string>()
  for (const [index, column] of value.columns.entries()) {
    if (
      !isObject(column) ||
      typeof column.name !== 'string' ||
      column.name.length === 0 ||
      typeof column.columnType !== 'string' ||
      !COLUMN_TYPES.has(column.columnType as TransactionQueryColumnType)
    ) {
      throw malformed(`${path}.columns[${index}] is invalid`)
    }
    if (names.has(column.name)) {
      throw malformed(`${path}.columns contains duplicate '${column.name}'`)
    }
    names.add(column.name)
  }
  if (!Array.isArray(value.relationships)) {
    throw malformed(`${path}.relationships must be an array`)
  }
  const relationships = new Set<string>()
  for (const [index, relationship] of value.relationships.entries()) {
    if (
      !isObject(relationship) ||
      typeof relationship.name !== 'string' ||
      relationship.name.length === 0
    ) {
      throw malformed(`${path}.relationships[${index}] is invalid`)
    }
    if (relationships.has(relationship.name) || names.has(relationship.name)) {
      throw malformed(`${path}.relationships contains conflicting '${relationship.name}'`)
    }
    relationships.add(relationship.name)
    assertNode(relationship.node, `${path}.relationships[${index}].node`, depth + 1)
  }
}

function assertPlan(value: unknown): asserts value is CompiledTransactionQueryPlan {
  if (!isObject(value)) throw malformed('must be an object')
  if (typeof value.rootTable !== 'string' || value.rootTable.length === 0) {
    throw malformed('rootTable must be a non-empty string')
  }
  if (typeof value.planHash !== 'string' || !PLAN_HASH.test(value.planHash)) {
    throw malformed('planHash must be 16 lowercase hexadecimal characters')
  }
  assertNode(value.root, 'root', 0)
  if (value.root.table !== value.rootTable) {
    throw malformed('root.table must match rootTable')
  }
}

function resolveBudget(
  budget: Partial<TransactionQueryBudget> | undefined
): TransactionQueryBudget {
  const resolved = { ...DEFAULT_TRANSACTION_QUERY_BUDGET, ...budget }
  for (const [name, value] of Object.entries(resolved)) {
    if (!Number.isSafeInteger(value) || value < 1) {
      throw new TypeError(
        `transaction query budget ${name} must be a positive safe integer`
      )
    }
  }
  return resolved
}

function decodeWireValue(value: TransactionQueryWireValue): unknown {
  switch (value.kind) {
    case 'null':
      return null
    case 'integer': {
      const exact = BigInt(value.value)
      return exact >= MIN_SAFE && exact <= MAX_SAFE ? Number(exact) : value.value
    }
    case 'real':
    case 'text':
      return value.value
    case 'blob':
      return Uint8Array.from(value.value)
  }
}

function bytes(value: unknown): Uint8Array | undefined {
  if (value instanceof ArrayBuffer) return new Uint8Array(value)
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
  }
  return undefined
}

function decodeColumn(
  value: unknown,
  columnType: TransactionQueryColumnType,
  path: string
): unknown {
  if (value === null) return null
  switch (columnType) {
    case 'string':
      if (typeof value === 'string') return value
      break
    case 'number':
      if (typeof value === 'number' && Number.isFinite(value)) return value
      if (typeof value === 'bigint' && value >= MIN_SAFE && value <= MAX_SAFE) {
        return Number(value)
      }
      break
    case 'boolean':
      if (typeof value === 'boolean') return value
      if (value === 0 || value === 0n || value === '0') return false
      if (value === 1 || value === 1n || value === '1') return true
      break
    case 'json': {
      if (typeof value === 'string') {
        try {
          return JSON.parse(value)
        } catch {
          throw new TypeError(`transaction query row ${path} contains invalid JSON`)
        }
      }
      const encoded = bytes(value)
      if (encoded) {
        try {
          return JSON.parse(new TextDecoder().decode(encoded))
        } catch {
          throw new TypeError(`transaction query row ${path} contains invalid JSON`)
        }
      }
      if (typeof value === 'object') return value
      break
    }
    case 'null':
      break
  }
  throw new TypeError(
    `transaction query row ${path} does not match schema type ${columnType}`
  )
}

function decodeRow(
  raw: MaterializedRow,
  node: CompiledTransactionQueryNode
): MaterializedRow {
  const decoded: MaterializedRow = {}
  for (const column of node.columns) {
    if (!Object.hasOwn(raw, column.name)) {
      throw new TypeError(
        `transaction query row for ${node.table} is missing column ${column.name}`
      )
    }
    decoded[column.name] = decodeColumn(
      raw[column.name],
      column.columnType,
      `${node.table}.${column.name}`
    )
  }
  return decoded
}

type TransactionQueryExecutionState = {
  budget: TransactionQueryBudget
  query: string
  rows: number
  selects: number
}

type PreparedSelect = {
  params: unknown[]
  state: TransactionQueryExecutionState
}

type DecodedSelect = {
  rows: Array<{ decoded: MaterializedRow; raw: MaterializedRow }>
  state: TransactionQueryExecutionState
}

type NodeExecution = {
  state: TransactionQueryExecutionState
  value: MaterializedRow[] | MaterializedRow | null | undefined
}

function createExecutionState(
  plan: CompiledTransactionQueryPlan,
  options: TransactionQueryExecutionOptions
): TransactionQueryExecutionState {
  assertPlan(plan)
  return {
    budget: resolveBudget(options.budget),
    query: options.queryName?.trim() || `${plan.rootTable}:${plan.planHash}`,
    rows: 0,
    selects: 0,
  }
}

function assertWithinBudget(state: TransactionQueryExecutionState): void {
  if (state.selects <= state.budget.maxSelects && state.rows <= state.budget.maxRows) {
    return
  }
  throw new TransactionQueryBudgetError(
    state.query,
    state.selects,
    state.rows,
    state.budget.maxSelects,
    state.budget.maxRows
  )
}

function prepareSelect(
  state: TransactionQueryExecutionState,
  node: CompiledTransactionQueryNode,
  parent?: MaterializedRow
): PreparedSelect {
  const params = node.bindings.map((binding) => {
    if (binding.kind === 'literal') return decodeWireValue(binding.value)
    if (!parent || !Object.hasOwn(parent, binding.field)) {
      throw new TypeError(
        `transaction query parent row for ${node.table} is missing field ${binding.field}`
      )
    }
    return parent[binding.field]
  })
  const next = { ...state, selects: state.selects + 1 }
  assertWithinBudget(next)
  return { params, state: next }
}

function decodeSelect(
  state: TransactionQueryExecutionState,
  node: CompiledTransactionQueryNode,
  materialized: readonly MaterializedRow[]
): DecodedSelect {
  if (!Array.isArray(materialized)) {
    throw new TypeError('transaction query select must return a materialized row array')
  }
  const next = { ...state, rows: state.rows + materialized.length }
  assertWithinBudget(next)
  const rows = materialized.map((raw) => {
    if (!isObject(raw)) {
      throw new TypeError(
        `transaction query select for ${node.table} returned a non-object row`
      )
    }
    return { decoded: decodeRow(raw, node), raw }
  })
  return { rows, state: next }
}

function shapeRows(
  node: CompiledTransactionQueryNode,
  rows: MaterializedRow[]
): NodeExecution['value'] {
  return node.singular ? rows[0] : rows
}

export function executeTransactionQueryPlan<Result = unknown>(
  plan: CompiledTransactionQueryPlan,
  execute: ExecuteSelect,
  options: TransactionQueryExecutionOptions = {}
): Result {
  const run = (
    node: CompiledTransactionQueryNode,
    state: TransactionQueryExecutionState,
    parent?: MaterializedRow
  ): NodeExecution => {
    const prepared = prepareSelect(state, node, parent)
    const selected = decodeSelect(
      prepared.state,
      node,
      execute(node.sql, prepared.params)
    )
    const hydrated: MaterializedRow[] = []
    let current = selected.state
    for (const { decoded, raw } of selected.rows) {
      for (const relationship of node.relationships) {
        const related = run(relationship.node, current, raw)
        current = related.state
        decoded[relationship.name] =
          relationship.node.singular && related.value === undefined ? null : related.value
      }
      hydrated.push(decoded)
    }
    return { state: current, value: shapeRows(node, hydrated) }
  }

  const state = createExecutionState(plan, options)
  return run(plan.root, state).value as Result
}

export async function executeTransactionQueryPlanAsync<Result = unknown>(
  plan: CompiledTransactionQueryPlan,
  execute: ExecuteSelectAsync,
  options: TransactionQueryExecutionOptions = {}
): Promise<Result> {
  const run = async (
    node: CompiledTransactionQueryNode,
    state: TransactionQueryExecutionState,
    parent?: MaterializedRow
  ): Promise<NodeExecution> => {
    const prepared = prepareSelect(state, node, parent)
    const selected = decodeSelect(
      prepared.state,
      node,
      await execute(node.sql, prepared.params)
    )
    const hydrated: MaterializedRow[] = []
    let current = selected.state
    for (const { decoded, raw } of selected.rows) {
      for (const relationship of node.relationships) {
        const related = await run(relationship.node, current, raw)
        current = related.state
        decoded[relationship.name] =
          relationship.node.singular && related.value === undefined ? null : related.value
      }
      hydrated.push(decoded)
    }
    return { state: current, value: shapeRows(node, hydrated) }
  }

  const state = createExecutionState(plan, options)
  const { value } = await run(plan.root, state)
  return value as Result
}
