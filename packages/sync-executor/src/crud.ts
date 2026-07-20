import type {
  ApplicationTransaction,
  JsonValue,
  SqlStatementMetadata,
  ZeroSchemaConfig,
} from './types.js'

type CrudKind = SqlStatementMetadata['kind']

function quoteIdentifier(value: string): string {
  if (!value) throw new TypeError('SQL identifier must not be empty')
  return `"${value.replaceAll('"', '""')}"`
}

function quoteTable(value: string, dialect: 'sqlite' | 'postgresql'): string {
  return dialect === 'postgresql'
    ? value.split('.').map(quoteIdentifier).join('.')
    : quoteIdentifier(value)
}

function placeholder(dialect: 'sqlite' | 'postgresql', index: number): string {
  return dialect === 'sqlite' ? '?' : `$${index}`
}

function encodeValue(
  dialect: 'sqlite' | 'postgresql',
  type: string,
  value: unknown
): unknown {
  if (value === null) return null
  if (dialect === 'sqlite') {
    if (type === 'boolean' && typeof value === 'boolean') return value ? 1 : 0
    if (type === 'json') return JSON.stringify(value)
  }
  return value
}

function valueRecord(value: unknown): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new TypeError('CRUD value must be an object')
  }
  return value as Record<string, unknown>
}

export async function executeCrud(
  tx: ApplicationTransaction,
  schema: ZeroSchemaConfig,
  dialect: 'sqlite' | 'postgresql',
  tableName: string,
  kind: CrudKind,
  input: unknown
): Promise<void> {
  const table = schema.tables[tableName]
  if (!table) throw new TypeError(`unknown table: ${tableName}`)

  const value = valueRecord(input)
  const entries = Object.entries(value).filter(([, field]) => field !== undefined)
  for (const [column] of entries) {
    if (!table.columns[column]) {
      throw new TypeError(`unknown column: ${tableName}.${column}`)
    }
  }
  for (const primaryKey of table.primaryKey) {
    if (!Object.hasOwn(value, primaryKey) || value[primaryKey] === undefined) {
      throw new TypeError(`missing primary key: ${tableName}.${primaryKey}`)
    }
  }

  const physicalTable = table.serverName ?? table.name ?? tableName
  const quotedTable = quoteTable(physicalTable, dialect)
  const physicalColumn = (column: string) => table.columns[column]?.serverName ?? column
  const metadata: SqlStatementMetadata = {
    table: physicalTable,
    publicTable: tableName,
    kind,
  }
  const primaryKeys = table.primaryKey.map((column) => physicalColumn(column))
  const whereParams = table.primaryKey.map((column) =>
    encodeValue(dialect, table.columns[column]!.type, value[column])
  )

  if (kind === 'delete') {
    const where = primaryKeys
      .map(
        (column, index) =>
          `${quoteIdentifier(column)} = ${placeholder(dialect, index + 1)}`
      )
      .join(' AND ')
    await tx.exec(`DELETE FROM ${quotedTable} WHERE ${where}`, whereParams, metadata)
    return
  }

  if (kind === 'update') {
    const mutable = entries.filter(([column]) => !table.primaryKey.includes(column))
    if (mutable.length === 0) return
    const params = mutable.map(([column, field]) =>
      encodeValue(dialect, table.columns[column]!.type, field)
    )
    const set = mutable
      .map(
        ([column], index) =>
          `${quoteIdentifier(physicalColumn(column))} = ${placeholder(dialect, index + 1)}`
      )
      .join(', ')
    const where = primaryKeys
      .map(
        (column, index) =>
          `${quoteIdentifier(column)} = ${placeholder(dialect, mutable.length + index + 1)}`
      )
      .join(' AND ')
    await tx.exec(
      `UPDATE ${quotedTable} SET ${set} WHERE ${where}`,
      [...params, ...whereParams],
      metadata
    )
    return
  }

  const columns = entries.map(([column]) => column)
  const params = entries.map(([column, field]) =>
    encodeValue(dialect, table.columns[column]!.type, field)
  )
  const insert = `INSERT INTO ${quotedTable} (${columns
    .map((column) => quoteIdentifier(physicalColumn(column)))
    .join(', ')}) VALUES (${params
    .map((_, index) => placeholder(dialect, index + 1))
    .join(', ')})`
  const conflict = primaryKeys.map(quoteIdentifier).join(', ')

  if (kind === 'insert') {
    await tx.exec(`${insert} ON CONFLICT (${conflict}) DO NOTHING`, params, metadata)
    return
  }

  const mutable = columns.filter((column) => !table.primaryKey.includes(column))
  const action =
    mutable.length === 0
      ? 'DO NOTHING'
      : `DO UPDATE SET ${mutable
          .map((column) => {
            const physical = quoteIdentifier(physicalColumn(column))
            return `${physical} = excluded.${physical}`
          })
          .join(', ')}`
  await tx.exec(`${insert} ON CONFLICT (${conflict}) ${action}`, params, metadata)
}

export function isJsonValue(value: unknown): value is JsonValue {
  if (
    value === null ||
    typeof value === 'string' ||
    typeof value === 'boolean' ||
    (typeof value === 'number' && Number.isFinite(value))
  ) {
    return true
  }
  if (Array.isArray(value)) return value.every(isJsonValue)
  if (typeof value !== 'object') return false
  return Object.values(value).every(isJsonValue)
}
