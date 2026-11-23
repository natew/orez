import {
  createAdapterFactory,
  type CleanedWhere,
  type CustomAdapter,
  type DBAdapter,
  type JoinConfig,
} from 'better-auth/adapters'

import type {
  LocalApplicationSqlClient,
  LocalApplicationSqlTransaction,
} from './local-application-sql.js'
import type { OneDataSchema, OneDataTable } from './data/one-schema.js'
import type { BetterAuthOptions } from 'better-auth'

type Database = LocalApplicationSqlClient
type Row = Record<string, unknown>

function quote(name: string) {
  return `"${name.replaceAll('"', '""')}"`
}

function modelTable(schema: OneDataSchema, model: string): OneDataTable {
  const table = Object.hasOwn(schema.tables, model)
    ? schema.tables[model]
    : Object.values(schema.tables).find((definition) => definition.dbName === model)
  if (!table || table.public)
    throw new Error(`Better Auth requires a private table for ${model}`)
  return table
}

function fieldColumn(table: OneDataTable, field: string) {
  const column = Object.hasOwn(table.columns, field)
    ? table.columns[field]
    : Object.values(table.columns).find((definition) => definition.name === field)
  if (!column) throw new Error(`unknown Better Auth field ${table.name}.${field}`)
  return { kind: column.kind, name: column.name ?? field }
}

function encode(value: unknown, kind: string) {
  if (value === null) return null
  if (value instanceof Date) return value.getTime()
  if (typeof value === 'boolean') return Number(value)
  if (kind === 'json') return JSON.stringify(value)
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'bigint')
    return value
  if (Array.isArray(value)) return JSON.stringify(value)
  throw new TypeError('unsupported Better Auth SQLite value')
}

function decode<T>(table: OneDataTable, row: Row): T {
  const result: Row = {}
  for (const [field, column] of Object.entries(table.columns)) {
    const name = column.name ?? field
    if (!Object.hasOwn(row, name)) continue
    let value = row[name]
    if (value !== null) {
      if (column.kind === 'timestamp' && typeof value === 'number')
        value = new Date(value)
      if (column.kind === 'boolean') value = value === 1 || value === true
      if (column.kind === 'json' && typeof value === 'string') value = JSON.parse(value)
    }
    Object.defineProperty(result, field, { value, enumerable: true, configurable: true })
    if (name !== field && !Object.hasOwn(table.columns, name)) {
      Object.defineProperty(result, name, { value, enumerable: true, configurable: true })
    }
  }
  return result as T
}

const comparisons = {
  eq: '=',
  ne: '!=',
  lt: '<',
  lte: '<=',
  gt: '>',
  gte: '>=',
}

function predicates(
  table: OneDataTable,
  where: CleanedWhere[] | undefined,
  params: unknown[]
) {
  if (!where?.length) return ''
  const and: string[] = []
  const or: string[] = []
  const bind = (value: unknown, kind: string) => {
    params.push(encode(value, kind))
    return `?${params.length}`
  }

  for (const { field, operator, value, connector, mode } of where) {
    const column = fieldColumn(table, field)
    const name = quote(column.name)
    const insensitive = mode === 'insensitive' && column.kind === 'string'
    const lhs = insensitive ? `lower(${name})` : name
    const rhs = (value: unknown) => {
      const arg = bind(value, column.kind)
      return insensitive ? `lower(${arg})` : arg
    }
    let expression: string
    switch (operator) {
      case 'eq':
      case 'ne':
      case 'lt':
      case 'lte':
      case 'gt':
      case 'gte':
        expression =
          value === null && (operator === 'eq' || operator === 'ne')
            ? `${lhs} IS ${operator === 'ne' ? 'NOT ' : ''}NULL`
            : `${lhs} ${comparisons[operator]} ${rhs(value)}`
        break
      case 'in':
      case 'not_in':
        if (!Array.isArray(value)) throw new TypeError(`${operator} requires an array`)
        expression = value.length
          ? `${lhs} ${operator === 'not_in' ? 'NOT ' : ''}IN (${value.map(rhs).join(', ')})`
          : operator === 'not_in'
            ? '1'
            : '0'
        break
      case 'contains':
      case 'starts_with':
      case 'ends_with': {
        if (typeof value !== 'string')
          throw new TypeError(`${operator} requires a string`)
        const needle = rhs(value)
        expression =
          operator === 'ends_with'
            ? `(${needle} = '' OR substr(${lhs}, -length(${needle})) = ${needle})`
            : `instr(${lhs}, ${needle}) ${operator === 'contains' ? '> 0' : '= 1'}`
        break
      }
    }
    ;(connector === 'OR' ? or : and).push(expression)
  }

  const terms = or.length ? [...and, `(${or.join(' OR ')})`] : and
  return ` WHERE ${terms.join(' AND ')}`
}

function singleRow(table: OneDataTable, where: CleanedWhere[], params: unknown[]) {
  const keys = table.primaryKey
    .map((key) => quote(fieldColumn(table, key).name))
    .join(', ')
  const filter = predicates(table, where, params)
  const target = quote(table.dbName ?? table.name)
  return ` WHERE ${table.primaryKey.length === 1 ? keys : `(${keys})`} IN (SELECT ${keys} FROM ${target}${filter} LIMIT 1)`
}

function assignments(table: OneDataTable, update: unknown, params: unknown[]) {
  if (update === null || typeof update !== 'object' || Array.isArray(update)) {
    throw new TypeError('Better Auth updates require an object')
  }
  return Object.entries(update).map(([key, value]) => {
    const column = fieldColumn(table, key)
    params.push(encode(value, column.kind))
    return `${quote(column.name)} = ?${params.length}`
  })
}

function customAdapter(
  db: Database,
  schema: OneDataSchema,
  bound: LocalApplicationSqlTransaction | undefined
): CustomAdapter {
  const noAst = () => {
    throw new Error('Better Auth uses SQLite statements')
  }
  const read = <T>(work: (tx: LocalApplicationSqlTransaction) => Promise<T>) =>
    bound ? work(bound) : db.readTransaction(noAst, work)
  const write = <T>(work: (tx: LocalApplicationSqlTransaction) => Promise<T>) =>
    bound ? work(bound) : db.transaction(noAst, work)
  const readJoins = async <T>(
    tx: LocalApplicationSqlTransaction,
    base: OneDataTable,
    row: Row,
    joins?: JoinConfig
  ): Promise<T> => {
    const output = decode<Row>(base, row)
    for (const [model, join] of Object.entries(joins ?? {})) {
      const value = output[join.on.from]
      const one = join.relation === 'one-to-one'
      const table = modelTable(schema, model)
      const column = fieldColumn(table, join.on.to)
      const rows =
        value === null || value === undefined
          ? []
          : await tx.query(
              `SELECT * FROM ${quote(table.dbName ?? table.name)} WHERE ${quote(column.name)} = ?1 LIMIT ?2`,
              [encode(value, column.kind), one ? 1 : (join.limit ?? 100)]
            )
      const related = rows.map((related) => decode(table, related))
      Object.defineProperty(output, model, {
        value: one ? (related[0] ?? null) : related,
        enumerable: true,
      })
    }
    return output as T
  }

  return {
    create: ({ model, data }) =>
      write(async (tx) => {
        const table = modelTable(schema, model)
        const fields = Object.entries(data).map(([field, value]) => {
          const column = fieldColumn(table, field)
          return { column: quote(column.name), value: encode(value, column.kind) }
        })
        const args = fields.map((field) => field.value)
        const values = fields.length
          ? `(${fields.map((field) => field.column).join(', ')}) VALUES (${fields.map((_, index) => `?${index + 1}`).join(', ')})`
          : 'DEFAULT VALUES'
        const rows = await tx.query(
          `INSERT INTO ${quote(table.dbName ?? model)} ${values} RETURNING *`,
          args
        )
        if (!rows[0]) throw new Error(`Better Auth create returned no ${model} row`)
        return decode(table, rows[0])
      }),
    findOne: <T>({ model, where, join }: Parameters<CustomAdapter['findOne']>[0]) =>
      read(async (tx) => {
        const table = modelTable(schema, model)
        const args: unknown[] = []
        const filter = predicates(table, where, args)
        const rows = await tx.query(
          `SELECT * FROM ${quote(table.dbName ?? model)}${filter} LIMIT 1`,
          args
        )
        return rows[0] ? readJoins<T>(tx, table, rows[0], join) : null
      }),
    findMany: <T>({
      model,
      where,
      limit,
      sortBy,
      offset,
      join,
    }: Parameters<CustomAdapter['findMany']>[0]) =>
      read(async (tx) => {
        const table = modelTable(schema, model)
        const args: unknown[] = []
        const filter = predicates(table, where, args)
        const order = sortBy
          ? ` ORDER BY ${quote(fieldColumn(table, sortBy.field).name)} ${sortBy.direction === 'desc' ? 'DESC' : 'ASC'}`
          : ''
        const page = ` LIMIT ?${args.length + 1} OFFSET ?${args.length + 2}`
        const rows = await tx.query(
          `SELECT * FROM ${quote(table.dbName ?? model)}${filter}${order}${page}`,
          [...args, limit, offset ?? 0]
        )
        const result: T[] = []
        for (const row of rows) result.push(await readJoins<T>(tx, table, row, join))
        return result
      }),
    count: ({ model, where }) =>
      read(async (tx) => {
        const table = modelTable(schema, model)
        const args: unknown[] = []
        const filter = predicates(table, where, args)
        const [row] = await tx.query<{ count: number }>(
          `SELECT COUNT(*) AS count FROM ${quote(table.dbName ?? model)}${filter}`,
          args
        )
        return row.count
      }),
    update: ({ model, where, update }) =>
      write(async (tx) => {
        const table = modelTable(schema, model)
        const args: unknown[] = []
        const values = assignments(table, update, args)
        const filter = singleRow(table, where, args)
        const target = quote(table.dbName ?? model)
        const [row] = await tx.query(
          values.length
            ? `UPDATE ${target} SET ${values.join(', ')}${filter} RETURNING *`
            : `SELECT * FROM ${target}${filter}`,
          args
        )
        return row ? decode(table, row) : null
      }),
    updateMany: ({ model, where, update }) =>
      write(async (tx) => {
        const table = modelTable(schema, model)
        const args: unknown[] = []
        const values = assignments(table, update, args)
        if (!values.length) return 0
        const filter = predicates(table, where, args)
        const { changes } = await tx.exec(
          `UPDATE ${quote(table.dbName ?? model)} SET ${values.join(', ')}${filter}`,
          args
        )
        return changes
      }),
    delete: ({ model, where }) =>
      write(async (tx) => {
        const table = modelTable(schema, model)
        const args: unknown[] = []
        const filter = singleRow(table, where, args)
        await tx.exec(`DELETE FROM ${quote(table.dbName ?? model)}${filter}`, args)
      }),
    deleteMany: ({ model, where }) =>
      write(async (tx) => {
        const table = modelTable(schema, model)
        const args: unknown[] = []
        const filter = predicates(table, where, args)
        const { changes } = await tx.exec(
          `DELETE FROM ${quote(table.dbName ?? model)}${filter}`,
          args
        )
        return changes
      }),
  }
}

export function betterAuthAdapter(db: Database, schema: OneDataSchema) {
  return (options: BetterAuthOptions) => {
    const configure = (bound?: LocalApplicationSqlTransaction): DBAdapter<BetterAuthOptions> =>
      createAdapterFactory({
        config: {
          adapterId: 'limesync-sqlite',
          supportsDates: true,
          supportsBooleans: true,
          supportsJSON: true,
          supportsArrays: true,
          transaction: (work) =>
            bound
              ? work(configure(bound))
              : db.transaction(
                  () => {
                    throw new Error('Better Auth uses SQLite statements')
                  },
                  (tx) => work(configure(tx))
                ),
        },
        adapter: () => customAdapter(db, schema, bound),
      })(options)
    return configure()
  }
}
