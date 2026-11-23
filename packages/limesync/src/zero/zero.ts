import { createSchema as createZeroSchema } from 'orez-sync-executor/zero'

import type {
  OneDataColumn,
  OneDataConnection,
  OneDataSchema,
  OneDataTable,
} from '../data/one-schema.js'
import type { Schema } from 'orez-sync-executor/zero'

export * from 'orez-sync-executor/zero'

const types = {
  string: 'string',
  integer: 'number',
  number: 'number',
  boolean: 'boolean',
  json: 'json',
  timestamp: 'number',
} as const

type PublicKeys<S extends OneDataSchema> = {
  [K in keyof S['tables']]: S['tables'][K]['public'] extends true ? K : never
}[keyof S['tables']]

type SyncType<C extends OneDataColumn> = {
  type: (typeof types)[C['kind']]
  optional: C['optional']
  customType: Exclude<C['value'], undefined>
  serverName?: string
}
type SyncTable<T extends OneDataTable> = {
  name: T['name']
  serverName?: string
  columns: { [K in keyof T['columns']]: SyncType<T['columns'][K]> }
  primaryKey: readonly [string, ...string[]]
}
type SyncSchema<S extends OneDataSchema> = {
  tables: { [K in PublicKeys<S>]: SyncTable<S['tables'][K]> }
  relationships: {
    [K in Extract<keyof S['relationships'], PublicKeys<S>>]: {
      [R in keyof S['relationships'][K]]:
        | readonly [S['relationships'][K][R][number]]
        | readonly [OneDataConnection, S['relationships'][K][R][number]]
    }
  }
}

type ZeroOptions = Parameters<typeof createZeroSchema>[0]

function isZeroOptions(
  input: ZeroOptions | Schema | OneDataSchema
): input is ZeroOptions {
  return Array.isArray(input.tables)
}

function isDataSchema(input: Schema | OneDataSchema): input is OneDataSchema {
  return Object.values(input.tables).some((table) => 'public' in table)
}

function project(input: OneDataSchema): Schema {
  const tables: Record<string, Schema['tables'][string]> = Object.create(null)
  for (const [name, table] of Object.entries(input.tables)) {
    if (!table.public) continue
    const [primary, ...secondary] = table.primaryKey
    if (primary === undefined)
      throw new Error(`public table ${name} requires a primary key`)
    tables[name] = {
      name,
      ...(table.dbName === undefined ? {} : { serverName: table.dbName }),
      primaryKey: [primary, ...secondary],
      columns: Object.fromEntries(
        Object.entries(table.columns).map(([name, column]) => [
          name,
          {
            type: types[column.kind],
            optional: column.optional,
            customType: null,
            ...(column.name === undefined ? {} : { serverName: column.name }),
          },
        ])
      ),
    }
  }
  const relationships: Record<string, Schema['relationships'][string]> =
    Object.create(null)
  for (const [name, relations] of Object.entries(input.relationships)) {
    if (!Object.hasOwn(tables, name)) continue
    relationships[name] = Object.fromEntries(
      Object.entries(relations).map(([key, path]) => {
        const [first, second] = path
        if (
          !first ||
          path.length > 2 ||
          path.some((connection) => !Object.hasOwn(tables, connection.destSchema))
        ) {
          throw new Error(`invalid public relationship ${name}.${key}`)
        }
        return [key, second ? [first, second] : [first]]
      })
    )
  }
  return { tables, relationships }
}

type CreateSchema = typeof createZeroSchema &
  (<const S extends OneDataSchema>(input: S) => SyncSchema<S>) &
  (<const S extends Schema>(input: S) => S)

export const createSchema = ((input: ZeroOptions | Schema | OneDataSchema) => {
  if (isZeroOptions(input)) return createZeroSchema(input)
  return isDataSchema(input) ? project(input) : input
}) as CreateSchema
