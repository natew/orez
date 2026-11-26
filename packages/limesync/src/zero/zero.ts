import { createSchema as createZeroSchema } from 'orez-sync-executor/zero'

import type {
  OneDataColumn,
  OneDataConnection,
  OneDataIndex,
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

type IsLocalTable<T> = T extends { readonly local: 'device' | 'memory' } ? true : false

type ClientKeys<S extends OneDataSchema> = {
  [K in keyof S['tables']]: S['tables'][K]['public'] extends true
    ? K
    : IsLocalTable<S['tables'][K]> extends true
      ? K
      : never
}[keyof S['tables']]

type LocalKeys<S extends OneDataSchema> = {
  [K in keyof S['tables']]: IsLocalTable<S['tables'][K]> extends true ? K : never
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
type ProjectedSchema<S extends OneDataSchema, Keys extends keyof S['tables']> = {
  tables: { [K in Keys]: SyncTable<S['tables'][K]> }
  relationships: {
    [K in Extract<keyof S['relationships'], Keys>]: {
      [R in keyof S['relationships'][K]]: S['relationships'][K][R] extends readonly [
        infer First extends OneDataConnection,
        infer Second extends OneDataConnection,
      ]
        ? readonly [First, Second]
        : readonly [S['relationships'][K][R][number]]
    }
  }
}
export type SyncSchema<S extends OneDataSchema> = ProjectedSchema<S, PublicKeys<S>>
export type ClientSchema<S extends OneDataSchema> = ProjectedSchema<S, ClientKeys<S>>
export type LocalZeroSchema<S extends OneDataSchema> = ProjectedSchema<S, LocalKeys<S>>

export type EmptyLocalSchema = {
  tables: Record<never, never>
  relationships: Record<never, never>
}

export type LocalTableMeta = {
  readonly name: string
  readonly dbName: string
  readonly persistence: 'device' | 'memory'
  readonly primaryKey: readonly [string, ...string[]]
  readonly columns: Readonly<Record<string, OneDataColumn>>
  readonly indexes?: Readonly<Record<string, OneDataIndex>>
}

export type LocalSchemaMeta<S extends Schema = Schema> = {
  readonly schema: S
  readonly tables: Readonly<Record<string, LocalTableMeta>>
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

function projectTables(
  input: OneDataSchema,
  include: (table: OneDataTable) => boolean,
  label: string
): Schema {
  const tables: Record<string, Schema['tables'][string]> = Object.create(null)
  for (const [name, table] of Object.entries(input.tables)) {
    if (!include(table)) continue
    const [primary, ...secondary] = table.primaryKey
    if (primary === undefined)
      throw new Error(`${label} table ${name} requires a primary key`)
    tables[name] = {
      name,
      ...(table.dbName === undefined ? {} : { serverName: table.dbName }),
      primaryKey: [primary, ...secondary],
      columns: Object.fromEntries(
        Object.entries(table.columns).map(([colName, column]) => [
          colName,
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
          throw new Error(`invalid ${label} relationship ${name}.${key}`)
        }
        return [key, second ? [first, second] : [first]]
      })
    )
  }
  return { tables, relationships }
}

function project(input: OneDataSchema): Schema {
  return projectTables(input, (table) => table.public, 'public')
}

export function createClientSchema<const S extends OneDataSchema>(
  input: S
): ClientSchema<S>
export function createClientSchema<const S extends Schema>(input: S): S
export function createClientSchema(input: OneDataSchema | Schema): Schema {
  if (!isDataSchema(input)) return input
  return projectTables(
    input,
    (table) => table.public || table.local !== undefined,
    'client'
  )
}

export function extractLocalSchema<const S extends OneDataSchema>(
  input: S
): LocalSchemaMeta<LocalZeroSchema<S>>
export function extractLocalSchema(input: Schema): LocalSchemaMeta<EmptyLocalSchema>
export function extractLocalSchema(input: OneDataSchema | Schema): LocalSchemaMeta<any> {
  if (!isDataSchema(input)) {
    return {
      schema: { tables: {}, relationships: {} },
      tables: Object.create(null),
    }
  }
  const localSchema = projectTables(input, (table) => table.local !== undefined, 'local')
  const tables: Record<string, LocalTableMeta> = Object.create(null)
  for (const [name, table] of Object.entries(input.tables)) {
    if (!table.local) continue
    const [primary, ...secondary] = table.primaryKey
    if (primary === undefined)
      throw new Error(`local table ${name} requires a primary key`)
    tables[name] = {
      name,
      dbName: table.dbName ?? name,
      persistence: table.local,
      primaryKey: [primary, ...secondary],
      columns: table.columns,
      ...(table.indexes ? { indexes: table.indexes } : {}),
    }
  }
  return { schema: localSchema, tables }
}

type CreateSchema = typeof createZeroSchema &
  (<const S extends OneDataSchema>(input: S) => SyncSchema<S>) &
  (<const S extends Schema>(input: S) => S)

export const createSchema = ((input: ZeroOptions | Schema | OneDataSchema) => {
  if (isZeroOptions(input)) return createZeroSchema(input)
  return isDataSchema(input) ? project(input) : input
}) as CreateSchema
