import type { Schema } from 'orez-sync-executor/zero'

export type OneColumnKind =
  | 'string'
  | 'integer'
  | 'number'
  | 'boolean'
  | 'json'
  | 'timestamp'

export type OneDataColumn = {
  kind: OneColumnKind
  name?: string
  optional: boolean
  value?: unknown
}

export type OneDataTable = {
  name: string
  dbName?: string
  public: boolean
  columns: Readonly<Record<string, OneDataColumn>>
  primaryKey: readonly string[]
}

export type OneDataConnection = Schema['relationships'][string][string][number]

export type OneDataSchema = {
  tables: Readonly<Record<string, OneDataTable>>
  relationships: Readonly<
    Record<string, Readonly<Record<string, readonly OneDataConnection[]>>>
  >
}
