import type { Schema } from 'orez-sync-executor/zero'

export type OneColumnKind =
  | 'string'
  | 'integer'
  | 'number'
  | 'boolean'
  | 'json'
  | 'timestamp'

export type OneLocalPersistence = 'device' | 'memory'

export type OneDeleteAction = 'cascade' | 'set null' | 'restrict'

export type OneColumnReference = {
  table: string
  column: string
  onDelete?: OneDeleteAction
}

export type OneDataColumn = {
  kind: OneColumnKind
  name?: string
  optional: boolean
  default?: unknown
  unique?: true
  references?: OneColumnReference
  value?: unknown
}

export type OneDataIndex = {
  columns: readonly string[]
  unique?: boolean
  where?: string
}

export type OneDataTable = {
  name: string
  dbName?: string
  public: boolean
  local?: OneLocalPersistence
  columns: Readonly<Record<string, OneDataColumn>>
  primaryKey: readonly string[]
  indexes?: Readonly<Record<string, OneDataIndex>>
}

export type OneDataConnection = Schema['relationships'][string][string][number]

export type OneDataSchema = {
  tables: Readonly<Record<string, OneDataTable>>
  relationships: Readonly<
    Record<string, Readonly<Record<string, readonly OneDataConnection[]>>>
  >
}
