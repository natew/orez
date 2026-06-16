/**
 * Schema metadata available to passes for type-aware translation.
 *
 * Implementations are caller-provided. The default NOOP_SCHEMA returns nothing
 * and passes degrade gracefully (regex/AST-shape fallbacks).
 *
 * For orez, the eventual implementation will read from `ctx.storage.sql`
 * PRAGMA + the `_orez_pg_metadata` catalog table.
 */
export interface SchemaInfo {
  /** PG type name (e.g. "jsonb", "text[]") for a column, or undefined. */
  getColumnType(schema: string, table: string, column: string): string | undefined

  /** ENUM definition lookup by PG type name. */
  getEnum(typeName: string): EnumInfo | undefined

  /** Validate an ENUM literal value. */
  isEnumValue(typeOid: number, label: string): boolean

  /** Column list for a table (for SELECT * expansion, RETURNING * etc.). */
  getTableColumns(schema: string, table: string): readonly string[] | undefined
}

export interface EnumInfo {
  typeOid: number
  values: readonly string[]
}

export interface CompileWarning {
  kind: string
  message: string
  /** PG AST node tag where the warning was raised. */
  near?: string
}

export interface CompileResult {
  sql: string
  warnings: CompileWarning[]
}

export interface CompileOptions {
  schema?: SchemaInfo
  pgVersion?: number
  /** Throw instead of returning SQL when compilation produces warnings. */
  strict?: boolean
  /** Override pass list (mainly for testing individual passes). */
  passes?: Pass[]
}

/** Context passed to every pass. */
export interface PassContext {
  schema: SchemaInfo
  warnings: CompileWarning[]
  /**
   * Optional pass list — if set, runPasses uses these instead of the default
   * pipeline. Otherwise the full default pipeline runs.
   */
  passes?: Pass[]
}

/** A pass is a function that mutates a RawStmt in place. */
export interface Pass {
  name: string
  run(rawStmt: any, ctx: PassContext): void
}
