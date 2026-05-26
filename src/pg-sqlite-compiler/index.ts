/**
 * pg-sqlite-compiler — PostgreSQL SQL → SQLite SQL.
 *
 * Single-pass visitor over the libpg_query AST, emitting via pgsql-deparser.
 *
 * Public API:
 *   compile(pgSql, opts?) → { sql, warnings }
 *   compileMany(pgSqls, opts?) → results[]
 */
import { deparseSync, parseSync } from 'pgsql-parser'

import { runPasses } from './passes/index.js'

import type { CompileOptions, CompileResult, SchemaInfo } from './types.js'

const DEFAULT_VERSION = 170004

const NOOP_SCHEMA: SchemaInfo = {
  getColumnType: () => undefined,
  getEnum: () => undefined,
  isEnumValue: () => false,
  getTableColumns: () => undefined,
}

function stripTrailingSemicolon(s: string): string {
  let i = s.length
  while (i > 0 && (s[i - 1] === ';' || s[i - 1] === ' ' || s[i - 1] === '\n')) i--
  return s.slice(0, i)
}

/**
 * Compile a single PG SQL statement into a SQLite-compatible statement.
 *
 * The compiler is best-effort: it applies the registered passes and emits via
 * pgsql-deparser. Some PG-isms are not translatable; those produce warnings
 * but the SQL is still emitted (caller can decide to reject or run anyway).
 */
export function compile(pgSql: string, opts: CompileOptions = {}): CompileResult {
  const schema = opts.schema ?? NOOP_SCHEMA
  const version = opts.pgVersion ?? DEFAULT_VERSION
  const passes = opts.passes
  const warnings: CompileResult['warnings'] = []

  const trimmed = stripTrailingSemicolon(pgSql.trim())
  if (!trimmed) return { sql: '', warnings }

  // parseSync returns a ParseResult: { version, stmts: [{ stmt: {...} }, ...] }
  const parsed = parseSync(trimmed) as { version?: number; stmts?: any[] } | any
  const stmts: any[] = Array.isArray(parsed?.stmts) ? parsed.stmts : []
  if (stmts.length === 0) {
    return {
      sql: trimmed,
      warnings: [{ kind: 'parse-empty', message: 'no statements parsed' }],
    }
  }

  // Run all passes on each top-level RawStmt entry (so passes can walk from root).
  for (let i = 0; i < stmts.length; i++) {
    runPasses(stmts[i], { schema, warnings, passes })
  }

  const emitted = deparseSync({ version: parsed.version ?? version, stmts } as any)
  return { sql: stripTrailingSemicolon(emitted.trim()), warnings }
}

export function compileMany(
  pgSqls: string[],
  opts: CompileOptions = {}
): CompileResult[] {
  return pgSqls.map((s) => compile(s, opts))
}

export type { CompileOptions, CompileResult, SchemaInfo } from './types.js'
