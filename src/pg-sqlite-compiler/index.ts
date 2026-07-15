/**
 * pg-sqlite-compiler — PostgreSQL SQL → SQLite SQL.
 *
 * Single-pass visitor over the libpg_query AST, emitting via pgsql-deparser.
 *
 * Public API:
 *   compile(pgSql, opts?) → { sql, warnings }
 *   compileMany(pgSqls, opts?) → results[]
 */
import { loadModule } from 'pgsql-parser'

await loadModule()

export * from './compiler.js'
