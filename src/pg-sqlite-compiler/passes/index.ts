import { catalogPass } from './catalog.js'
import { datetimePass } from './datetime.js'
import { typesPass } from './types.js'

/**
 * Pass pipeline.
 *
 * Each pass is a focused visitor over the PG AST that mutates nodes in place
 * to make the tree SQLite-emittable. Order matters: type normalization runs
 * first (so other passes see SQLite-native type names), datetime runs after
 * (function-form → SQLValueFunction), catalog rewrites last (after every
 * other pass has stabilized).
 */
import type { Pass, PassContext } from '../types.js'

export const DEFAULT_PASSES: Pass[] = [
  typesPass,
  datetimePass,
  catalogPass,
  // future:
  //   castPass,
  //   arrayPass,
  //   jsonPass,
  //   insertPass,
]

/**
 * Run all passes on a single top-level RawStmt entry.
 *
 * Input shape: `{ stmt: { TagName: data } }` (a libpg_query RawStmt).
 * Passes use `walkAst()` (in passes/ast-utils.ts) which expects a tag-wrapped
 * node — so we hand them `rawStmt.stmt`, the inner `{ TagName: data }`.
 * Callbacks receive (data, parent, key) and mutate via `parent[key] = ...`.
 */
export function runPasses(rawStmt: any, ctx: PassContext): void {
  const stmt = rawStmt?.stmt ?? rawStmt
  if (!stmt || typeof stmt !== 'object') return
  const passes = ctx.passes ?? DEFAULT_PASSES
  for (const pass of passes) {
    try {
      pass.run(stmt, ctx)
    } catch (err: any) {
      ctx.warnings.push({
        kind: 'pass-error',
        message: `pass ${pass.name} threw: ${err.message}`,
      })
    }
  }
}
