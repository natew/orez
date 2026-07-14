import { walkAst } from './ast-utils.js'

import type { Pass } from '../types.js'

/**
 * PG's JSON aggregates and constructors have direct SQLite equivalents with
 * the same argument shapes, but different names. Zero's z2s query compiler
 * emits these (json_agg over json_build_object rows is its relationship
 * hydration shape), so passing them through breaks every consumer on real
 * SQLite with "no such function: json_agg".
 *
 * jsonb variants map to the same SQLite functions: SQLite has one JSON text
 * representation, so the jsonb/json distinction erases.
 */
const SQLITE_FUNCTION_BY_PG_JSON_FUNCTION = new Map([
  ['json_agg', 'json_group_array'],
  ['jsonb_agg', 'json_group_array'],
  ['json_build_object', 'json_object'],
  ['jsonb_build_object', 'json_object'],
  ['json_build_array', 'json_array'],
  ['jsonb_build_array', 'json_array'],
  ['json_object_agg', 'json_group_object'],
  ['jsonb_object_agg', 'json_group_object'],
])

function functionName(func: any): string | null {
  const parts = func?.funcname
  if (!Array.isArray(parts) || parts.length === 0) return null
  const last = parts[parts.length - 1]
  const value = last?.String?.sval ?? last?.String?.str
  return typeof value === 'string' ? value.toLowerCase() : null
}

export const jsonFunctionsPass: Pass = {
  name: 'json-functions',
  run(rawStmt, ctx) {
    walkAst(rawStmt, {
      FuncCall: (node: any) => {
        const name = functionName(node)
        if (!name) return
        const replacement = SQLITE_FUNCTION_BY_PG_JSON_FUNCTION.get(name)
        if (!replacement) return
        // SQLite aggregates accept no ORDER BY / FILTER inside the call; a
        // silent rename would change semantics, so surface it instead.
        if (node.agg_order || node.agg_filter) {
          ctx.warnings.push({
            kind: 'unsupported-function',
            near: 'FuncCall',
            message: `${name}() with ORDER BY/FILTER has no SQLite equivalent`,
          })
          return
        }
        node.funcname = [{ String: { sval: replacement } }]
        node.funcformat = 'COERCE_EXPLICIT_CALL'
      },
    })
  },
}
