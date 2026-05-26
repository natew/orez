import { walkAst } from './ast-utils.js'

/**
 * datetime pass.
 *
 * Rewrites PG datetime functions to SQLite-native equivalents:
 *   NOW()              → CURRENT_TIMESTAMP   (function call → keyword)
 *   CURRENT_TIMESTAMP() → CURRENT_TIMESTAMP  (if it ever parses as a FuncCall)
 *   CURRENT_DATE()     → CURRENT_DATE
 *   CURRENT_TIME()     → CURRENT_TIME
 *   pg_catalog.now()   → CURRENT_TIMESTAMP
 *
 * Why it matters: in SQLite, `DEFAULT NOW()` is rejected because column
 * defaults only accept a small expression grammar. `DEFAULT CURRENT_TIMESTAMP`
 * is accepted everywhere (and is what every "I want NOW() in a default" PG
 * user actually wants).
 *
 * For richer datetime work (EXTRACT, DATE_TRUNC, INTERVAL arithmetic) we'll
 * extend this pass in follow-ups. v1 covers the high-frequency cases.
 */
import type { Pass } from '../types.js'

function lowerFuncName(funcname: any[] | undefined): string | undefined {
  if (!funcname || funcname.length === 0) return undefined
  const last = funcname[funcname.length - 1]
  const str = last?.String?.sval ?? last?.String?.str
  return typeof str === 'string' ? str.toLowerCase() : undefined
}

/**
 * Build a PG `SQLValueFunction` node — this is the canonical AST representation
 * of bareword time keywords like CURRENT_TIMESTAMP / CURRENT_DATE / CURRENT_TIME.
 * The deparser emits these as bareword keywords (no quotes, no parens), and
 * SQLite accepts CURRENT_TIMESTAMP/CURRENT_DATE/CURRENT_TIME as keywords too.
 */
function svfWrapper(op: string): {
  SQLValueFunction: { op: string; typmod: number; location: number }
} {
  return {
    SQLValueFunction: { op, typmod: -1, location: 0 },
  }
}

export const datetimePass: Pass = {
  name: 'datetime',
  run(rawStmt, _ctx) {
    walkAst(rawStmt, {
      FuncCall: (node: any, parent: any, key: string | number) => {
        if (parent == null) return // can't replace a root-positioned FuncCall
        const name = lowerFuncName(node.funcname)
        if (!name) return
        const argless = !node.args || (Array.isArray(node.args) && node.args.length === 0)
        if (!argless) return

        // Map PG datetime function calls → SQL bareword time keywords. The PG
        // AST distinguishes function-form (`now()`) from keyword-form
        // (`CURRENT_TIMESTAMP`) at the parse level; SQLite only accepts the
        // keyword form in DEFAULT clauses and uniformly elsewhere.
        if (name === 'now' || name === 'current_timestamp') {
          parent[key] = svfWrapper('SVFOP_CURRENT_TIMESTAMP')
          return
        }
        if (name === 'current_date') {
          parent[key] = svfWrapper('SVFOP_CURRENT_DATE')
          return
        }
        if (name === 'current_time') {
          parent[key] = svfWrapper('SVFOP_CURRENT_TIME')
          return
        }
      },
    })
  },
}
