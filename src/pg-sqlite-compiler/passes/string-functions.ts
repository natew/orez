import { walkAst } from './ast-utils.js'

import type { Pass } from '../types.js'

function functionName(funcname: any[] | undefined): string | undefined {
  if (!funcname || funcname.length === 0) return undefined
  const last = funcname[funcname.length - 1]
  const value = last?.String?.sval ?? last?.String?.str
  return typeof value === 'string' ? value.toLowerCase() : undefined
}

function stringNode(value: string): { String: { sval: string } } {
  return { String: { sval: value } }
}

/** Rewrite one libpg_query FuncCall to SQLite's ordinary trim functions. */
export function rewriteStringFunctionCall(node: any): boolean {
  const name = functionName(node?.funcname)
  if (name !== 'btrim' && name !== 'ltrim' && name !== 'rtrim') return false

  node.funcname = [stringNode(name === 'btrim' ? 'trim' : name)]
  node.funcformat = 'COERCE_EXPLICIT_CALL'
  return true
}

/**
 * PostgreSQL parses the SQL-standard TRIM forms as pg_catalog btrim/ltrim/rtrim
 * calls with a special deparse format. That format emits `TRIM(BOTH FROM x)`,
 * which SQLite rejects even though its trim functions have equivalent
 * argument ordering. Emit ordinary SQLite function calls instead.
 */
export const stringFunctionsPass: Pass = {
  name: 'string-functions',
  run(rawStmt, _ctx) {
    walkAst(rawStmt, {
      FuncCall: (node: any) => {
        rewriteStringFunctionCall(node)
      },
    })
  },
}
