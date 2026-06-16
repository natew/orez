import { walkAst } from './ast-utils.js'

import type { Pass } from '../types.js'

const UNSUPPORTED_FUNCTIONS = new Set([
  'date_trunc',
  'gen_random_uuid',
  'right',
  'unnest',
])

function functionName(func: any): string | null {
  const parts = func?.funcname
  if (!Array.isArray(parts) || parts.length === 0) return null
  const last = parts[parts.length - 1]
  const value = last?.String?.sval ?? last?.String?.str
  return typeof value === 'string' ? value.toLowerCase() : null
}

export const unsupportedPass: Pass = {
  name: 'unsupported',
  run(rawStmt, ctx) {
    walkAst(rawStmt, {
      FuncCall: (node: any) => {
        const name = functionName(node)
        if (!name || !UNSUPPORTED_FUNCTIONS.has(name)) return
        ctx.warnings.push({
          kind: 'unsupported-function',
          near: 'FuncCall',
          message: `${name}() is not supported by pg-to-sqlite`,
        })
      },
    })
  },
}
