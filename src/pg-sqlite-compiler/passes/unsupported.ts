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

function warn(
  ctx: Parameters<Pass['run']>[1],
  kind: string,
  near: string,
  message: string
) {
  ctx.warnings.push({ kind, near, message })
}

export const unsupportedPass: Pass = {
  name: 'unsupported',
  run(rawStmt, ctx) {
    walkAst(rawStmt, {
      FuncCall: (node: any) => {
        const name = functionName(node)
        if (!name || !UNSUPPORTED_FUNCTIONS.has(name)) return
        warn(
          ctx,
          'unsupported-function',
          'FuncCall',
          `${name}() is not supported by pg-to-sqlite`
        )
      },
      MinMaxExpr: (node: any) => {
        if (node?.op !== 'IS_GREATEST' && node?.op !== 'IS_LEAST') return
        const name = node.op === 'IS_GREATEST' ? 'greatest' : 'least'
        warn(
          ctx,
          'unsupported-function',
          'MinMaxExpr',
          `${name}() is not supported by pg-to-sqlite`
        )
      },
      SelectStmt: (node: any) => {
        const distinct = node?.distinctClause
        if (!Array.isArray(distinct)) return
        const hasDistinctOn = distinct.some(
          (item: any) => item && typeof item === 'object' && Object.keys(item).length > 0
        )
        if (!hasDistinctOn) return
        warn(
          ctx,
          'unsupported-syntax',
          'SelectStmt',
          'DISTINCT ON is not supported by pg-to-sqlite'
        )
      },
      RangeSubselect: (node: any) => {
        if (!node?.lateral) return
        warn(
          ctx,
          'unsupported-syntax',
          'RangeSubselect',
          'LATERAL is not supported by pg-to-sqlite'
        )
      },
      RangeFunction: (node: any) => {
        if (!node?.lateral) return
        warn(
          ctx,
          'unsupported-syntax',
          'RangeFunction',
          'LATERAL is not supported by pg-to-sqlite'
        )
      },
    })
  },
}
