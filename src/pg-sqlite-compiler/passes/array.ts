import { walkAst } from './ast-utils.js'

import type { Pass, PassContext } from '../types.js'

/**
 * PG's `col = ANY(<array>)` is the portable variable-length IN list: one bind
 * slot holds the whole array, so callers don't have to build `IN ($1,$2,$3)`
 * per call. SQLite has no ANY/ALL operator, so passing it through fails at
 * prepare with "no such function: ANY" — the same shape as the json_agg gap.
 *
 * The rewrite mirrors what the DO proxy already does for its own SQL:
 *
 *   col = ANY(<array>)   →  col IN (SELECT value FROM json_each(<array>))
 *   col <> ALL(<array>)  →  NOT (col IN (SELECT value FROM json_each(<array>)))
 *
 * json_each() reads a JSON array, so the operand has to arrive as JSON text:
 *
 *   - ARRAY[...] / '{a,b}' literals are folded to a JSON string here.
 *   - a bind param can't be: its value shows up at execute, not compile. So we
 *     record its number in `arrayParamNumbers` and the caller JSON-encodes that
 *     slot. Everything else (a column already holding JSON text) passes through.
 *
 * `= ANY(SELECT …)` is a SubLink, not an A_Expr, and is left alone.
 */

function operatorName(expr: any): string | null {
  const parts = expr?.name
  if (!Array.isArray(parts) || parts.length === 0) return null
  const value = parts[parts.length - 1]?.String?.sval
  return typeof value === 'string' ? value : null
}

function stringConst(value: string): any {
  return { A_Const: { sval: { sval: value } } }
}

/** `{a,"b c",NULL}` → ['a', 'b c', null]; null when not an array literal. */
function parsePgArrayLiteral(value: string): unknown[] | null {
  if (value[0] !== '{' || value[value.length - 1] !== '}') return null
  const items: unknown[] = []
  let i = 1
  while (i < value.length - 1) {
    if (value[i] === ',') {
      i++
      continue
    }
    if (value[i] === '"') {
      i++
      let out = ''
      while (i < value.length - 1) {
        const ch = value[i]
        if (ch === '\\') {
          if (i + 1 < value.length - 1) out += value[i + 1]
          i += 2
          continue
        }
        if (ch === '"') {
          i++
          break
        }
        out += ch
        i++
      }
      items.push(out)
      continue
    }
    const start = i
    while (i < value.length - 1 && value[i] !== ',') i++
    const token = value.slice(start, i)
    items.push(token === 'NULL' ? null : token)
  }
  return items
}

const NON_LITERAL = Symbol('non-literal')

function literalValue(node: any): unknown | typeof NON_LITERAL {
  const constValue = node?.A_Const
  if (constValue) {
    if (Object.hasOwn(constValue, 'isnull')) return null
    if (Object.hasOwn(constValue, 'sval')) return constValue.sval?.sval ?? ''
    if (Object.hasOwn(constValue, 'ival')) return constValue.ival?.ival ?? 0
    if (Object.hasOwn(constValue, 'fval')) {
      const raw = constValue.fval?.fval ?? ''
      const parsed = Number(raw)
      return Number.isFinite(parsed) ? parsed : raw
    }
    if (Object.hasOwn(constValue, 'boolval')) return constValue.boolval?.boolval === true
  }
  if (node?.A_ArrayExpr) return arrayLiteralValue(node.A_ArrayExpr)
  return NON_LITERAL
}

function arrayLiteralValue(arrayExpr: any): unknown[] | typeof NON_LITERAL {
  const values: unknown[] = []
  for (const element of arrayExpr?.elements ?? []) {
    const value = literalValue(element)
    if (value === NON_LITERAL) return NON_LITERAL
    values.push(value)
  }
  return values
}

/** Strip casts down to the node that carries the value (`$1::text[]` → `$1`). */
function unwrapTypeCast(node: any): any {
  let current = node
  while (current?.TypeCast) current = current.TypeCast.arg
  return current
}

/**
 * Turn an ANY/ALL operand into something json_each() can read, recording the
 * bind slot when the value only exists at execute time.
 */
function jsonArrayOperand(node: any, ctx: PassContext): any {
  const inner = unwrapTypeCast(node)

  const paramNumber = inner?.ParamRef?.number
  if (typeof paramNumber === 'number') {
    ctx.arrayParamNumbers?.add(paramNumber)
    return inner
  }

  if (inner?.A_ArrayExpr) {
    const values = arrayLiteralValue(inner.A_ArrayExpr)
    if (values !== NON_LITERAL) return stringConst(JSON.stringify(values))
    return inner
  }

  const literal = inner?.A_Const?.sval?.sval
  if (typeof literal === 'string') {
    const items = parsePgArrayLiteral(literal)
    if (items) return stringConst(JSON.stringify(items))
  }

  return inner
}

function jsonEachSelect(operand: any): any {
  return {
    SelectStmt: {
      targetList: [
        {
          ResTarget: {
            val: { ColumnRef: { fields: [{ String: { sval: 'value' } }], location: -1 } },
            location: -1,
          },
        },
      ],
      fromClause: [
        {
          RangeFunction: {
            functions: [
              {
                List: {
                  items: [
                    {
                      FuncCall: {
                        funcname: [{ String: { sval: 'json_each' } }],
                        args: [operand],
                        funcformat: 'COERCE_EXPLICIT_CALL',
                        location: -1,
                      },
                    },
                    {},
                  ],
                },
              },
            ],
          },
        },
      ],
      limitOption: 'LIMIT_OPTION_DEFAULT',
      op: 'SETOP_NONE',
    },
  }
}

/** `testexpr IN (SELECT value FROM json_each(operand))` */
function inJsonEach(testexpr: any, operand: any): any {
  return {
    SubLink: {
      subLinkType: 'ANY_SUBLINK',
      testexpr,
      subselect: jsonEachSelect(operand),
      location: -1,
    },
  }
}

export const arrayPass: Pass = {
  name: 'array',
  run(rawStmt, ctx) {
    walkAst(rawStmt, {
      A_Expr: (node: any, parent: any, key: string | number) => {
        if (!parent) return
        const op = operatorName(node)

        if (node.kind === 'AEXPR_OP_ANY' && op === '=') {
          parent[key] = inJsonEach(node.lexpr, jsonArrayOperand(node.rexpr, ctx))
          return
        }

        if (node.kind === 'AEXPR_OP_ALL' && op === '<>') {
          parent[key] = {
            BoolExpr: {
              boolop: 'NOT_EXPR',
              args: [inJsonEach(node.lexpr, jsonArrayOperand(node.rexpr, ctx))],
              location: -1,
            },
          }
          return
        }

        // every other ANY/ALL operator (> ANY, <= ALL, LIKE ANY …) has no
        // json_each equivalent that preserves semantics, so say so rather than
        // emit SQL that dies at prepare.
        if (node.kind === 'AEXPR_OP_ANY' || node.kind === 'AEXPR_OP_ALL') {
          const form = node.kind === 'AEXPR_OP_ANY' ? 'ANY' : 'ALL'
          ctx.warnings.push({
            kind: 'unsupported-array-operator',
            near: 'A_Expr',
            message: `${op ?? '?'} ${form}(...) is not supported by pg-to-sqlite`,
          })
        }
      },
    })
  },
}
