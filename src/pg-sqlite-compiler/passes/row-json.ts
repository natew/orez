import { walkAst } from './ast-utils.js'

import type { Pass } from '../types.js'

function stringValue(node: any): string | null {
  return node?.String?.sval ?? node?.String?.str ?? null
}

function functionName(func: any): string | null {
  const parts = func?.funcname
  if (!Array.isArray(parts) || parts.length === 0) return null
  return stringValue(parts[parts.length - 1])?.toLowerCase() ?? null
}

function columnName(value: any): string | null {
  const fields = value?.ColumnRef?.fields
  return Array.isArray(fields) ? stringValue(fields[fields.length - 1]) : null
}

function outputName(target: any, index: number): string {
  if (target?.name) return target.name
  return columnName(target?.val) ?? `_orez_col_${index + 1}`
}

function stringNode(value: string): any {
  return { A_Const: { sval: { sval: value }, location: -1 } }
}

function columnNode(alias: string, column: string): any {
  return {
    ColumnRef: {
      fields: [{ String: { sval: alias } }, { String: { sval: column } }],
      location: -1,
    },
  }
}

export const rowJsonPass: Pass = {
  name: 'row-json',
  run(rawStmt) {
    walkAst(rawStmt, {
      SelectStmt: (select) => {
        const shapes = new Map<string, string[]>()
        for (const fromNode of select?.fromClause ?? []) {
          const subselect = fromNode?.RangeSubselect
          const alias = subselect?.alias?.aliasname
          const targets = subselect?.subquery?.SelectStmt?.targetList
          if (!alias || !Array.isArray(targets)) continue
          shapes.set(
            alias,
            targets.map((target: any, index: number) =>
              outputName(target?.ResTarget, index)
            )
          )
        }
        if (shapes.size === 0) return

        walkAst(select.targetList ?? [], {
          FuncCall: (func, parent, key) => {
            if (functionName(func) !== 'row_to_json') return
            const alias = columnName(func.args?.[0])
            const columns = alias ? shapes.get(alias) : undefined
            if (!alias || !columns?.length) return
            parent[key] = {
              FuncCall: {
                funcname: [{ String: { sval: 'json_object' } }],
                args: columns.flatMap((column) => [
                  stringNode(column),
                  columnNode(alias, column),
                ]),
                funcformat: 'COERCE_EXPLICIT_CALL',
                location: -1,
              },
            }
          },
        })
      },
    })
  },
}
