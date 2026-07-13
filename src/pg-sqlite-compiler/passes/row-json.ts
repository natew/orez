import { walkAst } from './ast-utils.js'

import type { Pass, PassContext } from '../types.js'

function stringValue(node: any): string | null {
  return node?.String?.sval ?? node?.String?.str ?? null
}

function functionName(func: any): string | null {
  const parts = func?.funcname
  if (!Array.isArray(parts) || parts.length === 0) return null
  return stringValue(parts[parts.length - 1])?.toLowerCase() ?? null
}

/** last name of a ColumnRef (e.g. `s.id` → `id`), or null for `*` / non-columns. */
function columnName(value: any): string | null {
  const fields = value?.ColumnRef?.fields
  if (!Array.isArray(fields) || fields.length === 0) return null
  return stringValue(fields[fields.length - 1])
}

/** true when the ColumnRef ends in `*` (bare `*` or qualified `t.*`). */
function isStarColumnRef(value: any): boolean {
  const fields = value?.ColumnRef?.fields
  if (!Array.isArray(fields) || fields.length === 0) return false
  return fields[fields.length - 1]?.A_Star !== undefined
}

/** qualifier of a `t.*` star (the `t`), or null for a bare `*`. */
function starQualifier(value: any): string | null {
  const fields = value?.ColumnRef?.fields
  if (!Array.isArray(fields) || fields.length < 2) return null
  return stringValue(fields[0])
}

/** gather every RangeVar reachable through a FROM clause, descending JoinExprs. */
function collectRangeVars(node: any, out: any[]): void {
  if (!node || typeof node !== 'object') return
  if (Array.isArray(node)) {
    for (const child of node) collectRangeVars(child, out)
    return
  }
  if (node.RangeVar) {
    out.push(node.RangeVar)
    return
  }
  if (node.JoinExpr) {
    collectRangeVars(node.JoinExpr.larg, out)
    collectRangeVars(node.JoinExpr.rarg, out)
  }
  // RangeSubselect etc. expose no schema-resolvable columns, so the caller
  // declines when a star can't be expanded from a real table.
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

function decline(ctx: PassContext, message: string): null {
  ctx.warnings.push({ kind: 'row-json-unsupported', message, near: 'row_to_json' })
  return null
}

/**
 * Expand a `*` / `t.*` star into the real column names of its source table,
 * using the authoritative schema. Returns null (already warned) when the shape
 * can't be proven, and never invents column names.
 */
function expandStar(
  fromClause: any,
  qualifier: string | null,
  ctx: PassContext
): string[] | null {
  let rv: any
  if (qualifier == null) {
    // a bare `*` covers EVERY source in the FROM clause, so it is only
    // resolvable when that clause is exactly one base table. A second table, a
    // sub-select, a function, or a join contributes columns a single table's
    // shape can't account for, and silently expanding just the one table would
    // drop the rest. Decline instead of guessing.
    rv =
      Array.isArray(fromClause) && fromClause.length === 1
        ? fromClause[0]?.RangeVar
        : undefined
    if (!rv) {
      return decline(
        ctx,
        'row_to_json over SELECT * needs the from clause to be a single known table'
      )
    }
  } else {
    // a qualified `t.*` names exactly one table, so it stays resolvable even
    // amid other FROM sources — find that table wherever it sits.
    const rangeVars: any[] = []
    collectRangeVars(fromClause, rangeVars)
    rv = rangeVars.find((v) => (v.alias?.aliasname ?? v.relname) === qualifier)
    if (!rv) {
      return decline(ctx, `row_to_json star qualifier ${qualifier} matches no table`)
    }
  }

  const columns = ctx.schema.getTableColumns(rv.schemaname ?? 'public', rv.relname)
  if (!columns || columns.length === 0) {
    return decline(
      ctx,
      `row_to_json over ${rv.relname}.* needs schema info that is unavailable`
    )
  }
  return [...columns]
}

/**
 * Resolve the output column names a subquery produces, so row_to_json can be
 * rewritten to json_object() referencing real columns. Returns null (already
 * warned) when the shape can't be proven. Any synthetic aliases it needs are
 * applied to the subquery targets only on success, so a declined rewrite leaves
 * the AST untouched.
 */
function resolveShape(subselect: any, ctx: PassContext): string[] | null {
  const sub = subselect?.subquery?.SelectStmt
  const targets = sub?.targetList
  if (!Array.isArray(targets) || targets.length === 0) {
    return decline(ctx, 'row_to_json subquery has no resolvable target list')
  }

  const resolvedTargets: Array<{
    target: any
    columns: string[]
    needsAlias: boolean
  }> = []

  for (let i = 0; i < targets.length; i++) {
    const target = targets[i]?.ResTarget
    if (!target) return decline(ctx, 'row_to_json subquery has an unrecognized target')

    // an explicit alias is already the real output column name
    if (target.name) {
      resolvedTargets.push({ target, columns: [target.name], needsAlias: false })
      continue
    }

    const val = target.val

    // `*` / `t.*`: expand from the authoritative table shape or decline
    if (isStarColumnRef(val)) {
      const expanded = expandStar(sub.fromClause, starQualifier(val), ctx)
      if (!expanded) return null
      resolvedTargets.push({ target, columns: expanded, needsAlias: false })
      continue
    }

    // a plain column reference already names its output column (`s.id` → `id`)
    const named = columnName(val)
    if (named) {
      resolvedTargets.push({ target, columns: [named], needsAlias: false })
      continue
    }

    resolvedTargets.push({ target, columns: [], needsAlias: true })
  }

  // The rewrite addresses each subquery column as `alias.<name>`, so the real
  // (non-generated) output names must be unique or that reference is ambiguous:
  // duplicate columns (a.id, b.id), duplicate explicit aliases, and a star that
  // re-expands an explicitly-listed column all make two outputs share a name,
  // and SQLite silently resolves the reference to just one, losing the other.
  // SQLite matches column names case-insensitively, so compare case-folded.
  // Decline before mutating any target so a declined rewrite leaves the AST
  // untouched.
  const usedNames = new Set<string>()
  for (const resolved of resolvedTargets) {
    if (resolved.needsAlias) continue
    for (const column of resolved.columns) {
      const key = column.toLowerCase()
      if (usedNames.has(key)) {
        return decline(
          ctx,
          `row_to_json subquery has duplicate output column ${column}; cannot address it unambiguously`
        )
      }
      usedNames.add(key)
    }
  }

  for (let i = 0; i < resolvedTargets.length; i++) {
    const resolved = resolvedTargets[i]
    if (!resolved.needsAlias) continue

    // an unnamed expression has no stable output name in SQLite. Give it a
    // real alias that cannot collide with another target or expanded star.
    let suffix = i + 1
    let generated = `_orez_col_${suffix}`
    while (usedNames.has(generated.toLowerCase())) generated = `_orez_col_${++suffix}`
    resolved.target.name = generated
    resolved.columns = [generated]
    usedNames.add(generated.toLowerCase())
  }
  return resolvedTargets.flatMap((target) => target.columns)
}

export const rowJsonPass: Pass = {
  name: 'row-json',
  run(rawStmt, ctx) {
    walkAst(rawStmt, {
      SelectStmt: (select) => {
        const subselects = new Map<string, any>()
        for (const fromNode of select?.fromClause ?? []) {
          const subselect = fromNode?.RangeSubselect
          const alias = subselect?.alias?.aliasname
          if (alias && subselect?.subquery?.SelectStmt) {
            subselects.set(alias, subselect)
          }
        }
        if (subselects.size === 0) return

        // resolve each referenced subquery shape at most once (and warn once)
        const resolved = new Map<string, string[] | null>()
        const shapeFor = (alias: string): string[] | null => {
          if (!resolved.has(alias)) {
            resolved.set(alias, resolveShape(subselects.get(alias), ctx))
          }
          return resolved.get(alias) ?? null
        }

        walkAst(select.targetList ?? [], {
          FuncCall: (func, parent, key) => {
            if (functionName(func) !== 'row_to_json') return
            const alias = columnName(func.args?.[0])
            if (!alias || !subselects.has(alias)) return
            const columns = shapeFor(alias)
            if (!columns || columns.length === 0) return
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
