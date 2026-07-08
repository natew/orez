/**
 * Data-modifying CTEs (`WITH x AS (DELETE/INSERT/UPDATE …) …`) are
 * Postgres-only; SQLite rejects DML inside a WITH clause.
 *
 * The one shape we translate — because zero-cache's changeLog purge depends
 * on it (zero 1.6 storer.js: a leading `keep` boundary CTE, a
 * `purged AS (DELETE … RETURNING …)` CTE, and `SELECT COUNT(*) FROM purged`)
 * — is the "counted delete": an outer SELECT that only counts the rows a
 * single DELETE CTE removed. It restructures into a top-level
 * `WITH <other ctes> DELETE … RETURNING 1 AS "__orez_count__<col>"`, which
 * SQLite executes natively (plain CTEs ARE allowed on a DELETE).
 *
 * The result shape changes (N marker rows instead of one count row), so the
 * emitted SQL is self-describing: the RETURNING alias carries the count
 * column name behind COUNT_MARKER_PREFIX. Runtimes executing compiled SQL
 * (orez's DoBackend) detect the marker and fold the rows back into
 * `[{ <col>: rowCount }]` — see foldCountMarkerResult. Left untranslated,
 * every changeLog purge tick 500s on the DO backend and the log grows
 * forever (the 2026-07 CF rows-written burn).
 *
 * Every other data-modifying CTE gets a warning (strict mode rejects it).
 */
import { walkAst } from './ast-utils.js'

import type { Pass } from '../types.js'

export const COUNT_MARKER_PREFIX = '__orez_count__'

const DML_TAGS = ['DeleteStmt', 'InsertStmt', 'UpdateStmt'] as const

function cteDmlTag(cte: any): (typeof DML_TAGS)[number] | null {
  const query = cte?.CommonTableExpr?.ctequery
  if (!query || typeof query !== 'object') return null
  for (const tag of DML_TAGS) {
    if (query[tag]) return tag
  }
  return null
}

function isCountStarTarget(target: any): boolean {
  const func = target?.ResTarget?.val?.FuncCall
  if (!func?.agg_star) return false
  const parts = func.funcname
  if (!Array.isArray(parts) || parts.length === 0) return false
  const last = parts[parts.length - 1]
  const name = last?.String?.sval ?? last?.String?.str
  return typeof name === 'string' && name.toLowerCase() === 'count'
}

function referencesRelation(node: any, name: string): boolean {
  let found = false
  walkAst(node, {
    RangeVar: (rangeVar: any) => {
      if (!rangeVar?.schemaname && rangeVar?.relname === name) found = true
    },
  })
  return found
}

/**
 * Restructure a counted-delete CTE statement in place. `stmt` is the inner
 * tag-wrapped statement node (`{ SelectStmt: … }`). Returns the count column
 * name when the statement matched and was rewritten, else null (untouched).
 */
export function transformCountedDeleteCte(stmt: any): { countColumn: string } | null {
  const sel = stmt?.SelectStmt
  if (!sel?.withClause?.ctes || sel.withClause.recursive) return null
  if (
    sel.op !== 'SETOP_NONE' ||
    sel.whereClause ||
    sel.groupClause ||
    sel.havingClause ||
    sel.sortClause ||
    sel.limitCount ||
    sel.limitOffset ||
    sel.distinctClause ||
    sel.valuesLists
  ) {
    return null
  }
  const ctes: any[] = sel.withClause.ctes
  const dmlIndexes = ctes
    .map((cte, index) => (cteDmlTag(cte) ? index : -1))
    .filter((index) => index >= 0)
  if (dmlIndexes.length !== 1) return null
  const dmlIndex = dmlIndexes[0]
  const dmlCte = ctes[dmlIndex].CommonTableExpr
  if (cteDmlTag(ctes[dmlIndex]) !== 'DeleteStmt') return null
  const deleteStmt = dmlCte.ctequery.DeleteStmt

  // outer select must be exactly `SELECT COUNT(*) [AS col] FROM <dml cte>`
  if (sel.targetList?.length !== 1 || !isCountStarTarget(sel.targetList[0])) return null
  const from = sel.fromClause
  if (from?.length !== 1) return null
  const fromRel = from[0]?.RangeVar
  if (!fromRel || fromRel.schemaname || fromRel.relname !== dmlCte.ctename) return null

  // the delete CTE must not be read anywhere else (another CTE consuming its
  // RETURNING rows cannot be preserved once the DELETE moves to the top level)
  const others = ctes.filter((_, index) => index !== dmlIndex)
  if (
    others.some((cte) =>
      referencesRelation(cte.CommonTableExpr?.ctequery, dmlCte.ctename)
    )
  ) {
    return null
  }

  const countColumn = sel.targetList[0].ResTarget?.name ?? 'count'
  deleteStmt.returningList = [
    {
      ResTarget: {
        name: `${COUNT_MARKER_PREFIX}${countColumn}`,
        val: { A_Const: { ival: { ival: 1 } } },
      },
    },
  ]
  if (others.length) {
    deleteStmt.withClause = { ...sel.withClause, ctes: others }
  }
  delete stmt.SelectStmt
  stmt.DeleteStmt = deleteStmt
  return { countColumn }
}

/**
 * Fold an executed counted-delete result back into the original statement's
 * shape: N marker rows → `[{ <col>: N }]`. `columnsOrSql` is either the
 * result column list or the executed SQL (for zero-row results, where some
 * drivers report no columns). Returns null when the marker is absent.
 */
export function foldCountMarkerResult(
  rowCount: number,
  columnsOrSql: readonly string[] | string
): { rows: Array<Record<string, number>>; columns: string[] } | null {
  // the deparser quotes the alias only when required, so match both forms
  const column =
    typeof columnsOrSql === 'string'
      ? (columnsOrSql.match(new RegExp(`"${COUNT_MARKER_PREFIX}([^"]+)"`))?.[1] ??
        columnsOrSql.match(new RegExp(`\\b${COUNT_MARKER_PREFIX}(\\w+)\\b`))?.[1])
      : columnsOrSql.length === 1 && columnsOrSql[0].startsWith(COUNT_MARKER_PREFIX)
        ? columnsOrSql[0].slice(COUNT_MARKER_PREFIX.length)
        : undefined
  if (!column) return null
  return { rows: [{ [column]: rowCount }], columns: [column] }
}

export const dmlCtePass: Pass = {
  name: 'dml-cte',
  run(stmt, ctx) {
    transformCountedDeleteCte(stmt)
    // anything still holding DML inside a CTE is untranslatable
    walkAst(stmt, {
      CommonTableExpr: (cte: any) => {
        const query = cte?.ctequery
        if (!query || typeof query !== 'object') return
        for (const tag of DML_TAGS) {
          if (query[tag]) {
            ctx.warnings.push({
              kind: 'data-modifying-cte',
              near: 'CommonTableExpr',
              message:
                'data-modifying CTEs are Postgres-only (SQLite rejects DML in WITH); only the counted-delete shape is translated',
            })
            return
          }
        }
      },
    })
  },
}
