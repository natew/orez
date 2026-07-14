import { walkAst } from './ast-utils.js'

import type { Pass } from '../types.js'

function stringNode(value: string): any {
  return { String: { sval: value } }
}

function stringValue(node: any): string | null {
  return node?.String?.sval ?? node?.String?.str ?? null
}

export function flattenSchemaName(schema: string, name: string): string {
  if (schema === 'public' && name === 'migrations') return 'public_migrations'
  if (schema === 'public') return name
  if (schema === '_orez' && name === '_zero_changes') return '_zero_changes'
  if (schema === '_orez' && name === '_zero_replication_slots')
    return '_orez__zero_replication_slots'
  if (schema === '_orez') return `_orez__${name}`
  if (schema === '_zero') return `_zero_${name}`
  return `${schema.replaceAll('/', '_')}_${name}`
}

function flattenRangeVar(rangeVar: any): string | null {
  if (!rangeVar?.schemaname || !rangeVar.relname) return rangeVar?.relname ?? null
  const flattened = flattenSchemaName(rangeVar.schemaname, rangeVar.relname)
  rangeVar.relname = flattened
  delete rangeVar.schemaname
  return flattened
}

function flattenColumnRef(columnRef: any): void {
  const fields = columnRef?.fields
  if (!Array.isArray(fields) || fields.length < 3) return
  const schema = stringValue(fields[0])
  const table = stringValue(fields[1])
  if (!schema || !table) return
  fields.splice(0, 2, stringNode(flattenSchemaName(schema, table)))
}

function rewriteColumnRefQualifier(node: any, from: string, to: string): void {
  if (!node || typeof node !== 'object') return
  if (Array.isArray(node)) {
    for (const item of node) rewriteColumnRefQualifier(item, from, to)
    return
  }
  const fields = node.ColumnRef?.fields
  if (Array.isArray(fields) && fields.length > 1 && stringValue(fields[0]) === from) {
    fields[0] = stringNode(to)
  }
  for (const child of Object.values(node)) {
    rewriteColumnRefQualifier(child, from, to)
  }
}

function flattenSelectRangeVarQualifiers(stmt: any): void {
  const visitFromNode = (node: any) => {
    if (!node || typeof node !== 'object') return
    const rangeVar = node.RangeVar
    if (rangeVar?.schemaname && rangeVar.relname) {
      const from = rangeVar.alias?.aliasname ? null : rangeVar.relname
      const to = flattenRangeVar(rangeVar)
      if (from && to) rewriteColumnRefQualifier(stmt, from, to)
      return
    }
    const join = node.JoinExpr
    if (join) {
      visitFromNode(join.larg)
      visitFromNode(join.rarg)
    }
  }
  for (const fromNode of stmt?.fromClause ?? []) visitFromNode(fromNode)
}

function flattenWriteTargetQualifiers(stmt: any): void {
  const relation = stmt?.relation
  const from =
    relation?.alias?.aliasname ?? (relation?.schemaname ? relation.relname : null)
  const to = flattenRangeVar(relation)
  if (!from || !to) return
  if (relation?.alias?.aliasname) delete relation.alias
  rewriteColumnRefQualifier(stmt, from, to)
}

export const schemaPass: Pass = {
  name: 'schema',
  run(rawStmt, ctx) {
    walkAst(rawStmt, {
      AlterTableCmd: (node: any) => {
        if (node.subtype !== 'AT_DropColumn' || !node.behavior) return
        if (node.behavior === 'DROP_CASCADE') {
          ctx.warnings.push({
            kind: 'unsupported-alter-table-cascade',
            message: 'SQLite DROP COLUMN does not support CASCADE',
          })
          return
        }
        // PostgreSQL deparses its default DROP_RESTRICT behavior as an
        // explicit RESTRICT clause. SQLite has the same restrictive default
        // but rejects the keyword, so omit it from the emitted statement.
        delete node.behavior
      },
      SelectStmt: (node: any) => {
        flattenSelectRangeVarQualifiers(node)
      },
      InsertStmt: (node: any) => {
        flattenWriteTargetQualifiers(node)
      },
      UpdateStmt: (node: any) => {
        flattenWriteTargetQualifiers(node)
      },
      DeleteStmt: (node: any) => {
        flattenRangeVar(node.relation)
      },
      RangeVar: (node: any) => {
        flattenRangeVar(node)
      },
      ColumnRef: (node: any) => {
        flattenColumnRef(node)
      },
    })
  },
}
