import type {
  VisibilityExpression,
  VisibilityFilter,
  VisibilityOperand,
  VisibilityValue,
} from 'orez-sync-executor'

export type {
  VisibilityExpression,
  VisibilityFilter,
  VisibilityOperand,
  VisibilityValue,
} from 'orez-sync-executor'

export const visibility = {
  column(table: string, column: string, qualifier?: string): VisibilityOperand {
    return qualifier
      ? { type: 'column', table, column, qualifier }
      : { type: 'column', table, column }
  },
  value(value: VisibilityValue): VisibilityOperand {
    return { type: 'value', value }
  },
  comparison(
    left: VisibilityOperand,
    operator: '=' | '!=' | '<' | '>' | '<=' | '>=' | 'IS' | 'IS NOT',
    right: VisibilityOperand
  ): VisibilityExpression {
    return { type: 'comparison', operator, left, right }
  },
  and(...conditions: VisibilityExpression[]): VisibilityExpression {
    return { type: 'and', conditions }
  },
  or(...conditions: VisibilityExpression[]): VisibilityExpression {
    return { type: 'or', conditions }
  },
  exists(
    table: string,
    where: VisibilityExpression,
    qualifier?: string
  ): VisibilityExpression {
    return qualifier
      ? { type: 'exists', table, qualifier, where }
      : { type: 'exists', table, where }
  },
  filter(expression: VisibilityExpression): VisibilityFilter {
    return { kind: 'expression', expression }
  },
  raw(sql: string, params: readonly VisibilityValue[] = []): VisibilityFilter {
    return { kind: 'raw', sql, params }
  },
} as const
