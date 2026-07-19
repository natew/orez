export type VisibilityValue = string | number | boolean | null

export type VisibilityOperand =
  | {
      readonly type: 'column'
      readonly table: string
      readonly column: string
      readonly qualifier?: string
    }
  | { readonly type: 'value'; readonly value: VisibilityValue }

export type VisibilityExpression =
  | {
      readonly type: 'comparison'
      readonly operator: '=' | '!=' | '<' | '>' | '<=' | '>=' | 'IS' | 'IS NOT'
      readonly left: VisibilityOperand
      readonly right: VisibilityOperand
    }
  | { readonly type: 'and' | 'or'; readonly conditions: readonly VisibilityExpression[] }
  | {
      readonly type: 'exists'
      readonly table: string
      readonly qualifier?: string
      readonly where: VisibilityExpression
    }

export type VisibilityFilter =
  | {
      /** Structured expressions are compiled and validated at the Rust boundary. */
      readonly kind: 'expression'
      readonly expression: VisibilityExpression
      readonly sql?: never
      readonly params?: never
    }
  | {
      /** Raw SQL is accepted only when the schema has no encrypted columns. */
      readonly kind: 'raw'
      readonly sql: string
      readonly params?: readonly VisibilityValue[]
      readonly expression?: never
    }

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
