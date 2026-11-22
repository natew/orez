import { where } from './where.js'

import type { Condition } from '../zero/zero.js'
import type { TableName, Where } from './types.js'

export function serverWhere<Table extends TableName, Builder extends Where<Table>>(
  tableName: Table,
  builder: Builder
): Where<Table, Condition>

export function serverWhere<
  Table extends TableName,
  Builder extends Where = Where<Table>,
>(builder: Builder): Where<Table, Condition>

export function serverWhere<Table extends TableName, Builder extends Where<Table>>(
  a: Table | Builder,
  b?: Builder
): Where<Table, any> | Builder {
  return where(a as any, b as any, true)
}
