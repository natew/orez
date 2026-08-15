import type { ApplicationDatabase, ApplicationTransaction } from './types.js'

export { encodeSqlParams, encodeSqlValue } from './sql-wire.js'
export type { SqlWireValue } from './sql-wire.js'

export function createSQLiteApplicationDatabase(options: {
  transaction<Value>(
    work: (tx: ApplicationTransaction) => Value | Promise<Value>
  ): Promise<Value>
  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[]
  ): Promise<readonly Row[]>
}): ApplicationDatabase {
  return options
}
