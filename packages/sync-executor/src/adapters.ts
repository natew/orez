import type { ApplicationDatabase, ApplicationTransaction } from './types.js'

export function createSQLiteApplicationDatabase(options: {
  transaction<Value>(
    work: (tx: ApplicationTransaction) => Value | Promise<Value>
  ): Promise<Value>
  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[]
  ): Promise<readonly Row[]>
}): ApplicationDatabase {
  return { dialect: 'sqlite', ...options }
}
