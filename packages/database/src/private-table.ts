// marks a Drizzle table as private: it stays out of Zero replication. the
// symbol key is shared with @take-out/database so a table marked by either
// package reads as private to the other while consumers migrate.
const PRIVATE = Symbol.for('take-out/database/private')

export function privateTable<T extends (...args: any[]) => any>(createTable: T): T {
  return ((...args: any[]) => {
    const table = createTable(...args)
    ;(table as any)[PRIVATE] = true
    return table
  }) as unknown as T
}

export function isPrivateTable(table: unknown): boolean {
  return !!(table && typeof table === 'object' && PRIVATE in table)
}
