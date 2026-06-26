/**
 * Cloudflare DO-SQLite compatibility for a zero-cache embed.
 *
 * zero-cache's embedded SQLite issues storage-engine statements (VACUUM, ATTACH,
 * journal/checkpoint PRAGMAs) that a Durable Object's `ctx.storage.sql` does NOT
 * own — the DO manages journaling + checkpointing itself and rejects them with
 * SQLITE_AUTH, which aborts `/sync`. these helpers make those statements no-op so
 * the embed boots unmodified, while every real read/write passes straight
 * through. pure over the minimal `exec` shape (no `@cloudflare/...` types),
 * tested in zero-cache-do-sqlite.test.ts.
 */

interface DoExecSql {
  exec(sql: string, ...params: unknown[]): unknown
  [key: string]: unknown
}

interface DoStorageCtx {
  storage: {
    sql: DoExecSql
    transactionSync?: (...args: unknown[]) => unknown
  }
}

// storage-engine statements the DO rejects (it owns journaling/checkpointing).
export const DO_FORBIDDEN_SQLITE =
  /^\s*(?:VACUUM\b|ATTACH\b|PRAGMA\s+(?:journal_mode|synchronous|page_size|mmap_size|wal_checkpoint|wal_autocheckpoint|locking_mode|temp_store|cache_size)\b)/i

export function isDoForbiddenSqlite(sql: unknown): boolean {
  return typeof sql === 'string' && DO_FORBIDDEN_SQLITE.test(sql)
}

const GUARD_MARK = '__orezDoSqliteGuarded'

/**
 * patch `sql.exec` in place so EVERY caller (the embed, its sqlite shim,
 * transactionSync callbacks) skips DO-forbidden statements instead of throwing
 * SQLITE_AUTH. idempotent per handle.
 */
export function installDoForbiddenSqliteGuard(sql: DoExecSql): void {
  if (!sql || sql[GUARD_MARK]) return
  const rawExec = (sql.exec as (sql: string, ...params: unknown[]) => unknown).bind(sql)
  sql.exec = (statement: string, ...params: unknown[]) => {
    if (isDoForbiddenSqlite(statement)) {
      return {
        toArray: () => [],
        rowsRead: 0,
        rowsWritten: 0,
        columnNames: [],
        [Symbol.iterator]: () => [][Symbol.iterator](),
      }
    }
    return rawExec(statement, ...params)
  }
  sql[GUARD_MARK] = true
}

/**
 * wrap a DO's `storage.sql` for the zero-cache embed: DO-forbidden statements
 * no-op, and `transactionSync` is bound through when the platform exposes it.
 */
export function doSqliteStorage(ctx: DoStorageCtx): {
  exec: (sql: string, ...params: unknown[]) => unknown
  transactionSync: ((...args: unknown[]) => unknown) | undefined
} {
  const rawExec = (
    ctx.storage.sql.exec as (sql: string, ...params: unknown[]) => unknown
  ).bind(ctx.storage.sql)
  const exec = (sql: string, ...params: unknown[]) => {
    if (isDoForbiddenSqlite(sql)) {
      return {
        toArray: () => [],
        rowsRead: 0,
        [Symbol.iterator]: () => [][Symbol.iterator](),
      }
    }
    return rawExec(sql, ...params)
  }
  return {
    exec,
    transactionSync:
      typeof ctx.storage.transactionSync === 'function'
        ? ctx.storage.transactionSync.bind(ctx.storage)
        : undefined,
  }
}
