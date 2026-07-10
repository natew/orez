/**
 * runaway-write circuit breaker for a Durable Object's SQLite storage.
 *
 * a ZeroSqlDO (orez `ZeroDO`) runs the pg-over-DO backend in-instance, so a
 * buggy or malicious mutation flow can write unbounded rows into the DO's
 * SQLite and blow past the platform's per-object storage + billing limits
 * before anything notices. this wraps `sql.exec` to meter rows written per
 * rolling window and trip — refusing further writes — once the rate stays over
 * a soft threshold for a sustained period, or instantly past a hard threshold.
 *
 * the meter state lives in a single-row table in the same SQLite, so a tripped
 * breaker survives DO eviction (the object stays bricked-for-writes until an
 * operator clears the row). reads are never gated. the wrap is idempotent per
 * `sql` handle.
 *
 * pure logic over the minimal `DurableSqlStorage` shape (no `@cloudflare/...`
 * types), unit-tested in zero-sql-write-circuit.test.ts. consumers install it
 * from their ZeroSqlDO constructor with their own table/log prefix.
 */

export interface DurableSqlCursor {
  one(): Record<string, unknown> | undefined
  rowsWritten?: number
}

export interface DurableSqlStorage {
  exec(sql: string, ...params: unknown[]): DurableSqlCursor
}

export interface WriteCircuitOptions {
  /** single-row meter table name. default `_orez_write_circuit`. */
  table?: string
  /** soft cap: rows/window above which the sustained timer starts. default 2,000,000. */
  rowsPerWindow?: number
  /** hard cap: rows/window that trips instantly. default 10,000,000. */
  hardRowsPerWindow?: number
  /** rolling window length in ms. default 60,000. */
  windowMs?: number
  /** how long the rate must stay over the soft cap before tripping, in ms. default 180,000. */
  sustainedMs?: number
  /** log prefix for the trip diagnostics. default `[orez]`. */
  logPrefix?: string
}

// any statement that can write rows (so reads never pay the meter cost).
const MUTATION_RE =
  /^\s*(?:insert|update|delete|replace|create|alter|drop|truncate|vacuum|reindex)\b/i
// a WITH statement only writes when a data-modifying clause follows the CTEs;
// WITH ... SELECT is a read and must stay open while tripped (2026-07-10
// incident: gating `with` wholesale refused CTE reads during recovery). a
// keyword match inside a literal/identifier only over-gates, never under-gates:
// a real WITH write always carries insert/update/delete/replace.
const WITH_RE = /^\s*with\b/i
const WITH_MUTATION_RE = /\b(?:insert|update|delete|replace)\b/i

const isMutationStatement = (text: string): boolean =>
  MUTATION_RE.test(text) || (WITH_RE.test(text) && WITH_MUTATION_RE.test(text))

const INSTALLED = new WeakSet<DurableSqlStorage>()

/**
 * wrap `sql.exec` with the runaway-write circuit breaker. idempotent: a second
 * call on the same `sql` handle is a no-op.
 */
export function installZeroSqlWriteCircuitBreaker(
  sql: DurableSqlStorage,
  opts: WriteCircuitOptions = {}
): void {
  if (!sql || INSTALLED.has(sql)) return
  const table = opts.table ?? '_orez_write_circuit'
  const rowsPerWindow = opts.rowsPerWindow ?? 2_000_000
  const hardRowsPerWindow = opts.hardRowsPerWindow ?? 10_000_000
  const windowMs = opts.windowMs ?? 60 * 1000
  const sustainedMs = opts.sustainedMs ?? 3 * 60 * 1000
  const logPrefix = opts.logPrefix ?? '[orez]'

  const rawExec = sql.exec.bind(sql)
  let ready = false

  const ensureReady = () => {
    if (ready) return
    rawExec(
      'CREATE TABLE IF NOT EXISTS ' +
        table +
        ' (id INTEGER PRIMARY KEY CHECK (id = 1), window_start INTEGER NOT NULL DEFAULT 0, rows_in_window INTEGER NOT NULL DEFAULT 0, first_over_at INTEGER NOT NULL DEFAULT 0, tripped_at INTEGER NOT NULL DEFAULT 0, last_statement TEXT)'
    )
    rawExec(
      'INSERT OR IGNORE INTO ' +
        table +
        ' (id, window_start, rows_in_window, first_over_at, tripped_at, last_statement) VALUES (1, 0, 0, 0, 0, ?)',
      ''
    )
    ready = true
  }

  const readState = (): Record<string, unknown> => {
    ensureReady()
    return (
      rawExec(
        'SELECT window_start, rows_in_window, first_over_at, tripped_at FROM ' +
          table +
          ' WHERE id = 1'
      ).one() || {}
    )
  }

  const clippedStatement = (statement: unknown) =>
    String(statement || '')
      .replace(/\s+/g, ' ')
      .trim()
      .slice(0, 500)

  const assertOpen = (statement: unknown) => {
    const state = readState()
    const trippedAt = Number(state.tripped_at || 0)
    if (trippedAt > 0) {
      throw new Error(
        logPrefix +
          ' ZeroSqlDO write circuit breaker tripped at ' +
          new Date(trippedAt).toISOString() +
          '; refusing SQL write: ' +
          clippedStatement(statement)
      )
    }
  }

  const recordRowsWritten = (rowsWritten: unknown, statement: unknown): boolean => {
    const rows = Number(rowsWritten || 0)
    if (!Number.isFinite(rows) || rows <= 0) return false

    const now = Date.now()
    const state = readState()
    let windowStart = Number(state.window_start || 0)
    let rowsInWindow = Number(state.rows_in_window || 0)
    let firstOverAt = Number(state.first_over_at || 0)
    const trippedAt = Number(state.tripped_at || 0)
    if (trippedAt > 0) return true

    const windowAgeMs = windowStart ? now - windowStart : 0
    if (!windowStart || windowAgeMs >= windowMs) {
      const previousWindowWasOver =
        windowAgeMs < windowMs * 2 && rowsInWindow > rowsPerWindow
      windowStart = now
      rowsInWindow = 0
      if (!previousWindowWasOver) firstOverAt = 0
    }

    rowsInWindow += rows
    let nextTrippedAt = 0
    if (rowsInWindow > rowsPerWindow) {
      if (!firstOverAt) firstOverAt = now
      if (rowsInWindow > hardRowsPerWindow || now - firstOverAt >= sustainedMs) {
        nextTrippedAt = now
      }
    }

    rawExec(
      'UPDATE ' +
        table +
        ' SET window_start = ?, rows_in_window = ?, first_over_at = ?, tripped_at = ?, last_statement = ? WHERE id = 1',
      windowStart,
      rowsInWindow,
      firstOverAt,
      nextTrippedAt,
      clippedStatement(statement)
    )

    if (nextTrippedAt) {
      console.error(
        logPrefix +
          ' ZeroSqlDO write circuit breaker tripped: rows_in_window=' +
          rowsInWindow +
          ', rows_written=' +
          rows +
          ', statement=' +
          clippedStatement(statement)
      )
      return true
    }
    return false
  }

  sql.exec = (statement: string, ...params: unknown[]) => {
    const text = String(statement || '')
    const isCircuitStatement = text.includes(table)
    const isMutation = isMutationStatement(text) && !isCircuitStatement
    if (isMutation) assertOpen(text)
    const cursor = rawExec(statement, ...params)
    if (isMutation && recordRowsWritten(cursor && cursor.rowsWritten, text)) {
      assertOpen(text)
    }
    return cursor
  }
  INSTALLED.add(sql)
}
