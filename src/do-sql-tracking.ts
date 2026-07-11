export const RETURNING_INTERNAL_PREFIX = '__orez_returning_'

export interface TrackedRowFilter {
  rowColumns?: string[]
}

export interface RowWriteBudgetOptions {
  budgetRows: number
  windowMs: number
  now: () => number
}

export interface RowWriteBudgetStatus {
  windowRows: number
  budget: number
  windowMs: number
  windowStartedAt: number | null
  windowEndsAt: number | null
  tripped: boolean
  trippedAt: number | null
}

type RowWriteSample = { at: number; rows: number }

/** Structured error used by CF-facing layers to return an HTTP 429. */
export class WriteBudgetExceededError extends Error {
  readonly error = 'writeBudgetExceeded'

  constructor(
    readonly windowRows: number,
    readonly budget: number,
    readonly windowMs: number
  ) {
    super(`row write budget exceeded: ${windowRows}/${budget} rows in ${windowMs}ms`)
    this.name = 'WriteBudgetExceededError'
  }

  toJSON(): { error: string; windowRows: number; budget: number } {
    return { error: this.error, windowRows: this.windowRows, budget: this.budget }
  }
}

/**
 * In-memory rolling row counter for a single Durable Object isolate.
 *
 * Persistence is deliberately left to the owner and only needed when the
 * circuit changes state. Updating a SQLite meter row for every application
 * write would itself increase Durable Object rows-written and amplify a burn.
 */
export class RollingRowWriteBudget {
  readonly #budgetRows: number
  readonly #windowMs: number
  readonly #bucketMs: number
  readonly #now: () => number
  #samples: RowWriteSample[] = []
  #windowRows = 0
  #trippedAt: number | null = null

  constructor(options: RowWriteBudgetOptions) {
    if (!Number.isSafeInteger(options.budgetRows) || options.budgetRows < 1)
      throw new TypeError('budgetRows must be a positive safe integer')
    if (!Number.isSafeInteger(options.windowMs) || options.windowMs < 1)
      throw new TypeError('windowMs must be a positive safe integer')
    this.#budgetRows = options.budgetRows
    this.#windowMs = options.windowMs
    this.#bucketMs = Math.min(1_000, options.windowMs)
    this.#now = options.now
  }

  #prune(now: number): void {
    const cutoff = now - this.#windowMs
    let remove = 0
    while (
      remove < this.#samples.length &&
      this.#samples[remove]!.at + this.#bucketMs <= cutoff
    ) {
      this.#windowRows -= this.#samples[remove]!.rows
      remove++
    }
    if (remove > 0) this.#samples.splice(0, remove)
  }

  status(): RowWriteBudgetStatus {
    const now = this.#now()
    this.#prune(now)
    const windowStartedAt = this.#samples[0]?.at ?? null
    return {
      windowRows: this.#windowRows,
      budget: this.#budgetRows,
      windowMs: this.#windowMs,
      windowStartedAt,
      windowEndsAt: windowStartedAt === null ? null : windowStartedAt + this.#windowMs,
      tripped: this.#trippedAt !== null,
      trippedAt: this.#trippedAt,
    }
  }

  assertOpen(): void {
    if (this.#trippedAt === null) return
    const status = this.status()
    throw new WriteBudgetExceededError(status.windowRows, status.budget, status.windowMs)
  }

  record(rowsWritten: unknown): RowWriteBudgetStatus {
    this.assertOpen()
    const rows = Number(rowsWritten)
    if (!Number.isSafeInteger(rows) || rows <= 0) return this.status()
    const now = this.#now()
    this.#prune(now)
    const bucketAt = Math.floor(now / this.#bucketMs) * this.#bucketMs
    const last = this.#samples[this.#samples.length - 1]
    if (last?.at === bucketAt) last.rows += rows
    else this.#samples.push({ at: bucketAt, rows })
    this.#windowRows += rows
    if (this.#windowRows > this.#budgetRows) {
      this.#trippedAt = now
      throw new WriteBudgetExceededError(
        this.#windowRows,
        this.#budgetRows,
        this.#windowMs
      )
    }
    return this.status()
  }

  restoreTrip(trippedAt: number): void {
    if (Number.isFinite(trippedAt) && trippedAt > 0) this.#trippedAt = trippedAt
  }

  reopen(): RowWriteBudgetStatus {
    this.#samples = []
    this.#windowRows = 0
    this.#trippedAt = null
    return this.status()
  }
}

const SQL_MUTATION_RE =
  /(?:^|;)\s*(?:insert|update|delete|replace|create|alter|drop|truncate|vacuum|reindex)\b/i
const SQL_WITH_RE = /(?:^|;)\s*with\b/i
const SQL_WITH_MUTATION_RE = /\b(?:insert|update|delete|replace)\b/i

/** Conservative SQL classifier used to block writes before execution. */
export function isSqlMutation(sql: unknown): boolean {
  // SqlStorage accepts comments before a statement. Remove only trivia at the
  // beginning (and after statement separators) so a comment cannot bypass the
  // pre-execution gate on a sticky circuit.
  const text = String(sql ?? '').replace(
    /(^|;)\s*(?:(?:--[^\n]*(?:\n|$))|(?:\/\*[\s\S]*?\*\/))\s*/g,
    '$1'
  )
  return (
    SQL_MUTATION_RE.test(text) ||
    (SQL_WITH_RE.test(text) && SQL_WITH_MUTATION_RE.test(text))
  )
}

export function trackedChangeRow(
  row: Record<string, unknown>,
  track: TrackedRowFilter
): Record<string, unknown> {
  const allowed = track.rowColumns ? new Set(track.rowColumns) : null
  const out: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(row)) {
    if (key.startsWith(RETURNING_INTERNAL_PREFIX)) continue
    if (allowed && !allowed.has(key)) continue
    out[key] = value
  }
  return out
}
