export const RETURNING_INTERNAL_PREFIX = '__orez_returning_'

export interface TrackedRowFilter {
  rowColumns?: string[]
}

export interface RowWriteBudgetOptions {
  budgetRows: number
  windowMs: number
  now: () => number
}

/**
 * What the circuit saw at the moment it tripped.
 *
 * The rolling counter keeps decaying after a trip and an evicted object comes
 * back with none of it, so the live window cannot describe the trip: soot's
 * tripped namespace reported `0/300000` and hid how far over budget it went.
 * This is the durable evidence, carried through persistence and restore.
 */
export interface RowWriteBudgetTrip {
  at: number
  /** Null only for a trip restored from the legacy bare-timestamp format. */
  windowRows: number | null
  budget: number
  windowMs: number
}

export interface RowWriteBudgetStatus {
  windowRows: number
  billableRows: number
  logicalRows: number
  budget: number
  windowMs: number
  windowStartedAt: number | null
  windowEndsAt: number | null
  tripped: boolean
  trippedAt: number | null
  /** Billable rows counted in the window when the circuit tripped. */
  trippedWindowRows: number | null
  /** Budget in force when the circuit tripped, which an env change can outlive. */
  trippedBudget: number | null
}

type RowWriteSample = { at: number; billableRows: number; logicalRows: number }

/** Structured error used by CF-facing layers to return an HTTP 429. */
export class WriteBudgetExceededError extends Error {
  readonly error = 'writeBudgetExceeded'

  constructor(
    readonly windowRows: number | null,
    readonly budget: number,
    readonly windowMs: number
  ) {
    super(
      `row write budget exceeded: ${windowRows ?? 'unrecorded'}/${budget} rows in ${windowMs}ms`
    )
    this.name = 'WriteBudgetExceededError'
  }

  toJSON(): { error: string; windowRows: number | null; budget: number } {
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
  #billableRows = 0
  #logicalRows = 0
  #trip: RowWriteBudgetTrip | null = null

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
      this.#billableRows -= this.#samples[remove]!.billableRows
      this.#logicalRows -= this.#samples[remove]!.logicalRows
      remove++
    }
    if (remove > 0) this.#samples.splice(0, remove)
  }

  status(): RowWriteBudgetStatus {
    const now = this.#now()
    this.#prune(now)
    const windowStartedAt = this.#samples[0]?.at ?? null
    return {
      windowRows: this.#billableRows,
      billableRows: this.#billableRows,
      logicalRows: this.#logicalRows,
      budget: this.#budgetRows,
      windowMs: this.#windowMs,
      windowStartedAt,
      windowEndsAt: windowStartedAt === null ? null : windowStartedAt + this.#windowMs,
      tripped: this.#trip !== null,
      trippedAt: this.#trip?.at ?? null,
      trippedWindowRows: this.#trip?.windowRows ?? null,
      trippedBudget: this.#trip?.budget ?? null,
    }
  }

  /** The trip to persist, so a restored object still reports what tripped it. */
  trip(): RowWriteBudgetTrip | null {
    return this.#trip
  }

  /** Trip immediately for an operator-requested, non-destructive write stop. */
  forceTrip(): RowWriteBudgetStatus {
    if (this.#trip === null) {
      this.#prune(this.#now())
      this.#trip = {
        at: this.#now(),
        windowRows: this.#billableRows,
        budget: this.#budgetRows,
        windowMs: this.#windowMs,
      }
    }
    return this.status()
  }

  assertOpen(): void {
    const trip = this.#trip
    if (trip === null) return
    throw new WriteBudgetExceededError(trip.windowRows, trip.budget, trip.windowMs)
  }

  #sample(): RowWriteSample {
    const now = this.#now()
    this.#prune(now)
    const bucketAt = Math.floor(now / this.#bucketMs) * this.#bucketMs
    const last = this.#samples[this.#samples.length - 1]
    if (last?.at === bucketAt) return last
    const sample = { at: bucketAt, billableRows: 0, logicalRows: 0 }
    this.#samples.push(sample)
    return sample
  }

  record(rowsWritten: unknown): RowWriteBudgetStatus {
    return this.recordBillable(rowsWritten)
  }

  recordBillable(rowsWritten: unknown): RowWriteBudgetStatus {
    this.assertOpen()
    const rows = Number(rowsWritten)
    if (!Number.isSafeInteger(rows) || rows <= 0) return this.status()
    const sample = this.#sample()
    sample.billableRows += rows
    this.#billableRows += rows
    if (this.#billableRows > this.#budgetRows) {
      this.#trip = {
        at: this.#now(),
        windowRows: this.#billableRows,
        budget: this.#budgetRows,
        windowMs: this.#windowMs,
      }
      throw new WriteBudgetExceededError(
        this.#billableRows,
        this.#budgetRows,
        this.#windowMs
      )
    }
    return this.status()
  }

  recordLogical(rowsWritten: unknown): RowWriteBudgetStatus {
    const rows = Number(rowsWritten)
    if (!Number.isSafeInteger(rows) || rows <= 0) return this.status()
    const sample = this.#sample()
    sample.logicalRows += rows
    this.#logicalRows += rows
    return this.status()
  }

  /**
   * Restore a sticky trip from durable storage.
   *
   * Accepts the bare timestamp written before trip counts were persisted. That
   * format cannot say how far over budget the object went, so its count stays
   * unrecorded rather than being reported as zero. Those values disappear the
   * first time the namespace is reopened, which is the only way out anyway.
   */
  restoreTrip(persisted: number | Partial<RowWriteBudgetTrip>): void {
    const trip = typeof persisted === 'number' ? { at: persisted } : persisted
    const at = Number(trip?.at)
    if (!Number.isFinite(at) || at <= 0) return
    const windowRows = Number(trip.windowRows)
    const budget = Number(trip.budget)
    const windowMs = Number(trip.windowMs)
    this.#trip = {
      at,
      windowRows: Number.isSafeInteger(windowRows) && windowRows > 0 ? windowRows : null,
      budget: Number.isSafeInteger(budget) && budget > 0 ? budget : this.#budgetRows,
      windowMs:
        Number.isSafeInteger(windowMs) && windowMs > 0 ? windowMs : this.#windowMs,
    }
  }

  reopen(): RowWriteBudgetStatus {
    this.#samples = []
    this.#billableRows = 0
    this.#logicalRows = 0
    this.#trip = null
    return this.status()
  }
}

const SQL_MUTATION_RE =
  /(?:^|;)\s*(?:insert|update|delete|replace|create|alter|drop|truncate|vacuum|reindex)\b/i
const SQL_ROW_MUTATION_RE = /(?:^|;)\s*(?:insert|update|delete|replace)\b/i
const SQL_WITH_RE = /(?:^|;)\s*with\b/i
const SQL_WITH_MUTATION_RE = /\b(?:insert|update|delete|replace)\b/i

function sqlWithoutLeadingTrivia(sql: unknown): string {
  const text = String(sql ?? '')
  if (!text.includes('--') && !text.includes('/*')) return text
  return text.replace(/(^|;)\s*(?:(?:--[^\n]*(?:\n|$))|(?:\/\*[\s\S]*?\*\/))\s*/g, '$1')
}

export interface SqlClassification {
  mutation: boolean
  rowMutation: boolean
}

/** Classify a statement once for the write gate, CDC, and logical accounting. */
export function classifySql(sql: unknown): SqlClassification {
  // SqlStorage accepts comments before a statement. Remove only trivia at the
  // beginning (and after statement separators) so a comment cannot bypass the
  // pre-execution gate on a sticky circuit.
  const text = sqlWithoutLeadingTrivia(sql)
  const withMutation = SQL_WITH_RE.test(text) && SQL_WITH_MUTATION_RE.test(text)
  return {
    mutation: SQL_MUTATION_RE.test(text) || withMutation,
    rowMutation: SQL_ROW_MUTATION_RE.test(text) || withMutation,
  }
}

/** Conservative SQL classifier used to block writes before execution. */
export function isSqlMutation(sql: unknown): boolean {
  return classifySql(sql).mutation
}

/** Row-level DML classifier used to decide when a CDC drain must be atomic. */
export function isSqlRowMutation(sql: unknown): boolean {
  return classifySql(sql).rowMutation
}

/** Normalize Zero's optional PostgreSQL-style public schema prefix. */
export function stripPublicPrefix(tableName: string): string {
  return tableName.startsWith('public.') ? tableName.slice('public.'.length) : tableName
}

type SqlCursorLike = {
  rowsWritten?: number
  next?: (...args: unknown[]) => unknown
  one?: (...args: unknown[]) => unknown
  toArray?: (...args: unknown[]) => unknown
  raw?: (...args: unknown[]) => unknown
  [Symbol.iterator]?: () => Iterator<unknown>
}

/**
 * Account a mutation cursor's final billing rows as it is consumed.
 *
 * Cloudflare's `rowsWritten` can increase during cursor iteration (notably for
 * `... RETURNING` statements). Sampling only when `sql.exec()` returns misses
 * those writes. This proxy records monotonic deltas after every consumption
 * method while preserving the cursor's native `this` binding.
 */
export function trackSqlCursorRowsWritten<Cursor extends SqlCursorLike>(
  cursor: Cursor,
  record: (rows: number) => void
): Cursor {
  if (!cursor || typeof cursor !== 'object') return cursor
  let accountedRows = 0

  const account = () => {
    const current = Number(cursor.rowsWritten ?? 0)
    if (!Number.isSafeInteger(current) || current <= accountedRows) return
    const delta = current - accountedRows
    accountedRows = current
    record(delta)
  }

  const wrapIterator = (iterator: object): object =>
    new Proxy(iterator, {
      get(target, property) {
        const value = Reflect.get(target, property, target)
        if (property === 'next' && typeof value === 'function') {
          return (...args: unknown[]) => {
            try {
              return Reflect.apply(value, target, args)
            } finally {
              account()
            }
          }
        }
        return typeof value === 'function' ? value.bind(target) : value
      },
    })

  let proxy: Cursor
  proxy = new Proxy(cursor, {
    get(target, property) {
      const value = Reflect.get(target, property, target)
      if (property === Symbol.iterator) {
        return () => proxy as unknown as Iterator<unknown>
      }
      if (
        (property === 'next' || property === 'one' || property === 'toArray') &&
        typeof value === 'function'
      ) {
        return (...args: unknown[]) => {
          try {
            return Reflect.apply(value, target, args)
          } finally {
            account()
          }
        }
      }
      if (property === 'raw' && typeof value === 'function') {
        return (...args: unknown[]) =>
          wrapIterator(Reflect.apply(value, target, args) as object)
      }
      return typeof value === 'function' ? value.bind(target) : value
    },
  })
  account()
  return proxy
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
