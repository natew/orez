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
    return this.#chargeBillable(rowsWritten, false)
  }

  // trusted maintenance retains the sticky trip while charging actual work.
  recordMaintenanceBillable(rowsWritten: unknown): RowWriteBudgetStatus {
    if (this.#trip === null)
      throw new Error('maintenance requires a closed write circuit')
    return this.#chargeBillable(rowsWritten, true)
  }

  #chargeBillable(rowsWritten: unknown, maintenance: boolean): RowWriteBudgetStatus {
    const rows = Number(rowsWritten)
    if (!Number.isSafeInteger(rows) || rows <= 0) return this.status()
    const sample = this.#sample()
    sample.billableRows += rows
    this.#billableRows += rows
    if (!maintenance && this.#billableRows > this.#budgetRows) {
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

  /**
   * Application rows a statement changed, the denominator of the
   * billable/logical ratio. Callers must pass rows CHANGED, not rows returned:
   * reading a cursor's length here reported zero for every write on the
   * application-SQL path, because nothing that path emits carries a RETURNING
   * clause, and the ratio divided by that zero.
   *
   * What this deliberately does NOT count, which anyone picking a ratio
   * threshold has to know: only TRACKED mutations reach it. the sync path
   * writes outside any tracked mutation -- claiming a client into
   * `_zsync_clients`, rotating and rewriting `_zsync_log_segments` -- and all
   * of that is billable while none of it is logical. So a quiet or pull-dominated
   * namespace inflates the ratio by construction, with no amplification
   * involved. Measured on soot's control namespace: a write-dominated window
   * sits near 24, an idle one runs far higher on pull traffic alone. A
   * threshold picked against an idle window pages on nothing.
   */
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

/** tokenize complete statements, keeping trigger bodies together and skipping CTE headers. */
export function sqlStatementTokens(sql: string): string[][] {
  const tokens = [
    ...sql.matchAll(
      /--[^\r\n]*|\/\*[\s\S]*?\*\/|"(?:[^"]|"")*"|`(?:[^`]|``)*`|\[[^\]]*\]|'(?:[^']|'')*'|(?:[A-Za-z0-9_$]|[^\x00-\x7f])+|[^\s]/gu
    ),
  ]
    .map((match) => match[0])
    .filter((token) => !token.startsWith('--') && !token.startsWith('/*'))
  const statements: string[][] = []
  let statement: string[] = []
  let triggerBody = false
  let caseDepth = 0
  const finish = () => {
    if (statement.length) statements.push(statement)
    statement = []
  }
  for (const token of tokens) {
    const command = token.toUpperCase()
    const trigger =
      statement[0]?.toUpperCase() === 'CREATE' &&
      (statement[1]?.toUpperCase() === 'TRIGGER' ||
        (['TEMP', 'TEMPORARY'].includes(statement[1]?.toUpperCase() ?? '') &&
          statement[2]?.toUpperCase() === 'TRIGGER'))
    if (trigger && command === 'BEGIN') triggerBody = true
    else if (triggerBody && command === 'CASE') caseDepth++
    else if (triggerBody && command === 'END') {
      if (caseDepth > 0) caseDepth--
      else triggerBody = false
    }
    if (token === ';' && !triggerBody) finish()
    else statement.push(token)
  }
  finish()
  return statements.map((tokens) => {
    if (tokens[0]?.toUpperCase() !== 'WITH') return tokens
    let cursor = tokens[1]?.toUpperCase() === 'RECURSIVE' ? 2 : 1
    const skipGroup = () => {
      if (tokens[cursor] !== '(') return
      let depth = 0
      do {
        if (tokens[cursor] === '(') depth++
        if (tokens[cursor] === ')') depth--
        cursor++
      } while (cursor < tokens.length && depth > 0)
    }
    do {
      cursor++ // cte name, which can itself be an unquoted SQLite keyword
      skipGroup() // optional column names
      if (tokens[cursor]?.toUpperCase() !== 'AS') return []
      cursor++
      if (tokens[cursor]?.toUpperCase() === 'NOT') cursor++
      if (tokens[cursor]?.toUpperCase() === 'MATERIALIZED') cursor++
      if (tokens[cursor] !== '(') return []
      skipGroup()
      if (tokens[cursor] !== ',') break
      cursor++
    } while (cursor < tokens.length)
    return tokens.slice(cursor)
  })
}

export interface SqlClassification {
  mutation: boolean
  rowMutation: boolean
}

/** Classify a statement once for the write gate, CDC, and logical accounting. */
export function classifySql(sql: unknown): SqlClassification {
  const commands = sqlStatementTokens(String(sql ?? '')).map((tokens) =>
    tokens[0]?.toUpperCase()
  )
  return {
    mutation: commands.some((command) =>
      [
        'INSERT',
        'UPDATE',
        'DELETE',
        'REPLACE',
        'CREATE',
        'ALTER',
        'DROP',
        'TRUNCATE',
        'VACUUM',
        'REINDEX',
      ].includes(command ?? '')
    ),
    rowMutation: commands.some((command) =>
      ['INSERT', 'UPDATE', 'DELETE', 'REPLACE'].includes(command ?? '')
    ),
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
