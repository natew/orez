export type IngestBreakerReason = 'ingestBudgetExceeded' | 'ingestCursorStalled'

export type IngestBreakerOptions = {
  budgetRows: number
  windowMs: number
  initialBackoffMs: number
  maxBackoffMs: number
  now: () => number
}

export type IngestBreakerStatus = {
  windowRows: number
  billableRows: number
  logicalRows: number
  budget: number
  windowMs: number
  tripped: boolean
  reason: IngestBreakerReason | null
  retryAt: number | null
  retryAfterMs: number
  consecutiveTrips: number
}

type Sample = { at: number; billableRows: number; logicalRows: number }

export class IngestBreakerError extends Error {
  readonly status = 429

  constructor(
    readonly error: IngestBreakerReason,
    readonly windowRows: number,
    readonly budget: number,
    readonly retryAfterMs: number
  ) {
    super(`${error}: ${windowRows}/${budget} rows; retry after ${retryAfterMs}ms`)
    this.name = 'IngestBreakerError'
  }
}

export class IngestCircuitBreaker {
  readonly #options: IngestBreakerOptions
  #samples: Sample[] = []
  #billableRows = 0
  #logicalRows = 0
  #reason: IngestBreakerReason | null = null
  #retryAt: number | null = null
  #consecutiveTrips = 0

  constructor(options: IngestBreakerOptions) {
    for (const [name, value] of Object.entries(options)) {
      if (name === 'now') continue
      if (!Number.isSafeInteger(value) || Number(value) < 1)
        throw new TypeError(`${name} must be a positive safe integer`)
    }
    this.#options = options
  }

  #prune(now: number): void {
    const cutoff = now - this.#options.windowMs
    let remove = 0
    while (remove < this.#samples.length && this.#samples[remove]!.at <= cutoff) {
      this.#billableRows -= this.#samples[remove]!.billableRows
      this.#logicalRows -= this.#samples[remove]!.logicalRows
      remove++
    }
    if (remove > 0) this.#samples.splice(0, remove)
  }

  status(): IngestBreakerStatus {
    const now = this.#options.now()
    this.#prune(now)
    const retryAfterMs = Math.max(0, (this.#retryAt ?? now) - now)
    return {
      windowRows: this.#billableRows,
      billableRows: this.#billableRows,
      logicalRows: this.#logicalRows,
      budget: this.#options.budgetRows,
      windowMs: this.#options.windowMs,
      tripped: this.#reason !== null && retryAfterMs > 0,
      reason: this.#reason,
      retryAt: this.#retryAt,
      retryAfterMs,
      consecutiveTrips: this.#consecutiveTrips,
    }
  }

  assertReady(): void {
    const status = this.status()
    if (status.tripped && status.reason) {
      throw new IngestBreakerError(
        status.reason,
        status.windowRows,
        status.budget,
        status.retryAfterMs
      )
    }
  }

  record(rows: number): void {
    this.recordLogical(rows)
    this.recordBillable(rows)
  }

  #sample(now: number): Sample {
    this.#prune(now)
    const last = this.#samples[this.#samples.length - 1]
    if (last?.at === now) return last
    const sample = { at: now, billableRows: 0, logicalRows: 0 }
    this.#samples.push(sample)
    return sample
  }

  recordLogical(rows: number): void {
    if (!Number.isSafeInteger(rows) || rows <= 0) return
    const sample = this.#sample(this.#options.now())
    sample.logicalRows += rows
    this.#logicalRows += rows
  }

  recordBillable(rows: number): void {
    this.assertReady()
    if (!Number.isSafeInteger(rows) || rows <= 0) return
    const now = this.#options.now()
    const sample = this.#sample(now)
    sample.billableRows += rows
    this.#billableRows += rows
    if (this.#billableRows > this.#options.budgetRows) this.trip('ingestBudgetExceeded')
  }

  trip(reason: IngestBreakerReason): never {
    const now = this.#options.now()
    this.#consecutiveTrips++
    const delay = Math.min(
      this.#options.maxBackoffMs,
      this.#options.initialBackoffMs * 2 ** (this.#consecutiveTrips - 1)
    )
    this.#reason = reason
    this.#retryAt = now + delay
    throw new IngestBreakerError(
      reason,
      this.#billableRows,
      this.#options.budgetRows,
      delay
    )
  }

  recovered(): void {
    this.#reason = null
    this.#retryAt = null
    this.#consecutiveTrips = 0
  }

  restore(reason: IngestBreakerReason, retryAt: number, consecutiveTrips: number): void {
    if (!Number.isFinite(retryAt) || retryAt <= 0) return
    this.#reason = reason
    this.#retryAt = retryAt
    this.#consecutiveTrips = Math.max(1, Math.floor(consecutiveTrips) || 1)
  }

  reopen(): void {
    this.#samples = []
    this.#billableRows = 0
    this.#logicalRows = 0
    this.recovered()
  }
}

type BillableCursor = {
  rowsWritten?: number
  next?: (...args: unknown[]) => unknown
  one?: (...args: unknown[]) => unknown
  toArray?: (...args: unknown[]) => unknown
  raw?: (...args: unknown[]) => unknown
  [Symbol.iterator]?: () => Iterator<unknown>
}

export function trackBillableCursorRows<Cursor extends BillableCursor>(
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
      if (property === Symbol.iterator) return () => proxy as unknown as Iterator<unknown>
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

export function retryDelayMs(
  attempt: number,
  initialBackoffMs: number,
  maxBackoffMs: number
): number {
  return Math.min(maxBackoffMs, initialBackoffMs * 2 ** Math.max(0, attempt - 1))
}

export function shouldRetryDelegatedPush(
  responseStatus: number | null,
  attempt: number,
  maxAttempts: number
): boolean {
  if (attempt >= maxAttempts) return false
  return responseStatus === null || responseStatus === 429 || responseStatus >= 500
}
