import type { JsonValue } from './types.js'

// details is optional because zero's ApplicationError treats it that way and
// omits it from the mutation result when absent; inventing one here would put a
// field on the wire that upstream would not have sent.
export class MutationApplicationError extends Error {
  readonly details: JsonValue | undefined

  constructor(details?: JsonValue, message?: string) {
    super(message ?? (typeof details === 'string' ? details : 'mutation rejected'))
    this.name = 'MutationApplicationError'
    this.details = details
  }
}

// a mutation that cannot run right now, as opposed to one that can never
// succeed. an application error is permanent, so the push acknowledges it and
// advances the ledger; the client has to move on or it retries that id forever
// and blocks every later mutation in its group. a retryable rejection is the
// opposite: it aborts the whole push, writes nothing at all, and leaves the
// mutation id unconsumed so the client sends the same mutation again after
// retryAfterMs.
//
// the distinction is load-bearing whenever the reason for refusing IS cost. a
// budget or rate limit that acknowledges "I refuse to write" with a ledger
// advance and its lmid change row makes the mechanism that exists to stop
// spending spend on every refusal, and drops the caller's data on the floor
// while doing it.
export class MutationRetryError extends Error {
  readonly status = 429
  readonly retryAfterMs: number
  readonly details: JsonValue | undefined

  constructor(retryAfterMs: number, message?: string, details?: JsonValue) {
    super(message ?? 'mutation cannot be applied yet')
    if (!Number.isSafeInteger(retryAfterMs) || retryAfterMs < 0) {
      throw new TypeError('retryAfterMs must be a non-negative safe integer')
    }
    this.name = 'MutationRetryError'
    this.retryAfterMs = retryAfterMs
    this.details = details
  }
}

export class SyncExecutorRequestError extends Error {
  constructor(
    readonly status: 400 | 403,
    message: string
  ) {
    super(message)
    this.name = 'SyncExecutorRequestError'
  }
}

export class MutationWriteSetError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'MutationWriteSetError'
  }
}
