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

export class SyncExecutorRequestError extends Error {
  constructor(
    readonly status: 400 | 403,
    message: string
  ) {
    super(message)
    this.name = 'SyncExecutorRequestError'
  }
}
