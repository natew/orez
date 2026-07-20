import type { JsonValue } from './types.js'

export class MutationApplicationError extends Error {
  readonly details: JsonValue

  constructor(details: JsonValue, message?: string) {
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
