export type ExpectedServerOutcome =
  | 'success'
  | {
      type: 'app-error'
      message?: string | RegExp
    }

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function actualOutcome(result: unknown): string {
  try {
    return JSON.stringify(result)
  } catch {
    return String(result)
  }
}

// Zero 1.7.0 normalizes mutation failures into a resolved MutatorResultDetails
// union. Attach this check immediately when the mutation is issued so every
// write declares and verifies its semantic server outcome.
export function assertServerOutcome(
  promise: Promise<unknown>,
  expected: ExpectedServerOutcome,
  label: string
): Promise<void> {
  return promise.then(
    (result: unknown) => {
      if (expected === 'success') {
        if (
          typeof result !== 'object' ||
          result === null ||
          !('type' in result) ||
          result.type !== 'success'
        ) {
          throw new Error(
            `${label}: expected server success, got ${actualOutcome(result)}`
          )
        }
        return
      }

      if (
        typeof result !== 'object' ||
        result === null ||
        !('type' in result) ||
        result.type !== 'error' ||
        !('error' in result) ||
        typeof result.error !== 'object' ||
        result.error === null ||
        !('type' in result.error) ||
        result.error.type !== 'app'
      ) {
        throw new Error(
          `${label}: expected server app error, got ${actualOutcome(result)}`
        )
      }

      if (expected.message !== undefined) {
        const message =
          'message' in result.error && typeof result.error.message === 'string'
            ? result.error.message
            : undefined
        const matches =
          typeof expected.message === 'string'
            ? message === expected.message
            : message !== undefined && expected.message.test(message)
        if (!matches) {
          throw new Error(
            `${label}: expected app error message ${String(expected.message)}, got ${String(message)}`
          )
        }
      }
    },
    (error: unknown) => {
      throw new Error(`${label}: server promise rejected: ${errorMessage(error)}`, {
        cause: error,
      })
    }
  )
}
