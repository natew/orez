import { EnsureError } from './errors'

export function ensure<T>(
  value: T,
  msg = ''
): asserts value is Exclude<T, null | undefined | false> {
  if (!value) {
    throw new EnsureError(`ensure() invalid: (${value}): ${msg} ${new Error().stack}`)
  }
}
