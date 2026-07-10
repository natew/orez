import type { PullCaps } from './types.js'

export function validatePullCaps(caps: PullCaps): PullCaps {
  if (!Number.isSafeInteger(caps.maxChangeRows) || caps.maxChangeRows < 1) {
    throw new TypeError('caps.maxChangeRows must be a positive safe integer')
  }
  return caps
}
