import { getAuthData } from '../state.js'
import { isInZeroMutation, mutatorContext } from './mutatorContext.js'
import { isInQueryContext, queryAuthData } from './queryContext.js'

import type { AuthData } from '../types.js'

export function ensureAuth(): AuthData {
  const auth = getAuth()
  if (!auth) {
    throw new Error('Not authenticated')
  }
  return auth
}

export function getAuth(): AuthData | null {
  if (isInZeroMutation()) {
    return mutatorContext().authData as AuthData
  }
  if (isInQueryContext()) {
    return queryAuthData()
  }
  // client-side fallback (browser global is fine, single-threaded)
  return getAuthData()
}
