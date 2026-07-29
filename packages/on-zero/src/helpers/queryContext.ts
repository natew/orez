import { createAsyncContext } from './asyncContext'

import type { AuthData } from '../types'

const asyncContext = createAsyncContext<{ authData: AuthData | null }>()

// synchronous scope for host-driven query resolution: a sync host resolves a
// whole desired-query patch inside one synchronous call so patch application
// order is arrival order, which AsyncLocalStorage cannot serve (its run() is
// async). query builders are synchronous, so a plain stack scope is exact.
let syncScope: { authData: AuthData | null } | null = null

export function runWithSyncQueryContext<T>(
  context: { authData: AuthData | null },
  fn: () => T
): T {
  const previous = syncScope
  syncScope = context
  try {
    return fn()
  } finally {
    syncScope = previous
  }
}

export function queryAuthData(): AuthData | null {
  if (syncScope) return syncScope.authData
  return asyncContext.get()?.authData ?? null
}

export function isInQueryContext() {
  return !!syncScope || !!asyncContext.get()
}

export function runWithQueryContext<T>(
  context: { authData: AuthData | null },
  fn: () => T | Promise<T>
): Promise<T> {
  return asyncContext.run(context, fn)
}
