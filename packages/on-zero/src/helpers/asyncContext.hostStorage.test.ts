// force the non-server runtime before anything reads platform.ts. this file is
// the case where on-zero's server bindings run somewhere its own bundle does not
// call a server — Contrast's web preview runs a tenant's server in a browser
// worker — so `node:async_hooks` is unreachable and the host supplies its own.
// every on-zero module here is imported dynamically for that reason: a static
// import is hoisted above this assignment and would read the real platform.
process.env.VITE_ENVIRONMENT = 'client'

import { AsyncLocalStorage } from 'node:async_hooks'

import { afterAll, beforeEach, describe, expect, test } from 'vitest'

const store = globalThis as typeof globalThis & {
  __onZeroMutatorContext__?: unknown
  __onZeroAuthScopeContext__?: unknown
}

async function resetInstalledStorage() {
  const { setupAsyncLocalStorage } = await import('./asyncContext')
  setupAsyncLocalStorage(null)
}

beforeEach(async () => {
  store.__onZeroMutatorContext__ = undefined
  store.__onZeroAuthScopeContext__ = undefined
  await resetInstalledStorage()
})

afterAll(async () => {
  await resetInstalledStorage()
})

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

const contextFor = (authData: { id: string }) =>
  ({
    tx: {} as never,
    authData,
    environment: 'server',
    can: {} as never,
  }) as never

describe('a host-installed AsyncLocalStorage', () => {
  test('takes effect when installed after on-zero loaded, and a mutation keeps its own auth', async () => {
    const { isInZeroMutation, runWithContext } = await import('./mutatorContext')
    const { ensureLoggedIn } = await import('./ensureLoggedIn')
    const { setupAsyncLocalStorage } = await import('./asyncContext')
    const { setAuthData } = await import('../state')

    // with nothing installed this runtime cannot scope a mutation at all, which
    // is what left every web-preview mutator running as a logged-out user
    const unscoped = await runWithContext(
      contextFor({ id: 'mutation_user' }),
      async () => {
        await sleep(5)
        return isInZeroMutation()
      }
    )
    expect(unscoped).toBe(false)

    // the contexts were built while the modules above loaded, so an install can
    // only ever come later — it still has to work
    setupAsyncLocalStorage(AsyncLocalStorage)

    // a different identity in the client-side fallback slot, so resolving to it
    // instead of the mutation's own context is visible rather than accidentally
    // producing the right answer
    setAuthData({ id: 'ambient_user' } as never)

    const seenInside: boolean[] = []
    const auth = await runWithContext(contextFor({ id: 'mutation_user' }), async () => {
      seenInside.push(isInZeroMutation())
      await sleep(5)
      seenInside.push(isInZeroMutation())
      return ensureLoggedIn()
    })

    expect(seenInside).toEqual([true, true])
    expect(auth.id).toBe('mutation_user')
    expect(isInZeroMutation()).toBe(false)
  })

  test('an async task scoped to a mutation resolves that mutation auth', async () => {
    const { getScopedAuthData, runWithAuthScope } = await import('./mutatorContext')
    const { setupAsyncLocalStorage } = await import('./asyncContext')

    setupAsyncLocalStorage(AsyncLocalStorage)

    const resolved = await runWithAuthScope({ id: 'task_user' } as never, async () => {
      await sleep(5)
      return getScopedAuthData()
    })

    expect(resolved?.id).toBe('task_user')
    expect(getScopedAuthData()).toBeUndefined()
  })
})
