// force the non-server runtime before anything reads platform.ts, so this file
// exercises the browser / react-native path rather than AsyncLocalStorage.
process.env.VITE_ENVIRONMENT = 'client'

import { beforeEach, describe, expect, test } from 'vitest'

import { setAuthData } from '../state'

const store = globalThis as typeof globalThis & {
  __onZeroMutatorContext__?: unknown
  __onZeroAuthScopeContext__?: unknown
}

beforeEach(() => {
  store.__onZeroMutatorContext__ = undefined
  store.__onZeroAuthScopeContext__ = undefined
})

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

const fakeContext = (name: string) =>
  ({
    tx: { name } as never,
    authData: null,
    environment: 'client',
    can: {} as never,
  }) as never

describe('mutator context on runtimes without AsyncLocalStorage', () => {
  test('a task running alongside a mutator is never told it is in a mutation', async () => {
    const { isInZeroMutation, runWithContext } = await import('./mutatorContext')

    // stands in for agentQaRunner: ordinary async work on its own timeline that
    // asks "am I inside a mutation?" to decide how to run a query. it started
    // before any mutator and belongs to no transaction.
    const observations: boolean[] = []
    const bystander = (async () => {
      for (let i = 0; i < 6; i++) {
        await sleep(5)
        observations.push(isInZeroMutation())
      }
    })()

    await sleep(8)
    await runWithContext(fakeContext('A'), async () => {
      await sleep(20)
    })
    await bystander

    expect(observations).toHaveLength(6)
    expect(observations.filter(Boolean)).toEqual([])
  })

  test('overlapping mutators leave Promise.prototype alone', async () => {
    const { runWithContext } = await import('./mutatorContext')
    const pristine = {
      then: Promise.prototype.then,
      catch: Promise.prototype.catch,
      finally: Promise.prototype.finally,
    }

    const first = runWithContext(fakeContext('A'), () => sleep(20))
    await sleep(5)
    const second = runWithContext(fakeContext('B'), () => sleep(20))
    await Promise.all([first, second])

    expect(Promise.prototype.then).toBe(pristine.then)
    expect(Promise.prototype.catch).toBe(pristine.catch)
    expect(Promise.prototype.finally).toBe(pristine.finally)
  })

  test('runWithContext still returns the function result and propagates throws', async () => {
    const { runWithContext } = await import('./mutatorContext')

    await expect(runWithContext(fakeContext('A'), async () => 'ok')).resolves.toBe('ok')
    await expect(
      runWithContext(fakeContext('A'), async () => {
        throw new Error('boom')
      })
    ).rejects.toThrow('boom')
  })

  test('ensureLoggedIn works inside a mutator without an ambient context', async () => {
    const { runWithContext } = await import('./mutatorContext')
    const { ensureLoggedIn } = await import('./ensureLoggedIn')

    setAuthData({ id: 'user_1' } as never)

    const auth = await runWithContext(fakeContext('A'), async () => {
      await sleep(5)
      return ensureLoggedIn()
    })

    expect(auth.id).toBe('user_1')
  })
})
