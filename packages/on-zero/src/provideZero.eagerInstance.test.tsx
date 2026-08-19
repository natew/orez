// @vitest-environment jsdom
//
// Zero must be available in the consumer's FIRST render. Not one effect tick
// later, not after a passive-effect flush — immediately.
//
// regression: soot's sootsim tenant render worker rendered ProvideZero (its
// `globalThis.__testZero` was set, which only the provider's render body does)
// and still had no Zero: `zero.query` threw "Zero instance not initialized",
// and the http-pull page registry symbol was absent because the transport is
// installed inside constructZeroInstance. That tenant's passive effects never
// ran, and construction lived in a useEffect. Anywhere effects are delayed,
// discarded, or never flushed — workers, double-rendered roots, concurrent
// React — the same hole opens.

import { createSchema, string, table } from '@rocicorp/zero'
import { useZero } from '@rocicorp/zero/react'
import { act, StrictMode } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { renderToString } from 'react-dom/server'
import { afterEach, beforeEach, expect, test, vi } from 'vitest'

// counting constructions is the whole point here, so Zero is faked: a real
// client would open a store and a socket per test.
const fakeZero = vi.hoisted(() => {
  const instances: any[] = []
  class FakeZero {
    readonly clientID: string
    readonly context = {}
    readonly query = {}
    readonly connection = {
      state: { current: { name: 'connecting' }, subscribe: () => () => {} },
      connect: vi.fn(async () => {}),
    }
    readonly close = vi.fn()
    readonly delete = vi.fn(async () => ({ errors: [] }))
    readonly run = vi.fn(async () => [])
    constructor(options: { userID?: string }) {
      instances.push(this)
      this.clientID = `fake-${instances.length}-${options.userID ?? 'anon'}`
    }
  }
  return { FakeZero, instances }
})

vi.mock('@rocicorp/zero', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@rocicorp/zero')>()
  return { ...actual, Zero: fakeZero.FakeZero }
})

import { createZeroClient } from './createZeroClient'

declare global {
  // eslint-disable-next-line no-var
  var IS_REACT_ACT_ENVIRONMENT: boolean | undefined
}
globalThis.IS_REACT_ACT_ENVIRONMENT = true

const todoTable = table('todo')
  .columns({ id: string(), title: string() })
  .primaryKey('id')
const schema = createSchema({ tables: [todoTable] })

let clientCount = 0
// each test gets its own client: the instance cache and the namespace registry
// are per-client and per-module, so sharing one leaks state across tests.
const makeClient = () =>
  createZeroClient({
    schema,
    models: {},
    groupedQueries: {},
    instanceName: `eager-instance-${++clientCount}`,
  })

let container: HTMLDivElement
let root: Root | null

beforeEach(() => {
  fakeZero.instances.length = 0
  container = document.createElement('div')
  root = null
})

afterEach(() => {
  if (root) act(() => root?.unmount())
})

test('a consumer reading zero in its first render gets a live instance, with no effects flushed', () => {
  const client = makeClient()
  const install = vi.fn()
  const seen: {
    facadeClientID?: string
    contextClientID?: string
    error?: string
    transportInstalled?: boolean
  } = {}

  const Probe = () => {
    // the transport has to be installed BEFORE the instance connects, so by
    // the time a child renders it must already have happened.
    seen.transportInstalled = install.mock.calls.length > 0
    seen.contextClientID = (useZero() as unknown as { clientID: string }).clientID
    try {
      seen.facadeClientID = (client.zero as unknown as { clientID: string }).clientID
    } catch (error) {
      seen.error = (error as Error).message
    }
    return null
  }

  // renderToString never flushes an effect — that is exactly the environment
  // the soot tenant was in.
  renderToString(
    <client.ProvideZero
      cacheURL="http://127.0.0.1:7788/zero"
      userID="first-render"
      transport={{ install }}
    >
      <Probe />
    </client.ProvideZero>
  )

  expect(seen.error).toBeUndefined()
  expect(fakeZero.instances.length).toBe(1)
  expect(seen.facadeClientID).toBe(fakeZero.instances[0].clientID)
  expect(seen.contextClientID).toBe(fakeZero.instances[0].clientID)
  expect(seen.transportInstalled).toBe(true)
})

test('the first committed render already carries the real instance', () => {
  const client = makeClient()
  const clientIDs: string[] = []
  const Probe = () => {
    clientIDs.push((useZero() as unknown as { clientID: string }).clientID)
    return null
  }

  root = createRoot(container)
  act(() => {
    root?.render(
      <client.ProvideZero cacheURL="http://127.0.0.1:7788/zero" userID="committed">
        <Probe />
      </client.ProvideZero>
    )
  })

  // no render of a child ever sees the disabled stub
  expect(clientIDs.length).toBeGreaterThan(0)
  expect(clientIDs).not.toContain('disabled')
  expect(clientIDs[0]).toBe(fakeZero.instances[0].clientID)
})

test('StrictMode double-invoked render constructs exactly one instance', () => {
  const client = makeClient()
  root = createRoot(container)
  act(() => {
    root?.render(
      <StrictMode>
        <client.ProvideZero cacheURL="http://127.0.0.1:7788/zero" userID="strict">
          <span>ok</span>
        </client.ProvideZero>
      </StrictMode>
    )
  })

  expect(fakeZero.instances.length).toBe(1)
  expect(fakeZero.instances[0].close).not.toHaveBeenCalled()
})

test('an identity change rotates the instance and closes the outgoing one', () => {
  const client = makeClient()
  root = createRoot(container)
  const render = (userID: string) =>
    act(() => {
      root?.render(
        <client.ProvideZero cacheURL="http://127.0.0.1:7788/zero" userID={userID}>
          <span>ok</span>
        </client.ProvideZero>
      )
    })

  render('one')
  expect(fakeZero.instances.length).toBe(1)
  render('two')
  expect(fakeZero.instances.length).toBe(2)
  expect(fakeZero.instances[0].close).toHaveBeenCalled()
  expect(fakeZero.instances[1].close).not.toHaveBeenCalled()
})

test('a rotation that never commits is undone, not duplicated', () => {
  const client = makeClient()
  root = createRoot(container)
  const mount = (userID: string) =>
    act(() => {
      root?.render(
        <client.ProvideZero cacheURL="http://127.0.0.1:7788/zero" userID={userID}>
          <span>ok</span>
        </client.ProvideZero>
      )
    })

  mount('committed')
  const first = fakeZero.instances[0]

  // stand-in for a render react throws away: a full render pass of the same
  // provider at a different identity that no commit ever follows.
  renderToString(
    <client.ProvideZero cacheURL="http://127.0.0.1:7788/zero" userID="abandoned">
      <span>ok</span>
    </client.ProvideZero>
  )
  expect(fakeZero.instances.length).toBe(2)
  // the committed tree's instance survives an uncommitted rotation
  expect(first.close).not.toHaveBeenCalled()

  // back to the committed identity: revive the original rather than build a
  // third client beside it, and close the abandoned one
  mount('committed')
  expect(fakeZero.instances.length).toBe(2)
  expect(first.close).not.toHaveBeenCalled()
  expect(fakeZero.instances[1].close).toHaveBeenCalled()
})

test('disable=true constructs nothing', () => {
  const client = makeClient()
  root = createRoot(container)
  act(() => {
    root?.render(
      <client.ProvideZero cacheURL="http://127.0.0.1:7788/zero" userID="off" disable>
        <span>ok</span>
      </client.ProvideZero>
    )
  })

  expect(fakeZero.instances.length).toBe(0)
})
