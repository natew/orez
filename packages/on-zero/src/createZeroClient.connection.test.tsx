// @vitest-environment jsdom

import { createSchema, string, table } from '@rocicorp/zero'
import { act, startTransition, StrictMode, Suspense } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { afterEach, beforeEach, expect, test, vi } from 'vitest'

// a controllable connection whose state useConnectionState (useSyncExternalStore)
// subscribes to — set() notifies listeners so ConnectionMonitor's effect re-runs.
const fakeZero = vi.hoisted(() => {
  class FakeConnectionState {
    current: { name: string; reason?: string }
    listeners = new Set<() => void>()
    constructor(initial: { name: string; reason?: string }) {
      this.current = initial
    }
    subscribe = (cb: () => void) => {
      this.listeners.add(cb)
      return () => {
        this.listeners.delete(cb)
      }
    }
    set(next: { name: string; reason?: string }) {
      this.current = next
      for (const cb of this.listeners) cb()
    }
  }

  class FakeZero {
    readonly context = {}
    readonly connection = {
      state: new FakeConnectionState({ name: 'connecting' }),
      connect: vi.fn(async () => {}),
    }
    readonly delete = vi.fn(async () => ({ errors: [] }))
    readonly close = vi.fn()
    readonly run = vi.fn(async () => [])
    readonly preload = vi.fn(() => ({ cleanup: () => {}, complete: Promise.resolve() }))

    constructor(readonly options?: { auth?: string }) {
      instances.push(this)
    }
  }

  const instances: FakeZero[] = []
  return { FakeZero, FakeConnectionState, instances }
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

const noteTable = table('note').columns({ id: string(), body: string() }).primaryKey('id')
const schema = createSchema({ tables: [noteTable] })

const client = createZeroClient({
  schema,
  models: {},
  groupedQueries: {},
  instanceName: 'connection-test',
})

let container: HTMLDivElement
let root: Root | null

beforeEach(() => {
  fakeZero.instances.length = 0
  container = document.createElement('div')
  root = null
  delete document.body.dataset.zeroState
  delete document.body.dataset.zeroConnected
  delete document.body.dataset.zeroReason
})

afterEach(() => {
  if (root) act(() => root?.unmount())
})

async function mount(extraProps: Record<string, unknown> = {}, userID = 'conn') {
  root = createRoot(container)
  await act(async () => {
    root?.render(
      <client.ProvideZero
        cacheURL="http://127.0.0.1:7788/zero"
        userID={userID}
        {...extraProps}
      >
        <span>ok</span>
      </client.ProvideZero>
    )
    await Promise.resolve()
  })
  return fakeZero.instances.at(-1)!
}

test('waitForZero resolves when the provider publishes its active instance', async () => {
  const readinessClient = createZeroClient({
    schema,
    models: {},
    groupedQueries: {},
    instanceName: 'readiness-test',
  })
  const ready = readinessClient.waitForZero()

  root = createRoot(container)
  await act(async () => {
    root?.render(
      <readinessClient.ProvideZero
        cacheURL="http://127.0.0.1:7788/zero"
        userID="readiness"
      >
        <span>ok</span>
      </readinessClient.ProvideZero>
    )
    await Promise.resolve()
  })

  expect(await ready).toBe(fakeZero.instances.at(-1))
})

test('installs the supplied client transport for the provider server', async () => {
  const install = vi.fn()

  await mount({ transport: { install } }, 'transport-test')

  expect(install).toHaveBeenCalledOnce()
  expect(install).toHaveBeenCalledWith('http://127.0.0.1:7788/zero')
})

test('refreshAuth reconnects in place on needs-auth, once per transition', async () => {
  const refreshAuth = vi.fn(async () => 'fresh-token')
  const events: Array<{ type: string; reasonKey?: string }> = []
  const off = client.zeroEvents.listen((event) => {
    if (event) events.push(event)
  })
  const instance = await mount({ refreshAuth }, 'conn-auth')

  await act(async () => {
    instance.connection.state.set({ name: 'needs-auth', reason: 'token expired' })
    await Promise.resolve()
  })

  expect(refreshAuth).toHaveBeenCalledTimes(1)
  await act(async () => {
    await Promise.resolve()
  })
  expect(instance.connection.connect).toHaveBeenCalledWith({ auth: 'fresh-token' })
  expect(events).toContainEqual({
    type: 'error',
    reasonKey: 'connection-needs-auth',
    message: 'token expired',
  })
  off()
})

test('a disabled logout cannot revive its closed authenticated instance', async () => {
  const first = await mount({ auth: 'old-token' }, 'conn-auth-cycle')

  await act(async () => {
    root?.render(
      <client.ProvideZero
        cacheURL="http://127.0.0.1:7788/zero"
        userID="conn-auth-cycle"
        auth={undefined}
        disable
      >
        <span>ok</span>
      </client.ProvideZero>
    )
    await Promise.resolve()
  })

  expect(first.close).toHaveBeenCalledTimes(1)

  await act(async () => {
    root?.render(
      <client.ProvideZero
        cacheURL="http://127.0.0.1:7788/zero"
        userID="conn-auth-cycle"
        auth="fresh-token"
      >
        <span>ok</span>
      </client.ProvideZero>
    )
    await Promise.resolve()
  })

  expect(fakeZero.instances).toHaveLength(2)
  expect(first.close).toHaveBeenCalledTimes(1)
  expect(fakeZero.instances[1]).not.toBe(first)
  expect(fakeZero.instances[1]?.options?.auth).toBe('fresh-token')
})

test('a remounted provider reconnects its cached instance when auth changed', async () => {
  const instance = await mount({ auth: 'old-token' }, 'conn-auth-remount')

  await act(async () => {
    root?.unmount()
    root = null
    await Promise.resolve()
  })

  root = createRoot(container)
  await act(async () => {
    root?.render(
      <client.ProvideZero
        cacheURL="http://127.0.0.1:7788/zero"
        userID="conn-auth-remount"
        auth="fresh-token"
      >
        <span>ok</span>
      </client.ProvideZero>
    )
    await Promise.resolve()
  })

  expect(fakeZero.instances).toHaveLength(1)
  expect(instance.connection.connect).toHaveBeenCalledWith({ auth: 'fresh-token' })
})

test('a discarded auth render cannot consume the committed reconnect', async () => {
  const authClient = createZeroClient({
    schema,
    models: {},
    groupedQueries: {},
    instanceName: 'discarded-auth-test',
  })
  const renderedTokens: string[] = []
  const suspended = new Promise<void>(() => {})

  function RenderProbe({ auth, suspend }: { auth: string; suspend?: boolean }) {
    renderedTokens.push(auth)
    if (suspend) throw suspended
    return <span>{auth}</span>
  }

  const render = (auth: string, suspend = false) => (
    <StrictMode>
      <Suspense fallback={<span>waiting</span>}>
        <authClient.ProvideZero
          cacheURL="http://127.0.0.1:7788/zero"
          userID="discarded-auth-user"
          auth={auth}
        >
          <RenderProbe auth={auth} suspend={suspend} />
        </authClient.ProvideZero>
      </Suspense>
    </StrictMode>
  )

  root = createRoot(container)
  await act(async () => {
    root?.render(render('token-1'))
    await Promise.resolve()
  })
  const instance = fakeZero.instances.at(-1)!

  await act(async () => {
    startTransition(() => root?.render(render('token-2', true)))
    await new Promise((resolve) => setTimeout(resolve, 0))
  })

  expect(renderedTokens).toContain('token-2')
  expect(fakeZero.instances).toHaveLength(1)
  expect(instance.connection.connect).not.toHaveBeenCalled()

  await act(async () => {
    root?.render(render('token-2'))
    await Promise.resolve()
  })

  expect(fakeZero.instances).toHaveLength(1)
  expect(instance.connection.connect).toHaveBeenCalledTimes(1)
  expect(instance.connection.connect).toHaveBeenCalledWith({ auth: 'token-2' })
})

test('closing an unmounted provider retires its cached authenticated instance', async () => {
  const retiringClient = createZeroClient({
    schema,
    models: {},
    groupedQueries: {},
    instanceName: 'retire-auth-test',
  })
  root = createRoot(container)
  await act(async () => {
    root?.render(
      <retiringClient.ProvideZero
        cacheURL="http://127.0.0.1:7788/zero"
        userID="retire-auth-user"
        auth="token-1"
      >
        <span>first</span>
      </retiringClient.ProvideZero>
    )
    await Promise.resolve()
  })
  const first = fakeZero.instances.at(-1)!

  await act(async () => {
    root?.unmount()
    root = null
    await Promise.resolve()
  })
  retiringClient.zero.close()

  root = createRoot(container)
  await act(async () => {
    root?.render(
      <retiringClient.ProvideZero
        cacheURL="http://127.0.0.1:7788/zero"
        userID="retire-auth-user"
        auth="token-1"
      >
        <span>second</span>
      </retiringClient.ProvideZero>
    )
    await Promise.resolve()
  })

  expect(first.close).toHaveBeenCalledTimes(1)
  expect(fakeZero.instances).toHaveLength(2)
  expect(fakeZero.instances[1]).not.toBe(first)
})

test('stale-poke error reconnects instead of surfacing a fatal error', async () => {
  const events: Array<{ type: string }> = []
  const off = client.zeroEvents.listen((event) => {
    if (event) events.push(event)
  })
  const instance = await mount({}, 'conn-stale')

  await act(async () => {
    instance.connection.state.set({
      name: 'error',
      reason: 'Server returned unexpected base cookie during sync',
    })
    await Promise.resolve()
  })

  // reconnect issued, no auth arg (plain resume)
  expect(instance.connection.connect).toHaveBeenCalledTimes(1)
  expect(instance.connection.connect).toHaveBeenCalledWith()
  // the stale-poke reason must NOT be emitted as an error event
  expect(events.some((event) => event.type === 'error')).toBe(false)
  off()
})

test('transport error reconnects in place and publishes trying status', async () => {
  const events: Array<{ type: string; status?: string; reasonKey?: string }> = []
  const off = client.zeroEvents.listen((event) => {
    if (event) events.push(event)
  })
  const instance = await mount({}, 'conn-transport')

  await act(async () => {
    instance.connection.state.set({
      name: 'error',
      reason: 'Unexpected internal error: Failed to fetch',
    })
    await Promise.resolve()
  })

  expect(instance.connection.connect).toHaveBeenCalledTimes(1)
  expect(events).toContainEqual({
    type: 'reconnect',
    status: 'trying',
    reasonKey: 'transport',
    reason: 'Unexpected internal error: Failed to fetch',
  })
  expect(events.some((event) => event.type === 'error')).toBe(false)
  await act(async () => {
    instance.connection.state.set({ name: 'connected' })
    await Promise.resolve()
  })
  expect(events).toContainEqual({ type: 'reconnect', status: 'connected' })
  off()
})

test('server overload publishes waiting status while Zero owns retry backoff', async () => {
  const events: Array<{ type: string; status?: string; reasonKey?: string }> = []
  const off = client.zeroEvents.listen((event) => {
    if (event) events.push(event)
  })
  const instance = await mount({}, 'conn-overload')

  await act(async () => {
    instance.connection.state.set({
      name: 'connecting',
      reason: 'ServerOverloaded: retry later',
    })
    await Promise.resolve()
  })

  expect(instance.connection.connect).not.toHaveBeenCalled()
  expect(events).toContainEqual({
    type: 'reconnect',
    status: 'waiting',
    reasonKey: 'server-overloaded',
    reason: 'ServerOverloaded: retry later',
  })
  await act(async () => {
    instance.connection.state.set({ name: 'connecting' })
    await Promise.resolve()
  })
  expect(events).toContainEqual({
    type: 'reconnect',
    status: 'trying',
    reasonKey: 'server-overloaded',
    reason: 'ServerOverloaded: retry later',
  })
  off()
})

test('connectionDataset mirrors connection state onto the body dataset', async () => {
  const instance = await mount({ connectionDataset: true }, 'conn-dataset')

  await act(async () => {
    instance.connection.state.set({ name: 'connected' })
    await Promise.resolve()
  })
  expect(document.body.dataset.zeroState).toBe('connected')
  expect(document.body.dataset.zeroConnected).toBe('true')
  expect(document.body.dataset.zeroCacheUrl).toBe('http://127.0.0.1:7788/zero')

  await act(async () => {
    instance.connection.state.set({ name: 'error', reason: 'boom' })
    await Promise.resolve()
  })
  expect(document.body.dataset.zeroState).toBe('error')
  expect(document.body.dataset.zeroConnected).toBeUndefined()
  expect(document.body.dataset.zeroReason).toBe('boom')
})

test('fatal error state stays terminal and reports the reason', async () => {
  vi.useFakeTimers()
  try {
    const instance = await mount({}, 'conn-fatal')
    const events: Array<{ type: string; message?: string }> = []
    const off = client.zeroEvents.listen((event) => {
      if (event) events.push(event)
    })

    await act(async () => {
      instance.connection.state.set({
        name: 'error',
        reason: 'Got open event but connect start time is undefined',
      })
      await Promise.resolve()
    })

    await act(async () => {
      vi.advanceTimersByTime(60_000)
      await Promise.resolve()
    })
    expect(instance.connection.connect).not.toHaveBeenCalled()
    expect(events).toContainEqual({
      type: 'error',
      reasonKey: 'connection-error',
      message: 'Got open event but connect start time is undefined',
    })
    off()
  } finally {
    vi.useRealTimers()
  }
})

// the browser gets this recovery from ConnectionMonitor, a react component. a
// headless host has no tree, so before connectHeadless shared the same watcher
// it got none of it: an expired token parked the instance in needs-auth
// permanently, every pull answered 401, and the host went dark with no way back.
// this is the same transition as the provider test above, driven with no react.
test('connectHeadless refreshes auth on needs-auth with no react tree', async () => {
  const headlessClient = createZeroClient({
    schema,
    models: {},
    groupedQueries: {},
    instanceName: 'headless-auth-test',
  })
  const refreshAuth = vi.fn(async () => 'headless-fresh-token')
  const connection = headlessClient.connectHeadless({
    userID: 'headless-auth',
    cacheURL: 'http://127.0.0.1:7788/zero',
    refreshAuth,
  })
  const instance = fakeZero.instances.at(-1)!

  instance.connection.state.set({ name: 'needs-auth', reason: 'token expired' })
  await Promise.resolve()

  expect(refreshAuth).toHaveBeenCalledTimes(1)
  await Promise.resolve()
  expect(instance.connection.connect).toHaveBeenCalledWith({
    auth: 'headless-fresh-token',
  })

  // one refresh per transition, not per state emission — a stuck needs-auth
  // must not retry-storm the token endpoint.
  instance.connection.state.set({ name: 'needs-auth', reason: 'token expired again' })
  await Promise.resolve()
  expect(refreshAuth).toHaveBeenCalledTimes(1)

  await connection.close()
})
