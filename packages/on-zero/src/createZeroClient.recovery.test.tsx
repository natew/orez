// @vitest-environment jsdom

import { createSchema, string, table, UpdateNeededReasonType } from '@rocicorp/zero'
import { act, Suspense, useLayoutEffect } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { afterEach, beforeEach, expect, test, vi } from 'vitest'

const fakeZero = vi.hoisted(() => {
  // the client group the next constructed instance joins: tests bump this to
  // simulate newer code landing (a registry edit) between transitions.
  const group = { current: 'g1' }

  class FakeZero {
    readonly context = {}
    readonly connection!: {
      state: {
        current: { name: string }
        subscribe: (listener: () => void) => () => void
      }
      connect: ReturnType<typeof vi.fn>
    }
    readonly delete = vi.fn(async () => ({ errors: [] }))
    readonly close = vi.fn()
    readonly run = vi.fn(async () => [])
    readonly mutate = {
      note: {
        insert: vi.fn(() => ({
          client: Promise.resolve({}),
          server: Promise.resolve({}),
        })),
      },
    }
    readonly preload = vi.fn(() => ({
      cleanup: () => {},
      complete: Promise.resolve(),
    }))
    readonly clientGroupID: Promise<string>

    constructor(readonly options: Record<string, any>) {
      instances.push(this)
      this.clientGroupID = Promise.resolve(group.current)
      const listeners = new Set<() => void>()
      this.connection = {
        state: {
          current: { name: 'closed' },
          subscribe: (listener: () => void) => {
            listeners.add(listener)
            return () => {
              listeners.delete(listener)
            }
          },
        },
        connect: vi.fn(),
      }
      ;(this as any).__connectionListeners = listeners
    }
  }

  const instances: FakeZero[] = []

  function setConnectionState(instance: FakeZero, name: string) {
    instance.connection.state.current.name = name
    const listeners = (instance as any).__connectionListeners as Set<() => void>
    for (const listener of [...listeners]) listener()
  }

  return { FakeZero, instances, group, setConnectionState }
})

vi.mock('@rocicorp/zero', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@rocicorp/zero')>()
  return {
    ...actual,
    Zero: fakeZero.FakeZero,
  }
})

import { createZeroClient } from './createZeroClient'
import {
  isGroupTransitionOpen,
  resetRecoveryStateForTests,
} from './helpers/recoverZeroClient'

import type { ZeroEvent } from './types'

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
  instanceName: 'recovery-cache-test',
})

let container: HTMLDivElement
let root: Root | null

beforeEach(() => {
  window.sessionStorage.clear()
  resetRecoveryStateForTests()
  fakeZero.instances.length = 0
  fakeZero.group.current = 'g1'
  container = document.createElement('div')
  root = null
})

afterEach(() => {
  if (root) act(() => root?.unmount())
})

async function mount() {
  root = createRoot(container)
  await act(async () => {
    root?.render(
      <client.ProvideZero cacheURL="http://127.0.0.1:7777/zero" userID="team-machine">
        <span>ok</span>
      </client.ProvideZero>
    )
    await Promise.resolve()
  })
}

test('zero.delete invalidates cached instance before a remount recovery', async () => {
  await mount()
  const first = fakeZero.instances[0]
  expect(first).toBeDefined()

  const deleteZero = client.zero.delete
  await act(async () => {
    await deleteZero()
  })

  expect(first?.delete).toHaveBeenCalledTimes(1)
  expect(first?.close).toHaveBeenCalledTimes(1)

  act(() => root?.unmount())
  await mount()

  expect(fakeZero.instances).toHaveLength(2)
  expect(fakeZero.instances[1]).not.toBe(first)
})

test('remint drops local state and reconstructs a fresh instance in place', async () => {
  // unique userID so instanceKey misses the module-level cache from prior tests
  // and a fresh instance is genuinely constructed here.
  root = createRoot(container)
  await act(async () => {
    root?.render(
      <client.ProvideZero cacheURL="http://127.0.0.1:7777/zero" userID="remint-test">
        <span>ok</span>
      </client.ProvideZero>
    )
    await Promise.resolve()
  })
  const first = fakeZero.instances.at(-1)
  expect(first).toBeDefined()
  const countBefore = fakeZero.instances.length

  let result: boolean | undefined
  await act(async () => {
    result = await client.remint()
    // let the generation bump re-render and the rotate effect mint the fresh one
    await Promise.resolve()
  })

  expect(result).toBe(true)
  // dropped the rejected store + closed it, then minted a genuinely new client
  // without unmounting the provider (no page reload).
  expect(first?.delete).toHaveBeenCalledTimes(1)
  expect(fakeZero.instances.length).toBe(countBefore + 1)
  expect(fakeZero.instances.at(-1)).not.toBe(first)
})

test('closed headless connection ignores a late update without a recovery error', async () => {
  const isolated = createZeroClient({
    schema,
    models: {},
    groupedQueries: {},
    instanceName: 'closed-headless-recovery-test',
  })
  const connection = isolated.connectHeadless({
    cacheURL: 'http://127.0.0.1:7777/zero',
    userID: 'closed-headless-recovery',
    kvStore: 'mem',
    storageKey: 'closed-headless-recovery',
  })
  const instance = fakeZero.instances.at(-1)
  expect(instance).toBeDefined()
  await connection.close()

  const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
  try {
    instance?.options.onUpdateNeeded({
      type: UpdateNeededReasonType.NewClientGroup,
    })
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(consoleError).not.toHaveBeenCalledWith(
      '[on-zero] recovery could not reload or reconstruct the client'
    )
  } finally {
    consoleError.mockRestore()
  }
})

test('provider generation changes with the actual instance during remint', async () => {
  const generationClient = createZeroClient({
    schema,
    models: {},
    groupedQueries: {},
    instanceName: 'provider-generation-test',
  })
  const generations: Array<
    NonNullable<ReturnType<typeof generationClient.useZeroProviderGeneration>>
  > = []

  function GenerationProbe() {
    const generation = generationClient.useZeroProviderGeneration()
    useLayoutEffect(() => {
      if (generation) generations.push(generation)
    }, [generation])
    return null
  }

  root = createRoot(container)
  await act(async () => {
    root?.render(
      <generationClient.ProvideZero
        cacheURL="http://127.0.0.1:7777/zero"
        userID="generation-test"
      >
        <GenerationProbe />
      </generationClient.ProvideZero>
    )
    await Promise.resolve()
  })

  expect(generations).toHaveLength(1)
  const firstGeneration = generations[0]
  expect(firstGeneration?.isCurrent()).toBe(true)

  await act(async () => {
    expect(await generationClient.remint({ dropLocalState: false })).toBe(true)
    await Promise.resolve()
  })

  expect(generations).toHaveLength(2)
  expect(generations[1]).not.toBe(firstGeneration)
  expect(firstGeneration?.isCurrent()).toBe(false)
  expect(generations[1]?.isCurrent()).toBe(true)

  act(() => root?.unmount())
  root = null
  expect(generations[1]?.isCurrent()).toBe(false)
})

test('suspense hiding keeps the mounted provider generation writable', async () => {
  const isolated = createZeroClient({
    schema,
    models: {},
    groupedQueries: {},
    instanceName: 'suspense-hidden-provider-test',
  })
  let hidden = false
  const hiddenUntil = new Promise<void>(() => {})

  function MaybeHidden() {
    if (hidden) throw hiddenUntil
    return <span>ready</span>
  }

  function App() {
    return (
      <Suspense fallback={<span>loading</span>}>
        <isolated.ProvideZero
          cacheURL="http://127.0.0.1:7777/zero"
          userID="suspense-hidden"
        >
          <MaybeHidden />
        </isolated.ProvideZero>
      </Suspense>
    )
  }

  root = createRoot(container)
  await act(async () => {
    root?.render(<App />)
    await Promise.resolve()
  })

  let settleServer: (value: string) => void = () => {}
  const server = new Promise<string>((resolve) => {
    settleServer = resolve
  })
  let settled = false
  const acknowledgement = isolated
    .awaitMutationServer(
      { client: Promise.resolve('client'), server },
      'hidden provider mutation'
    )
    .finally(() => {
      settled = true
    })

  hidden = true
  await act(async () => {
    root?.render(<App />)
    await Promise.resolve()
  })

  expect(container.textContent).toContain('loading')
  expect(settled).toBe(false)
  settleServer('server')
  await expect(acknowledgement).resolves.toBe('server')
})

test('remint with no provider mounted returns false without burning the guard budget', async () => {
  // own client so the shared remint guard state is fresh for this assertion.
  const isolated = createZeroClient({
    schema,
    models: {},
    groupedQueries: {},
    instanceName: 'remint-unmounted-test',
  })

  // nothing mounted → remintControl.bump is null. these must NOT start the 12s
  // cooldown or consume the attempt budget.
  expect(await isolated.remint()).toBe(false)
  expect(await isolated.remint()).toBe(false)

  root = createRoot(container)
  await act(async () => {
    root?.render(
      <isolated.ProvideZero
        cacheURL="http://127.0.0.1:7777/zero"
        userID="remint-unmounted"
      >
        <span>ok</span>
      </isolated.ProvideZero>
    )
    await Promise.resolve()
  })

  // a mounted remint immediately after is still allowed — proof the unmounted
  // calls didn't burn the guard (old code set lastRemintAt before this check).
  let result: boolean | undefined
  await act(async () => {
    result = await isolated.remint()
    await Promise.resolve()
  })
  expect(result).toBe(true)
})

test('two consecutive server acknowledgement timeouts reconnect without reload or delete', async () => {
  vi.useFakeTimers()
  try {
    const isolated = createZeroClient({
      schema,
      models: {},
      groupedQueries: {},
      instanceName: 'ack-timeout-recovery-test',
    })
    const scheduleReload = vi.fn()
    const events: Array<{ type: string; status?: string; reasonKey?: string }> = []
    const off = isolated.zeroEvents.listen((event) => {
      if (event) events.push(event)
    })

    root = createRoot(container)
    await act(async () => {
      root?.render(
        <isolated.ProvideZero
          cacheURL="http://127.0.0.1:7777/zero"
          userID="ack-timeout"
          scheduleReload={scheduleReload}
        >
          <span>ok</span>
        </isolated.ProvideZero>
      )
      await Promise.resolve()
    })

    const timeout = async (label: string) => {
      const result = isolated.awaitMutationServer(
        { client: Promise.resolve({}), server: new Promise(() => {}) },
        label,
        10
      )
      const timedOut = expect(result).rejects.toMatchObject({
        name: 'MutationTimeoutError',
        phase: 'server',
      })
      await act(async () => {
        await vi.advanceTimersByTimeAsync(10)
      })
      await timedOut
    }

    await timeout('first write')
    expect(events).toEqual([])
    await timeout('second write')
    await act(async () => {
      await Promise.resolve()
    })
    expect(events).toContainEqual({
      type: 'reconnect',
      status: 'trying',
      reasonKey: 'server-ack-timeout',
      reason:
        'second write server acknowledgement timed out 2 consecutive times (10ms each)',
    })
    expect(scheduleReload).not.toHaveBeenCalled()
    expect(fakeZero.instances[0]?.delete).not.toHaveBeenCalled()
    off()
  } finally {
    vi.useRealTimers()
  }
})

test('transport and app benign log patterns suppress classified recovery', async () => {
  const isolated = createZeroClient({
    schema,
    models: {},
    groupedQueries: {},
    instanceName: 'transport-log-classification-test',
  })
  const scheduleReload = vi.fn()
  const install = vi.fn()
  root = createRoot(container)
  await act(async () => {
    root?.render(
      <isolated.ProvideZero
        cacheURL="http://127.0.0.1:7777/zero"
        userID="transport-log"
        transport={{
          install,
          logClassifications: { benign: ['ClientNotFound'] },
        }}
        benignLogPatterns={[/sent mutation ID .* but expected/]}
        scheduleReload={scheduleReload}
      >
        <span>ok</span>
      </isolated.ProvideZero>
    )
    await Promise.resolve()
  })

  const instance = fakeZero.instances.at(-1)!
  instance.options.logSink.log('error', undefined, 'ClientNotFound: cold boot')
  instance.options.logSink.log('error', undefined, 'sent mutation ID 5 but expected 4')
  await Promise.resolve()
  expect(install).toHaveBeenCalledOnce()
  expect(scheduleReload).not.toHaveBeenCalled()
})

test('retire tears down on every sign-out, including inside remint guard window', async () => {
  // own client so remint's shared guard state cannot mask the assertion.
  const isolated = createZeroClient({
    schema,
    models: {},
    groupedQueries: {},
    instanceName: 'retire-signout-test',
  })

  root = createRoot(container)
  await act(async () => {
    root?.render(
      <isolated.ProvideZero cacheURL="http://127.0.0.1:7777/zero" userID="retire-signout">
        <span>ok</span>
      </isolated.ProvideZero>
    )
    await Promise.resolve()
  })

  const first = fakeZero.instances.at(-1)
  expect(first).toBeDefined()

  await act(async () => {
    await isolated.retire()
    await Promise.resolve()
  })

  // dropped the signed-out account's local store and minted a clean client
  expect(first?.delete).toHaveBeenCalledTimes(1)
  expect(first?.close).toHaveBeenCalledTimes(1)
  const second = fakeZero.instances.at(-1)
  expect(second).not.toBe(first)

  // a second sign-out immediately after is well inside remint's 12s cooldown.
  // it must still drop the store — remint() refuses here and would leave the
  // previous account's local state cached for the next sign-in to revive.
  await act(async () => {
    await isolated.retire()
    await Promise.resolve()
  })

  expect(second?.delete).toHaveBeenCalledTimes(1)
  expect(fakeZero.instances.at(-1)).not.toBe(second)
})

test('headless NewClientGroup recovery reconstructs its client without a provider', async () => {
  const isolated = createZeroClient({
    schema,
    models: {},
    groupedQueries: {},
    instanceName: 'headless-recovery-test',
  })
  const connection = isolated.connectHeadless({
    cacheURL: 'http://127.0.0.1:7777/zero',
    userID: 'headless-recovery',
  })
  const first = fakeZero.instances.at(-1)!
  const countBefore = fakeZero.instances.length

  first.options.onUpdateNeeded({ type: 'NewClientGroup' })

  await vi.waitFor(() => expect(fakeZero.instances).toHaveLength(countBefore + 1))
  expect(connection.zero).not.toBe(first)
  expect(await isolated.waitForZero()).toBe(connection.zero)
  expect(first.close).toHaveBeenCalledOnce()

  await connection.close()
})

function makeTransitionClient(instanceName: string) {
  return createZeroClient({
    schema,
    models: {},
    groupedQueries: {},
    instanceName,
  })
}

function collectClientEvents(client: {
  zeroEvents: { listen: (listener: (event: ZeroEvent | null) => void) => () => void }
}) {
  const events: ZeroEvent[] = []
  const off = client.zeroEvents.listen((event) => {
    if (event) events.push(event)
  })
  return { events, off }
}

async function mountTransitionClient(
  client: ReturnType<typeof makeTransitionClient>,
  userID: string
) {
  const el = document.createElement('div')
  const mountedRoot = createRoot(el)
  await act(async () => {
    mountedRoot.render(
      <client.ProvideZero cacheURL="http://127.0.0.1:7777/zero" userID={userID}>
        <span>ok</span>
      </client.ProvideZero>
    )
    await Promise.resolve()
  })
  return { el, root: mountedRoot, instance: fakeZero.instances.at(-1)! }
}

async function unmountTransitionClient(mounted: { root: Root }) {
  await act(async () => {
    mounted.root.unmount()
  })
}

function withoutPageReload() {
  const originalLocation = globalThis.location
  Object.defineProperty(globalThis, 'location', {
    configurable: true,
    value: undefined,
  })
  return () => {
    Object.defineProperty(globalThis, 'location', {
      configurable: true,
      value: originalLocation,
    })
  }
}

test('two providers ride one group transition and keep querying and mutating', async () => {
  const restoreLocation = withoutPageReload()
  const clientA = makeTransitionClient('transition-two-a')
  const clientB = makeTransitionClient('transition-two-b')
  const collectedA = collectClientEvents(clientA)
  const collectedB = collectClientEvents(clientB)
  // siblings of one user ride one transition.
  const mountedA = await mountTransitionClient(clientA, 'transition-user')
  const mountedB = await mountTransitionClient(clientB, 'transition-user')
  const firstA = mountedA.instance
  const firstB = mountedB.instance
  const countBefore = fakeZero.instances.length
  try {
    // one registry change: both stale instances fire in the same tick.
    firstA.options.onUpdateNeeded({ type: 'NewClientGroup' })
    firstB.options.onUpdateNeeded({ type: 'NewClientGroup' })
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 0))
    })

    // each old instance closed and reminted exactly once.
    expect(fakeZero.instances).toHaveLength(countBefore + 2)
    const secondA = fakeZero.instances[countBefore]!
    const secondB = fakeZero.instances[countBefore + 1]!
    expect(secondA).not.toBe(firstA)
    expect(secondB).not.toBe(firstB)
    expect(firstA.close).toHaveBeenCalledTimes(1)
    expect(firstB.close).toHaveBeenCalledTimes(1)

    // the transition resolves only when every new group is connected.
    expect(isGroupTransitionOpen()).toBe(true)
    await act(async () => {
      fakeZero.setConnectionState(secondA, 'connected')
      fakeZero.setConnectionState(secondB, 'connected')
      await Promise.resolve()
    })
    await vi.waitFor(() => expect(isGroupTransitionOpen()).toBe(false))

    // one transition: exactly one recovering across both providers, no fatal.
    const allEvents = [...collectedA.events, ...collectedB.events]
    expect(allEvents.filter((event) => event.type === 'recovering')).toHaveLength(1)
    expect(allEvents.filter((event) => event.type === 'fatal')).toHaveLength(0)
    // NewClientGroup never deletes IndexedDB.
    expect(firstA.delete).not.toHaveBeenCalled()
    expect(firstB.delete).not.toHaveBeenCalled()

    // both providers still query and mutate through their new instances.
    await clientA.zero.run({} as never)
    await clientB.zero.run({} as never)
    expect(secondA.run).toHaveBeenCalled()
    expect(secondB.run).toHaveBeenCalled()
    // the fake instance carries an untyped mutate stub: the assertion is that
    // the facade resolves it on the NEW instance at call time.
    ;(clientA.zero.mutate as any).note.insert({ id: 'a1' })
    ;(clientB.zero.mutate as any).note.insert({ id: 'b1' })
    expect(secondA.mutate.note.insert).toHaveBeenCalledTimes(1)
    expect(secondB.mutate.note.insert).toHaveBeenCalledTimes(1)
  } finally {
    collectedA.off()
    collectedB.off()
    await unmountTransitionClient(mountedA)
    await unmountTransitionClient(mountedB)
    restoreLocation()
  }
})

test('a second registry change after recovery remints again without fatal', async () => {
  const restoreLocation = withoutPageReload()
  const clientA = makeTransitionClient('transition-second-a')
  const clientB = makeTransitionClient('transition-second-b')
  const collectedA = collectClientEvents(clientA)
  const collectedB = collectClientEvents(clientB)
  const mountedA = await mountTransitionClient(clientA, 'transition-second-user')
  const mountedB = await mountTransitionClient(clientB, 'transition-second-user')
  try {
    const fireBoth = (instances: Array<{ options: Record<string, any> }>) => {
      instances[0]!.options.onUpdateNeeded({ type: 'NewClientGroup' })
      instances[1]!.options.onUpdateNeeded({ type: 'NewClientGroup' })
    }
    const settleRemints = async (countBefore: number) => {
      await act(async () => {
        await new Promise((resolve) => setTimeout(resolve, 0))
      })
      expect(fakeZero.instances).toHaveLength(countBefore + 2)
      const next = fakeZero.instances.slice(countBefore)
      await act(async () => {
        for (const instance of next) fakeZero.setConnectionState(instance, 'connected')
        await Promise.resolve()
      })
      await vi.waitFor(() => expect(isGroupTransitionOpen()).toBe(false))
      return next
    }

    // first change.
    const countBeforeFirst = fakeZero.instances.length
    fireBoth(fakeZero.instances.slice(-2))
    await settleRemints(countBeforeFirst)

    // second change with newer code, well inside the old 12s remint cooldown.
    fakeZero.group.current = 'g2'
    const countBefore = fakeZero.instances.length
    fireBoth(fakeZero.instances.slice(-2))
    const third = await settleRemints(countBefore)

    const allEvents = [...collectedA.events, ...collectedB.events]
    expect(allEvents.filter((event) => event.type === 'recovering')).toHaveLength(2)
    expect(allEvents.filter((event) => event.type === 'fatal')).toHaveLength(0)
    // the facades resolve to the newest instances.
    await clientA.zero.run({} as never)
    await clientB.zero.run({} as never)
    expect(third[0]!.run).toHaveBeenCalled()
    expect(third[1]!.run).toHaveBeenCalled()
  } finally {
    collectedA.off()
    collectedB.off()
    await unmountTransitionClient(mountedA)
    await unmountTransitionClient(mountedB)
    restoreLocation()
  }
})

test('negative control: an incompatible schema emits exactly one terminal error, never a loop', async () => {
  const restoreLocation = withoutPageReload()
  const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
  const clientA = makeTransitionClient('transition-negative-a')
  const clientB = makeTransitionClient('transition-negative-b')
  const collectedA = collectClientEvents(clientA)
  const collectedB = collectClientEvents(clientB)
  const mountedA = await mountTransitionClient(clientA, 'transition-negative-user')
  const mountedB = await mountTransitionClient(clientB, 'transition-negative-user')
  const firstA = mountedA.instance
  const firstB = mountedB.instance
  const countBefore = fakeZero.instances.length
  try {
    firstA.options.onUpdateNeeded({ type: 'SchemaVersionNotSupported' })
    firstB.options.onUpdateNeeded({ type: 'SchemaVersionNotSupported' })
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 0))
    })
    // schema incompatibility drops the stale stores, then remints once each.
    expect(firstA.delete).toHaveBeenCalledTimes(1)
    expect(firstB.delete).toHaveBeenCalledTimes(1)
    expect(fakeZero.instances).toHaveLength(countBefore + 2)
    const secondA = fakeZero.instances[countBefore]!
    const secondB = fakeZero.instances[countBefore + 1]!
    expect(isGroupTransitionOpen()).toBe(true)

    // the new code still cannot join the server group: same group fires again.
    secondA.options.onUpdateNeeded({ type: 'SchemaVersionNotSupported' })
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 0))
    })

    const allEvents = () => [...collectedA.events, ...collectedB.events]
    expect(allEvents().filter((event) => event.type === 'fatal')).toHaveLength(1)
    const [fatal] = allEvents().filter((event) => event.type === 'fatal')
    expect(fatal).toMatchObject({
      reasonKey: 'SchemaVersionNotSupported',
      reason: expect.stringContaining('g1'),
    })
    expect(consoleError).toHaveBeenCalledTimes(1)
    expect(consoleError.mock.calls[0]![0]).toContain('g1')

    // every later re-fire stays silent and remints nothing: never a loop.
    secondB.options.onUpdateNeeded({ type: 'SchemaVersionNotSupported' })
    secondA.options.onUpdateNeeded({ type: 'SchemaVersionNotSupported' })
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 0))
    })
    expect(allEvents().filter((event) => event.type === 'fatal')).toHaveLength(1)
    expect(consoleError).toHaveBeenCalledTimes(1)
    expect(fakeZero.instances).toHaveLength(countBefore + 2)
  } finally {
    collectedA.off()
    collectedB.off()
    await unmountTransitionClient(mountedA)
    await unmountTransitionClient(mountedB)
    restoreLocation()
    consoleError.mockRestore()
  }
})

test('concurrent update signals across providers schedule a single reload', async () => {
  const reloadPage = vi.fn()
  const originalLocation = globalThis.location
  Object.defineProperty(globalThis, 'location', {
    configurable: true,
    value: { href: 'http://localhost/reload-test', reload: reloadPage },
  })
  const scheduleA = vi.fn()
  const scheduleB = vi.fn()
  const clientA = makeTransitionClient('transition-reload-a')
  const clientB = makeTransitionClient('transition-reload-b')
  const collectedA = collectClientEvents(clientA)
  const collectedB = collectClientEvents(clientB)
  const elA = document.createElement('div')
  const elB = document.createElement('div')
  const rootA = createRoot(elA)
  const rootB = createRoot(elB)
  try {
    await act(async () => {
      rootA.render(
        <clientA.ProvideZero
          cacheURL="http://127.0.0.1:7777/zero"
          userID="transition-reload-user"
          scheduleReload={scheduleA}
        >
          <span>ok</span>
        </clientA.ProvideZero>
      )
      await Promise.resolve()
    })
    await act(async () => {
      rootB.render(
        <clientB.ProvideZero
          cacheURL="http://127.0.0.1:7777/zero"
          userID="transition-reload-user"
          scheduleReload={scheduleB}
        >
          <span>ok</span>
        </clientB.ProvideZero>
      )
      await Promise.resolve()
    })
    const firstA = fakeZero.instances.at(-2)!
    const firstB = fakeZero.instances.at(-1)!

    firstA.options.onUpdateNeeded({ type: 'NewClientGroup' })
    firstB.options.onUpdateNeeded({ type: 'NewClientGroup' })
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 0))
    })

    // one transition drives one deferred reload, however many providers join.
    expect(scheduleA.mock.calls.length + scheduleB.mock.calls.length).toBe(1)
    expect(reloadPage).not.toHaveBeenCalled()
    const allEvents = [...collectedA.events, ...collectedB.events]
    expect(allEvents.filter((event) => event.type === 'fatal')).toHaveLength(0)

    const ctx = (scheduleA.mock.calls[0] ?? scheduleB.mock.calls[0])![0]
    await act(async () => {
      await ctx.performReload()
    })
    expect(reloadPage).toHaveBeenCalledTimes(1)
    expect(isGroupTransitionOpen()).toBe(false)
    // the reload persisted the recovered-from groups for the next page load.
    const marker = window.sessionStorage.getItem(
      'on-zero-recover-http://localhost/reload-test-NewClientGroup'
    )
    expect(marker).toContain('g1')
  } finally {
    collectedA.off()
    collectedB.off()
    await act(async () => {
      rootA.unmount()
    })
    await act(async () => {
      rootB.unmount()
    })
    Object.defineProperty(globalThis, 'location', {
      configurable: true,
      value: originalLocation,
    })
  }
})
