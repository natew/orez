import { UpdateNeededReasonType } from '@rocicorp/zero'
import { beforeEach, describe, expect, test, vi } from 'vitest'

// @vitest-environment jsdom
import { createEmitter } from './emitter'
import {
  classifyZeroRecoveryLog,
  composeRecoveryLogSink,
  isGroupTransitionOpen,
  isRecoverableZeroStalePokeMessage,
  makeZeroRecovery,
  resetRecoveryStateForTests,
} from './recoverZeroClient'

import type { ZeroEvent } from '../types'
import type { ScheduleReloadContext, ZeroRecoveryDeps } from './recoverZeroClient'

let emitterSeq = 0

function setup() {
  const events: ZeroEvent[] = []
  const zeroEvents = createEmitter<ZeroEvent | null>(`test-recover-${emitterSeq++}`, null)
  zeroEvents.listen((event) => {
    if (event) events.push(event)
  })
  const deleteLocalState = vi.fn(() => Promise.resolve())
  const reload = vi.fn()
  const deps: ZeroRecoveryDeps = { deleteLocalState, zeroEvents, reload }
  return { deps, deleteLocalState, reload, events }
}

// recovery chains deletes -> beforeReload -> reload across several microtasks;
// a macrotask boundary drains the whole chain deterministically.
async function flush() {
  await new Promise((resolve) => setTimeout(resolve, 0))
}

beforeEach(() => {
  window.sessionStorage.clear()
  resetRecoveryStateForTests()
})

describe('zero recovery', () => {
  test('SchemaVersionNotSupported drops local state and reloads, emitting recovering', async () => {
    const { deps, deleteLocalState, reload, events } = setup()
    makeZeroRecovery(deps).onUpdateNeeded({
      type: UpdateNeededReasonType.SchemaVersionNotSupported,
    })
    expect(events).toEqual([
      {
        type: 'recovering',
        reasonKey: 'SchemaVersionNotSupported',
        reason: expect.stringContaining('SchemaVersionNotSupported'),
      },
    ])
    await flush()
    expect(deleteLocalState).toHaveBeenCalledTimes(1)
    expect(reload).toHaveBeenCalledTimes(1)
  })

  test('NewClientGroup / VersionNotSupported reload WITHOUT deleting (sibling-tab safe)', async () => {
    const { deps, deleteLocalState, reload } = setup()
    const recovery = makeZeroRecovery(deps)
    recovery.onUpdateNeeded({ type: UpdateNeededReasonType.NewClientGroup })
    await flush()
    resetRecoveryStateForTests()
    recovery.onUpdateNeeded({ type: UpdateNeededReasonType.VersionNotSupported })
    await flush()
    expect(deleteLocalState).not.toHaveBeenCalled()
    expect(reload).toHaveBeenCalledTimes(2)
  })

  test('onClientStateNotFound drops local state and reloads', async () => {
    const { deps, deleteLocalState, reload } = setup()
    makeZeroRecovery(deps).onClientStateNotFound()
    await flush()
    expect(deleteLocalState).toHaveBeenCalledTimes(1)
    expect(reload).toHaveBeenCalledTimes(1)
  })

  test('combined client: every instance deletes its own store, but only ONE reload', async () => {
    // two instances (control + project) fail on the same page-load. each must
    // drop its OWN store; only one page reload should fire.
    const a = setup()
    const b = setup()
    makeZeroRecovery(a.deps).onClientStateNotFound()
    makeZeroRecovery(b.deps).onClientStateNotFound()
    await flush()
    expect(a.deleteLocalState).toHaveBeenCalledTimes(1)
    expect(b.deleteLocalState).toHaveBeenCalledTimes(1)
    expect(a.reload.mock.calls.length + b.reload.mock.calls.length).toBe(1)
  })

  test('combined native client remints every instance when page reload is unavailable', async () => {
    const a = setup()
    const b = setup()
    a.deps.reload = undefined
    b.deps.reload = undefined
    const recoverA = vi.fn(() => Promise.resolve(true))
    const recoverB = vi.fn(() => Promise.resolve(true))
    a.deps.recoverInPlace = recoverA
    b.deps.recoverInPlace = recoverB
    const originalLocation = globalThis.location
    Object.defineProperty(globalThis, 'location', {
      configurable: true,
      value: undefined,
    })
    try {
      makeZeroRecovery(a.deps).onClientStateNotFound()
      makeZeroRecovery(b.deps).onClientStateNotFound()
      await flush()
    } finally {
      Object.defineProperty(globalThis, 'location', {
        configurable: true,
        value: originalLocation,
      })
    }
    expect(recoverA).toHaveBeenCalledTimes(1)
    expect(recoverB).toHaveBeenCalledTimes(1)
  })

  test('duplicate native recovery signals remint one client once', async () => {
    const { deps } = setup()
    deps.reload = undefined
    const recoverInPlace = vi
      .fn<() => Promise<boolean>>()
      .mockResolvedValueOnce(true)
      .mockResolvedValue(false)
    deps.recoverInPlace = recoverInPlace
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
    const originalLocation = globalThis.location
    Object.defineProperty(globalThis, 'location', {
      configurable: true,
      value: undefined,
    })
    try {
      const recovery = makeZeroRecovery(deps)
      recovery.onUpdateNeeded({ type: UpdateNeededReasonType.NewClientGroup })
      recovery.onUpdateNeeded({ type: UpdateNeededReasonType.NewClientGroup })
      await flush()
      expect(recoverInPlace).toHaveBeenCalledTimes(1)
      expect(consoleError).not.toHaveBeenCalledWith(
        '[on-zero] recovery could not reload or reconstruct the client'
      )
    } finally {
      Object.defineProperty(globalThis, 'location', {
        configurable: true,
        value: originalLocation,
      })
      consoleError.mockRestore()
    }
  })

  test('a second trigger in the same page-load adds no extra reload or fatal', async () => {
    const { deps, reload, events } = setup()
    const recovery = makeZeroRecovery(deps)
    recovery.onClientStateNotFound()
    recovery.onUpdateNeeded({ type: UpdateNeededReasonType.NewClientGroup })
    await flush()
    expect(reload).toHaveBeenCalledTimes(1)
    expect(events.filter((event) => event.type === 'fatal')).toEqual([])
  })

  test('after a reload, a re-failing reason emits fatal instead of reloading again', async () => {
    const { deps, reload, events } = setup()
    const recovery = makeZeroRecovery(deps)
    recovery.onClientStateNotFound()
    await flush()
    resetRecoveryStateForTests() // simulate the page reload clearing in-memory state
    recovery.onClientStateNotFound()
    await flush()
    expect(reload).toHaveBeenCalledTimes(1)
    expect(events.some((event) => event.type === 'fatal')).toBe(true)
  })

  test('after a reload, a different reason still recovers', async () => {
    const { deps, reload } = setup()
    const recovery = makeZeroRecovery(deps)
    recovery.onClientStateNotFound()
    await flush()
    resetRecoveryStateForTests()
    recovery.onUpdateNeeded({ type: UpdateNeededReasonType.SchemaVersionNotSupported })
    await flush()
    expect(reload).toHaveBeenCalledTimes(2)
  })

  test('same-origin sibling documents do not share a recovery guard', async () => {
    const { deps, reload, events } = setup()
    const originalUrl = window.location.href
    try {
      window.history.replaceState({}, '', '/preview/first')
      makeZeroRecovery(deps).onClientStateNotFound()
      await flush()
      resetRecoveryStateForTests()
      window.history.replaceState({}, '', '/preview/second')
      makeZeroRecovery(deps).onClientStateNotFound()
      await flush()
    } finally {
      window.history.replaceState({}, '', originalUrl)
    }
    expect(reload).toHaveBeenCalledTimes(2)
    expect(events.filter((event) => event.type === 'fatal')).toEqual([])
  })

  test('logSink recovers on local-store-lost and forwards to the consumer sink', async () => {
    const { deps, deleteLocalState, reload } = setup()
    const consumer = { log: vi.fn() }
    const sink = composeRecoveryLogSink(deps, consumer)
    sink.log('error', undefined, 'Error during persist: Expected IndexedDB not found')
    expect(consumer.log).toHaveBeenCalledTimes(1)
    await flush()
    expect(deleteLocalState).toHaveBeenCalledTimes(1)
    expect(reload).toHaveBeenCalledTimes(1)
  })

  test('logSink recovers on native sqlite finalized-statement local-store loss', async () => {
    const { deps, deleteLocalState, reload } = setup()
    const sqliteError = new Error('This statement has been finalized')
    sqliteError.name = 'SqliteError'
    const sink = composeRecoveryLogSink(deps)
    sink.log('error', { bgIntervalProcess: 'Heartbeat' }, 'Error running.', sqliteError)
    await flush()
    expect(deleteLocalState).toHaveBeenCalledTimes(1)
    expect(reload).toHaveBeenCalledTimes(1)
  })

  test('logSink recovers on repeated store-closed local-store loss', async () => {
    const nowSpy = vi.spyOn(Date, 'now')
    const { deps, deleteLocalState, reload } = setup()
    try {
      const sink = composeRecoveryLogSink(deps)
      nowSpy.mockReturnValue(10_000)
      sink.log('error', undefined, 'Failed to connect.', new Error('Store is closed'))
      await flush()
      expect(deleteLocalState).not.toHaveBeenCalled()
      expect(reload).not.toHaveBeenCalled()

      nowSpy.mockReturnValue(13_000)
      sink.log('error', undefined, 'Failed to connect.', new Error('Store is closed'))
      await flush()
      expect(deleteLocalState).toHaveBeenCalledTimes(1)
      expect(reload).toHaveBeenCalledTimes(1)
    } finally {
      nowSpy.mockRestore()
    }
  })

  test('logSink with no consumer preserves console output and still watches', async () => {
    const { deps, deleteLocalState, reload } = setup()
    const infoSpy = vi.spyOn(console, 'info').mockImplementation(() => {})
    const sink = composeRecoveryLogSink(deps)
    sink.log('info', { worker: 'sync' }, 'connected')
    expect(infoSpy).toHaveBeenCalledWith('worker=sync', 'connected')
    infoSpy.mockRestore()
    sink.log('error', undefined, 'Expected IndexedDB not found')
    await flush()
    expect(deleteLocalState).toHaveBeenCalledTimes(1)
    expect(reload).toHaveBeenCalledTimes(1)
  })

  test('logSink flush calls through the consumer sink (preserves its this)', async () => {
    const { deps } = setup()
    class ClassSink {
      flushed = false
      log(): void {}
      flush(): Promise<void> {
        this.flushed = true
        return Promise.resolve()
      }
    }
    const consumer = new ClassSink()
    const sink = composeRecoveryLogSink(deps, consumer)
    await sink.flush?.()
    expect(consumer.flushed).toBe(true)
  })

  test('logSink ignores non-error level and non-matching messages', async () => {
    const { deps, deleteLocalState, reload } = setup()
    const sink = composeRecoveryLogSink(deps)
    sink.log('info', undefined, 'Expected IndexedDB not found')
    sink.log('error', undefined, 'some unrelated error')
    await flush()
    expect(deleteLocalState).not.toHaveBeenCalled()
    expect(reload).not.toHaveBeenCalled()
  })

  test('recovery log classification is narrow', () => {
    const sqliteError = new Error('This statement has been finalized')
    sqliteError.name = 'SqliteError'
    expect(
      classifyZeroRecoveryLog('error', ['Error running.', sqliteError])
    ).toMatchObject({
      reasonKey: 'sqlite-statement-finalized',
      dropLocalState: true,
    })
    expect(
      classifyZeroRecoveryLog('error', [
        { name: 'SqliteError', message: 'database is locked' },
      ])
    ).toBeUndefined()
    expect(
      classifyZeroRecoveryLog('warn', ['Expected IndexedDB not found'])
    ).toBeUndefined()
    expect(classifyZeroRecoveryLog('error', ['Store is closed'], 10_000)).toBeUndefined()
    expect(classifyZeroRecoveryLog('error', ['Store is closed'], 11_000)).toBeUndefined()
    expect(classifyZeroRecoveryLog('error', ['Store is closed'], 14_000)).toMatchObject({
      reasonKey: 'store-closed-repeat',
      dropLocalState: true,
    })
    resetRecoveryStateForTests()
    expect(
      classifyZeroRecoveryLog(
        'error',
        ['Mutator "send" error on server', 'Store is closed'],
        20_000
      )
    ).toBeUndefined()
  })

  // a host with a `window` shim but no real `location` — the sootsim tenant
  // render-worker, which hides `location` for isolation. the DEFAULT reload
  // path (no injected `deps.reload`) must still drop stale IDB and reconstruct
  // the client in place rather than leaving the worker on the rejected client.
  test('default reload path remints in place when location is absent', async () => {
    const events: ZeroEvent[] = []
    const zeroEvents = createEmitter<ZeroEvent | null>(
      `test-recover-${emitterSeq++}`,
      null
    )
    zeroEvents.listen((event) => {
      if (event) events.push(event)
    })
    const deleteLocalState = vi.fn(() => Promise.resolve())
    const recoverInPlace = vi.fn(() => Promise.resolve(true))
    // no `reload` dep — exercises the default `globalThis.location?.reload?.()`.
    const deps: ZeroRecoveryDeps = { deleteLocalState, zeroEvents, recoverInPlace }
    const originalLocation = globalThis.location
    // simulate the worker: window exists (jsdom) but location is absent.
    Object.defineProperty(globalThis, 'location', {
      configurable: true,
      value: undefined,
    })
    try {
      makeZeroRecovery(deps).onClientStateNotFound()
      await flush()
    } finally {
      Object.defineProperty(globalThis, 'location', {
        configurable: true,
        value: originalLocation,
      })
    }
    expect(deleteLocalState).toHaveBeenCalledTimes(1)
    expect(recoverInPlace).toHaveBeenCalledTimes(1)
    expect(events).toContainEqual({
      type: 'recovering',
      reasonKey: 'client-state-not-found',
      reason: 'client state not found',
    })
  })

  test('scheduleReload defers the reload AND the store delete until performReload runs', async () => {
    const { deps, deleteLocalState, reload } = setup()
    let captured: ScheduleReloadContext | undefined
    deps.scheduleReload = (ctx) => {
      captured = ctx
    }
    makeZeroRecovery(deps).onClientStateNotFound()
    await flush()
    // deferred: nothing reloaded and the store is NOT yet deleted (so the app
    // isn't left running on a deleted store while the reload is gated).
    expect(reload).not.toHaveBeenCalled()
    expect(deleteLocalState).not.toHaveBeenCalled()
    expect(captured).toMatchObject({
      reasonKey: 'client-state-not-found',
      dropLocalState: true,
    })
    await captured!.performReload()
    await flush()
    expect(deleteLocalState).toHaveBeenCalledTimes(1)
    expect(reload).toHaveBeenCalledTimes(1)
  })

  test('scheduleReload that reloads directly skips the delete and beforeReload', async () => {
    const { deps, deleteLocalState, reload } = setup()
    const nativeReload = vi.fn()
    const beforeReload = vi.fn(async () => {})
    deps.beforeReload = beforeReload
    deps.scheduleReload = () => {
      nativeReload()
    }

    makeZeroRecovery(deps).onClientStateNotFound()
    await flush()

    expect(nativeReload).toHaveBeenCalledTimes(1)
    expect(deleteLocalState).not.toHaveBeenCalled()
    expect(beforeReload).not.toHaveBeenCalled()
    expect(reload).not.toHaveBeenCalled()
  })

  test('scheduleReload performReload is idempotent (one reload even if called twice)', async () => {
    const { deps, deleteLocalState, reload } = setup()
    let captured: ScheduleReloadContext | undefined
    deps.scheduleReload = (ctx) => {
      captured = ctx
    }
    makeZeroRecovery(deps).onClientStateNotFound()
    await flush()
    await captured!.performReload()
    await captured!.performReload()
    await flush()
    expect(deleteLocalState).toHaveBeenCalledTimes(1)
    expect(reload).toHaveBeenCalledTimes(1)
  })

  test('scheduleReload deferred PAST the latch timeout still deletes and reloads exactly once', async () => {
    vi.useFakeTimers()
    try {
      const { deps, deleteLocalState, reload } = setup()
      let captured: ScheduleReloadContext | undefined
      deps.scheduleReload = (ctx) => {
        captured = ctx
      }
      makeZeroRecovery(deps).onClientStateNotFound()
      expect(captured).toBeDefined()

      // the consumer holds performReload behind a gate for longer than the latch
      // timeout (soot's IDE gate can hold for minutes). the timeout re-opens
      // scheduling but must NOT drop the pending delete thunk.
      await vi.advanceTimersByTimeAsync(20_000)
      expect(deleteLocalState).not.toHaveBeenCalled()
      expect(reload).not.toHaveBeenCalled()

      // when the consumer finally commits, the store STILL gets dropped and the
      // reload fires exactly once — never a bare reload back onto the bad store.
      await captured!.performReload()
      expect(deleteLocalState).toHaveBeenCalledTimes(1)
      expect(reload).toHaveBeenCalledTimes(1)
    } finally {
      vi.useRealTimers()
    }
  })

  test('a slow beforeReload holds the latch through the timeout window (no double reload)', async () => {
    vi.useFakeTimers()
    try {
      const { deps, reload } = setup()
      let releaseBeforeReload: () => void = () => {}
      deps.beforeReload = () =>
        new Promise<void>((resolve) => {
          releaseBeforeReload = resolve
        })
      const recovery = makeZeroRecovery(deps)
      // default path: performReload runs immediately, disarms the latch, then
      // blocks on beforeReload (soot's waitForOriginReachable can exceed 15s).
      recovery.onClientStateNotFound()
      await vi.advanceTimersByTimeAsync(20_000)
      // a second recovery arrives while the first reload is still in-flight.
      recovery.onClientStateNotFound()
      await vi.advanceTimersByTimeAsync(0)
      expect(reload).not.toHaveBeenCalled()
      // releasing the first beforeReload reloads exactly once, never twice.
      releaseBeforeReload()
      await vi.advanceTimersByTimeAsync(0)
      expect(reload).toHaveBeenCalledTimes(1)
    } finally {
      vi.useRealTimers()
    }
  })

  test('injectable guardStorage gives cross-reload loop protection (Hermes has no sessionStorage)', async () => {
    const store = new Map<string, string>()
    const { deps, reload, events } = setup()
    deps.guardStorage = {
      getItem: (key) => store.get(key) ?? null,
      setItem: (key, value) => {
        store.set(key, value)
      },
    }
    const recovery = makeZeroRecovery(deps)
    recovery.onClientStateNotFound()
    await flush()
    expect(reload).toHaveBeenCalledTimes(1)
    // a reload wipes in-memory state but NOT the injected store.
    resetRecoveryStateForTests()
    recovery.onClientStateNotFound()
    await flush()
    // the injected store catches the immediate re-fire → fatal, no reload storm.
    expect(reload).toHaveBeenCalledTimes(1)
    expect(events.some((event) => event.type === 'fatal')).toBe(true)
  })

  test('a guardStorage that throws never crashes recovery (in-memory is the floor)', async () => {
    const { deps, reload } = setup()
    deps.guardStorage = {
      getItem: () => {
        throw new Error('no storage on this platform')
      },
      setItem: () => {
        throw new Error('no storage on this platform')
      },
    }
    makeZeroRecovery(deps).onClientStateNotFound()
    await flush()
    expect(reload).toHaveBeenCalledTimes(1)
  })

  test('classifier recognizes the mutation / connection desync class', () => {
    const cases: Array<[readonly unknown[], string]> = [
      [['sent mutation ID 5 but expected 4'], 'mutation-desync'],
      [['oooMutation detected'], 'mutation-desync'],
      [['Server reported an out-of-order mutation'], 'mutation-desync'],
      [['Ignoring mutation 3, already processed. Expected: 4'], 'mutation-desync'],
      [['InvalidConnectionRequestLastMutationID'], 'mutation-desync'],
      [['InvalidConnectionRequestBaseCookie'], 'connection-cookie-invalid'],
      [['ClientNotFound: client gone'], 'client-not-found'],
      [['connection userID mismatch'], 'connection-userid-mismatch'],
    ]
    for (const [args, reason] of cases) {
      expect(classifyZeroRecoveryLog('error', args)).toMatchObject({
        reasonKey: reason,
        dropLocalState: true,
      })
    }
    // ack timeouts use the typed mutation path; synthetic strings do not classify.
    expect(
      classifyZeroRecoveryLog('error', ['consecutive server-ack timeouts'])
    ).toBeUndefined()
    expect(
      classifyZeroRecoveryLog('error', ['Connection attempt timed out after 10 seconds'])
    ).toBeUndefined()
    expect(classifyZeroRecoveryLog('warn', ['ClientNotFound'])).toBeUndefined()
  })

  test('benignLogPatterns suppress recovery for matching classified logs', async () => {
    const { deps, deleteLocalState, reload } = setup()
    deps.benignLogPatterns = ['ClientNotFound']
    const sink = composeRecoveryLogSink(deps)
    sink.log('error', undefined, 'ClientNotFound: gone')
    await flush()
    expect(deleteLocalState).not.toHaveBeenCalled()
    expect(reload).not.toHaveBeenCalled()
    // a non-benign desync still recovers through the same sink.
    sink.log('error', undefined, 'sent mutation ID 5 but expected 4')
    await flush()
    expect(reload).toHaveBeenCalledTimes(1)
  })

  test('isRecoverableZeroStalePokeMessage matches only the stale-cookie signatures', () => {
    expect(
      isRecoverableZeroStalePokeMessage(
        'Server returned unexpected base cookie during sync'
      )
    ).toBe(true)
    expect(
      isRecoverableZeroStalePokeMessage(
        'Received cookie 5 is < than last snapshot cookie 9, ignoring client view'
      )
    ).toBe(true)
    expect(isRecoverableZeroStalePokeMessage('client state not found')).toBe(false)
  })
})

describe('group transitions', () => {
  function setupTransition(opts?: {
    group?: string
    identity?: string
    reload?: () => void | boolean
    remint?: () => Promise<boolean>
    connected?: Promise<boolean>
  }) {
    const events: ZeroEvent[] = []
    const zeroEvents = createEmitter<ZeroEvent | null>(
      `test-transition-${emitterSeq++}`,
      null
    )
    zeroEvents.listen((event) => {
      if (event) events.push(event)
    })
    const deleteLocalState = vi.fn(() => Promise.resolve())
    const reload = vi.fn(opts?.reload ?? (() => {}))
    const recoverInPlace = opts?.remint ? vi.fn(opts.remint) : undefined
    const awaitReconnected = opts?.connected ? vi.fn(() => opts.connected!) : undefined
    const deps: ZeroRecoveryDeps = {
      deleteLocalState,
      zeroEvents,
      reload,
      recoverInPlace,
      awaitReconnected,
      clientIdentity: opts?.identity,
      getGroupID: opts?.group === undefined ? undefined : () => opts.group!,
    }
    return { deps, deleteLocalState, reload, recoverInPlace, events }
  }

  function fatals(events: ZeroEvent[]): Extract<ZeroEvent, { type: 'fatal' }>[] {
    return events.filter(
      (event): event is Extract<ZeroEvent, { type: 'fatal' }> => event.type === 'fatal'
    )
  }

  function recoverings(events: ZeroEvent[]): ZeroEvent[] {
    return events.filter((event) => event.type === 'recovering')
  }

  test('concurrent NewClientGroup signals join one transition: one reload, joins silent, no fatal, no IDB delete', async () => {
    const a = setupTransition({ group: 'g-a' })
    const b = setupTransition({ group: 'g-b' })
    // both instances (and their recovery wrappers) exist before either fires,
    // exactly like two mounted providers meeting one registry change.
    const recoveryA = makeZeroRecovery(a.deps)
    const recoveryB = makeZeroRecovery(b.deps)
    recoveryA.onUpdateNeeded({ type: UpdateNeededReasonType.NewClientGroup })
    recoveryB.onUpdateNeeded({ type: UpdateNeededReasonType.NewClientGroup })
    await flush()
    // one transition: one reload, one recovering (the join stays silent).
    expect(a.reload.mock.calls.length + b.reload.mock.calls.length).toBe(1)
    expect(recoverings(a.events)).toHaveLength(1)
    expect(recoverings(b.events)).toHaveLength(0)
    expect(fatals([...a.events, ...b.events])).toEqual([])
    // NewClientGroup never deletes IndexedDB (sibling-tab safe).
    expect(a.deleteLocalState).not.toHaveBeenCalled()
    expect(b.deleteLocalState).not.toHaveBeenCalled()
    expect(isGroupTransitionOpen()).toBe(false)
  })

  test('a second change with a newer group starts a fresh transition after a reload', async () => {
    const a = setupTransition({ group: 'g1' })
    makeZeroRecovery(a.deps).onUpdateNeeded({
      type: UpdateNeededReasonType.NewClientGroup,
    })
    await flush()
    expect(a.reload).toHaveBeenCalledTimes(1)

    resetRecoveryStateForTests() // simulate the page reload clearing in-memory state
    const b = setupTransition({ group: 'g2' })
    makeZeroRecovery(b.deps).onUpdateNeeded({
      type: UpdateNeededReasonType.NewClientGroup,
    })
    await flush()
    // newer code verifiably landed: a fresh transition, not a fatal.
    expect(b.reload).toHaveBeenCalledTimes(1)
    expect(recoverings(b.events)).toHaveLength(1)
    expect(fatals(b.events)).toEqual([])
  })

  test('an unchanged group after a reload goes terminal once, then silent', async () => {
    const a = setupTransition({ group: 'g1' })
    makeZeroRecovery(a.deps).onUpdateNeeded({
      type: UpdateNeededReasonType.NewClientGroup,
    })
    await flush()
    expect(a.reload).toHaveBeenCalledTimes(1)

    resetRecoveryStateForTests()
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
    try {
      const b = setupTransition({ group: 'g1' })
      const recovery = makeZeroRecovery(b.deps)
      recovery.onUpdateNeeded({ type: UpdateNeededReasonType.NewClientGroup })
      await flush()
      // the reload did not change the group: one terminal error with the IDs.
      expect(b.reload).not.toHaveBeenCalled()
      expect(fatals(b.events)).toHaveLength(1)
      expect(fatals(b.events)[0]).toMatchObject({
        reasonKey: 'NewClientGroup',
        reason: expect.stringContaining('g1'),
      })
      expect(consoleError).toHaveBeenCalledTimes(1)

      // re-fires stay silent: never a loop, never a cascade.
      recovery.onUpdateNeeded({ type: UpdateNeededReasonType.NewClientGroup })
      const c = setupTransition({ group: 'g1' })
      makeZeroRecovery(c.deps).onUpdateNeeded({
        type: UpdateNeededReasonType.NewClientGroup,
      })
      await flush()
      expect(fatals([...b.events, ...c.events])).toHaveLength(1)
      expect(consoleError).toHaveBeenCalledTimes(1)
      expect(b.reload).not.toHaveBeenCalled()
    } finally {
      consoleError.mockRestore()
    }
  })

  test('a reminted instance re-firing with an unchanged group fails terminal once', async () => {
    let releaseConnected: (value: boolean) => void = () => {}
    const connected = new Promise<boolean>((resolve) => {
      releaseConnected = resolve
    })
    const a = setupTransition({
      group: 'g1',
      reload: () => false,
      remint: () => Promise.resolve(true),
      connected,
    })
    makeZeroRecovery(a.deps).onUpdateNeeded({
      type: UpdateNeededReasonType.SchemaVersionNotSupported,
    })
    await flush()
    expect(a.recoverInPlace).toHaveBeenCalledTimes(1)
    expect(isGroupTransitionOpen()).toBe(true)

    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
    try {
      // the replacement instance (built during the transition, same group
      // because the new code is genuinely incompatible) fires: terminal.
      const b = setupTransition({ group: 'g1' })
      const recovery = makeZeroRecovery(b.deps)
      recovery.onUpdateNeeded({
        type: UpdateNeededReasonType.SchemaVersionNotSupported,
      })
      await flush()
      expect(fatals(b.events)).toHaveLength(1)
      expect(fatals(b.events)[0]?.reason).toContain('g1')
      expect(consoleError).toHaveBeenCalledTimes(1)
      expect(a.recoverInPlace).toHaveBeenCalledTimes(1)
      expect(isGroupTransitionOpen()).toBe(false)

      // the failed transition releases its connected wait (no wedged latch)
      // and every later re-fire stays silent.
      releaseConnected(true)
      recovery.onUpdateNeeded({
        type: UpdateNeededReasonType.SchemaVersionNotSupported,
      })
      makeZeroRecovery(a.deps).onUpdateNeeded({
        type: UpdateNeededReasonType.SchemaVersionNotSupported,
      })
      await flush()
      expect(fatals([...a.events, ...b.events])).toHaveLength(1)
      expect(consoleError).toHaveBeenCalledTimes(1)
    } finally {
      consoleError.mockRestore()
    }
  })

  test('a reminted instance with a changed group supersedes into a fresh transition', async () => {
    const a = setupTransition({
      group: 'g1',
      reload: () => false,
      remint: () => Promise.resolve(true),
      connected: new Promise<boolean>(() => {}),
    })
    makeZeroRecovery(a.deps).onUpdateNeeded({
      type: UpdateNeededReasonType.NewClientGroup,
    })
    await flush()
    expect(isGroupTransitionOpen()).toBe(true)

    // an overlapping second edit: the replacement already runs newer code,
    // so this is a new legitimate transition, not a failure.
    const b = setupTransition({
      group: 'g2',
      reload: () => false,
      remint: () => Promise.resolve(true),
    })
    makeZeroRecovery(b.deps).onUpdateNeeded({
      type: UpdateNeededReasonType.NewClientGroup,
    })
    await flush()
    expect(recoverings(b.events)).toHaveLength(1)
    expect(fatals([...a.events, ...b.events])).toEqual([])
    expect(b.recoverInPlace).toHaveBeenCalledTimes(1)
  })

  test('unreadable groups allow one supersede, then go terminal', async () => {
    const a = setupTransition({
      reload: () => false,
      remint: () => Promise.resolve(true),
      connected: new Promise<boolean>(() => {}),
    })
    makeZeroRecovery(a.deps).onUpdateNeeded({
      type: UpdateNeededReasonType.NewClientGroup,
    })
    await flush()

    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
    try {
      // the superseding transition must stay open (blocked on its own
      // connected wait) so the next unknown signal is judged against it.
      const b = setupTransition({
        reload: () => false,
        remint: () => Promise.resolve(true),
        connected: new Promise<boolean>(() => {}),
      })
      makeZeroRecovery(b.deps).onUpdateNeeded({
        type: UpdateNeededReasonType.NewClientGroup,
      })
      await flush()
      // first unknown post-transition signal: allowed as a fresh transition.
      expect(recoverings(b.events)).toHaveLength(1)
      expect(fatals(b.events)).toEqual([])

      const c = setupTransition({ reload: () => false })
      makeZeroRecovery(c.deps).onUpdateNeeded({
        type: UpdateNeededReasonType.NewClientGroup,
      })
      await flush()
      // second one in the window: terminal once, then silent.
      expect(fatals(c.events)).toHaveLength(1)
      expect(consoleError).toHaveBeenCalledTimes(1)
      makeZeroRecovery(c.deps).onUpdateNeeded({
        type: UpdateNeededReasonType.NewClientGroup,
      })
      await flush()
      expect(fatals(c.events)).toHaveLength(1)
      expect(consoleError).toHaveBeenCalledTimes(1)
    } finally {
      consoleError.mockRestore()
    }
  })

  test('SchemaVersionNotSupported drops the store inside its transition', async () => {
    const a = setupTransition({
      group: 'g1',
      reload: () => false,
      remint: () => Promise.resolve(true),
    })
    makeZeroRecovery(a.deps).onUpdateNeeded({
      type: UpdateNeededReasonType.SchemaVersionNotSupported,
    })
    await flush()
    expect(a.deleteLocalState).toHaveBeenCalledTimes(1)
    expect(a.recoverInPlace).toHaveBeenCalledTimes(1)
    expect(fatals(a.events)).toEqual([])
  })

  test('different identities ride separate transitions but share the one reload', async () => {
    const a = setupTransition({ group: 'g-a', identity: 'user-a' })
    const b = setupTransition({ group: 'g-b', identity: 'user-b' })
    const recoveryA = makeZeroRecovery(a.deps)
    const recoveryB = makeZeroRecovery(b.deps)
    recoveryA.onUpdateNeeded({ type: UpdateNeededReasonType.NewClientGroup })
    recoveryB.onUpdateNeeded({ type: UpdateNeededReasonType.NewClientGroup })
    await flush()
    // one transition per shared identity, but still a single real reload.
    expect(recoverings(a.events)).toHaveLength(1)
    expect(recoverings(b.events)).toHaveLength(1)
    expect(a.reload.mock.calls.length + b.reload.mock.calls.length).toBe(1)
    expect(fatals([...a.events, ...b.events])).toEqual([])
  })

  test('a legacy bare-timestamp marker goes terminal once (legacy parity)', async () => {
    const key = `on-zero-recover-${globalThis.location?.href}-NewClientGroup`
    window.sessionStorage.setItem(key, String(Date.now()))
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
    try {
      const a = setupTransition({ group: 'g1' })
      const recovery = makeZeroRecovery(a.deps)
      recovery.onUpdateNeeded({ type: UpdateNeededReasonType.NewClientGroup })
      await flush()
      expect(a.reload).not.toHaveBeenCalled()
      expect(fatals(a.events)).toHaveLength(1)
      expect(consoleError).toHaveBeenCalledTimes(1)
      recovery.onUpdateNeeded({ type: UpdateNeededReasonType.NewClientGroup })
      await flush()
      expect(fatals(a.events)).toHaveLength(1)
      expect(consoleError).toHaveBeenCalledTimes(1)
    } finally {
      consoleError.mockRestore()
    }
  })

  test('a participant that cannot remint skips without failing the transition', async () => {
    const a = setupTransition({
      group: 'g1',
      reload: () => false,
      remint: () => Promise.resolve(false),
    })
    makeZeroRecovery(a.deps).onUpdateNeeded({
      type: UpdateNeededReasonType.NewClientGroup,
    })
    await flush()
    // false means "nothing to reconstruct" (unmounted): the transition
    // resolves, and a later signal starts a fresh one instead of fataling.
    expect(isGroupTransitionOpen()).toBe(false)
    makeZeroRecovery(a.deps).onUpdateNeeded({
      type: UpdateNeededReasonType.NewClientGroup,
    })
    await flush()
    expect(recoverings(a.events)).toHaveLength(2)
    expect(fatals(a.events)).toEqual([])
  })
})
