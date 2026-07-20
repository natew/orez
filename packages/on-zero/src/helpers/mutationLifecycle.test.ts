import { beforeEach, describe, expect, test, vi } from 'vitest'

import {
  createMutationLifecycle,
  MutationTimeoutError,
  StaleGenerationError,
} from './mutationLifecycle'

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  let reject!: (error: unknown) => void
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise
    reject = rejectPromise
  })
  return { promise, reject, resolve }
}

function setup(threshold = 2) {
  const recoverFromAckTimeout = vi.fn()
  const lifecycle = createMutationLifecycle({
    ackTimeoutRecoveryThreshold: threshold,
    recoverFromAckTimeout,
  })
  lifecycle.activate()
  return { lifecycle, recoverFromAckTimeout }
}

beforeEach(() => {
  vi.restoreAllMocks()
})

describe('mutation lifecycle', () => {
  test('extracts typed mutation errors and times out each settlement phase', async () => {
    vi.useFakeTimers()
    try {
      const { lifecycle } = setup()
      await expect(
        lifecycle.awaitMutationClient(
          { client: Promise.resolve({ type: 'error', error: { message: 'denied' } }) },
          'save note',
        ),
      ).rejects.toMatchObject({
        name: 'MutationResultError',
        label: 'save note',
        phase: 'client',
      })

      const waiting = lifecycle.awaitMutationClient(
        { client: new Promise(() => {}) },
        'stuck note',
        20,
      )
      const timedOut = expect(waiting).rejects.toMatchObject({
        name: 'MutationTimeoutError',
        phase: 'client',
        timeoutMs: 20,
      })
      await vi.advanceTimersByTimeAsync(20)
      await timedOut
    } finally {
      vi.useRealTimers()
    }
  })

  test('serializes work and skips superseded coalesced writes', async () => {
    const { lifecycle } = setup()
    const blocker = deferred<void>()
    const first = lifecycle.enqueueBackgroundMutation('blocker', () => blocker.promise)
    const superseded = vi.fn()
    const latest = vi.fn(async () => {})
    const oldWrite = lifecycle.enqueueBackgroundMutation('old row', superseded, {
      coalesceKey: 'message:1',
    })
    const newWrite = lifecycle.enqueueBackgroundMutation('new row', latest, {
      coalesceKey: 'message:1',
    })

    blocker.resolve()
    await Promise.all([first, oldWrite, newWrite])
    expect(superseded).not.toHaveBeenCalled()
    expect(latest).toHaveBeenCalledOnce()
  })

  test('one failed background mutation does not poison later work', async () => {
    const { lifecycle } = setup()
    vi.spyOn(console, 'warn').mockImplementation(() => {})
    const denied = lifecycle.enqueueBackgroundMutation('stamp', async () => {
      throw new Error('PermissionError: stale task')
    })
    await expect(denied).rejects.toThrow('PermissionError')

    const next = vi.fn(async () => {})
    await lifecycle.enqueueBackgroundMutation('clear stamp', next)
    expect(next).toHaveBeenCalledOnce()
  })

  test('recovery or close quietly drops in-flight and queued background work', async () => {
    const { lifecycle } = setup()
    const client = deferred<unknown>()
    const queuedCreate = vi.fn(async () => {})
    const inFlight = lifecycle.enqueueBackgroundMutation('in flight', () => ({
      client: client.promise,
    }))
    const queued = lifecycle.enqueueBackgroundMutation('queued', queuedCreate)

    lifecycle.fence()
    await expect(inFlight).resolves.toBeUndefined()
    await expect(queued).resolves.toBeUndefined()
    expect(queuedCreate).not.toHaveBeenCalled()

    const direct = lifecycle.awaitMutationClient(
      { client: Promise.resolve({}) },
      'direct after close',
    )
    await expect(direct).rejects.toBeInstanceOf(StaleGenerationError)
  })

  test('recovers only after consecutive server acknowledgement timeouts', async () => {
    vi.useFakeTimers()
    try {
      const { lifecycle, recoverFromAckTimeout } = setup()
      const timeout = async (label: string) => {
        const result = lifecycle.awaitMutationServer(
          { client: Promise.resolve({}), server: new Promise(() => {}) },
          label,
          10,
        )
        const timedOut = expect(result).rejects.toMatchObject({
          name: 'MutationTimeoutError',
          phase: 'server',
        })
        await vi.advanceTimersByTimeAsync(10)
        await timedOut
      }

      await timeout('first')
      expect(recoverFromAckTimeout).not.toHaveBeenCalled()
      await timeout('second')
      expect(recoverFromAckTimeout).toHaveBeenCalledOnce()
      expect(recoverFromAckTimeout).toHaveBeenCalledWith({
        label: 'second',
        timeoutMs: 10,
        consecutiveTimeouts: 2,
      })
    } finally {
      vi.useRealTimers()
    }
  })

  test('a server response resets the consecutive timeout count', async () => {
    vi.useFakeTimers()
    try {
      const { lifecycle, recoverFromAckTimeout } = setup()
      const timeout = async () => {
        const result = lifecycle.awaitMutationServer(
          { client: Promise.resolve({}), server: new Promise(() => {}) },
          'write',
          10,
        )
        const timedOut = expect(result).rejects.toBeInstanceOf(MutationTimeoutError)
        await vi.advanceTimersByTimeAsync(10)
        await timedOut
      }

      await timeout()
      await expect(
        lifecycle.awaitMutationServer(
          { client: Promise.resolve({}), server: Promise.resolve({}) },
          'acked write',
          10,
        ),
      ).resolves.toEqual({})
      await timeout()
      expect(recoverFromAckTimeout).not.toHaveBeenCalled()
    } finally {
      vi.useRealTimers()
    }
  })
})
