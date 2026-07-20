export type MutationLike = {
  client: Promise<unknown>
  server?: Promise<unknown>
}

export type BackgroundMutationOptions = {
  coalesceKey?: string
  settle?: 'client' | 'server'
  timeoutMs?: number
}

export type MutationPhase = 'client' | 'server'

export class StaleGenerationError extends Error {
  readonly label: string

  constructor(label: string) {
    super(`${label} stopped because its Zero instance was replaced or recovered`)
    this.name = 'StaleGenerationError'
    this.label = label
  }
}

export class MutationTimeoutError extends Error {
  readonly label: string
  readonly phase: MutationPhase
  readonly timeoutMs: number

  constructor(label: string, phase: MutationPhase, timeoutMs: number) {
    super(
      phase === 'server'
        ? `${label} server acknowledgement did not complete within ${timeoutMs}ms`
        : `${label} did not complete within ${timeoutMs}ms`
    )
    this.name = 'MutationTimeoutError'
    this.label = label
    this.phase = phase
    this.timeoutMs = timeoutMs
  }
}

export class MutationResultError extends Error {
  readonly label: string
  readonly phase: MutationPhase
  readonly result: unknown

  constructor(label: string, phase: MutationPhase, result: unknown, message: string) {
    super(`${label} failed${phase === 'server' ? ' on server' : ''}: ${message}`)
    this.name = 'MutationResultError'
    this.label = label
    this.phase = phase
    this.result = result
  }
}

export function isStaleGenerationError(error: unknown): error is StaleGenerationError {
  return error instanceof StaleGenerationError
}

export function mutationErrorMessage(result: unknown): string | null {
  if (!result || typeof result !== 'object') return null
  const typed = result as { type?: unknown; error?: unknown }
  if (typed.type !== 'error') return null
  return describeMutationError(typed.error)
}

function describeMutationError(error: unknown): string {
  if (error instanceof Error) return error.message
  if (error && typeof error === 'object') {
    const typed = error as { type?: unknown; message?: unknown }
    const parts = [typed.type, typed.message].filter(
      (part): part is string => typeof part === 'string' && part.length > 0
    )
    if (parts.length) return parts.join(': ')
  }
  return String(error)
}

type GenerationWaiter = {
  reject: () => void
}

type BackgroundMutationQueue = {
  tail: Promise<void>
  lastErrorMessage: string | null
  coalesceSequence: Map<string, number>
}

export type MutationLifecycle = ReturnType<typeof createMutationLifecycle>

export function createMutationLifecycle(options: {
  ackTimeoutRecoveryThreshold: number
  recoverFromAckTimeout: (input: {
    label: string
    timeoutMs: number
    consecutiveTimeouts: number
  }) => void
}) {
  const queue: BackgroundMutationQueue = {
    tail: Promise.resolve(),
    lastErrorMessage: null,
    coalesceSequence: new Map(),
  }
  const waiters = new Set<GenerationWaiter>()
  let generation = 0
  let active = false
  let consecutiveServerAckTimeouts = 0

  function stale(label: string): StaleGenerationError {
    return new StaleGenerationError(label)
  }

  function fence() {
    if (!active) return
    active = false
    generation += 1
    consecutiveServerAckTimeouts = 0
    queue.coalesceSequence.clear()
    for (const waiter of [...waiters]) {
      waiters.delete(waiter)
      waiter.reject()
    }
  }

  function activate() {
    if (active) return
    generation += 1
    active = true
    consecutiveServerAckTimeouts = 0
    queue.lastErrorMessage = null
  }

  function observeGeneration(capturedGeneration: number, label: string) {
    let waiter: GenerationWaiter | undefined
    const promise = new Promise<never>((_, reject) => {
      if (!active || capturedGeneration !== generation) {
        reject(stale(label))
        return
      }
      waiter = {
        reject: () => reject(stale(label)),
      }
      waiters.add(waiter)
      if (!active || capturedGeneration !== generation) {
        waiters.delete(waiter)
        waiter = undefined
        reject(stale(label))
      }
    })
    return {
      promise,
      dispose() {
        if (waiter) waiters.delete(waiter)
      },
    }
  }

  async function awaitInGeneration<T>(input: {
    promise: Promise<T>
    label: string
    phase: MutationPhase
    timeoutMs: number
    generation: number
  }): Promise<T> {
    if (!active || input.generation !== generation) throw stale(input.label)
    let timer: ReturnType<typeof setTimeout> | undefined
    const generationChange = observeGeneration(input.generation, input.label)
    try {
      return await Promise.race([
        input.promise,
        generationChange.promise,
        new Promise<never>((_, reject) => {
          timer = setTimeout(
            () =>
              reject(new MutationTimeoutError(input.label, input.phase, input.timeoutMs)),
            input.timeoutMs
          )
        }),
      ])
    } finally {
      generationChange.dispose()
      if (timer) clearTimeout(timer)
    }
  }

  async function settleMutationClient(
    mutation: MutationLike,
    label: string,
    timeoutMs: number,
    capturedGeneration: number,
    observeServerFailure: boolean
  ): Promise<unknown> {
    const result = await awaitInGeneration({
      promise: mutation.client,
      label,
      phase: 'client',
      timeoutMs,
      generation: capturedGeneration,
    })
    const message = mutationErrorMessage(result)
    if (message) throw new MutationResultError(label, 'client', result, message)
    if (observeServerFailure) {
      mutation.server?.catch((error: unknown) => {
        if (!active || capturedGeneration !== generation) return
        console.warn(`[on-zero] ${label} server replication failed:`, error)
      })
    }
    return result
  }

  function awaitMutationClient(
    mutation: MutationLike,
    label: string,
    timeoutMs = 30_000
  ): Promise<unknown> {
    return settleMutationClient(mutation, label, timeoutMs, generation, true)
  }

  async function awaitMutationServer(
    mutation: MutationLike,
    label: string,
    timeoutMs = 30_000
  ): Promise<unknown> {
    const capturedGeneration = generation
    const clientResult = await settleMutationClient(
      mutation,
      label,
      timeoutMs,
      capturedGeneration,
      false
    )
    if (!mutation.server) return clientResult

    try {
      const serverResult = await awaitInGeneration({
        promise: mutation.server,
        label,
        phase: 'server',
        timeoutMs,
        generation: capturedGeneration,
      })
      consecutiveServerAckTimeouts = 0
      const message = mutationErrorMessage(serverResult)
      if (message) throw new MutationResultError(label, 'server', serverResult, message)
      return serverResult
    } catch (error) {
      if (error instanceof MutationTimeoutError && error.phase === 'server') {
        consecutiveServerAckTimeouts += 1
        if (consecutiveServerAckTimeouts >= options.ackTimeoutRecoveryThreshold) {
          options.recoverFromAckTimeout({
            label,
            timeoutMs,
            consecutiveTimeouts: consecutiveServerAckTimeouts,
          })
        }
      }
      throw error
    }
  }

  function enqueueBackgroundMutation(
    label: string,
    create: () => unknown,
    mutationOptions: BackgroundMutationOptions = {}
  ): Promise<void> {
    const capturedGeneration = generation
    const { coalesceKey = '', settle = 'client', timeoutMs = 120_000 } = mutationOptions
    const sequence = coalesceKey ? (queue.coalesceSequence.get(coalesceKey) ?? 0) + 1 : 0
    if (coalesceKey) queue.coalesceSequence.set(coalesceKey, sequence)

    const queued = queue.tail.then(async () => {
      if (!active || capturedGeneration !== generation) throw stale(label)
      if (coalesceKey) {
        if (queue.coalesceSequence.get(coalesceKey) !== sequence) return
        queue.coalesceSequence.delete(coalesceKey)
      }
      const result = await create()
      if (result && typeof result === 'object' && 'client' in result) {
        const mutation = result as MutationLike
        if (settle === 'server' && mutation.server) {
          await awaitMutationServer(mutation, label, timeoutMs)
        } else {
          await settleMutationClient(mutation, label, timeoutMs, capturedGeneration, true)
        }
      } else {
        await result
      }
      queue.lastErrorMessage = null
    })

    const result = queued.catch((error: unknown) => {
      if (isStaleGenerationError(error)) return
      if (!active || capturedGeneration !== generation) return
      const message = describeMutationError(error)
      const alreadyLogged = queue.lastErrorMessage === message
      queue.lastErrorMessage = message
      if (!alreadyLogged) {
        console.warn(`[on-zero] ${label} background mutation failed:`, error)
      }
      throw error
    })
    queue.tail = result.catch(() => {})
    return result
  }

  return {
    activate,
    fence,
    enqueueBackgroundMutation,
    awaitMutationClient,
    awaitMutationServer,
  }
}
