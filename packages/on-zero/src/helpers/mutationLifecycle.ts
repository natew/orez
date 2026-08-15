import { globalValue } from './globalValue'

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

export type MutationLifecycle = {
  activate: () => void
  fence: () => void
  drainBackgroundMutations: () => Promise<void>
  enqueueBackgroundMutation: (
    label: string,
    create: () => unknown,
    options?: BackgroundMutationOptions
  ) => Promise<void>
  awaitMutationClient: (
    mutation: MutationLike,
    label: string,
    timeoutMs?: number
  ) => Promise<unknown>
  awaitMutationServer: (
    mutation: MutationLike,
    label: string,
    timeoutMs?: number
  ) => Promise<unknown>
  // the owning client calls these at its mutate boundary: assertWritable
  // refuses a queued write whose instance was replaced since it was queued,
  // claimMutation records who issued the mutation so acknowledgement finds it.
  assertWritable: () => void
  claimMutation: (result: unknown) => void
  // generation a queued write pins to, or null when this instance has no
  // published client (nothing to pin to, so a later write is not fenced)
  pinnedGeneration: () => number | null
  isCurrentGeneration: (generation: number) => boolean
}

type MutationOrigin = {
  lifecycle: MutationLifecycle
  generation: number
}

// every mutation a client's mutate facade issues is recorded with the instance
// that issued it and the generation that instance was on. acknowledgement
// follows this tag, so one instance recovering can never cancel another
// instance's in-flight mutation. globalValue keeps one map when a bundler
// dual-loads the package (cjs + esm copies).
const mutationOrigins = globalValue<WeakMap<object, MutationOrigin>>(
  'on-zero:mutation-origins',
  () => new WeakMap()
)

function mutationOrigin(mutation: unknown): MutationOrigin | undefined {
  return mutation && typeof mutation === 'object'
    ? mutationOrigins.get(mutation as object)
    : undefined
}

// generations pinned when a background mutation was queued. the queue installs
// this for the synchronous window in which create() calls zero.mutate, so the
// OWNING client refuses a write whose instance was replaced while the write sat
// in the queue, and every other instance keeps writing.
type MutationGuard = {
  generations: ReadonlyMap<MutationLifecycle, number>
  label: string
}

const guard = globalValue<{ current: MutationGuard | null }>(
  'on-zero:mutation-guard',
  () => ({ current: null })
)

function runGuarded<T>(next: MutationGuard, create: () => T): T {
  const previous = guard.current
  guard.current = next
  try {
    return create()
  } finally {
    guard.current = previous
  }
}

type BackgroundMutationQueue = {
  tail: Promise<void>
  lastErrorMessage: string | null
  coalesceSequence: Map<string, number>
  serverSettlements: Set<Promise<void>>
  serverErrors: unknown[]
}

type BackgroundMutationQueueControls = Pick<
  MutationLifecycle,
  'drainBackgroundMutations' | 'enqueueBackgroundMutation'
>

// one serial queue, however many instances it spans. each item pins the
// generation of every live instance when it is queued, then acknowledges
// through whichever instance actually issued the write.
function createBackgroundMutationQueue(instances: {
  all: () => readonly MutationLifecycle[]
  // settles work no instance claimed: a create() that returns a plain promise,
  // or a mutation issued by a client outside this set
  fallback: () => MutationLifecycle
}): BackgroundMutationQueueControls {
  const queue: BackgroundMutationQueue = {
    tail: Promise.resolve(),
    lastErrorMessage: null,
    coalesceSequence: new Map(),
    serverSettlements: new Set(),
    serverErrors: [],
  }

  function enqueueBackgroundMutation(
    label: string,
    create: () => unknown,
    mutationOptions: BackgroundMutationOptions = {}
  ) {
    const { coalesceKey = '', settle = 'client', timeoutMs = 120_000 } = mutationOptions
    const pinned = new Map<MutationLifecycle, number>()
    for (const lifecycle of instances.all()) {
      const generation = lifecycle.pinnedGeneration()
      if (generation !== null) pinned.set(lifecycle, generation)
    }
    const anyPinnedCurrent = () =>
      [...pinned].some(([lifecycle, generation]) =>
        lifecycle.isCurrentGeneration(generation)
      )
    const sequence = coalesceKey ? (queue.coalesceSequence.get(coalesceKey) ?? 0) + 1 : 0
    if (coalesceKey) queue.coalesceSequence.set(coalesceKey, sequence)

    const queued = queue.tail.then(async () => {
      if (coalesceKey) {
        if (queue.coalesceSequence.get(coalesceKey) !== sequence) return
        queue.coalesceSequence.delete(coalesceKey)
      }
      // every instance this write could have reached is gone
      if (!anyPinnedCurrent()) throw new StaleGenerationError(label)

      const result = await runGuarded({ generations: pinned, label }, create)
      if (result && typeof result === 'object' && 'client' in result) {
        const mutation = result as MutationLike
        // the helpers route to the issuing instance themselves; entering
        // through the fallback only decides where an UNTAGGED mutation settles
        const settleOn = instances.fallback()
        const serverSettlement = mutation.server
          ? settleOn.awaitMutationServer(mutation, label, timeoutMs)
          : null
        if (settle === 'server' && mutation.server) {
          await serverSettlement
        } else {
          if (serverSettlement) {
            const trackedServerSettlement = serverSettlement.then(
              () => {},
              (error: unknown) => {
                queue.serverErrors.push(error)
              }
            )
            queue.serverSettlements.add(trackedServerSettlement)
            void trackedServerSettlement.then(() => {
              queue.serverSettlements.delete(trackedServerSettlement)
            })
          }
          await settleOn.awaitMutationClient(mutation, label, timeoutMs)
        }
      }
      queue.lastErrorMessage = null
    })

    const result = queued.catch((error: unknown) => {
      if (isStaleGenerationError(error)) return
      if (!anyPinnedCurrent()) return
      const message = describeMutationError(error)
      // a write that keeps failing the same way logs once per queue, until a
      // queued write succeeds — a recovery in between does not re-arm it
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

  async function drainBackgroundMutations() {
    for (;;) {
      const tail = queue.tail
      await tail
      const pending = [...queue.serverSettlements]
      await Promise.all(pending)
      if (queue.serverErrors.length > 0) {
        const [error] = queue.serverErrors.splice(0)
        throw error
      }
      if (tail === queue.tail && queue.serverSettlements.size === 0) return
    }
  }

  return { drainBackgroundMutations, enqueueBackgroundMutation }
}

type GenerationWaiter = {
  reject: () => void
}

export function createMutationLifecycle(options: {
  ackTimeoutRecoveryThreshold: number
  recoverFromAckTimeout: (input: {
    label: string
    timeoutMs: number
    consecutiveTimeouts: number
  }) => void
}): MutationLifecycle {
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
  }

  function pinnedGeneration(): number | null {
    return active ? generation : null
  }

  function isCurrentGeneration(pinned: number): boolean {
    return active && pinned === generation
  }

  function assertWritable() {
    const pending = guard.current
    if (!pending) return
    const pinned = pending.generations.get(lifecycle)
    if (pinned === undefined) return
    if (!isCurrentGeneration(pinned)) throw stale(pending.label)
  }

  function claimMutation(result: unknown) {
    if (!result || typeof result !== 'object') return
    mutationOrigins.set(result as object, { lifecycle, generation })
  }

  // acknowledgement follows the mutation's own tag: the instance that issued
  // it, in the generation it was issued in. asked to settle another instance's
  // mutation, a client hands it to that instance rather than fencing it on a
  // generation that has nothing to do with it — so the per-client helpers are
  // correct in a multi-instance app too, and only an untagged mutation settles
  // wherever it was handed in.
  function foreignOwner(mutation: MutationLike): MutationLifecycle | null {
    const origin = mutationOrigin(mutation)
    return origin && origin.lifecycle !== lifecycle ? origin.lifecycle : null
  }

  // a client replaced between the call and the await can never settle that
  // call, so its waiter fails fast instead of running out the timeout.
  // ONLY VALID FOR A MUTATION THIS LIFECYCLE ISSUED (or an untagged one) —
  // instances count generations independently, so a foreign generation would
  // match or miss by coincidence. both callers run below their foreignOwner
  // check, which is what keeps that true.
  function issuedGeneration(mutation: MutationLike): number {
    return mutationOrigin(mutation)?.generation ?? generation
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
    const owner = foreignOwner(mutation)
    if (owner) return owner.awaitMutationClient(mutation, label, timeoutMs)
    return settleMutationClient(
      mutation,
      label,
      timeoutMs,
      issuedGeneration(mutation),
      true
    )
  }

  async function awaitMutationServer(
    mutation: MutationLike,
    label: string,
    timeoutMs = 30_000
  ): Promise<unknown> {
    const owner = foreignOwner(mutation)
    if (owner) return owner.awaitMutationServer(mutation, label, timeoutMs)
    const capturedGeneration = issuedGeneration(mutation)
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

  const backgroundQueue = createBackgroundMutationQueue({
    all: () => [lifecycle],
    fallback: () => lifecycle,
  })
  const lifecycle: MutationLifecycle = {
    activate,
    fence,
    ...backgroundQueue,
    awaitMutationClient,
    awaitMutationServer,
    assertWritable,
    claimMutation,
    pinnedGeneration,
    isCurrentGeneration,
  }
  return lifecycle
}

export type CombinedMutationLifecycle = Pick<
  MutationLifecycle,
  | 'awaitMutationClient'
  | 'awaitMutationServer'
  | 'drainBackgroundMutations'
  | 'enqueueBackgroundMutation'
>

// the mutation half of combineZeroClients: acknowledgement goes to the instance
// that issued the mutation, over ONE background queue whose serial order and
// coalescing span every instance. binding these to a single instance is what
// let a control-plane recovery cancel unrelated project mutations.
export function combineMutationLifecycles(
  lifecycles: readonly [MutationLifecycle, ...MutationLifecycle[]]
): CombinedMutationLifecycle {
  const primary = lifecycles[0]
  const backgroundQueue = createBackgroundMutationQueue({
    all: () => lifecycles,
    fallback: () => primary,
  })

  return {
    // every lifecycle already hands a mutation to the instance that issued it,
    // so entering through the primary only decides where a mutation from
    // outside these clients settles — the instance the facade sends all its
    // other unclaimed work to. the queue is what the combined form adds: one
    // serial tail and one coalescing map spanning every instance.
    awaitMutationClient: primary.awaitMutationClient,
    awaitMutationServer: primary.awaitMutationServer,
    ...backgroundQueue,
  }
}
