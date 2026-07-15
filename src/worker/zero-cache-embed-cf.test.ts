import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

type RunMode =
  | 'delayed-stop'
  | 'exit-after-ready'
  | 'never-ready-never-stop'
  | 'never-stop'
  | 'ready'
  | 'reject'
  | 'sigquit-stop'
  | 'stop-reject'
  | 'timeout'

type Gate = {
  promise: Promise<void>
  reject(error: unknown): void
  resolve(): void
}

function gate(): Gate {
  let resolve!: () => void
  let reject!: (error: unknown) => void
  const promise = new Promise<void>((onResolve, onReject) => {
    resolve = onResolve
    reject = onReject
  })
  return { promise, reject, resolve }
}

const harness = vi.hoisted(() => ({
  activeGenerations: 0,
  backendCloses: 0,
  backendConstructorError: new Error('backend constructor failed'),
  backendConstructorRejectAt: -1,
  backendOpens: 0,
  backendWaitReadyError: new Error('backend waitReady failed'),
  backendWaitReadyGates: new Map<number, Gate>(),
  backendWaitReadyRejectAt: -1,
  envs: [] as Array<Record<string, string>>,
  events: [] as string[],
  maxActiveGenerations: 0,
  modes: [] as RunMode[],
  proxyCloseError: null as Error | null,
  proxyCloseGate: null as Gate | null,
  proxyCloses: 0,
  proxyCreateError: null as Error | null,
  proxyOpens: 0,
  rejectedError: new Error('runWorker failed before ready'),
  runCalls: 0,
  stopError: new Error('runWorker failed during termination'),
  sweepError: null as Error | null,
  workerReleases: [] as Array<() => void>,
}))

vi.mock('./zero-cache-run-worker.js', () => ({
  runWorker: (
    parent: {
      once(type: string, listener: () => void): void
      send(message: unknown): boolean
    },
    env: Record<string, string>
  ) => {
    harness.envs.push(env)
    const mode = harness.modes[harness.runCalls++]
    harness.activeGenerations++
    harness.maxActiveGenerations = Math.max(
      harness.maxActiveGenerations,
      harness.activeGenerations
    )

    return new Promise<void>((resolve, reject) => {
      let settled = false
      const finish = (complete: () => void) => {
        if (settled) return
        settled = true
        harness.activeGenerations--
        harness.events.push('worker-terminated')
        complete()
      }
      const release = () => finish(resolve)

      if (
        mode === 'delayed-stop' ||
        mode === 'exit-after-ready' ||
        mode === 'never-ready-never-stop' ||
        mode === 'never-stop' ||
        mode === 'sigquit-stop'
      ) {
        harness.workerReleases.push(release)
        parent.once('SIGTERM', () => {
          harness.events.push('sigterm')
          if (mode === 'exit-after-ready') release()
        })
        parent.once('SIGQUIT', () => {
          harness.events.push('sigquit')
          if (mode === 'sigquit-stop') release()
        })
      } else if (mode === 'stop-reject') {
        parent.once('SIGTERM', () => finish(() => reject(harness.stopError)))
      } else {
        parent.once('SIGTERM', () => finish(resolve))
      }

      if (
        mode === 'ready' ||
        mode === 'delayed-stop' ||
        mode === 'exit-after-ready' ||
        mode === 'never-stop' ||
        mode === 'sigquit-stop' ||
        mode === 'stop-reject'
      ) {
        queueMicrotask(() => {
          parent.send(['ready'])
        })
      } else if (mode === 'reject') {
        queueMicrotask(() => finish(() => reject(harness.rejectedError)))
      }
    })
  },
}))

vi.mock('../log.js', () => ({ setLogLevel: () => {} }))
vi.mock('../replication/handler.js', () => ({
  deleteReplicationState: () => {},
  resetReplicationState: () => {},
  signalReplicationChange: () => {},
}))
vi.mock('../pg-proxy-browser.js', () => ({
  createBrowserProxy: async () => {
    if (harness.proxyCreateError) throw harness.proxyCreateError
    harness.proxyOpens++
    return {
      async close() {
        harness.proxyCloses++
        harness.events.push('proxy-close')
        await harness.proxyCloseGate?.promise
        if (harness.proxyCloseError) throw harness.proxyCloseError
      },
      handleConnection() {},
    }
  },
}))
vi.mock('../pg-proxy-do-backend.js', () => ({
  releaseDoBackendInstanceCaches: () => {},
  DoBackend: class {
    readonly index: number
    readonly waitReady: Promise<void>

    constructor() {
      const index = harness.backendOpens
      if (index === harness.backendConstructorRejectAt) {
        throw harness.backendConstructorError
      }
      this.index = index
      harness.backendOpens++
      this.waitReady =
        harness.backendWaitReadyGates.get(index)?.promise ??
        (index === harness.backendWaitReadyRejectAt
          ? Promise.reject(harness.backendWaitReadyError)
          : Promise.resolve())
    }

    async close() {
      harness.backendCloses++
      harness.events.push(`backend-close-${this.index}`)
    }
  },
}))
vi.mock('./durable-object-websocket-handoff.js', () => ({
  DurableObjectWebSocketHandoff: class {
    readonly activeConnections = 0
    closeAll() {}
  },
}))
vi.mock('./embed-generation.js', () => ({
  sweepCFInstanceSqliteHandles: () => {
    if (harness.sweepError) throw harness.sweepError
    return 0
  },
}))
vi.mock('./local-sql-backend.js', () => ({
  createLocalSqlBackend: () => ({
    fetch: async () => new Response('ok'),
    recoverOrphanedTransactions: () => {},
  }),
}))
vi.mock('./cf-instance-runtime.js', async (importOriginal) => {
  const actual = await importOriginal<typeof import('./cf-instance-runtime.js')>()
  return {
    ...actual,
    dispatcherFastifyForCFInstance: () => ({ generation: harness.runCalls }),
  }
})

import { startZeroCacheEmbedCF } from './zero-cache-embed-cf.js'

const embedGlobals = [
  '__orez_do_sqlite',
  '__orez_fastify_instance',
  '__orez_fastify_instances',
  '__orez_proxy_connect',
  '__orez_proxy_password',
  '__orez_proxy_user',
] as const

type GlobalSnapshot = {
  hadValue: boolean
  value: unknown
}

let originalEnv: NodeJS.ProcessEnv
let originalExit: typeof process.exit
let originalFetch: typeof fetch
let originalGlobals: Map<string, GlobalSnapshot>
let originalKill: typeof process.kill

function options(readyTimeout = 1_000, instanceId = 'test-instance') {
  return {
    backendFetch: async () => new Response('ok'),
    doSqlite: {},
    instanceId,
    readyTimeout,
  }
}

async function turn(): Promise<void> {
  await Promise.resolve()
  await Promise.resolve()
}

describe('startZeroCacheEmbedCF lifecycle', () => {
  beforeEach(() => {
    harness.activeGenerations = 0
    harness.backendCloses = 0
    harness.backendConstructorRejectAt = -1
    harness.backendOpens = 0
    harness.backendWaitReadyGates = new Map()
    harness.backendWaitReadyRejectAt = -1
    harness.envs = []
    harness.events = []
    harness.maxActiveGenerations = 0
    harness.modes = []
    harness.proxyCloseError = null
    harness.proxyCloseGate = null
    harness.proxyCloses = 0
    harness.proxyCreateError = null
    harness.proxyOpens = 0
    harness.runCalls = 0
    harness.sweepError = null
    harness.workerReleases = []

    originalEnv = { ...process.env }
    originalExit = process.exit
    originalFetch = fetch
    originalKill = process.kill
    originalGlobals = new Map(
      embedGlobals.map((name) => [
        name,
        {
          hadValue: Object.prototype.hasOwnProperty.call(globalThis, name),
          value: (globalThis as Record<string, unknown>)[name],
        },
      ])
    )
  })

  afterEach(() => {
    vi.useRealTimers()
    for (const key of Object.keys(process.env)) delete process.env[key]
    Object.assign(process.env, originalEnv)
    process.exit = originalExit
    process.kill = originalKill
    globalThis.fetch = originalFetch
    for (const [name, snapshot] of originalGlobals) {
      if (snapshot.hadValue) {
        ;(globalThis as Record<string, unknown>)[name] = snapshot.value
      } else {
        delete (globalThis as Record<string, unknown>)[name]
      }
    }
  })

  it('cleans worker resources while preserving a pre-ready runWorker error', async () => {
    harness.modes = ['reject']

    await expect(startZeroCacheEmbedCF(options())).rejects.toBe(harness.rejectedError)

    expect(harness.activeGenerations).toBe(0)
    expect(harness.proxyCloses).toBe(1)
    expect(harness.backendCloses).toBe(3)
  })

  it('closes backends already constructed when a later constructor rejects', async () => {
    harness.backendConstructorRejectAt = 2

    await expect(startZeroCacheEmbedCF(options())).rejects.toBe(
      harness.backendConstructorError
    )

    expect(harness.backendOpens).toBe(2)
    expect(harness.backendCloses).toBe(2)
    expect(harness.proxyOpens).toBe(0)
  })

  it('closes every backend when waitReady rejects', async () => {
    harness.backendWaitReadyRejectAt = 1

    await expect(startZeroCacheEmbedCF(options())).rejects.toBe(
      harness.backendWaitReadyError
    )

    expect(harness.backendOpens).toBe(3)
    expect(harness.backendCloses).toBe(3)
    expect(harness.proxyOpens).toBe(0)
  })

  it('waits for every backend initializer to settle before cleanup completes', async () => {
    const delayedReady = gate()
    harness.backendWaitReadyRejectAt = 1
    harness.backendWaitReadyGates.set(2, delayedReady)

    const starting = startZeroCacheEmbedCF(options())
    await turn()
    expect(harness.backendCloses).toBe(0)

    delayedReady.resolve()
    await expect(starting).rejects.toBe(harness.backendWaitReadyError)
    expect(harness.backendCloses).toBe(3)
  })

  it('bounds remote transaction recovery from the entry deadline and aborts its fetch', async () => {
    vi.useFakeTimers()
    const logs: Array<Record<string, unknown>> = []
    let recoverySignal: AbortSignal | null = null
    const starting = startZeroCacheEmbedCF({
      ...options(1_000, 'recovery-timeout-instance'),
      backendFetch: (_input, init) =>
        new Promise<Response>((_resolve, reject) => {
          recoverySignal = init?.signal ?? null
          recoverySignal?.addEventListener(
            'abort',
            () => reject(recoverySignal?.reason),
            {
              once: true,
            }
          )
        }),
      log: (event) => logs.push(event),
    }).catch((error) => error)

    await vi.advanceTimersByTimeAsync(1_000)
    const error = await starting

    expect(error).toBeInstanceOf(Error)
    expect(String(error)).toContain('phase remote-transaction-recovery')
    expect(recoverySignal?.aborted).toBe(true)
    expect(harness.backendOpens).toBe(0)
    expect(logs).toContainEqual(
      expect.objectContaining({
        event: 'startup-phase-timeout',
        phase: 'remote-transaction-recovery',
      })
    )

    harness.modes = ['ready']
    const retry = await startZeroCacheEmbedCF(options(1_000, 'recovery-timeout-instance'))
    await retry.stop()
  })

  it('bounds backend initialization before the worker readiness timer would begin', async () => {
    vi.useFakeTimers()
    harness.backendWaitReadyGates.set(0, gate())
    const starting = startZeroCacheEmbedCF(
      options(1_000, 'backend-timeout-instance')
    ).catch((error) => error)

    await vi.advanceTimersByTimeAsync(1_000)
    const error = await starting

    expect(error).toBeInstanceOf(Error)
    expect(String(error)).toContain('phase backend-initialization')
    expect(harness.backendCloses).toBe(3)
    expect(harness.runCalls).toBe(0)

    harness.modes = ['ready']
    const retry = await startZeroCacheEmbedCF(options(1_000, 'backend-timeout-instance'))
    await retry.stop()
  })

  it('closes every backend when proxy setup rejects', async () => {
    harness.proxyCreateError = new Error('proxy setup failed')

    await expect(startZeroCacheEmbedCF(options())).rejects.toBe(harness.proxyCreateError)

    expect(harness.backendCloses).toBe(3)
    expect(harness.proxyCloses).toBe(0)
  })

  it('restores complete global and env state, including initially absent values', async () => {
    const globalRecord = globalThis as Record<string, unknown>
    const sentinels = new Map<string, unknown>()
    for (const name of embedGlobals) {
      const value = { name }
      sentinels.set(name, value)
      globalRecord[name] = value
    }
    for (const key of [
      'NODE_ENV',
      'SINGLE_PROCESS',
      'ZERO_ADMIN_PASSWORD',
      'ZERO_APP_ID',
      'ZERO_CVR_DB',
      'ZERO_REPLICA_FILE',
      'ZERO_UPSTREAM_DB',
      'OREZ_TEST_NEW',
    ]) {
      delete process.env[key]
    }
    process.env.OREZ_TEST_EXISTING = 'before'
    const expectedEnv = { ...process.env }
    delete (process as unknown as Record<string, unknown>).exit
    delete (process as unknown as Record<string, unknown>).kill
    const seededFetch = vi.fn(originalFetch)
    globalThis.fetch = seededFetch as typeof fetch

    harness.modes = ['ready']
    const embed = await startZeroCacheEmbedCF({
      ...options(),
      apiFetch: async () => new Response('api'),
      env: {
        OREZ_TEST_EXISTING: 'during',
        OREZ_TEST_NEW: 'during',
      },
    })
    await embed.stop()

    expect({ ...process.env }).toEqual(expectedEnv)
    expect(Object.prototype.hasOwnProperty.call(process, 'exit')).toBe(false)
    expect(Object.prototype.hasOwnProperty.call(process, 'kill')).toBe(false)
    expect(globalThis.fetch).toBe(seededFetch)
    for (const [name, value] of sentinels) expect(globalRecord[name]).toBe(value)
  })

  it('does not clobber values replaced by a co-resident owner', async () => {
    delete process.env.ZERO_APP_ID
    const previousUser = (globalThis as Record<string, unknown>).__orez_proxy_user
    harness.modes = ['ready']
    const embed = await startZeroCacheEmbedCF(options())

    ;(globalThis as Record<string, unknown>).__orez_proxy_user = 'newer-owner'
    process.env.ZERO_APP_ID = 'newer-owner'
    await embed.stop()

    expect((globalThis as Record<string, unknown>).__orez_proxy_user).toBe('newer-owner')
    expect(process.env.ZERO_APP_ID).toBe('newer-owner')
    ;(globalThis as Record<string, unknown>).__orez_proxy_user = previousUser
  })

  it('does not let additional env replace instance routing fields', async () => {
    harness.modes = ['ready']
    const embed = await startZeroCacheEmbedCF({
      ...options(1_000, 'protected-routes'),
      env: {
        ZERO_CHANGE_DB: 'postgres://wrong/change',
        ZERO_CVR_DB: 'postgres://wrong/cvr',
        ZERO_PORT: '1',
        ZERO_REPLICA_FILE: '/wrong/replica.db',
        ZERO_STORAGE_DB_TMP_DIR: '/wrong/tmp',
        ZERO_TASK_ID: 'wrong-task',
        ZERO_UPSTREAM_DB: 'postgres://wrong/upstream',
      },
    })

    const env = harness.envs[0]
    expect(env.ZERO_UPSTREAM_DB).toContain(
      '70726f7465637465642d726f75746573.orez-pg.local'
    )
    expect(env.ZERO_CVR_DB).toContain('70726f7465637465642d726f75746573.orez-pg.local')
    expect(env.ZERO_CHANGE_DB).toContain('70726f7465637465642d726f75746573.orez-pg.local')
    expect(env.ZERO_REPLICA_FILE).toContain(
      '/__orez_cf_instance__/70726f7465637465642d726f75746573/'
    )
    expect(env.ZERO_STORAGE_DB_TMP_DIR).toBe(
      '/__orez_cf_instance__/70726f7465637465642d726f75746573'
    )
    expect(env.ZERO_PORT).not.toBe('1')
    expect(env.ZERO_TASK_ID).toBe('orez-cf-70726f7465637465642d726f75746573')

    await embed.stop()
  })

  it('rejects API routes without an instance fetch handler before starting', async () => {
    await expect(
      startZeroCacheEmbedCF({
        ...options(1_000, 'missing-api-fetch'),
        env: { ZERO_MUTATE_URL: 'https://api.example/mutate' },
      })
    ).rejects.toThrow('apiFetch is required')

    expect(harness.runCalls).toBe(0)
    expect(harness.backendOpens).toBe(0)
    expect(harness.proxyOpens).toBe(0)
  })

  it('waits for delayed SIGTERM completion before closing proxy and backends', async () => {
    harness.modes = ['delayed-stop']
    const embed = await startZeroCacheEmbedCF(options())

    const stopping = embed.stop()
    await turn()
    expect(harness.proxyCloses).toBe(0)
    expect(harness.backendCloses).toBe(0)

    harness.workerReleases[0]()
    await stopping

    expect(harness.events.indexOf('worker-terminated')).toBeLessThan(
      harness.events.indexOf('proxy-close')
    )
    expect(harness.backendCloses).toBe(3)
  })

  it('waits for delayed proxy cleanup before closing root backends', async () => {
    harness.modes = ['ready']
    harness.proxyCloseGate = gate()
    const embed = await startZeroCacheEmbedCF(options())

    const stopping = embed.stop()
    await vi.waitFor(() => expect(harness.proxyCloses).toBe(1))
    expect(harness.backendCloses).toBe(0)

    harness.proxyCloseGate.resolve()
    await stopping
    expect(harness.backendCloses).toBe(3)
  })

  it('returns one shutdown promise for concurrent idempotent stop calls', async () => {
    harness.modes = ['ready']
    const embed = await startZeroCacheEmbedCF(options())

    const first = embed.stop()
    const second = embed.stop()
    expect(second).toBe(first)
    await Promise.all([first, second])

    expect(harness.proxyCloses).toBe(1)
    expect(harness.backendCloses).toBe(3)
  })

  it('surfaces a worker failure during termination after closing resources', async () => {
    harness.modes = ['stop-reject']
    const embed = await startZeroCacheEmbedCF(options())

    const error = await embed.stop().catch((reason) => reason)

    expect(error).toBeInstanceOf(AggregateError)
    expect((error as AggregateError).errors).toContain(harness.stopError)
    expect(harness.proxyCloses).toBe(1)
    expect(harness.backendCloses).toBe(3)
  })

  it('marks the embed unavailable and cleans up an unexpected worker exit', async () => {
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
    harness.modes = ['exit-after-ready']
    const embed = await startZeroCacheEmbedCF(options())

    harness.workerReleases[0]()
    await vi.waitFor(() => expect(harness.proxyCloses).toBe(1))

    expect(embed.ready).toBe(false)
    const error = await embed.stop().catch((reason) => reason)
    expect(error).toBeInstanceOf(AggregateError)
    expect(
      (error as AggregateError).errors.some((reason) =>
        String(reason).includes('runWorker exited after becoming ready')
      )
    ).toBe(true)
    consoleError.mockRestore()
  })

  it('finishes teardown before retrying a ready-timeout generation', async () => {
    harness.modes = ['timeout', 'ready']

    await expect(startZeroCacheEmbedCF(options(1))).rejects.toThrow(
      'phase worker-readiness'
    )
    const retry = await startZeroCacheEmbedCF(options())

    expect(harness.runCalls).toBe(2)
    expect(harness.activeGenerations).toBe(1)
    expect(harness.maxActiveGenerations).toBe(1)
    await retry.stop()
  })

  it('fails closed until a timed-out worker actually terminates', async () => {
    vi.useFakeTimers()
    harness.modes = ['never-stop', 'ready', 'ready']
    const embed = await startZeroCacheEmbedCF(options(1_000, 'wedged-instance'))

    const stopResult = embed.stop().catch((error) => error)
    await vi.advanceTimersByTimeAsync(10_000)
    const stopError = await stopResult

    expect(stopError).toBeInstanceOf(AggregateError)
    expect(String(stopError)).toContain('teardown failed')
    expect(
      (stopError as AggregateError).errors.some((error) =>
        String(error).includes('did not terminate after SIGTERM')
      )
    ).toBe(true)
    expect(process.env.SINGLE_PROCESS).toBe('1')
    await expect(
      startZeroCacheEmbedCF(options(1_000, 'wedged-instance'))
    ).rejects.toThrow('instance "wedged-instance" is active or still tearing down')

    harness.workerReleases[0]()
    await turn()
    expect(process.env.SINGLE_PROCESS).toBe(originalEnv.SINGLE_PROCESS)
    const retry = await startZeroCacheEmbedCF(options(1_000, 'wedged-instance'))
    await retry.stop()

    const other = await startZeroCacheEmbedCF(options(1_000, 'other-instance'))
    await other.stop()
    expect(harness.maxActiveGenerations).toBe(1)
  })

  it('force-stops a worker that ignores graceful termination before retrying', async () => {
    vi.useFakeTimers()
    harness.modes = ['sigquit-stop', 'ready']
    const embed = await startZeroCacheEmbedCF(options(1_000, 'force-stop-instance'))

    const stopping = embed.stop()
    await vi.advanceTimersByTimeAsync(5_000)
    await stopping

    expect(harness.events).toContain('sigterm')
    expect(harness.events).toContain('sigquit')
    expect(harness.activeGenerations).toBe(0)

    const retry = await startZeroCacheEmbedCF(options(1_000, 'force-stop-instance'))
    await retry.stop()
    expect(harness.maxActiveGenerations).toBe(1)
  })

  it('stops an abandoned logical instance before starting its replacement', async () => {
    harness.modes = ['ready', 'ready']
    const abandoned = await startZeroCacheEmbedCF(options(1_000, 'reset-instance'))

    const replacement = await startZeroCacheEmbedCF(options(1_000, 'reset-instance'))

    expect(abandoned.ready).toBe(false)
    expect(harness.events).toContain('worker-terminated')
    expect(harness.activeGenerations).toBe(1)
    expect(harness.maxActiveGenerations).toBe(1)
    await replacement.stop()
  })

  it('bounds replacement cleanup and remains fail-closed until it finishes', async () => {
    vi.useFakeTimers()
    harness.modes = ['ready', 'ready']
    harness.proxyCloseGate = gate()
    const abandoned = await startZeroCacheEmbedCF(
      options(1_000, 'replacement-timeout-instance')
    )

    const replacing = startZeroCacheEmbedCF(
      options(1_000, 'replacement-timeout-instance')
    ).catch((error) => error)
    await vi.advanceTimersByTimeAsync(1_000)
    const replacementError = await replacing

    expect(replacementError).toBeInstanceOf(Error)
    expect(String(replacementError)).toContain('phase replacement-stop')
    expect(abandoned.ready).toBe(false)
    const stillReplacing = startZeroCacheEmbedCF(
      options(1_000, 'replacement-timeout-instance')
    ).catch((error) => error)
    await vi.advanceTimersByTimeAsync(1_000)
    expect(String(await stillReplacing)).toContain('phase replacement-stop')

    harness.proxyCloseGate.resolve()
    await turn()
    harness.proxyCloseGate = null
    const replacement = await startZeroCacheEmbedCF(
      options(1_000, 'replacement-timeout-instance')
    )
    await replacement.stop()
  })

  it('releases a startup-timeout claim after the worker eventually terminates', async () => {
    vi.useFakeTimers()
    harness.modes = ['never-ready-never-stop', 'ready']
    const starting = startZeroCacheEmbedCF(
      options(1_000, 'startup-timeout-instance')
    ).catch((error) => error)

    await vi.advanceTimersByTimeAsync(11_000)
    const startupError = await starting

    expect(startupError).toBeInstanceOf(AggregateError)
    expect(String(startupError)).toContain('startup failed and teardown also failed')
    await expect(
      startZeroCacheEmbedCF(options(1_000, 'startup-timeout-instance'))
    ).rejects.toThrow(
      'instance "startup-timeout-instance" is active or still tearing down'
    )

    harness.workerReleases[0]()
    await turn()
    const retry = await startZeroCacheEmbedCF(options(1_000, 'startup-timeout-instance'))
    await retry.stop()
    expect(harness.maxActiveGenerations).toBe(1)
  })

  it('fails closed for one logical instance after SQLite teardown fails', async () => {
    harness.modes = ['ready', 'ready']
    const embed = await startZeroCacheEmbedCF(options(1_000, 'sqlite-close-failed'))
    harness.sweepError = new Error('sqlite close failed')

    const error = await embed.stop().catch((reason) => reason)

    expect(error).toBeInstanceOf(AggregateError)
    expect(String(error)).toContain('teardown failed')
    await expect(
      startZeroCacheEmbedCF(options(1_000, 'sqlite-close-failed'))
    ).rejects.toThrow('instance "sqlite-close-failed" is active or still tearing down')

    harness.sweepError = null
    const other = await startZeroCacheEmbedCF(options(1_000, 'sqlite-close-other'))
    await other.stop()
  })

  it('preserves the startup error when resource cleanup also rejects', async () => {
    harness.modes = ['reject']
    harness.proxyCloseError = new Error('proxy cleanup failed')

    const error = await startZeroCacheEmbedCF(options()).catch((reason) => reason)

    expect(error).toBeInstanceOf(AggregateError)
    expect((error as AggregateError).cause).toBe(harness.rejectedError)
    expect((error as AggregateError).errors[0]).toBe(harness.rejectedError)
    expect((error as AggregateError).errors[1]).toBeInstanceOf(AggregateError)
  })

  it('bounds startup cleanup when a resource close never settles', async () => {
    vi.useFakeTimers()
    harness.modes = ['reject', 'ready']
    harness.proxyCloseGate = gate()
    const starting = startZeroCacheEmbedCF(
      options(60_000, 'cleanup-timeout-instance')
    ).catch((error) => error)

    await vi.waitFor(() => expect(harness.proxyCloses).toBe(1))
    await vi.advanceTimersByTimeAsync(15_000)
    const error = await starting

    expect(error).toBeInstanceOf(AggregateError)
    expect(String((error as AggregateError).errors[1])).toContain(
      'startup cleanup timed out after 15000ms'
    )

    harness.proxyCloseGate.resolve()
    await turn()
    harness.proxyCloseGate = null
    const retry = await startZeroCacheEmbedCF(options(1_000, 'cleanup-timeout-instance'))
    await retry.stop()
  })
})
