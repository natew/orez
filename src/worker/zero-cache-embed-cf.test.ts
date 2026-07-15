import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

type RunMode = 'ready' | 'reject' | 'timeout'

const harness = vi.hoisted(() => ({
  activeGenerations: 0,
  backendCloses: 0,
  backendOpens: 0,
  maxActiveGenerations: 0,
  modes: [] as RunMode[],
  proxyCloses: 0,
  proxyOpens: 0,
  rejectedError: new Error('runWorker failed before ready'),
  runCalls: 0,
}))

vi.mock('./zero-cache-run-worker.js', () => ({
  runWorker: (parent: {
    once(type: string, listener: () => void): void
    send(message: unknown): boolean
  }) => {
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
        complete()
      }

      parent.once('SIGTERM', () => finish(resolve))
      if (mode === 'ready') {
        queueMicrotask(() => parent.send(['ready']))
      } else if (mode === 'reject') {
        queueMicrotask(() => finish(() => reject(harness.rejectedError)))
      }
    })
  },
}))

vi.mock('../log.js', () => ({ setLogLevel: () => {} }))
vi.mock('../replication/handler.js', () => ({ resetReplicationState: () => {} }))
vi.mock('../pg-proxy-browser.js', () => ({
  createBrowserProxy: async () => {
    harness.proxyOpens++
    return {
      close() {
        harness.proxyCloses++
      },
    }
  },
}))
vi.mock('../pg-proxy-do-backend.js', () => ({
  DoBackend: class {
    readonly waitReady = Promise.resolve()

    constructor() {
      harness.backendOpens++
    }

    async close() {
      harness.backendCloses++
    }
  },
}))
vi.mock('./durable-object-websocket-handoff.js', () => ({
  DurableObjectWebSocketHandoff: class {
    readonly activeConnections = 0
  },
}))
vi.mock('./embed-generation.js', () => ({ sweepLeakedSqliteHandles: () => 0 }))
vi.mock('./local-sql-backend.js', () => ({
  createLocalSqlBackend: () => ({
    fetch: async () => new Response('ok'),
    recoverOrphanedTransactions: () => {},
  }),
}))
vi.mock('./shims/fastify.js', () => ({ resetFastifyRegistry: () => {} }))

import { startZeroCacheEmbedCF } from './zero-cache-embed-cf.js'

const embedGlobals = [
  '__orez_do_sqlite',
  '__orez_proxy_connect',
  '__orez_proxy_password',
  '__orez_proxy_user',
] as const

let originalEnv: NodeJS.ProcessEnv
let originalExit: typeof process.exit
let originalKill: typeof process.kill
let originalGlobals: Map<string, unknown>

function options(readyTimeout: number) {
  return {
    backendFetch: async () => new Response('ok'),
    doSqlite: {},
    readyTimeout,
  }
}

describe('startZeroCacheEmbedCF startup cleanup', () => {
  beforeEach(() => {
    harness.activeGenerations = 0
    harness.backendCloses = 0
    harness.backendOpens = 0
    harness.maxActiveGenerations = 0
    harness.modes = []
    harness.proxyCloses = 0
    harness.proxyOpens = 0
    harness.runCalls = 0

    originalEnv = { ...process.env }
    originalExit = process.exit
    originalKill = process.kill
    originalGlobals = new Map(
      embedGlobals.map((name) => [name, (globalThis as Record<string, unknown>)[name]])
    )
  })

  afterEach(() => {
    for (const key of Object.keys(process.env)) delete process.env[key]
    Object.assign(process.env, originalEnv)
    process.exit = originalExit
    process.kill = originalKill
    for (const name of embedGlobals) {
      const value = originalGlobals.get(name)
      if (value === undefined) delete (globalThis as Record<string, unknown>)[name]
      else (globalThis as Record<string, unknown>)[name] = value
    }
  })

  it('cleans worker resources while preserving a pre-ready runWorker error', async () => {
    harness.modes = ['reject']

    await expect(startZeroCacheEmbedCF(options(1_000))).rejects.toBe(
      harness.rejectedError
    )

    expect(harness.activeGenerations).toBe(0)
    expect(harness.proxyCloses).toBe(1)
    expect(harness.backendCloses).toBe(3)
    expect((globalThis as Record<string, unknown>).__orez_proxy_connect).toBeUndefined()
    expect(process.env.SINGLE_PROCESS).toBeUndefined()
  })

  it('stops a timed-out generation before a retry starts', async () => {
    harness.modes = ['timeout', 'ready']

    await expect(startZeroCacheEmbedCF(options(1))).rejects.toThrow(
      'timed out waiting for ready'
    )
    const retry = await startZeroCacheEmbedCF(options(1_000))

    expect(harness.runCalls).toBe(2)
    expect(harness.activeGenerations).toBe(1)
    expect(harness.maxActiveGenerations).toBe(1)
    expect(harness.proxyCloses).toBe(1)
    expect(harness.backendCloses).toBe(3)

    await Promise.all([retry.stop(), retry.stop()])

    expect(harness.activeGenerations).toBe(0)
    expect(harness.proxyOpens).toBe(2)
    expect(harness.proxyCloses).toBe(2)
    expect(harness.backendOpens).toBe(6)
    expect(harness.backendCloses).toBe(6)
  })
})
