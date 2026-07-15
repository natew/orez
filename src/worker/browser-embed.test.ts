import { afterEach, describe, expect, it, vi } from 'vitest'

const harness = vi.hoisted(() => ({
  envs: [] as Array<Record<string, string>>,
  proxyCloses: 0,
  workerStops: 0,
}))

vi.mock('./zero-cache-run-worker.js', () => ({
  runWorker(
    parent: {
      once(event: string, listener: () => void): void
      send(message: unknown): boolean
    },
    env: Record<string, string>
  ) {
    harness.envs.push(env)
    queueMicrotask(() => parent.send(['ready']))
    return new Promise<void>((resolve) => {
      parent.once('SIGTERM', () => {
        harness.workerStops++
        resolve()
      })
    })
  },
}))

vi.mock('../pg-proxy-browser.js', () => ({
  createBrowserProxy: async () => ({
    async close() {
      harness.proxyCloses++
    },
    handleConnection() {},
  }),
}))

vi.mock('../replication/handler.js', () => ({
  deleteReplicationState: () => {},
  resetReplicationState: () => {},
}))

vi.mock('./embed-generation.js', () => ({
  sweepCFInstanceSqliteHandles: () => 0,
}))

vi.mock('./cf-instance-runtime.js', async (importOriginal) => {
  const actual = await importOriginal<typeof import('./cf-instance-runtime.js')>()
  return {
    ...actual,
    dispatcherFastifyForCFInstance: (instanceId: string) => ({
      inject: async () => ({
        body: instanceId,
        headers: { 'content-type': 'text/plain' },
        statusCode: 200,
      }),
      server: { emit: () => true },
    }),
  }
})

import { startZeroCacheEmbedBrowser } from './browser-embed.js'
import { requireCFInstanceRuntime } from './cf-instance-runtime.js'

describe('startZeroCacheEmbedBrowser routing', () => {
  afterEach(() => {
    harness.envs = []
    harness.proxyCloses = 0
    harness.workerStops = 0
  })

  it('runs co-resident browser embeds with explicit isolated routes', async () => {
    const [alpha, bravo] = await Promise.all([
      startZeroCacheEmbedBrowser({
        env: {
          ZERO_PORT: '1',
          ZERO_REPLICA_FILE: '/wrong/alpha.db',
          ZERO_UPSTREAM_DB: 'postgres://wrong/alpha',
        },
        instanceId: 'browser-alpha',
        pglite: {} as never,
      }),
      startZeroCacheEmbedBrowser({
        instanceId: 'browser-bravo',
        pglite: {} as never,
      }),
    ])

    expect(alpha.ready).toBe(true)
    expect(bravo.ready).toBe(true)
    expect(harness.envs).toHaveLength(2)

    const alphaEnv = harness.envs.find(
      (env) => env.ZERO_TASK_ID === 'orez-browser-62726f777365722d616c706861'
    )!
    const bravoEnv = harness.envs.find(
      (env) => env.ZERO_TASK_ID === 'orez-browser-62726f777365722d627261766f'
    )!
    expect(alphaEnv.ZERO_UPSTREAM_DB).toContain(
      '62726f777365722d616c706861.orez-pg.local'
    )
    expect(bravoEnv.ZERO_UPSTREAM_DB).toContain(
      '62726f777365722d627261766f.orez-pg.local'
    )
    expect(alphaEnv.ZERO_REPLICA_FILE).not.toBe(bravoEnv.ZERO_REPLICA_FILE)
    expect(alphaEnv.ZERO_PORT).not.toBe(bravoEnv.ZERO_PORT)

    await expect(alpha.handleHttp({ method: 'GET', url: '/sync' })).resolves.toEqual({
      body: 'browser-alpha',
      headers: { 'content-type': 'text/plain' },
      status: 200,
    })
    expect(requireCFInstanceRuntime('browser-alpha').instanceId).toBe('browser-alpha')
    expect(requireCFInstanceRuntime('browser-bravo').instanceId).toBe('browser-bravo')

    await Promise.all([alpha.stop(), bravo.stop()])

    expect(harness.workerStops).toBe(2)
    expect(harness.proxyCloses).toBe(2)
    expect(() => requireCFInstanceRuntime('browser-alpha')).toThrow('no active runtime')
    expect(() => requireCFInstanceRuntime('browser-bravo')).toThrow('no active runtime')
  })
})
