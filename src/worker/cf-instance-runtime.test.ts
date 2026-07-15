import { describe, expect, it, vi } from 'vitest'

import {
  apiURLForCFInstance,
  fetchCFInstanceAPI,
  logCFInstance,
  postgresURLForCFInstance,
  registerCFInstanceFastify,
  registerCFInstanceRuntime,
  releaseCFInstanceRuntime,
  routeCFInstanceFastifyURL,
  routeCFPostgresHost,
  routeCFPostgresURL,
  routeCFSqlitePath,
  setCFInstanceRuntimeAbandon,
  setCFInstanceRuntimeStop,
  sqlitePathForCFInstance,
  stopCFInstanceRuntimeForReplacement,
} from './cf-instance-runtime.js'

function runtimeInput(instanceId: string, apiFetch?: typeof fetch) {
  return {
    apiFetch,
    doSqlite: { instanceId },
    env: {},
    instanceId,
    pgPassword: `${instanceId}-password`,
    pgUser: `${instanceId}-user`,
  }
}

describe('CF instance runtime routing', () => {
  it('requires a non-empty logical Durable Object identity', () => {
    expect(() =>
      registerCFInstanceRuntime({
        ...runtimeInput(''),
        instanceId: '',
      })
    ).toThrow('instanceId is required')
  })

  it('normalizes identity once and isolates routing from diagnostic failures', () => {
    const runtime = registerCFInstanceRuntime({
      ...runtimeInput(' alpha '),
      log: () => {
        throw new Error('diagnostic sink failed')
      },
    })
    expect(runtime.instanceId).toBe('alpha')
    expect(() => registerCFInstanceRuntime(runtimeInput('alpha'))).toThrow(
      'instance "alpha" is active or still tearing down'
    )
    expect(() => releaseCFInstanceRuntime(runtime)).not.toThrow()
  })

  it('rejects a duplicate identity while allowing co-resident identities', () => {
    const alpha = registerCFInstanceRuntime(runtimeInput('alpha'))
    const bravo = registerCFInstanceRuntime(runtimeInput('bravo'))
    try {
      expect(alpha.basePort).not.toBe(bravo.basePort)
      expect(() => registerCFInstanceRuntime(runtimeInput('alpha'))).toThrow(
        'instance "alpha" is active or still tearing down'
      )
    } finally {
      releaseCFInstanceRuntime(alpha)
      releaseCFInstanceRuntime(bravo)
    }
  })

  it('routes sqlite, postgres, HTTP, Fastify, and logging by exact identity', async () => {
    const requests: string[] = []
    const logs: Array<Record<string, unknown>> = []
    const apiFetch = vi.fn(async (request: Request) => {
      requests.push(request.url)
      return Response.json({ ok: true })
    }) as typeof fetch
    const alpha = registerCFInstanceRuntime({
      ...runtimeInput('alpha', apiFetch),
      log: (event) => logs.push(event),
    })
    const bravo = registerCFInstanceRuntime(runtimeInput('bravo'))
    try {
      const sqlitePath = sqlitePathForCFInstance('alpha')
      expect(routeCFSqlitePath(sqlitePath).runtime).toBe(alpha)
      expect(routeCFSqlitePath(`${sqlitePath}?orezRole=replica-writer`)).toEqual({
        role: 'replica-writer',
        runtime: alpha,
      })

      const postgresURL = postgresURLForCFInstance('alpha', 'zero_cvr', alpha.pgUser)
      expect(routeCFPostgresURL(postgresURL)).toBe(alpha)
      expect(routeCFPostgresHost(new URL(postgresURL).hostname)).toBe(alpha)

      const dispatcher = { instanceId: 'alpha' }
      registerCFInstanceFastify(alpha.basePort, dispatcher)
      expect(
        routeCFInstanceFastifyURL(`ws://localhost:${alpha.basePort}/sync/v1/connect`)
      ).toEqual({ instance: dispatcher, runtime: alpha })
      expect(() =>
        routeCFInstanceFastifyURL(`ws://localhost:${bravo.basePort}/sync/v1/connect`)
      ).toThrow(`no Fastify runtime for port ${bravo.basePort}`)

      const originalAPIURL = 'https://api.example/mutate?client=alpha'
      const apiURL = apiURLForCFInstance('alpha', originalAPIURL)
      await expect(fetchCFInstanceAPI(apiURL)).resolves.toMatchObject({ ok: true })
      expect(requests).toEqual([originalAPIURL])

      const otherOrigin = 'https://query.example/pull'
      const otherAPIURL = apiURLForCFInstance('alpha', otherOrigin)
      expect(new URL(otherAPIURL).hostname).not.toBe(new URL(apiURL).hostname)
      await fetchCFInstanceAPI(otherAPIURL)
      expect(requests).toEqual([originalAPIURL, otherOrigin])

      logCFInstance(alpha, { event: 'ready' })
      expect(logs).toContainEqual({ instanceId: 'alpha', event: 'ready' })
    } finally {
      releaseCFInstanceRuntime(alpha)
      releaseCFInstanceRuntime(bravo)
    }
  })

  it('removes every route when the matching runtime is released', () => {
    const runtime = registerCFInstanceRuntime(runtimeInput('released'))
    const sqlitePath = sqlitePathForCFInstance(runtime.instanceId)
    const postgresURL = postgresURLForCFInstance(
      runtime.instanceId,
      'postgres',
      runtime.pgUser
    )
    releaseCFInstanceRuntime(runtime)

    expect(() => routeCFSqlitePath(sqlitePath)).toThrow('no active runtime')
    expect(() => routeCFPostgresURL(postgresURL)).toThrow('no active runtime')
  })

  it('joins a replacement in the same Durable Object incarnation', async () => {
    const runtime = registerCFInstanceRuntime(runtimeInput('same-incarnation'))
    const stop = vi.fn(async () => releaseCFInstanceRuntime(runtime))
    const abandon = vi.fn()
    setCFInstanceRuntimeStop(runtime, stop)
    setCFInstanceRuntimeAbandon(runtime, abandon)

    await expect(
      stopCFInstanceRuntimeForReplacement(runtime.instanceId, runtime.doSqlite)
    ).resolves.toBe('stopped')
    expect(stop).toHaveBeenCalledOnce()
    expect(abandon).not.toHaveBeenCalled()
  })

  it('abandons a runtime retained from a reset Durable Object', async () => {
    const runtime = registerCFInstanceRuntime(runtimeInput('reset-incarnation'))
    const stop = vi.fn(async () => {})
    const abandon = vi.fn()
    setCFInstanceRuntimeStop(runtime, stop)
    setCFInstanceRuntimeAbandon(runtime, abandon)

    await expect(
      stopCFInstanceRuntimeForReplacement(runtime.instanceId, {
        replacement: true,
      })
    ).resolves.toBe('abandoned')
    expect(abandon).toHaveBeenCalledOnce()
    expect(stop).not.toHaveBeenCalled()
    const replacement = registerCFInstanceRuntime(runtimeInput('reset-incarnation'))
    releaseCFInstanceRuntime(replacement)
  })
})
