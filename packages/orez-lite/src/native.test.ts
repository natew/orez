import { describe, expect, it } from 'vitest'

import { createNativeHost } from './native.js'

const options = () => ({
  schema: { tables: {} },
  initSql: ['CREATE TABLE example (id TEXT PRIMARY KEY)'],
  dataDir: '.orez-native',
  port: 4848,
  adminTokenEnv: 'OREZ_ADMIN_TOKEN',
  callbacks: {
    authenticate: 'http://127.0.0.1:3000/auth',
    authorizeWake: 'http://127.0.0.1:3000/wake',
    transformQueries: 'http://127.0.0.1:3000/queries',
  },
})

describe('createNativeHost', () => {
  it('creates a reusable native host definition without starting a process', () => {
    const host = createNativeHost(options())
    expect(host).toEqual({ start: expect.any(Function) })
  })

  it('rejects invalid process configuration before startup', () => {
    expect(() => createNativeHost({ ...options(), port: 0 })).toThrow(TypeError)
    expect(() =>
      createNativeHost({
        ...options(),
        workerRetention: { idleMs: 30_000, sweepIntervalMs: 0 },
      })
    ).toThrow(TypeError)
  })
})
