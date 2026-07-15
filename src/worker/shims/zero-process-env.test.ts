import { afterEach, describe, expect, it, vi } from 'vitest'

const originalNodeEnv = process.env.NODE_ENV
const originalSingleProcess = process.env.SINGLE_PROCESS

afterEach(() => {
  if (originalNodeEnv === undefined) delete process.env.NODE_ENV
  else process.env.NODE_ENV = originalNodeEnv
  if (originalSingleProcess === undefined) delete process.env.SINGLE_PROCESS
  else process.env.SINGLE_PROCESS = originalSingleProcess
})

describe('acquireZeroProcessEnv', () => {
  it('restores overwritten bootstrap environment values', async () => {
    process.env.NODE_ENV = 'production-sentinel'
    process.env.SINGLE_PROCESS = 'disabled-sentinel'
    vi.resetModules()

    const { acquireZeroProcessEnv } = await import('./zero-process-env.js')
    expect(process.env.NODE_ENV).toBe('production-sentinel')
    expect(process.env.SINGLE_PROCESS).toBe('1')

    const release = acquireZeroProcessEnv()
    release()

    expect(process.env.NODE_ENV).toBe('production-sentinel')
    expect(process.env.SINGLE_PROCESS).toBe('disabled-sentinel')
  })

  it('keeps the process environment until every lease is released', async () => {
    process.env.NODE_ENV = 'production-sentinel'
    process.env.SINGLE_PROCESS = 'disabled-sentinel'
    vi.resetModules()

    const { acquireZeroProcessEnv } = await import('./zero-process-env.js')
    const releaseFirst = acquireZeroProcessEnv()
    const releaseSecond = acquireZeroProcessEnv()

    releaseFirst()
    expect(process.env.NODE_ENV).toBe('production-sentinel')
    expect(process.env.SINGLE_PROCESS).toBe('1')

    releaseSecond()
    expect(process.env.NODE_ENV).toBe('production-sentinel')
    expect(process.env.SINGLE_PROCESS).toBe('disabled-sentinel')
  })
})
