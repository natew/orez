import { describe, expect, it } from 'vitest'

import { getConfig } from './config.js'

describe('getConfig', () => {
  it('preserves port 0 as auto-allocate', () => {
    const config = getConfig({ pgPort: 0, zeroPort: 0, adminPort: 0 })

    expect(config.pgPort).toBe(0)
    expect(config.zeroPort).toBe(0)
    expect(config.adminPort).toBe(0)
  })

  it('uses a bounded onDbReady timeout that callers can extend', () => {
    expect(getConfig().onDbReadyTimeoutMs).toBe(30_000)
    expect(getConfig({ onDbReadyTimeoutMs: 90_000 }).onDbReadyTimeoutMs).toBe(90_000)
  })
})
