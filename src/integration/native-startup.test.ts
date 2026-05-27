import { existsSync, rmSync } from 'node:fs'
import { resolve } from 'node:path'

import { afterAll, beforeAll, describe, expect, test } from 'vitest'

import { startZeroLite } from '../index.js'

describe('native sqlite startup integration', { timeout: 120_000 }, () => {
  let shutdown: (() => Promise<void>) | undefined
  let zeroPort = 0
  let dataDir = ''

  beforeAll(async () => {
    const basePort = 29000 + Math.floor(Math.random() * 1000)
    dataDir = `.orez-native-startup-test-${Date.now()}`

    const started = await startZeroLite({
      pgPort: basePort,
      zeroPort: basePort + 100,
      dataDir,
      logLevel: 'warn',
      skipZeroCache: false,
      disableWasmSqlite: true,
    })

    shutdown = started.stop
    zeroPort = started.zeroPort
  }, 60_000)

  afterAll(async () => {
    if (shutdown) await shutdown()
    if (dataDir) {
      try {
        rmSync(dataDir, { recursive: true, force: true })
      } catch {
        // ignore cleanup failures in test teardown
      }
    }
  })

  test('zero-cache responds in native mode', async () => {
    const response = await fetch(`http://127.0.0.1:${zeroPort}/`)
    expect([200, 404]).toContain(response.status)
  })

  test('creates the replica file before sync workers open it', () => {
    expect(existsSync(resolve(dataDir, 'zero-replica.db'))).toBe(true)
  })
})
