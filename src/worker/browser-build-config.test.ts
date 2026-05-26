import { describe, it, expect } from 'vitest'

import {
  getBrowserAliases,
  getBrowserDefine,
  getBrowserBuildConfig,
} from './browser-build-config.js'
import { getHeapStatistics } from './shims/node-stub.js'

describe('browser build config', () => {
  describe('getBrowserAliases', () => {
    it('returns an alias map', () => {
      const aliases = getBrowserAliases()
      expect(typeof aliases).toBe('object')
    })

    it('includes orez shims', () => {
      const aliases = getBrowserAliases()
      expect(aliases.postgres).toBe('orez/worker/shims/postgres-browser')
      expect(aliases['@rocicorp/zero-sqlite3']).toBe('orez/worker/shims/sqlite')
      expect(aliases.fastify).toBe('orez/worker/shims/fastify')
      expect(aliases.ws).toBe('orez/worker/shims/ws')
      expect(aliases.oxfmt).toBe('orez/worker/shims/oxfmt')
    })

    it('includes Node.js polyfills', () => {
      const aliases = getBrowserAliases()
      expect(aliases['node:events']).toBe('events')
      expect(aliases['node:stream']).toBe('orez/worker/shims/stream-browser')
      expect(aliases['node:path']).toBe('path-browserify')
      expect(aliases['node:os']).toBe('orez/worker/shims/node-stub')
    })

    it('includes Node.js stubs', () => {
      const aliases = getBrowserAliases()
      expect(aliases['node:fs']).toBe('orez/worker/shims/node-stub')
      expect(aliases['node:net']).toBe('orez/worker/shims/node-stub')
      expect(aliases['node:child_process']).toBe('orez/worker/shims/node-stub')
      expect(aliases['node:http']).toBe('orez/worker/shims/node-stub')
      expect(aliases['node:crypto']).toBe('orez/worker/shims/node-stub')
      expect(aliases['node:v8']).toBe('orez/worker/shims/node-stub')
    })
  })

  describe('getBrowserDefine', () => {
    it('returns define map', () => {
      const define = getBrowserDefine()
      expect(define['process.env.NODE_ENV']).toBe('"development"')
      expect(define['process.env.SINGLE_PROCESS']).toBe('"1"')
    })
  })

  describe('getBrowserBuildConfig', () => {
    it('returns combined config', () => {
      const config = getBrowserBuildConfig()
      expect(config.alias).toBeDefined()
      expect(config.define).toBeDefined()
      expect(config.format).toBe('esm')
      expect(config.platform).toBe('browser')
      expect(config.bundle).toBe(true)
    })
  })

  describe('node:v8 shim', () => {
    it('reports a positive worker heap budget', () => {
      const stats = getHeapStatistics()
      expect(stats.heap_size_limit).toBe(128 * 1024 * 1024)
      expect(stats.heap_size_limit - stats.used_heap_size).toBeGreaterThan(0)
    })
  })
})
