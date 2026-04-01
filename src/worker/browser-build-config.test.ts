import { describe, it, expect } from 'vitest'

import {
  getBrowserAliases,
  getBrowserDefine,
  getBrowserBuildConfig,
} from './browser-build-config.js'

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
    })

    it('includes Node.js polyfills', () => {
      const aliases = getBrowserAliases()
      expect(aliases['node:events']).toBe('events')
      expect(aliases['node:stream']).toBe('orez/worker/shims/stream-browser')
      expect(aliases['node:path']).toBe('path-browserify')
    })

    it('includes Node.js stubs', () => {
      const aliases = getBrowserAliases()
      expect(aliases['node:fs']).toBe('orez/worker/shims/node-stub')
      expect(aliases['node:net']).toBe('orez/worker/shims/node-stub')
      expect(aliases['node:child_process']).toBe('orez/worker/shims/node-stub')
      expect(aliases['node:http']).toBe('orez/worker/shims/node-stub')
      expect(aliases['node:crypto']).toBe('orez/worker/shims/node-stub')
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
})
