import { readFileSync } from 'node:fs'

import { describe, expect, it } from 'vitest'

describe('pg-sqlite-compiler entrypoints', () => {
  it('keeps parser initialization out of the Cloudflare backend module graph', () => {
    const backend = readFileSync(
      new URL('../pg-proxy-do-backend.ts', import.meta.url),
      'utf8'
    )
    const compiler = readFileSync(new URL('./compiler.ts', import.meta.url), 'utf8')

    expect(backend).toContain("from './pg-sqlite-compiler/compiler.js'")
    expect(backend).not.toContain("from './pg-sqlite-compiler/index.js'")
    expect(compiler).not.toContain('loadModule')
    expect(compiler).not.toMatch(/^await /m)
  })
})
