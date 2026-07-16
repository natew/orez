import assert from 'node:assert/strict'
import { readFile, readdir } from 'node:fs/promises'
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'

import { orezSyncCfHostWasm } from 'orez-sync-cf-host/vite-wasm-loader'
import { build, createServer } from 'vite'

const server = await createServer({
  appType: 'custom',
  configFile: false,
  plugins: [orezSyncCfHostWasm()],
  root: import.meta.dirname,
  server: { middlewareMode: true },
})

try {
  const module = await server.ssrLoadModule('orez-sync-cf-host/query-compiler')
  assert.equal(typeof module.createQueryCompiler, 'function')
} finally {
  await server.close()
}

const outDir = join(import.meta.dirname, '.wrangler', 'vite-query-compiler')
await build({
  configFile: false,
  logLevel: 'silent',
  plugins: [orezSyncCfHostWasm()],
  root: import.meta.dirname,
  build: {
    emptyOutDir: true,
    outDir,
    rollupOptions: { output: { entryFileNames: 'entry.mjs' } },
    ssr: join(import.meta.dirname, 'vite-query-compiler-entry.mjs'),
  },
})

const output = await readFile(join(outDir, 'entry.mjs'), 'utf8')
assert.match(output, /new WebAssembly\.Module/)
assert.doesNotMatch(output, /readFile/)

const built = await import(
  `${pathToFileURL(join(outDir, 'entry.mjs')).href}?${Date.now()}`
)
assert.match(built.compiledSql, /FROM "accounts"/)
assert.deepEqual(await readdir(outDir), ['entry.mjs'])
