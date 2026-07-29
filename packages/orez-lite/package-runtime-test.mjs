import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import {
  mkdirSync,
  mkdtempSync,
  readdirSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'

const temporary = mkdtempSync(join(tmpdir(), 'orez-lite-pack-'))
const repositoryRoot = resolve(import.meta.dirname, '..', '..')

function linkDependency(nodeModules, name, target) {
  const path = join(nodeModules, name)
  mkdirSync(resolve(path, '..'), { recursive: true })
  symlinkSync(target, path, 'dir')
}

try {
  execFileSync('bun', ['pm', 'pack', '--destination', temporary, '--quiet'], {
    cwd: import.meta.dirname,
    stdio: 'pipe',
  })
  const tarballName = readdirSync(temporary).find((name) => name.endsWith('.tgz'))
  if (!tarballName) throw new Error('orez-lite pack produced no tarball')

  const consumer = join(temporary, 'consumer')
  const nodeModules = join(consumer, 'node_modules')
  mkdirSync(nodeModules, { recursive: true })
  execFileSync('tar', ['-xzf', join(temporary, tarballName), '-C', nodeModules])
  symlinkSync(join(nodeModules, 'package'), join(nodeModules, 'orez-lite'), 'dir')

  linkDependency(
    nodeModules,
    'orez-sync-cf-host',
    join(repositoryRoot, 'packages', 'sync-cf-host')
  )
  linkDependency(
    nodeModules,
    'orez-sync-executor',
    join(repositoryRoot, 'packages', 'sync-executor')
  )
  linkDependency(
    nodeModules,
    '@rocicorp/zero',
    join(repositoryRoot, 'node_modules', '@rocicorp', 'zero')
  )
  linkDependency(nodeModules, 'vite', join(repositoryRoot, 'node_modules', 'vite'))

  writeFileSync(
    join(consumer, 'entry.mjs'),
    `import { createQueryCompiler } from 'orez-lite/cloudflare/query-compiler'
const compile = createQueryCompiler({
  tables: {
    account: {
      name: 'account',
      serverName: 'accounts',
      columns: { id: { type: 'string' } },
      primaryKey: ['id'],
    },
  },
})
export const compiledSql = compile(
  { table: 'account' },
  { singular: false, relationships: {} },
).root.sql
`
  )
  writeFileSync(
    join(consumer, 'build.mjs'),
    `import { join } from 'node:path'
import { pathToFileURL } from 'node:url'
import { orezSyncCfHostWasm } from 'orez-lite/cloudflare/vite-wasm-loader'
import { build } from 'vite'

await build({
  configFile: false,
  logLevel: 'silent',
  plugins: [
    ...orezSyncCfHostWasm(),
    {
      name: 'consumer-ssr-config',
      config() {
        return { ssr: { noExternal: ['consumer-package'] } }
      },
    },
  ],
  root: import.meta.dirname,
  build: {
    emptyOutDir: true,
    outDir: join(import.meta.dirname, 'dist'),
    rollupOptions: { output: { entryFileNames: 'entry.mjs' } },
    ssr: join(import.meta.dirname, 'entry.mjs'),
  },
})

const built = await import(pathToFileURL(join(import.meta.dirname, 'dist', 'entry.mjs')))
if (typeof built.compiledSql !== 'string' || built.compiledSql.length === 0) {
  throw new Error('packed Vite SSR compiler failed')
}
`
  )

  execFileSync('node', ['build.mjs'], { cwd: consumer, stdio: 'pipe' })
  const native = await import(
    new URL('./node_modules/orez-lite/dist/native.js', `file://${consumer}/`)
  )
  assert.equal(typeof native.createNativeHost, 'function')
  const local = await import('orez-lite/local')
  assert.equal(typeof local.defineLocalConfig, 'function')
  assert.equal(typeof local.loadLocalConfig, 'function')
  assert.equal(typeof local.startLocalSyncHost, 'function')
  const vite = await import('orez-lite/vite')
  const plugin = vite.orez()
  assert.equal(plugin.name, 'orez-lite')
  assert.equal(plugin.apply, 'serve')
  execFileSync(
    'node',
    [
      '--import=orez-lite/cloudflare/node-wasm-loader',
      '--input-type=module',
      '-e',
      `const compiler = await import('orez-lite/cloudflare/query-compiler')
const compile = compiler.createQueryCompiler({
  tables: {
    account: {
      name: 'account',
      serverName: 'accounts',
      columns: { id: { type: 'string' } },
      primaryKey: ['id'],
    },
  },
})
const result = compile(
  { table: 'account' },
  { singular: false, relationships: {} },
)
if (typeof result.root.sql !== 'string' || result.root.sql.length === 0) {
  throw new Error('packed Node compiler failed')
}`,
    ],
    { cwd: consumer, stdio: 'pipe' }
  )
  assert.equal(readdirSync(join(consumer, 'dist')).length, 1)
} finally {
  rmSync(temporary, { force: true, recursive: true })
}
