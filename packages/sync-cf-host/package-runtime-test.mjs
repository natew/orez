import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { existsSync, mkdirSync, mkdtempSync, readdirSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

const temporary = mkdtempSync(join(tmpdir(), 'orez-sync-cf-host-pack-'))

try {
  execFileSync('bun', ['pm', 'pack', '--destination', temporary, '--quiet'], {
    cwd: import.meta.dirname,
    stdio: 'pipe',
  })
  const tarball = join(
    temporary,
    readdirSync(temporary).find((name) => name.endsWith('.tgz'))
  )
  const packedPackage = JSON.parse(
    execFileSync('tar', ['-xOf', tarball, 'package/package.json'], {
      encoding: 'utf8',
    })
  )

  const targets = []
  const collectTargets = (value) => {
    if (typeof value === 'string') targets.push(value)
    else for (const nested of Object.values(value)) collectTargets(nested)
  }
  collectTargets(packedPackage.exports)

  assert.ok(targets.every((target) => target.startsWith('./dist/')))
  assert.ok(
    targets.every((target) => !target.endsWith('.ts') || target.endsWith('.d.ts'))
  )
  assert.equal(
    packedPackage.exports['./wasm-module.wasm'].import,
    './dist/generated/sync_wasm_bg.wasm'
  )

  const listing = execFileSync('tar', ['-tzf', tarball], { encoding: 'utf8' })
  assert.match(listing, /package\/dist\/generated\/sync_wasm_bg\.wasm\n/)
  assert.doesNotMatch(listing, /package\/src\//)

  const extracted = join(temporary, 'extracted')
  mkdirSync(extracted)
  execFileSync('tar', ['-xzf', tarball, '-C', extracted])
  const packageRoot = join(extracted, 'package')
  execFileSync(
    'node',
    [
      '--input-type=module',
      '-e',
      `import { existsSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
const expected = new Map([
  ['orez-sync-cf-host', '/dist/index.js'],
  ['orez-sync-cf-host/post-commit', '/dist/post-commit.js'],
  ['orez-sync-cf-host/mutation-error', '/dist/mutation-error.js'],
  ['orez-sync-cf-host/node-wasm-loader', '/dist/node-wasm-loader.js'],
  ['orez-sync-cf-host/bun-wasm-loader', '/dist/bun-wasm-loader.js'],
  ['orez-sync-cf-host/query-compiler', '/dist/query-compiler.js'],
  ['orez-sync-cf-host/transaction-query', '/dist/transaction-query.js'],
  ['orez-sync-cf-host/vite-wasm-loader', '/dist/vite-wasm-loader.js'],
])
for (const [specifier, suffix] of expected) {
  const resolved = import.meta.resolve(specifier)
  if (!resolved.endsWith(suffix)) throw new Error(specifier + ' resolved to ' + resolved)
}
const wasm = import.meta.resolve('orez-sync-cf-host/wasm-module.wasm')
if (!wasm.endsWith('/dist/generated/sync_wasm_bg.wasm')) throw new Error(wasm)
if (!existsSync(fileURLToPath(wasm))) throw new Error('wasm export is missing')
const transaction = await import('orez-sync-cf-host/transaction-query')
if (typeof transaction.executeTransactionQueryPlan !== 'function') {
  throw new Error('built transaction-query export did not load')
}
const viteLoader = await import('orez-sync-cf-host/vite-wasm-loader')
if (typeof viteLoader.orezSyncCfHostWasm !== 'function') {
  throw new Error('built Vite loader export did not load')
}`,
    ],
    { cwd: packageRoot, stdio: 'pipe' }
  )
  assert.ok(existsSync(join(packageRoot, 'dist', 'index.d.ts')))
  execFileSync(
    'node',
    [
      '--import=orez-sync-cf-host/node-wasm-loader',
      '--input-type=module',
      '-e',
      `const compiler = await import('orez-sync-cf-host/query-compiler')
if (typeof compiler.createQueryCompiler !== 'function') {
  throw new Error('query compiler did not load through the Node Wasm loader')
}`,
    ],
    { cwd: packageRoot, stdio: 'pipe' }
  )
} finally {
  rmSync(temporary, { force: true, recursive: true })
}
