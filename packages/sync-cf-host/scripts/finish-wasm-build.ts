import { Buffer } from 'node:buffer'
import { cp, mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

const packageDir = fileURLToPath(new URL('..', import.meta.url))
const generatedDir = join(packageDir, 'src/generated')
const bundlerDir = await mkdtemp(join(tmpdir(), 'orez-sync-cf-host-bundler-'))
const platformProbes = process.argv.includes('--platform-probes')

try {
  const build = Bun.spawn(
    [
      'wasm-pack',
      'build',
      '../../crates/sync-wasm',
      '--target',
      'bundler',
      '--release',
      '--out-dir',
      bundlerDir,
      '--out-name',
      'sync_wasm',
      ...(platformProbes ? ['--', '--features', 'platform-probes'] : []),
    ],
    { cwd: packageDir, stderr: 'inherit', stdout: 'inherit' }
  )
  const exitCode = await build.exited
  if (exitCode !== 0) throw new Error(`bundler Wasm build exited ${exitCode}`)

  const webModule = Buffer.from(
    await Bun.file(join(generatedDir, 'sync_wasm_bg.wasm')).arrayBuffer()
  )
  const bundlerModule = Buffer.from(
    await Bun.file(join(bundlerDir, 'sync_wasm_bg.wasm')).arrayBuffer()
  )
  if (!webModule.equals(bundlerModule)) {
    throw new Error('web and bundler Wasm builds produced different modules')
  }

  await cp(join(bundlerDir, 'sync_wasm_bg.js'), join(generatedDir, 'sync_wasm_bg.js'))
  await Bun.write(
    join(generatedDir, 'sync_wasm_bg.wasm.d.ts'),
    `declare const module: WebAssembly.Module
export default module
`
  )
} finally {
  await rm(bundlerDir, { force: true, recursive: true })
}
