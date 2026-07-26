import { spawnSync } from 'node:child_process'
import {
  mkdtempSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { afterEach, describe, expect, it } from 'vitest'

import { bundleCloudflareRustSyncAppWorker } from './bundle.js'
import { cfDeployConfig } from './config.js'
import { buildMigrationModuleSource } from './migration.js'
import { buildAppShimSource, buildRustSyncUserShimSource } from './shims.js'

const workerDirs: string[] = []

afterEach(() => {
  for (const dir of workerDirs.splice(0)) rmSync(dir, { recursive: true, force: true })
})

describe('bundleCloudflareRustSyncAppWorker', () => {
  it('bundles the generated static app shim without a lazy app chunk', async () => {
    const workerDir = mkdtempSync(join(tmpdir(), 'orez-app-shim-split-'))
    workerDirs.push(workerDir)
    const orezDir = join(workerDir, 'node_modules', 'orez')
    mkdirSync(join(orezDir, 'dist', 'worker'), { recursive: true })
    writeFileSync(
      join(orezDir, 'package.json'),
      JSON.stringify({
        name: 'orez',
        type: 'module',
        exports: {
          './worker/cf-do-shim': './dist/worker/cf-do-shim.js',
        },
      })
    )
    writeFileSync(
      join(orezDir, 'dist', 'worker', 'cf-do-shim.js'),
      'export function isValidNamespace() { return true }\n'
    )
    writeFileSync(
      join(workerDir, 'index.js'),
      "export default { fetch() { return new Response('ok') } }\n"
    )
    const shimPath = join(workerDir, 'app-shim.js')
    const outfile = join(workerDir, 'index.js')
    writeFileSync(shimPath, buildAppShimSource(cfDeployConfig('contrast')))

    await bundleCloudflareRustSyncAppWorker(cfDeployConfig('contrast'), {
      workerDir,
      shimPath,
      outfile,
      writeMigrationModule: async (dir) => {
        writeFileSync(join(dir, 'orez-migrations.js'), 'export {}\n')
        writeFileSync(join(dir, 'orez-schema-version.js'), 'export {}\n')
        return join(dir, 'orez-migrations.js')
      },
    })

    expect(
      readdirSync(workerDir).filter((name) => /^one-app-[A-Za-z0-9_-]+\.js$/.test(name))
    ).toHaveLength(0)
  })

  it('keeps external One asset imports linked to the lazy app chunk', async () => {
    const workerDir = mkdtempSync(join(tmpdir(), 'orez-one-asset-link-'))
    workerDirs.push(workerDir)
    mkdirSync(join(workerDir, 'assets'), { recursive: true })
    mkdirSync(join(workerDir, 'node_modules'))
    writeFileSync(join(workerDir, 'package.json'), JSON.stringify({ type: 'module' }))
    writeFileSync(
      join(workerDir, 'index.js'),
      [
        'export function n(initialize) {',
        '  let initialized = false',
        '  return () => {',
        '    if (initialized) return',
        '    initialized = true',
        '    initialize()',
        '  }',
        '}',
        'export default {',
        '  async fetch() {',
        "    const { readValue } = await import('./assets/route.js')",
        '    return new Response(readValue())',
        '  },',
        '}',
      ].join('\n')
    )
    writeFileSync(
      join(workerDir, 'assets', 'route.js'),
      [
        "import { n } from '../index.js'",
        "let value = 'uninitialized'",
        "const initialize = n(() => { value = 'linked' })",
        'export function readValue() {',
        '  initialize()',
        '  return value',
        '}',
      ].join('\n')
    )
    const shimPath = join(workerDir, 'user-shim.js')
    const outfile = join(workerDir, 'index.js')
    writeFileSync(
      shimPath,
      [
        'let workerPromise',
        'function getWorker() {',
        "  workerPromise ||= import('./one-app.js').then((module) => module.default)",
        '  return workerPromise',
        '}',
        'export default {',
        '  async fetch(request) {',
        '    return (await getWorker()).fetch(request)',
        '  },',
        '}',
      ].join('\n')
    )

    await bundleCloudflareRustSyncAppWorker(cfDeployConfig('contrast'), {
      workerDir,
      shimPath,
      outfile,
      writeMigrationModule: async (dir) => {
        writeFileSync(join(dir, 'orez-migrations.js'), 'export {}\n')
        writeFileSync(join(dir, 'orez-schema-version.js'), 'export {}\n')
        return join(dir, 'orez-migrations.js')
      },
    })

    const runtime = spawnSync(
      process.execPath,
      [
        '-e',
        [
          'const workerModule = await import(new URL(`file://${process.env.WORKER_ENTRY}`))',
          "const response = await workerModule.default.fetch(new Request('https://app.test/'))",
          'console.log(await response.text())',
        ].join(';'),
      ],
      {
        encoding: 'utf8',
        env: { ...process.env, WORKER_ENTRY: outfile },
      }
    )
    expect({ status: runtime.status, stderr: runtime.stderr }).toEqual({
      status: 0,
      stderr: '',
    })
    expect(runtime.stdout.trim()).toBe('linked')
  })
})
