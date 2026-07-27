import { spawnSync } from 'node:child_process'
import {
  existsSync,
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

import { bundleCloudflareLiteAppWorker } from './bundle.js'
import { defineCloudflareConfig } from './config.js'
import { configureCloudflareWorker } from './wrangler.js'

const workerDirs: string[] = []

afterEach(() => {
  for (const dir of workerDirs.splice(0)) rmSync(dir, { recursive: true, force: true })
})

describe('Cloudflare app bundling', () => {
  it('attaches root split chunks to a bare Wrangler upload', () => {
    const workerDir = mkdtempSync(join(tmpdir(), 'orez-wrangler-modules-'))
    workerDirs.push(workerDir)
    mkdirSync(join(workerDir, 'assets'))
    writeFileSync(
      join(workerDir, 'index.js'),
      [
        "import { value } from './chunk-EXAMPLE.js'",
        'export class ZeroSqlDO {}',
        'export default { fetch() { return new Response(value) } }',
      ].join('\n')
    )
    writeFileSync(join(workerDir, 'chunk-EXAMPLE.js'), "export const value = 'ok'\n")
    writeFileSync(join(workerDir, 'assets', 'route.js'), "export const route = 'ok'\n")
    writeFileSync(join(workerDir, 'assets', 'message.mdx'), 'caller rule preserved\n')
    const configPath = join(workerDir, 'wrangler.json')
    const dryRunDir = join(workerDir, 'dry-run')
    writeFileSync(
      configPath,
      JSON.stringify(
        configureCloudflareWorker({
          name: 'orez-root-module-test',
          main: 'index.js',
          no_bundle: true,
          rules: [
            { type: 'ESModule', globs: ['assets/**/*.js'], fallthrough: true },
            { type: 'Text', globs: ['assets/**/*.mdx'], fallthrough: true },
          ],
        })
      )
    )

    const dryRun = spawnSync(
      join(process.cwd(), 'node_modules', '.bin', 'wrangler'),
      ['deploy', '--dry-run', '--outdir', dryRunDir, '--config', configPath],
      {
        cwd: workerDir,
        encoding: 'utf8',
      }
    )

    expect({ status: dryRun.status, stderr: dryRun.stderr }).toEqual({
      status: 0,
      stderr: '',
    })
    expect(existsSync(join(dryRunDir, 'chunk-EXAMPLE.js'))).toBe(true)
    expect(existsSync(join(dryRunDir, 'assets', 'route.js'))).toBe(true)
    expect(existsSync(join(dryRunDir, 'assets', 'message.mdx'))).toBe(true)
  })

  it('bundles a caller-owned entrypoint with stable virtual imports', async () => {
    const workerDir = mkdtempSync(join(tmpdir(), 'orez-lite-entry-'))
    workerDirs.push(workerDir)
    mkdirSync(join(workerDir, 'node_modules'))
    writeFileSync(
      join(workerDir, 'index.js'),
      "export default { fetch() { return new Response('ok') } }\n"
    )
    const entryPoint = join(workerDir, 'cloudflare-worker.js')
    writeFileSync(
      entryPoint,
      [
        "import { SCHEMA_VERSION } from 'orez:cloudflare-migrations'",
        "const app = import('orez:application-worker')",
        'export default {',
        '  async fetch(request, env, ctx) {',
        '    const worker = (await app).default',
        '    return worker.fetch(request, env, ctx)',
        '  },',
        '}',
        'export { SCHEMA_VERSION }',
      ].join('\n')
    )
    const outfile = join(workerDir, 'index.js')

    await bundleCloudflareLiteAppWorker(defineCloudflareConfig('contrast'), {
      workerDir,
      entryPoint,
      outfile,
      writeMigrationModule: async (dir) => {
        writeFileSync(
          join(dir, 'orez-migrations.js'),
          "export const SCHEMA_VERSION = 'v1'\n"
        )
        return join(dir, 'orez-migrations.js')
      },
    })

    expect(
      readdirSync(workerDir).filter((name) => /^one-app-[A-Za-z0-9_-]+\.js$/.test(name))
    ).toHaveLength(1)
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
    const entryPoint = join(workerDir, 'user-shim.js')
    const outfile = join(workerDir, 'index.js')
    writeFileSync(
      entryPoint,
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

    await bundleCloudflareLiteAppWorker(defineCloudflareConfig('contrast'), {
      workerDir,
      entryPoint,
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
