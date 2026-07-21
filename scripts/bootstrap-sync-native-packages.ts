#!/usr/bin/env bun

import { execFileSync, spawnSync } from 'node:child_process'
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { relative, resolve } from 'node:path'
import { createInterface } from 'node:readline/promises'

import { prepareBootstrapPackages } from './sync-native-package.js'
import { SYNC_NATIVE_PLATFORMS } from './sync-native-platforms.js'

const root = resolve(import.meta.dirname, '..')
const nodeVersion = '24.16.0'
const npmVersion = '12.0.1'
const bootstrapVersion = '0.0.0-bootstrap.0'
const bootstrapDir = resolve(root, '.release/bootstrap')
const npmPrefix = resolve(root, `.release/npm-runtime`)
const nodeCli = resolve(npmPrefix, 'node_modules/node/bin/node')
const npmCli = resolve(npmPrefix, 'node_modules/npm/bin/npm-cli.js')
const packages = [
  'orez-sync-native',
  ...SYNC_NATIVE_PLATFORMS.map(({ npmPackage }) => npmPackage),
]

function capture(command: string, args: string[], cwd = root): string {
  return execFileSync(command, args, { cwd, encoding: 'utf8' }).trim()
}

function captureNpm(args: string[], cwd = root) {
  return spawnSync(nodeCli, [npmCli, ...args], {
    cwd,
    encoding: 'utf8',
  })
}

function runNpm(args: string[], cwd = root): void {
  const result = spawnSync(nodeCli, [npmCli, ...args], {
    cwd,
    stdio: 'inherit',
  })
  if (result.error) throw result.error
  if (result.status !== 0) {
    throw new Error(`npm ${args[0]} failed with exit code ${result.status}`)
  }
}

function packageExists(name: string): boolean {
  const result = captureNpm(['view', name, 'name', '--json'])
  if (result.status === 0) return true
  if (result.stderr.includes('E404')) return false
  throw new Error(`could not check ${name}: ${result.stderr.trim()}`)
}

function verifyOwner(name: string): void {
  const result = captureNpm(['view', name, 'maintainers', '--json'])
  if (result.status !== 0) {
    throw new Error(`could not read ${name} maintainers: ${result.stderr.trim()}`)
  }
  const raw = JSON.parse(result.stdout)
  const maintainers = (Array.isArray(raw) ? raw : [raw]).map(
    (maintainer: string | { name?: string }) =>
      typeof maintainer === 'string' ? maintainer.match(/^[^ <]+/)?.[0] : maintainer.name
  )
  if (!maintainers.includes('nwienert')) {
    throw new Error(`${name} exists but is not owned by nwienert`)
  }
}

type TrustConfig = {
  type?: string
  file?: string
  repository?: string
  environment?: string
  permissions?: string[]
}

function readTrust(name: string): TrustConfig | undefined {
  const result = captureNpm(['trust', 'list', name, '--json'])
  if (result.status !== 0) {
    throw new Error(`could not read ${name} trust: ${result.stderr.trim()}`)
  }
  if (!result.stdout.trim()) return undefined
  return JSON.parse(result.stdout)
}

function trustMatches(config: TrustConfig): boolean {
  return (
    config.type === 'github' &&
    config.repository === 'natew/orez' &&
    config.file === 'release-sync-native.yml' &&
    config.environment === undefined &&
    config.permissions?.length === 1 &&
    config.permissions[0] === 'createPackage'
  )
}

async function main(): Promise<void> {
  if (capture('git', ['branch', '--show-current']) !== 'main') {
    throw new Error('run this only from the Orez main branch')
  }
  const head = capture('git', ['rev-parse', 'HEAD'])
  const remoteMain = capture('git', ['ls-remote', 'origin', 'refs/heads/main']).split(
    /\s/
  )[0]
  if (head !== remoteMain) {
    throw new Error(`local HEAD ${head} is not current origin/main ${remoteMain}`)
  }
  const relevantDiff = spawnSync(
    'git',
    [
      'diff',
      '--quiet',
      'HEAD',
      '--',
      '.github/workflows/release-sync-native.yml',
      'scripts/bootstrap-sync-native-packages.ts',
      'scripts/sync-native-package.ts',
      'scripts/sync-native-platforms.ts',
      'packages/orez-sync-native',
      ...SYNC_NATIVE_PLATFORMS.map(({ packageDir }) => packageDir),
    ],
    { cwd: root, stdio: 'inherit' }
  )
  if (relevantDiff.error) throw relevantDiff.error
  if (relevantDiff.status !== 0) {
    throw new Error('native release files differ from the current commit')
  }

  const runs = JSON.parse(
    capture('gh', [
      'api',
      `/repos/natew/orez/actions/workflows/ci.yml/runs?head_sha=${head}&status=completed&per_page=100`,
    ])
  ) as { workflow_runs?: { conclusion?: string }[] }
  if (!runs.workflow_runs?.some(({ conclusion }) => conclusion === 'success')) {
    throw new Error(`hosted CI has not passed for ${head}`)
  }

  mkdirSync(npmPrefix, { recursive: true })
  execFileSync(
    'npm',
    [
      'install',
      '--prefix',
      npmPrefix,
      '--ignore-scripts=false',
      '--no-package-lock',
      '--no-save',
      `node@${nodeVersion}`,
      `npm@${npmVersion}`,
    ],
    { cwd: root, stdio: 'inherit' }
  )
  if (capture(nodeCli, ['--version']) !== `v${nodeVersion}`) {
    throw new Error(`failed to install Node.js ${nodeVersion}`)
  }
  if (capture(nodeCli, [npmCli, '--version']) !== npmVersion) {
    throw new Error(`failed to install npm ${npmVersion}`)
  }
  if (capture(nodeCli, [npmCli, 'whoami']) !== 'nwienert') {
    throw new Error('npm must be authenticated as nwienert')
  }

  const generated = prepareBootstrapPackages(bootstrapDir, bootstrapVersion)
  const generatedByName = new Map(
    generated.map((packageDir) => {
      const manifest = JSON.parse(
        readFileSync(resolve(packageDir, 'package.json'), 'utf8')
      ) as { name: string }
      return [manifest.name, packageDir]
    })
  )
  if (
    generatedByName.size !== packages.length ||
    packages.some((name) => !generatedByName.has(name) || name.startsWith('@'))
  ) {
    throw new Error('generated bootstrap packages do not match the unscoped platform map')
  }

  const missing: string[] = []
  for (const name of packages) {
    if (!packageExists(name)) {
      missing.push(name)
      continue
    }
    verifyOwner(name)
    const trust = readTrust(name)
    if (trust && !trustMatches(trust)) {
      throw new Error(`${name} already has a different trusted publisher`)
    }
  }

  console.log(`\nThis permanently claims and configures these npm package names:\n`)
  for (const name of packages) console.log(`  ${name}`)
  console.log(
    `\n${missing.length} package name(s) need the ${bootstrapVersion} bootstrap publish.`
  )
  console.log(
    'When npm opens the browser, approve with Touch ID or your security key and select "skip two-factor authentication for the next 5 minutes".'
  )
  const prompt = createInterface({ input: process.stdin, output: process.stdout })
  const confirmation = await prompt.question(
    '\nType CLAIM OREZ NATIVE PACKAGES to continue: '
  )
  prompt.close()
  if (confirmation !== 'CLAIM OREZ NATIVE PACKAGES') {
    throw new Error('package bootstrap cancelled')
  }

  if (missing.length > 0) {
    writeFileSync(
      resolve(bootstrapDir, 'package.json'),
      JSON.stringify(
        {
          name: 'orez-sync-native-bootstrap-workspace',
          private: true,
          workspaces: missing.map((name) =>
            relative(bootstrapDir, generatedByName.get(name)!)
          ),
        },
        null,
        2
      ) + '\n'
    )
    runNpm(
      [
        'publish',
        '--workspaces',
        '--access',
        'public',
        '--tag',
        'bootstrap',
        '--ignore-scripts',
        '--auth-type',
        'web',
      ],
      bootstrapDir
    )
  }

  for (const name of packages) {
    if (!packageExists(name)) throw new Error(`${name} was not published`)
    verifyOwner(name)
    const trust = readTrust(name)
    if (trustMatches(trust ?? {})) {
      console.log(`${name}: trusted publisher already configured`)
      continue
    }
    if (trust) throw new Error(`${name} already has a different trusted publisher`)
    runNpm([
      'trust',
      'github',
      name,
      '--repo',
      'natew/orez',
      '--file',
      'release-sync-native.yml',
      '--allow-publish',
      '--yes',
    ])
    const configured = readTrust(name)
    if (!configured || !trustMatches(configured)) {
      throw new Error(`${name} trusted publisher did not match after configuration`)
    }
    await Bun.sleep(2_000)
  }

  console.log('\nAll nine Orez native package names and trusted publishers are ready.')
}

main().catch((error) => {
  console.error(`\nBootstrap stopped: ${error instanceof Error ? error.message : error}`)
  console.error(
    'Fix the reported problem, then rerun the same command. Completed names are skipped.'
  )
  process.exitCode = 1
})
