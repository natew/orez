#!/usr/bin/env bun

import { execFileSync, spawnSync } from 'node:child_process'
import { cpSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
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

type BootstrapTarget = {
  confirmation: string
  errorLabel: string
  packageNames: string[]
  preparePackages: () => string[]
  relevantPaths: string[]
  successMessage: string
  validateName: (name: string) => boolean
  workflow: string
  workspaceName: string
}

function writeJson(path: string, value: unknown): void {
  writeFileSync(path, JSON.stringify(value, null, 2) + '\n')
}

function prepareSharedBootstrapPackages(): string[] {
  const packageDirs = ['packages/helpers', 'packages/env', 'packages/cli']
  rmSync(bootstrapDir, { recursive: true, force: true })

  return packageDirs.map((sourceDir) => {
    const manifest = JSON.parse(
      readFileSync(resolve(root, sourceDir, 'package.json'), 'utf8')
    ) as Record<string, unknown>
    const packageDir = resolve(bootstrapDir, sourceDir.replace('packages/', ''))
    mkdirSync(packageDir, { recursive: true })
    manifest.version = bootstrapVersion
    manifest.description = `${String(manifest.description)} (package-name bootstrap only)`
    delete manifest.bin
    delete manifest.dependencies
    delete manifest.devDependencies
    delete manifest.exports
    delete manifest.files
    delete manifest.main
    delete manifest.module
    delete manifest.scripts
    delete manifest.types
    writeJson(resolve(packageDir, 'package.json'), manifest)
    cpSync(resolve(root, 'LICENSE'), resolve(packageDir, 'LICENSE'))
    return packageDir
  })
}

function resolveTarget(): BootstrapTarget {
  const requested = process.argv[2] ?? 'native'
  if (requested === 'shared') {
    return {
      confirmation: 'CLAIM OREZ SHARED PACKAGES',
      errorLabel: 'shared release files',
      packageNames: ['@o/helpers', '@o/env', '@o/cli'],
      preparePackages: prepareSharedBootstrapPackages,
      relevantPaths: [
        '.github/workflows/release.yml',
        'scripts/bootstrap-sync-native-packages.ts',
        'packages/helpers',
        'packages/env',
        'packages/cli',
      ],
      successMessage:
        'All three Orez shared package names and trusted publishers are ready.',
      validateName: (name) => name.startsWith('@o/'),
      workflow: 'release.yml',
      workspaceName: 'orez-shared-bootstrap-workspace',
    }
  }
  if (requested !== 'native') {
    throw new Error('usage: bootstrap-sync-native-packages.ts [native|shared]')
  }

  const packageNames = [
    'orez-sync-native',
    ...SYNC_NATIVE_PLATFORMS.map(({ npmPackage }) => npmPackage),
  ]
  return {
    confirmation: 'CLAIM OREZ NATIVE PACKAGES',
    errorLabel: 'native release files',
    packageNames,
    preparePackages: () => prepareBootstrapPackages(bootstrapDir, bootstrapVersion),
    relevantPaths: [
      '.github/workflows/release-sync-native.yml',
      'scripts/bootstrap-sync-native-packages.ts',
      'scripts/sync-native-package.ts',
      'scripts/sync-native-platforms.ts',
      'packages/orez-sync-native',
      ...SYNC_NATIVE_PLATFORMS.map(({ packageDir }) => packageDir),
    ],
    successMessage:
      'All nine Orez native package names and trusted publishers are ready.',
    validateName: (name) => !name.startsWith('@'),
    workflow: 'release-sync-native.yml',
    workspaceName: 'orez-sync-native-bootstrap-workspace',
  }
}

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

function trustMatches(config: TrustConfig, workflow: string): boolean {
  return (
    config.type === 'github' &&
    config.repository === 'natew/orez' &&
    config.file === workflow &&
    config.environment === undefined &&
    config.permissions?.length === 1 &&
    config.permissions[0] === 'createPackage'
  )
}

async function main(): Promise<void> {
  const target = resolveTarget()
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
    ['diff', '--quiet', 'HEAD', '--', ...target.relevantPaths],
    { cwd: root, stdio: 'inherit' }
  )
  if (relevantDiff.error) throw relevantDiff.error
  if (relevantDiff.status !== 0) {
    throw new Error(`${target.errorLabel} differ from the current commit`)
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

  const generated = target.preparePackages()
  const generatedByName = new Map(
    generated.map((packageDir) => {
      const manifest = JSON.parse(
        readFileSync(resolve(packageDir, 'package.json'), 'utf8')
      ) as { name: string }
      return [manifest.name, packageDir]
    })
  )
  if (
    generatedByName.size !== target.packageNames.length ||
    target.packageNames.some(
      (name) => !generatedByName.has(name) || !target.validateName(name)
    )
  ) {
    throw new Error(
      'generated bootstrap packages do not match the requested package names'
    )
  }

  const missing: string[] = []
  for (const name of target.packageNames) {
    if (!packageExists(name)) {
      missing.push(name)
      continue
    }
    verifyOwner(name)
    const trust = readTrust(name)
    if (trust && !trustMatches(trust, target.workflow)) {
      throw new Error(`${name} already has a different trusted publisher`)
    }
  }

  console.log(`\nThis permanently claims and configures these npm package names:\n`)
  for (const name of target.packageNames) console.log(`  ${name}`)
  console.log(
    `\n${missing.length} package name(s) need the ${bootstrapVersion} bootstrap publish.`
  )
  console.log(
    'When npm opens the browser, approve with Touch ID or your security key and select "skip two-factor authentication for the next 5 minutes".'
  )
  const prompt = createInterface({ input: process.stdin, output: process.stdout })
  const confirmation = await prompt.question(
    `\nType ${target.confirmation} to continue: `
  )
  prompt.close()
  if (confirmation !== target.confirmation) {
    throw new Error('package bootstrap cancelled')
  }

  if (missing.length > 0) {
    writeFileSync(
      resolve(bootstrapDir, 'package.json'),
      JSON.stringify(
        {
          name: target.workspaceName,
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

  for (const name of target.packageNames) {
    if (!packageExists(name)) throw new Error(`${name} was not published`)
    verifyOwner(name)
    const trust = readTrust(name)
    if (trustMatches(trust ?? {}, target.workflow)) {
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
      target.workflow,
      '--allow-publish',
      '--yes',
    ])
    const configured = readTrust(name)
    if (!configured || !trustMatches(configured, target.workflow)) {
      throw new Error(`${name} trusted publisher did not match after configuration`)
    }
    await Bun.sleep(2_000)
  }

  console.log(`\n${target.successMessage}`)
}

main().catch((error) => {
  console.error(`\nBootstrap stopped: ${error instanceof Error ? error.message : error}`)
  console.error(
    'Fix the reported problem, then rerun the same command. Completed names are skipped.'
  )
  process.exitCode = 1
})
