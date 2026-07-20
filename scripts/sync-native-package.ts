#!/usr/bin/env bun

import { execFileSync } from 'node:child_process'
import {
  chmodSync,
  cpSync,
  existsSync,
  mkdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs'
import { resolve } from 'node:path'

import {
  findSyncNativePlatform,
  renderSyncNativeShim,
  SYNC_NATIVE_PLATFORMS,
  syncNativeMatrix,
} from './sync-native-platforms.js'

const root = resolve(import.meta.dirname, '..')

function readJson(path: string): any {
  return JSON.parse(readFileSync(path, 'utf8'))
}

function writeJson(path: string, value: unknown): void {
  writeFileSync(path, JSON.stringify(value, null, 2) + '\n')
}

export function syncNativeVersion(): string {
  const metadata = JSON.parse(
    execFileSync('cargo', ['metadata', '--format-version=1', '--no-deps'], {
      cwd: root,
      encoding: 'utf8',
    })
  )
  const pkg = metadata.packages.find((candidate: { name: string }) => {
    return candidate.name === 'sync-native'
  })
  if (!pkg) throw new Error('cargo metadata did not contain sync-native')
  return pkg.version
}

export function validateSyncNativePackages(version = syncNativeVersion()): void {
  const launcherPath = resolve(root, 'packages/orez-sync-native/package.json')
  const launcher = readJson(launcherPath)
  if (launcher.version !== version) {
    throw new Error(`orez-sync-native is ${launcher.version}, expected ${version}`)
  }

  const expectedDependencies = Object.fromEntries(
    SYNC_NATIVE_PLATFORMS.map(({ npmPackage }) => [npmPackage, 'workspace:*'])
  )
  if (
    JSON.stringify(launcher.optionalDependencies) !== JSON.stringify(expectedDependencies)
  ) {
    throw new Error('orez-sync-native optionalDependencies do not match the platform map')
  }

  for (const platform of SYNC_NATIVE_PLATFORMS) {
    const manifest = readJson(resolve(root, platform.packageDir, 'package.json'))
    if (manifest.name !== platform.npmPackage || manifest.version !== version) {
      throw new Error(
        `${platform.packageDir} name/version does not match the platform map`
      )
    }
    if (manifest.os?.[0] !== platform.os || manifest.cpu?.[0] !== platform.cpu) {
      throw new Error(`${platform.npmPackage} os/cpu metadata is incorrect`)
    }
    if ((manifest.libc?.[0] ?? undefined) !== platform.libc) {
      throw new Error(`${platform.npmPackage} libc metadata is incorrect`)
    }
    if (!manifest.files?.includes('LICENSES.txt')) {
      throw new Error(`${platform.npmPackage} does not ship LICENSES.txt`)
    }
  }
}

export function preparePlatformPackage(
  id: string,
  binaryPath: string,
  outputDir: string,
  version = syncNativeVersion(),
  sourceCommit?: string
): string {
  validateSyncNativePackages(version)
  const platform = SYNC_NATIVE_PLATFORMS.find((candidate) => candidate.id === id)
  if (!platform) throw new Error(`unknown sync-native platform ${id}`)
  if (!existsSync(binaryPath))
    throw new Error(`missing sync-native binary: ${binaryPath}`)

  rmSync(outputDir, { recursive: true, force: true })
  mkdirSync(resolve(outputDir, 'bin'), { recursive: true })
  const manifest = readJson(resolve(root, platform.packageDir, 'package.json'))
  manifest.version = version
  if (sourceCommit) manifest.orezSourceCommit = sourceCommit
  writeJson(resolve(outputDir, 'package.json'), manifest)
  cpSync(resolve(root, 'LICENSE'), resolve(outputDir, 'LICENSE'))
  cpSync(resolve(root, 'LICENSES.txt'), resolve(outputDir, 'LICENSES.txt'))
  const destination = resolve(outputDir, 'bin', platform.executable)
  cpSync(binaryPath, destination)
  if (platform.os !== 'win32') chmodSync(destination, 0o755)
  return outputDir
}

export function prepareLauncherPackage(
  outputDir: string,
  version = syncNativeVersion(),
  sourceCommit?: string
): string {
  validateSyncNativePackages(version)
  rmSync(outputDir, { recursive: true, force: true })
  mkdirSync(resolve(outputDir, 'bin'), { recursive: true })
  const manifest = readJson(resolve(root, 'packages/orez-sync-native/package.json'))
  manifest.version = version
  if (sourceCommit) manifest.orezSourceCommit = sourceCommit
  manifest.optionalDependencies = Object.fromEntries(
    SYNC_NATIVE_PLATFORMS.map(({ npmPackage }) => [npmPackage, version])
  )
  writeJson(resolve(outputDir, 'package.json'), manifest)
  cpSync(resolve(root, 'LICENSE'), resolve(outputDir, 'LICENSE'))
  const shim = resolve(outputDir, 'bin/sync-native.cjs')
  writeFileSync(shim, renderSyncNativeShim())
  chmodSync(shim, 0o755)
  return outputDir
}

export function prepareBootstrapPackages(
  outputDir: string,
  version = '0.0.0-bootstrap.0'
): string[] {
  if (!/^0\.0\.0-bootstrap\.\d+$/.test(version)) {
    throw new Error('bootstrap version must match 0.0.0-bootstrap.<number>')
  }
  validateSyncNativePackages()
  rmSync(outputDir, { recursive: true, force: true })

  const packages = [
    {
      dir: 'launcher',
      manifest: readJson(resolve(root, 'packages/orez-sync-native/package.json')),
    },
    ...SYNC_NATIVE_PLATFORMS.map((platform) => ({
      dir: platform.id,
      manifest: readJson(resolve(root, platform.packageDir, 'package.json')),
    })),
  ]

  return packages.map(({ dir, manifest }) => {
    const packageDir = resolve(outputDir, dir)
    mkdirSync(packageDir, { recursive: true })
    manifest.version = version
    manifest.description += ' (package-name bootstrap only)'
    delete manifest.bin
    delete manifest.files
    delete manifest.optionalDependencies
    writeJson(resolve(packageDir, 'package.json'), manifest)
    cpSync(resolve(root, 'LICENSE'), resolve(packageDir, 'LICENSE'))
    return packageDir
  })
}

export function currentSyncNativePlatform() {
  const libc =
    process.platform === 'linux'
      ? process.report?.getReport().header.glibcVersionRuntime
        ? 'glibc'
        : 'musl'
      : undefined
  return findSyncNativePlatform(process.platform, process.arch, libc)
}

if (import.meta.main) {
  const [command, ...args] = process.argv.slice(2)
  if (command === 'matrix') {
    const excluded = args[0]
    if (!excluded) console.log(syncNativeMatrix())
    else {
      console.log(
        JSON.stringify({
          include: SYNC_NATIVE_PLATFORMS.filter(({ id }) => id !== excluded),
        })
      )
    }
  } else if (command === 'version') {
    console.log(syncNativeVersion())
  } else if (command === 'check') {
    validateSyncNativePackages(args[0])
  } else if (command === 'prepare-platform') {
    const [id, binaryPath, outputDir, version, sourceCommit] = args
    if (!id || !binaryPath || !outputDir) {
      throw new Error(
        'usage: prepare-platform <id> <binary> <output-dir> [version] [source-commit]'
      )
    }
    preparePlatformPackage(id, binaryPath, outputDir, version, sourceCommit)
  } else if (command === 'prepare-launcher') {
    const [outputDir, version, sourceCommit] = args
    if (!outputDir) {
      throw new Error('usage: prepare-launcher <output-dir> [version] [source-commit]')
    }
    prepareLauncherPackage(outputDir, version, sourceCommit)
  } else if (command === 'prepare-bootstrap') {
    const [outputDir, version] = args
    if (!outputDir) {
      throw new Error('usage: prepare-bootstrap <output-dir> [bootstrap-version]')
    }
    for (const packageDir of prepareBootstrapPackages(outputDir, version)) {
      console.log(packageDir)
    }
  } else {
    throw new Error(
      'usage: sync-native-package.ts matrix|version|check|prepare-platform|prepare-launcher|prepare-bootstrap'
    )
  }
}
