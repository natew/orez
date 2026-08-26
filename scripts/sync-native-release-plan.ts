#!/usr/bin/env bun

import { execFileSync, spawnSync } from 'node:child_process'
import { createHash } from 'node:crypto'
import {
  chmodSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { resolve } from 'node:path'

import { currentSyncNativePlatform, syncNativeVersion } from './sync-native-package.js'
import { SYNC_NATIVE_PLATFORMS } from './sync-native-platforms.js'

const launcherPackage = 'orez-sync-native'
const root = resolve(import.meta.dirname, '..')
const nativePackages = [
  launcherPackage,
  ...SYNC_NATIVE_PLATFORMS.map(({ npmPackage }) => npmPackage),
]
const nativeSourceInputs = [
  'Cargo.lock',
  'Cargo.toml',
  'rust-toolchain.toml',
  'crates/sync-core/Cargo.toml',
  'crates/sync-core/src',
  'crates/sync-native/Cargo.toml',
  'crates/sync-native/src',
]

type PackageMetadata = {
  version?: string
  orezSourceCommit?: string
  orezNativeSourceRevision?: string
}

export type SyncNativeReleasePlan = {
  publish: boolean
  version: string
  localRevision: string
  publishedRevision?: string
  localSourceRevision: string
  publishedSourceRevision?: string
  reason: string
}

type SyncNativeReleaseDecision = {
  latestVersion: string
  sourceVersion: string
  completeRelease: boolean
  localRevision: string
  publishedRevision?: string
  localSourceRevision: string
  publishedSourceRevision?: string
}

export function syncNativeContractCheckMode({
  dryRun,
  packOnly,
  rePublish,
  trustedPublishing,
}: {
  dryRun: boolean
  packOnly: boolean
  rePublish: boolean
  trustedPublishing: boolean
}): 'select-and-verify' | 'verify' | 'skip' {
  if (packOnly || rePublish) return 'skip'
  if (trustedPublishing && !dryRun) return 'select-and-verify'
  return 'verify'
}

function parseVersion(version: string): [number, number, number] {
  const match = /^(\d+)\.(\d+)\.(\d+)$/.exec(version)
  if (!match) throw new Error(`sync-native version must be major.minor.patch: ${version}`)
  return [Number(match[1]), Number(match[2]), Number(match[3])]
}

function compareVersions(left: string, right: string): number {
  const leftParts = parseVersion(left)
  const rightParts = parseVersion(right)
  for (let index = 0; index < leftParts.length; index++) {
    if (leftParts[index] !== rightParts[index]) {
      return leftParts[index] - rightParts[index]
    }
  }
  return 0
}

export function nextSyncNativeVersion(
  publishedVersion: string,
  sourceVersion: string
): string {
  const [major, minor, patch] = parseVersion(publishedVersion)
  parseVersion(sourceVersion)
  const nextPublished = `${major}.${minor}.${patch + 1}`
  return compareVersions(sourceVersion, nextPublished) > 0 ? sourceVersion : nextPublished
}

export function syncNativeRevision(versionOutput: string): string {
  const fields = versionOutput.trim().split(/\s+/)
  if (fields[0] !== 'sync-native' || fields.length < 3 || !fields[2].startsWith('core')) {
    throw new Error(`invalid sync-native --version output: ${versionOutput.trim()}`)
  }
  return fields.slice(2).join(' ')
}

function sourceFiles(relativePath: string): string[] {
  const absolutePath = resolve(root, relativePath)
  if (!statSync(absolutePath).isDirectory()) return [relativePath]
  return readdirSync(absolutePath)
    .flatMap((name) => sourceFiles(`${relativePath}/${name}`))
    .sort()
}

// The schema revision answers whether the installed durable shape is
// compatible. This source revision separately answers whether the native
// package was built from these Rust inputs. Hash inputs rather than artifacts:
// native binaries are not reproducible across release runners.
export function syncNativeSourceRevision(): string {
  const hash = createHash('sha256')
  for (const path of nativeSourceInputs.flatMap(sourceFiles).sort()) {
    hash.update(path)
    hash.update('\0')
    hash.update(readFileSync(resolve(root, path)))
    hash.update('\0')
  }
  return `sha256:${hash.digest('hex')}`
}

export function decideSyncNativeRelease({
  latestVersion,
  sourceVersion,
  completeRelease,
  localRevision,
  publishedRevision,
  localSourceRevision,
  publishedSourceRevision,
}: SyncNativeReleaseDecision): SyncNativeReleasePlan {
  const currentContract = publishedRevision === localRevision
  const currentSource = publishedSourceRevision === localSourceRevision
  if (completeRelease && currentContract && currentSource) {
    return {
      publish: false,
      version: latestVersion,
      localRevision,
      publishedRevision,
      localSourceRevision,
      publishedSourceRevision,
      reason: `npm ${latestVersion} already carries this contract and native source on every platform`,
    }
  }

  const version = nextSyncNativeVersion(latestVersion, sourceVersion)
  let reason: string
  if (!completeRelease) {
    reason = `npm ${latestVersion} is incomplete or has mixed source provenance`
  } else if (!currentContract) {
    reason = `npm ${latestVersion} carries contract ${publishedRevision ?? 'none'}`
  } else {
    reason = `npm ${latestVersion} was built from native source ${publishedSourceRevision ?? 'none'}`
  }
  return {
    publish: true,
    version,
    localRevision,
    publishedRevision,
    localSourceRevision,
    publishedSourceRevision,
    reason,
  }
}

export function parseNpmMetadata(output: string): PackageMetadata {
  const raw = JSON.parse(output) as
    | PackageMetadata
    | string
    | Array<PackageMetadata | string>
  const metadata = Array.isArray(raw) ? raw.at(-1) : raw
  if (!metadata) return {}
  return typeof metadata === 'string' ? { version: metadata } : metadata
}

function npmMetadata(spec: string): PackageMetadata | undefined {
  const result = spawnSync(
    'npm',
    [
      'view',
      spec,
      'version',
      'orezSourceCommit',
      'orezNativeSourceRevision',
      '--json',
      '--prefer-online',
    ],
    { encoding: 'utf8' }
  )
  if (result.error) throw result.error
  if (result.status === 0) return parseNpmMetadata(result.stdout)
  if (/E404|404 Not Found|is not in this registry/i.test(result.stderr)) return undefined
  throw new Error(`could not read npm metadata for ${spec}: ${result.stderr.trim()}`)
}

function packedRevision(
  packageName: string,
  version: string,
  executable: string
): string {
  const temporary = mkdtempSync(resolve(tmpdir(), 'orez-sync-native-plan-'))
  try {
    const packed = execFileSync(
      'npm',
      ['pack', `${packageName}@${version}`, '--pack-destination', temporary, '--silent'],
      { encoding: 'utf8' }
    )
      .trim()
      .split(/\r?\n/)
      .at(-1)
    if (!packed)
      throw new Error(`npm pack produced no archive for ${packageName}@${version}`)
    const unpacked = resolve(temporary, 'package')
    mkdirSync(unpacked)
    execFileSync('tar', [
      '-xzf',
      resolve(temporary, packed),
      '-C',
      unpacked,
      '--strip-components=1',
    ])
    const binary = resolve(unpacked, 'bin', executable)
    chmodSync(binary, 0o755)
    return syncNativeRevision(execFileSync(binary, ['--version'], { encoding: 'utf8' }))
  } finally {
    rmSync(temporary, { recursive: true, force: true })
  }
}

export function planSyncNativeRelease(localBinary: string): SyncNativeReleasePlan {
  const platform = currentSyncNativePlatform()
  if (!platform) {
    throw new Error(`sync-native does not support ${process.platform} ${process.arch}`)
  }
  const localRevision = syncNativeRevision(
    execFileSync(localBinary, ['--version'], { encoding: 'utf8' })
  )
  const latest = npmMetadata(`${launcherPackage}@latest`)
  if (!latest?.version) throw new Error(`${launcherPackage}@latest has no version`)
  parseVersion(latest.version)

  const metadata = new Map(
    nativePackages.map((packageName) => [
      packageName,
      npmMetadata(`${packageName}@${latest.version}`),
    ])
  )
  const sourceCommit = metadata.get(launcherPackage)?.orezSourceCommit
  const publishedSourceRevision = metadata.get(launcherPackage)?.orezNativeSourceRevision
  // orezSourceCommit proves only that the package family is internally
  // complete and from one commit. It does not compare that commit with the
  // current native source, which is why behavior-only fixes were once skipped.
  const completeRelease =
    Boolean(sourceCommit) &&
    Boolean(publishedSourceRevision) &&
    nativePackages.every((packageName) => {
      const candidate = metadata.get(packageName)
      return (
        candidate?.version === latest.version &&
        candidate.orezSourceCommit === sourceCommit &&
        candidate.orezNativeSourceRevision === publishedSourceRevision
      )
    })
  const currentPlatform = metadata.get(platform.npmPackage)
  const publishedRevision = currentPlatform
    ? packedRevision(platform.npmPackage, latest.version, platform.executable)
    : undefined
  return decideSyncNativeRelease({
    latestVersion: latest.version,
    sourceVersion: syncNativeVersion(),
    completeRelease,
    localRevision,
    publishedRevision,
    localSourceRevision: syncNativeSourceRevision(),
    publishedSourceRevision,
  })
}

if (import.meta.main) {
  const localBinary = process.argv[2]
  if (!localBinary) {
    throw new Error('usage: sync-native-release-plan.ts <local-sync-native-binary>')
  }
  console.log(JSON.stringify(planSyncNativeRelease(resolve(localBinary))))
}
