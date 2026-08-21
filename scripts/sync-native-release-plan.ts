#!/usr/bin/env bun

import { execFileSync, spawnSync } from 'node:child_process'
import { chmodSync, mkdirSync, mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { resolve } from 'node:path'

import { currentSyncNativePlatform, syncNativeVersion } from './sync-native-package.js'
import { SYNC_NATIVE_PLATFORMS } from './sync-native-platforms.js'

const launcherPackage = 'orez-sync-native'
const nativePackages = [
  launcherPackage,
  ...SYNC_NATIVE_PLATFORMS.map(({ npmPackage }) => npmPackage),
]

type PackageMetadata = {
  version?: string
  orezSourceCommit?: string
}

export type SyncNativeReleasePlan = {
  publish: boolean
  version: string
  localRevision: string
  publishedRevision?: string
  reason: string
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
    ['view', spec, 'version', 'orezSourceCommit', '--json', '--prefer-online'],
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
  const completeRelease =
    Boolean(sourceCommit) &&
    nativePackages.every((packageName) => {
      const candidate = metadata.get(packageName)
      return (
        candidate?.version === latest.version &&
        candidate.orezSourceCommit === sourceCommit
      )
    })
  const currentPlatform = metadata.get(platform.npmPackage)
  const publishedRevision = currentPlatform
    ? packedRevision(platform.npmPackage, latest.version, platform.executable)
    : undefined

  if (completeRelease && publishedRevision === localRevision) {
    return {
      publish: false,
      version: latest.version,
      localRevision,
      publishedRevision,
      reason: `npm ${latest.version} already carries this contract on every platform`,
    }
  }

  const version = nextSyncNativeVersion(latest.version, syncNativeVersion())
  return {
    publish: true,
    version,
    localRevision,
    publishedRevision,
    reason:
      publishedRevision === localRevision
        ? `npm ${latest.version} is incomplete or has mixed source provenance`
        : `npm ${latest.version} carries ${publishedRevision ?? 'no contract'}`,
  }
}

if (import.meta.main) {
  const localBinary = process.argv[2]
  if (!localBinary) {
    throw new Error('usage: sync-native-release-plan.ts <local-sync-native-binary>')
  }
  console.log(JSON.stringify(planSyncNativeRelease(resolve(localBinary))))
}
