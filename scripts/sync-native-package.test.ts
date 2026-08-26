import { describe, expect, it } from 'bun:test'
import { execFileSync, spawn, spawnSync } from 'node:child_process'
import {
  chmodSync,
  cpSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  writeFileSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { resolve } from 'node:path'

import {
  currentSyncNativePlatform,
  prepareBootstrapPackages,
  prepareLauncherPackage,
  preparePlatformPackage,
  syncNativeVersion,
  validateSyncNativePackages,
} from './sync-native-package.js'
import { renderSyncNativeShim } from './sync-native-platforms.js'

describe('sync-native npm packages', () => {
  it('keeps Cargo, launcher, and platform package metadata aligned', () => {
    expect(() => validateSyncNativePackages()).not.toThrow()
    expect(readFileSync('packages/orez-sync-native/bin/sync-native.cjs', 'utf8')).toBe(
      renderSyncNativeShim()
    )
  })

  it('packages and executes the current platform without install scripts', () => {
    const platform = currentSyncNativePlatform()
    if (!platform || platform.os === 'win32') return

    const temporary = mkdtempSync(resolve(tmpdir(), 'orez-sync-native-package-'))
    const fakeBinary = resolve(temporary, 'fake-sync-native')
    writeFileSync(fakeBinary, '#!/bin/sh\nprintf \'%s\\n\' "$@"\n')
    chmodSync(fakeBinary, 0o755)

    const platformDir = resolve(
      temporary,
      'node_modules',
      ...platform.npmPackage.split('/')
    )
    preparePlatformPackage(platform.id, fakeBinary, platformDir)
    const launcherDir = resolve(temporary, 'node_modules/orez-sync-native')
    prepareLauncherPackage(launcherDir)

    const shim = resolve(launcherDir, 'bin/sync-native.cjs')
    expect(
      execFileSync(process.execPath, [shim, 'one', 'two'], { encoding: 'utf8' })
    ).toBe('one\ntwo\n')
  })

  it('writes exact versions into publishable package copies', () => {
    const platform = currentSyncNativePlatform()
    if (!platform || platform.os === 'win32') return

    const temporary = mkdtempSync(resolve(tmpdir(), 'orez-sync-native-version-'))
    const fakeBinary = resolve(temporary, 'fake-sync-native')
    cpSync(process.execPath, fakeBinary)
    const version = '9.8.7'
    const sourceRevision = 'sha256:def456'
    const platformDir = resolve(temporary, 'platform')
    preparePlatformPackage(
      platform.id,
      fakeBinary,
      platformDir,
      version,
      'abc123',
      sourceRevision
    )
    const launcherDir = resolve(temporary, 'launcher')
    prepareLauncherPackage(launcherDir, version, 'abc123', sourceRevision)

    const platformManifest = JSON.parse(
      readFileSync(resolve(platformDir, 'package.json'), 'utf8')
    )
    const launcherManifest = JSON.parse(
      readFileSync(resolve(launcherDir, 'package.json'), 'utf8')
    )
    expect(platformManifest.version).toBe(version)
    expect(launcherManifest.version).toBe(version)
    expect(platformManifest.orezSourceCommit).toBe('abc123')
    expect(launcherManifest.orezSourceCommit).toBe('abc123')
    expect(platformManifest.orezNativeSourceRevision).toBe(sourceRevision)
    expect(launcherManifest.orezNativeSourceRevision).toBe(sourceRevision)
    expect(syncNativeVersion()).not.toBe(version)
    expect(readFileSync(resolve(platformDir, 'LICENSES.txt'), 'utf8')).toBe(
      readFileSync('LICENSES.txt', 'utf8')
    )
    expect(new Set(Object.values(launcherManifest.optionalDependencies))).toEqual(
      new Set([version])
    )
  })

  it('prepares package-name bootstraps without consuming a release version', () => {
    const temporary = mkdtempSync(resolve(tmpdir(), 'orez-sync-native-bootstrap-'))
    const packages = prepareBootstrapPackages(temporary)

    expect(packages).toHaveLength(9)
    expect(readdirSync(temporary)).toHaveLength(10)
    expect(
      JSON.parse(readFileSync(resolve(temporary, 'package.json'), 'utf8')).workspaces
    ).toHaveLength(9)
    for (const packageDir of packages) {
      const manifest = JSON.parse(
        readFileSync(resolve(packageDir, 'package.json'), 'utf8')
      )
      expect(manifest.version).toBe('0.0.0-bootstrap.0')
      expect(manifest.bin).toBeUndefined()
      expect(manifest.files).toBeUndefined()
      expect(manifest.optionalDependencies).toBeUndefined()
    }

    expect(() => prepareBootstrapPackages(temporary, '0.1.0')).toThrow(
      'bootstrap version must match'
    )
  })

  it('terminates with the same signal as the native child', async () => {
    const platform = currentSyncNativePlatform()
    if (!platform || platform.os === 'win32') return

    const temporary = mkdtempSync(resolve(tmpdir(), 'orez-sync-native-signal-'))
    const fakeBinary = resolve(temporary, 'fake-sync-native')
    writeFileSync(fakeBinary, "#!/bin/sh\nprintf 'ready\\n'\nwhile :; do sleep 1; done\n")
    chmodSync(fakeBinary, 0o755)

    const platformDir = resolve(
      temporary,
      'node_modules',
      ...platform.npmPackage.split('/')
    )
    preparePlatformPackage(platform.id, fakeBinary, platformDir)
    const launcherDir = resolve(temporary, 'node_modules/orez-sync-native')
    prepareLauncherPackage(launcherDir)

    const shim = resolve(launcherDir, 'bin/sync-native.cjs')
    const result = await new Promise<{
      code: number | null
      signal: NodeJS.Signals | null
    }>((resolveResult, reject) => {
      const child = spawn(process.execPath, [shim], { stdio: ['ignore', 'pipe', 'pipe'] })
      child.on('error', reject)
      child.stdout.once('data', () => child.kill('SIGTERM'))
      child.on('exit', (code, signal) => resolveResult({ code, signal }))
    })

    expect(result).toEqual({ code: null, signal: 'SIGTERM' })
  })
})
