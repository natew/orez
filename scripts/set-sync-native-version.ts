#!/usr/bin/env bun

import { execFileSync } from 'node:child_process'
import { readFileSync, writeFileSync } from 'node:fs'
import { resolve } from 'node:path'

import { SYNC_NATIVE_PLATFORMS } from './sync-native-platforms.js'

const version = process.argv[2]
if (!version || !/^\d+\.\d+\.\d+$/.test(version)) {
  throw new Error('usage: set-sync-native-version.ts <major.minor.patch>')
}

const root = resolve(import.meta.dirname, '..')
const cargoPath = resolve(root, 'Cargo.toml')
const cargo = readFileSync(cargoPath, 'utf8')
const versionPattern = /(\[workspace\.package\][\s\S]*?\nversion = ")[^"]+("\n)/
if (!versionPattern.test(cargo)) {
  throw new Error('could not find workspace.package.version')
}
writeFileSync(cargoPath, cargo.replace(versionPattern, `$1${version}$2`))

for (const packageDir of [
  'packages/orez-sync-native',
  ...SYNC_NATIVE_PLATFORMS.map(({ packageDir }) => packageDir),
]) {
  const manifestPath = resolve(root, packageDir, 'package.json')
  const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'))
  manifest.version = version
  writeFileSync(manifestPath, JSON.stringify(manifest, null, 2) + '\n')
}

execFileSync('cargo', ['metadata', '--format-version=1', '--no-deps'], {
  cwd: root,
  stdio: 'ignore',
})
execFileSync('bun', ['install'], { cwd: root, stdio: 'inherit' })
console.log(`sync-native manifests are now ${version}`)
