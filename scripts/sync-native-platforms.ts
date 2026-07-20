export type SyncNativeLibc = 'glibc' | 'musl'

export interface SyncNativePlatform {
  id: string
  target: string
  runner: string
  npmPackage: string
  packageDir: string
  os: NodeJS.Platform
  cpu: NodeJS.Architecture
  libc?: SyncNativeLibc
  executable: 'sync-native' | 'sync-native.exe'
}

const platform = (
  id: string,
  target: string,
  runner: string,
  os: NodeJS.Platform,
  cpu: NodeJS.Architecture,
  libc?: SyncNativeLibc
): SyncNativePlatform => ({
  id,
  target,
  runner,
  npmPackage: `@nwienert/orez-sync-native-${id}`,
  packageDir: `packages/sync-native-${id}`,
  os,
  cpu,
  libc,
  executable: os === 'win32' ? 'sync-native.exe' : 'sync-native',
})

export const SYNC_NATIVE_PLATFORMS: readonly SyncNativePlatform[] = [
  platform('darwin-arm64', 'aarch64-apple-darwin', 'macos-15', 'darwin', 'arm64'),
  platform('darwin-x64', 'x86_64-apple-darwin', 'macos-15-intel', 'darwin', 'x64'),
  platform(
    'linux-arm64-gnu',
    'aarch64-unknown-linux-gnu',
    'ubuntu-22.04-arm',
    'linux',
    'arm64',
    'glibc'
  ),
  platform(
    'linux-arm64-musl',
    'aarch64-unknown-linux-musl',
    'ubuntu-22.04-arm',
    'linux',
    'arm64',
    'musl'
  ),
  platform(
    'linux-x64-gnu',
    'x86_64-unknown-linux-gnu',
    'ubuntu-22.04',
    'linux',
    'x64',
    'glibc'
  ),
  platform(
    'linux-x64-musl',
    'x86_64-unknown-linux-musl',
    'ubuntu-22.04',
    'linux',
    'x64',
    'musl'
  ),
  platform('win32-arm64', 'aarch64-pc-windows-msvc', 'windows-11-arm', 'win32', 'arm64'),
  platform('win32-x64', 'x86_64-pc-windows-msvc', 'windows-2025', 'win32', 'x64'),
]

export function findSyncNativePlatform(
  os: NodeJS.Platform,
  cpu: NodeJS.Architecture,
  libc?: SyncNativeLibc
): SyncNativePlatform | undefined {
  return SYNC_NATIVE_PLATFORMS.find(
    (candidate) =>
      candidate.os === os &&
      candidate.cpu === cpu &&
      (candidate.os !== 'linux' || candidate.libc === libc)
  )
}

export function syncNativeMatrix(): string {
  return JSON.stringify({ include: SYNC_NATIVE_PLATFORMS })
}

export function renderSyncNativeShim(): string {
  const packages = SYNC_NATIVE_PLATFORMS.map(
    ({ os, cpu, libc, npmPackage, executable }) =>
      `  '${os}-${cpu}${libc ? `-${libc}` : ''}': '${npmPackage}/bin/${executable}',`
  ).join('\n')

  return `#!/usr/bin/env node
'use strict'

const { createRequire } = require('node:module')
const { spawn } = require('node:child_process')

const PACKAGES = {
${packages}
}

const libc =
  process.platform === 'linux'
    ? process.report?.getReport().header.glibcVersionRuntime
      ? 'glibc'
      : 'musl'
    : undefined
const key = [process.platform, process.arch, libc].filter(Boolean).join('-')
const packageBinary = PACKAGES[key]

if (!packageBinary) {
  console.error(\`sync-native does not support \${key}\`)
  process.exit(1)
}

let binary
try {
  binary = createRequire(__filename).resolve(packageBinary)
} catch (error) {
  console.error(
    \`sync-native binary package for \${key} is missing. \` +
      'Reinstall orez-sync-native without omitting optional dependencies.'
  )
  process.exit(1)
}

const child = spawn(binary, process.argv.slice(2), { stdio: 'inherit' })
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, () => child.kill(signal))
}
child.on('error', (error) => {
  throw error
})
child.on('exit', (code, signal) => {
  if (!signal) {
    process.exitCode = code ?? 1
    return
  }
  process.removeAllListeners(signal)
  process.kill(process.pid, signal)
})
`
}
