#!/usr/bin/env node
'use strict'

const { createRequire } = require('node:module')
const { spawn } = require('node:child_process')

const PACKAGES = {
  'darwin-arm64': '@nwienert/orez-sync-native-darwin-arm64/bin/sync-native',
  'darwin-x64': '@nwienert/orez-sync-native-darwin-x64/bin/sync-native',
  'linux-arm64-glibc': '@nwienert/orez-sync-native-linux-arm64-gnu/bin/sync-native',
  'linux-arm64-musl': '@nwienert/orez-sync-native-linux-arm64-musl/bin/sync-native',
  'linux-x64-glibc': '@nwienert/orez-sync-native-linux-x64-gnu/bin/sync-native',
  'linux-x64-musl': '@nwienert/orez-sync-native-linux-x64-musl/bin/sync-native',
  'win32-arm64': '@nwienert/orez-sync-native-win32-arm64/bin/sync-native.exe',
  'win32-x64': '@nwienert/orez-sync-native-win32-x64/bin/sync-native.exe',
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
  console.error(`sync-native does not support ${key}`)
  process.exit(1)
}

let binary
try {
  binary = createRequire(__filename).resolve(packageBinary)
} catch (error) {
  console.error(
    `sync-native binary package for ${key} is missing. ` +
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
