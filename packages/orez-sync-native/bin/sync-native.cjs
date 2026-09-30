#!/usr/bin/env node
'use strict'

const { createRequire } = require('node:module')
const { spawn } = require('node:child_process')
const { assertPortAvailable } = require('./port-check.cjs')

const PACKAGES = {
  'darwin-arm64': {
    packageName: 'orez-sync-native-darwin-arm64',
    binary: 'orez-sync-native-darwin-arm64/bin/sync-native',
  },
  'darwin-x64': {
    packageName: 'orez-sync-native-darwin-x64',
    binary: 'orez-sync-native-darwin-x64/bin/sync-native',
  },
  'linux-arm64-glibc': {
    packageName: 'orez-sync-native-linux-arm64-gnu',
    binary: 'orez-sync-native-linux-arm64-gnu/bin/sync-native',
  },
  'linux-arm64-musl': {
    packageName: 'orez-sync-native-linux-arm64-musl',
    binary: 'orez-sync-native-linux-arm64-musl/bin/sync-native',
  },
  'linux-x64-glibc': {
    packageName: 'orez-sync-native-linux-x64-gnu',
    binary: 'orez-sync-native-linux-x64-gnu/bin/sync-native',
  },
  'linux-x64-musl': {
    packageName: 'orez-sync-native-linux-x64-musl',
    binary: 'orez-sync-native-linux-x64-musl/bin/sync-native',
  },
  'win32-arm64': {
    packageName: 'orez-sync-native-win32-arm64',
    binary: 'orez-sync-native-win32-arm64/bin/sync-native.exe',
  },
  'win32-x64': {
    packageName: 'orez-sync-native-win32-x64',
    binary: 'orez-sync-native-win32-x64/bin/sync-native.exe',
  },
}

const libc =
  process.platform === 'linux'
    ? process.report?.getReport().header.glibcVersionRuntime
      ? 'glibc'
      : 'musl'
    : undefined
const key = [process.platform, process.arch, libc].filter(Boolean).join('-')
const selected = PACKAGES[key]

if (!selected) {
  console.error(`sync-native does not support ${key}`)
  process.exit(1)
}

const load = createRequire(__filename)
let binary
try {
  const launcherVersion = load('../package.json').version
  const platformVersion = load(`${selected.packageName}/package.json`).version
  if (platformVersion !== launcherVersion) {
    console.error(
      `sync-native binary package for ${key} is ${platformVersion}, expected ${launcherVersion}. ` +
        'Reinstall orez-sync-native so its optional dependencies match.'
    )
    process.exit(1)
  }
  binary = load.resolve(selected.binary)
} catch (error) {
  console.error(
    `sync-native binary package for ${key} is missing. ` +
      'Reinstall orez-sync-native without omitting optional dependencies.'
  )
  process.exit(1)
}

async function main() {
  const args = process.argv.slice(2)
  const parentPid = Number(process.env.OREZ_SYNC_NATIVE_PARENT_PID ?? process.ppid)
  const parentAlive = () => {
    if (process.ppid !== parentPid) return false
    try {
      process.kill(parentPid, 0)
      return true
    } catch (error) {
      if (error.code === 'ESRCH') return false
      if (error.code === 'EPERM') return true
      throw error
    }
  }
  let child
  let stopping = false
  let forceExit
  const stop = (signal = 'SIGTERM') => {
    if (stopping) return
    stopping = true
    if (!child) process.exit(1)
    child.kill(signal)
    forceExit = setTimeout(() => child.kill('SIGKILL'), 2000)
  }
  for (const signal of ['SIGINT', 'SIGTERM']) {
    process.on(signal, () => stop(signal))
  }
  // reparenting also catches SIGKILL, which cannot run the parent's cleanup.
  const parentWatch = setInterval(() => {
    if (!parentAlive()) stop()
  }, 100)
  try {
    if (args[0] === 'serve') {
      const portIndex = args.indexOf('--port')
      const hostIndex = args.indexOf('--host')
      if (portIndex >= 0) {
        await assertPortAvailable(
          Number(args[portIndex + 1]),
          hostIndex >= 0 ? args[hostIndex + 1] : undefined
        )
      }
    }
    if (!parentAlive()) return stop()
    child = spawn(binary, args, {
      // the pipe is owned only by this launcher. EOF kills the native host
      // even if the launcher itself is killed before it can forward a signal.
      stdio: ['pipe', 'inherit', 'inherit'],
      env: { ...process.env, OREZ_SYNC_NATIVE_PARENT_PIPE: '1' },
    })
    child.on('error', (error) => {
      console.error(`sync-native could not start: ${error.message}`)
      clearInterval(parentWatch)
      clearTimeout(forceExit)
      process.exitCode = 1
    })
    child.on('exit', (code, signal) => {
      clearInterval(parentWatch)
      clearTimeout(forceExit)
      if (!signal) {
        process.exitCode = code ?? 1
        return
      }
      process.removeAllListeners(signal)
      process.kill(process.pid, signal)
    })
  } catch (error) {
    clearInterval(parentWatch)
    console.error(`sync-native: ${error.message}`)
    process.exitCode = 1
  }
}

void main()
