#!/usr/bin/env node

import { spawn } from 'node:child_process'

import { loadLocalConfig, startLocalSyncHost, type LocalSyncHostExit } from './local.js'

function usage(): never {
  console.error(
    'usage: orez-lite dev [--config orez-lite.config.ts] -- <command> [args...]'
  )
  process.exit(1)
}

const args = process.argv.slice(2)
if (args.shift() !== 'dev') usage()

const separator = args.indexOf('--')
if (separator < 0 || separator === args.length - 1) usage()

const options = args.slice(0, separator)
let configPath = 'orez-lite.config.ts'
for (let index = 0; index < options.length; index++) {
  if (options[index] !== '--config' || !options[index + 1]) usage()
  configPath = options[index + 1]
  index++
}

const command = args.slice(separator + 1)
const config = await loadLocalConfig(configPath)
const host = await startLocalSyncHost(config)
const child = spawn(command[0], command.slice(1), {
  env: process.env,
  stdio: 'inherit',
})

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.once(signal, () => child.kill(signal))
}

const appExited = new Promise<{
  source: 'app'
  code: number | null
  signal: NodeJS.Signals | null
}>((resolveExit, rejectExit) => {
  child.once('error', rejectExit)
  child.once('exit', (code, signal) => {
    resolveExit({ source: 'app', code, signal })
  })
})
const hostExited = host.exited.then((exit: LocalSyncHostExit) => ({
  source: 'host' as const,
  ...exit,
}))

try {
  const exit = await Promise.race([appExited, hostExited])
  if (exit.source === 'host' && !exit.expected) {
    const reason = exit.signal ? `signal ${exit.signal}` : `code ${exit.code}`
    console.error(`[orez-lite] native sync host exited (${reason})`)
    child.kill('SIGTERM')
    await appExited
    process.exitCode = 1
  } else {
    process.exitCode = exit.code ?? (exit.signal ? 1 : 0)
  }
} finally {
  await host.close()
}
