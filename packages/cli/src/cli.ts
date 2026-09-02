#!/usr/bin/env node

import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'

import { parseRunArgs, runParallelScripts } from './run-all.js'

async function main(): Promise<void> {
  const [command, ...args] = process.argv.slice(2)
  if (command !== 'run-all') {
    console.error('Usage: o run-all [--pty] [--watch] [--flags=last] <script...>')
    process.exitCode = 1
    return
  }

  const usePty = args.includes('--pty')
  const filteredArgs = args.filter((arg) => arg !== '--pty')
  if (!usePty) {
    await runParallelScripts(parseRunArgs(filteredArgs))
    return
  }

  const scriptPath = fileURLToPath(new URL('./run-pty.mjs', import.meta.url))
  const child = spawn(process.execPath, [scriptPath, ...filteredArgs], {
    stdio: 'inherit',
    shell: false,
  })
  const code = await new Promise<number>((resolve) => {
    child.on('exit', (exitCode) => resolve(exitCode ?? 0))
  })
  process.exitCode = code
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : String(error))
  process.exitCode = 1
})
