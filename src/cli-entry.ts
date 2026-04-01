#!/usr/bin/env node

// thin wrapper that ensures the orez process has enough heap for pglite wasm.
// calculates ~50% of system memory and re-execs with --max-old-space-size if needed.

import { spawn } from 'node:child_process'
import { totalmem } from 'node:os'
import { resolve, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'

import { orezTitle } from './process-title.js'

process.title = orezTitle()

const currentOpts = process.env.NODE_OPTIONS || ''

if (!currentOpts.includes('--max-old-space-size') && !process.env.__OREZ_SPAWNED) {
  const memMB = Math.round(totalmem() / 1024 / 1024)
  const heapMB = Math.max(4096, Math.round(memMB * 0.5))
  const cliPath = resolve(dirname(fileURLToPath(import.meta.url)), 'cli.js')

  // enable --experimental-strip-types so orez.config.ts works on Node 22.6+
  // (Node 23.6+ has it by default; harmless if already enabled)
  let nodeOpts = `--max-old-space-size=${heapMB} ${currentOpts}`.trim()
  if (!nodeOpts.includes('--experimental-strip-types')) {
    const [major, minor] = process.versions.node.split('.').map(Number)
    if (major > 22 || (major === 22 && minor >= 6)) {
      nodeOpts = `--experimental-strip-types ${nodeOpts}`
    }
  }

  const child = spawn(process.execPath, [cliPath, ...process.argv.slice(2)], {
    env: {
      ...process.env,
      NODE_OPTIONS: nodeOpts,
      __OREZ_SPAWNED: '1',
    },
    stdio: 'inherit',
  })

  // forward signals to child
  for (const sig of ['SIGINT', 'SIGTERM'] as const) {
    process.on(sig, () => child.kill(sig))
  }

  child.on('exit', (code, signal) => {
    if (signal) process.kill(process.pid, signal)
    else process.exit(code ?? 1)
  })
} else {
  // already have heap configured, run cli directly
  await import('./cli.js')
}
