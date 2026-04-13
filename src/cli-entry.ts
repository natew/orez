#!/usr/bin/env node

// by default runs cli.js in-process. when --bump-heap (or OREZ_BUMP_HEAP=1)
// is set, re-execs with --max-old-space-size at ~50% of system memory —
// needed for pglite wasm workloads. default off since memory usage was reduced.

import { spawn } from 'node:child_process'
import { totalmem } from 'node:os'
import { resolve, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'

import { orezTitle } from './process-title.js'

const currentOpts = process.env.NODE_OPTIONS || ''
const bumpHeapRequested =
  process.argv.includes('--bump-heap') ||
  process.env.OREZ_BUMP_HEAP === '1' ||
  process.env.OREZ_BUMP_HEAP === 'true'

const willRespawn =
  bumpHeapRequested &&
  !currentOpts.includes('--max-old-space-size') &&
  !process.env.__OREZ_SPAWNED

// label the wrapper distinctly so it's not confused with the real orez process
process.title = willRespawn ? orezTitle('orez [wrapper]') : orezTitle()

if (willRespawn) {
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

  // strip --bump-heap from argv so cli.ts doesn't see it as an unknown flag
  const childArgs = process.argv.slice(2).filter((a) => a !== '--bump-heap')

  const child = spawn(process.execPath, [cliPath, ...childArgs], {
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
  // run cli directly — no heap bump, no wrapper process
  // strip --bump-heap in case it was passed but we're already the spawned child
  if (process.argv.includes('--bump-heap')) {
    process.argv = process.argv.filter((a) => a !== '--bump-heap')
  }
  const [{ runMain }, { main }] = await Promise.all([import('citty'), import('./cli.js')])
  runMain(main)
}
