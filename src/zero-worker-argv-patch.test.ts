import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'

import { afterEach, describe, expect, it } from 'vitest'

import {
  applyZeroMainArgvPatch,
  applyZeroWorkerArgvPatch,
} from './zero-worker-argv-patch.js'

let tmpDirs: string[] = []

afterEach(() => {
  for (const dir of tmpDirs) {
    rmSync(dir, { recursive: true, force: true })
  }
  tmpDirs = []
})

describe('applyZeroWorkerArgvPatch', () => {
  it('inherits host arguments only for forked workers', () => {
    const file = writeProcesses(`
function childWorker(moduleUrl, env, ...args) {
\targs.push(...process.argv.slice(2));
\tif (singleProcessMode()) {
\t\treturn runWorker(...args);
\t}
\treturn wrap(fork(moduleUrl, args, {
\t\tenv
\t}));
}
`)

    applyZeroWorkerArgvPatch(file)

    const patched = readFileSync(file, 'utf-8')
    const singleProcessIndex = patched.indexOf('if (singleProcessMode())')
    const hostArgvIndex = patched.indexOf('args.push(...process.argv.slice(2))')
    const forkIndex = patched.indexOf('return wrap(fork')

    expect(patched).toContain('/* orez: host argv belongs only to forked workers */')
    expect(hostArgvIndex).toBeGreaterThan(singleProcessIndex)
    expect(hostArgvIndex).toBeLessThan(forkIndex)
  })

  it('is idempotent', () => {
    const file = writeProcesses(`
function childWorker(moduleUrl, env, ...args) {
\targs.push(...process.argv.slice(2));
\tif (singleProcessMode()) {
\t\treturn runWorker(...args);
\t}
\treturn wrap(fork(moduleUrl, args, { env }));
}
`)

    applyZeroWorkerArgvPatch(file)
    applyZeroWorkerArgvPatch(file)

    const patched = readFileSync(file, 'utf-8')
    expect(patched.match(/host argv belongs only to forked workers/g)).toHaveLength(1)
  })

  it('fails loudly when zero-cache changes the worker shape', () => {
    const file = writeProcesses('function childWorker() {}')

    expect(() => applyZeroWorkerArgvPatch(file)).toThrow(
      'could not isolate embedded zero-cache worker arguments'
    )
  })
})

describe('applyZeroMainArgvPatch', () => {
  it('ignores host arguments only in single-process mode', () => {
    const file = writeMain(`
async function runWorker(parent, env) {
\tconst config = getNormalizedZeroConfig({ env });
\treturn config;
}
`)

    applyZeroMainArgvPatch(file)

    const patched = readFileSync(file, 'utf-8')
    expect(patched).toContain('singleProcessMode() ? { env, argv: [] } : { env }')
  })

  it('is idempotent', () => {
    const file = writeMain(`
async function runWorker(parent, env) {
\tconst config = getNormalizedZeroConfig({ env });
}
`)

    applyZeroMainArgvPatch(file)
    applyZeroMainArgvPatch(file)

    const patched = readFileSync(file, 'utf-8')
    expect(patched.match(/embedded main ignores host argv/g)).toHaveLength(1)
  })

  it('fails loudly when zero-cache changes the main worker shape', () => {
    const file = writeMain('async function runWorker() {}')

    expect(() => applyZeroMainArgvPatch(file)).toThrow(
      'could not isolate embedded zero-cache main arguments'
    )
  })
})

function writeProcesses(content: string): string {
  const dir = mkdtempSync(join(tmpdir(), 'orez-worker-argv-patch-'))
  tmpDirs.push(dir)
  const file = resolve(dir, 'processes.js')
  writeFileSync(file, content)
  return file
}

function writeMain(content: string): string {
  const dir = mkdtempSync(join(tmpdir(), 'orez-main-argv-patch-'))
  tmpDirs.push(dir)
  const file = resolve(dir, 'main.js')
  writeFileSync(file, content)
  return file
}
