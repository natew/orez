import { existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { afterEach, expect, it } from 'vitest'

import { pruneWorkerChunksBySignature } from './prune.js'

const workerDirs: string[] = []

afterEach(() => {
  for (const dir of workerDirs.splice(0)) rmSync(dir, { recursive: true, force: true })
})

it('refuses to prune a signature match statically imported by a retained module', () => {
  const workerDir = mkdtempSync(join(tmpdir(), 'orez-prune-static-import-'))
  workerDirs.push(workerDir)
  const assetsDir = join(workerDir, 'assets')
  mkdirSync(assetsDir)
  writeFileSync(join(workerDir, 'index.js'), 'import "./assets/middleware.js"\n')
  writeFileSync(
    join(assetsDir, 'middleware.js'),
    'import { catalog } from "./env.js"\nexport default catalog\n'
  )
  writeFileSync(
    join(assetsDir, 'env.js'),
    'export const catalog = { "browser-only-signature": "dependency-name" }\n'
  )

  expect(() =>
    pruneWorkerChunksBySignature(workerDir, {
      browserOnlyChunkSignature: /browser-only-signature/,
      serverNodeOnlyChunkSignatures: [],
    })
  ).toThrow(/env\.js.*statically imported by.*middleware\.js/)
  expect(existsSync(join(assetsDir, 'env.js'))).toBe(true)
})

it('prunes a signature match reached only by a dynamic import', () => {
  const workerDir = mkdtempSync(join(tmpdir(), 'orez-prune-dynamic-import-'))
  workerDirs.push(workerDir)
  const assetsDir = join(workerDir, 'assets')
  mkdirSync(assetsDir)
  writeFileSync(join(workerDir, 'index.js'), 'import("./assets/editor.js")\n')
  writeFileSync(
    join(assetsDir, 'editor.js'),
    'export const source = "browser-only-signature"\n'
  )

  expect(
    pruneWorkerChunksBySignature(workerDir, {
      browserOnlyChunkSignature: /browser-only-signature/,
      serverNodeOnlyChunkSignatures: [],
    }).removed
  ).toBe(1)
  expect(existsSync(join(assetsDir, 'editor.js'))).toBe(false)
})
