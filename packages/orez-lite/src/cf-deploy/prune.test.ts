import { existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { afterEach, expect, it } from 'vitest'

import {
  assertWorkerStaticModuleImportsResolve,
  pruneWorkerChunksBySignature,
} from './prune.js'

const workerDirs: string[] = []

afterEach(() => {
  for (const dir of workerDirs.splice(0)) rmSync(dir, { recursive: true, force: true })
})

it('refuses to prune a signature match statically imported by a worker root module', () => {
  const workerDir = mkdtempSync(join(tmpdir(), 'orez-prune-static-import-'))
  workerDirs.push(workerDir)
  const assetsDir = join(workerDir, 'assets')
  mkdirSync(assetsDir)
  writeFileSync(
    join(workerDir, 'index.js'),
    'import { catalog } from "./assets/env.js"\n'
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
  ).toThrow(/env\.js.*static dependency chain.*index\.js/)
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

it('prunes asset wrappers that statically depend on a signature match', () => {
  const workerDir = mkdtempSync(join(tmpdir(), 'orez-prune-static-wrapper-'))
  workerDirs.push(workerDir)
  const assetsDir = join(workerDir, 'assets')
  mkdirSync(assetsDir)
  writeFileSync(join(workerDir, 'index.js'), 'import("./assets/editor.js")\n')
  writeFileSync(
    join(assetsDir, 'editor.js'),
    'export { source } from "./editor-implementation.js"\n'
  )
  writeFileSync(
    join(assetsDir, 'editor-implementation.js'),
    'export const source = "browser-only-signature"\n'
  )

  expect(
    pruneWorkerChunksBySignature(workerDir, {
      browserOnlyChunkSignature: /browser-only-signature/,
      serverNodeOnlyChunkSignatures: [],
    }).removed
  ).toBe(2)
  expect(existsSync(join(assetsDir, 'editor.js'))).toBe(false)
  expect(existsSync(join(assetsDir, 'editor-implementation.js'))).toBe(false)
  expect(() => assertWorkerStaticModuleImportsResolve(workerDir)).not.toThrow()
})

it('refuses a signature prune whose static dependent reaches a worker root', () => {
  const workerDir = mkdtempSync(join(tmpdir(), 'orez-prune-root-wrapper-'))
  workerDirs.push(workerDir)
  const assetsDir = join(workerDir, 'assets')
  mkdirSync(assetsDir)
  writeFileSync(join(workerDir, 'index.js'), 'import "./assets/runtime.js"\n')
  writeFileSync(
    join(assetsDir, 'runtime.js'),
    'export { runtime } from "./environment.js"\n'
  )
  writeFileSync(
    join(assetsDir, 'environment.js'),
    'export const runtime = "browser-only-signature"\n'
  )

  expect(() =>
    pruneWorkerChunksBySignature(workerDir, {
      browserOnlyChunkSignature: /browser-only-signature/,
      serverNodeOnlyChunkSignatures: [],
    })
  ).toThrow(/environment\.js.*runtime\.js.*index\.js/)
  expect(existsSync(join(assetsDir, 'runtime.js'))).toBe(true)
  expect(existsSync(join(assetsDir, 'environment.js'))).toBe(true)
})

it('reports a retained static import whose target is absent', () => {
  const workerDir = mkdtempSync(join(tmpdir(), 'orez-prune-missing-static-'))
  workerDirs.push(workerDir)
  const assetsDir = join(workerDir, 'assets')
  mkdirSync(assetsDir)
  writeFileSync(join(workerDir, 'index.js'), 'import "./assets/runtime.js"\n')
  writeFileSync(join(assetsDir, 'runtime.js'), 'export { runtime } from "./missing.js"\n')

  expect(() => assertWorkerStaticModuleImportsResolve(workerDir)).toThrow(
    /assets\/runtime\.js.*\.\/missing\.js/
  )
})
