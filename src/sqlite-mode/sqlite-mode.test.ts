/**
 * sqlite-mode tests - mode resolution, shim generation, and mode transitions
 */

import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import {
  applySqliteMode,
  backupOriginal,
  cleanupShim,
  getShimMode,
  hasBackup,
  restoreOriginal,
} from './apply-mode.js'
import { resolveSqliteMode, resolveSqliteModeConfig } from './resolve-mode.js'
import { generateCjsShim, generateEsmShim } from './shim-template.js'
import { BACKUP_MARKER, JOURNAL_MODE } from './types.js'

describe('sqlite mode types', () => {
  it('journal_mode differs between native and wasm', () => {
    expect(JOURNAL_MODE.native).toBe('wal2')
    expect(JOURNAL_MODE.wasm).toBe('delete')
  })
})

describe('sqlite mode resolution', () => {
  it('resolves wasm mode when disableWasmSqlite is false', () => {
    expect(resolveSqliteMode(false)).toBe('wasm')
  })

  it('resolves native mode when disableWasmSqlite is true', () => {
    expect(resolveSqliteMode(true)).toBe('native')
  })

  it('returns config with mode for native', () => {
    const config = resolveSqliteModeConfig(true)
    expect(config).not.toBeNull()
    expect(config?.mode).toBe('native')
  })
})

describe('shim template generation', () => {
  it('generates cjs shim with correct journal_mode for wasm', () => {
    const shim = generateCjsShim({
      mode: 'wasm',
      bedrockPath: '/path/to/bedrock',
    })
    expect(shim).toContain("db.pragma('journal_mode = delete')")
    expect(shim).toContain('// mode: wasm')
    expect(shim).toContain('orez sqlite shim')
  })

  it('generates cjs shim with correct journal_mode for native', () => {
    const shim = generateCjsShim({
      mode: 'native',
      bedrockPath: '/path/to/bedrock',
    })
    expect(shim).toContain("db.pragma('journal_mode = wal2')")
    expect(shim).toContain('// mode: native')
  })

  it('generates esm shim with correct journal_mode for wasm', () => {
    const shim = generateEsmShim({
      mode: 'wasm',
      bedrockPath: '/path/to/bedrock',
    })
    expect(shim).toContain("db.pragma('journal_mode = delete')")
    expect(shim).toContain('// mode: wasm')
  })

  it('includes common pragmas in both shim types', () => {
    const cjs = generateCjsShim({ mode: 'wasm', bedrockPath: '/path' })
    const esm = generateEsmShim({ mode: 'wasm', bedrockPath: '/path' })

    for (const shim of [cjs, esm]) {
      expect(shim).toContain("db.pragma('busy_timeout = 30000')")
      expect(shim).toContain("db.pragma('synchronous = normal')")
    }
  })

  it('includes api polyfills in generated shims', () => {
    const shim = generateCjsShim({ mode: 'wasm', bedrockPath: '/path' })

    expect(shim).toContain('Database.prototype.unsafeMode')
    expect(shim).toContain('Database.prototype.defaultSafeIntegers')
    expect(shim).toContain('Database.prototype.serialize')
    expect(shim).toContain('Database.prototype.backup')
    expect(shim).toContain('SP.scanStatus')
    expect(shim).toContain('SP.scanStatusV2')
    expect(shim).toContain('SQLITE_SCANSTAT_NLOOP')
  })

  it('includes pragma wrapper to skip optimize', () => {
    const shim = generateCjsShim({ mode: 'wasm', bedrockPath: '/path' })
    expect(shim).toContain("str.trim().toLowerCase().startsWith('optimize')")
  })

  it('includes tracing when enabled', () => {
    const withTracing = generateCjsShim({
      mode: 'wasm',
      bedrockPath: '/path',
      includeTracing: true,
    })
    const withoutTracing = generateCjsShim({
      mode: 'wasm',
      bedrockPath: '/path',
      includeTracing: false,
    })

    expect(withTracing).toContain('_zero.changeLog')
    expect(withoutTracing).not.toContain('_zero.changeLog')
  })
})

describe('shim backup/restore lifecycle', () => {
  let testDir: string
  let mockPackageDir: string
  let mockIndexPath: string

  const originalContent = `'use strict';
// original @rocicorp/zero-sqlite3 content
module.exports = require('better-sqlite3');
`

  beforeEach(() => {
    testDir = join(tmpdir(), `orez-test-${Date.now()}-${Math.random().toString(36).slice(2)}`)
    mockPackageDir = join(testDir, 'node_modules', '@rocicorp', 'zero-sqlite3', 'lib')
    mkdirSync(mockPackageDir, { recursive: true })
    mockIndexPath = join(mockPackageDir, 'index.js')

    // create mock package.json
    writeFileSync(
      join(testDir, 'node_modules', '@rocicorp', 'zero-sqlite3', 'package.json'),
      JSON.stringify({ name: '@rocicorp/zero-sqlite3', main: 'lib/index.js' })
    )

    // create original index.js
    writeFileSync(mockIndexPath, originalContent)
  })

  afterEach(() => {
    rmSync(testDir, { recursive: true, force: true })
  })

  it('backups original file before shimming', () => {
    expect(hasBackup(mockIndexPath)).toBe(false)
    expect(backupOriginal(mockIndexPath)).toBe(true)
    expect(hasBackup(mockIndexPath)).toBe(true)

    const backupContent = readFileSync(mockIndexPath + BACKUP_MARKER, 'utf-8')
    expect(backupContent).toBe(originalContent)
  })

  it('does not re-backup if backup already exists', () => {
    backupOriginal(mockIndexPath)
    const backupPath = mockIndexPath + BACKUP_MARKER

    // modify original (simulate shim)
    writeFileSync(mockIndexPath, 'shimmed content')

    // backup should not overwrite existing backup
    expect(backupOriginal(mockIndexPath)).toBe(true)
    expect(readFileSync(backupPath, 'utf-8')).toBe(originalContent)
  })

  it('restores original from backup', () => {
    backupOriginal(mockIndexPath)
    writeFileSync(mockIndexPath, 'shimmed content')

    expect(restoreOriginal(mockIndexPath)).toBe(true)
    expect(readFileSync(mockIndexPath, 'utf-8')).toBe(originalContent)
    expect(hasBackup(mockIndexPath)).toBe(false)
  })

  it('returns false when no backup to restore', () => {
    expect(restoreOriginal(mockIndexPath)).toBe(false)
  })

  it('getShimMode returns null for unshimmed file', () => {
    expect(getShimMode(mockIndexPath)).toBeNull()
  })

  it('getShimMode returns mode for shimmed file', () => {
    const shimContent = generateCjsShim({
      mode: 'wasm',
      bedrockPath: '/path/to/bedrock',
    })
    writeFileSync(mockIndexPath, shimContent)

    expect(getShimMode(mockIndexPath)).toBe('wasm')
  })

  it('cleanupShim restores original if backup exists', () => {
    backupOriginal(mockIndexPath)
    writeFileSync(mockIndexPath, 'shimmed')

    cleanupShim(
      join(testDir, 'node_modules', '@rocicorp', 'zero-sqlite3', 'lib', 'index.js')
    )

    expect(readFileSync(mockIndexPath, 'utf-8')).toBe(originalContent)
  })
})

describe('mode transitions', () => {
  let testDir: string
  let mockPackageDir: string
  let mockIndexPath: string

  const originalContent = `'use strict';
// original @rocicorp/zero-sqlite3
module.exports = require('better-sqlite3');
`

  beforeEach(() => {
    testDir = join(tmpdir(), `orez-test-${Date.now()}-${Math.random().toString(36).slice(2)}`)
    mockPackageDir = join(testDir, 'node_modules', '@rocicorp', 'zero-sqlite3', 'lib')
    mkdirSync(mockPackageDir, { recursive: true })
    mockIndexPath = join(mockPackageDir, 'index.js')

    writeFileSync(
      join(testDir, 'node_modules', '@rocicorp', 'zero-sqlite3', 'package.json'),
      JSON.stringify({ name: '@rocicorp/zero-sqlite3', main: 'lib/index.js' })
    )
    writeFileSync(mockIndexPath, originalContent)
  })

  afterEach(() => {
    rmSync(testDir, { recursive: true, force: true })
  })

  it('native -> wasm -> native preserves original', () => {
    // start in native mode (no shim)
    expect(getShimMode(mockIndexPath)).toBeNull()

    // switch to wasm mode
    const wasmResult = applySqliteMode({
      mode: 'wasm',
      bedrockPath: '/path/to/bedrock',
      zeroSqlitePath: mockIndexPath,
    })
    expect(wasmResult.success).toBe(true)
    expect(getShimMode(mockIndexPath)).toBe('wasm')
    expect(hasBackup(mockIndexPath)).toBe(true)

    // switch back to native mode
    const nativeResult = applySqliteMode({
      mode: 'native',
      zeroSqlitePath: mockIndexPath,
    })
    expect(nativeResult.success).toBe(true)
    expect(getShimMode(mockIndexPath)).toBeNull()
    expect(readFileSync(mockIndexPath, 'utf-8')).toBe(originalContent)
    expect(hasBackup(mockIndexPath)).toBe(false)
  })

  it('wasm -> native -> wasm works correctly', () => {
    // start in wasm mode
    applySqliteMode({
      mode: 'wasm',
      bedrockPath: '/path/to/bedrock',
      zeroSqlitePath: mockIndexPath,
    })
    expect(getShimMode(mockIndexPath)).toBe('wasm')
    const wasmContent = readFileSync(mockIndexPath, 'utf-8')

    // switch to native
    applySqliteMode({
      mode: 'native',
      zeroSqlitePath: mockIndexPath,
    })
    expect(getShimMode(mockIndexPath)).toBeNull()

    // switch back to wasm
    applySqliteMode({
      mode: 'wasm',
      bedrockPath: '/path/to/bedrock',
      zeroSqlitePath: mockIndexPath,
    })
    expect(getShimMode(mockIndexPath)).toBe('wasm')

    // shim should have same journal_mode
    const newWasmContent = readFileSync(mockIndexPath, 'utf-8')
    expect(newWasmContent).toContain("journal_mode = delete")
  })

  it('multiple wasm applies are idempotent', () => {
    applySqliteMode({
      mode: 'wasm',
      bedrockPath: '/path/to/bedrock',
      zeroSqlitePath: mockIndexPath,
    })
    const firstShim = readFileSync(mockIndexPath, 'utf-8')

    // apply again
    applySqliteMode({
      mode: 'wasm',
      bedrockPath: '/path/to/bedrock',
      zeroSqlitePath: mockIndexPath,
    })
    const secondShim = readFileSync(mockIndexPath, 'utf-8')

    expect(firstShim).toBe(secondShim)
    expect(hasBackup(mockIndexPath)).toBe(true)
  })

  it('does not backup shimmed content', () => {
    // apply wasm shim
    applySqliteMode({
      mode: 'wasm',
      bedrockPath: '/path/to/bedrock',
      zeroSqlitePath: mockIndexPath,
    })

    // manually delete backup to simulate corruption
    const backupPath = mockIndexPath + BACKUP_MARKER
    rmSync(backupPath)

    // try to backup again - should fail because file is shimmed
    expect(backupOriginal(mockIndexPath)).toBe(false)
  })

  it('wasm re-apply is idempotent even without backup', () => {
    // apply wasm shim first
    applySqliteMode({
      mode: 'wasm',
      bedrockPath: '/path/to/bedrock',
      zeroSqlitePath: mockIndexPath,
    })

    // delete backup to simulate corruption
    const backupPath = mockIndexPath + BACKUP_MARKER
    rmSync(backupPath)

    // re-applying same wasm mode should succeed (idempotent)
    const result = applySqliteMode({
      mode: 'wasm',
      bedrockPath: '/path/to/bedrock',
      zeroSqlitePath: mockIndexPath,
    })
    expect(result.success).toBe(true)
  })

  it('wasm shim fails on unshimmed file if backup cannot be created', () => {
    // manually write a shimmed file without backup (simulates external corruption)
    const shimContent = generateCjsShim({ mode: 'native', bedrockPath: '/path' })
    writeFileSync(mockIndexPath, shimContent)

    // try to apply wasm shim - should fail because file is shimmed but no backup
    const result = applySqliteMode({
      mode: 'wasm',
      bedrockPath: '/path/to/bedrock',
      zeroSqlitePath: mockIndexPath,
    })
    expect(result.success).toBe(false)
    expect(result.error).toContain('no backup')
  })

  it('native mode fails if shimmed with no backup', () => {
    // apply wasm shim first
    applySqliteMode({
      mode: 'wasm',
      bedrockPath: '/path/to/bedrock',
      zeroSqlitePath: mockIndexPath,
    })

    // delete backup to simulate corruption
    const backupPath = mockIndexPath + BACKUP_MARKER
    rmSync(backupPath)

    // try to restore native - should fail
    const result = applySqliteMode({
      mode: 'native',
      zeroSqlitePath: mockIndexPath,
    })
    expect(result.success).toBe(false)
    expect(result.error).toContain('no backup')
  })
})

describe('shim contract tests', () => {
  it('wasm shim sets journal_mode = delete', () => {
    const shim = generateCjsShim({ mode: 'wasm', bedrockPath: '/path' })
    expect(shim).toContain("db.pragma('journal_mode = delete')")
    expect(shim).not.toContain("journal_mode = wal2")
  })

  it('native shim sets journal_mode = wal2', () => {
    const shim = generateCjsShim({ mode: 'native', bedrockPath: '/path' })
    expect(shim).toContain("db.pragma('journal_mode = wal2')")
    expect(shim).not.toContain("journal_mode = delete")
  })

  it('both modes set busy_timeout and synchronous', () => {
    for (const mode of ['wasm', 'native'] as const) {
      const shim = generateCjsShim({ mode, bedrockPath: '/path' })
      expect(shim).toContain("db.pragma('busy_timeout = 30000')")
      expect(shim).toContain("db.pragma('synchronous = normal')")
    }
  })

  it('shim exports Database and SqliteError', () => {
    const shim = generateCjsShim({ mode: 'wasm', bedrockPath: '/path' })
    expect(shim).toContain('module.exports = Database')
    expect(shim).toContain('module.exports.SqliteError = SqliteError')
  })

  it('esm shim exports default and named SqliteError', () => {
    const shim = generateEsmShim({ mode: 'wasm', bedrockPath: '/path' })
    expect(shim).toContain('export default Database')
    expect(shim).toContain('export { SqliteError }')
  })
})
