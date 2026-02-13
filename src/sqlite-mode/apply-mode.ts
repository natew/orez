/**
 * apply sqlite mode - handles shim installation with backup/restore lifecycle
 *
 * this module manages the in-place patching of @rocicorp/zero-sqlite3 with proper
 * backup/restore to prevent mode contamination when switching between wasm and native.
 */

import { existsSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { resolve } from 'node:path'

import { generateCjsShim } from './shim-template.js'
import { BACKUP_MARKER, type SqliteMode, type SqliteModeConfig } from './types.js'

interface ApplyResult {
  success: boolean
  shimPath?: string
  error?: string
}

/**
 * find the index.js file to shim in @rocicorp/zero-sqlite3
 */
function findZeroSqliteIndex(zeroSqlitePath: string): string | null {
  // find package root (contains package.json)
  let dir = zeroSqlitePath
  while (dir && !existsSync(resolve(dir, 'package.json'))) {
    const parent = resolve(dir, '..')
    if (parent === dir) break
    dir = parent
  }

  // try lib/index.js first, then root index.js
  const libIndex = resolve(dir, 'lib', 'index.js')
  if (existsSync(libIndex)) return libIndex

  const rootIndex = resolve(dir, 'index.js')
  if (existsSync(rootIndex)) return rootIndex

  return null
}

/**
 * get backup path for an index file
 */
function getBackupPath(indexPath: string): string {
  return indexPath + BACKUP_MARKER
}

/**
 * check if a file is already shimmed by orez
 */
function isOrezShimmed(indexPath: string): boolean {
  if (!existsSync(indexPath)) return false
  const content = readFileSync(indexPath, 'utf-8')
  return content.includes('orez sqlite shim')
}

/**
 * check what mode an existing shim is configured for
 */
export function getShimMode(indexPath: string): SqliteMode | null {
  if (!existsSync(indexPath)) return null
  const content = readFileSync(indexPath, 'utf-8')
  if (!content.includes('orez sqlite shim')) return null

  // extract mode from comment: "// mode: wasm, journal_mode: delete"
  const match = content.match(/\/\/ mode: (native|wasm),/)
  return match ? (match[1] as SqliteMode) : null
}

/**
 * backup the original @rocicorp/zero-sqlite3 index.js
 * only backs up if not already backed up and not already shimmed
 */
export function backupOriginal(indexPath: string): boolean {
  const backupPath = getBackupPath(indexPath)

  // already have a backup
  if (existsSync(backupPath)) return true

  // don't backup if file is already shimmed (would backup the shim)
  if (isOrezShimmed(indexPath)) {
    return false
  }

  if (!existsSync(indexPath)) return false

  const content = readFileSync(indexPath, 'utf-8')
  writeFileSync(backupPath, content)
  return true
}

/**
 * restore the original @rocicorp/zero-sqlite3 from backup
 */
export function restoreOriginal(indexPath: string): boolean {
  const backupPath = getBackupPath(indexPath)

  if (!existsSync(backupPath)) {
    // no backup to restore from
    return false
  }

  const content = readFileSync(backupPath, 'utf-8')
  writeFileSync(indexPath, content)
  rmSync(backupPath)
  return true
}

/**
 * check if backup exists
 */
export function hasBackup(indexPath: string): boolean {
  return existsSync(getBackupPath(indexPath))
}

/**
 * apply wasm mode shim to @rocicorp/zero-sqlite3
 * backs up original first, writes shim in place
 */
export function applyWasmShim(config: SqliteModeConfig): ApplyResult {
  if (config.mode !== 'wasm') {
    return { success: false, error: 'applyWasmShim called with non-wasm mode' }
  }

  if (!config.zeroSqlitePath) {
    return { success: false, error: '@rocicorp/zero-sqlite3 not found' }
  }

  if (!config.bedrockPath) {
    return { success: false, error: 'bedrock-sqlite not found' }
  }

  const indexPath = findZeroSqliteIndex(config.zeroSqlitePath)
  if (!indexPath) {
    return { success: false, error: 'could not find @rocicorp/zero-sqlite3 index.js' }
  }

  // check if already shimmed for wasm mode
  const existingMode = getShimMode(indexPath)
  if (existingMode === 'wasm') {
    return { success: true, shimPath: indexPath }
  }

  // backup original before shimming - must succeed
  const backedUp = backupOriginal(indexPath)
  if (!backedUp && !hasBackup(indexPath)) {
    // file is shimmed (possibly by another mode) but no backup exists
    // cannot safely proceed - would lose ability to restore
    return {
      success: false,
      error: 'cannot apply wasm shim: file is already shimmed with no backup. reinstall @rocicorp/zero-sqlite3',
    }
  }

  // generate and write shim
  const shimCode = generateCjsShim({
    mode: 'wasm',
    bedrockPath: config.bedrockPath,
    includeTracing: false,
  })

  writeFileSync(indexPath, shimCode)

  return { success: true, shimPath: indexPath }
}

/**
 * restore native mode by removing shim and restoring original
 */
export function restoreNativeMode(zeroSqlitePath: string): ApplyResult {
  const indexPath = findZeroSqliteIndex(zeroSqlitePath)
  if (!indexPath) {
    return { success: false, error: 'could not find @rocicorp/zero-sqlite3 index.js' }
  }

  // check if currently shimmed
  if (!isOrezShimmed(indexPath)) {
    // not shimmed, nothing to do
    return { success: true }
  }

  // restore from backup
  if (restoreOriginal(indexPath)) {
    return { success: true }
  }

  // no backup available - this is a problem
  return {
    success: false,
    error: 'cannot restore native mode: no backup found. reinstall @rocicorp/zero-sqlite3'
  }
}

/**
 * apply sqlite mode - main entry point
 * handles both installing wasm shim and restoring to native
 */
export function applySqliteMode(config: SqliteModeConfig): ApplyResult {
  if (config.mode === 'native') {
    // for native mode, restore original if shimmed
    if (config.zeroSqlitePath) {
      return restoreNativeMode(config.zeroSqlitePath)
    }
    return { success: true }
  }

  return applyWasmShim(config)
}

/**
 * cleanup any shim artifacts - used during shutdown or cleanup
 */
export function cleanupShim(zeroSqlitePath: string | undefined): void {
  if (!zeroSqlitePath) return

  const indexPath = findZeroSqliteIndex(zeroSqlitePath)
  if (!indexPath) return

  // restore original if we have a backup
  restoreOriginal(indexPath)
}
