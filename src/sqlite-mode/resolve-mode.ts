/**
 * mode resolution - canonical place to determine sqlite mode from config/env
 */

import { createRequire } from 'node:module'

import type { SqliteMode, SqliteModeConfig } from './types.js'

/**
 * resolve a package entry path
 * import.meta.resolve doesn't work in vitest, so we fall back to require.resolve
 */
export function resolvePackage(pkg: string): string {
  try {
    const resolved = import.meta.resolve(pkg)
    if (resolved) return resolved.replace('file://', '')
  } catch {}
  try {
    const require = createRequire(import.meta.url)
    return require.resolve(pkg)
  } catch {}
  return ''
}

/**
 * resolve sqlite mode from config
 * single source of truth for mode selection
 */
export function resolveSqliteMode(disableWasmSqlite: boolean): SqliteMode {
  return disableWasmSqlite ? 'native' : 'wasm'
}

/**
 * resolve full sqlite mode config including paths
 * returns null if required packages aren't installed
 */
export function resolveSqliteModeConfig(
  disableWasmSqlite: boolean
): SqliteModeConfig | null {
  const mode = resolveSqliteMode(disableWasmSqlite)
  const zeroSqlitePath = resolvePackage('@rocicorp/zero-sqlite3') || undefined

  // native mode may still need zero-sqlite3 path for restoring from a prior shim
  if (mode === 'native') {
    return { mode, zeroSqlitePath }
  }

  // wasm mode needs bedrock-sqlite and zero-sqlite3 paths
  const bedrockPath = resolvePackage('bedrock-sqlite')

  if (!bedrockPath) {
    return null // bedrock-sqlite not installed
  }

  if (!zeroSqlitePath) {
    return null // zero-sqlite3 not installed
  }

  return {
    mode,
    bedrockPath,
    zeroSqlitePath,
  }
}

/**
 * get mode display string for logging
 */
export function getModeDisplayString(mode: SqliteMode): string {
  return mode
}
