/**
 * mode resolution - canonical place to determine sqlite mode from config/env
 *
 * priority:
 * 1. explicit --disable-wasm-sqlite flag → native (requires compiled binary)
 * 2. default → wasm (works everywhere, no compilation needed)
 */

import { resolvePackage } from './package-resolve.js'

import type { SqliteMode, SqliteModeConfig } from './types.js'
export { resolvePackage } from './package-resolve.js'

/**
 * resolve sqlite mode from config
 * single source of truth for mode selection
 *
 * wasm is the default - it works everywhere without compilation.
 * native is opt-in via --disable-wasm-sqlite for users who need it.
 *
 * @param disableWasmSqlite - explicit flag to force native mode
 * @param _forceWasmSqlite - deprecated, wasm is now default
 */
export function resolveSqliteMode(
  disableWasmSqlite: boolean,
  _forceWasmSqlite: boolean = false
): SqliteMode {
  // explicit native request
  if (disableWasmSqlite) return 'native'

  // wasm is the default - works everywhere
  return 'wasm'
}

/**
 * resolve full sqlite mode config including paths
 * returns null if required packages aren't installed
 */
export function resolveSqliteModeConfig(
  disableWasmSqlite: boolean,
  forceWasmSqlite: boolean = false
): SqliteModeConfig | null {
  const mode = resolveSqliteMode(disableWasmSqlite, forceWasmSqlite)
  const zeroSqlitePath = resolvePackage('@rocicorp/zero-sqlite3') || undefined

  // native mode needs zero-sqlite3 path
  if (mode === 'native') {
    return { mode, zeroSqlitePath }
  }

  // wasm mode only needs bedrock-sqlite - we write a shim
  // directly into node_modules/@rocicorp/zero-sqlite3
  const bedrockPath = resolvePackage('bedrock-sqlite')

  if (!bedrockPath) {
    return null // bedrock-sqlite not installed
  }

  return {
    mode,
    bedrockPath,
    zeroSqlitePath, // optional - may not exist if using shim
  }
}

/**
 * get mode display string for logging
 */
export function getModeDisplayString(mode: SqliteMode): string {
  return mode
}
