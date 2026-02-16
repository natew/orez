/**
 * mode resolution - canonical place to determine sqlite mode from config/env
 *
 * priority:
 * 1. explicit --disable-wasm-sqlite flag → native (requires compiled binary)
 * 2. explicit --force-wasm-sqlite flag → wasm
 * 3. native binary exists → native (auto-detect, faster)
 * 4. fallback → wasm (works everywhere)
 */

import { inspectNativeSqliteBinary } from './native-binary.js'
import { resolvePackage } from './package-resolve.js'

import type { SqliteMode, SqliteModeConfig } from './types.js'
export { resolvePackage } from './package-resolve.js'

/**
 * resolve sqlite mode from config
 * single source of truth for mode selection
 *
 * auto-detects native if binary exists (faster), otherwise wasm.
 *
 * @param disableWasmSqlite - explicit flag to force native mode
 * @param forceWasmSqlite - explicit flag to force wasm mode
 */
export function resolveSqliteMode(
  disableWasmSqlite: boolean,
  forceWasmSqlite: boolean = false
): SqliteMode {
  // explicit native request
  if (disableWasmSqlite) return 'native'

  // explicit wasm request
  if (forceWasmSqlite) return 'wasm'

  // auto-detect: use native if binary is compiled
  const nativeCheck = inspectNativeSqliteBinary()
  if (nativeCheck.found) return 'native'

  // fallback to wasm
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
