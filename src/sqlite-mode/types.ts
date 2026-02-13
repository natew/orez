/**
 * sqlite mode types and constants
 */

export type SqliteMode = 'native' | 'wasm'

export interface SqliteModeConfig {
  mode: SqliteMode
  // path to bedrock-sqlite for wasm mode
  bedrockPath?: string
  // path to @rocicorp/zero-sqlite3 package
  zeroSqlitePath?: string
}

// journal mode differs between native and wasm
// - native: wal2 for better concurrency
// - wasm: delete for compatibility (wal2 corrupts wasm vfs on certain operations)
export const JOURNAL_MODE: Record<SqliteMode, string> = {
  native: 'wal2',
  wasm: 'delete',
}

// common pragmas shared by both modes
export const COMMON_PRAGMAS = {
  busy_timeout: '30000',
  synchronous: 'normal',
}

// backup file marker for identifying orez-shimmed packages
export const BACKUP_MARKER = '.orez-backup'
