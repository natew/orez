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

// journal mode - zero-cache requires wal2 for replica sync (BEGIN CONCURRENT)
// both modes use wal2 now - bedrock-sqlite wasm should support it
export const JOURNAL_MODE: Record<SqliteMode, string> = {
  native: 'wal2',
  wasm: 'wal2',
}

// common pragmas shared by both modes
export const COMMON_PRAGMAS = {
  busy_timeout: '30000',
  synchronous: 'normal',
}

// backup file marker for identifying orez-shimmed packages
export const BACKUP_MARKER = '.orez-backup'
