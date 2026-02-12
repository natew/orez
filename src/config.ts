import type { PGliteOptions } from '@electric-sql/pglite'

export type LogLevel = 'error' | 'warn' | 'info' | 'debug'

// lifecycle hooks - can be shell command string (CLI) or callback (programmatic)
export type Hook = string | (() => void | Promise<void>)

export interface ZeroLiteConfig {
  dataDir: string
  pgPort: number
  zeroPort: number
  adminPort: number
  pgUser: string
  pgPassword: string
  migrationsDir: string
  seedFile: string
  skipZeroCache: boolean
  disableWasmSqlite: boolean
  logLevel: LogLevel
  pgliteOptions: Partial<PGliteOptions>
  // lifecycle hooks
  onDbReady?: Hook // after db+proxy ready, before zero-cache
  onHealthy?: Hook // after all services ready
}

export function getConfig(overrides: Partial<ZeroLiteConfig> = {}): ZeroLiteConfig {
  return {
    dataDir: overrides.dataDir || '.orez',
    pgPort: overrides.pgPort || 6434,
    zeroPort: overrides.zeroPort || 5849,
    adminPort: overrides.adminPort || 0,
    pgUser: overrides.pgUser || 'user',
    pgPassword: overrides.pgPassword || 'password',
    migrationsDir: overrides.migrationsDir || '',
    seedFile: overrides.seedFile || 'src/database/seed.sql',
    skipZeroCache: overrides.skipZeroCache || false,
    disableWasmSqlite: overrides.disableWasmSqlite ?? false,
    logLevel: overrides.logLevel || 'warn',
    pgliteOptions: overrides.pgliteOptions || {},
    onDbReady: overrides.onDbReady,
    onHealthy: overrides.onHealthy,
  }
}

export function getConnectionString(config: ZeroLiteConfig, dbName = 'postgres'): string {
  return `postgresql://${config.pgUser}:${config.pgPassword}@127.0.0.1:${config.pgPort}/${dbName}`
}
