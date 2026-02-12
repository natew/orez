import type { PGliteOptions } from '@electric-sql/pglite'

export type LogLevel = 'error' | 'warn' | 'info' | 'debug'

export interface ZeroLiteConfig {
  dataDir: string
  pgPort: number
  zeroPort: number
  pgUser: string
  pgPassword: string
  migrationsDir: string
  seedFile: string
  skipZeroCache: boolean
  disableWasmSqlite: boolean
  logLevel: LogLevel
  pgliteOptions: Partial<PGliteOptions>
  logEnv: boolean
  onDbReady: string
  beforeZero: ((db: import('@electric-sql/pglite').PGlite) => Promise<void>) | null
  admin: boolean
  adminPort: number
  adminLogs: boolean
}

export function getConfig(overrides: Partial<ZeroLiteConfig> = {}): ZeroLiteConfig {
  return {
    dataDir: overrides.dataDir || '.orez',
    pgPort: overrides.pgPort || 6434,
    zeroPort: overrides.zeroPort || 5849,
    pgUser: overrides.pgUser || 'user',
    pgPassword: overrides.pgPassword || 'password',
    migrationsDir: overrides.migrationsDir || '',
    seedFile: overrides.seedFile || 'src/database/seed.sql',
    skipZeroCache: overrides.skipZeroCache || false,
    disableWasmSqlite: overrides.disableWasmSqlite ?? false,
    logLevel: overrides.logLevel || 'warn',
    pgliteOptions: overrides.pgliteOptions || {},
    logEnv: overrides.logEnv ?? false,
    onDbReady: overrides.onDbReady || '',
    beforeZero: overrides.beforeZero || null,
    admin: overrides.admin ?? false,
    adminPort: overrides.adminPort || 0,
    adminLogs: overrides.adminLogs ?? true,
  }
}

export function getConnectionString(config: ZeroLiteConfig, dbName = 'postgres'): string {
  return `postgresql://${config.pgUser}:${config.pgPassword}@127.0.0.1:${config.pgPort}/${dbName}`
}
