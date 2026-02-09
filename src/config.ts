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
  logLevel: LogLevel
  pgliteOptions: Partial<PGliteOptions>
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
    logLevel: overrides.logLevel || 'info',
    pgliteOptions: overrides.pgliteOptions || {},
  }
}

export function getConnectionString(config: ZeroLiteConfig, dbName = 'postgres'): string {
  return `postgresql://${config.pgUser}:${config.pgPassword}@127.0.0.1:${config.pgPort}/${dbName}`
}
