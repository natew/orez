import { availableParallelism } from 'node:os'

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
  forceWasmSqlite: boolean
  useWorkerThreads: boolean
  singleDb: boolean
  readReplicas: number
  logLevel: LogLevel
  pgliteOptions: Partial<PGliteOptions>
  zeroPublications?: string
  zeroMutateUrl?: string
  zeroQueryUrl?: string
  // storage controls
  checkpointIntervalMs: number // WAL checkpoint interval (default: 5min)
  maxLogFileSize: number // log rotation threshold in bytes (default: 2MB)
  disableDiskLogs: boolean // skip writing logs to disk (default: false)
  // lifecycle hooks
  onDbReady?: Hook // after db+proxy ready, before zero-cache
  onHealthy?: Hook // after all services ready
}

/**
 * user-facing config for orez.config.ts
 *
 * mirrors CLI flags in camelCase. all fields optional — defaults
 * match the CLI defaults.
 */
export interface OrezConfig {
  /** data directory (default: ".orez") */
  dataDir?: string
  /** postgresql proxy port (default: 6434) */
  pgPort?: number
  /** zero-cache port (default: 5849) */
  zeroPort?: number
  /** admin dashboard port (default: 6477) */
  adminPort?: number
  /** s3 server port (default: 9200) */
  s3Port?: number
  /** postgresql user (default: "user") */
  pgUser?: string
  /** postgresql password (default: "password") */
  pgPassword?: string
  /** migrations directory */
  migrationsDir?: string
  /** alias for migrationsDir */
  migrations?: string
  /** seed file path */
  seedFile?: string
  /** alias for seedFile */
  seed?: string
  /** run pglite + proxy only, skip zero-cache */
  skipZeroCache?: boolean
  /** also start a local s3-compatible server */
  s3?: boolean
  /** disable admin dashboard */
  disableAdmin?: boolean
  /** force native @rocicorp/zero-sqlite3 */
  disableWasmSqlite?: boolean
  /** force wasm bedrock-sqlite even if native is available */
  forceWasmSqlite?: boolean
  /** run pglite in-process instead of worker threads */
  noWorkerThreads?: boolean
  /** use worker threads for pglite (default: true) — inverse of noWorkerThreads */
  useWorkerThreads?: boolean
  /** single pglite instance for all databases */
  singleDb?: boolean
  /** log level: error, warn, info, debug (default: "warn") */
  logLevel?: LogLevel
  /** command to run after db+proxy ready, before zero-cache */
  onDbReady?: Hook
  /** command to run once all services healthy */
  onHealthy?: Hook
  /** number of pglite read replicas for postgres (default: auto, 0 to disable) */
  readReplicas?: number
  /** pglite options */
  pgliteOptions?: Partial<PGliteOptions>
  /** ZERO_APP_PUBLICATIONS — comma-separated publication names */
  zeroPublications?: string
  /** ZERO_MUTATE_URL — push/mutate endpoint for zero-cache */
  zeroMutateUrl?: string
  /** ZERO_QUERY_URL — pull/query endpoint for zero-cache */
  zeroQueryUrl?: string
  /** WAL checkpoint interval in ms (default: 300000 = 5min, 0 to disable) */
  checkpointIntervalMs?: number
  /** max log file size in bytes before rotation (default: 2097152 = 2MB) */
  maxLogFileSize?: number
  /** disable writing logs to disk (default: false) */
  disableDiskLogs?: boolean
}

/** type-safe helper for orez.config.ts */
export function defineConfig(config: OrezConfig): OrezConfig {
  return config
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
    forceWasmSqlite: overrides.forceWasmSqlite ?? false,
    useWorkerThreads: overrides.useWorkerThreads ?? true,
    singleDb: overrides.singleDb ?? false,
    // singleDb shares one pglite instance for all databases — replicas make no sense
    readReplicas:
      (overrides.singleDb ?? false)
        ? 0
        : (overrides.readReplicas ??
          Math.min(Math.ceil(availableParallelism() * 0.5), 4)),
    logLevel: overrides.logLevel || (process.env.OREZ_LOG_LEVEL as LogLevel) || 'warn',
    pgliteOptions: overrides.pgliteOptions || {},
    zeroPublications: overrides.zeroPublications,
    zeroMutateUrl: overrides.zeroMutateUrl,
    zeroQueryUrl: overrides.zeroQueryUrl,
    checkpointIntervalMs: overrides.checkpointIntervalMs ?? 5 * 60 * 1000,
    maxLogFileSize: overrides.maxLogFileSize ?? 2 * 1024 * 1024,
    disableDiskLogs: overrides.disableDiskLogs ?? false,
    onDbReady: overrides.onDbReady,
    onHealthy: overrides.onHealthy,
  }
}

export function getConnectionString(config: ZeroLiteConfig, dbName = 'postgres'): string {
  return `postgresql://${config.pgUser}:${config.pgPassword}@127.0.0.1:${config.pgPort}/${dbName}`
}
