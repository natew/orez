import { availableParallelism } from 'node:os'

import type { PGliteOptions } from '@electric-sql/pglite'

export type LogLevel = 'error' | 'warn' | 'info' | 'debug'

/**
 * Context handed to a programmatic lifecycle callback.
 *
 * An onDbReady callback that declares this context argument opts into the same
 * startup barrier as a shell hook. While it executes, ordinary PG clients are
 * held at connection startup so they cannot race schema provisioning. The
 * callback must therefore connect through one of the connection strings below,
 * which carry `applicationName`, the tag that bypasses the barrier.
 *
 * Legacy zero-argument callbacks do not opt into the barrier and retain their
 * pre-context behavior, including the ability to open an ordinary proxy
 * connection.
 */
export interface HookContext {
  /** privileged connection string for the primary (upstream) database. */
  upstreamConnectionString: string
  /** privileged connection string for the zero_cvr database. */
  cvrConnectionString: string
  /** privileged connection string for the zero_cdb database. */
  cdbConnectionString: string
  /**
   * application_name that marks a connection as privileged (bypasses the
   * startup barrier). Present only while a barrier is active (onDbReady on the
   * PGlite/DO proxy backends); undefined for post-startup hooks and native pg.
   */
  applicationName?: string
  /** the port the orez PG proxy listens on. */
  pgPort: number
}

export type LegacyHookCallback = () => void | Promise<void>
export type ContextHookCallback = (ctx: HookContext) => void | Promise<void>

// Lifecycle hooks can be shell commands (CLI) or callbacks (programmatic).
// Callback arity is an intentional runtime contract for onDbReady: zero declared
// parameters select legacy unrestricted startup, while one declared parameter
// opts into the race-free startup barrier and its privileged HookContext URLs.
export type Hook = string | LegacyHookCallback | ContextHookCallback

export interface ZeroLiteConfig {
  /**
   * database backend:
   * - 'pglite' (default): embedded WASM postgres, no native deps
   * - 'postgres': real postgres via the optional `embedded-postgres` package —
   *   zero-cache runs its native logical-replication path, no proxy/emulation
   */
  backend: 'pglite' | 'postgres'
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
  ephemeral: boolean
  ephemeralDir?: string
  readReplicas: number
  logLevel: LogLevel
  pgliteOptions: Partial<PGliteOptions>
  zeroPublications?: string
  zeroMutateUrl?: string
  zeroQueryUrl?: string
  // storage controls
  checkpointIntervalMs: number // WAL checkpoint interval (default: 5min)
  maxLogFileSize: number // log rotation threshold in bytes (default: 2MB)
  /** DO backend URL — replaces PGlite with Durable Object SQLite */
  doBackendUrl?: string
  disableDiskLogs: boolean // skip writing logs to disk (default: false)
  // lifecycle hooks
  onDbReady?: Hook // after db+proxy ready, before zero-cache
  onDbReadyTimeoutMs?: number // bounded startup-hook/barrier wait (default: 30s)
  onHealthy?: Hook // after all services ready
}

/**
 * user-facing config for orez.config.ts
 *
 * mirrors CLI flags in camelCase. all fields optional — defaults
 * match the CLI defaults.
 */
export interface OrezConfig {
  /**
   * database backend (default: "pglite").
   * "postgres" runs real postgres via the optional `embedded-postgres` package.
   */
  backend?: 'pglite' | 'postgres'
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
  /** keep PGlite state in memory and zero-cache replica state in a per-run temp dir */
  ephemeral?: boolean
  /** log level: error, warn, info, debug (default: "warn") */
  logLevel?: LogLevel
  /** command to run after db+proxy ready, before zero-cache */
  onDbReady?: Hook
  /** maximum onDbReady execution and startup-barrier wait (default: 30000ms) */
  onDbReadyTimeoutMs?: number
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
  /** DO backend URL — replaces PGlite with Durable Object SQLite */
  doBackendUrl?: string
}

/** type-safe helper for orez.config.ts */
export function defineConfig(config: OrezConfig): OrezConfig {
  return config
}

export function getConfig(overrides: Partial<ZeroLiteConfig> = {}): ZeroLiteConfig {
  return {
    backend:
      overrides.backend ??
      (process.env.OREZ_BACKEND === 'postgres' ? 'postgres' : 'pglite'),
    dataDir: overrides.dataDir || '.orez',
    pgPort: overrides.pgPort ?? 6434,
    zeroPort: overrides.zeroPort ?? 5849,
    adminPort: overrides.adminPort ?? 0,
    pgUser: overrides.pgUser || 'user',
    pgPassword: overrides.pgPassword || 'password',
    migrationsDir: overrides.migrationsDir || '',
    seedFile: overrides.seedFile || 'src/database/seed.sql',
    skipZeroCache: overrides.skipZeroCache || false,
    disableWasmSqlite: overrides.disableWasmSqlite ?? false,
    forceWasmSqlite: overrides.forceWasmSqlite ?? false,
    useWorkerThreads: overrides.useWorkerThreads ?? true,
    singleDb: overrides.singleDb ?? false,
    ephemeral: overrides.ephemeral ?? false,
    ephemeralDir: overrides.ephemeralDir,
    // singleDb shares one pglite instance for all databases — replicas make no sense
    readReplicas: overrides.readReplicas ?? 0,
    logLevel: overrides.logLevel || (process.env.OREZ_LOG_LEVEL as LogLevel) || 'warn',
    pgliteOptions: overrides.pgliteOptions || {},
    zeroPublications: overrides.zeroPublications,
    zeroMutateUrl: overrides.zeroMutateUrl,
    zeroQueryUrl: overrides.zeroQueryUrl,
    checkpointIntervalMs: overrides.checkpointIntervalMs ?? 5 * 60 * 1000,
    maxLogFileSize: overrides.maxLogFileSize ?? 2 * 1024 * 1024,
    disableDiskLogs: overrides.disableDiskLogs ?? false,
    doBackendUrl: overrides.doBackendUrl ?? process.env.DO_BACKEND_URL,
    onDbReady: overrides.onDbReady,
    onDbReadyTimeoutMs: overrides.onDbReadyTimeoutMs ?? 30_000,
    onHealthy: overrides.onHealthy,
  }
}

export function getConnectionString(config: ZeroLiteConfig, dbName = 'postgres'): string {
  return `postgresql://${config.pgUser}:${config.pgPassword}@127.0.0.1:${config.pgPort}/${dbName}`
}
