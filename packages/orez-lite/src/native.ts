import { spawn, type ChildProcess, type SpawnOptions } from 'node:child_process'
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

export interface NativeHostCallbacks {
  authenticate: string | URL
  authorizeWake: string | URL
  transformQueries: string | URL
  certificateAuthority?: string
}

export interface NativeHostWorkerRetention {
  idleMs: number
  sweepIntervalMs: number
}

export interface NativeHostOptions {
  schema: unknown
  initSql: readonly string[]
  dataDir: string
  port: number
  adminTokenEnv: string
  callbacks: NativeHostCallbacks
  host?: string
  allowedOrigins?: readonly string[]
  workerRetention?: NativeHostWorkerRetention
  changeLogRows?: number
}

export interface NativeHost {
  start(options?: SpawnOptions): ChildProcess
}

const load = createRequire(import.meta.url)

export function createNativeHost(options: NativeHostOptions): NativeHost {
  validateOptions(options)

  return {
    start(spawnOptions = {}) {
      const configDir = mkdtempSync(join(tmpdir(), 'orez-native-'))
      const schemaPath = join(configDir, 'schema.json')
      const initSqlPath = join(configDir, 'init-sql.json')
      try {
        writeJson(schemaPath, options.schema)
        writeJson(initSqlPath, options.initSql)
        const launcher = load.resolve('orez-sync-native/launcher')
        const child = spawn(
          process.execPath,
          [launcher, ...nativeHostArguments(options, schemaPath, initSqlPath)],
          { stdio: 'inherit', ...spawnOptions }
        )
        const cleanup = () => rmSync(configDir, { force: true, recursive: true })
        child.once('error', cleanup)
        child.once('close', cleanup)
        return child
      } catch (error) {
        rmSync(configDir, { force: true, recursive: true })
        throw error
      }
    },
  }
}

function nativeHostArguments(
  options: NativeHostOptions,
  schemaPath: string,
  initSqlPath: string
): string[] {
  const args = [
    'serve',
    '--schema',
    schemaPath,
    '--init-sql',
    initSqlPath,
    '--data-dir',
    options.dataDir,
    '--port',
    String(options.port),
    '--admin-token-env',
    options.adminTokenEnv,
    '--auth-url',
    String(options.callbacks.authenticate),
    '--wake-authorize-url',
    String(options.callbacks.authorizeWake),
    '--query-transform-url',
    String(options.callbacks.transformQueries),
  ]
  if (options.host) args.push('--host', options.host)
  if (options.callbacks.certificateAuthority) {
    args.push('--callback-ca', options.callbacks.certificateAuthority)
  }
  for (const origin of options.allowedOrigins ?? []) {
    args.push('--allow-origin', origin)
  }
  if (options.workerRetention) {
    args.push(
      '--retention',
      'workers',
      '--worker-idle-ms',
      String(options.workerRetention.idleMs),
      '--worker-sweep-ms',
      String(options.workerRetention.sweepIntervalMs)
    )
  }
  if (options.changeLogRows !== undefined) {
    args.push('--retain-changes', String(options.changeLogRows))
  }
  return args
}

function validateOptions(options: NativeHostOptions) {
  positiveInteger(options.port, 'port')
  if (!options.adminTokenEnv) throw new TypeError('adminTokenEnv must not be empty')
  if (!options.dataDir) throw new TypeError('dataDir must not be empty')
  if (!Array.isArray(options.initSql)) throw new TypeError('initSql must be an array')
  for (const statement of options.initSql) {
    if (typeof statement !== 'string' || !statement.trim()) {
      throw new TypeError('initSql statements must be non-empty strings')
    }
  }
  if (options.workerRetention) {
    positiveInteger(options.workerRetention.idleMs, 'workerRetention.idleMs')
    positiveInteger(
      options.workerRetention.sweepIntervalMs,
      'workerRetention.sweepIntervalMs'
    )
  }
  if (options.changeLogRows !== undefined) {
    positiveInteger(options.changeLogRows, 'changeLogRows')
  }
}

function positiveInteger(value: number, name: string) {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new TypeError(`${name} must be a positive integer`)
  }
}

function writeJson(path: string, value: unknown) {
  const json = JSON.stringify(value)
  if (json === undefined) throw new TypeError('native host configuration must be JSON')
  writeFileSync(path, json, { encoding: 'utf8', mode: 0o600 })
}
