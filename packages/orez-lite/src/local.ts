import { type ChildProcess } from 'node:child_process'
import { randomBytes } from 'node:crypto'
import { resolve } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { pathToFileURL } from 'node:url'

import {
  createNativeHost,
  type NativeHostCallbacks,
  type NativeHostWorkerRetention,
} from './native.js'

type LocalSchema = {
  tables: Record<string, { name: string }>
}

export interface LocalSyncHostConfig {
  schema: LocalSchema
  dataDir: string
  namespace: string
  port: number
  callbacks: NativeHostCallbacks
  allowedOrigins?: readonly string[]
  prepare?: () => void | Promise<void>
  host?: string
  workerRetention?: NativeHostWorkerRetention
  changeLogRows?: number
  startupTimeoutMs?: number
}

export interface LocalSyncHostExit {
  code: number | null
  signal: NodeJS.Signals | null
  expected: boolean
}

export interface LocalSyncHost {
  child: ChildProcess
  exited: Promise<LocalSyncHostExit>
  close(): Promise<void>
}

export function defineLocalConfig(config: LocalSyncHostConfig): LocalSyncHostConfig {
  return config
}

export async function loadLocalConfig(path: string): Promise<LocalSyncHostConfig> {
  const imported = await import(pathToFileURL(resolve(path)).href)
  const config: LocalSyncHostConfig = imported.default
  if (!config || typeof config !== 'object') {
    throw new TypeError(`${path} must default export defineLocalConfig({ ... })`)
  }
  return config
}

export async function startLocalSyncHost(
  config: LocalSyncHostConfig
): Promise<LocalSyncHost> {
  if (!/^[A-Za-z0-9_-]{1,64}$/.test(config.namespace)) {
    throw new TypeError(`invalid native sync namespace: ${config.namespace}`)
  }

  await config.prepare?.()

  const dataDir = resolve(config.dataDir)
  const database = new DatabaseSync(resolve(dataDir, `${config.namespace}.sqlite`))
  const initSql: string[] = []
  try {
    for (const table of Object.values(config.schema.tables)) {
      const row = database
        .prepare(
          `SELECT sql FROM sqlite_schema
           WHERE type = 'table' AND name = ? AND sql IS NOT NULL`
        )
        .get(table.name)
      const sql = row && typeof row === 'object' ? Reflect.get(row, 'sql') : null
      if (typeof sql !== 'string') {
        throw new Error(`native sync schema is missing SQLite DDL for ${table.name}`)
      }
      initSql.push(sql.replace(/^CREATE TABLE\s+/i, 'CREATE TABLE IF NOT EXISTS '))
    }

    const tableNames = Object.values(config.schema.tables).map((table) => table.name)
    for (const tableName of tableNames) {
      const rows = database
        .prepare(
          `SELECT sql FROM sqlite_schema
           WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL
           ORDER BY name`
        )
        .all(tableName)
      for (const row of rows) {
        const sql = row && typeof row === 'object' ? Reflect.get(row, 'sql') : null
        if (typeof sql !== 'string') {
          throw new TypeError(`native sync schema has invalid index DDL for ${tableName}`)
        }
        initSql.push(
          sql
            .replace(/^CREATE UNIQUE INDEX\s+/i, 'CREATE UNIQUE INDEX IF NOT EXISTS ')
            .replace(/^CREATE INDEX\s+/i, 'CREATE INDEX IF NOT EXISTS ')
        )
      }
    }
  } finally {
    database.close()
  }

  const adminTokenEnv = 'OREZ_SYNC_NATIVE_ADMIN_TOKEN'
  const adminToken = randomBytes(32).toString('hex')
  const child = createNativeHost({
    schema: config.schema,
    initSql,
    dataDir,
    port: config.port,
    adminTokenEnv,
    callbacks: config.callbacks,
    host: config.host,
    allowedOrigins: config.allowedOrigins,
    workerRetention: config.workerRetention,
    changeLogRows: config.changeLogRows,
  }).start({
    env: { ...process.env, [adminTokenEnv]: adminToken },
    stdio: 'inherit',
  })

  let expectedExit = false
  const exited = new Promise<LocalSyncHostExit>((resolveExit) => {
    child.once('exit', (code, signal) => {
      resolveExit({ code, signal, expected: expectedExit })
    })
  })
  let startError: Error | null = null
  child.once('error', (error) => {
    startError = error
  })

  const readyDeadline = Date.now() + (config.startupTimeoutMs ?? 30_000)
  while (Date.now() < readyDeadline) {
    if (startError) throw startError
    if (child.exitCode !== null || child.signalCode !== null) {
      throw new Error(
        `native sync host exited before startup (${child.signalCode ? `signal ${child.signalCode}` : `code ${child.exitCode}`})`
      )
    }
    try {
      const response = await fetch(
        `http://${config.host ?? '127.0.0.1'}:${config.port}/admin/health`,
        { headers: { 'x-admin-key': adminToken } }
      )
      if (response.ok) {
        return {
          child,
          exited,
          async close() {
            if (child.exitCode !== null || child.signalCode !== null) return
            expectedExit = true
            child.kill('SIGTERM')
            await exited
          },
        }
      }
    } catch {}
    await new Promise((resolveReady) => setTimeout(resolveReady, 100))
  }

  expectedExit = true
  child.kill('SIGTERM')
  await exited
  throw new Error(`native sync host did not become ready on port ${config.port}`)
}
