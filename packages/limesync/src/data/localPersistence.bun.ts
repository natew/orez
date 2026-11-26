import { mkdirSync } from 'node:fs'
import { join } from 'node:path'

import {
  createSQLiteLocalPersistence,
  type LocalPersistenceAdapter,
  type LocalSQLiteExecutor,
} from './localStore.js'

import type { Row } from '../zero/zero.js'
import type { LocalPersistenceOptions } from './localPersistence.js'

const PRAGMA_INIT_SQL =
  'PRAGMA busy_timeout = 5000; PRAGMA journal_mode = WAL; PRAGMA auto_vacuum = INCREMENTAL; PRAGMA journal_size_limit = 4194304'

type BunSqliteStatement = {
  all(...params: unknown[]): Row[]
  run(...params: unknown[]): unknown
  finalize?(): void
}

type BunSqliteDatabase = {
  exec(sql: string): void
  prepare(sql: string): BunSqliteStatement
  close(): void
}

function sanitizeStorageKey(storageKey: string): string {
  return storageKey.replace(/[^a-zA-Z0-9._-]/g, '_')
}

async function openBunSqlite(dbPath: string): Promise<BunSqliteDatabase> {
  const specifier = 'bun:sqlite'
  const mod = (await import(/* @vite-ignore */ specifier)) as {
    Database: new (path: string) => BunSqliteDatabase
  }
  const db = new mod.Database(dbPath)
  try {
    db.exec(PRAGMA_INIT_SQL)
    return db
  } catch (error) {
    try {
      db.close()
    } catch {}
    throw error
  }
}

function createBunSqliteExecutor(
  dbPath: string
): LocalSQLiteExecutor & { close(): Promise<void> } {
  let dbPromise: Promise<BunSqliteDatabase> | null = null

  const getDb = () => {
    if (!dbPromise) {
      dbPromise = openBunSqlite(dbPath).catch((error) => {
        dbPromise = null
        throw error
      })
    }
    return dbPromise
  }

  return {
    async exec(sql: string) {
      const db = await getDb()
      db.exec(sql)
    },
    async all(sql: string, params: readonly unknown[] = []): Promise<Row[]> {
      const db = await getDb()
      const stmt = db.prepare(sql)
      try {
        return stmt.all(...params)
      } finally {
        stmt.finalize?.()
      }
    },
    async run(sql: string, params: readonly unknown[] = []) {
      const db = await getDb()
      const stmt = db.prepare(sql)
      try {
        return stmt.run(...params)
      } finally {
        stmt.finalize?.()
      }
    },
    async close() {
      const pending = dbPromise
      dbPromise = null
      if (!pending) return
      const db = await pending.catch(() => null)
      db?.close()
    },
  }
}

const sharedAdapters = new Map<string, LocalPersistenceAdapter>()

/**
 * Bun host local persistence for `.local({ persistence: 'device' })` tables.
 * Uses `bun:sqlite` WAL files inside `options.directory ?? process.env.LIMESYNC_LOCAL_DIR`.
 * Fails loud in headless Bun when no storage directory is configured.
 */
export function createLocalPersistence(
  storageKey = 'default',
  options?: LocalPersistenceOptions
): LocalPersistenceAdapter {
  const directory = options?.directory ?? process.env.LIMESYNC_LOCAL_DIR
  if (!directory) {
    throw new Error(
      `[limesync] local device persistence in Bun requires options.directory or LIMESYNC_LOCAL_DIR ` +
        `(or pass an explicit local.persistence adapter). ` +
        `Use .local({ persistence: 'memory' }) for intentional RAM-only tables.`
    )
  }
  const filename =
    options?.filename ?? `limesync-local-${sanitizeStorageKey(storageKey)}.db`
  mkdirSync(directory, { recursive: true })
  const dbPath = join(directory, filename)
  const existing = sharedAdapters.get(dbPath)
  if (existing) return existing

  const executor = createBunSqliteExecutor(dbPath)
  const base = createSQLiteLocalPersistence(executor)
  const adapter: LocalPersistenceAdapter = {
    open: (tables) => base.open(tables),
    commit: (ops) => base.commit(ops),
    clear: (tables) => base.clear?.(tables) ?? Promise.resolve(),
    close: async () => {
      sharedAdapters.delete(dbPath)
      await executor.close()
    },
  }
  sharedAdapters.set(dbPath, adapter)
  return adapter
}
