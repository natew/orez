import { Database, FileSystem, platform, type AsyncDatabase } from 'one'

import {
  createSQLiteLocalPersistence,
  type LocalPersistenceAdapter,
  type LocalSQLiteExecutor,
} from './localStore.js'

import type { Row } from '../zero/zero.js'
import type { LocalPersistenceOptions } from './localPersistence.js'

const SQLITE_REVISION_ROW = '__limesync_store_revision__'
const PRAGMA_INIT_SQL =
  'PRAGMA busy_timeout = 5000; PRAGMA journal_mode = WAL; PRAGMA auto_vacuum = INCREMENTAL; PRAGMA journal_size_limit = 4194304'

function sanitizeStorageKey(storageKey: string): string {
  return storageKey.replace(/[^a-zA-Z0-9._-]/g, '_')
}

function resolveFilename(storageKey: string, options?: LocalPersistenceOptions): string {
  return options?.filename ?? `limesync-local-${sanitizeStorageKey(storageKey)}.db`
}

function sqliteDirectory() {
  return new FileSystem.Directory(FileSystem.Paths.document, 'SQLite')
}

function sqliteFile(filename: string) {
  return new FileSystem.File(sqliteDirectory(), filename)
}

function sqlitePathSync(filename: string): string {
  return decodeURIComponent(sqliteFile(filename).uri.replace(/^file:\/\//, ''))
}

async function ensureSqliteDirectory(): Promise<void> {
  const dir = sqliteDirectory()
  if (typeof FileSystem.makeDirectory === 'function') {
    await FileSystem.makeDirectory(dir.uri)
  } else {
    dir.create({ idempotent: true })
  }
}

type OpenedOneDb = AsyncDatabase | Database

async function openOneDatabase(filename: string): Promise<OpenedOneDb> {
  await ensureSqliteDirectory()
  const path = sqlitePathSync(filename)
  const db: OpenedOneDb =
    platform === 'ios' ? await Database.openAsync(path) : new Database(path)
  try {
    // set busy_timeout before WAL so a concurrent reader/writer cannot hit SQLITE_BUSY
    await db.exec(PRAGMA_INIT_SQL)
    return db
  } catch (error) {
    try {
      await db.close()
    } catch {}
    throw error
  }
}

function createOneDatabaseExecutor(
  filename: string
): LocalSQLiteExecutor & { close(): Promise<void> } {
  let dbPromise: Promise<OpenedOneDb> | null = null

  const getDb = () => {
    if (!dbPromise) {
      dbPromise = openOneDatabase(filename).catch((error) => {
        dbPromise = null
        throw error
      })
    }
    return dbPromise
  }

  return {
    async exec(sql: string) {
      const db = await getDb()
      await db.exec(sql)
    },
    async all(sql: string, params: readonly unknown[] = []): Promise<Row[]> {
      const db = await getDb()
      const stmt = await db.prepare(sql)
      try {
        const rows = await stmt.all(...params)
        return rows as Row[]
      } finally {
        await stmt.finalize()
      }
    },
    async run(sql: string, params: readonly unknown[] = []) {
      const db = await getDb()
      const stmt = await db.prepare(sql)
      try {
        return await stmt.run(...params)
      } finally {
        await stmt.finalize()
      }
    },
    async close() {
      const pending = dbPromise
      dbPromise = null
      if (!pending) return
      const db = await pending.catch(() => null)
      await db?.close()
    },
  }
}

const sharedAdapters = new Map<string, LocalPersistenceAdapter>()

/**
 * One native local persistence for `.local({ persistence: 'device' })` tables.
 * Stores all device tables in one shared `One.Database` WAL SQLite file under
 * `Documents/SQLite/`, using `Database.openAsync` on iOS to stay off the UI thread.
 */
export function createLocalPersistence(
  storageKey = 'default',
  options?: LocalPersistenceOptions
): LocalPersistenceAdapter {
  const filename = resolveFilename(storageKey, options)
  const existing = sharedAdapters.get(filename)
  if (existing) return existing

  const executor = createOneDatabaseExecutor(filename)
  const base = createSQLiteLocalPersistence(executor)
  const adapter: LocalPersistenceAdapter = {
    open: (tables) => base.open(tables),
    commit: (ops) => base.commit(ops),
    clear: (tables) => base.clear?.(tables) ?? Promise.resolve(),
    close: async () => {
      sharedAdapters.delete(filename)
      await executor.close()
    },
  }
  sharedAdapters.set(filename, adapter)
  return adapter
}

/**
 * Synchronous native-only boot read of all persisted rows in one local device table.
 * Designed for zero-flash frame-1 shell restore before async hydration settles.
 * Exported only on the native condition (`limesync/data/local-persistence/native`).
 */
export function readLocalTableRowsSync<R extends Row = Row>(
  storageKey: string,
  tableName: string,
  options?: Pick<LocalPersistenceOptions, 'filename'>
): R[] {
  const filename = resolveFilename(storageKey, options)
  const file = sqliteFile(filename)
  if (!file.exists) return []
  let db: Database | null = null
  try {
    db = new Database(sqlitePathSync(filename))
    db.exec('PRAGMA busy_timeout = 5000')
    const stmt = db.prepare(
      'SELECT row_json FROM limesync_local_rows WHERE table_name = ?'
    )
    try {
      const rows = stmt.all(tableName) as Array<{ row_json?: unknown }>
      const out: R[] = []
      for (const row of rows) {
        if (typeof row.row_json !== 'string') continue
        const parsed = JSON.parse(row.row_json) as R
        if (parsed && typeof parsed === 'object') out.push(parsed)
      }
      return out
    } finally {
      stmt.finalize()
    }
  } catch {
    return []
  } finally {
    try {
      db?.close()
    } catch {}
  }
}

/**
 * Synchronous native-only atomic delete of rows in one local device table,
 * bumping the store revision so open async handles detect and reconcile the change.
 * Exported only on the native condition (`limesync/data/local-persistence/native`).
 */
export function deleteLocalTableRowsSync(
  storageKey: string,
  tableName: string,
  keys?: readonly string[],
  options?: Pick<LocalPersistenceOptions, 'filename'>
): boolean {
  const filename = resolveFilename(storageKey, options)
  const file = sqliteFile(filename)
  if (!file.exists) return true
  let db: Database | null = null
  try {
    db = new Database(sqlitePathSync(filename))
    db.exec('PRAGMA busy_timeout = 5000')
    db.exec('BEGIN IMMEDIATE')
    try {
      if (keys && keys.length > 0) {
        const del = db.prepare(
          'DELETE FROM limesync_local_rows WHERE table_name = ? AND row_key = ?'
        )
        try {
          for (const key of keys) del.run(tableName, key)
        } finally {
          del.finalize()
        }
      } else {
        const del = db.prepare('DELETE FROM limesync_local_rows WHERE table_name = ?')
        try {
          del.run(tableName)
        } finally {
          del.finalize()
        }
      }
      const revStmt = db.prepare(
        'SELECT schema_json FROM limesync_local_meta WHERE table_name = ?'
      )
      let currentRev = 0
      try {
        const revRows = revStmt.all(SQLITE_REVISION_ROW) as Array<{
          schema_json?: unknown
        }>
        if (revRows[0]?.schema_json) {
          currentRev = Number(JSON.parse(String(revRows[0].schema_json)))
        }
      } finally {
        revStmt.finalize()
      }
      const nextRev = (Number.isFinite(currentRev) ? currentRev : 0) + 1
      const bump = db.prepare(
        'INSERT OR REPLACE INTO limesync_local_meta (table_name, schema_json) VALUES (?, ?)'
      )
      try {
        bump.run(SQLITE_REVISION_ROW, JSON.stringify(nextRev))
      } finally {
        bump.finalize()
      }
      db.exec('COMMIT')
      return true
    } catch {
      try {
        db.exec('ROLLBACK')
      } catch {}
      return false
    }
  } catch {
    return false
  } finally {
    try {
      db?.close()
    } catch {}
  }
}
