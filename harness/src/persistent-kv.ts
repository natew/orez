import { Database } from 'bun:sqlite'
import { rmSync } from 'node:fs'

import {
  SQLiteStore,
  type SQLiteDatabase,
  type SQLiteStoreOptions,
  type StoreProvider,
} from '@rocicorp/zero/sqlite'

// Bun adapter for Zero's own transactional SQLiteStore. Unlike kvStore:'idb'
// (which falls back to memory when indexedDB is absent), this writes the real
// Replicache DAG to disk so close/reopen lanes exercise persisted cookies,
// client groups, and pending mutations.
class BunSQLiteDatabase implements SQLiteDatabase {
  readonly #db: Database
  readonly #filename: string
  #closed = false

  constructor(filename: string) {
    this.#filename = filename
    this.#db = new Database(filename, { create: true })
  }

  prepare(sql: string) {
    const statement = this.#db.query(sql)
    return {
      async exec(params: string[]) {
        statement.run(...params)
      },
      async all(params: string[]) {
        return statement.values(...params) as unknown[][]
      },
    }
  }

  execSync(sql: string) {
    this.#db.exec(sql)
  }

  close() {
    if (this.#closed) return
    this.#closed = true
    this.#db.close()
  }

  destroy() {
    this.close()
    for (const suffix of ['', '-wal', '-shm']) {
      rmSync(`${this.#filename}${suffix}`, { force: true })
    }
  }
}

export function persistentKVStoreProvider(directory: string): StoreProvider {
  const options: SQLiteStoreOptions = { directory }
  const createDatabase = (filename: string) => new BunSQLiteDatabase(filename)
  const generations = new Map<string, number>()
  const physicalName = (name: string) =>
    `${name}:generation-${generations.get(name) ?? 0}`
  return {
    create: (name) => new SQLiteStore(physicalName(name), createDatabase, options),
    // IndexedDB deletion invalidates future opens without force-closing the
    // active page mid-callback. Use a fresh physical generation to preserve
    // that behavior; the lane removes the whole temporary directory at exit.
    drop: async (name) => {
      generations.set(name, (generations.get(name) ?? 0) + 1)
    },
  }
}
