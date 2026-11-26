import { Database, FileSystem, platform } from 'one'

import { dropStore, SQLiteStore, type StoreProvider } from '../zero/sqlite.js'

// a One app without React Native keeps the zero local cache in One's own
// platform SQLite, in the directory the OS backs up with app data.
export function createSyncKvStore(): StoreProvider {
  const { Directory, File, Paths } = FileSystem
  const directory = new Directory(
    Paths.document.parentDirectory,
    platform === 'ios' ? 'Library' : 'databases'
  )
  directory.create({ idempotent: true })

  const open = (filename: string) => {
    const db = new Database(
      decodeURIComponent(new URL(new File(directory, filename).uri).pathname)
    )
    return {
      close: () => db.close(),
      destroy() {
        for (const suffix of ['', '-wal', '-shm', '-journal']) {
          const file = new File(directory, filename + suffix)
          if (file.exists) file.delete()
        }
      },
      prepare(sql: string) {
        const statement = db.prepare(sql)
        return {
          async exec(params: string[]) {
            statement.run(params)
          },
          async all(params: string[]) {
            return statement.values(params)
          },
        }
      },
      execSync: (sql: string) => db.exec(sql),
    }
  }

  return {
    create: (name) => new SQLiteStore(name, open),
    drop: (name) => dropStore(name, open),
  }
}
