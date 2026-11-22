import { opSQLiteStoreProvider } from '../zero/op-sqlite.js'

// native keeps the zero local cache in SQLite through zero's op-sqlite adapter,
// which needs @op-engineering/op-sqlite in the app.
export function createZeroKvStore(): ReturnType<typeof opSQLiteStoreProvider> {
  return opSQLiteStoreProvider()
}
