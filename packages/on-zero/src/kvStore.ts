// the zero local cache a platform keeps: IndexedDB on web, and memory only for
// a server render, which has no IndexedDB. kvStore.native.ts keeps it in
// SQLite through zero's op-sqlite adapter. opt-in: createZeroClient's own
// default stays 'mem'.
export function createZeroKvStore(): 'idb' | 'mem' {
  return typeof indexedDB === 'undefined' ? 'mem' : 'idb'
}
