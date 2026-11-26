import {
  createIndexedDBLocalPersistence,
  type LocalPersistenceAdapter,
} from './localStore.js'

export type LocalPersistenceOptions = {
  /** custom database filename (used by native/bun SQLite adapters) */
  filename?: string
  /** custom directory (used by Bun SQLite adapter) */
  directory?: string
  /** max bounded retries on cross-tab revision conflict (default 6 on IndexedDB) */
  maxConflictRetries?: number
}

/**
 * Browser/default local persistence for `.local({ persistence: 'device' })` tables.
 * Uses isolated IndexedDB with built-in cross-tab Web Locks, BroadcastChannel
 * peer invalidation, and bounded rebase retry. Fails loud when IndexedDB is absent.
 */
export function createLocalPersistence(
  storageKey = 'default',
  options?: LocalPersistenceOptions
): LocalPersistenceAdapter {
  if (typeof indexedDB === 'undefined') {
    throw new Error(
      `[limesync] local device persistence '${storageKey}' requires IndexedDB ` +
        `or an explicit local.persistence adapter (such as createSQLiteLocalPersistence(db)). ` +
        `Use .local({ persistence: 'memory' }) for intentional RAM-only tables.`
    )
  }
  return createIndexedDBLocalPersistence(storageKey, {
    maxConflictRetries: options?.maxConflictRetries,
  })
}
