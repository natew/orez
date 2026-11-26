const LOCAL_DATA_PREFIX = 'limesync-local-'

// exact Replicache/Zero IndexedDB naming formats:
//   - `rep:<name>:<formatVersion>[:<schemaVersion>]` (makeIDBName)
//   - `[<testNamespace>]replicache-dbs-v<N>` (IDBDatabasesStore registry)
//   - explicit `zero-*` / `replicache-*` prefixed sync stores
const REPLICACHE_DATA_RE = /^rep:/i
const REPLICACHE_REGISTRY_RE = /^(?:[a-z0-9]+)?replicache-dbs-v\d+$/i
const SYNC_PREFIX_RE = /^(?:zero|replicache)[-_:.]/i

export function isLocalDataDB(name: string): boolean {
  return name.toLowerCase().startsWith(LOCAL_DATA_PREFIX)
}

export function matchesSyncDB(name: string): boolean {
  if (isLocalDataDB(name)) return false
  return (
    REPLICACHE_DATA_RE.test(name) ||
    REPLICACHE_REGISTRY_RE.test(name) ||
    SYNC_PREFIX_RE.test(name)
  )
}

function deleteIndexedDB(name: string): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const req = indexedDB.deleteDatabase(name)
    req.onsuccess = () => resolve()
    req.onerror = () => reject(req.error ?? new Error('failed to delete database'))
    req.onblocked = () => reject(new Error('database deletion blocked'))
  })
}

export type ClearZeroClientDataOptions = {
  /** close the sync client before clearing */
  closeZero?: () => Promise<void>
  /** optional alias for closeZero */
  closeClient?: () => Promise<void>
  /** explicit sync IndexedDB names to delete (preferred when active client IDs are known) */
  dbNames?: readonly string[]
  /** also clear local persisted tables (default: false — sync repair never wipes local state) */
  includeLocal?: boolean
  /** called with info about what was cleared */
  onCleared?: (info: { count: number; names: string[] }) => void
  /** called on error */
  onError?: (error: unknown) => void
  /** reload the page after clearing (default: true) */
  reload?: boolean
  /** delay before reload in ms (default: 1000) */
  reloadDelay?: number
}

export async function clearZeroClientData(options: ClearZeroClientDataOptions = {}) {
  const {
    closeZero,
    closeClient,
    dbNames,
    includeLocal = false,
    onCleared,
    onError,
    reload = true,
    reloadDelay = 1000,
  } = options

  try {
    const closer = closeClient ?? closeZero
    if (closer) {
      await closer().catch(() => {})
    }

    const databases = await indexedDB.databases()
    const explicitSet = dbNames ? new Set(dbNames) : null

    // only delete databases owned by sync (either exact IDs when supplied or
    // strict Replicache/Zero prefixes) and optionally local tables when
    // explicitly requested. never wipe unrelated app IndexedDB databases.
    const targets = databases.filter((db) => {
      if (!db.name) return false
      if (isLocalDataDB(db.name)) return includeLocal
      if (explicitSet) return explicitSet.has(db.name)
      return matchesSyncDB(db.name)
    })

    if (targets.length > 0) {
      await Promise.all(targets.map((db) => deleteIndexedDB(db.name!)))
    }
    const names = targets.map((db) => db.name!)
    onCleared?.({ count: names.length, names })

    if (reload) {
      setTimeout(() => {
        window.location.reload()
      }, reloadDelay)
    }
  } catch (error) {
    console.error('[limesync] error clearing client data:', error)
    onError?.(error)
  }
}

export const clearSyncClientData = clearZeroClientData
