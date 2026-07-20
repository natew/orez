import type { BedrockBrowserModule } from './sqlite-adapter.js'

const DATABASE_NAME = 'orez-sync-browser-host'
const DATABASE_VERSION = 1
const STORE_NAME = 'snapshots'
const SNAPSHOT_FORMAT_VERSION = 1

type SnapshotFile = {
  path: string
  size: number
  data: ArrayBuffer
}

type SnapshotRecord = {
  storageKey: string
  formatVersion: number
  files: SnapshotFile[]
}

function requestResult<Value>(request: IDBRequest<Value>): Promise<Value> {
  return new Promise((resolve, reject) => {
    request.addEventListener('success', () => resolve(request.result), { once: true })
    request.addEventListener(
      'error',
      () => reject(request.error ?? new Error('IndexedDB request failed')),
      { once: true }
    )
  })
}

function transactionDone(transaction: IDBTransaction): Promise<void> {
  return new Promise((resolve, reject) => {
    transaction.addEventListener('complete', () => resolve(), { once: true })
    transaction.addEventListener(
      'abort',
      () => reject(transaction.error ?? new Error('IndexedDB transaction aborted')),
      { once: true }
    )
    transaction.addEventListener(
      'error',
      () => reject(transaction.error ?? new Error('IndexedDB transaction failed')),
      { once: true }
    )
  })
}

async function openSnapshotDatabase(): Promise<IDBDatabase> {
  const request = indexedDB.open(DATABASE_NAME, DATABASE_VERSION)
  request.addEventListener('upgradeneeded', () => {
    if (!request.result.objectStoreNames.contains(STORE_NAME)) {
      request.result.createObjectStore(STORE_NAME, { keyPath: 'storageKey' })
    }
  })
  return requestResult(request)
}

export async function deleteBrowserSyncHostSnapshot(storageKey: string): Promise<void> {
  if (!storageKey) throw new TypeError('storageKey must not be empty')

  const database = await openSnapshotDatabase()
  try {
    const transaction = database.transaction(STORE_NAME, 'readwrite')
    transaction.objectStore(STORE_NAME).delete(storageKey)
    await transactionDone(transaction)
  } finally {
    database.close()
  }
}

function validateSnapshot(value: unknown, storageKey: string): SnapshotRecord {
  if (!value || typeof value !== 'object') {
    throw new Error(`invalid browser database snapshot for ${storageKey}`)
  }
  const record = value as Partial<SnapshotRecord>
  if (record.storageKey !== storageKey) {
    throw new Error(`browser database snapshot key mismatch for ${storageKey}`)
  }
  if (record.formatVersion !== SNAPSHOT_FORMAT_VERSION) {
    throw new Error(
      `unsupported browser database snapshot format ${String(record.formatVersion)}`
    )
  }
  if (!Array.isArray(record.files)) {
    throw new Error(`browser database snapshot has no files for ${storageKey}`)
  }
  const paths = new Set<string>()
  for (const file of record.files) {
    if (
      !file ||
      typeof file.path !== 'string' ||
      !Number.isSafeInteger(file.size) ||
      file.size < 0 ||
      !(file.data instanceof ArrayBuffer) ||
      file.size > file.data.byteLength ||
      paths.has(file.path)
    ) {
      throw new Error(`invalid browser database snapshot file for ${storageKey}`)
    }
    paths.add(file.path)
  }
  return record as SnapshotRecord
}

export class IndexedDbSnapshotStore {
  readonly #database: Promise<IDBDatabase>

  constructor(readonly storageKey: string) {
    if (!storageKey) throw new TypeError('storageKey must not be empty')
    this.#database = openSnapshotDatabase()
  }

  async restore(module: BedrockBrowserModule): Promise<boolean> {
    const database = await this.#database
    const transaction = database.transaction(STORE_NAME, 'readonly')
    const value = await requestResult(
      transaction.objectStore(STORE_NAME).get(this.storageKey)
    )
    await transactionDone(transaction)
    if (value === undefined) return false

    const snapshot = validateSnapshot(value, this.storageKey)
    const files: BedrockBrowserModule['_memfs']['files'] = {}
    for (const file of snapshot.files) {
      const data = new Uint8Array(file.data.slice(0))
      files[file.path] = { data, size: file.size }
    }
    module._memfs.files = files
    return true
  }

  async checkpoint(module: BedrockBrowserModule): Promise<void> {
    const files: SnapshotFile[] = Object.entries(module._memfs.files).map(
      ([path, file]) => {
        if (
          !Number.isSafeInteger(file.size) ||
          file.size < 0 ||
          file.size > file.data.length
        ) {
          throw new Error(`invalid Bedrock VFS file ${path}`)
        }
        return {
          path,
          size: file.size,
          data: file.data.slice(0, file.size).buffer,
        }
      }
    )
    const record: SnapshotRecord = {
      storageKey: this.storageKey,
      formatVersion: SNAPSHOT_FORMAT_VERSION,
      files,
    }

    const database = await this.#database
    const transaction = database.transaction(STORE_NAME, 'readwrite')
    transaction.objectStore(STORE_NAME).put(record)
    await transactionDone(transaction)
  }

  async close(): Promise<void> {
    ;(await this.#database).close()
  }
}
