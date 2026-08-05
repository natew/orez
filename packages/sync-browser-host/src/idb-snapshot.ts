import type { BedrockBrowserModule } from './sqlite-adapter.js'

const LEGACY_DATABASE_NAME = 'orez-sync-browser-host'
export const BROWSER_SYNC_HOST_DATABASE_PREFIX = 'orez-sync-browser-host:'
const MIGRATION_DATABASE_NAME = 'orez-sync-browser-host-migrations'
const DATABASE_VERSION = 2
const MANIFEST_STORE_NAME = 'snapshot-manifests'
const CHUNK_STORE_NAME = 'snapshot-chunks'
const SNAPSHOT_FORMAT_VERSION = 2
export const SNAPSHOT_CHUNK_BYTES = 64 * 1024

type ChunkHash = {
  first: number
  second: number
}

type SnapshotManifestFile = {
  path: string
  size: number
  hashes: ChunkHash[]
}

type SnapshotManifest = {
  storageKey: string
  formatVersion: number
  files: SnapshotManifestFile[]
}

type SnapshotChunk = {
  key: string
  path: string
  index: number
  data: ArrayBuffer
}

export type SnapshotCheckpointStats = {
  snapshotBytes: number
  writtenBytes: number
  snapshotChunks: number
  writtenChunks: number
}

type BedrockSnapshotModule = Pick<BedrockBrowserModule, '_memfs'>

function requestResult<Value>(request: IDBRequest<Value>): Promise<Value> {
  return new Promise((resolve, reject) => {
    request.addEventListener('success', () => resolve(request.result), {
      once: true,
    })
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

async function deleteLegacySnapshotDatabases(): Promise<void> {
  const databases = await indexedDB.databases()
  const obsoleteNames = databases.flatMap((database) => {
    if (database.name === LEGACY_DATABASE_NAME) return [database.name]
    if (database.name === MIGRATION_DATABASE_NAME) return [database.name]
    return []
  })
  await Promise.all(obsoleteNames.map((name) => deleteDatabase(name)))
}

async function openCurrentSnapshotDatabase(
  storageKey: string
): Promise<IDBDatabase | null> {
  const databaseName = snapshotDatabaseName(storageKey)
  let obsolete = false
  const request = indexedDB.open(databaseName, DATABASE_VERSION)
  request.addEventListener('upgradeneeded', (event) => {
    if (event.oldVersion === 1) {
      obsolete = true
      request.transaction?.abort()
      return
    }
    if (!request.result.objectStoreNames.contains(MANIFEST_STORE_NAME)) {
      request.result.createObjectStore(MANIFEST_STORE_NAME, {
        keyPath: 'storageKey',
      })
    }
    if (!request.result.objectStoreNames.contains(CHUNK_STORE_NAME)) {
      request.result.createObjectStore(CHUNK_STORE_NAME, { keyPath: 'key' })
    }
  })
  try {
    const database = await requestResult(request)
    database.addEventListener('versionchange', () => database.close())
    return database
  } catch (error) {
    if (obsolete) return null
    throw error
  }
}

async function openSnapshotDatabase(storageKey: string): Promise<IDBDatabase> {
  let database = await openCurrentSnapshotDatabase(storageKey)
  if (!database) {
    await deleteDatabase(snapshotDatabaseName(storageKey))
    await deleteLegacySnapshotDatabases()
    database = await openCurrentSnapshotDatabase(storageKey)
    if (!database) throw new Error(`could not replace obsolete snapshot ${storageKey}`)
  }

  const transaction = database.transaction(MANIFEST_STORE_NAME, 'readonly')
  const done = transactionDone(transaction)
  const manifest = await requestResult(
    transaction.objectStore(MANIFEST_STORE_NAME).get(storageKey)
  )
  await done
  if (manifest === undefined) await deleteLegacySnapshotDatabases()
  return database
}

function snapshotDatabaseName(storageKey: string): string {
  return `${BROWSER_SYNC_HOST_DATABASE_PREFIX}${storageKey}`
}

function chunkKey(path: string, index: number): string {
  return `${path.length}:${path}:${index}`
}

function chunkHash(data: Uint8Array, start: number, end: number): ChunkHash {
  let first = 0x811c9dc5
  let second = 0x9e3779b9
  const view = new DataView(data.buffer, data.byteOffset + start, end - start)
  const wordBytes = view.byteLength & ~3
  let offset = 0
  for (; offset < wordBytes; offset += 4) {
    const word = view.getUint32(offset, true)
    first = Math.imul(first ^ word, 0x01000193)
    second = Math.imul(second ^ word, 0x27d4eb2d)
  }
  for (; offset < view.byteLength; offset++) {
    const byte = view.getUint8(offset)
    first = Math.imul(first ^ byte, 0x01000193)
    second = Math.imul(second ^ byte, 0x27d4eb2d)
  }
  return { first: first >>> 0, second: second >>> 0 }
}

function chunkHashesEqual(left: ChunkHash, right: ChunkHash): boolean {
  return left.first === right.first && left.second === right.second
}

async function deleteDatabase(name: string): Promise<void> {
  const request = indexedDB.deleteDatabase(name)
  await requestResult(request)
}

export async function deleteBrowserSyncHostSnapshot(storageKey: string): Promise<void> {
  if (!storageKey) throw new TypeError('storageKey must not be empty')

  await navigator.locks.request(snapshotDatabaseName(storageKey), async () => {
    await deleteLegacySnapshotDatabases()
    await deleteDatabase(snapshotDatabaseName(storageKey))
  })
}

function validateManifest(value: unknown, storageKey: string): SnapshotManifest {
  if (!value || typeof value !== 'object') {
    throw new Error(`invalid browser database snapshot manifest for ${storageKey}`)
  }
  // indexeddb returns untyped structured-clone data, so validation below owns this cast.
  const manifest = value as Partial<SnapshotManifest>
  if (manifest.storageKey !== storageKey) {
    throw new Error(`browser database snapshot manifest key mismatch for ${storageKey}`)
  }
  if (manifest.formatVersion !== SNAPSHOT_FORMAT_VERSION) {
    throw new Error(
      `unsupported browser database snapshot format ${String(manifest.formatVersion)}`
    )
  }
  if (!Array.isArray(manifest.files)) {
    throw new Error(`browser database snapshot manifest has no files for ${storageKey}`)
  }
  const paths = new Set<string>()
  for (const file of manifest.files) {
    if (
      !file ||
      typeof file.path !== 'string' ||
      !Number.isSafeInteger(file.size) ||
      file.size < 0 ||
      !Array.isArray(file.hashes) ||
      file.hashes.length !== Math.ceil(file.size / SNAPSHOT_CHUNK_BYTES) ||
      file.hashes.some(
        (hash) =>
          !hash ||
          typeof hash !== 'object' ||
          !Number.isSafeInteger(Reflect.get(hash, 'first')) ||
          Reflect.get(hash, 'first') < 0 ||
          Reflect.get(hash, 'first') > 0xffffffff ||
          !Number.isSafeInteger(Reflect.get(hash, 'second')) ||
          Reflect.get(hash, 'second') < 0 ||
          Reflect.get(hash, 'second') > 0xffffffff
      ) ||
      paths.has(file.path)
    ) {
      throw new Error(`invalid browser database snapshot manifest file for ${storageKey}`)
    }
    paths.add(file.path)
  }
  return manifest as SnapshotManifest
}

function validateChunk(
  value: unknown,
  path: string,
  index: number,
  size: number
): ArrayBuffer {
  if (!value || typeof value !== 'object') {
    throw new Error(`missing browser database snapshot chunk ${path}:${index}`)
  }
  // indexeddb returns untyped structured-clone data, so validation below owns this cast.
  const chunk = value as Partial<SnapshotChunk>
  if (
    chunk.key !== chunkKey(path, index) ||
    chunk.path !== path ||
    chunk.index !== index ||
    !(chunk.data instanceof ArrayBuffer) ||
    chunk.data.byteLength !== size
  ) {
    throw new Error(`invalid browser database snapshot chunk ${path}:${index}`)
  }
  return chunk.data
}

function manifestFor(
  module: BedrockSnapshotModule,
  storageKey: string
): SnapshotManifest {
  const files = Object.entries(module._memfs.files).map(([path, file]) => {
    if (
      !Number.isSafeInteger(file.size) ||
      file.size < 0 ||
      file.size > file.data.length
    ) {
      throw new Error(`invalid Bedrock VFS file ${path}`)
    }
    const hashes: ChunkHash[] = []
    for (let offset = 0; offset < file.size; offset += SNAPSHOT_CHUNK_BYTES) {
      hashes.push(
        chunkHash(file.data, offset, Math.min(offset + SNAPSHOT_CHUNK_BYTES, file.size))
      )
    }
    return { path, size: file.size, hashes }
  })
  files.sort((left, right) => left.path.localeCompare(right.path))
  return {
    storageKey,
    formatVersion: SNAPSHOT_FORMAT_VERSION,
    files,
  }
}

export class IndexedDbSnapshotStore {
  readonly #database: Promise<IDBDatabase>
  #manifest: SnapshotManifest | null = null
  #releaseLock: (() => void) | null = null

  constructor(readonly storageKey: string) {
    if (!storageKey) throw new TypeError('storageKey must not be empty')
    const lockReleased = new Promise<void>((resolve) => {
      this.#releaseLock = resolve
    })
    this.#database = new Promise((resolve, reject) => {
      navigator.locks
        .request(snapshotDatabaseName(storageKey), async () => {
          try {
            resolve(await openSnapshotDatabase(storageKey))
            await lockReleased
          } catch (error) {
            reject(error)
          }
        })
        .catch(reject)
    })
  }

  async restore(module: BedrockSnapshotModule): Promise<boolean> {
    const database = await this.#database
    const transaction = database.transaction(
      [MANIFEST_STORE_NAME, CHUNK_STORE_NAME],
      'readonly'
    )
    const done = transactionDone(transaction)
    const [manifestValue, chunkValues] = await Promise.all([
      requestResult(transaction.objectStore(MANIFEST_STORE_NAME).get(this.storageKey)),
      requestResult(transaction.objectStore(CHUNK_STORE_NAME).getAll()),
    ])
    await done

    if (manifestValue !== undefined) {
      try {
        const manifest = validateManifest(manifestValue, this.storageKey)
        const chunks = new Map<string, unknown>()
        for (const value of chunkValues) {
          if (value && typeof value === 'object') {
            const key = Reflect.get(value, 'key')
            if (typeof key === 'string') chunks.set(key, value)
          }
        }
        const files: BedrockSnapshotModule['_memfs']['files'] = {}
        for (const file of manifest.files) {
          const data = new Uint8Array(file.size)
          for (const [index, hash] of file.hashes.entries()) {
            const offset = index * SNAPSHOT_CHUNK_BYTES
            const size = Math.min(SNAPSHOT_CHUNK_BYTES, file.size - offset)
            const chunk = validateChunk(
              chunks.get(chunkKey(file.path, index)),
              file.path,
              index,
              size
            )
            const bytes = new Uint8Array(chunk)
            if (!chunkHashesEqual(chunkHash(bytes, 0, bytes.byteLength), hash)) {
              throw new Error(
                `browser database snapshot chunk hash mismatch ${file.path}:${index}`
              )
            }
            data.set(bytes, offset)
          }
          files[file.path] = { data, size: file.size }
        }
        module._memfs.files = files
        this.#manifest = manifest
        return true
      } catch {
        const discard = database.transaction(
          [MANIFEST_STORE_NAME, CHUNK_STORE_NAME],
          'readwrite'
        )
        const discarded = transactionDone(discard)
        discard.objectStore(MANIFEST_STORE_NAME).clear()
        discard.objectStore(CHUNK_STORE_NAME).clear()
        await discarded
        this.#manifest = null
        return false
      }
    }
    return false
  }

  async checkpoint(module: BedrockSnapshotModule): Promise<SnapshotCheckpointStats> {
    const manifest = manifestFor(module, this.storageKey)
    const previousFiles = new Map(
      (this.#manifest?.files ?? []).map((file) => [file.path, file])
    )
    let snapshotBytes = 0
    let snapshotChunks = 0
    let writtenBytes = 0
    let writtenChunks = 0
    const database = await this.#database
    const transaction = database.transaction(
      [MANIFEST_STORE_NAME, CHUNK_STORE_NAME],
      'readwrite'
    )
    const done = transactionDone(transaction)
    const chunkStore = transaction.objectStore(CHUNK_STORE_NAME)

    // keep this diff and the manifest put synchronous; an await can auto-commit the atomic transaction early
    for (const file of manifest.files) {
      snapshotBytes += file.size
      snapshotChunks += file.hashes.length
      const previous = previousFiles.get(file.path)
      const source = module._memfs.files[file.path]
      for (let index = 0; index < file.hashes.length; index++) {
        const offset = index * SNAPSHOT_CHUNK_BYTES
        const currentSize = Math.min(SNAPSHOT_CHUNK_BYTES, file.size - offset)
        const previousHash = previous?.hashes[index]
        const previousSize = previous
          ? Math.min(SNAPSHOT_CHUNK_BYTES, previous.size - offset)
          : -1
        if (
          previousHash &&
          previousSize === currentSize &&
          chunkHashesEqual(previousHash, file.hashes[index])
        ) {
          continue
        }
        const data = source.data.slice(offset, offset + currentSize).buffer
        const chunk: SnapshotChunk = {
          key: chunkKey(file.path, index),
          path: file.path,
          index,
          data,
        }
        chunkStore.put(chunk)
        writtenBytes += data.byteLength
        writtenChunks++
      }
      for (
        let index = file.hashes.length;
        index < (previous?.hashes.length ?? 0);
        index++
      ) {
        chunkStore.delete(chunkKey(file.path, index))
      }
      previousFiles.delete(file.path)
    }
    for (const previous of previousFiles.values()) {
      for (let index = 0; index < previous.hashes.length; index++) {
        chunkStore.delete(chunkKey(previous.path, index))
      }
    }
    transaction.objectStore(MANIFEST_STORE_NAME).put(manifest)
    await done
    this.#manifest = manifest
    return { snapshotBytes, writtenBytes, snapshotChunks, writtenChunks }
  }

  async close(): Promise<void> {
    try {
      ;(await this.#database).close()
    } finally {
      this.#releaseLock?.()
      this.#releaseLock = null
    }
  }
}
