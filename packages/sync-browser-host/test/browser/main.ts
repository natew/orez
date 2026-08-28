import {
  BROWSER_SYNC_HOST_DATABASE_PREFIX,
  createBrowserSyncHostPortClient,
  deleteBrowserSyncHostSnapshot,
} from 'orez-lite/browser'

import { IndexedDbSnapshotStore, SNAPSHOT_CHUNK_BYTES } from '../../src/idb-snapshot.js'

import type { BrowserHostTestFaultPoint } from '../../src/host.js'
import type { BrowserSyncHostPortClient } from '../../src/types.js'

type WorkerControlMessage =
  | { type: 'ready' }
  | { type: 'boot-error'; message: string; stack?: string }
  | { type: 'fault-reached'; point: BrowserHostTestFaultPoint }
  | { type: 'effect-complete'; id: string }
  | { type: 'connected'; id: string }
  | { type: 'application-transaction-effect'; id: string }
  | { type: 'application-transaction-complete'; id: string; rows: unknown[] }
  | { type: 'application-transaction-error'; id: string; message: string }
  | { type: 'application-transaction-rollback-effect'; id: string }
  | { type: 'application-transaction-rollback-complete'; id: string; message: string }
  | { type: 'application-transaction-rollback-error'; id: string; message: string }
  | { type: 'seed-wave-finance-complete'; id: string }
  | { type: 'seed-wave-finance-error'; id: string; message: string }

type Connection = {
  worker: Worker
  client: BrowserSyncHostPortClient
  attachClient(): Promise<BrowserSyncHostPortClient>
  waitForFault(point: BrowserHostTestFaultPoint): Promise<void>
  countFaults(point: BrowserHostTestFaultPoint): number
  waitForFaultCount(point: BrowserHostTestFaultPoint, count: number): Promise<void>
  waitForEffect(id: string): Promise<void>
  runApplicationTransaction(): Promise<{ rows: unknown[]; effectBeforeResolve: boolean }>
  seedWaveFinance(): Promise<void>
  runRolledBackApplicationTransaction(): Promise<{
    message: string
    effectRan: boolean
  }>
  terminate(): void
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}

function equal(actual: unknown, expected: unknown, message: string): void {
  const canonical = (value: unknown): unknown => {
    if (Array.isArray(value)) return value.map(canonical)
    if (value && typeof value === 'object' && !(value instanceof Uint8Array)) {
      return Object.fromEntries(
        Object.entries(value)
          .sort(([left], [right]) => left.localeCompare(right))
          .map(([key, entry]) => [key, canonical(entry)])
      )
    }
    return value
  }
  if (JSON.stringify(canonical(actual)) !== JSON.stringify(canonical(expected))) {
    throw new Error(
      `${message}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`
    )
  }
}

function indexedDbRequest<Value>(request: IDBRequest<Value>): Promise<Value> {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result)
    request.onerror = () => reject(request.error)
  })
}

function indexedDbTransaction(transaction: IDBTransaction): Promise<void> {
  return new Promise((resolve, reject) => {
    transaction.oncomplete = () => resolve()
    transaction.onabort = () => reject(transaction.error)
    transaction.onerror = () => reject(transaction.error)
  })
}

async function openIndexedDb(name: string, storeName: string): Promise<IDBDatabase> {
  const request = indexedDB.open(name)
  request.onupgradeneeded = () => {
    request.result.createObjectStore(storeName, { keyPath: 'storageKey' })
  }
  return indexedDbRequest(request)
}

async function openConnection(
  storageKey: string,
  faultPoint?: BrowserHostTestFaultPoint,
  checkpointFailure = false,
  slowCheckpointMs = 0
): Promise<Connection> {
  const worker = new Worker('/worker.js', { type: 'module' })
  const channel = new MessageChannel()
  const messages: WorkerControlMessage[] = []
  const waiters = new Set<() => void>()
  worker.addEventListener('message', (event: MessageEvent<WorkerControlMessage>) => {
    messages.push(event.data)
    for (const wake of waiters) wake()
  })
  const waitFor = async (
    predicate: (message: WorkerControlMessage) => boolean
  ): Promise<WorkerControlMessage> => {
    for (;;) {
      const match = messages.find(predicate)
      if (match) return match
      await new Promise<void>((resolve) => {
        const wake = () => {
          waiters.delete(wake)
          resolve()
        }
        waiters.add(wake)
      })
    }
  }
  worker.postMessage(
    {
      type: 'start',
      storageKey,
      faultPoint,
      checkpointFailure,
      slowCheckpointMs,
      port: channel.port1,
    },
    [channel.port1]
  )
  const boot = await waitFor(
    (message) => message.type === 'ready' || message.type === 'boot-error'
  )
  if (boot.type === 'boot-error') {
    worker.terminate()
    throw new Error(boot.message)
  }
  const client = createBrowserSyncHostPortClient(channel.port2)
  return {
    worker,
    client,
    async attachClient() {
      const additional = new MessageChannel()
      const id = crypto.randomUUID()
      worker.postMessage({ type: 'connect', id, port: additional.port1 }, [
        additional.port1,
      ])
      await waitFor((message) => message.type === 'connected' && message.id === id)
      return createBrowserSyncHostPortClient(additional.port2)
    },
    async waitForFault(point) {
      await waitFor(
        (message) => message.type === 'fault-reached' && message.point === point
      )
    },
    countFaults(point) {
      return messages.filter(
        (message) => message.type === 'fault-reached' && message.point === point
      ).length
    },
    async waitForFaultCount(point, count) {
      for (;;) {
        const seen = messages.filter(
          (message) => message.type === 'fault-reached' && message.point === point
        ).length
        if (seen >= count) return
        await new Promise<void>((resolve) => {
          const wake = () => {
            waiters.delete(wake)
            resolve()
          }
          waiters.add(wake)
        })
      }
    },
    async waitForEffect(id) {
      await waitFor((message) => message.type === 'effect-complete' && message.id === id)
    },
    async runApplicationTransaction() {
      const id = crypto.randomUUID()
      worker.postMessage({ type: 'application-transaction', id })
      const complete = await waitFor(
        (message) =>
          (message.type === 'application-transaction-complete' ||
            message.type === 'application-transaction-error') &&
          message.id === id
      )
      if (complete.type !== 'application-transaction-complete') {
        throw new Error(
          complete.type === 'application-transaction-error'
            ? complete.message
            : 'unexpected application transaction response'
        )
      }
      return {
        rows: complete.rows,
        effectBeforeResolve: messages.some(
          (message) =>
            message.type === 'application-transaction-effect' && message.id === id
        ),
      }
    },
    async seedWaveFinance() {
      const id = crypto.randomUUID()
      worker.postMessage({ type: 'seed-wave-finance', id })
      const complete = await waitFor(
        (message) =>
          (message.type === 'seed-wave-finance-complete' ||
            message.type === 'seed-wave-finance-error') &&
          message.id === id
      )
      if (complete.type !== 'seed-wave-finance-complete') {
        throw new Error(
          complete.type === 'seed-wave-finance-error'
            ? complete.message
            : 'unexpected wave finance seed response'
        )
      }
    },
    async runRolledBackApplicationTransaction() {
      const id = crypto.randomUUID()
      worker.postMessage({ type: 'application-transaction-rollback', id })
      const complete = await waitFor(
        (message) =>
          (message.type === 'application-transaction-rollback-complete' ||
            message.type === 'application-transaction-rollback-error') &&
          message.id === id
      )
      if (complete.type !== 'application-transaction-rollback-complete') {
        throw new Error(
          complete.type === 'application-transaction-rollback-error'
            ? complete.message
            : 'unexpected application transaction rollback response'
        )
      }
      return {
        message: complete.message,
        effectRan: messages.some(
          (message) =>
            message.type === 'application-transaction-rollback-effect' &&
            message.id === id
        ),
      }
    },
    terminate() {
      worker.terminate()
      client.close()
    },
  }
}

function mutation(
  clientID: string,
  id: number,
  name: string,
  args: Record<string, unknown>
): Record<string, unknown> {
  return {
    clientGroupID: `group-${clientID}`,
    pushVersion: 1,
    mutations: [{ type: 'custom', clientID, id, name, args: [args] }],
  }
}

async function post(
  client: BrowserSyncHostPortClient,
  path: '/pull' | '/push',
  body: unknown,
  authenticated = true
): Promise<{ status: number; body: Record<string, unknown> }> {
  const response = await client.fetch(`http://preview.invalid${path}`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      ...(authenticated ? { authorization: 'Bearer preview-token' } : {}),
    },
    body: JSON.stringify(body),
  })
  return {
    status: response.status,
    body: (await response.json()) as Record<string, unknown>,
  }
}

function isReplay(response: Record<string, unknown>): boolean {
  return JSON.stringify(response).includes('alreadyProcessed')
}

async function runFaultCase(point: BrowserHostTestFaultPoint) {
  const storageKey = `fault:${point}:${crypto.randomUUID()}`
  const clientID = `client-${point}`
  const rowID = `row-${point}`
  const body = mutation(clientID, 1, 'todo.create', {
    id: rowID,
    title: point,
  })
  const connection = await openConnection(storageKey, point)
  const fault = connection.waitForFault(point)
  void post(connection.client, '/push', body).catch(() => undefined)
  await fault
  connection.terminate()

  const restarted = await openConnection(storageKey)
  const rows = await restarted.client.query<{ count: number }>(
    'SELECT COUNT(*) AS count FROM todo WHERE id = ?',
    [rowID]
  )
  const durable =
    point === 'after_idb_commit_before_response' || point === 'during_response_delivery'
  equal(rows, [{ count: durable ? 1 : 0 }], `${point} restored row state`)
  const replay = await post(restarted.client, '/push', body)
  equal(replay.status, 200, `${point} replay status`)
  equal(isReplay(replay.body), durable, `${point} replay classification`)
  const afterReplay = await restarted.client.query<{ count: number }>(
    'SELECT COUNT(*) AS count FROM todo WHERE id = ?',
    [rowID]
  )
  equal(afterReplay, [{ count: 1 }], `${point} applies application effect once`)
  restarted.terminate()
  return { point, durable, replay: isReplay(replay.body) }
}

async function runCheckpointFailureCase() {
  const storageKey = `checkpoint-failure:${crypto.randomUUID()}`
  const body = mutation('checkpoint-client', 1, 'todo.create', {
    id: 'checkpoint-row',
    title: 'must restore old snapshot',
  })
  const connection = await openConnection(storageKey, undefined, true)
  const failed = await post(connection.client, '/push', body)
  equal(failed.status, 500, 'checkpoint failure rejects push')
  assert(
    String(failed.body.error).includes('host terminated'),
    `checkpoint failure returns a fatal durability error, got: ${String(failed.body.error)}`
  )
  let rejectedAfterFailure = false
  try {
    await connection.client.query('SELECT 1')
  } catch (error) {
    rejectedAfterFailure = String(error).includes('checkpoint failed')
  }
  assert(rejectedAfterFailure, 'host rejects operations after checkpoint failure')
  connection.terminate()

  const restarted = await openConnection(storageKey)
  const rows = await restarted.client.query<{ count: number }>(
    'SELECT COUNT(*) AS count FROM todo WHERE id = ?',
    ['checkpoint-row']
  )
  equal(rows, [{ count: 0 }], 'restart restores the last durable snapshot')
  const replay = await post(restarted.client, '/push', body)
  equal(isReplay(replay.body), false, 'failed checkpoint leaves mutation replayable')
  restarted.terminate()
  return { fatal: true, restoredOldSnapshot: true }
}

async function runSnapshotDeletionCase() {
  const targetStorageKey = `snapshot-delete-target:${crypto.randomUUID()}`
  const siblingStorageKey = `snapshot-delete-sibling:${crypto.randomUUID()}`
  const target = await openConnection(targetStorageKey)
  const sibling = await openConnection(siblingStorageKey)
  const targetSeed = await post(
    target.client,
    '/push',
    mutation('snapshot-delete-target', 1, 'todo.create', {
      id: 'target',
      title: 'delete this snapshot',
    })
  )
  const siblingSeed = await post(
    sibling.client,
    '/push',
    mutation('snapshot-delete-sibling', 1, 'todo.create', {
      id: 'sibling',
      title: 'preserve this snapshot',
    })
  )
  equal(targetSeed.status, 200, 'target snapshot seed status')
  equal(siblingSeed.status, 200, 'sibling snapshot seed status')
  target.terminate()
  sibling.terminate()

  await deleteBrowserSyncHostSnapshot(targetStorageKey)

  const freshTarget = await openConnection(targetStorageKey)
  const restoredSibling = await openConnection(siblingStorageKey)
  equal(
    await freshTarget.client.query('SELECT id FROM todo WHERE id = ?', ['target']),
    [],
    'snapshot deletion removes only the requested browser host database'
  )
  equal(
    await restoredSibling.client.query('SELECT id FROM todo WHERE id = ?', ['sibling']),
    [{ id: 'sibling' }],
    'snapshot deletion preserves sibling browser host databases'
  )
  freshTarget.terminate()
  restoredSibling.terminate()
  await deleteBrowserSyncHostSnapshot(targetStorageKey)
  await deleteBrowserSyncHostSnapshot(siblingStorageKey)
  return { deletedTarget: true, preservedSibling: true }
}

async function runLegacySnapshotDeletionCase() {
  const storageKey = `legacy-deletion:${crypto.randomUUID()}`
  const databaseName = `${BROWSER_SYNC_HOST_DATABASE_PREFIX}${storageKey}`
  const snapshot = {
    storageKey,
    formatVersion: 1,
    files: [{ path: '/project.db', size: 4, data: new Uint8Array([1, 2, 3, 4]) }],
  }

  const isolatedLegacyDatabase = await openIndexedDb(databaseName, 'snapshots')
  const isolatedLegacyTransaction = isolatedLegacyDatabase.transaction(
    'snapshots',
    'readwrite'
  )
  isolatedLegacyTransaction.objectStore('snapshots').put(snapshot)
  await indexedDbTransaction(isolatedLegacyTransaction)
  isolatedLegacyDatabase.close()

  const legacyDatabase = await openIndexedDb('orez-sync-browser-host', 'snapshots')
  const legacyTransaction = legacyDatabase.transaction('snapshots', 'readwrite')
  legacyTransaction.objectStore('snapshots').put(snapshot)
  await indexedDbTransaction(legacyTransaction)
  legacyDatabase.close()

  const migrationDatabase = await openIndexedDb(
    'orez-sync-browser-host-migrations',
    'migrations'
  )
  const migrationTransaction = migrationDatabase.transaction('migrations', 'readwrite')
  migrationTransaction.objectStore('migrations').put({ storageKey: 'legacy-marker' })
  await indexedDbTransaction(migrationTransaction)
  migrationDatabase.close()

  const fresh = await openConnection(storageKey)
  equal(
    await fresh.client.query('SELECT id, title FROM todo'),
    [],
    'legacy snapshot is discarded instead of restored'
  )
  fresh.terminate()

  const databases = await indexedDB.databases()
  assert(
    !databases.some(({ name }) => name === 'orez-sync-browser-host'),
    'legacy shared database is deleted'
  )
  assert(
    !databases.some(({ name }) => name === 'orez-sync-browser-host-migrations'),
    'legacy marker database is deleted'
  )
  const v2Database = await openIndexedDb(databaseName, 'unused')
  assert(v2Database.version === 2, 'fresh snapshot database uses v2 schema')
  assert(
    !v2Database.objectStoreNames.contains('snapshots'),
    'fresh snapshot database has no legacy store'
  )
  assert(
    v2Database.objectStoreNames.contains('snapshot-manifests') &&
      v2Database.objectStoreNames.contains('snapshot-chunks'),
    'fresh snapshot database has v2 stores'
  )
  v2Database.close()
  await deleteBrowserSyncHostSnapshot(storageKey)
  return { discarded: true, sharedDatabasesDeleted: true, bootedFresh: true }
}

async function runChunkChangeDetectionCase() {
  const zeroStorageKey = `chunk-zero-growth:${crypto.randomUUID()}`
  const zeroStore = new IndexedDbSnapshotStore(zeroStorageKey)
  const zeroData = new Uint8Array(SNAPSHOT_CHUNK_BYTES)
  const zeroPrefix = 0xc9dc5
  zeroData[0] = zeroPrefix & 0xff
  zeroData[1] = (zeroPrefix >>> 8) & 0xff
  zeroData[2] = (zeroPrefix >>> 16) & 0xff
  zeroData[3] = (zeroPrefix >>> 24) & 0xff
  const zeroModule = {
    _memfs: {
      files: { '/project.db': { data: zeroData, size: 4 } },
      fds: {},
      nextFd: 1,
    },
  }
  await zeroStore.checkpoint(zeroModule)
  zeroModule._memfs.files['/project.db'].size = 4 + 4096
  const zeroGrowth = await zeroStore.checkpoint(zeroModule)
  await zeroStore.close()
  let zeroGrowthRestored = false
  const zeroRestore = new IndexedDbSnapshotStore(zeroStorageKey)
  const zeroRestoredModule = {
    _memfs: {
      files: {} as Record<string, { data: Uint8Array; size: number }>,
      fds: {},
      nextFd: 1,
    },
  }
  try {
    zeroGrowthRestored = await zeroRestore.restore(zeroRestoredModule)
  } catch {}
  await zeroRestore.close()

  const collisionStorageKey = `chunk-collision:${crypto.randomUUID()}`
  const collisionStore = new IndexedDbSnapshotStore(collisionStorageKey)
  const oldBytes = new Uint8Array([35, 67, 0, 0, 51, 246, 110, 38])
  const newBytes = new Uint8Array([175, 150, 0, 0, 255, 229, 57, 169])
  const collisionModule = {
    _memfs: {
      files: { '/project.db': { data: oldBytes.slice(), size: oldBytes.length } },
      fds: {},
      nextFd: 1,
    },
  }
  await collisionStore.checkpoint(collisionModule)
  collisionModule._memfs.files['/project.db'].data.set(newBytes)
  const collision = await collisionStore.checkpoint(collisionModule)
  await collisionStore.close()
  const collisionRestore = new IndexedDbSnapshotStore(collisionStorageKey)
  const collisionRestoredModule = {
    _memfs: {
      files: {} as Record<string, { data: Uint8Array; size: number }>,
      fds: {},
      nextFd: 1,
    },
  }
  const collisionRestored = await collisionRestore.restore(collisionRestoredModule)
  await collisionRestore.close()
  const collisionBytesMatch =
    collisionRestored &&
    collisionRestoredModule._memfs.files['/project.db'].data.every(
      (byte, index) => byte === newBytes[index]
    )

  await deleteBrowserSyncHostSnapshot(zeroStorageKey)
  await deleteBrowserSyncHostSnapshot(collisionStorageKey)
  assert(
    zeroGrowth.writtenChunks === 1 &&
      zeroGrowthRestored &&
      collision.writtenChunks === 1 &&
      collisionBytesMatch,
    `chunk detector missed changes: ${JSON.stringify({
      zeroGrowthWritten: zeroGrowth.writtenChunks,
      zeroGrowthRestored,
      collisionWritten: collision.writtenChunks,
      collisionBytesMatch,
    })}`
  )
  return { zeroGrowth: true, collision: true }
}

async function runSnapshotStoreSerializationCase() {
  const storageKey = `snapshot-store-lock:${crypto.randomUUID()}`
  const first = new IndexedDbSnapshotStore(storageKey)
  const module = {
    _memfs: {
      files: {
        '/project.db': { data: new Uint8Array([1, 2, 3, 4]), size: 4 },
      },
      fds: {},
      nextFd: 1,
    },
  }
  await first.checkpoint(module)
  const second = new IndexedDbSnapshotStore(storageKey)
  const restoredModule = {
    _memfs: {
      files: {} as Record<string, { data: Uint8Array; size: number }>,
      fds: {},
      nextFd: 1,
    },
  }
  const restore = second.restore(restoredModule)
  const beforeClose = await Promise.race([
    restore.then(() => 'restored'),
    new Promise<'waiting'>((resolve) => setTimeout(() => resolve('waiting'), 50)),
  ])
  equal(beforeClose, 'waiting', 'second snapshot store waits for the first store lock')
  await first.close()
  equal(await restore, true, 'second snapshot store restores after the first closes')
  equal(
    Array.from(restoredModule._memfs.files['/project.db'].data),
    [1, 2, 3, 4],
    'serialized snapshot store restores complete bytes'
  )
  await second.close()
  await deleteBrowserSyncHostSnapshot(storageKey)
  return { waitedForClose: true, restored: true }
}

async function runInvalidSnapshotCacheMissCase() {
  const storageKey = `invalid-snapshot:${crypto.randomUUID()}`
  const databaseName = `${BROWSER_SYNC_HOST_DATABASE_PREFIX}${storageKey}`
  const first = new IndexedDbSnapshotStore(storageKey)
  await first.checkpoint({
    _memfs: {
      files: {
        '/project.db': { data: new Uint8Array([1, 2, 3, 4]), size: 4 },
      },
      fds: {},
      nextFd: 1,
    },
  })
  await first.close()

  const database = await openIndexedDb(databaseName, 'unused')
  const corrupt = database.transaction('snapshot-chunks', 'readwrite')
  corrupt.objectStore('snapshot-chunks').clear()
  await indexedDbTransaction(corrupt)
  database.close()

  const cacheMiss = new IndexedDbSnapshotStore(storageKey)
  const restored = await cacheMiss.restore({
    _memfs: { files: {}, fds: {}, nextFd: 1 },
  })
  equal(restored, false, 'invalid snapshot is discarded as a cache miss')
  const freshBytes = new Uint8Array([5, 6, 7, 8])
  await cacheMiss.checkpoint({
    _memfs: {
      files: { '/project.db': { data: freshBytes, size: freshBytes.length } },
      fds: {},
      nextFd: 1,
    },
  })
  await cacheMiss.close()

  const reopened = new IndexedDbSnapshotStore(storageKey)
  const reopenedModule = {
    _memfs: {
      files: {} as Record<string, { data: Uint8Array; size: number }>,
      fds: {},
      nextFd: 1,
    },
  }
  equal(await reopened.restore(reopenedModule), true, 'fresh checkpoint restores')
  equal(
    Array.from(reopenedModule._memfs.files['/project.db'].data),
    Array.from(freshBytes),
    'fresh checkpoint replaces the discarded cache'
  )
  await reopened.close()
  await deleteBrowserSyncHostSnapshot(storageKey)
  return { discarded: true, rebuilt: true }
}

async function runSteadyStateDatabaseDiscoveryCase() {
  const storageKey = `steady-state-discovery:${crypto.randomUUID()}`
  const first = new IndexedDbSnapshotStore(storageKey)
  await first.checkpoint({
    _memfs: {
      files: {
        '/project.db': { data: new Uint8Array([1, 2, 3, 4]), size: 4 },
      },
      fds: {},
      nextFd: 1,
    },
  })
  await first.close()

  const originalDatabases = indexedDB.databases
  let calls = 0
  Object.defineProperty(indexedDB, 'databases', {
    configurable: true,
    value: () => {
      calls++
      throw new Error('steady-state boot called indexedDB.databases()')
    },
  })
  try {
    const reopened = new IndexedDbSnapshotStore(storageKey)
    equal(
      await reopened.restore({ _memfs: { files: {}, fds: {}, nextFd: 1 } }),
      true,
      'steady-state snapshot restores without origin database discovery'
    )
    await reopened.close()
  } finally {
    Object.defineProperty(indexedDB, 'databases', {
      configurable: true,
      value: originalDatabases,
    })
  }
  equal(calls, 0, 'steady-state boot makes no origin database discovery call')
  await deleteBrowserSyncHostSnapshot(storageKey)
  return { databaseDiscoveryCalls: calls }
}

async function runIncrementalCheckpointCase() {
  const entries = Array.from({ length: 8 }, (_, index) => {
    const storageKey = `incremental-checkpoint:${index}:${crypto.randomUUID()}`
    return {
      storageKey,
      store: new IndexedDbSnapshotStore(storageKey),
      module: {
        _memfs: {
          files: {
            '/project.db': {
              data: new Uint8Array(8 * 1024 * 1024),
              size: 8 * 1024 * 1024,
            },
          },
          fds: {},
          nextFd: 1,
        },
      },
    }
  })
  for (const entry of entries) {
    equal(await entry.store.restore(entry.module), false, 'fresh chunk store is empty')
  }
  const initial = await Promise.all(
    entries.map((entry) => entry.store.checkpoint(entry.module))
  )
  for (const [index, entry] of entries.entries()) {
    entry.module._memfs.files['/project.db'].data[index * 4096] = index + 1
  }
  const incremental = await Promise.all(
    entries.map((entry) => entry.store.checkpoint(entry.module))
  )
  const initialBytes = initial.reduce((sum, result) => sum + result.writtenBytes, 0)
  const snapshotBytes = incremental.reduce((sum, result) => sum + result.snapshotBytes, 0)
  const writtenBytes = incremental.reduce((sum, result) => sum + result.writtenBytes, 0)
  const writtenChunks = incremental.reduce((sum, result) => sum + result.writtenChunks, 0)
  equal(initialBytes, 8 * 8 * 1024 * 1024, 'initial checkpoint writes every byte')
  equal(snapshotBytes, 8 * 8 * 1024 * 1024, 'incremental checkpoint scans full snapshots')
  equal(
    writtenBytes,
    8 * SNAPSHOT_CHUNK_BYTES,
    'incremental checkpoint writes changed chunks'
  )
  equal(writtenChunks, 8, 'incremental checkpoint writes one chunk per worker')
  for (const entry of entries) {
    await entry.store.close()
    const restored = new IndexedDbSnapshotStore(entry.storageKey)
    const files: Record<string, { data: Uint8Array; size: number }> = {}
    const module = { _memfs: { files, fds: {}, nextFd: 1 } }
    equal(await restored.restore(module), true, 'incremental snapshot restores')
    equal(
      module._memfs.files['/project.db'].data,
      entry.module._memfs.files['/project.db'].data,
      'incremental snapshot preserves changed bytes'
    )
    await restored.close()
    await deleteBrowserSyncHostSnapshot(entry.storageKey)
  }
  return { initialBytes, snapshotBytes, writtenBytes, writtenChunks }
}

// a checkpoint that promises durability to nobody must not sit on the operation
// queue. w156 measured 25-68s IndexedDB commits for an unchanged 172KB snapshot,
// and every read, pull and forwarded auth request queued behind them; both ends
// of the native auth bridge give up at 30s.
async function runCheckpointDecouplingCase() {
  const slowMs = 600
  const storageKey = `checkpoint-decoupling:${crypto.randomUUID()}`
  const connection = await openConnection(storageKey, undefined, false, slowMs)
  const readCount = async () => {
    const rows = await connection.client.query<{ count: number }>(
      'SELECT COUNT(*) AS count FROM todo'
    )
    return rows[0]!.count
  }

  // the first checkpoint writes the whole database; everything below measures
  // steady state, where only changed chunks move
  const warmup = await post(
    connection.client,
    '/push',
    mutation('decoupling-warmup', 1, 'todo.create', {
      id: 'decoupling-warmup',
      title: 'warmup',
    })
  )
  equal(warmup.status, 200, 'decoupling warmup push status')

  // a pull writes and snapshots, and a lost pull simply re-pulls. the read below
  // is issued only after the worker reports that pull's commit has begun, so it
  // cannot win a race into the operation queue ahead of the pull.
  const seenCommits = connection.countFaults('before_snapshot_commit')
  const pull = post(connection.client, '/pull', {
    clientID: 'decoupling-client',
    clientGroupID: 'decoupling-group',
    cookie: null,
    queries: {
      version: 1,
      patch: [{ op: 'put', hash: 'q-all-todos', name: 'allTodos', args: [] }],
    },
  })
  await connection.waitForFaultCount('before_snapshot_commit', seenCommits + 1)
  const readStartedAt = performance.now()
  await readCount()
  const readBehindPullMs = performance.now() - readStartedAt
  equal((await pull).status, 200, 'decoupling pull status')

  // a push is acked to its client as durable, so it still waits for its snapshot
  const pushStartedAt = performance.now()
  const push = await post(
    connection.client,
    '/push',
    mutation('decoupling-push', 1, 'todo.create', {
      id: 'decoupling-push',
      title: 'push',
    })
  )
  const pushMs = performance.now() - pushStartedAt
  equal(push.status, 200, 'decoupling push status')

  assert(
    readBehindPullMs < slowMs / 2,
    `a read must not wait for a pull's snapshot commit (waited ${readBehindPullMs.toFixed(0)}ms of a ${slowMs}ms commit)`
  )
  assert(
    pushMs >= slowMs,
    `a push must still wait for its own snapshot commit (returned after ${pushMs.toFixed(0)}ms of a ${slowMs}ms commit)`
  )

  // captures pile up ahead of their commits here. every one of them copied the
  // wasm heap while the next write was already queued, so a torn capture would
  // restore a database state that never existed.
  for (let index = 0; index < 3; index++) {
    const response = await post(
      connection.client,
      '/push',
      mutation(`decoupling-load-${index}`, 1, 'todo.create', {
        id: `decoupling-load-${index}`,
        title: `load ${index}`,
      })
    )
    equal(response.status, 200, `decoupling load push ${index} status`)
  }
  // commits land in capture order, so the last push's awaited snapshot proves
  // every handed-off one before it already landed
  const written = await readCount()
  connection.terminate()

  const restarted = await openConnection(storageKey)
  const restored = await restarted.client.query<{ count: number }>(
    'SELECT COUNT(*) AS count FROM todo'
  )
  equal(restored, [{ count: written }], 'handed-off snapshots restore every row')
  restarted.terminate()
  await deleteBrowserSyncHostSnapshot(storageKey)

  return {
    slowMs,
    readBehindPullMs: Math.round(readBehindPullMs),
    pushMs: Math.round(pushMs),
    restoredRows: written,
  }
}

async function runHybridCaptureCase() {
  const connection = await openConnection(`hybrid-capture:${crypto.randomUUID()}`)
  const exact = await post(
    connection.client,
    '/push',
    mutation('helper-exact', 1, 'test.helperExact', { id: 'helper-exact' })
  )
  equal(exact.status, 200, 'browser exact raw write set commits')

  for (const [clientID, name, status] of [
    ['raw-write', 'test.rawWrite', 200],
    ['mixed', 'test.mixedCapture', 200],
    ['helper-wrong', 'test.helperWrong', 500],
    ['raw-query', 'test.rawQueryWrite', 200],
  ] as const) {
    const response = await post(
      connection.client,
      '/push',
      mutation(clientID, 1, name, { id: clientID })
    )
    equal(response.status, status, `browser ${clientID} uses its capture lane`)
  }

  await connection.client.exec(
    'INSERT INTO todo (id, title, done) VALUES (?, ?, ?)',
    ['helper-direct-exact', 'direct exact', 0],
    {
      table: 'todo',
      publicTable: 'todo',
      kind: 'insert',
      capture: 'exact',
      primaryKeys: [{ after: { id: 'helper-direct-exact' } }],
    }
  )
  await connection.client.exec('INSERT INTO todo (id, title, done) VALUES (?, ?, ?)', [
    'raw-direct',
    'direct raw trigger',
    0,
  ])

  equal(
    await connection.client.query('SELECT id FROM todo ORDER BY id'),
    [
      { id: 'helper-direct-exact' },
      { id: 'helper-exact' },
      { id: 'mixed-helper' },
      { id: 'mixed-raw' },
      { id: 'raw-direct' },
      { id: 'raw-query' },
      { id: 'raw-write' },
    ],
    'browser helper and raw writes commit while wrong helper keys roll back'
  )
  equal(
    await connection.client.query(
      "SELECT clientID FROM _zsync_clients WHERE clientID IN ('helper-exact', 'helper-wrong', 'mixed', 'raw-query', 'raw-write') ORDER BY clientID"
    ),
    [
      { clientID: 'helper-exact' },
      { clientID: 'mixed' },
      { clientID: 'raw-query' },
      { clientID: 'raw-write' },
    ],
    'browser wrong helper keys do not consume a mutation id'
  )
  connection.terminate()
  return { helper: true, raw: true, wrongHelperRolledBack: true }
}

async function runExactTriggerSideEffectCase() {
  const connection = await openConnection(`exact-trigger:${crypto.randomUUID()}`)
  const initial = await post(connection.client, '/pull', {
    clientID: 'summary-reader',
    clientGroupID: 'summary-group',
    cookie: null,
    queries: {
      version: 1,
      patch: [
        { op: 'put', hash: 'expense-summaries', name: 'allExpenseSummaries', args: [] },
      ],
    },
  })
  equal(initial.status, 200, 'exact-trigger initial pull status')

  const pushed = await post(
    connection.client,
    '/push',
    mutation('summary-writer', 1, 'test.expenseExact', {
      id: 'exact-trigger-expense',
      amount: 4250,
      category: 'Food',
      date: 1,
    })
  )
  equal(pushed.status, 200, 'exact-trigger push status')

  const incremental = await post(connection.client, '/pull', {
    clientID: 'summary-reader',
    clientGroupID: 'summary-group',
    cookie: initial.body.cookie,
  })
  equal(incremental.status, 200, 'exact-trigger incremental pull status')
  assert(
    (incremental.body.rowsPatch as Array<Record<string, unknown>>).some(
      (entry) =>
        entry.op === 'put' &&
        entry.tableName === 'expenseSummary' &&
        (
          entry.value as {
            category?: unknown
            expenseCount?: unknown
            totalAmount?: unknown
          }
        )?.category === 'Food' &&
        (entry.value as { expenseCount?: unknown }).expenseCount === 1 &&
        (entry.value as { totalAmount?: unknown }).totalAmount === 4250
    ),
    'incremental pull includes the database-trigger-derived summary row'
  )
  connection.terminate()
  return { summaryUpdated: true }
}

async function runBrowserHostSpike() {
  const storageKey = `browser-host:${crypto.randomUUID()}`
  let connection = await openConnection(storageKey)
  let wakes = 0
  connection.client.subscribe(() => wakes++)
  const secondClient = await connection.attachClient()
  let secondWakes = 0
  secondClient.subscribe(() => secondWakes++)

  const unauthorized = await post(
    connection.client,
    '/pull',
    { clientID: 'unauthorized', clientGroupID: 'unauthorized', cookie: null },
    false
  )
  equal(unauthorized.status, 401, 'pull authentication')

  const initial = await post(connection.client, '/pull', {
    clientID: 'client-main',
    clientGroupID: 'group-client-main',
    cookie: null,
    queries: {
      version: 1,
      patch: [{ op: 'put', hash: 'q-all-todos', name: 'allTodos', args: [] }],
    },
  })
  equal(initial.status, 200, 'initial pull status')
  assert('cookie' in initial.body, 'initial pull returns a cookie')

  await connection.client.exec(
    'CREATE TABLE IF NOT EXISTS blob_probe (id TEXT PRIMARY KEY, payload BLOB NOT NULL)'
  )
  const blobInsert = await connection.client.exec(
    'INSERT INTO blob_probe (id, payload) VALUES (?, ?)',
    ['blob', Uint8Array.from([0, 127, 128, 255])]
  )
  equal(blobInsert, { changes: 1 }, 'direct SQL reports affected rows')
  const blobRows = await connection.client.query<{ payload: Uint8Array }>(
    'SELECT payload FROM blob_probe WHERE id = ?',
    ['blob']
  )
  equal(
    Array.from(blobRows[0]?.payload ?? []),
    [0, 127, 128, 255],
    'Bedrock browser BLOB values cross the worker port'
  )
  equal(wakes, 2, 'direct SQL wakes the first attached client')
  equal(secondWakes, 2, 'direct SQL wakes the second attached client')
  wakes = 0
  secondWakes = 0

  const rolledBackApplicationTransaction =
    await connection.runRolledBackApplicationTransaction()
  equal(
    rolledBackApplicationTransaction.message,
    'rollback requested',
    'application transaction returns the callback error'
  )
  equal(
    rolledBackApplicationTransaction.effectRan,
    false,
    'application transaction drops deferred effects on rollback'
  )
  equal(
    await connection.client.query('SELECT id FROM todo WHERE id = ?', [
      'application-transaction-rollback',
    ]),
    [],
    'application transaction rolls back row writes'
  )
  equal(wakes, 0, 'rolled-back application transaction does not wake clients')
  equal(secondWakes, 0, 'rolled-back application transaction does not wake peer clients')

  const applicationTransaction = await connection.runApplicationTransaction()
  equal(
    applicationTransaction.rows,
    [{ id: 'application-transaction', title: 'trusted', done: false }],
    'application transaction materializes queryAst rows'
  )
  equal(
    applicationTransaction.effectBeforeResolve,
    true,
    'application transaction runs deferred effects before resolving'
  )
  equal(wakes, 1, 'application transaction wakes the first attached client')
  equal(secondWakes, 1, 'application transaction wakes the second attached client')
  wakes = 0
  secondWakes = 0

  await connection.seedWaveFinance()
  const seedSqlCounts = await connection.client.query<{
    tableName: string
    count: number
  }>(
    `SELECT 'budget' AS tableName, COUNT(*) AS count FROM budget
     UNION ALL SELECT 'expense', COUNT(*) FROM expense
     UNION ALL SELECT 'savingsGoal', COUNT(*) FROM savingsGoal`
  )
  equal(
    seedSqlCounts,
    [
      { tableName: 'budget', count: 7 },
      { tableName: 'expense', count: 9 },
      { tableName: 'savingsGoal', count: 2 },
    ],
    'wave finance seed direct SQL counts'
  )
  const seedChanges = await connection.client.query<{ tableName: string; count: number }>(
    `SELECT json_extract(change.value, '$[0]') AS tableName, COUNT(*) AS count
     FROM _zsync_log_segments AS segment,
          json_each(segment.payload, '$.transactions') AS tx,
          json_each(tx.value, '$.changes') AS change
     WHERE json_extract(change.value, '$[0]') IN ('budget', 'expense', 'savingsGoal')
     GROUP BY tableName ORDER BY tableName`
  )
  equal(
    seedChanges,
    [
      { tableName: 'budget', count: 7 },
      { tableName: 'expense', count: 9 },
      { tableName: 'savingsGoal', count: 2 },
    ],
    'wave finance seed enters the change stream'
  )
  const seedTriggers = await connection.client.query<{
    tableName: string
    count: number
  }>(
    `SELECT tbl_name AS tableName, COUNT(*) AS count FROM sqlite_master
     WHERE type = 'trigger' AND name LIKE '_zsync_tr_%'
       AND tbl_name IN ('budget', 'expense', 'savingsGoal')
     GROUP BY tbl_name ORDER BY tbl_name`
  )
  equal(
    seedTriggers,
    [
      { tableName: 'budget', count: 3 },
      { tableName: 'expense', count: 3 },
      { tableName: 'savingsGoal', count: 3 },
    ],
    'wave finance tables have insert, update, and delete change triggers'
  )
  const seedQueryPull = await post(connection.client, '/pull', {
    clientID: 'wave-finance-client',
    clientGroupID: 'wave-finance-group',
    cookie: null,
    queries: {
      version: 1,
      patch: [
        { op: 'put', hash: 'all-expenses', name: 'allExpenses', args: [] },
        { op: 'put', hash: 'all-budgets', name: 'allBudgets', args: [] },
        { op: 'put', hash: 'all-savings-goals', name: 'allSavingsGoals', args: [] },
      ],
    },
  })
  equal(seedQueryPull.status, 200, 'wave finance fresh named query status')
  const seedQueryCounts = Object.fromEntries(
    ['budget', 'expense', 'savingsGoal'].map((tableName) => [
      tableName,
      (seedQueryPull.body.rowsPatch as Array<Record<string, unknown>>).filter(
        (entry) => entry.op === 'put' && entry.tableName === tableName
      ).length,
    ])
  )
  equal(
    seedQueryCounts,
    { budget: 7, expense: 9, savingsGoal: 2 },
    'wave finance fresh named query counts match direct SQL'
  )
  const seedWatermark = await connection.client.query<{ high: number; log: number }>(
    `SELECT endVersion AS high, endVersion AS log
     FROM _zsync_log_segments ORDER BY startVersion DESC LIMIT 1`
  )
  equal(seedWatermark, [{ high: 19, log: 19 }], 'wave finance pull advances watermark')
  equal(wakes, 1, 'wave finance seed wakes the first attached client')
  equal(secondWakes, 1, 'wave finance seed wakes the second attached client')
  wakes = 0
  secondWakes = 0

  const createBody = mutation('client-main', 1, 'todo.create', {
    id: 'persistent',
    title: 'first',
  })
  const created = await post(connection.client, '/push', createBody)
  equal(created.status, 200, 'create push status')
  equal(isReplay(created.body), false, 'first create is not a replay')
  equal(wakes, 1, 'durable push sends one advisory wake')
  equal(secondWakes, 1, 'durable push wakes every attached client')

  const querySeed = await post(
    connection.client,
    '/push',
    mutation('query-seed', 1, 'todo.create', {
      id: 'query-done',
      title: 'query member',
      done: true,
    })
  )
  equal(querySeed.status, 200, 'query seed status')
  const queryPull = await post(connection.client, '/pull', {
    clientID: 'query-client',
    clientGroupID: 'query-group',
    cookie: null,
    queries: {
      version: 1,
      patch: [{ op: 'put', hash: 'done', name: 'todosDone', args: [] }],
    },
  })
  equal(queryPull.status, 200, 'scoped query pull status')
  equal(
    queryPull.body.gotQueries,
    { version: 1, patch: [{ op: 'put', hash: 'done' }] },
    'named query is resolved and acknowledged'
  )
  const queryRows = (queryPull.body.rowsPatch as Array<Record<string, unknown>>)
    .filter((entry) => entry.op === 'put' && entry.tableName === 'todo')
    .map((entry) => (entry.value as { id: string }).id)
  equal(queryRows, ['query-done'], 'scoped query pull includes only matching rows')

  const tag = await post(
    connection.client,
    '/push',
    mutation('query-read', 1, 'todo.addTag', {
      id: 'tag-1',
      todoId: 'query-done',
      label: 'important',
    })
  )
  equal(tag.status, 200, 'transaction query tag seed status')
  const copied = await post(
    connection.client,
    '/push',
    mutation('query-read', 2, 'todo.copyFromQuery', {
      sourceId: 'query-done',
      targetId: 'query-copy',
    })
  )
  equal(copied.status, 200, 'transaction query mutation status')
  equal(
    await connection.client.query('SELECT title, done FROM todo WHERE id = ?', [
      'query-copy',
    ]),
    [{ title: 'query member:important', done: 1 }],
    'browser mutator hydrates a related transaction query'
  )

  const inserted = await connection.client.query(
    'SELECT id, title, done FROM todo WHERE id = ?',
    ['persistent']
  )
  equal(
    inserted,
    [{ id: 'persistent', title: 'first', done: 0 }],
    'create commits application row'
  )

  const incremental = await post(connection.client, '/pull', {
    clientID: 'client-main',
    clientGroupID: 'group-client-main',
    cookie: initial.body.cookie,
  })
  equal(incremental.status, 200, 'incremental pull status')
  const insertedPatch = incremental.body.rowsPatch as Array<Record<string, unknown>>
  assert(
    insertedPatch.some(
      (entry) =>
        entry.op === 'put' &&
        entry.tableName === 'todo' &&
        (entry.value as { id?: unknown })?.id === 'persistent'
    ),
    'incremental pull includes inserted todo'
  )

  const replay = await post(connection.client, '/push', createBody)
  equal(replay.status, 200, 'replay status')
  equal(isReplay(replay.body), true, 'duplicate mutation is classified as replay')
  const oneRow = await connection.client.query(
    'SELECT COUNT(*) AS count FROM todo WHERE id = ?',
    ['persistent']
  )
  equal(oneRow, [{ count: 1 }], 'replay does not duplicate application effect')

  const deferredEffect = connection.waitForEffect('deferred')
  const deferred = await post(
    connection.client,
    '/push',
    mutation('effect-client', 1, 'todo.createDeferred', {
      id: 'deferred',
      title: 'post-commit',
    })
  )
  equal(deferred.status, 200, 'deferred-effect mutation status')
  await deferredEffect
  const deferredRows = await connection.client.query(
    'SELECT title FROM todo WHERE id = ?',
    ['deferred']
  )
  equal(
    deferredRows,
    [{ title: 'post-commit' }],
    'deferred effect runs after durable application commit'
  )

  const renamed = await post(
    connection.client,
    '/push',
    mutation('client-main', 2, 'todo.rename', {
      id: 'persistent',
      title: 'renamed',
    })
  )
  equal(renamed.status, 200, 'rename status')
  const renamePull = await post(connection.client, '/pull', {
    clientID: 'client-main',
    clientGroupID: 'group-client-main',
    cookie: incremental.body.cookie,
  })
  assert(
    (renamePull.body.rowsPatch as Array<Record<string, unknown>>).some(
      (entry) =>
        entry.op === 'put' && (entry.value as { title?: unknown })?.title === 'renamed'
    ),
    'incremental pull includes update'
  )

  const temporary = await post(
    connection.client,
    '/push',
    mutation('client-main', 3, 'todo.create', { id: 'temporary', title: 'delete me' })
  )
  equal(temporary.status, 200, 'temporary create status')
  const beforeDelete = await post(connection.client, '/pull', {
    clientID: 'client-main',
    clientGroupID: 'group-client-main',
    cookie: renamePull.body.cookie,
  })
  const deleted = await post(
    connection.client,
    '/push',
    mutation('client-main', 4, 'todo.delete', { id: 'temporary' })
  )
  equal(deleted.status, 200, 'delete status')
  const deletePull = await post(connection.client, '/pull', {
    clientID: 'client-main',
    clientGroupID: 'group-client-main',
    cookie: beforeDelete.body.cookie,
  })
  assert(
    (deletePull.body.rowsPatch as Array<Record<string, unknown>>).some(
      (entry) => entry.op === 'del' && entry.tableName === 'todo'
    ),
    'incremental pull includes delete'
  )

  secondClient.close()
  connection.terminate()
  connection = await openConnection(storageKey)
  const restoredOnce = await connection.client.query(
    'SELECT id, title FROM todo WHERE id = ?',
    ['persistent']
  )
  equal(
    restoredOnce,
    [{ id: 'persistent', title: 'renamed' }],
    'first worker restart restores committed database'
  )
  connection.terminate()
  connection = await openConnection(storageKey)
  const restoredTwice = await connection.client.query(
    'SELECT id, title FROM todo WHERE id = ?',
    ['persistent']
  )
  equal(
    restoredTwice,
    [{ id: 'persistent', title: 'renamed' }],
    'second worker restart restores the same database'
  )
  connection.terminate()

  const faultPoints: BrowserHostTestFaultPoint[] = [
    'before_mutation',
    'after_app_write_before_sqlite_commit',
    'after_sqlite_commit_before_idb_commit',
    'after_idb_commit_before_response',
    'during_response_delivery',
  ]
  const faults = []
  for (const point of faultPoints) faults.push(await runFaultCase(point))
  const checkpointFailure = await runCheckpointFailureCase()
  const snapshotDeletion = await runSnapshotDeletionCase()
  const legacySnapshotDeletion = await runLegacySnapshotDeletionCase()
  const chunkChangeDetection = await runChunkChangeDetectionCase()
  const snapshotStoreSerialization = await runSnapshotStoreSerializationCase()
  const invalidSnapshotCacheMiss = await runInvalidSnapshotCacheMissCase()
  const steadyStateDatabaseDiscovery = await runSteadyStateDatabaseDiscoveryCase()
  const incrementalCheckpoint = await runIncrementalCheckpointCase()
  const checkpointDecoupling = await runCheckpointDecouplingCase()
  const hybridCapture = await runHybridCaptureCase()
  const exactTriggerSideEffect = await runExactTriggerSideEffectCase()

  const result = {
    initialCookie: initial.body.cookie,
    wakes,
    restored: restoredTwice,
    faults,
    checkpointFailure,
    snapshotDeletion,
    legacySnapshotDeletion,
    chunkChangeDetection,
    snapshotStoreSerialization,
    invalidSnapshotCacheMiss,
    steadyStateDatabaseDiscovery,
    incrementalCheckpoint,
    checkpointDecoupling,
    hybridCapture,
    exactTriggerSideEffect,
    seedProbe: {
      sqlCounts: seedSqlCounts,
      changeCounts: seedChanges,
      triggerCounts: seedTriggers,
      queryCounts: seedQueryCounts,
      watermark: seedWatermark,
    },
  }
  const output = document.querySelector('#result')
  if (output) output.textContent = JSON.stringify(result, null, 2)
  return result
}

;(
  globalThis as unknown as { runBrowserHostSpike: typeof runBrowserHostSpike }
).runBrowserHostSpike = runBrowserHostSpike
