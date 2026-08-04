import type { BedrockBrowserModule } from "./sqlite-adapter.js";

const LEGACY_DATABASE_NAME = "orez-sync-browser-host";
export const BROWSER_SYNC_HOST_DATABASE_PREFIX = "orez-sync-browser-host:";
const MIGRATION_DATABASE_NAME = "orez-sync-browser-host-migrations";
const MIGRATION_LOCK_NAME = "orez:per-storage-database-v1";
const DATABASE_VERSION = 2;
const LEGACY_STORE_NAME = "snapshots";
const MANIFEST_STORE_NAME = "snapshot-manifests";
const CHUNK_STORE_NAME = "snapshot-chunks";
const LEGACY_SNAPSHOT_FORMAT_VERSION = 1;
const SNAPSHOT_FORMAT_VERSION = 2;
export const SNAPSHOT_CHUNK_BYTES = 64 * 1024;

type LegacySnapshotFile = {
  path: string;
  size: number;
  data: ArrayBuffer;
};

type LegacySnapshotRecord = {
  storageKey: string;
  formatVersion: number;
  files: LegacySnapshotFile[];
};

type SnapshotManifestFile = {
  path: string;
  size: number;
  hashes: number[];
};

type SnapshotManifest = {
  storageKey: string;
  formatVersion: number;
  files: SnapshotManifestFile[];
};

type SnapshotChunk = {
  key: string;
  path: string;
  index: number;
  data: ArrayBuffer;
};

export type SnapshotCheckpointStats = {
  snapshotBytes: number;
  writtenBytes: number;
  snapshotChunks: number;
  writtenChunks: number;
};

type BedrockSnapshotModule = Pick<BedrockBrowserModule, "_memfs">;

function requestResult<Value>(request: IDBRequest<Value>): Promise<Value> {
  return new Promise((resolve, reject) => {
    request.addEventListener("success", () => resolve(request.result), {
      once: true,
    });
    request.addEventListener(
      "error",
      () => reject(request.error ?? new Error("IndexedDB request failed")),
      { once: true },
    );
  });
}

function transactionDone(transaction: IDBTransaction): Promise<void> {
  return new Promise((resolve, reject) => {
    transaction.addEventListener("complete", () => resolve(), { once: true });
    transaction.addEventListener(
      "abort",
      () =>
        reject(transaction.error ?? new Error("IndexedDB transaction aborted")),
      { once: true },
    );
    transaction.addEventListener(
      "error",
      () =>
        reject(transaction.error ?? new Error("IndexedDB transaction failed")),
      { once: true },
    );
  });
}

async function openSnapshotDatabase(
  databaseName: string,
): Promise<IDBDatabase> {
  const request = indexedDB.open(databaseName, DATABASE_VERSION);
  request.addEventListener("upgradeneeded", () => {
    if (!request.result.objectStoreNames.contains(LEGACY_STORE_NAME)) {
      request.result.createObjectStore(LEGACY_STORE_NAME, {
        keyPath: "storageKey",
      });
    }
    if (!request.result.objectStoreNames.contains(MANIFEST_STORE_NAME)) {
      request.result.createObjectStore(MANIFEST_STORE_NAME, {
        keyPath: "storageKey",
      });
    }
    if (!request.result.objectStoreNames.contains(CHUNK_STORE_NAME)) {
      request.result.createObjectStore(CHUNK_STORE_NAME, { keyPath: "key" });
    }
  });
  const database = await requestResult(request);
  database.addEventListener("versionchange", () => database.close());
  return database;
}

function snapshotDatabaseName(storageKey: string): string {
  return `${BROWSER_SYNC_HOST_DATABASE_PREFIX}${storageKey}`;
}

function chunkKey(path: string, index: number): string {
  return `${path.length}:${path}:${index}`;
}

function chunkHash(data: Uint8Array, start: number, end: number): number {
  let hash = 0x811c9dc5;
  const view = new DataView(data.buffer, data.byteOffset + start, end - start);
  const wordBytes = view.byteLength & ~3;
  let offset = 0;
  for (; offset < wordBytes; offset += 4) {
    hash = Math.imul(hash ^ view.getUint32(offset, true), 0x01000193);
  }
  for (; offset < view.byteLength; offset++) {
    hash = Math.imul(hash ^ view.getUint8(offset), 0x01000193);
  }
  return hash >>> 0;
}

async function readRecord(
  database: IDBDatabase,
  storeName: string,
  key: IDBValidKey,
): Promise<unknown> {
  const transaction = database.transaction(storeName, "readonly");
  const done = transactionDone(transaction);
  const value = await requestResult(
    transaction.objectStore(storeName).get(key),
  );
  await done;
  return value;
}

async function writeLegacySnapshotRecord(
  database: IDBDatabase,
  record: LegacySnapshotRecord,
): Promise<void> {
  const transaction = database.transaction(LEGACY_STORE_NAME, "readwrite");
  const done = transactionDone(transaction);
  transaction.objectStore(LEGACY_STORE_NAME).put(record);
  await done;
}

async function databaseExists(name: string): Promise<boolean> {
  return (await indexedDB.databases()).some(
    (database) => database.name === name,
  );
}

async function deleteDatabase(name: string): Promise<void> {
  const request = indexedDB.deleteDatabase(name);
  await requestResult(request);
}

async function openExistingLegacyDatabase(): Promise<IDBDatabase | null> {
  if (!(await databaseExists(LEGACY_DATABASE_NAME))) return null;
  const request = indexedDB.open(LEGACY_DATABASE_NAME);
  return requestResult(request);
}

async function migrateLegacySnapshotDatabase(): Promise<void> {
  if (!(await databaseExists(LEGACY_DATABASE_NAME))) return;

  await navigator.locks.request(MIGRATION_LOCK_NAME, async () => {
    const legacyDatabase = await openExistingLegacyDatabase();
    if (!legacyDatabase) return;
    try {
      if (!legacyDatabase.objectStoreNames.contains(LEGACY_STORE_NAME)) {
        throw new Error("legacy browser database has no snapshot store");
      }
      const transaction = legacyDatabase.transaction(
        LEGACY_STORE_NAME,
        "readonly",
      );
      const done = transactionDone(transaction);
      const values = await requestResult(
        transaction.objectStore(LEGACY_STORE_NAME).getAll(),
      );
      await done;
      for (const value of values) {
        if (!value || typeof value !== "object") {
          throw new Error("invalid legacy browser database snapshot");
        }
        const storageKey = Reflect.get(value, "storageKey");
        if (typeof storageKey !== "string") {
          throw new Error("invalid legacy browser database snapshot key");
        }
        const snapshot = validateLegacySnapshot(value, storageKey);
        const database = await openSnapshotDatabase(
          snapshotDatabaseName(storageKey),
        );
        try {
          const manifest = await readRecord(
            database,
            MANIFEST_STORE_NAME,
            storageKey,
          );
          const legacy = await readRecord(
            database,
            LEGACY_STORE_NAME,
            storageKey,
          );
          if (manifest === undefined && legacy === undefined) {
            await writeLegacySnapshotRecord(database, snapshot);
          }
        } finally {
          database.close();
        }
      }
    } finally {
      legacyDatabase.close();
    }
    await deleteDatabase(LEGACY_DATABASE_NAME);
    if (await databaseExists(MIGRATION_DATABASE_NAME)) {
      await deleteDatabase(MIGRATION_DATABASE_NAME);
    }
  });
}

export async function deleteBrowserSyncHostSnapshot(
  storageKey: string,
): Promise<void> {
  if (!storageKey) throw new TypeError("storageKey must not be empty");

  await migrateLegacySnapshotDatabase();
  const database = await openSnapshotDatabase(snapshotDatabaseName(storageKey));
  try {
    const transaction = database.transaction(
      [LEGACY_STORE_NAME, MANIFEST_STORE_NAME, CHUNK_STORE_NAME],
      "readwrite",
    );
    const done = transactionDone(transaction);
    transaction.objectStore(LEGACY_STORE_NAME).delete(storageKey);
    transaction.objectStore(MANIFEST_STORE_NAME).delete(storageKey);
    transaction.objectStore(CHUNK_STORE_NAME).clear();
    await done;
  } finally {
    database.close();
  }
}

function validateLegacySnapshot(
  value: unknown,
  storageKey: string,
): LegacySnapshotRecord {
  if (!value || typeof value !== "object") {
    throw new Error(`invalid browser database snapshot for ${storageKey}`);
  }
  const record = value as Partial<LegacySnapshotRecord>;
  if (record.storageKey !== storageKey) {
    throw new Error(`browser database snapshot key mismatch for ${storageKey}`);
  }
  if (record.formatVersion !== LEGACY_SNAPSHOT_FORMAT_VERSION) {
    throw new Error(
      `unsupported browser database snapshot format ${String(record.formatVersion)}`,
    );
  }
  if (!Array.isArray(record.files)) {
    throw new Error(`browser database snapshot has no files for ${storageKey}`);
  }
  const paths = new Set<string>();
  for (const file of record.files) {
    if (
      !file ||
      typeof file.path !== "string" ||
      !Number.isSafeInteger(file.size) ||
      file.size < 0 ||
      !(file.data instanceof ArrayBuffer) ||
      file.size > file.data.byteLength ||
      paths.has(file.path)
    ) {
      throw new Error(
        `invalid browser database snapshot file for ${storageKey}`,
      );
    }
    paths.add(file.path);
  }
  return record as LegacySnapshotRecord;
}

function validateManifest(
  value: unknown,
  storageKey: string,
): SnapshotManifest {
  if (!value || typeof value !== "object") {
    throw new Error(
      `invalid browser database snapshot manifest for ${storageKey}`,
    );
  }
  const manifest = value as Partial<SnapshotManifest>;
  if (manifest.storageKey !== storageKey) {
    throw new Error(
      `browser database snapshot manifest key mismatch for ${storageKey}`,
    );
  }
  if (manifest.formatVersion !== SNAPSHOT_FORMAT_VERSION) {
    throw new Error(
      `unsupported browser database snapshot format ${String(manifest.formatVersion)}`,
    );
  }
  if (!Array.isArray(manifest.files)) {
    throw new Error(
      `browser database snapshot manifest has no files for ${storageKey}`,
    );
  }
  const paths = new Set<string>();
  for (const file of manifest.files) {
    if (
      !file ||
      typeof file.path !== "string" ||
      !Number.isSafeInteger(file.size) ||
      file.size < 0 ||
      !Array.isArray(file.hashes) ||
      file.hashes.length !== Math.ceil(file.size / SNAPSHOT_CHUNK_BYTES) ||
      file.hashes.some(
        (hash) => !Number.isSafeInteger(hash) || hash < 0 || hash > 0xffffffff,
      ) ||
      paths.has(file.path)
    ) {
      throw new Error(
        `invalid browser database snapshot manifest file for ${storageKey}`,
      );
    }
    paths.add(file.path);
  }
  return manifest as SnapshotManifest;
}

function validateChunk(
  value: unknown,
  path: string,
  index: number,
  size: number,
) {
  if (!value || typeof value !== "object") {
    throw new Error(`missing browser database snapshot chunk ${path}:${index}`);
  }
  const chunk = value as Partial<SnapshotChunk>;
  if (
    chunk.key !== chunkKey(path, index) ||
    chunk.path !== path ||
    chunk.index !== index ||
    !(chunk.data instanceof ArrayBuffer) ||
    chunk.data.byteLength !== size
  ) {
    throw new Error(`invalid browser database snapshot chunk ${path}:${index}`);
  }
  return chunk.data;
}

function manifestFor(module: BedrockSnapshotModule, storageKey: string) {
  const files = Object.entries(module._memfs.files).map(([path, file]) => {
    if (
      !Number.isSafeInteger(file.size) ||
      file.size < 0 ||
      file.size > file.data.length
    ) {
      throw new Error(`invalid Bedrock VFS file ${path}`);
    }
    const hashes: number[] = [];
    for (let offset = 0; offset < file.size; offset += SNAPSHOT_CHUNK_BYTES) {
      hashes.push(
        chunkHash(
          file.data,
          offset,
          Math.min(offset + SNAPSHOT_CHUNK_BYTES, file.size),
        ),
      );
    }
    return { path, size: file.size, hashes };
  });
  files.sort((left, right) => left.path.localeCompare(right.path));
  return {
    storageKey,
    formatVersion: SNAPSHOT_FORMAT_VERSION,
    files,
  } satisfies SnapshotManifest;
}

export class IndexedDbSnapshotStore {
  readonly #database: Promise<IDBDatabase>;
  #manifest: SnapshotManifest | null = null;
  #legacyRecordPresent = false;

  constructor(readonly storageKey: string) {
    if (!storageKey) throw new TypeError("storageKey must not be empty");
    this.#database = migrateLegacySnapshotDatabase().then(() =>
      openSnapshotDatabase(snapshotDatabaseName(storageKey)),
    );
  }

  async restore(module: BedrockSnapshotModule): Promise<boolean> {
    const database = await this.#database;
    const transaction = database.transaction(
      [LEGACY_STORE_NAME, MANIFEST_STORE_NAME, CHUNK_STORE_NAME],
      "readonly",
    );
    const done = transactionDone(transaction);
    const [manifestValue, legacyValue, chunkValues] = await Promise.all([
      requestResult(
        transaction.objectStore(MANIFEST_STORE_NAME).get(this.storageKey),
      ),
      requestResult(
        transaction.objectStore(LEGACY_STORE_NAME).get(this.storageKey),
      ),
      requestResult(transaction.objectStore(CHUNK_STORE_NAME).getAll()),
    ]);
    await done;

    if (manifestValue !== undefined) {
      const manifest = validateManifest(manifestValue, this.storageKey);
      const chunks = new Map<string, unknown>();
      for (const value of chunkValues) {
        if (value && typeof value === "object") {
          const key = Reflect.get(value, "key");
          if (typeof key === "string") chunks.set(key, value);
        }
      }
      const files: BedrockBrowserModule["_memfs"]["files"] = {};
      for (const file of manifest.files) {
        const data = new Uint8Array(file.size);
        for (const [index, hash] of file.hashes.entries()) {
          const offset = index * SNAPSHOT_CHUNK_BYTES;
          const size = Math.min(SNAPSHOT_CHUNK_BYTES, file.size - offset);
          const chunk = validateChunk(
            chunks.get(chunkKey(file.path, index)),
            file.path,
            index,
            size,
          );
          const bytes = new Uint8Array(chunk);
          if (chunkHash(bytes, 0, bytes.byteLength) !== hash) {
            throw new Error(
              `browser database snapshot chunk hash mismatch ${file.path}:${index}`,
            );
          }
          data.set(bytes, offset);
        }
        files[file.path] = { data, size: file.size };
      }
      module._memfs.files = files;
      this.#manifest = manifest;
      this.#legacyRecordPresent = legacyValue !== undefined;
      return true;
    }

    if (legacyValue === undefined) return false;
    const snapshot = validateLegacySnapshot(legacyValue, this.storageKey);
    const files: BedrockBrowserModule["_memfs"]["files"] = {};
    for (const file of snapshot.files) {
      const data = new Uint8Array(file.data.slice(0, file.size));
      files[file.path] = { data, size: file.size };
    }
    module._memfs.files = files;
    this.#manifest = null;
    this.#legacyRecordPresent = true;
    return true;
  }

  async checkpoint(
    module: BedrockSnapshotModule,
  ): Promise<SnapshotCheckpointStats> {
    const manifest = manifestFor(module, this.storageKey);
    const previousFiles = new Map(
      (this.#manifest?.files ?? []).map((file) => [file.path, file]),
    );
    let snapshotBytes = 0;
    let snapshotChunks = 0;
    let writtenBytes = 0;
    let writtenChunks = 0;

    const database = await this.#database;
    const transaction = database.transaction(
      [LEGACY_STORE_NAME, MANIFEST_STORE_NAME, CHUNK_STORE_NAME],
      "readwrite",
    );
    const done = transactionDone(transaction);
    const chunkStore = transaction.objectStore(CHUNK_STORE_NAME);

    for (const file of manifest.files) {
      snapshotBytes += file.size;
      snapshotChunks += file.hashes.length;
      const previous = previousFiles.get(file.path);
      const source = module._memfs.files[file.path];
      for (let index = 0; index < file.hashes.length; index++) {
        if (previous?.hashes[index] === file.hashes[index]) continue;
        const offset = index * SNAPSHOT_CHUNK_BYTES;
        const data = source.data.slice(
          offset,
          Math.min(offset + SNAPSHOT_CHUNK_BYTES, file.size),
        ).buffer;
        const chunk: SnapshotChunk = {
          key: chunkKey(file.path, index),
          path: file.path,
          index,
          data,
        };
        chunkStore.put(chunk);
        writtenBytes += data.byteLength;
        writtenChunks++;
      }
      for (
        let index = file.hashes.length;
        index < (previous?.hashes.length ?? 0);
        index++
      ) {
        chunkStore.delete(chunkKey(file.path, index));
      }
      previousFiles.delete(file.path);
    }
    for (const previous of previousFiles.values()) {
      for (let index = 0; index < previous.hashes.length; index++) {
        chunkStore.delete(chunkKey(previous.path, index));
      }
    }

    transaction.objectStore(MANIFEST_STORE_NAME).put(manifest);
    if (this.#legacyRecordPresent) {
      transaction.objectStore(LEGACY_STORE_NAME).delete(this.storageKey);
    }
    await done;
    this.#manifest = manifest;
    this.#legacyRecordPresent = false;
    return { snapshotBytes, writtenBytes, snapshotChunks, writtenChunks };
  }

  async close(): Promise<void> {
    (await this.#database).close();
  }
}
