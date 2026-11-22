import {RWLock} from '@rocicorp/lock';
import {resolver} from '@rocicorp/resolver';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {deepFreeze} from '../frozen-json.ts';
import type {Read, Store, Write} from './store.ts';
import {
  maybeTransactionIsClosedRejection,
  throwIfStoreClosed,
  transactionError,
} from './throw-if-closed.ts';
import {deleteSentinel, WriteImplBase} from './write-impl-base.ts';

/**
 * A SQLite prepared statement.
 */
export interface PreparedStatement {
  exec(params: (string | null)[]): Promise<void>;
  all(params: (string | null)[]): Promise<unknown[][]>;
  iterate?(params: (string | null)[]): AsyncIterable<unknown[]>;
  batch?(rows: string[][]): Promise<void>;
}

export interface SQLiteDatabase {
  /**
   * Close the database connection.
   */
  close(): void | Promise<void>;

  /**
   * Destroy or delete the database (e.g. delete file).
   */
  destroy(): void | Promise<void>;

  /**
   * Prepare a SQL string, returning a statement you can execute.
   * E.g. `const stmt = db.prepare("SELECT * FROM todos WHERE id=?");`
   */
  prepare(sql: string): PreparedStatement | Promise<PreparedStatement>;

  // for PRAGMA statements, schema creation and transaction control.
  exec?(sql: string): Promise<void>;
  execSync?(sql: string): void;
  initialize?(): Promise<void>;
  maintain?(): Promise<void>;
}

export type CreateSQLiteDatabase = (
  filename: string,
  opts?: SQLiteStoreOptions,
) => SQLiteDatabase;

/**
 * SQLite-based implementation of the Store interface using a configurable delegate.
 * Supports shared connections between multiple store instances with the same name,
 * providing efficient resource utilization and proper transaction isolation.
 * Uses parameterized queries for safety and performance.
 */
export class SQLiteStore implements Store {
  readonly #filename: string;
  readonly #entry: StoreEntry;

  #closed = false;
  #closing: Promise<void> | undefined;

  constructor(
    name: string,
    create: CreateSQLiteDatabase,
    opts?: SQLiteStoreOptions,
  ) {
    this.#filename = resolveFilename(name, opts);
    this.#entry = getOrCreateEntry(this.#filename, create, opts);
  }

  async read(): Promise<Read> {
    throwIfStoreClosed(this);
    const entry = this.#entry;
    const release = await entry.lock.read();
    let joined = false;
    try {
      throwIfStoreClosed(this);
      if (entry.dropped) throw transactionError();
      const prepared = await entry.prepare();
      if (entry.activeReaders++ === 0) {
        entry.reading = entry.ending.then(() => entry.exec('BEGIN'));
      }
      joined = true;
      await entry.reading;
      return new SQLiteStoreRead(() => {
        if (--entry.activeReaders === 0) {
          finishTransaction(entry, entry.exec('COMMIT'), release);
        } else {
          release();
        }
      }, prepared);
    } catch (error) {
      if (joined) entry.activeReaders--;
      release();
      throw error;
    }
  }

  async write(): Promise<Write> {
    throwIfStoreClosed(this);
    const entry = this.#entry;
    const release = await entry.lock.write();
    try {
      throwIfStoreClosed(this);
      if (entry.dropped) throw transactionError();
      await entry.ending;
      const prepared = await entry.prepare();
      await entry.exec('BEGIN IMMEDIATE');
      return new SQLiteWrite(release, entry, prepared);
    } catch (error) {
      release();
      throw error;
    }
  }

  close(): Promise<void> {
    this.#closed = true;
    const entry = this.#entry;
    return this.#closing ??= entry.lock.withWrite(async () => {
      if (entry.dropped || --entry.refCount !== 0) return;
      await closeEntry(entry);
      if (stores.get(this.#filename) === entry && !entry.dropped) {
        stores.delete(this.#filename);
      }
    });
  }

  get closed(): boolean {
    return this.#closed;
  }
}

export function safeFilename(name: string): string {
  return name.replace(/[^a-zA-Z0-9]/g, '_');
}

function resolveFilename(name: string, opts?: SQLiteStoreOptions): string {
  const safe = safeFilename(name);
  const dir = opts?.directory;
  return dir ? `${dir}/${safe}` : safe;
}

export type PreparedStatements = {
  has: PreparedStatement;
  get: PreparedStatement;
  hasMany: PreparedStatement;
  getMany: PreparedStatement;
  put: PreparedStatement;
  del: PreparedStatement;
};

export interface SQLiteStoreOptions {
  // Common options
  busyTimeout?: number;
  journalMode?: 'WAL' | 'DELETE';
  synchronous?: 'NORMAL' | 'FULL';
  readUncommitted?: boolean;
  /** Directory in which to create the SQLite file. Defaults to the process CWD. */
  directory?: string | undefined;
}

/**
 * Common database setup logic shared between expo-sqlite and op-sqlite implementations.
 * Configures SQLite pragmas, creates the entry table, and prepares common statements.
 */

async function setupDatabase(
  delegate: SQLiteDatabase,
  exec: (sql: string) => Promise<void>,
  opts?: SQLiteStoreOptions,
): Promise<PreparedStatements> {
  // Configure SQLite pragmas for optimal performance
  await exec(`PRAGMA busy_timeout = ${opts?.busyTimeout ?? 200}`);
  await exec(`PRAGMA journal_mode = '${opts?.journalMode ?? 'WAL'}'`);
  await exec(`PRAGMA synchronous = '${opts?.synchronous ?? 'NORMAL'}'`);
  await exec(
    `PRAGMA read_uncommitted = ${Boolean(opts?.readUncommitted)}`,
  );

  // Create the entry table
  await exec(`
    CREATE TABLE IF NOT EXISTS entry (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    ) WITHOUT ROWID
  `);

  // Prepare common statements
  await delegate.initialize?.();
  const keys = Array(BATCH_KEYS).fill('?').join(', ');
  const [has, get, hasMany, getMany, put, del] = await Promise.all([
    delegate.prepare('SELECT 1 FROM entry WHERE key = ? LIMIT 1'),
    delegate.prepare('SELECT value FROM entry WHERE key = ?'),
    delegate.prepare(`SELECT key FROM entry WHERE key IN (${keys})`),
    delegate.prepare(`SELECT key, value FROM entry WHERE key IN (${keys})`),
    delegate.prepare(upsertStatement(1)),
    delegate.prepare(`DELETE FROM entry WHERE key IN (${keys})`),
  ]);
  return {has, get, hasMany, getMany, put, del};
}

const BATCH_KEYS = 64;
const BATCH_VALUE_UNITS = 128 * 1024;

function upsertStatement(length: number): string {
  const values = Array(length).fill('(?, ?)').join(', ');
  return `INSERT INTO entry (key, value) VALUES ${values}
    ON CONFLICT(key) DO UPDATE SET value = excluded.value WHERE entry.value IS NOT excluded.value`;
}

function* keyBatches(keys: string[]): Iterable<(string | null)[]> {
  for (let offset = 0; offset < keys.length; offset += BATCH_KEYS) {
    const batch: (string | null)[] = keys.slice(offset, offset + BATCH_KEYS);
    while (batch.length < BATCH_KEYS) batch.push(null);
    yield batch;
  }
}

// Callbacks are stored as striped pairs: [resolve, reject, resolve, reject, ...]
const CB_STRIDE = 2;
const CB_RESOLVE = 0;
const CB_REJECT = 1;

type GetResolve = (v: ReadonlyJSONValue | undefined) => void;
type HasResolve = (v: boolean) => void;
type Reject = (e: unknown) => void;

function rejectAll(callbacks: unknown[], e: unknown): void {
  for (let i = CB_REJECT; i < callbacks.length; i += CB_STRIDE) {
    (callbacks[i] as Reject)(e);
  }
}

function parseRawValue(raw: string | undefined): ReadonlyJSONValue | undefined {
  return raw === undefined
    ? undefined
    : deepFreeze(JSON.parse(raw) as ReadonlyJSONValue);
}

function resolveGet(
  resolve: GetResolve,
  reject: Reject,
  raw: string | undefined,
): void {
  try {
    resolve(parseRawValue(raw));
  } catch (e) {
    reject(e);
  }
}

async function flushGets(
  keys: string[],
  callbacks: unknown[],
  ps: PreparedStatements,
): Promise<void> {
  try {
    if (keys.length === 1) {
      const rows = await ps.get.all(keys);
      resolveGet(callbacks[CB_RESOLVE] as GetResolve, callbacks[CB_REJECT] as Reject, rows[0]?.[0] as string | undefined);
      return;
    }
    const waiting = new Map<string, number[]>();
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i];
      const offsets = waiting.get(key);
      if (offsets) offsets.push(i * CB_STRIDE);
      else waiting.set(key, [i * CB_STRIDE]);
    }
    for (const batch of keyBatches([...waiting.keys()])) {
      const {getMany} = ps;
      const rows = getMany.iterate ? getMany.iterate(batch) : await getMany.all(batch);
      for await (const row of rows) {
        const [key, raw] = row as [string, string];
        for (const offset of waiting.get(key) ?? []) {
          resolveGet(callbacks[offset + CB_RESOLVE] as GetResolve, callbacks[offset + CB_REJECT] as Reject, raw);
        }
        waiting.delete(key);
      }
    }
    for (const offsets of waiting.values()) {
      for (const offset of offsets) (callbacks[offset + CB_RESOLVE] as GetResolve)(undefined);
    }
  } catch (error) {
    rejectAll(callbacks, error);
  }
}

async function flushHas(
  keys: string[],
  callbacks: unknown[],
  ps: PreparedStatements,
): Promise<void> {
  try {
    if (keys.length === 1) {
      (callbacks[CB_RESOLVE] as HasResolve)((await ps.has.all(keys)).length > 0);
      return;
    }
    const existing = new Set<string>();
    for (const batch of keyBatches(keys)) {
      for (const [key] of await ps.hasMany.all(batch)) existing.add(key as string);
    }
    for (let i = 0; i < keys.length; i++) {
      (callbacks[i * CB_STRIDE + CB_RESOLVE] as HasResolve)(existing.has(keys[i]));
    }
  } catch (error) {
    rejectAll(callbacks, error);
  }
}

export class SQLiteStoreRead implements Read {
  readonly #release: () => void;
  readonly #preparedStatements: PreparedStatements;
  #closed = false;
  #activeLookups = 0;
  #pendingGetKeys: string[] = [];
  #pendingGetCallbacks: unknown[] = [];
  #pendingHasKeys: string[] = [];
  #pendingHasCallbacks: unknown[] = [];
  #scheduled = false;

  constructor(release: () => void, preparedStatements: PreparedStatements) {
    this.#release = release;
    this.#preparedStatements = preparedStatements;
  }

  has(key: string): Promise<boolean> {
    return (
      maybeTransactionIsClosedRejection(this) ??
      new Promise((resolve, reject) => {
        this.#pendingHasKeys.push(key);
        this.#pendingHasCallbacks.push(resolve, reject);
        this.#scheduleLookup();
      })
    );
  }

  get(key: string): Promise<ReadonlyJSONValue | undefined> {
    return (
      maybeTransactionIsClosedRejection(this) ??
      new Promise((resolve, reject) => {
        this.#pendingGetKeys.push(key);
        this.#pendingGetCallbacks.push(resolve, reject);
        this.#scheduleLookup();
      })
    );
  }

  #scheduleLookup(): void {
    if (!this.#scheduled) {
      this.#scheduled = true;
      queueMicrotask(() => {
        this.#scheduled = false;

        const ps = this.#preparedStatements;
        const getKeys = this.#pendingGetKeys;
        this.#pendingGetKeys = [];
        const getCallbacks = this.#pendingGetCallbacks;
        this.#pendingGetCallbacks = [];
        const hasKeys = this.#pendingHasKeys;
        this.#pendingHasKeys = [];
        const hasCallbacks = this.#pendingHasCallbacks;
        this.#pendingHasCallbacks = [];

        if (this.#closed) {
          const e = transactionError();
          rejectAll(getCallbacks, e);
          rejectAll(hasCallbacks, e);
          return;
        }

        if (getKeys.length > 0) {
          this.#trackLookup(flushGets(getKeys, getCallbacks, ps));
        }
        if (hasKeys.length > 0) {
          this.#trackLookup(flushHas(hasKeys, hasCallbacks, ps));
        }
      });
    }
  }

  #trackLookup(lookup: Promise<void>): void {
    this.#activeLookups++;
    void lookup.then(() => {
      if (--this.#activeLookups === 0 && this.#closed) this.#release();
    });
  }

  release(): void {
    if (!this.#closed) {
      this.#closed = true;
      if (this.#activeLookups === 0) this.#release();
    }
  }

  get closed(): boolean {
    return this.#closed;
  }
}

export class SQLiteWrite extends WriteImplBase implements Write {
  readonly #release: () => void;
  readonly #entry: StoreEntry;
  readonly #preparedStatements: PreparedStatements;
  readonly #readReleased: Promise<void>;
  #committing: Promise<void> | undefined;
  #closed = false;

  constructor(
    release: () => void,
    entry: StoreEntry,
    preparedStatements: PreparedStatements,
  ) {
    const readReleased = resolver<void>();
    super(new SQLiteStoreRead(readReleased.resolve, preparedStatements));
    this.#release = release;
    this.#entry = entry;
    this.#preparedStatements = preparedStatements;
    this.#readReleased = readReleased.promise;
  }

  commit(): Promise<void> {
    if (this.#closed) return Promise.reject(transactionError());
    return this.#committing ??= this.#commit();
  }

  async #commit(): Promise<void> {
    const {del, put} = this.#preparedStatements;
    const upsert = put.batch?.bind(put) ?? (async (rows: string[][]) => {
      const statement = await this.#entry.db.prepare(upsertStatement(rows.length));
      await statement.exec(rows.flat());
    });
    const deleteKeys = [];
    for (const [key, value] of this._pending) {
      if (value === deleteSentinel) deleteKeys.push(key);
    }
    for (const batch of keyBatches(deleteKeys)) await del.exec(batch);
    let rows: string[][] = [];
    let units = 0;
    for (const [key, value] of this._pending) {
      if (value === deleteSentinel) continue;
      const encoded = JSON.stringify(value);
      rows.push([key, encoded]);
      units += key.length + encoded.length;
      if (rows.length === BATCH_KEYS || units >= BATCH_VALUE_UNITS) {
        await upsert(rows);
        rows = [];
        units = 0;
      }
    }
    if (rows.length > 0) await upsert(rows);
    await this.#entry.exec('COMMIT');
    this._pending.clear();
  }

  release(): void {
    if (this.#closed) return;
    this.#closed = true;
    super.release();
    const rollback = () => this.#readReleased.then(() => this.#entry.exec('ROLLBACK'));
    const ended = this.#committing
      ? this.#committing.then(() => {
        scheduleMaintenance(this.#entry);
        return this.#readReleased;
      }, rollback)
      : rollback();
    finishTransaction(this.#entry, ended, this.#release);
  }

  get closed(): boolean {
    return this.#closed;
  }
}

type StoreEntry = {
  readonly lock: RWLock;
  readonly db: SQLiteDatabase;
  readonly exec: (sql: string) => Promise<void>;
  readonly prepare: () => Promise<PreparedStatements>;
  readonly previousClose: Promise<void>;
  refCount: number;
  activeReaders: number;
  reading?: Promise<void>;
  ending: Promise<void>;
  closing?: Promise<void>;
  dropping: Promise<void> | undefined;
  dropped: boolean;
  maintenanceAfter: number;
};

const stores = new Map<string, StoreEntry>();

function finishTransaction(entry: StoreEntry, ended: Promise<void>, release: () => void) {
  entry.ending = ended;
  void ended.then(release, release);
}

function scheduleMaintenance(entry: StoreEntry) {
  const now = Date.now();
  if (!entry.db.maintain || entry.dropped || now < entry.maintenanceAfter) return;
  entry.maintenanceAfter = now + 60_000;
  void entry.lock.withWrite(async () => {
    if (entry.refCount > 0 && !entry.dropped) await entry.db.maintain!();
  }).catch(error => console.error('SQLite store maintenance failed', error));
}

function getOrCreateEntry(
  filename: string,
  create: CreateSQLiteDatabase,
  opts?: SQLiteStoreOptions,
): StoreEntry {
  const entry = stores.get(filename);
  if (entry?.dropped) throw transactionError();
  if (entry && !entry.closing) {
    entry.refCount++;
    return entry;
  }
  const db = create(filename, opts);
  const execSync = db.execSync?.bind(db);
  const exec = db.exec?.bind(db) ?? (async (sql: string) => {
    if (!execSync) throw new TypeError('SQLite adapters must provide exec or execSync');
    execSync(sql);
  });
  const previousClose = entry?.closing ?? Promise.resolve();
  let prepared: Promise<PreparedStatements> | undefined;
  const opened: StoreEntry = {
    lock: new RWLock(),
    db,
    exec,
    prepare: () => prepared ??= previousClose.then(() => setupDatabase(db, exec, opts)),
    previousClose,
    refCount: 1,
    activeReaders: 0,
    ending: previousClose,
    dropping: undefined,
    dropped: false,
    maintenanceAfter: Date.now() + 30_000,
  };
  stores.set(filename, opened);
  return opened;
}

function closeEntry(entry: StoreEntry): Promise<void> {
  return entry.closing ??= entry.previousClose.then(() => entry.db.close());
}

export async function clearAllNamedStoresForTesting(): Promise<void> {
  await Promise.all([...stores.values()].map(entry => entry.lock.withWrite(() => closeEntry(entry))));
  stores.clear();
}

export function dropStore(
  name: string,
  create: CreateSQLiteDatabase,
  opts?: SQLiteStoreOptions,
): Promise<void> {
  const filename = resolveFilename(name, opts);
  const entry = stores.get(filename) ?? getOrCreateEntry(filename, create, opts);
  entry.dropped = true;
  return entry.dropping ??= entry.lock.withWrite(async () => {
    await closeEntry(entry);
    await entry.db.destroy();
    stores.delete(filename);
  }).finally(() => {
    entry.dropping = undefined;
  });
}
