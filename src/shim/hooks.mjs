// esm loader hooks — intercept @rocicorp/zero-sqlite3 with bedrock-sqlite wasm.
//
// NOTE: this file is currently UNUSED. orez uses in-place CJS patching via
// src/sqlite-mode/apply-mode.ts instead. this file is kept for potential future
// use with Node.js ESM loader hooks (--import flag).
//
// __BEDROCK_PATH__ is replaced at runtime by orez before writing to tmpdir.

const SHIM_URL = 'orez-sqlite-shim://shim'
const BEDROCK_PATH = '__BEDROCK_PATH__'
// __JOURNAL_MODE__ would be replaced at runtime (delete for wasm, wal2 for native)
const JOURNAL_MODE = '__JOURNAL_MODE__'

export function resolve(specifier, context, nextResolve) {
  if (
    specifier === '@rocicorp/zero-sqlite3' ||
    specifier.startsWith('@rocicorp/zero-sqlite3/')
  ) {
    return { url: SHIM_URL, shortCircuit: true }
  }
  return nextResolve(specifier, context)
}

export function load(url, context, nextLoad) {
  if (url === SHIM_URL) {
    // journal mode differs between native and wasm:
    // - native: wal2 for better concurrency
    // - wasm: delete for compatibility (wal2 can corrupt wasm vfs)
    const journalMode = JOURNAL_MODE === '__JOURNAL_MODE__' ? 'delete' : JOURNAL_MODE

    return {
      format: 'module',
      shortCircuit: true,
      source: `
// orez sqlite shim - wraps bedrock-sqlite for zero-cache compatibility
// journal_mode: ${journalMode}

// catch uncaught exceptions from bedrock-sqlite wasm clearly
process.on('uncaughtException', (err) => {
  console.error('[orez-shim] UNCAUGHT EXCEPTION:', err?.message || err);
  console.error('[orez-shim] code:', err?.code, 'name:', err?.name);
  console.error('[orez-shim] stack:', err?.stack?.split('\\n').slice(0, 5).join('\\n'));
  process.exit(1);
});

import { createRequire } from 'node:module';
const require = createRequire('${BEDROCK_PATH}');
const mod = require('${BEDROCK_PATH}');
const OrigDatabase = mod.Database;
const SqliteError = mod.SqliteError;

function Database(...args) {
  const db = new OrigDatabase(...args);
  try {
    db.pragma('journal_mode = ${journalMode}');
    db.pragma('busy_timeout = 30000');
    db.pragma('synchronous = normal');
  } catch(e) {}
  return db;
}

Database.prototype = OrigDatabase.prototype;
Database.prototype.constructor = Database;
Object.keys(OrigDatabase).forEach(k => { Database[k] = OrigDatabase[k]; });

// api polyfills for better-sqlite3 compatibility
Database.prototype.unsafeMode = function() { return this; };
if (!Database.prototype.defaultSafeIntegers) {
  Database.prototype.defaultSafeIntegers = function() { return this; };
}
if (!Database.prototype.serialize) {
  Database.prototype.serialize = function() { throw new Error('not supported in wasm'); };
}
if (!Database.prototype.backup) {
  Database.prototype.backup = function() { throw new Error('not supported in wasm'); };
}

// wrap pragma to skip optimize (can corrupt wasm vfs) and swallow sqlite errors
const origPragma = OrigDatabase.prototype.pragma;
Database.prototype.pragma = function(str, opts) {
  if (str && str.trim().toLowerCase().startsWith('optimize')) return [];
  try { return origPragma.call(this, str, opts); }
  catch(e) { if (e && (e.code === 'SQLITE_CORRUPT' || e.code === 'SQLITE_IOERR')) return []; throw e; }
};

// wrap close to swallow wasm errors during shutdown
const origClose = OrigDatabase.prototype.close;
Database.prototype.close = function() {
  try { return origClose.call(this); }
  catch(e) { console.error('[orez-shim] close error (swallowed):', e?.message || e); }
};

// statement prototype polyfills
const tmpDb = new OrigDatabase(':memory:');
const tmpStmt = tmpDb.prepare('SELECT 1');
const SP = Object.getPrototypeOf(tmpStmt);
if (!SP.safeIntegers) SP.safeIntegers = function() { return this; };
SP.scanStatus = function() { return undefined; };
SP.scanStatusV2 = function() { return []; };
SP.scanStatusReset = function() {};
tmpDb.close();

// scanstat constants for query planner compatibility
Database.SQLITE_SCANSTAT_NLOOP = 0;
Database.SQLITE_SCANSTAT_NVISIT = 1;
Database.SQLITE_SCANSTAT_EST = 2;
Database.SQLITE_SCANSTAT_NAME = 3;
Database.SQLITE_SCANSTAT_EXPLAIN = 4;
Database.SQLITE_SCANSTAT_SELECTID = 5;
Database.SQLITE_SCANSTAT_PARENTID = 6;
Database.SQLITE_SCANSTAT_NCYCLE = 7;
Database.SQLITE_SCANSTAT_COMPLEX = 8;

export default Database;
export { SqliteError };
`,
    }
  }
  return nextLoad(url, context)
}
