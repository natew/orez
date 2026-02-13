/**
 * shim template generator - single source of truth for both cjs and esm shims.
 * generates the shim code that wraps bedrock-sqlite to be compatible with
 * @rocicorp/zero-sqlite3 (better-sqlite3 api).
 */

import { COMMON_PRAGMAS, JOURNAL_MODE, type SqliteMode } from './types.js'

export interface ShimOptions {
  mode: SqliteMode
  bedrockPath: string
  // include debug tracing for changeLog/replicationState writes
  includeTracing?: boolean
}

/**
 * generate the core shim code (shared between cjs and esm)
 * this is the constructor wrapper and api polyfills
 */
function generateShimCore(opts: ShimOptions): string {
  const journalMode = JOURNAL_MODE[opts.mode]
  const { busy_timeout, synchronous } = COMMON_PRAGMAS

  return `
var OrigDatabase = mod.Database;
var SqliteError = mod.SqliteError;

function Database() {
  var db = new OrigDatabase(...arguments);
  try {
    db.pragma('journal_mode = ${journalMode}');
    db.pragma('busy_timeout = ${busy_timeout}');
    db.pragma('synchronous = ${synchronous}');
  } catch(e) {}
  return db;
}

Database.prototype = OrigDatabase.prototype;
Database.prototype.constructor = Database;
Object.keys(OrigDatabase).forEach(function(k) { Database[k] = OrigDatabase[k]; });

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
var origPragma = OrigDatabase.prototype.pragma;
Database.prototype.pragma = function(str, opts) {
  if (str && str.trim().toLowerCase().startsWith('optimize')) return [];
  try { return origPragma.call(this, str, opts); }
  catch(e) { if (e && (e.code === 'SQLITE_CORRUPT' || e.code === 'SQLITE_IOERR')) return []; throw e; }
};

// wrap close to swallow wasm errors during shutdown
var origClose = OrigDatabase.prototype.close;
Database.prototype.close = function() {
  try { return origClose.call(this); }
  catch(e) { console.error('[orez-shim] close error (swallowed):', e?.message || e); }
};

// statement prototype polyfills
var tmpDb = new OrigDatabase(':memory:');
var tmpStmt = tmpDb.prepare('SELECT 1');
var SP = Object.getPrototypeOf(tmpStmt);
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
`.trim()
}

/**
 * generate debug tracing code for run() method
 */
function generateTracing(): string {
  return `
// trace writes to _zero.changeLog and _zero.replicationState for debugging
var origRun = OrigDatabase.prototype.run;
Database.prototype.run = function(sql) {
  var args = Array.prototype.slice.call(arguments, 1);
  if (typeof sql === 'string') {
    if (sql.includes('_zero.changeLog')) {
      console.info('[orez-shim] changeLog write:', sql.slice(0, 120), args.length ? JSON.stringify(args[0]).slice(0, 80) : '');
    }
    if (sql.includes('_zero.replicationState') && (sql.includes('UPDATE') || sql.includes('INSERT'))) {
      console.info('[orez-shim] replicationState update:', sql.slice(0, 120), args.length ? JSON.stringify(args[0]).slice(0, 80) : '');
    }
  }
  return origRun.apply(this, arguments);
};
`.trim()
}

/**
 * generate commonjs shim for in-place patching of @rocicorp/zero-sqlite3
 */
export function generateCjsShim(opts: ShimOptions): string {
  const core = generateShimCore(opts)
  const tracing = opts.includeTracing ? '\n' + generateTracing() : ''

  return `'use strict';
// orez sqlite shim - wraps bedrock-sqlite for zero-cache compatibility
// mode: ${opts.mode}, journal_mode: ${JOURNAL_MODE[opts.mode]}
var mod = require('${opts.bedrockPath}');
${core}
${tracing}
module.exports = Database;
module.exports.SqliteError = SqliteError;
`
}

/**
 * generate esm shim for loader hooks
 */
export function generateEsmShim(opts: ShimOptions): string {
  const core = generateShimCore(opts)
  const tracing = opts.includeTracing ? '\n' + generateTracing() : ''

  return `// orez sqlite shim - wraps bedrock-sqlite for zero-cache compatibility
// mode: ${opts.mode}, journal_mode: ${JOURNAL_MODE[opts.mode]}

// catch uncaught exceptions from bedrock-sqlite wasm clearly
process.on('uncaughtException', (err) => {
  console.error('[orez-shim] UNCAUGHT EXCEPTION:', err?.message || err);
  console.error('[orez-shim] code:', err?.code, 'name:', err?.name);
  console.error('[orez-shim] stack:', err?.stack?.split('\\n').slice(0, 5).join('\\n'));
  process.exit(1);
});

import { createRequire } from 'node:module';
const require = createRequire('${opts.bedrockPath}');
var mod = require('${opts.bedrockPath}');
${core}
${tracing}
export default Database;
export { SqliteError };
`
}
