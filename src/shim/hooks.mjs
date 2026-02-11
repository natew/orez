// esm loader hooks — intercept @rocicorp/zero-sqlite3 with bedrock-sqlite wasm.
// __BEDROCK_PATH__ is replaced at runtime by orez before writing to tmpdir.

const SHIM_URL = 'orez-sqlite-shim://shim';
const BEDROCK_PATH = '__BEDROCK_PATH__';

export function resolve(specifier, context, nextResolve) {
  if (specifier === '@rocicorp/zero-sqlite3' || specifier.startsWith('@rocicorp/zero-sqlite3/')) {
    return { url: SHIM_URL, shortCircuit: true };
  }
  return nextResolve(specifier, context);
}

export function load(url, context, nextLoad) {
  if (url === SHIM_URL) {
    return {
      format: 'module',
      shortCircuit: true,
      source: `
import { createRequire } from 'node:module';
const require = createRequire('${BEDROCK_PATH}');
const mod = require('${BEDROCK_PATH}');
const OrigDatabase = mod.Database;
const SqliteError = mod.SqliteError;
function Database(...args) {
  const db = new OrigDatabase(...args);
  try { db.pragma('busy_timeout = 30000'); db.pragma('synchronous = normal'); } catch(e) {}
  return db;
}
Database.prototype = OrigDatabase.prototype;
Database.prototype.constructor = Database;
Object.keys(OrigDatabase).forEach(k => { Database[k] = OrigDatabase[k]; });
Database.prototype.unsafeMode = function() { return this; };
if (!Database.prototype.defaultSafeIntegers) Database.prototype.defaultSafeIntegers = function() { return this; };
if (!Database.prototype.serialize) Database.prototype.serialize = function() { throw new Error('not supported in wasm'); };
if (!Database.prototype.backup) Database.prototype.backup = function() { throw new Error('not supported in wasm'); };
const tmpDb = new OrigDatabase(':memory:');
const tmpStmt = tmpDb.prepare('SELECT 1');
const SP = Object.getPrototypeOf(tmpStmt);
if (!SP.safeIntegers) SP.safeIntegers = function() { return this; };
SP.scanStatus = function() { return undefined; };
SP.scanStatusV2 = function() { return []; };
SP.scanStatusReset = function() {};
tmpDb.close();
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
`
    };
  }
  return nextLoad(url, context);
}
