// shim that makes bedrock-sqlite (wasm) compatible with @rocicorp/zero-sqlite3 (native)
// patches missing methods that zero-cache expects
'use strict'

const mod = require('bedrock-sqlite')
const OrigDatabase = mod.Database
const SqliteError = mod.SqliteError

// wrap Database constructor to set pragmas immediately on every connection.
// wasm vfs can't share SHM between processes, so WAL mode coordination breaks.
// force DELETE journal mode and set busy_timeout before any queries run.
function Database() {
  const db = new OrigDatabase(...arguments)
  try {
    db.pragma('journal_mode = delete')
    db.pragma('busy_timeout = 30000')
    db.pragma('synchronous = normal')
  } catch {}
  return db
}
Database.prototype = OrigDatabase.prototype
Database.prototype.constructor = Database
Object.keys(OrigDatabase).forEach((key) => {
  Database[key] = OrigDatabase[key]
})

Database.prototype.unsafeMode = function () {
  // pragmas already set in constructor, just return this
  return this
}
if (!Database.prototype.defaultSafeIntegers) {
  Database.prototype.defaultSafeIntegers = function () {
    return this
  }
}
if (!Database.prototype.serialize) {
  Database.prototype.serialize = function () {
    throw new Error('serialize() not supported in wasm build')
  }
}
if (!Database.prototype.backup) {
  Database.prototype.backup = function () {
    throw new Error('backup() not supported in wasm build')
  }
}

// patch Statement prototype - find it from a temp db instance
const tmpDb = new Database(':memory:')
const tmpStmt = tmpDb.prepare('SELECT 1')
const StmtProto = Object.getPrototypeOf(tmpStmt)

if (!StmtProto.safeIntegers) {
  StmtProto.safeIntegers = function () {
    return this
  }
}
if (!StmtProto.scanStatus) {
  // returns undefined immediately to signal no scan stats available.
  // zero-cache loops calling scanStatus(idx++) until it returns undefined.
  StmtProto.scanStatus = function () {
    return undefined
  }
}
if (!StmtProto.scanStatusV2) {
  // returns empty stats - zero-cache uses this for query performance tracking
  StmtProto.scanStatusV2 = function () {
    return []
  }
}
if (!StmtProto.scanStatusReset) {
  StmtProto.scanStatusReset = function () {}
}

tmpDb.close()

// scanstat constants (zero-cache references these as static properties)
Database.SQLITE_SCANSTAT_NLOOP = 0
Database.SQLITE_SCANSTAT_NVISIT = 1
Database.SQLITE_SCANSTAT_EST = 2
Database.SQLITE_SCANSTAT_NAME = 3
Database.SQLITE_SCANSTAT_EXPLAIN = 4
Database.SQLITE_SCANSTAT_SELECTID = 5
Database.SQLITE_SCANSTAT_PARENTID = 6
Database.SQLITE_SCANSTAT_NCYCLE = 7
Database.SQLITE_SCANSTAT_COMPLEX = 8

// export in the same shape as @rocicorp/zero-sqlite3:
// default export is the Database constructor, SqliteError is a named export
module.exports = Database
module.exports.SqliteError = SqliteError
