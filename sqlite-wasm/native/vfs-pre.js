// adapted from node-sqlite3-wasm (MIT license, copyright 2022-2024 Tobias Enderle)

'use strict'

const path = require('node:path')
const crypto = require('node:crypto')

const SQLITE_CANTOPEN = 14
const SQLITE_IOERR_READ = 266
const SQLITE_IOERR_SHORT_READ = 522
const SQLITE_IOERR_FSYNC = 1034
const SQLITE_IOERR_WRITE = 778
const SQLITE_IOERR_DELETE = 2570
const SQLITE_IOERR_CLOSE = 4106
const SQLITE_IOERR_TRUNCATE = 1546
const SQLITE_IOERR_FSTAT = 1802
const SQLITE_IOERR_LOCK = 3850
const SQLITE_IOERR_UNLOCK = 2058

const SQLITE_OPEN_READONLY = 1
const SQLITE_OPEN_READWRITE = 2
const SQLITE_OPEN_CREATE = 4
const SQLITE_OPEN_EXCLUSIVE = 16

const SQLITE_ACCESS_READWRITE = 1
const SQLITE_ACCESS_READ = 2

const SQLITE_LOCK_NONE = 0
const SQLITE_LOCK_SHARED = 1
const SQLITE_LOCK_RESERVED = 2
const SQLITE_LOCK_PENDING = 3
const SQLITE_LOCK_EXCLUSIVE = 4
const SQLITE_BUSY = 5

function _fd(fileInfo) {
  return getValue(fileInfo + 4, 'i32')
}

function _isLocked(fileInfo) {
  return getValue(fileInfo + 8, 'i32') != 0
}

function _setLocked(fileInfo, locked) {
  setValue(fileInfo + 8, locked ? 1 : 0, 'i32')
}

function _path(fileInfo) {
  return UTF8ToString(getValue(fileInfo + 12, 'i32'))
}

function _safeInt(bigInt) {
  if (bigInt < Number.MIN_SAFE_INTEGER || bigInt > Number.MAX_SAFE_INTEGER) throw 0
  return Number(bigInt)
}
