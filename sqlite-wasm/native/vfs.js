// adapted from node-sqlite3-wasm (MIT license, copyright 2022-2024 Tobias Enderle)

'use strict'

addToLibrary({
  $_shmRegistry: {},
  // per-file lock tracking for multi-connection coordination.
  // keys are file paths, values are { holders: Map<fi, level> }.
  // sqlite lock levels: NONE=0, SHARED=1, RESERVED=2, PENDING=3, EXCLUSIVE=4
  $_fileLocks: {},
  nodejsAccess: function (vfs, filePath, flags, outResult) {
    let aflags = fs.constants.F_OK
    if (flags == SQLITE_ACCESS_READWRITE) aflags = fs.constants.R_OK | fs.constants.W_OK
    if (flags == SQLITE_ACCESS_READ) aflags = fs.constants.R_OK
    try {
      fs.accessSync(UTF8ToString(filePath), aflags)
      setValue(outResult, 1, 'i32')
    } catch {
      setValue(outResult, 0, 'i32')
    }
    return SQLITE_OK
  },
  nodejsFullPathname: function (vfs, relPath, sizeFullPath, outFullPath) {
    const full = path.resolve(UTF8ToString(relPath))
    stringToUTF8(full, outFullPath, sizeFullPath)
    return full.length < sizeFullPath ? SQLITE_OK : SQLITE_CANTOPEN
  },
  nodejsWrite: function (fi, buffer, bytes, offset) {
    try {
      const bytesWritten = fs.writeSync(
        _fd(fi),
        HEAPU8.subarray(buffer, buffer + bytes),
        0,
        bytes,
        _safeInt(offset)
      )
      return bytesWritten != bytes ? SQLITE_IOERR_WRITE : SQLITE_OK
    } catch {
      return SQLITE_IOERR_WRITE
    }
  },
  nodejsSync: function (fi, flags) {
    try {
      fs.fsyncSync(_fd(fi))
    } catch {
      return SQLITE_IOERR_FSYNC
    }
    return SQLITE_OK
  },
  nodejsClose: function (fi) {
    _nodejsUnlock(fi, SQLITE_LOCK_NONE)
    try {
      fs.closeSync(_fd(fi))
    } catch {
      return SQLITE_IOERR_CLOSE
    }
    return SQLITE_OK
  },
  nodejsRead: function (fi, outBuffer, bytes, offset) {
    const buf = HEAPU8.subarray(outBuffer, outBuffer + bytes)
    let bytesRead
    try {
      bytesRead = fs.readSync(_fd(fi), buf, 0, bytes, _safeInt(offset))
    } catch {
      return SQLITE_IOERR_READ
    }
    if (bytesRead == bytes) {
      return SQLITE_OK
    } else if (bytesRead >= 0) {
      if (bytesRead < bytes) {
        try {
          buf.fill(0, bytesRead)
        } catch {
          return SQLITE_IOERR_READ
        }
      }
      return SQLITE_IOERR_SHORT_READ
    }
    return SQLITE_IOERR_READ
  },
  nodejsDelete: function (vfs, filePath, dirSync) {
    const pathStr = UTF8ToString(filePath)
    try {
      fs.unlinkSync(pathStr)
    } catch (err) {
      if (err.code != 'ENOENT') return SQLITE_IOERR_DELETE
    }
    if (dirSync) {
      let fd = -1
      try {
        fd = fs.openSync(path.dirname(pathStr), 'r')
        fs.fsyncSync(fd)
      } catch {
        return SQLITE_IOERR_FSYNC
      } finally {
        try {
          fs.closeSync(fd)
        } catch {
          return SQLITE_IOERR_FSYNC
        }
      }
    }
    return SQLITE_OK
  },
  nodejsRandomness: function (vfs, bytes, outBuffer) {
    const buf = HEAPU8.subarray(outBuffer, outBuffer + bytes)
    crypto.randomFillSync(buf)
    return bytes
  },
  nodejsTruncate: function (fi, size) {
    try {
      fs.ftruncateSync(_fd(fi), _safeInt(size))
    } catch {
      return SQLITE_IOERR_TRUNCATE
    }
    return SQLITE_OK
  },
  nodejsFileSize: function (fi, outSize) {
    try {
      setValue(outSize, fs.fstatSync(_fd(fi)).size, 'i64')
    } catch {
      return SQLITE_IOERR_FSTAT
    }
    return SQLITE_OK
  },
  nodejsLock__deps: ['$_fileLocks'],
  nodejsLock: function (fi, level) {
    var filePath = _path(fi)
    if (!_fileLocks[filePath]) _fileLocks[filePath] = new Map()
    var holders = _fileLocks[filePath]
    var myLevel = holders.get(fi) || SQLITE_LOCK_NONE

    // upgrading to RESERVED or higher: check for conflicts
    if (level >= SQLITE_LOCK_RESERVED && myLevel < SQLITE_LOCK_RESERVED) {
      for (var [other, otherLevel] of holders) {
        if (other !== fi && otherLevel >= SQLITE_LOCK_RESERVED) {
          return SQLITE_BUSY
        }
      }
    }

    // upgrading to EXCLUSIVE: check no other connection holds SHARED or above
    if (level >= SQLITE_LOCK_EXCLUSIVE && myLevel < SQLITE_LOCK_EXCLUSIVE) {
      for (var [other, otherLevel] of holders) {
        if (other !== fi && otherLevel >= SQLITE_LOCK_SHARED) {
          return SQLITE_BUSY
        }
      }
    }

    holders.set(fi, level)
    if (level > SQLITE_LOCK_NONE) _setLocked(fi, true)
    return SQLITE_OK
  },
  nodejsUnlock__deps: ['$_fileLocks'],
  nodejsUnlock: function (fi, level) {
    var filePath = _path(fi)
    var holders = _fileLocks[filePath]
    if (holders) {
      if (level == SQLITE_LOCK_NONE) {
        holders.delete(fi)
        _setLocked(fi, false)
      } else {
        holders.set(fi, level)
      }
    } else {
      if (level == SQLITE_LOCK_NONE) _setLocked(fi, false)
    }
    return SQLITE_OK
  },
  nodejsCheckReservedLock__deps: ['$_fileLocks'],
  nodejsCheckReservedLock: function (fi, outResult) {
    var filePath = _path(fi)
    var holders = _fileLocks[filePath]
    var reserved = 0
    if (holders) {
      for (var [other, otherLevel] of holders) {
        if (other !== fi && otherLevel >= SQLITE_LOCK_RESERVED) {
          reserved = 1
          break
        }
      }
    }
    setValue(outResult, reserved, 'i32')
    return SQLITE_OK
  },
  nodejs_max_path_length: function () {
    return process.platform == 'win32' ? 260 : 4096
  },
  // SHM methods for WAL/WAL2 support
  // in single-threaded WASM, SHM is just malloc'd memory shared by pointer
  nodejsShmMap__deps: ['$_shmRegistry'],
  nodejsShmMap: function (fi, pgno, pgsz, isWrite, ppOut) {
    const filePath = _path(fi)
    if (!_shmRegistry[filePath]) _shmRegistry[filePath] = {}
    const regions = _shmRegistry[filePath]
    if (!(pgno in regions)) {
      if (!isWrite) {
        setValue(ppOut, 0, '*')
        return SQLITE_OK
      }
      const buf = _malloc(pgsz)
      HEAPU8.fill(0, buf, buf + pgsz)
      regions[pgno] = buf
    }
    setValue(ppOut, regions[pgno], '*')
    return SQLITE_OK
  },
  nodejsShmLock: function (fi, offset, n, flags) {
    // single-threaded: all locks succeed immediately
    return SQLITE_OK
  },
  nodejsShmBarrier: function (fi) {
    // no-op in single-threaded WASM
  },
  nodejsShmUnmap__deps: ['$_shmRegistry'],
  nodejsShmUnmap: function (fi, deleteFlag) {
    if (deleteFlag) {
      const filePath = _path(fi)
      const regions = _shmRegistry[filePath]
      if (regions) {
        for (const pgno in regions) _free(regions[pgno])
        delete _shmRegistry[filePath]
      }
    }
    return SQLITE_OK
  },
  // real blocking sleep for busy_timeout support (C sleep/usleep are no-ops in WASM)
  nodejsSleep: function (vfs, nMicro) {
    var ms = Math.max(1, Math.floor(nMicro / 1000))
    var sab = new SharedArrayBuffer(4)
    Atomics.wait(new Int32Array(sab), 0, 0, ms)
    return nMicro
  },
  nodejs_open: function (filePath, flags, mode) {
    let oflags = 0
    if (flags & SQLITE_OPEN_EXCLUSIVE) oflags |= fs.constants.O_EXCL
    if (flags & SQLITE_OPEN_CREATE) oflags |= fs.constants.O_CREAT
    if (flags & SQLITE_OPEN_READONLY) oflags |= fs.constants.O_RDONLY
    if (flags & SQLITE_OPEN_READWRITE) oflags |= fs.constants.O_RDWR

    try {
      return fs.openSync(UTF8ToString(filePath), oflags, mode)
    } catch {
      return -1
    }
  },
})
