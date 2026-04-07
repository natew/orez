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
  // file-backed SHM for cross-process WAL2 coordination:
  // - pages are malloc'd in WASM heap (per-process) but synced via a -shm file
  // - writers flush dirty pages to the file on exclusive lock release
  // - readers refresh from the file on xShmBarrier (only if another process wrote)
  // SHM lock flag constants (from sqlite3.h, passed by C code):
  //   SQLITE_SHM_UNLOCK=1, SQLITE_SHM_LOCK=2, SQLITE_SHM_SHARED=4, SQLITE_SHM_EXCLUSIVE=8
  nodejsShmMap__deps: ['$_shmRegistry'],
  nodejsShmMap: function (fi, pgno, pgsz, isWrite, ppOut) {
    var filePath = _path(fi)
    if (!_shmRegistry[filePath]) _shmRegistry[filePath] = {}
    var regions = _shmRegistry[filePath]
    if (!(pgno in regions) || typeof regions[pgno] !== 'object') {
      if (!isWrite) {
        setValue(ppOut, 0, '*')
        return SQLITE_OK
      }
      var buf = _malloc(pgsz)
      HEAPU8.fill(0, buf, buf + pgsz)
      regions[pgno] = { ptr: buf, pgsz: pgsz }
    }
    setValue(ppOut, regions[pgno].ptr, '*')
    return SQLITE_OK
  },
  nodejsShmLock: function (fi, offset, n, flags) {
    var filePath = _path(fi)
    if (!_shmRegistry[filePath]) _shmRegistry[filePath] = {}
    var reg = _shmRegistry[filePath]
    // track exclusive lock for barrier/flush coordination
    if (flags & 2 && flags & 8) {
      // acquiring exclusive lock
      reg._hasExclusiveLock = true
    }
    if (flags & 1 && flags & 8 && reg._hasExclusiveLock) {
      // releasing exclusive lock — flush all pages to -shm file
      var shmPath = filePath + '-shm'
      var fd
      try {
        fd = fs.openSync(shmPath, fs.constants.O_RDWR | fs.constants.O_CREAT, 0o644)
      } catch (e) {
        reg._hasExclusiveLock = false
        return SQLITE_OK
      }
      for (var pgno in reg) {
        if (typeof reg[pgno] !== 'object' || !reg[pgno].ptr) continue
        var r = reg[pgno]
        try {
          fs.writeSync(
            fd,
            HEAPU8.subarray(r.ptr, r.ptr + r.pgsz),
            0,
            r.pgsz,
            Number(pgno) * r.pgsz
          )
        } catch (e) {}
      }
      try {
        fs.fsyncSync(fd)
      } catch (e) {}
      // record our own write mtime so the barrier can skip reads from our own flushes
      try {
        reg._lastShmWrite = fs.fstatSync(fd).mtimeMs
      } catch (e) {
        reg._lastShmWrite = Date.now()
      }
      fs.closeSync(fd)
      reg._hasExclusiveLock = false
    }
    return SQLITE_OK
  },
  nodejsShmBarrier: function (fi) {
    // reader refresh: read from -shm file only when another process has written.
    // skip if: holding exclusive lock (writer), or file mtime matches our last write
    // (same process wrote it — WASM heap already has the data).
    var filePath = _path(fi)
    var reg = _shmRegistry[filePath]
    if (!reg || reg._hasExclusiveLock) return
    var shmPath = filePath + '-shm'
    var stat
    try {
      stat = fs.statSync(shmPath)
    } catch (e) {
      return
    } // file doesn't exist yet
    // if we wrote the file and nobody else has since, skip — heap is already current
    if (reg._lastShmWrite && stat.mtimeMs <= reg._lastShmWrite) return
    var fd
    try {
      fd = fs.openSync(shmPath, fs.constants.O_RDONLY)
    } catch (e) {
      return
    }
    for (var pgno in reg) {
      if (typeof reg[pgno] !== 'object' || !reg[pgno].ptr) continue
      var r = reg[pgno]
      try {
        fs.readSync(
          fd,
          HEAPU8.subarray(r.ptr, r.ptr + r.pgsz),
          0,
          r.pgsz,
          Number(pgno) * r.pgsz
        )
      } catch (e) {} // short read is fine — page doesn't exist in file yet
    }
    fs.closeSync(fd)
    reg._lastShmWrite = stat.mtimeMs // treat as if we wrote it — prevents re-reading same data
  },
  nodejsShmUnmap__deps: ['$_shmRegistry'],
  nodejsShmUnmap: function (fi, deleteFlag) {
    if (deleteFlag) {
      var filePath = _path(fi)
      var regions = _shmRegistry[filePath]
      if (regions) {
        for (var pgno in regions) {
          if (typeof regions[pgno] === 'object' && regions[pgno].ptr)
            _free(regions[pgno].ptr)
        }
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
