// in-memory VFS for browser Web Workers.
// replaces Node.js fs operations with ArrayBuffer-backed storage.
// SHM operations are unchanged (already malloc-based).

'use strict'

addToLibrary({
  $_shmRegistry: {},
  $_fileLocks: {},

  // in-memory file storage
  $_memfs: {
    files: {}, // path → { data: Uint8Array, size: number }
    fds: {}, // fd → { path: string, pos: number }
    nextFd: 10,
  },

  $_memfsGetOrCreate__deps: ['$_memfs'],
  $_memfsGetOrCreate: function (path) {
    if (!_memfs.files[path]) {
      _memfs.files[path] = { data: new Uint8Array(4096), size: 0 }
    }
    return _memfs.files[path]
  },

  $_memfsEnsureSize__deps: ['$_memfs'],
  $_memfsEnsureSize: function (file, needed) {
    if (needed <= file.data.length) return
    var newSize = Math.max(file.data.length * 2, needed)
    var newData = new Uint8Array(newSize)
    newData.set(file.data.subarray(0, file.size))
    file.data = newData
  },

  nodejsAccess__deps: ['$_memfs'],
  nodejsAccess: function (vfs, filePath, flags, outResult) {
    var path = UTF8ToString(filePath)
    setValue(outResult, _memfs.files[path] ? 1 : 0, 'i32')
    return SQLITE_OK
  },

  nodejsFullPathname: function (vfs, relPath, sizeFullPath, outFullPath) {
    var full = UTF8ToString(relPath)
    // normalize: strip leading ./ and ensure absolute-ish
    if (full.startsWith('./')) full = full.substring(2)
    if (!full.startsWith('/')) full = '/' + full
    stringToUTF8(full, outFullPath, sizeFullPath)
    return full.length < sizeFullPath ? SQLITE_OK : SQLITE_CANTOPEN
  },

  nodejs_open__deps: ['$_memfs', '$_memfsGetOrCreate'],
  nodejs_open: function (filePath, flags, mode) {
    var path = UTF8ToString(filePath)
    var exclusive = flags & SQLITE_OPEN_EXCLUSIVE

    if (exclusive && _memfs.files[path]) return -1

    // always create in memfs — no persistent storage to check
    _memfsGetOrCreate(path)
    var fd = _memfs.nextFd++
    _memfs.fds[fd] = { path: path, pos: 0 }
    return fd
  },

  nodejsRead__deps: ['$_memfs'],
  nodejsRead: function (fi, outBuffer, bytes, offset) {
    var fdInfo = _memfs.fds[_fd(fi)]
    if (!fdInfo) return SQLITE_IOERR_READ
    var file = _memfs.files[fdInfo.path]
    if (!file) return SQLITE_IOERR_READ

    var off = Number(offset)
    var buf = HEAPU8.subarray(outBuffer, outBuffer + bytes)

    if (off >= file.size) {
      buf.fill(0)
      return SQLITE_IOERR_SHORT_READ
    }

    var available = Math.min(bytes, file.size - off)
    buf.set(file.data.subarray(off, off + available))

    if (available < bytes) {
      buf.fill(0, available)
      return SQLITE_IOERR_SHORT_READ
    }

    return SQLITE_OK
  },

  nodejsWrite__deps: ['$_memfs', '$_memfsEnsureSize'],
  nodejsWrite: function (fi, buffer, bytes, offset) {
    var fdInfo = _memfs.fds[_fd(fi)]
    if (!fdInfo) return SQLITE_IOERR_WRITE
    var file = _memfs.files[fdInfo.path]
    if (!file) return SQLITE_IOERR_WRITE

    var off = Number(offset)
    _memfsEnsureSize(file, off + bytes)
    file.data.set(HEAPU8.subarray(buffer, buffer + bytes), off)
    if (off + bytes > file.size) file.size = off + bytes

    return SQLITE_OK
  },

  nodejsSync: function (fi, flags) {
    // no-op: in-memory, always synced
    return SQLITE_OK
  },

  nodejsClose__deps: ['$_memfs'],
  nodejsClose: function (fi) {
    _nodejsUnlock(fi, SQLITE_LOCK_NONE)
    var fd = _fd(fi)
    delete _memfs.fds[fd]
    return SQLITE_OK
  },

  nodejsTruncate__deps: ['$_memfs'],
  nodejsTruncate: function (fi, size) {
    var fdInfo = _memfs.fds[_fd(fi)]
    if (!fdInfo) return SQLITE_IOERR_TRUNCATE
    var file = _memfs.files[fdInfo.path]
    if (!file) return SQLITE_IOERR_TRUNCATE

    var sz = Number(size)
    if (sz < file.size) {
      file.data.fill(0, sz, file.size)
      file.size = sz
    } else if (sz > file.size) {
      _memfsEnsureSize(file, sz)
      file.data.fill(0, file.size, sz)
      file.size = sz
    }
    return SQLITE_OK
  },

  nodejsFileSize__deps: ['$_memfs'],
  nodejsFileSize: function (fi, outSize) {
    var fdInfo = _memfs.fds[_fd(fi)]
    if (!fdInfo) return SQLITE_IOERR_FSTAT
    var file = _memfs.files[fdInfo.path]
    if (!file) return SQLITE_IOERR_FSTAT
    setValue(outSize, file.size, 'i64')
    return SQLITE_OK
  },

  nodejsDelete__deps: ['$_memfs'],
  nodejsDelete: function (vfs, filePath, dirSync) {
    var path = UTF8ToString(filePath)
    delete _memfs.files[path]
    return SQLITE_OK
  },

  nodejsRandomness: function (vfs, bytes, outBuffer) {
    var buf = HEAPU8.subarray(outBuffer, outBuffer + bytes)
    if (typeof globalThis.crypto !== 'undefined' && globalThis.crypto.getRandomValues) {
      globalThis.crypto.getRandomValues(buf)
    } else {
      for (var i = 0; i < bytes; i++) buf[i] = Math.floor(Math.random() * 256)
    }
    return bytes
  },

  nodejsLock__deps: ['$_fileLocks'],
  nodejsLock: function (fi, level) {
    var filePath = _path(fi)
    if (!_fileLocks[filePath]) _fileLocks[filePath] = new Map()
    var holders = _fileLocks[filePath]
    var myLevel = holders.get(fi) || SQLITE_LOCK_NONE

    if (level >= SQLITE_LOCK_RESERVED && myLevel < SQLITE_LOCK_RESERVED) {
      for (var [other, otherLevel] of holders) {
        if (other !== fi && otherLevel >= SQLITE_LOCK_RESERVED) {
          return SQLITE_BUSY
        }
      }
    }

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
    return 4096
  },

  // SHM methods for WAL/WAL2 support — same as Node version (malloc-based)
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
    return SQLITE_OK
  },

  nodejsShmBarrier: function (fi) {},

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

  // sleep using Atomics.wait (works in Web Workers with SharedArrayBuffer)
  nodejsSleep: function (vfs, nMicro) {
    // in browser main thread, Atomics.wait isn't allowed — just return
    if (typeof SharedArrayBuffer === 'undefined') return nMicro
    try {
      var ms = Math.max(1, Math.floor(nMicro / 1000))
      var sab = new SharedArrayBuffer(4)
      Atomics.wait(new Int32Array(sab), 0, 0, ms)
    } catch {}
    return nMicro
  },
})
