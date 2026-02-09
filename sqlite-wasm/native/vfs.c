// adapted from node-sqlite3-wasm (MIT license, copyright 2022-2024 Tobias Enderle)
// https://github.com/tndrle/node-sqlite3-wasm
// extended with SHM methods for WAL/WAL2 support

#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#include "sqlite3.h"

typedef struct NodeJsFile NodeJsFile;
struct NodeJsFile {
  sqlite3_file base;
  int fd;
  int isLocked;
  const char *path;
};

extern int nodejsWrite(sqlite3_file *, const void *, int, sqlite_int64);
extern int nodejsClose(sqlite3_file *);
extern int nodejsRead(sqlite3_file *, void *, int, sqlite_int64);
extern int nodejsSync(sqlite3_file *, int);
extern int nodejsDelete(sqlite3_vfs *, const char *, int);
extern int nodejsFullPathname(sqlite3_vfs *, const char *, int, char *);
extern int nodejsAccess(sqlite3_vfs *, const char *, int, int *);
extern int nodejsRandomness(sqlite3_vfs *, int, char *);
extern int nodejsTruncate(sqlite3_file *, sqlite_int64);
extern int nodejsFileSize(sqlite3_file *, sqlite_int64 *);
extern int nodejsLock(sqlite3_file *, int);
extern int nodejsUnlock(sqlite3_file *, int);
extern int nodejsCheckReservedLock(sqlite3_file *, int *);

// SHM methods for WAL support
extern int nodejsShmMap(sqlite3_file *, int, int, int, void volatile **);
extern int nodejsShmLock(sqlite3_file *, int, int, int);
extern void nodejsShmBarrier(sqlite3_file *);
extern int nodejsShmUnmap(sqlite3_file *, int);

extern int nodejs_open(const char *, int, int);
extern int nodejs_max_path_length();

static int nodejsFileControl(sqlite3_file *pFile, int op, void *pArg) {
  return SQLITE_NOTFOUND;
}

static int nodejsSectorSize(sqlite3_file *pFile) { return 0; }
static int nodejsDeviceCharacteristics(sqlite3_file *pFile) { return 0; }

static int nodejsOpen(
    sqlite3_vfs *pVfs,
    const char *zName,
    sqlite3_file *pFile,
    int flags,
    int *pOutFlags
) {
  static const sqlite3_io_methods nodejsio = {
      2,                           // iVersion 2 for WAL/SHM support
      nodejsClose,
      nodejsRead,
      nodejsWrite,
      nodejsTruncate,
      nodejsSync,
      nodejsFileSize,
      nodejsLock,
      nodejsUnlock,
      nodejsCheckReservedLock,
      nodejsFileControl,
      nodejsSectorSize,
      nodejsDeviceCharacteristics,
      nodejsShmMap,
      nodejsShmLock,
      nodejsShmBarrier,
      nodejsShmUnmap
  };

  NodeJsFile *p = (NodeJsFile *)pFile;
  memset(p, 0, sizeof(NodeJsFile));

  if (zName == NULL) return SQLITE_IOERR;

  p->fd = nodejs_open(zName, flags, 0600);
  if (p->fd < 0) return SQLITE_CANTOPEN;
  if (pOutFlags) *pOutFlags = flags;
  p->base.pMethods = &nodejsio;
  p->path = zName;
  return SQLITE_OK;
}

static void *nodejsDlOpen(sqlite3_vfs *pVfs, const char *zPath) { return NULL; }
static void nodejsDlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg) {
  sqlite3_snprintf(nByte, zErrMsg, "Loadable extensions are not supported");
}
static void (*nodejsDlSym(sqlite3_vfs *pVfs, void *pH, const char *z))(void) {
  return 0;
}
static void nodejsDlClose(sqlite3_vfs *pVfs, void *pHandle) {}

// implemented in JS (vfs.js) via Atomics.wait for real blocking sleep
extern int nodejsSleep(sqlite3_vfs *, int);

static int nodejsCurrentTimeInt64(sqlite3_vfs *pVfs, sqlite3_int64 *piNow) {
  static const sqlite3_int64 unixEpoch = 24405875 * (sqlite3_int64)8640000;
  struct timeval sNow;
  gettimeofday(&sNow, 0);
  *piNow = unixEpoch + 1000 * (sqlite3_int64)sNow.tv_sec + sNow.tv_usec / 1000;
  return SQLITE_OK;
}

SQLITE_API int sqlite3_os_init(void) {
  static sqlite3_vfs nodejsvfs = {
      2,
      sizeof(NodeJsFile),
      -1,
      NULL,
      "nodejs",
      NULL,
      nodejsOpen,
      nodejsDelete,
      nodejsAccess,
      nodejsFullPathname,
      nodejsDlOpen,
      nodejsDlError,
      nodejsDlSym,
      nodejsDlClose,
      nodejsRandomness,
      nodejsSleep,
      NULL,
      NULL,
      nodejsCurrentTimeInt64
  };
  nodejsvfs.mxPathname = nodejs_max_path_length();
  sqlite3_vfs_register(&nodejsvfs, 1);
  return SQLITE_OK;
}

SQLITE_API int sqlite3_os_end(void) { return SQLITE_OK; }
