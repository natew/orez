#include "sqlite-vec.h"

int sqlite3Fts5BetterTrigramInit(sqlite3 *db);
int sqlite3Fts5HtmlInit(sqlite3 *db);

// sqlite auto-extensions receive three arguments; the upstream core initializers
// take only the connection. wrappers preserve that ABI and return their errors.
static int bedrock_fts_init(sqlite3 *db, char **error,
                            const sqlite3_api_routines *api) {
  int rc = sqlite3Fts5BetterTrigramInit(db);
  if (rc == SQLITE_OK) rc = sqlite3Fts5HtmlInit(db);
  return rc;
}

// sqlite invokes this once; vec0 and both tokenizers initialize on every open.
int bedrock_sqlite_init(const char *unused) {
  int rc = sqlite3_auto_extension((void (*)(void))sqlite3_vec_init);
  if (rc == SQLITE_OK) {
    rc = sqlite3_auto_extension((void (*)(void))bedrock_fts_init);
  }
  return rc;
}
