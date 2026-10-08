#include "sqlite-vec.h"

// sqlite invokes this once at initialization; every connection receives vec0.
int bedrock_sqlite_init(const char *unused) {
  return sqlite3_auto_extension((void (*)(void))sqlite3_vec_init);
}
