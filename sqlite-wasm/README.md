# Bedrock SQLite WASM

This build shares native SQLite across Node and browser consumers. The Makefile
keeps its existing Bedrock, FTS, vector and VFS inputs and enables
`SQLITE_ENABLE_SESSION` and `SQLITE_ENABLE_PREUPDATE_HOOK` for native sessions.
Build the synchronous, asynchronous and browser outputs together with
`make all browser`. Publish the JavaScript and WASM from the same build.

`Database.backupTo(destination, sourceName, destinationName)` uses SQLite's
backup API, including named attached schemas. `filename(name)` reads
SQLite's actual schema filename. `updateHook(callback)` installs one callback
per connection with operation code, database name, table name and row ID.
Row IDs above JavaScript's safe integer range remain bigint. Passing null
removes the callback; closing a connection removes it and closes its sessions.
`commitHook(callback)` and `rollbackHook(callback)` likewise use SQLite native
hooks. The commit callback returns zero to accept the commit or nonzero to
roll it back. Hook callbacks run inside SQLite and must not modify that
connection. Passing null removes a hook.

`createSession(databaseName)` creates a native SQLite session. Its `attach`,
`enable`, `changeset` and `close` methods call the SQLite session API. A null
attachment selects all tables. Changeset buffers are copied before release
with `sqlite3_free`. `applyChangeset(bytes, onConflict)` exposes SQLite's
conflict disposition values; its default aborts. `invertChangeset(bytes)` uses
the native inverse operation. Neither method rewrites SQL or replays queries.

The integration case in `test/database.test.ts` covers backup, changeset
application and inversion, conflict callbacks, large row IDs and cleanup.
`FTS5-SOURCES.md` records the unchanged pinned native extension inputs and
their earlier validation. These APIs add no cloud queries or writes.
