# `sync-browser-host`

`sync-browser-host` runs Orez's Rust sync engine and Bedrock SQLite in one
browser worker. It exposes authenticated Zero `/pull` and `/push` handlers,
direct SQL for the generated project backend, and a MessagePort client/server
pair. Every operation is serialized. Every transaction that can write SQLite
is checkpointed to IndexedDB before its response succeeds.

Generated applications keep their on-zero authoring surface. The on-zero
adapter supplies this host's mutator registry and named-query resolver; the host
does not run on-zero's legacy `PushProcessor` or its bookkeeping tables.

Orez ships the Bedrock and sync-engine WASM assets with the subpath. Consumers
can override their URLs for a custom asset pipeline, but ordinary Vite workers
need no asset glue.

```ts
const host = await createBrowserSyncHost({
  storageKey: 'project:route-session:format-v1',
  schema,
  initialize(sql) {
    sql.exec('CREATE TABLE todo (id TEXT PRIMARY KEY, title TEXT NOT NULL)')
  },
  authenticate(request) {
    return request.headers.has('authorization') ? { userID: 'preview' } : null
  },
  mutators,
})

serveBrowserSyncHostPort(host, port)
```

Run the package test lane with `bun run test:sync-browser-host` from the Orez
root. It builds the current Rust WASM and drives a real Chromium worker through
snapshot pulls, mutations, replay, two attached ports, IndexedDB restore, and
worker termination at every transaction boundary.
