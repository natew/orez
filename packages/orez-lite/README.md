# orez-lite

`orez-lite` is the SQLite and Rust sync engine for Zero applications.

- `orez-lite` provides the host-neutral mutation executor.
- `orez-lite/client` connects a stock Zero client to the HTTP sync protocol.
- `orez-lite/browser` runs the engine with SQLite WASM in a browser worker.
- `orez-lite/native` runs the prebuilt native engine from an application-owned
  Zero schema, SQLite initializer, and HTTP policy callbacks.
- `orez-lite/local` prepares SQLite and owns the local native process.
- `orez-lite/vite` starts the local host only while Vite is serving.
- `orez-lite/aggregate` generates count and sum migrations and projects their
  optimistic client updates.
- `orez-lite/cloudflare` provides the Cloudflare runtime and data-worker factory.
- `orez-lite/cloudflare/build` provides Node-side worker build and deployment tools.

The package derives its database projection from the application’s Zero schema.
Applications do not maintain separate table or column maps.

The HTTP client reports rejected pull and push status codes through its lifecycle
callback. A 401 or 403 emits Zero's `Unauthorized` frame and parks the client in
`needs-auth`; other non-retryable 4xx responses are terminal. Applications own
the bounded token refresh policy and access-denied surface.

## Native host

The native host is configured entirely by the application:

```ts
import { createNativeHost } from 'orez-lite/native'

const host = createNativeHost({
  schema,
  initSql,
  dataDir: '.orez/native',
  port: 7849,
  adminTokenEnv: 'OREZ_ADMIN_TOKEN',
  callbacks: {
    authenticate: 'https://localhost:3000/api/zero/auth',
    authorizeWake: 'https://localhost:3000/api/zero/wake-authorize',
    transformQueries: 'https://localhost:3000/api/zero/pull',
    certificateAuthority: '.certs/local-root.pem',
  },
  allowedOrigins: ['https://localhost:3000'],
})

const process = host.start({
  env: { ...globalThis.process.env, OREZ_ADMIN_TOKEN: adminToken },
})
```

Orez owns the sync protocol, namespace databases, query cache, and wake
delivery. The callbacks keep authentication, authorization, and query policy in
the application. Storage retention is disabled unless the application
explicitly supplies `workerRetention`.

Application development normally uses the higher-level local configuration:

```ts
// orez-lite.config.ts
import { defineLocalConfig } from 'orez-lite/local'

export default defineLocalConfig({
  schema,
  dataDir: '.orez/application-sql',
  namespace: 'app',
  port: 4949,
  prepare: migrate,
  callbacks: {
    authenticate: 'http://127.0.0.1:4100/api/zero/auth',
    authorizeWake: 'http://127.0.0.1:4100/api/zero/wake-authorize',
    transformQueries: 'http://127.0.0.1:4100/api/zero/queries',
  },
  allowedOrigins: ['http://127.0.0.1:4100'],
})
```

```ts
// vite.config.ts
import { orez } from 'orez-lite/vite'

export default {
  plugins: [orez()],
}
```

The Vite plugin applies only to local serve mode. Production builds keep using
the Cloudflare host. Projects without Vite run the same supervisor through the
CLI:

```sh
orez-lite dev -- node server.js
```

## Cloudflare data-object status

The Cloudflare data worker forwards `GET /<namespace>/_orez/status` to the
namespace's data Durable Object. The request must present the configured
`OREZ_DO_WRITE_BUDGET_ADMIN_TOKEN` as `x-orez-admin-token` or a bearer token.
The response identifies the namespace and object, reports database size,
application SQL reader/writer queue depth, recent write-grant wait p50/p99/max,
SQL rows read and written, and request/session counters.

These measurements are bounded, in-memory values since the current object boot.
They reset on eviction and do not add storage writes. The existing environment
variable name is retained for deployed compatibility even though the token now
protects the broader status route as well as write-budget controls.

Cloudflare namespace backup summaries also include `tableRows`, the row count
observed for every exported table during the existing streaming scan. Consumers
can persist fleet profiles without issuing a second set of table reads.

`backupManager.exportNamespace(env, namespace)` copies exportable tables into
physical `_orez_bk_*` snapshot tables in one synchronous storage transaction,
admitted through the application writer queue. The snapshot captures source
schema and the committed write marker together. It does not advance the marker,
fire CDC triggers, or charge the application write-budget window; physical copy
rows remain visible in billing telemetry.

Snapshot columns use temporary `c0`, `c1`, … names so source `rowid`, `_rowid_`,
`oid`, and cursor-shaped columns cannot interfere with paging. The export maps
these back to source names and pages immutable rowid tables in bounded background
read sessions.
Writers may preempt a chunk, which is retried up to `chunkAttempts` times. Live
commits never invalidate the snapshot, and R2 uploads run outside read sessions.
Dumps keep their existing source table names, CREATE statements, and format.
Each copy has generation-specific table names so restarting the object cannot
substitute a new snapshot into an older scan. Exports release ownership before
cleanup admission; a failed cleanup is reclaimed by the next export. An RPC
lease also releases ownership when disposed, and boot removes stale snapshots.
Concurrent exports cannot overwrite each other's snapshots. Schema discovery and restore
exclude the reserved `_orez_bk_*` prefix.
