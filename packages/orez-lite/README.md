# orez-lite

`orez-lite` is the SQLite and Rust sync engine for Zero applications.

- `orez-lite` provides the host-neutral mutation executor.
- `orez-lite/client` connects a stock Zero client to the HTTP sync protocol.
- `orez-lite/browser` runs the engine with SQLite WASM in a browser worker.
- `orez-lite/native` runs the prebuilt native engine from an application-owned
  Zero schema, SQLite initializer, and HTTP policy callbacks.
- `orez-lite/local` prepares SQLite and owns the local native process.
- `orez-lite/vite` starts the local host only while Vite is serving.
- `orez-lite/rollup` generates count and sum migrations and projects their
  optimistic client updates.
- `orez-lite/cloudflare` provides the Cloudflare runtime and data-worker factory.
- `orez-lite/cloudflare/build` provides Node-side worker build and deployment tools.

The package derives its database projection from the application’s Zero schema.
Applications do not maintain separate table or column maps.

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
