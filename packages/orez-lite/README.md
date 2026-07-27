# orez-lite

`orez-lite` is the SQLite and Rust sync engine for Zero applications.

- `orez-lite` provides the host-neutral mutation executor.
- `orez-lite/client` connects a stock Zero client to the HTTP sync protocol.
- `orez-lite/browser` runs the engine with SQLite WASM in a browser worker.
- `orez-lite/native` runs the prebuilt native engine from an application-owned
  Zero schema, SQLite initializer, and HTTP policy callbacks.
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
