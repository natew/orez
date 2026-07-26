# orez-lite

`orez-lite` is the SQLite and Rust sync engine for Zero applications.

- `orez-lite` provides the host-neutral mutation executor.
- `orez-lite/client` connects a stock Zero client to the HTTP sync protocol.
- `orez-lite/browser` runs the engine with SQLite WASM in a browser worker.
- `orez-lite/cloudflare` provides the Cloudflare runtime and data-worker factory.
- `orez-lite/cloudflare/build` provides Node-side worker build and deployment tools.

The package derives its database projection from the application’s Zero schema.
Applications do not maintain separate table or column maps.
