# CF-Compatible PGlite Build

PGlite compiled without `-sMAIN_MODULE=2` and `-sALLOW_TABLE_GROWTH`
for Cloudflare Workers compatibility.

Key differences from standard PGlite:

- No dynamic linking (extensions must be statically linked)
- No `addFunction`/`removeFunction` (no runtime WASM compilation)
- `ENVIRONMENT=web,worker` only (no node — avoids require("fs"))
- No side modules (no dlopen/dlsym)

Files:

- `pglite.wasm` — CF-compatible postgres WASM binary
- `pglite.data` — filesystem bundle (postgres data directory)
- `pglite.js` — Emscripten JS glue

Build: `cd postgres-pglite && docker run ... ./build-pglite-cf.sh`
