# Orez and Lime Sync websites

This One app builds two independent documentation sites from one shared
component and styling system:

- Orez: https://orez-docs.natewienert.workers.dev
- Lime Sync: https://lime-sync.natewienert.workers.dev

Orez documents the local runner around stock zero-cache. Lime Sync documents
the Rust and SQLite replacement sync server. Orez MDX lives in `data/docs`;
Lime Sync MDX lives in `data/lime-sync-docs`.

## Develop

```sh
bun install
bun dev
```

`bun dev` starts the Orez site. Use `bun run dev:lime-sync` for Lime Sync.

## Build

```sh
bun run build
```

The command builds Orez into `dist/orez` and Lime Sync into `dist/lime-sync`.
Each output contains pre-rendered pages under `client` and an independent
Cloudflare Worker under `worker`.

Pushes to `main` build and deploy both sites after every required CI job
passes. The GitHub repository must have a `CLOUDFLARE_API_TOKEN` Actions secret
with Workers Scripts write access for the Lightstrike Labs account. Tags build
and archive both sites but do not deploy them.

The MDX parser runs only at build time. The `@vite-ignore` annotations on its
dynamic imports and the WASI alias in `vite.config.ts` keep the parser's native
and fallback binaries out of the runtime Worker bundles.
