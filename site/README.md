# orez website

The public documentation site for orez. It is a small [One](https://onestack.dev)
app with a static homepage and Rust-powered MDX documentation.

Live site: https://orez-docs.lslcf.workers.dev

## Develop

```sh
bun install
bun dev
```

## Build

```sh
bun run build
```

One emits pre-rendered pages in `dist/client` and a Cloudflare Worker in
`dist/worker`. Documentation lives in `data/docs`; the navigation is defined in
`app/docs/_layout.tsx`, and the design is contained in `app/styles.css`.

Pushes to `main` build and deploy the site after every required CI job passes.
The GitHub repository must have a `CLOUDFLARE_API_TOKEN` Actions secret with
Workers Scripts write access for the Lightstrike Labs account. Tags build and
archive the site but do not deploy it.

The MDX parser runs only at build time. The `@vite-ignore` annotations on its
dynamic imports and the WASI alias in `vite.config.ts` keep the parser's native
and fallback binaries out of the runtime Worker bundle.
