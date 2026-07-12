# orez website

The public documentation site for orez. It is a small [One](https://onestack.dev)
app with a static homepage and Rust-powered MDX documentation.

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

The MDX parser runs only at build time. The `@vite-ignore` annotations on its
dynamic imports and the WASI alias in `vite.config.ts` keep the parser's native
and fallback binaries out of the runtime Worker bundle.
