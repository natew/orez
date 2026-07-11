# orez docs site

The orez documentation website. A minimal [One](https://onestack.dev) app:
a one-screen homepage plus MDX docs, statically generated and deployed to
Cloudflare Workers.

Live preview: https://orez-docs.lslcf.workers.dev

## Layout

- `app/` One file-based routes. `index.tsx` is the homepage; `docs/[slug].tsx`
  renders one MDX doc; `docs/_layout.tsx` is the sidebar shell.
- `data/docs/*.mdx` the doc content, mirrored from `../docs/sync/*.md` with
  frontmatter added. Edit these to change the docs.
- `app/styles.css` the whole design. Plain typography, no component library.
- `components/MDXComponents.tsx` maps internal links to client-side navigation;
  every other element renders as plain HTML.

## Develop

```sh
bun install
bun dev        # one dev
```

## Build and deploy

```sh
bun run build  # one build -> dist/client (SSG) + dist/worker (CF worker)

# deploy the preview to the Lightstrike Labs (lslcf) account
CLOUDFLARE_ACCOUNT_ID=6afff1f79e2fd12f1cfd1bfe1dfd08d1 \
  bunx wrangler deploy --name orez-docs
```

`bun run build` produces both the static HTML and a Cloudflare worker that
serves it. Deploy runs from the repo root, where One writes a
`.wrangler/deploy/config.json` that redirects wrangler to
`dist/worker/wrangler.json`.

## The `@vite-ignore` on the MDX imports

The doc routes call `@vxrn/mdx-rust` (which wraps the satteri MDX parser) only
inside `generateStaticParams` and the loader, both of which run at build time.
One's Cloudflare worker build runs with `configFile: false`, so `vite.config`
externals do not reach it, and the parser's wasm fallback
(`@bruits/satteri-wasm32-wasi`, pulled through `satteri/browser.js`) fails to
resolve in the workerd bundle. Marking those dynamic imports with
`/* @vite-ignore */` stops the bundler from following them into the worker,
which is correct because they never run at request time for a static site.
The `satteri-wasi-stub.mjs` alias in `vite.config.ts` covers the same fallback
for the client build.
