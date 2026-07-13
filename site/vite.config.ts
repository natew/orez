import { fileURLToPath, URL } from 'node:url'

import { one } from 'one/vite'
import { defineConfig } from 'vite'

const satteriWasiStub = fileURLToPath(new URL('./satteri-wasi-stub.mjs', import.meta.url))

export default defineConfig({
  resolve: {
    // satteri's browser.js re-exports its wasm-wasi binding, which the workerd
    // build tries to resolve. the MDX parse only runs at build time via the
    // native binding, so stub the wasm fallback everywhere it is dead code.
    alias: [{ find: '@bruits/satteri-wasm32-wasi', replacement: satteriWasiStub }],
  },

  // the MDX pipeline (satteri, Rust) runs at build time only; keep it external
  // to the SSR bundle so its native binding is never bundled.
  ssr: {
    noExternal: true,
    external: ['@vxrn/mdx-rust', 'satteri', 'satteri-expressive-code'],
  },

  plugins: [
    one({
      web: {
        defaultRenderMode: 'ssg',
        deploy: { target: 'cloudflare' },
      },
      build: {
        server: { unified: true },
      },
    }),
  ],
})
