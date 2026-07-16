import { Buffer } from 'node:buffer'

import { file, plugin } from 'bun'

plugin({
  name: 'orez-sync-cf-host-wasm',
  setup(build) {
    build.onLoad({ filter: /sync_wasm_bg\.wasm$/ }, async ({ path }) => {
      const bytes = Buffer.from(await file(path).arrayBuffer()).toString('base64')
      return {
        contents: `export default new WebAssembly.Module(Uint8Array.fromBase64('${bytes}'))`,
        loader: 'js',
      }
    })
  },
})
