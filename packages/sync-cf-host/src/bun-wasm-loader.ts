import { Buffer } from 'node:buffer'
import { fileURLToPath } from 'node:url'

import { file, plugin } from 'bun'

const wasmModuleID = 'orez-sync-cf-host/wasm-module.wasm'
const wasmModulePath = fileURLToPath(import.meta.resolve(wasmModuleID))

plugin({
  name: 'orez-sync-cf-host-wasm',
  setup(build) {
    build.onResolve({ filter: /^orez-sync-cf-host\/wasm-module\.wasm$/ }, () => ({
      path: wasmModulePath,
    }))
    build.onLoad({ filter: /sync_wasm_bg\.wasm$/ }, async ({ path }) => {
      const bytes = Buffer.from(await file(path).arrayBuffer()).toString('base64')
      return {
        contents: `export default new WebAssembly.Module(Uint8Array.fromBase64('${bytes}'))`,
        loader: 'js',
      }
    })
  },
})
