import { readFile } from 'node:fs/promises'

import type { Plugin } from 'vite'

const wasmModuleID = 'orez-sync-cf-host/wasm-module'
const resolvedWasmModuleID = `\0${wasmModuleID}`

/** Load the sync engine for Vite's Node serve, SSR, and production build paths. */
export function orezSyncCfHostWasm(): Plugin {
  return {
    name: 'orez-sync-cf-host-wasm',
    enforce: 'pre',
    config() {
      return { ssr: { noExternal: ['orez-sync-cf-host'] } }
    },
    resolveId(source) {
      return source === wasmModuleID ? resolvedWasmModuleID : null
    },
    async load(id) {
      if (id !== resolvedWasmModuleID) return null
      const bytes = await readFile(
        new URL('./generated/sync_wasm_bg.wasm', import.meta.url)
      )
      return `import { Buffer } from 'node:buffer'
export default new WebAssembly.Module(Buffer.from('${bytes.toString('base64')}', 'base64'))
`
    },
  }
}
