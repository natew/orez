import { readFile } from 'node:fs/promises'

import type { Plugin } from 'vite'

const wasmModuleID = 'orez-sync-cf-host/wasm-module.wasm'
const resolvedWasmModuleID = `\0${wasmModuleID}`

type OrezSyncCfHostWasmOptions = {
  noExternal?: string[]
}

/** load the sync engine for Vite's Node serve, SSR, and production build paths. */
export function orezSyncCfHostWasm(options: OrezSyncCfHostWasmOptions = {}): Plugin[] {
  const noExternal = ['orez-sync-cf-host', ...(options.noExternal ?? [])]
  return [
    {
      name: 'orez-sync-cf-host-wasm',
      enforce: 'pre',
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
    },
    {
      name: 'orez-sync-cf-host-ssr-bundle',
      enforce: 'post',
      config() {
        return { ssr: { noExternal } }
      },
    },
  ]
}
