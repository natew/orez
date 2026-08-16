import { readFile } from 'node:fs/promises'

import type { Plugin } from 'vite'

const wasmModuleID = 'orez-sync-cf-host/wasm-module.wasm'
const resolvedWasmModuleID = `\0${wasmModuleID}`

type OrezSyncCfHostWasmOptions = {
  noExternal?: string[]
  runtime?: 'node' | 'workerd'
}

/** load the sync engine into a Vite SSR graph for Node or Workerd. */
export function orezSyncCfHostWasm(options: OrezSyncCfHostWasmOptions = {}): Plugin[] {
  const noExternal = ['orez-sync-cf-host', ...(options.noExternal ?? [])]
  let command: 'build' | 'serve' = 'serve'
  return [
    {
      name: 'orez-sync-cf-host-wasm',
      enforce: 'pre',
      configResolved(config) {
        command = config.command
      },
      resolveId(source) {
        if (source !== wasmModuleID) return null
        return options.runtime === 'workerd' && command === 'build'
          ? { id: wasmModuleID, external: true }
          : resolvedWasmModuleID
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
