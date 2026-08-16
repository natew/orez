import { orezSyncCfHostWasm as syncCfHostWasm } from 'orez-sync-cf-host/vite-wasm-loader'

type OrezSyncCfHostWasmOptions = {
  runtime?: 'node' | 'workerd'
}

/** load Orez Lite and its sync engine into Vite's SSR bundle. */
export function orezSyncCfHostWasm(options: OrezSyncCfHostWasmOptions = {}) {
  return syncCfHostWasm({ noExternal: ['orez-lite'], runtime: options.runtime })
}
