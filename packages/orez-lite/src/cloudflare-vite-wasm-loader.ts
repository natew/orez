import { orezSyncCfHostWasm as syncCfHostWasm } from 'orez-sync-cf-host/vite-wasm-loader'

/** load Orez Lite and its sync engine into Vite's SSR bundle. */
export function orezSyncCfHostWasm() {
  return syncCfHostWasm({ noExternal: ['orez-lite'] })
}
