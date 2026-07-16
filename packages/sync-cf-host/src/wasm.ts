import wasmModule from 'orez-sync-cf-host/wasm-module'

import { initSync } from './generated/sync_wasm.js'

if (!(wasmModule instanceof WebAssembly.Module)) {
  throw new TypeError(
    '.wasm import did not resolve to WebAssembly.Module; Bun consumers must register orez-sync-cf-host/bun-wasm-loader via bunfig preload, and Vite/Node consumers must use orez-sync-cf-host/vite-wasm-loader; see the package README runtime matrix'
  )
}

initSync({ module: wasmModule })

export {
  engine_apply_snapshot_changes,
  engine_apply_snapshot_page,
  engine_apply_upstream,
  engine_assemble_push_response,
  engine_begin_snapshot_generation,
  engine_compile_query,
  engine_finalize,
  engine_finalize_snapshot_generation,
  engine_handle_pull,
  engine_handle_query_pull,
  engine_init_query_schema,
  engine_init_schema,
  engine_invalidate,
  engine_memory_bytes,
  engine_preflight,
  engine_prune,
  engine_push_validate,
  engine_read_snapshot_progress,
  engine_record_app_error,
  engine_state,
  engine_version,
} from './generated/sync_wasm.js'
