import wasmModule from 'orez-sync-cf-host/wasm-module.wasm'

import * as generated from './generated/sync_wasm.js'

let initialized = false

function ensureWasm(): void {
  if (initialized) return
  if (!(wasmModule instanceof WebAssembly.Module)) {
    throw new TypeError(
      '.wasm import did not resolve to WebAssembly.Module; see the orez-sync-cf-host README Query compiler runtimes matrix'
    )
  }
  generated.initSync({ module: wasmModule })
  initialized = true
}

export function withWasm<Args extends unknown[], Result>(
  operation: (...args: Args) => Result
): (...args: Args) => Result {
  return (...args) => {
    ensureWasm()
    return operation(...args)
  }
}

export const engine_apply_snapshot_changes = withWasm(
  generated.engine_apply_snapshot_changes
)
export const engine_apply_snapshot_page = withWasm(generated.engine_apply_snapshot_page)
export const engine_apply_upstream = withWasm(generated.engine_apply_upstream)
export const engine_assemble_push_response = withWasm(
  generated.engine_assemble_push_response
)
export const engine_authorize_realtime_subscription = withWasm(
  generated.engine_authorize_realtime_subscription
)
export const engine_begin_snapshot_generation = withWasm(
  generated.engine_begin_snapshot_generation
)
export const engine_compile_query = withWasm(generated.engine_compile_query)
export const engine_finalize = withWasm(generated.engine_finalize)
export const engine_finalize_snapshot_generation = withWasm(
  generated.engine_finalize_snapshot_generation
)
export const engine_handle_query_pull = withWasm(generated.engine_handle_query_pull)
export const engine_init_query_schema = withWasm(generated.engine_init_query_schema)
export const engine_init_schema = withWasm(generated.engine_init_schema)
export const engine_invalidate = withWasm(generated.engine_invalidate)
export const engine_memory_bytes = withWasm(generated.engine_memory_bytes)
export const engine_preflight = withWasm(generated.engine_preflight)
export const engine_prune = withWasm(generated.engine_prune)
export const engine_push_validate = withWasm(generated.engine_push_validate)
export const engine_read_snapshot_progress = withWasm(
  generated.engine_read_snapshot_progress
)
export const engine_record_app_error = withWasm(generated.engine_record_app_error)
export const engine_state = withWasm(generated.engine_state)
export const engine_version = withWasm(generated.engine_version)
