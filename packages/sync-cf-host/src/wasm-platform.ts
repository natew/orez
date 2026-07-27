import * as generated from './generated/sync_wasm.js'
import { withWasm } from './wasm.js'

export const init_probe_schema = withWasm(generated.init_probe_schema)
export const pull_snapshot = withWasm(generated.pull_snapshot)
export const push_finalize = withWasm(generated.push_finalize)
export const push_preflight = withWasm(generated.push_preflight)
export const rust_panic_after_writes = withWasm(generated.rust_panic_after_writes)
export const value_round_trip = withWasm(generated.value_round_trip)
