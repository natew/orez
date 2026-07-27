import * as generated from './generated/sync_wasm.js'
import { withWasm } from './wasm.js'

function platformOperation(name: string): (...args: unknown[]) => unknown {
  const operation = Reflect.get(generated, name)
  if (typeof operation !== 'function') {
    throw new TypeError(`sync WASM platform probe '${name}' is unavailable`)
  }
  return withWasm((...args: unknown[]) => Reflect.apply(operation, undefined, args))
}

export const init_probe_schema = platformOperation('init_probe_schema')
export const pull_snapshot = platformOperation('pull_snapshot')
export const push_finalize = platformOperation('push_finalize')
export const push_preflight = platformOperation('push_preflight')
export const rust_panic_after_writes = platformOperation('rust_panic_after_writes')
export const value_round_trip = platformOperation('value_round_trip')
