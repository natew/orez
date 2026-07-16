import { Buffer } from 'node:buffer'
import { readFileSync } from 'node:fs'
import * as nodeModule from 'node:module'
import { fileURLToPath } from 'node:url'

const wasmModuleID = 'orez-sync-cf-host/wasm-module.wasm'
const wasmModulePath = fileURLToPath(import.meta.resolve(wasmModuleID))
const wasmBase64 = readFileSync(wasmModulePath).toString('base64')
const wasmModuleSource = `import { Buffer } from 'node:buffer'
export default new WebAssembly.Module(Buffer.from('${wasmBase64}', 'base64'))
`
const virtualWasmModuleURL = `data:text/javascript;base64,${Buffer.from(wasmModuleSource).toString('base64')}`

type NextResolve = (specifier: string, context: unknown) => unknown
type RegisterHooks = (hooks: {
  resolve(specifier: string, context: unknown, nextResolve: NextResolve): unknown
}) => unknown

const registerHooks = Reflect.get(nodeModule, 'registerHooks') as
  | RegisterHooks
  | undefined
if (!registerHooks) {
  throw new Error('orez-sync-cf-host/node-wasm-loader requires Node 22.15 or newer')
}

registerHooks({
  resolve(specifier, context, nextResolve) {
    return specifier === wasmModuleID
      ? { shortCircuit: true, url: virtualWasmModuleURL }
      : nextResolve(specifier, context)
  },
})
