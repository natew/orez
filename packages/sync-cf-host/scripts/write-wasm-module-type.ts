const declaration = new URL('../src/generated/sync_wasm_bg.wasm.d.ts', import.meta.url)

await Bun.write(
  declaration,
  `declare const module: WebAssembly.Module
export default module
`
)
