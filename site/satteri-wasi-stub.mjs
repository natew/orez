// satteri's browser.js re-exports its wasm-wasi binding (@bruits/satteri-wasm32-wasi).
// the MDX parse runs at build time through the native darwin binding, so the
// wasm fallback is dead code in the client and worker bundles. this empty stub
// satisfies the resolver without shipping the wasm module.
export {}
