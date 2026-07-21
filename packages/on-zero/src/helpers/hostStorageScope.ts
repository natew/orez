// an embedding host that runs many apps on one origin (a preview shell, a
// multi-app runner) injects a storage scope before the app's module graph
// evaluates; on-zero folds it into every storage key so co-located apps never
// share local zero state. same-origin iframes read the top window so the host
// sets it in exactly one place. standalone apps have no scope.
type ScopedGlobal = { __on_zero_storage_scope?: string }

export function readHostStorageScope(): string | undefined {
  const runtime = globalThis as ScopedGlobal
  if (typeof window !== 'undefined') {
    try {
      const top = (window.top ?? window) as unknown as ScopedGlobal
      if (top.__on_zero_storage_scope) return top.__on_zero_storage_scope
    } catch {
      // cross-origin top window: not an embedding host
    }
  }
  return runtime.__on_zero_storage_scope
}
