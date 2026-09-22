// Ensures only one instance ever exists, but replaces it every time it's called

const Controllers = new Map<symbol, AbortController>()

export function ensureOne<T>(key: string, factory: (signal: AbortSignal) => T): T {
  const symbolKey = Symbol.for(key)
  const g = globalThis as Record<symbol, unknown>

  if (g[symbolKey]) {
    Controllers.get(symbolKey)?.abort()
  }

  const controller = new AbortController()
  Controllers.set(symbolKey, controller)
  g[symbolKey] = factory(controller.signal)

  return g[symbolKey] as T
}
