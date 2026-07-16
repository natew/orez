// node 25 defines globalThis.localStorage by default, but without a
// --localstorage-file path its methods are undefined. replicache (inside the
// zero client) feature-detects localStorage by presence and then calls
// getItem, which throws and permanently stalls query completion. node 24 has
// no localStorage at all. install a functional in-memory stub in both cases
// install the same in-memory stub used by downstream test setups so Zero sees a
// working Storage on every node.
if (typeof globalThis.localStorage?.getItem !== 'function') {
  const store = new Map<string, string>()
  Object.defineProperty(globalThis, 'localStorage', {
    configurable: true,
    value: {
      get length() {
        return store.size
      },
      clear() {
        store.clear()
      },
      getItem(key: string) {
        return store.get(String(key)) ?? null
      },
      key(index: number) {
        return [...store.keys()][index] ?? null
      },
      removeItem(key: string) {
        store.delete(String(key))
      },
      setItem(key: string, value: string) {
        store.set(String(key), String(value))
      },
    },
  })
}
