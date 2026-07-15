// node 25 defines globalThis.localStorage by default, but without a
// --localstorage-file path its methods are undefined. replicache (inside the
// zero client) feature-detects localStorage by presence and then calls
// getItem, which throws and permanently stalls query completion. remove the
// broken stub so detection takes the no-localstorage path on every node.
const maybeLocalStorage = (globalThis as { localStorage?: unknown }).localStorage
if (
  maybeLocalStorage &&
  typeof (maybeLocalStorage as { getItem?: unknown }).getItem !== 'function'
) {
  delete (globalThis as { localStorage?: unknown }).localStorage
}
