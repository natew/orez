import { IS_SERVER_RUNTIME } from './platform'

interface AsyncContext<T> {
  get(): T | undefined
  run<R>(value: T, fn: () => R | Promise<R>): Promise<R>
}

interface NodeAsyncLocalStorage<T> {
  getStore(): T | undefined
  run<R>(store: T, callback: () => R): R
}

interface AsyncLocalStorageConstructor {
  new <T>(): NodeAsyncLocalStorage<T>
}

let nodeAsyncLocalStorageCache: AsyncLocalStorageConstructor | null = null
let configuredAsyncLocalStorage: AsyncLocalStorageConstructor | null = null

// hide from vite/esbuild static analysis to avoid browser compat warning
const nodeModuleId = ['node', 'async_hooks'].join(':')

// install an AsyncLocalStorage for a host that runs the server bindings on a
// runtime this build is not compiled as: Contrast's web preview, for instance,
// runs a tenant's server in a browser worker, where `node:async_hooks` cannot be
// imported but the host can supply an equivalent. the caller is responsible for
// the guarantee node gives for free — at most one region per context open at a
// time — because a userland implementation has a single slot and cannot follow a
// native `await`. a host without that guarantee must install nothing.
export function setupAsyncLocalStorage(
  AsyncLocalStorage: AsyncLocalStorageConstructor | null
): void {
  configuredAsyncLocalStorage = AsyncLocalStorage
}

async function getNodeAsyncLocalStorage(): Promise<AsyncLocalStorageConstructor | null> {
  if (!nodeAsyncLocalStorageCache) {
    try {
      const module = await import(/* @vite-ignore */ nodeModuleId)
      nodeAsyncLocalStorageCache =
        module.AsyncLocalStorage as AsyncLocalStorageConstructor
    } catch {
      return null
    }
  }
  return nodeAsyncLocalStorageCache
}

// which storage backs a context is decided at FIRST USE, never here. these
// contexts are created while on-zero's own modules load, so a host can only call
// setupAsyncLocalStorage() afterwards; deciding at construction time made that
// injection point unreachable and silently dropped every server mutator's
// authData to null on runtimes that report themselves as non-server.
export function createAsyncContext<T>(): AsyncContext<T> {
  let storage: NodeAsyncLocalStorage<T> | null = null
  let nodeStorageReady: Promise<void> | null = null

  // returns a promise only while the node storage still has to be imported
  const resolveStorage = (): Promise<void> | null => {
    if (storage) return null
    const configured = configuredAsyncLocalStorage
    if (configured) {
      storage = new configured<T>()
      return null
    }
    if (!IS_SERVER_RUNTIME) return null
    nodeStorageReady ??= getNodeAsyncLocalStorage().then((AsyncLocalStorage) => {
      if (AsyncLocalStorage && !storage) storage = new AsyncLocalStorage<T>()
    })
    return nodeStorageReady
  }

  return {
    get(): T | undefined {
      resolveStorage()
      return storage?.getStore()
    },

    async run<R>(value: T, fn: () => R | Promise<R>): Promise<R> {
      const pending = resolveStorage()
      if (pending) await pending
      if (storage) return storage.run(value, fn)
      if (IS_SERVER_RUNTIME) {
        throw new Error(`AsyncContext storage unavailable in server runtime`)
      }
      // browsers and the react-native JS runtime have no AsyncLocalStorage of
      // their own, and an ambient context cannot be emulated soundly on them: a
      // module-level "current context" is visible to every other task that runs
      // while a mutator awaits, and patching Promise.prototype does not follow a
      // native `await` at all. so there is no ambient context here unless a host
      // installed one. createMutators still passes each mutator its own context,
      // and transactional reads go through that transaction (`tx.run(zql...)`).
      return await fn()
    },
  }
}
