import { IS_SERVER_RUNTIME } from './platform.js'

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

let configuredAsyncLocalStorage: AsyncLocalStorageConstructor | null = null

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

// which storage backs a context is decided at FIRST USE, never here. these
// contexts are created while limesync's own modules load, so a host can only call
// setupAsyncLocalStorage() afterwards; deciding at construction time made that
// injection point unreachable and silently dropped every server mutator's
// authData to null on runtimes that report themselves as non-server.
export function createAsyncContext<T>(): AsyncContext<T> {
  let storage: NodeAsyncLocalStorage<T> | null = null

  const resolveStorage = () => {
    if (storage) return storage
    const AsyncLocalStorage =
      configuredAsyncLocalStorage ??
      (IS_SERVER_RUNTIME
        ? process.getBuiltinModule?.('node:async_hooks')?.AsyncLocalStorage
        : undefined)
    if (AsyncLocalStorage) storage = new AsyncLocalStorage<T>()
    return storage
  }

  return {
    get(): T | undefined {
      return resolveStorage()?.getStore()
    },

    async run<R>(value: T, fn: () => R | Promise<R>): Promise<R> {
      const storage = resolveStorage()
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
