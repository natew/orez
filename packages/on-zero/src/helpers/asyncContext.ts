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

export function createAsyncContext<T>(): AsyncContext<T> {
  if (IS_SERVER_RUNTIME) {
    let storage: NodeAsyncLocalStorage<T> | null = null

    const storageReady = Promise.resolve(configuredAsyncLocalStorage)
      .then((AsyncLocalStorage) => AsyncLocalStorage || getNodeAsyncLocalStorage())
      .then((AsyncLocalStorage) => {
        if (AsyncLocalStorage && !storage) {
          storage = new AsyncLocalStorage<T>()
        }
      })

    return {
      get(): T | undefined {
        if (!storage) {
          throw new Error(`AsyncContext storage not initialized`)
        }
        return storage.getStore()
      },

      async run<R>(value: T, fn: () => R | Promise<R>): Promise<R> {
        if (!storage) {
          await storageReady
        }
        if (!storage) {
          throw new Error(`AsyncContext storage unavailable in server runtime`)
        }
        return storage.run(value, fn)
      },
    }
  }

  return createJsRuntimeAsyncContext<T>()
}

// browsers and the react-native JS runtime have no AsyncLocalStorage, and an
// ambient mutator context cannot be emulated soundly on them. a module-level
// "current context" is visible to every other task that runs while a mutator
// awaits, and patching Promise.prototype does not follow a native `await` at
// all. the emulation that used to live here handed a mutator a *different*
// mutator's transaction whenever two overlapped, and left Promise.prototype
// patched for the life of the page once two runs interleaved.
//
// so on these runtimes there is no ambient mutator context. createMutators
// already passes each mutator its own context, and transactional reads go
// through that transaction (`tx.run(zql...)`) rather than through ambient
// state.
function createJsRuntimeAsyncContext<T>(): AsyncContext<T> {
  return {
    get(): T | undefined {
      return undefined
    },
    async run<R>(_value: T, fn: () => R | Promise<R>): Promise<R> {
      return await fn()
    },
  }
}
