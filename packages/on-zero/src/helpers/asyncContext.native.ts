interface AsyncContext<T> {
  get(): T | undefined
  run<R>(value: T, fn: () => R | Promise<R>): Promise<R>
}

export function setupAsyncLocalStorage(_AsyncLocalStorage: unknown): void {}

// react native has no node:async_hooks, and an ambient mutator context cannot
// be emulated soundly without it: a module-level "current context" is visible
// to every other task that runs while a mutator awaits, and patching
// Promise.prototype does not follow a native `await` at all. so there is no
// ambient mutator context here — createMutators passes each mutator its own
// context, and transactional reads go through that transaction
// (`tx.run(zql...)`). see asyncContext.ts for the same reasoning on the web.
export function createAsyncContext<T>(): AsyncContext<T> {
  return {
    get(): T | undefined {
      return undefined
    },
    async run<R>(_value: T, fn: () => R | Promise<R>): Promise<R> {
      return await fn()
    },
  }
}
