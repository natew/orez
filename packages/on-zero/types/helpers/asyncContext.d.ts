interface AsyncContext<T> {
    get(): T | undefined;
    run<R>(value: T, fn: () => R | Promise<R>): Promise<R>;
}
interface NodeAsyncLocalStorage<T> {
    getStore(): T | undefined;
    run<R>(store: T, callback: () => R): R;
}
interface AsyncLocalStorageConstructor {
    new <T>(): NodeAsyncLocalStorage<T>;
}
export declare function setupAsyncLocalStorage(AsyncLocalStorage: AsyncLocalStorageConstructor | null): void;
export declare function createAsyncContext<T>(): AsyncContext<T>;
export {};
//# sourceMappingURL=asyncContext.d.ts.map