interface AsyncContext<T> {
    get(): T | undefined;
    run<R>(value: T, fn: () => R | Promise<R>): Promise<R>;
}
export declare function setupAsyncLocalStorage(_AsyncLocalStorage: unknown): void;
export declare function createAsyncContext<T>(): AsyncContext<T>;
export {};
//# sourceMappingURL=asyncContext.native.d.ts.map