export declare function mapObject<T extends Record<string, any>, R>(obj: T, fn: <K extends keyof T>(value: T[K], key: K) => R): {
    [K in keyof T]: R;
};
//# sourceMappingURL=mapObject.d.ts.map