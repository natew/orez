/**
 * Helper to store a value that's not duplicated within the global context Uses
 * a symbol stored on `globalThis` to achieve it.
 *
 * There's two main uses for this (for us, so far):
 *
 *   - in dev mode, HMR re-runs files, but you may want to preserve globals
 *
 *   - in published node modules, sometimes Vite or other bundlers can cause
 *     duplicate dependencies either through bad setup or heuristics, they may
 *     compile a module but then another code path doesn't compile it, so you
 *     have duplicate modules. in this case it can be good to just use a
 *     globalValue and warn the end-user. because you can't account for ever
 *     possible weird configuration, and often fixing that config can be a
 *     massive pain in the ass, it's better to retain functionality while
 *     warning them so they can fix it at their own pace. this often happens
 *     with WeakMaps.
 *
 */
export declare function globalValue<T>(key: string, factory: () => T, opts?: {
    warnMessage?: string;
}): T;
//# sourceMappingURL=globalValue.d.ts.map