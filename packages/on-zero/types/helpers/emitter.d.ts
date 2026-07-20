type EmitterOptions<T> = {
    name: string;
    silent?: boolean;
    comparator?: (a: T, b: T) => boolean;
};
type CreateEmitterOptions<T> = Omit<EmitterOptions<T>, 'name'>;
export declare class Emitter<const T> {
    private listeners;
    value: T;
    options?: EmitterOptions<T>;
    constructor(value: T, options?: EmitterOptions<T>);
    listen: (listener: (value: T) => void) => (() => void);
    emit: (next: T) => void;
}
export declare function createEmitter<T>(name: string, defaultValue: T, options?: CreateEmitterOptions<T>): Emitter<T>;
type EmitterValue<E extends Emitter<any>> = E extends Emitter<infer Value> ? Value : never;
export declare function useEmitterValue<E extends Emitter<any>>(emitter: E, options?: {
    disable?: boolean;
}): EmitterValue<E>;
export {};
//# sourceMappingURL=emitter.d.ts.map