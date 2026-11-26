import type { ZeroRunner } from './zeroRunner';
import type { AnyQueryRegistry } from '@rocicorp/zero';
export type ZeroClientRuntime = {
    zero: unknown | null;
    readyWaiters: Set<(zero: unknown) => void>;
    runner: ZeroRunner | null;
};
export type ZeroClientInstance = {
    name: string;
    customQueries: AnyQueryRegistry;
    runtime: ZeroClientRuntime;
    runner: ZeroRunner | null;
};
export declare function releaseClientInstances(names: readonly string[]): void;
export declare function registerClientInstance({ name, namespaces, customQueries, queryNames, }: {
    name: string;
    namespaces: string[];
    customQueries: AnyQueryRegistry;
    queryNames?: string[];
}): ZeroClientInstance;
export declare function getInstanceForNamespace(namespace: string): ZeroClientInstance | undefined;
export declare function getInstanceForQueryFn(fn: Function): ZeroClientInstance | undefined;
//# sourceMappingURL=instanceRegistry.d.ts.map