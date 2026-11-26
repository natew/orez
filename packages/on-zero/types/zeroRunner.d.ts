import type { HumanReadable, Query, RunOptions, Schema as ZeroSchema } from '@rocicorp/zero';
export type { RunOptions };
export type ZeroRunner = <TReturn>(query: Query<any, ZeroSchema, TReturn>, options?: RunOptions) => Promise<HumanReadable<TReturn>>;
export declare function setRunner(r: ZeroRunner | null): void;
export declare function getRunner(instance?: {
    runner: ZeroRunner | null;
}): ZeroRunner;
export declare function getAmbientRunner(instance?: {
    runner: ZeroRunner | null;
}): ZeroRunner;
//# sourceMappingURL=zeroRunner.d.ts.map