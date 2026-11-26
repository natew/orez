export type MutationLike = {
    client: Promise<unknown>;
    server?: Promise<unknown>;
};
export type BackgroundMutationOptions = {
    coalesceKey?: string;
    settle?: 'client' | 'server';
    timeoutMs?: number;
};
export type BackgroundMutationDrainResult = {
    errors: unknown[];
    pendingBackgroundMutations: number;
    pendingServerAcknowledgements: number;
    timedOut: boolean;
};
export type MutationPhase = 'client' | 'server';
export declare class StaleGenerationError extends Error {
    readonly label: string;
    constructor(label: string);
}
export declare class MutationTimeoutError extends Error {
    readonly label: string;
    readonly phase: MutationPhase;
    readonly timeoutMs: number;
    constructor(label: string, phase: MutationPhase, timeoutMs: number);
}
export declare class MutationResultError extends Error {
    readonly label: string;
    readonly phase: MutationPhase;
    readonly result: unknown;
    constructor(label: string, phase: MutationPhase, result: unknown, message: string);
}
export declare function isStaleGenerationError(error: unknown): error is StaleGenerationError;
export declare function mutationErrorMessage(result: unknown): string | null;
export type MutationLifecycle = {
    activate: () => void;
    fence: () => void;
    drainBackgroundMutations: (options?: {
        timeoutMs?: number;
    }) => Promise<BackgroundMutationDrainResult>;
    enqueueBackgroundMutation: (label: string, create: () => unknown, options?: BackgroundMutationOptions) => Promise<void>;
    awaitMutationClient: (mutation: MutationLike, label: string, timeoutMs?: number) => Promise<unknown>;
    awaitMutationServer: (mutation: MutationLike, label: string, timeoutMs?: number) => Promise<unknown>;
    assertWritable: () => void;
    claimMutation: (result: unknown) => void;
    pinnedGeneration: () => number | null;
    isCurrentGeneration: (generation: number) => boolean;
};
export declare function createMutationLifecycle(options: {
    ackTimeoutRecoveryThreshold: number;
    recoverFromAckTimeout: (input: {
        label: string;
        timeoutMs: number;
        consecutiveTimeouts: number;
    }) => void;
}): MutationLifecycle;
export type CombinedMutationLifecycle = Pick<MutationLifecycle, 'awaitMutationClient' | 'awaitMutationServer' | 'drainBackgroundMutations' | 'enqueueBackgroundMutation'>;
export declare function combineMutationLifecycles(lifecycles: readonly [MutationLifecycle, ...MutationLifecycle[]]): CombinedMutationLifecycle;
//# sourceMappingURL=mutationLifecycle.d.ts.map