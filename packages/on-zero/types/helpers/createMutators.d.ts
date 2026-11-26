import type { AuthData, Can, GenericModels, GetZeroMutators, MutatorContext, Transaction } from '../types';
import type { AggregateSet } from 'orez-lite/aggregate';
export type ValidateMutationFn = (args: {
    authData: AuthData | null;
    mutatorName: string;
    tableName: string;
    args: unknown;
}) => void | Promise<void>;
export type { ValidateMutationFn as CreateMutatorsValidateFn };
export declare function createMutators<Models extends GenericModels>({ environment, authData, createServerActions, enqueueTask, enqueueAction, bindCan, models, validateMutation, mutationValidators, resolveAuthData, aggregates, }: {
    environment: 'server' | 'client';
    authData: AuthData | null;
    bindCan: (tx: Transaction, authData: AuthData | null) => Can;
    models: Models;
    enqueueTask?: NonNullable<MutatorContext['server']>['enqueueTask'];
    enqueueAction?: NonNullable<MutatorContext['server']>['enqueueAction'];
    createServerActions?: () => Record<string, any>;
    validateMutation?: ValidateMutationFn;
    /** valibot schemas keyed by model.mutationName, auto-validates args before running */
    mutationValidators?: Record<string, Record<string, any>>;
    resolveAuthData?: () => AuthData | null;
    aggregates?: AggregateSet;
}): GetZeroMutators<Models>;
//# sourceMappingURL=createMutators.d.ts.map