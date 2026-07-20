import type { AuthData, Can, GenericModels, GetZeroMutators, MutatorContext } from '../types';
export type ValidateMutationFn = (args: {
    authData: AuthData | null;
    mutatorName: string;
    tableName: string;
    args: unknown;
}) => void | Promise<void>;
export type { ValidateMutationFn as CreateMutatorsValidateFn };
export declare function createMutators<Models extends GenericModels>({ environment, authData, createServerActions, enqueueTask, can, models, validateMutation, mutationValidators, resolveAuthData, }: {
    environment: 'server' | 'client';
    authData: AuthData | null;
    can: Can;
    models: Models;
    enqueueTask?: NonNullable<MutatorContext['server']>['enqueueTask'];
    createServerActions?: () => Record<string, any>;
    validateMutation?: ValidateMutationFn;
    /** valibot schemas keyed by model.mutationName, auto-validates args before running */
    mutationValidators?: Record<string, Record<string, any>>;
    resolveAuthData?: () => AuthData | null;
}): GetZeroMutators<Models>;
//# sourceMappingURL=createMutators.d.ts.map