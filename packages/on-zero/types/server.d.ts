import type { handleQueryRequest as zeroHandleQueryRequest } from '@rocicorp/zero/server';
import type { AdminRoleMode, AuthData, GenericModels, MutatorContext, QueryBuilder, Transaction } from './types';
import type { AnyQueryRegistry, HumanReadable, Query, Schema as ZeroSchema, ServerTransaction as RocicorpServerTransaction } from '@rocicorp/zero';
export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | readonly JsonValue[] | {
    readonly [key: string]: JsonValue;
};
export type NormalizedClaims = {
    readonly userID: string;
    readonly [claim: string]: JsonValue;
};
export type TransactionQueryFormat = {
    readonly relationships: Readonly<Record<string, TransactionQueryFormat>>;
    readonly singular: boolean;
};
export type SqlStatementMetadata = {
    readonly table: string;
    readonly publicTable: string;
    readonly kind: 'delete' | 'insert' | 'update' | 'upsert';
};
export interface ZeroServerApplicationTransaction {
    exec(sql: string, params?: readonly unknown[], metadata?: SqlStatementMetadata): Promise<{
        readonly changes: number;
    }>;
    query<Row extends Record<string, unknown> = Record<string, unknown>>(sql: string, params?: readonly unknown[]): Promise<readonly Row[]>;
    queryAst<Result = unknown>(ast: JsonValue, format: TransactionQueryFormat, queryName?: string): Promise<Result>;
}
export type ZeroServerTransaction<Schema extends ZeroSchema> = RocicorpServerTransaction<Schema, ZeroServerApplicationTransaction>;
export type ZeroServerMutationContext = {
    readonly claims: NormalizedClaims;
    defer(effect: () => void | Promise<void>, options?: {
        readonly barrier?: boolean;
    }): void;
};
export type ZeroServerRegisteredMutator<Schema extends ZeroSchema = ZeroSchema> = (input: {
    readonly tx: ZeroServerTransaction<Schema>;
    readonly args: JsonValue;
    readonly ctx: ZeroServerMutationContext;
}) => void | Promise<void>;
export type ZeroServerMutatorRegistry<Schema extends ZeroSchema = ZeroSchema> = Readonly<Record<string, ZeroServerRegisteredMutator<Schema>>>;
export interface ZeroServerExecutor<Schema extends ZeroSchema> {
    execute(name: string, args: JsonValue, claims: NormalizedClaims): Promise<void>;
    transaction<Value>(claims: NormalizedClaims, work: (tx: ZeroServerTransaction<Schema>) => Value | Promise<Value>): Promise<Value>;
    query<Result>(claims: NormalizedClaims, work: (tx: ZeroServerTransaction<Schema>) => Result | Promise<Result>): Promise<Result>;
}
export type ValidateQueryArgs = {
    authData: AuthData | null;
    queryName: string;
    params: unknown;
};
export type ValidateMutationArgs = {
    authData: AuthData | null;
    mutatorName: string;
    tableName: string;
    args: unknown;
};
export type ValidateQueryFn = (args: ValidateQueryArgs) => void;
export type ValidateMutationFn = (args: ValidateMutationArgs) => void | Promise<void>;
type MutateAuthData = Pick<AuthData, 'id'> & Partial<AuthData>;
export type MutateOptions = {
    authData?: MutateAuthData;
};
export type ServerMutate<Models extends GenericModels> = {
    [Key in keyof Models]: {
        [K in keyof Models[Key]['mutate']]: Models[Key]['mutate'][K] extends (ctx: MutatorContext, arg: infer Arg) => any ? (arg: Arg, options?: MutateOptions) => Promise<void> : (options?: MutateOptions) => Promise<void>;
    };
};
export type CreateZeroServerBindingsOptions<Schema extends ZeroSchema, Models extends GenericModels, ServerActions extends Record<string, unknown>> = {
    schema: Schema;
    models: Models;
    createServerActions: () => ServerActions;
    queries?: AnyQueryRegistry;
    mutations?: Record<string, Record<string, unknown>>;
    validateQuery?: ValidateQueryFn;
    validateMutation?: ValidateMutationFn;
    defaultAllowAdminRole?: AdminRoleMode;
    mapClaims?: (claims: NormalizedClaims) => AuthData | null;
};
export type ZeroServerBindings<Schema extends ZeroSchema, Models extends GenericModels> = {
    mutators: ZeroServerMutatorRegistry<Schema>;
    resolveQuery(name: string, args: readonly JsonValue[], claims: NormalizedClaims): Promise<JsonValue>;
    transformQueryRequest(options: {
        authData: AuthData | null;
        request: Request;
    }): ReturnType<typeof zeroHandleQueryRequest>;
    server(executor: ZeroServerExecutor<Schema>): {
        mutate: ServerMutate<Models>;
        transaction<Value>(claims: NormalizedClaims, work: (tx: Transaction) => Value | Promise<Value>): Promise<Value>;
        query<Result>(claims: NormalizedClaims, work: (q: QueryBuilder) => Query<any, Schema, Result>): Promise<HumanReadable<Result>>;
    };
};
export declare function createZeroServerBindings<Schema extends ZeroSchema, Models extends GenericModels, ServerActions extends Record<string, unknown>>(options: CreateZeroServerBindingsOptions<Schema, Models, ServerActions>): ZeroServerBindings<Schema, Models>;
export declare function authDataToClaims(authData: AuthData | null | undefined, anonymousUserID?: string): NormalizedClaims;
export {};
//# sourceMappingURL=server.d.ts.map