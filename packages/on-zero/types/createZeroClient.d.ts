import { Zero as ZeroClient } from '@rocicorp/zero';
import { type Context, type ReactNode } from 'react';
import { type QueryControlMode, type UseQueryHook } from './createUseQuery';
import { type Emitter } from './helpers/emitter';
import { type RecoveryGuardStorage, type ScheduleReloadContext, type ZeroLogPattern } from './helpers/recoverZeroClient';
import { resolveQuery, type PlainQueryFn } from './resolveQuery';
import type { AuthData, GenericModels, GetZeroMutators, ZeroEventsEmitter } from './types';
import type { AnyQueryRegistry, Query, Row, ZeroOptions, Schema as ZeroSchema } from '@rocicorp/zero';
import type { AggregateSet } from 'orez-lite/aggregate';
type PreloadOptions = {
    ttl?: 'always' | 'never' | number | undefined;
};
export type GroupedQueries = Record<string, Record<string, (...args: any[]) => any>>;
export type PermissionStrategy = 'optimistic' | 'optimistic-deny' | 'optimistic-allow';
export type ZeroProviderTransport = {
    install(serverURL: string): unknown;
    logClassifications?: {
        benign?: readonly ZeroLogPattern[];
    };
};
export type WaitForZeroOptions = {
    signal?: AbortSignal;
};
export type CreateZeroClientOptions<Schema extends ZeroSchema, Models extends GenericModels> = {
    schema: Schema;
    models: Models;
    groupedQueries: GroupedQueries;
    aggregates?: AggregateSet<Schema>;
    permissionStrategy?: PermissionStrategy;
    serverAckTimeoutRecoveryThreshold?: number;
    instanceName?: string;
};
export type DirectQueryAdapter<Schema extends ZeroSchema> = (props: {
    DisabledContext: Context<QueryControlMode>;
    customQueries: AnyQueryRegistry;
    getZero: () => any;
    zeroVersion: Emitter<number>;
}) => UseQueryHook<Schema>;
export declare function createZeroClient<Schema extends ZeroSchema, Models extends GenericModels>(options: CreateZeroClientOptions<Schema, Models>): {
    instanceName: string;
    zeroEvents: ZeroEventsEmitter;
    reloadPage: () => boolean;
    ProvideZero: ({ children, authData: authDataIn }: Omit<ZeroOptions<Schema, GetZeroMutators<Models>>, "schema" | "mutators"> & {
        children: ReactNode;
        authData?: {} | null | undefined;
        disable?: boolean;
        transport?: ZeroProviderTransport;
        beforeReload?: (() => Promise<void>) | undefined;
        scheduleReload?: ((ctx: ScheduleReloadContext) => void) | undefined;
        guardStorage?: RecoveryGuardStorage;
        benignLogPatterns?: readonly ZeroLogPattern[];
        refreshAuth?: (() => Promise<string | undefined>) | undefined;
        connectionDataset?: boolean;
    }) => import("react").JSX.Element;
    connectHeadless: (props: Omit<ZeroOptions<Schema, GetZeroMutators<Models>>, "schema" | "mutators"> & {
        authData?: {} | null | undefined;
        transport?: ZeroProviderTransport;
        beforeReload?: (() => Promise<void>) | undefined;
        scheduleReload?: ((ctx: ScheduleReloadContext) => void) | undefined;
        guardStorage?: RecoveryGuardStorage;
        benignLogPatterns?: readonly ZeroLogPattern[];
        refreshAuth?: (() => Promise<string | undefined>) | undefined;
    }) => {
        zero: ZeroClient<Schema, GetZeroMutators<Models>, unknown>;
        close: () => Promise<void>;
    };
    ControlQueries: ({ children, action, whenDisabled, }: {
        children: ReactNode;
        action?: "enable" | "disable";
        whenDisabled?: "empty" | "last-value";
    }) => import("react").JSX.Element;
    useQuery: UseQueryHook<Schema>;
    useQueryDirect: UseQueryHook<Schema>;
    usePermission: (table: (string & {}) | (keyof Schema["tables"] & string), objOrId: string | Partial<Row<any>> | undefined, enabled?: boolean, debug?: boolean) => boolean | null;
    usePermissionDirect: (table: (string & {}) | (keyof Schema["tables"] & string), objOrId: string | Partial<Row<any>> | undefined, enabled?: boolean, debug?: boolean) => boolean | null;
    zero: ZeroClient<Schema, GetZeroMutators<Models>, unknown>;
    preload: {
        <TArg, TTable extends keyof Schema["tables"] & string, TReturn>(fn: PlainQueryFn<TArg, Query<TTable, Schema, TReturn>>, params: TArg, options?: PreloadOptions): {
            cleanup: () => void;
            complete: Promise<void>;
        };
        <TTable extends keyof Schema["tables"] & string, TReturn>(fn: PlainQueryFn<void, Query<TTable, Schema, TReturn>>, options?: PreloadOptions): {
            cleanup: () => void;
            complete: Promise<void>;
        };
    };
    getQuery: {
        <TArg, TTable extends keyof Schema["tables"] & string, TReturn>(fn: PlainQueryFn<TArg, Query<TTable, Schema, TReturn>>, params: TArg): ReturnType<typeof resolveQuery<Schema>>;
        <TTable extends keyof Schema["tables"] & string, TReturn>(fn: PlainQueryFn<void, Query<TTable, Schema, TReturn>>): ReturnType<typeof resolveQuery<Schema>>;
    };
    waitForZero: ({ signal }?: WaitForZeroOptions) => Promise<ZeroClient<Schema, GetZeroMutators<Models>, unknown>>;
    remint: (opts?: {
        dropLocalState?: boolean;
    }) => Promise<boolean>;
    mutationLifecycle: import(".").MutationLifecycle;
    drainBackgroundMutations: (options?: {
        timeoutMs?: number;
    }) => Promise<import(".").BackgroundMutationDrainResult>;
    enqueueBackgroundMutation: (label: string, create: () => unknown, options?: import(".").BackgroundMutationOptions) => Promise<void>;
    awaitMutationClient: (mutation: import(".").MutationLike, label: string, timeoutMs?: number) => Promise<unknown>;
    awaitMutationServer: (mutation: import(".").MutationLike, label: string, timeoutMs?: number) => Promise<unknown>;
};
export declare function createZeroClientInternal<Schema extends ZeroSchema, Models extends GenericModels>({ schema, models, groupedQueries, aggregates, permissionStrategy, instanceName, serverAckTimeoutRecoveryThreshold, createDirectUseQuery, }: CreateZeroClientOptions<Schema, Models> & {
    createDirectUseQuery?: DirectQueryAdapter<Schema>;
}): {
    instanceName: string;
    zeroEvents: ZeroEventsEmitter;
    reloadPage: () => boolean;
    ProvideZero: ({ children, authData: authDataIn }: Omit<ZeroOptions<Schema, GetZeroMutators<Models>>, "schema" | "mutators"> & {
        children: ReactNode;
        authData?: AuthData | null;
        disable?: boolean;
        transport?: ZeroProviderTransport;
        beforeReload?: () => Promise<void>;
        scheduleReload?: (ctx: ScheduleReloadContext) => void;
        guardStorage?: RecoveryGuardStorage;
        benignLogPatterns?: readonly ZeroLogPattern[];
        refreshAuth?: () => Promise<string | undefined>;
        connectionDataset?: boolean;
    }) => import("react").JSX.Element;
    connectHeadless: (props: Omit<ZeroOptions<Schema, GetZeroMutators<Models>>, "schema" | "mutators"> & {
        authData?: AuthData | null;
        transport?: ZeroProviderTransport;
        beforeReload?: () => Promise<void>;
        scheduleReload?: (ctx: ScheduleReloadContext) => void;
        guardStorage?: RecoveryGuardStorage;
        benignLogPatterns?: readonly ZeroLogPattern[];
        refreshAuth?: () => Promise<string | undefined>;
    }) => {
        zero: ZeroClient<Schema, GetZeroMutators<Models>, unknown>;
        close: () => Promise<void>;
    };
    ControlQueries: ({ children, action, whenDisabled, }: {
        children: ReactNode;
        action?: "enable" | "disable";
        whenDisabled?: "empty" | "last-value";
    }) => import("react").JSX.Element;
    useQuery: UseQueryHook<Schema>;
    useQueryDirect: UseQueryHook<Schema>;
    usePermission: (table: (keyof Schema["tables"] & string) | (string & {}), objOrId: string | Partial<Row<any>> | undefined, enabled?: boolean, debug?: boolean) => boolean | null;
    usePermissionDirect: (table: (keyof Schema["tables"] & string) | (string & {}), objOrId: string | Partial<Row<any>> | undefined, enabled?: boolean, debug?: boolean) => boolean | null;
    zero: ZeroClient<Schema, GetZeroMutators<Models>, unknown>;
    preload: {
        <TArg, TTable extends keyof Schema["tables"] & string, TReturn>(fn: PlainQueryFn<TArg, Query<TTable, Schema, TReturn>>, params: TArg, options?: PreloadOptions): {
            cleanup: () => void;
            complete: Promise<void>;
        };
        <TTable extends keyof Schema["tables"] & string, TReturn>(fn: PlainQueryFn<void, Query<TTable, Schema, TReturn>>, options?: PreloadOptions): {
            cleanup: () => void;
            complete: Promise<void>;
        };
    };
    getQuery: {
        <TArg, TTable extends keyof Schema["tables"] & string, TReturn>(fn: PlainQueryFn<TArg, Query<TTable, Schema, TReturn>>, params: TArg): ReturnType<typeof resolveQuery<Schema>>;
        <TTable extends keyof Schema["tables"] & string, TReturn>(fn: PlainQueryFn<void, Query<TTable, Schema, TReturn>>): ReturnType<typeof resolveQuery<Schema>>;
    };
    waitForZero: ({ signal }?: WaitForZeroOptions) => Promise<ZeroClient<Schema, GetZeroMutators<Models>, unknown>>;
    remint: (opts?: {
        dropLocalState?: boolean;
    }) => Promise<boolean>;
    mutationLifecycle: import(".").MutationLifecycle;
    drainBackgroundMutations: (options?: {
        timeoutMs?: number;
    }) => Promise<import(".").BackgroundMutationDrainResult>;
    enqueueBackgroundMutation: (label: string, create: () => unknown, options?: import(".").BackgroundMutationOptions) => Promise<void>;
    awaitMutationClient: (mutation: import(".").MutationLike, label: string, timeoutMs?: number) => Promise<unknown>;
    awaitMutationServer: (mutation: import(".").MutationLike, label: string, timeoutMs?: number) => Promise<unknown>;
};
export {};
//# sourceMappingURL=createZeroClient.d.ts.map