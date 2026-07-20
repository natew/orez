import { type Emitter } from './helpers/emitter';
import { type Context } from 'react';
import { type QueryControlMode, type UseQueryHook } from './createUseQuery';
import type { AnyQueryRegistry, Schema as ZeroSchema } from '@rocicorp/zero';
export type MaterializableZero = {
    clientID: string;
    context: unknown;
    materialize(query: any, options?: {
        ttl?: any;
    }): {
        addListener(cb: (data: any, resultType: string, error?: DirectQueryError) => void): void;
        destroy(): void;
        updateTTL(ttl: any): void;
    };
};
export type CreateUseQueryDirect<Schema extends ZeroSchema> = (props: {
    DisabledContext: Context<QueryControlMode>;
    customQueries: AnyQueryRegistry;
    getZero: () => MaterializableZero | null;
    zeroVersion: Emitter<number>;
}) => UseQueryHook<Schema>;
type DirectQueryError = {
    error: 'app' | 'parse';
    message?: string;
    details?: unknown;
};
export declare function createUseQueryDirect<Schema extends ZeroSchema>({ DisabledContext, customQueries, getZero, zeroVersion, }: Parameters<CreateUseQueryDirect<Schema>>[0]): UseQueryHook<Schema>;
export {};
//# sourceMappingURL=createUseQueryDirect.d.ts.map