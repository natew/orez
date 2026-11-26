import { useQuery as zeroUseQuery } from '@rocicorp/zero/react';
import { type Context } from 'react';
import { type PlainQueryFn } from './resolveQuery';
import type { AnyQueryRegistry, HumanReadable, Query, Schema as ZeroSchema, TTL } from '@rocicorp/zero';
export type QueryControlMode = false | 'empty' | 'last-value';
export type UseQueryOptions = {
    enabled?: boolean | undefined;
    ttl?: TTL | undefined;
};
type QueryResultDetails = ReturnType<typeof zeroUseQuery>[1];
export type QueryResult<TReturn> = readonly [HumanReadable<TReturn>, QueryResultDetails];
export type { PlainQueryFn };
export type UseQueryHook<Schema extends ZeroSchema> = {
    <TArg, TTable extends keyof Schema['tables'] & string, TReturn>(fn: PlainQueryFn<TArg, Query<TTable, Schema, TReturn>>, params: TArg, options?: UseQueryOptions | boolean): QueryResult<TReturn>;
    <TTable extends keyof Schema['tables'] & string, TReturn>(fn: PlainQueryFn<void, Query<TTable, Schema, TReturn>>, options?: UseQueryOptions | boolean): QueryResult<TReturn>;
};
export declare function emptyResponseFor(queryRequest: unknown): readonly [unknown, {
    type: string;
}];
export declare function parseUseQueryArgs(paramsOrOptions: any, optionsArg: any): {
    params: any;
    options: any;
};
export declare function createUseQuery<Schema extends ZeroSchema>({ DisabledContext, customQueries, }: {
    DisabledContext: Context<QueryControlMode>;
    customQueries: AnyQueryRegistry;
}): UseQueryHook<Schema>;
//# sourceMappingURL=createUseQuery.d.ts.map