import type { AnyQueryRegistry, Query, Schema as ZeroSchema } from '@rocicorp/zero';
export type PlainQueryFn<TArg = any, TReturn extends Query<any, any, any> = Query<any, any, any>> = (args: TArg) => TReturn;
/**
 * resolves a plain query function to a QueryRequest using the customQueries registry
 */
export declare function resolveQuery<Schema extends ZeroSchema>({ customQueries, fn, params, }: {
    customQueries: AnyQueryRegistry;
    fn: PlainQueryFn<any, Query<any>>;
    params?: any;
}): any;
//# sourceMappingURL=resolveQuery.d.ts.map