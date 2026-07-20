import type { Query, Row } from '@rocicorp/zero';
type ServerWithQuery = {
    query: (cb: (q: any) => any) => Promise<any>;
};
export declare function createBatchQuery(server: ServerWithQuery): <Q extends Query<any, any, any>, Item extends Row<Q>>(buildQuery: (q: any) => Q, mapper: (items: Item[]) => Promise<void>, { chunk, pause, stopAfter, }?: {
    chunk: number;
    pause?: number;
    stopAfter?: number;
}) => Promise<void>;
export {};
//# sourceMappingURL=batchQuery.d.ts.map