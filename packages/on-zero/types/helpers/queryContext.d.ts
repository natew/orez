import type { AuthData } from '../types';
export declare function queryAuthData(): AuthData | null;
export declare function isInQueryContext(): boolean;
export declare function runWithQueryContext<T>(context: {
    authData: AuthData | null;
}, fn: () => T | Promise<T>): Promise<T>;
//# sourceMappingURL=queryContext.d.ts.map