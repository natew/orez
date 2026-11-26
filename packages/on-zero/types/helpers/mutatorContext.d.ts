import type { AuthData, MutatorContext } from '../types';
export declare function mutatorContext(): MutatorContext;
export declare function isInZeroMutation(): boolean;
export declare function runWithContext<T>(context: MutatorContext, fn: () => T | Promise<T>): Promise<T>;
export declare function getScopedAuthData(): AuthData | null | undefined;
export declare function runWithAuthScope<T>(authData: AuthData | null, fn: () => T | Promise<T>): Promise<T>;
//# sourceMappingURL=mutatorContext.d.ts.map