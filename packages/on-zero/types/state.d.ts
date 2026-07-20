import type { Schema, SchemaQuery } from '@rocicorp/zero';
import type { AuthData, QueryBuilder } from './types';
export declare const getZQL: () => QueryBuilder;
export declare const getSchema: () => Schema;
export declare const setSchema: <S extends Schema>(_: S, zql: SchemaQuery<S>) => void;
export declare const getAuthData: () => {} | null;
export declare const setAuthData: (_: AuthData) => void;
export declare const getEnvironment: () => "client" | "server" | null;
export declare const setEnvironment: (env: "client" | "server") => void;
//# sourceMappingURL=state.d.ts.map