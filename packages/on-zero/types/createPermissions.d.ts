import type { AdminRoleMode, AuthData, Can, TableName, Where } from './types';
import type { Condition, ExpressionBuilder, Schema as ZeroSchema } from '@rocicorp/zero';
export declare class PermissionError extends Error {
    constructor(message: string);
}
export declare function createPermissions<Schema extends ZeroSchema>({ environment, schema, adminRoleMode, }: {
    environment: 'client' | 'server';
    schema: Schema;
    adminRoleMode?: AdminRoleMode;
}): {
    can: Can;
    buildPermissionQuery: <PermissionWhere extends Where<string>>(authData: AuthData | null, eb: ExpressionBuilder<any, any>, permissionWhere: PermissionWhere, objOrId: Record<string, any> | string, tableNameOverride?: TableName) => Condition;
};
//# sourceMappingURL=createPermissions.d.ts.map