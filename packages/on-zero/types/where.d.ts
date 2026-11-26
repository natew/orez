import type { TableName, Where } from './types';
import type { Condition } from '@rocicorp/zero';
export declare function setEvaluatingPermission(value: boolean): void;
export declare function where<Table extends TableName, Builder extends Where<Table>>(tableName: Table, builder: Builder, isServerOnly?: boolean): Where<Table, Condition>;
export declare function where<Table extends TableName, Builder extends Where = Where<Table>>(builder: Builder): Where<Table, Condition>;
export declare function getWhereTableName(where: Where): string | undefined;
export declare function getRawWhere(where: Where): Where | undefined;
//# sourceMappingURL=where.d.ts.map