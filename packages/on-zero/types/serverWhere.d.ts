import type { TableName, Where } from './types';
import type { Condition } from '@rocicorp/zero';
export declare function serverWhere<Table extends TableName, Builder extends Where<Table>>(tableName: Table, builder: Builder): Where<Table, Condition>;
export declare function serverWhere<Table extends TableName, Builder extends Where = Where<Table>>(builder: Builder): Where<Table, Condition>;
//# sourceMappingURL=serverWhere.d.ts.map