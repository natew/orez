import type { TableBuilderWithColumns } from '@rocicorp/zero';
import type { MutatorContext, Schema, TableInsertRow, TableName, TableUpdateRow, Where } from './types';
type SchemaTableBuilder<TName extends TableName> = TableBuilderWithColumns<Schema['tables'][TName]>;
type MutationBuilder<Obj = any> = (ctx: MutatorContext, obj?: Obj) => Promise<void>;
type MutationBuilders = Record<string, MutationBuilder>;
type GenericTable = TableBuilderWithColumns<any>;
type CRUDMutations<Table extends GenericTable> = {
    insert: MutationBuilder<TableInsertRow<Table>>;
    upsert: MutationBuilder<TableInsertRow<Table>>;
    update: MutationBuilder<TableUpdateRow<Table>>;
    delete: MutationBuilder<TableUpdateRow<Table>>;
};
type CRUDNames = 'insert' | 'upsert' | 'update' | 'delete';
type MutationsWithCRUD<Table extends GenericTable, Mutations extends MutationBuilders> = {
    [Key in CRUDNames | keyof Mutations]: Key extends keyof Mutations ? Mutations[Key] : Key extends keyof CRUDMutations<any> ? CRUDMutations<Table>[Key] : never;
};
export declare function mutations<Mutations extends MutationBuilders>(mutations: Mutations): Mutations;
export declare function mutations<Table extends GenericTable, Permissions extends Where>(table: Table, permissions: Permissions): MutationsWithCRUD<Table, {}>;
export declare function mutations<Table extends GenericTable, Permissions extends Where, Mutations extends MutationBuilders>(table: Table, permissions: Permissions, mutations: Mutations): MutationsWithCRUD<Table, Mutations>;
export declare function mutations<TName extends TableName, Permissions extends Where>(tableName: TName, permissions: Permissions): MutationsWithCRUD<SchemaTableBuilder<TName>, {}>;
export declare function mutations<TName extends TableName, Permissions extends Where, Mutations extends MutationBuilders>(tableName: TName, permissions: Permissions, mutations: Mutations): MutationsWithCRUD<SchemaTableBuilder<TName>, Mutations>;
export {};
//# sourceMappingURL=mutations.d.ts.map