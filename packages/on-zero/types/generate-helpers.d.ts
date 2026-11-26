export type SchemaColumn = {
    type: string;
    optional: boolean;
    customType: unknown;
    serverName?: string;
};
export type ExtractedMutation = {
    name: string;
    paramType: string;
    valibotCode: string;
};
export type ModelMutations = {
    modelName: string;
    hasCRUD: boolean;
    columns: Record<string, SchemaColumn>;
    primaryKeys: string[];
    custom: ExtractedMutation[];
};
export declare function shouldSkipObjectKey(name: string): boolean;
export declare function formatObjectKey(name: string): string;
export declare function getModelImportName(name: string): string;
/**
 * Assigns each name a module alias that cannot collide with another namespace's
 * name or with another alias. `app` and `appSource` are both legal namespaces,
 * so a plain `${name}Source` suffix is not safe on its own; the same is true of
 * the `user` → `userPublic` rename above when both namespaces exist. Emitting a
 * duplicate identifier produces a generated file that does not compile, so the
 * disambiguation has to happen here rather than in the consuming app.
 *
 * Iteration order is the caller's, so output stays deterministic.
 */
export declare function uniqueModuleAliases(names: readonly string[], alias: (name: string) => string): Map<string, string>;
export declare function parseTypeString(type: string): string | null;
export type GeneratedModule = {
    name: string;
    importPath: string;
};
export type GeneratedInstance = {
    name: string;
    scope: string | null;
    queryNames: string[];
    modelNames: string[];
    tables: string[];
    syncTables: string[];
    supportTables: string[];
};
export declare function generateModelsFile(modules: GeneratedModule[]): string;
export declare function generateTypesFile(modelNames: string[]): string;
export declare function generateTablesFile(modules: GeneratedModule[]): string;
export declare function generateReadmeFile(): string;
export declare function generateGroupedQueriesFile(queries: Array<{
    name: string;
    sourceFile: string;
    importPath: string;
}>): string;
export declare function generateInstancesFile(instances: GeneratedInstance[]): string;
export declare function generateAggregatesFile(namespaces: Array<{
    name: string;
    importPath: string;
}>): string;
export declare function generateSyncedQueriesFile(queries: Array<{
    name: string;
    params: string;
    valibotCode: string;
    sourceFile: string;
}>): string;
export declare function columnTypeToValibot(col: SchemaColumn): string;
export declare function schemaColumnsToValibot(columns: Record<string, SchemaColumn>, primaryKeys: string[], mode: 'insert' | 'update' | 'delete'): string;
export declare function extractValibotExpression(valibotCode: string): string;
export declare function parseColumnType(initText: string): SchemaColumn;
export declare function generateSyncedMutationsFile(modelMutations: ModelMutations[]): string;
//# sourceMappingURL=generate-helpers.d.ts.map