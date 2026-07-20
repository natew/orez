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
export declare function parseTypeString(type: string): string | null;
export declare function generateModelsFile(modelNames: string[], modelsDirName: string): string;
export declare function generateTypesFile(modelNames: string[]): string;
export declare function generateTablesFile(modelNames: string[], modelsDirName: string): string;
export declare function generateReadmeFile(): string;
export declare function generateGroupedQueriesFile(queries: Array<{
    name: string;
    sourceFile: string;
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