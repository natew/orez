export type LiteMutationExport = {
    modelName: string;
    handlers: Array<{
        name: string;
        paramTypeText: string | null;
    }>;
    schema: LiteSchemaInfo | null;
};
export type LiteSchemaInfo = {
    tableName: string;
    primaryKeys: string[];
    columns: Array<{
        name: string;
        builderText: string;
    }>;
};
export type LiteQueryExport = {
    name: string;
    rootTable: string;
    paramTypeText: string | null;
    relatedPaths: string[][];
};
export type LiteRelationInfo = {
    sourceTable: string;
    name: string;
    targetTable: string;
};
export type LiteTableInfo = {
    name: string;
    columns: string[];
};
export type LiteDataConfig = {
    instances: Record<string, {
        dir?: string;
        scope?: string;
        supportTables?: string[];
    }>;
};
export type LiteParsedFile = {
    mutations: LiteMutationExport[];
    queries: LiteQueryExport[];
    relations?: LiteRelationInfo[];
    tables?: LiteTableInfo[];
    supportTables?: string[];
    imports?: string[];
    dataConfig?: LiteDataConfig;
    parseError?: string;
};
export type LiteParseFn = (sourceCode: string, filePath: string) => LiteParsedFile;
export type LiteGenerateOptions = {
    files: Record<string, string>;
    dir: string;
    config?: string;
    parse: LiteParseFn;
};
export type LiteGenerateResult = {
    files: Record<string, string>;
    instances: Array<{
        name: string;
        syncTables: string[];
        supportTables: string[];
    }>;
    modelCount: number;
    queryCount: number;
    mutationCount: number;
    schemaCount: number;
};
export declare function generateLite(opts: LiteGenerateOptions): LiteGenerateResult;
//# sourceMappingURL=generate-lite.d.ts.map