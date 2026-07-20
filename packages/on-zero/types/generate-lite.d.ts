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
    paramTypeText: string | null;
};
export type LiteParsedFile = {
    mutations: LiteMutationExport[];
    queries: LiteQueryExport[];
};
export type LiteParseFn = (sourceCode: string, filePath: string) => LiteParsedFile;
export type LiteGenerateOptions = {
    files: Record<string, string>;
    dir: string;
    modelsDir?: 'mutations' | 'models';
    parse: LiteParseFn;
};
export type LiteGenerateResult = {
    files: Record<string, string>;
    modelCount: number;
    queryCount: number;
    mutationCount: number;
    schemaCount: number;
};
export declare function generateLite(opts: LiteGenerateOptions): LiteGenerateResult;
//# sourceMappingURL=generate-lite.d.ts.map