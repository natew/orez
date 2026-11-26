export type DataNamespace = {
    name: string;
    instance: string;
    queryPath: string | null;
    modelPath: string | null;
    aggregatePath: string | null;
    table: string | null;
    sourcePaths: string[];
};
export type DataInstance = {
    name: string;
    dir: string;
    scope: string | null;
    namespaces: DataNamespace[];
    tables: string[];
    syncTables: string[];
    supportTables: string[];
    /** `supportTables` declared in `on-zero.config.ts`. */
    declaredSupportTables: string[];
};
export type DataLayout = {
    instances: DataInstance[];
    namespaces: DataNamespace[];
    metadataPaths: string[];
    sourceRoots: string[];
};
export declare function discoverDataLayout(ts: typeof import('typescript'), baseDir: string, configPath?: string): DataLayout;
export declare function namespaceImportPath(baseDir: string, path: string): string;
//# sourceMappingURL=generate-layout.d.ts.map