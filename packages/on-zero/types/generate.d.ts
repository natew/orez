import type { SchemaColumn } from './generate-helpers';
type SchemaTable = {
    name: string;
    serverName?: string;
    columns: Record<string, SchemaColumn>;
    primaryKey: readonly string[];
};
type SchemaRelationHop = {
    sourceField: string[];
    destField: string[];
    destSchema: string;
    cardinality: 'one' | 'many';
};
type DrizzleZeroSchema = {
    tables: Record<string, SchemaTable>;
    relationships: Record<string, Record<string, SchemaRelationHop[]>>;
};
/**
 * generate a typed schema.ts from drizzle-zero output.
 * produces a file using table()/createSchema()/relationships() from @rocicorp/zero
 * so the full type system works (no `relationships: any`).
 *
 */
export declare function generateDrizzleSchemaFile(schema: DrizzleZeroSchema): string;
export interface GenerateOptions {
    /** base data directory */
    dir: string;
    /** run after generation */
    after?: string;
    /** suppress output */
    silent?: boolean;
    /** ignore the generation cache */
    force?: boolean;
}
export interface WatchOptions extends GenerateOptions {
    /** debounce delay in ms */
    debounce?: number;
}
export interface GenerateResult {
    filesChanged: number;
    modelCount: number;
    schemaCount: number;
    queryCount: number;
    mutationCount: number;
}
export declare function generate(options: GenerateOptions): Promise<GenerateResult>;
export declare function watch(options: WatchOptions): Promise<import("chokidar").FSWatcher>;
export {};
//# sourceMappingURL=generate.d.ts.map