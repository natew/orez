import { createZeroClientInternal, type CreateZeroClientOptions } from './createZeroClient';
import type { GenericModels } from './types';
import type { Schema as ZeroSchema } from '@rocicorp/zero';
import type { AggregateSet } from 'orez-lite/aggregate';
export * from './combineZeroClients';
export declare function createZeroClientWithDirectQueries<Schema extends ZeroSchema, Models extends GenericModels>(options: CreateZeroClientOptions<Schema, Models>): ReturnType<typeof createZeroClientInternal<Schema, Models>>;
export type ZeroInstanceManifestEntry<Schema extends ZeroSchema = ZeroSchema, Models extends GenericModels = GenericModels> = {
    schema: Schema;
    queries: CreateZeroClientOptions<Schema, Models>['groupedQueries'];
    models: Models;
    aggregates?: AggregateSet<Schema>;
    tables: readonly string[];
    syncTables: readonly string[];
    supportTables: readonly string[];
    scope: string | null;
    defaultVisibility: ((value: string) => {
        column: string;
        value: string;
    }) | null;
};
type ClientFor<Entry> = Entry extends ZeroInstanceManifestEntry<infer Schema, infer Models> ? ReturnType<typeof createZeroClientInternal<Schema, Models>> : never;
type ClientMap<Instances extends Record<string, ZeroInstanceManifestEntry>> = {
    [Name in keyof Instances]: ClientFor<Instances[Name]>;
};
type ClientUnion<Instances extends Record<string, ZeroInstanceManifestEntry>> = ClientMap<Instances>[keyof Instances];
export declare function createZeroClients<const Instances extends Record<string, ZeroInstanceManifestEntry>>(instances: Instances): {
    clients: ClientMap<Instances>;
    providers: { [Name in keyof Instances]: ClientMap<Instances>[Name]["ProvideZero"]; };
    combined: import("./combineZeroClients").CombinedZeroClients<[ClientUnion<Instances>, ...ClientUnion<Instances>[]]>;
};
//# sourceMappingURL=multi.d.ts.map