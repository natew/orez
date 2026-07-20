import { createZeroClientInternal, type CreateZeroClientOptions } from './createZeroClient';
import type { GenericModels } from './types';
import type { Schema as ZeroSchema } from '@rocicorp/zero';
export * from './combineZeroClients';
export declare function createZeroClientWithDirectQueries<Schema extends ZeroSchema, Models extends GenericModels>(options: CreateZeroClientOptions<Schema, Models>): ReturnType<typeof createZeroClientInternal<Schema, Models>>;
export declare function assertZeroInstancePartition(kind: string, entries: Record<string, unknown>, partitions: Record<string, Record<string, unknown>>): void;
//# sourceMappingURL=multi.d.ts.map