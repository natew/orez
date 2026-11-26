// the subset of One's platform API that kvStore.one.ts and localPersistence.one.ts read.
// One is an optional peer that only a One app's native build resolves.
declare module 'one' {
  export const platform: string
  export interface AsyncStatement {
    all(...params: unknown[]): Promise<Record<string, unknown>[]>
    run(...params: unknown[]): Promise<unknown>
    finalize(): Promise<void>
  }
  export interface AsyncDatabase {
    exec(sql: string): Promise<void>
    prepare(sql: string): Promise<AsyncStatement>
    close(): Promise<void>
  }
  export interface SyncStatement {
    all(...params: unknown[]): Record<string, unknown>[]
    run(...params: unknown[]): unknown
    values(params: readonly unknown[]): unknown[][]
    finalize(): void
  }
  export class Database {
    constructor(path: string)
    static openAsync(path?: string): Promise<AsyncDatabase>
    close(): void
    exec(sql: string): void
    prepare(sql: string): SyncStatement
  }
  type Entry = { readonly uri: string; readonly exists: boolean }
  export const FileSystem: {
    Directory: new (parent: Entry | string, name: string) => Entry & {
      readonly parentDirectory: Entry
      create(options?: { idempotent?: boolean }): void
    }
    File: new (parent: Entry, name: string) => Entry & { delete(): void }
    Paths: { readonly document: Entry & { readonly parentDirectory: Entry } }
    makeDirectory?(uri: string): Promise<void>
  }
}
