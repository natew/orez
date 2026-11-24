// the subset of One's platform API that kvStore.one.ts reads. One is an
// optional peer that only a One app's native build resolves.
declare module 'one' {
  export const platform: string
  export class Database {
    constructor(path: string)
    close(): void
    exec(sql: string): void
    prepare(sql: string): {
      run(params: readonly unknown[]): void
      values(params: readonly unknown[]): unknown[][]
    }
  }
  type Entry = { readonly uri: string; readonly exists: boolean }
  export const FileSystem: {
    Directory: new (parent: Entry | string, name: string) => Entry & {
      readonly parentDirectory: Entry
      create(options: { idempotent: boolean }): void
    }
    File: new (parent: Entry, name: string) => Entry & { delete(): void }
    Paths: { readonly document: { readonly parentDirectory: Entry } }
  }
}
