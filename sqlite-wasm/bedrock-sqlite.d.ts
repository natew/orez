declare module "bedrock-sqlite" {
  interface RunResult {
    changes: number;
    lastInsertRowid: number | bigint;
  }

  interface ColumnDefinition {
    name: string;
    column: string | null;
    table: string | null;
    database: string | null;
    type: string | null;
  }

  interface DatabaseOptions {
    readonly?: boolean;
    fileMustExist?: boolean;
  }

  interface FunctionOptions {
    deterministic?: boolean;
    directOnly?: boolean;
    varargs?: boolean;
  }

  interface AggregateOptions {
    step: (accumulator: any, ...args: any[]) => any;
    result?: (accumulator: any) => any;
    start?: any;
    inverse?: (accumulator: any, ...args: any[]) => any;
    deterministic?: boolean;
    directOnly?: boolean;
    varargs?: boolean;
  }

  interface Statement {
    readonly source: string;
    readonly reader: boolean;
    readonly readonly: boolean;
    run(...params: any[]): RunResult;
    get(...params: any[]): any;
    all(...params: any[]): any[];
    iterate(...params: any[]): IterableIterator<any>;
    pluck(toggle?: boolean): this;
    expand(toggle?: boolean): this;
    raw(toggle?: boolean): this;
    columns(): ColumnDefinition[];
    bind(...params: any[]): this;
  }

  interface Database {
    readonly open: boolean;
    readonly readonly: boolean;
    readonly name: string;
    readonly inTransaction: boolean;
    prepare(source: string): Statement;
    exec(sql: string): this;
    transaction<F extends (...args: any[]) => any>(
      fn: F
    ): F & {
      deferred: F;
      immediate: F;
      exclusive: F;
      database: Database;
    };
    pragma(source: string, options?: { simple?: boolean }): any;
    function(
      name: string,
      fn: (...args: any[]) => any,
      options?: FunctionOptions
    ): this;
    aggregate(name: string, options: AggregateOptions): this;
    close(): void;
  }

  interface DatabaseConstructor {
    new (filename: string, options?: DatabaseOptions): Database;
  }

  class SqliteError extends Error {
    code: string;
    constructor(message: string, code?: string);
  }

  const Database: DatabaseConstructor;

  export { Database, SqliteError };
  export type {
    RunResult,
    ColumnDefinition,
    DatabaseOptions,
    FunctionOptions,
    AggregateOptions,
    Statement,
    DatabaseConstructor,
  };
}
