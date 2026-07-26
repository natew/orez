import { executePostgresQuery } from '@rocicorp/zero/server'

import type {
  ApplicationDatabase,
  ApplicationTransaction,
  ExecResult,
  JsonValue,
  SqlStatementMetadata,
  TransactionQueryFormat,
} from './types.js'
import type { AST, Format, HumanReadable, Schema } from '@rocicorp/zero'
import type { DBTransaction, ServerSchema } from '@rocicorp/zero/server'

export type PostgreSQLQueryResult<Row extends Record<string, unknown>> = {
  readonly rows: readonly Row[]
  readonly rowCount?: number | null
}

export type PostgreSQLClient = {
  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[]
  ): Promise<PostgreSQLQueryResult<Row>>
  release(): void
}

export type PostgreSQLPool = {
  connect(): Promise<PostgreSQLClient>
  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[]
  ): Promise<PostgreSQLQueryResult<Row>>
}

export type PostgreSQLApplicationDatabaseOptions = {
  readonly internalSchema?: string
  readonly schema?: Schema
  readonly queryAst?: <Result>(
    tx: PostgreSQLClient,
    ast: JsonValue,
    format: TransactionQueryFormat,
    queryName?: string
  ) => Promise<Result>
}

export function createPostgreSQLApplicationDatabase(
  pool: PostgreSQLPool,
  options: PostgreSQLApplicationDatabaseOptions = {}
): ApplicationDatabase {
  let serverSchema: Promise<ServerSchema> | undefined

  async function introspectServerSchema(client: PostgreSQLClient): Promise<ServerSchema> {
    if (!options.schema) {
      throw new TypeError(
        'PostgreSQL application database requires schema or queryAst for ZQL reads'
      )
    }
    const tables = Object.values(options.schema.tables).map((table) => {
      const serverName = table.serverName ?? table.name
      const period = serverName.indexOf('.')
      return period < 0
        ? { schema: 'public', table: serverName }
        : {
            schema: serverName.slice(0, period),
            table: serverName.slice(period + 1),
          }
    })
    if (tables.length === 0) return {}
    const params = tables.flatMap((table) => [table.schema, table.table])
    const where = tables
      .map(
        (_, index) =>
          `(c.table_schema = $${index * 2 + 1} AND c.table_name = $${index * 2 + 2})`
      )
      .join(' OR ')
    const rows = (
      await client.query<{
        schema: string
        table: string
        column: string
        dataType: string
        typtype: string
        typename: string
        elemTyptype: string | null
        elemTypname: string | null
      }>(
        `SELECT
           c.table_schema::text AS schema,
           c.table_name::text AS table,
           c.column_name::text AS column,
           c.data_type::text AS "dataType",
           t.typtype::text AS typtype,
           t.typname::text AS typename,
           CASE WHEN t.typelem <> 0 THEN et.typtype::text ELSE NULL END AS "elemTyptype",
           CASE WHEN t.typelem <> 0 THEN et.typname::text ELSE NULL END AS "elemTypname"
         FROM information_schema.columns c
         JOIN pg_catalog.pg_type t ON c.udt_name = t.typname
         LEFT JOIN pg_catalog.pg_type et ON t.typelem = et.oid
         JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
         WHERE ${where}`,
        params
      )
    ).rows
    const result: ServerSchema = {}
    for (const row of rows) {
      const tableName = row.schema === 'public' ? row.table : `${row.schema}.${row.table}`
      const table = (result[tableName] ??= {})
      const isArray = row.elemTyptype !== null
      const isEnum = (row.elemTyptype ?? row.typtype) === 'e'
      table[row.column] = {
        type: isArray
          ? (row.elemTypname ?? row.dataType.toLowerCase())
          : isEnum
            ? row.typename
            : row.dataType.toLowerCase(),
        isArray,
        isEnum,
      }
    }
    return result
  }

  const transactionFor = (client: PostgreSQLClient): ApplicationTransaction => ({
    async exec(
      sql: string,
      params: readonly unknown[] = [],
      _metadata?: SqlStatementMetadata
    ): Promise<ExecResult> {
      const result = await client.query(sql, params)
      return { changes: result.rowCount ?? 0 }
    },
    async query<Row extends Record<string, unknown> = Record<string, unknown>>(
      sql: string,
      params: readonly unknown[] = []
    ): Promise<readonly Row[]> {
      return (await client.query<Row>(sql, params)).rows
    },
    queryAst<Result>(
      ast: JsonValue,
      format: TransactionQueryFormat,
      queryName?: string
    ): Promise<Result> {
      if (!options.queryAst) {
        if (!options.schema) {
          throw new TypeError(
            'PostgreSQL application database requires schema or queryAst for ZQL reads'
          )
        }
        serverSchema ??= introspectServerSchema(client)
        const dbTransaction: DBTransaction<PostgreSQLClient> = {
          wrappedTransaction: client,
          async query(sql: string, params: unknown[]) {
            return (await client.query(sql, params)).rows
          },
          runQuery<Result>(
            queryAst: AST,
            queryFormat: Format,
            schema: Schema,
            resolvedServerSchema: ServerSchema
          ): Promise<HumanReadable<Result>> {
            return executePostgresQuery<Result>(
              this,
              queryAst,
              queryFormat,
              schema,
              resolvedServerSchema
            )
          },
        }
        return serverSchema.then(
          (resolved) =>
            executePostgresQuery<Result>(
              dbTransaction,
              ast as AST,
              format,
              options.schema!,
              resolved
            ) as Promise<Result>
        )
      }
      return options.queryAst(client, ast, format, queryName)
    },
  })

  return {
    dialect: 'postgresql',
    internalSchema: options.internalSchema,
    async transaction<Value>(
      work: (tx: ApplicationTransaction) => Value | Promise<Value>
    ): Promise<Value> {
      const client = await pool.connect()
      try {
        await client.query('BEGIN')
        const result = await work(transactionFor(client))
        await client.query('COMMIT')
        return result
      } catch (error) {
        try {
          await client.query('ROLLBACK')
        } catch {
          // preserve the application or commit error that caused the rollback.
        }
        throw error
      } finally {
        client.release()
      }
    },
    async query<Row extends Record<string, unknown> = Record<string, unknown>>(
      sql: string,
      params: readonly unknown[] = []
    ): Promise<readonly Row[]> {
      return (await pool.query<Row>(sql, params)).rows
    },
  }
}

export function createSQLiteApplicationDatabase(options: {
  transaction<Value>(
    work: (tx: ApplicationTransaction) => Value | Promise<Value>
  ): Promise<Value>
  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[]
  ): Promise<readonly Row[]>
}): ApplicationDatabase {
  return { dialect: 'sqlite', ...options }
}
