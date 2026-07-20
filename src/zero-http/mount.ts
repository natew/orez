import { createSyncExecutor } from 'orez-sync-executor'

import type { Schema } from '@rocicorp/zero'
import type {
  ApplicationDatabase,
  ApplicationTransaction,
  MutatorRegistry,
  NormalizedClaims,
  ZeroSchemaConfig,
} from 'orez-sync-executor'

export type ZeroHttpSyncDb = {
  exec(sql: string, params?: readonly unknown[]): void
  all(sql: string, params?: readonly unknown[]): Record<string, unknown>[]
  transaction<Value>(work: () => Value): Value
}

export type ZeroHttpTables = Record<
  string,
  {
    columns: Record<string, 'string' | 'number' | 'boolean' | 'json' | 'null'>
    primaryKey: string[]
  }
>

export class ZeroHttpRequestError extends Error {
  constructor(
    readonly status: number,
    message: string
  ) {
    super(message)
    this.name = 'ZeroHttpRequestError'
  }
}

export type ZeroHttpVisibility = (
  table: string,
  userID: string
) => { sql: string; params: readonly unknown[] }

type TableConfig = {
  readonly logical: string
  readonly physical: string
  readonly columns: Record<string, { readonly physical: string; readonly type: string }>
  readonly primaryKey: readonly string[]
}

function quoteIdentifier(value: string): string {
  return `"${value.replaceAll('"', '""')}"`
}

function quoteLiteral(value: string): string {
  return `'${value.replaceAll("'", "''")}'`
}

export function createZeroHttpApplicationDatabase(
  db: ZeroHttpSyncDb,
  transaction?: <Value>(work: () => Value | Promise<Value>) => Promise<Value>
): ApplicationDatabase {
  const tx: ApplicationTransaction = {
    async exec(sql, params = []) {
      db.exec(sql, params)
      return { changes: 0 }
    },
    async query<Row extends Record<string, unknown> = Record<string, unknown>>(
      sql: string,
      params: readonly unknown[] = []
    ): Promise<readonly Row[]> {
      return db.all(sql, params) as Row[]
    },
    async queryAst() {
      throw new Error('this zero-http host does not execute server ZQL reads')
    },
  }

  let tail = Promise.resolve()
  return {
    dialect: 'sqlite',
    async transaction<Value>(
      work: (applicationTx: ApplicationTransaction) => Value | Promise<Value>
    ): Promise<Value> {
      if (transaction) return transaction(() => work(tx))
      const previous = tail
      let release = () => {}
      tail = new Promise<void>((resolve) => {
        release = resolve
      })
      await previous
      db.exec('BEGIN')
      try {
        const result = await work(tx)
        db.exec('COMMIT')
        return result
      } catch (error) {
        db.exec('ROLLBACK')
        throw error
      } finally {
        release()
      }
    },
    async query<Row extends Record<string, unknown> = Record<string, unknown>>(
      sql: string,
      params: readonly unknown[] = []
    ): Promise<readonly Row[]> {
      return db.all(sql, params) as Row[]
    },
  }
}

type PullBody = {
  readonly clientID: string
  readonly clientGroupID: string
  readonly cookie: number | null
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function validatePull(value: unknown): PullBody {
  if (
    !isRecord(value) ||
    typeof value.clientID !== 'string' ||
    typeof value.clientGroupID !== 'string' ||
    (value.cookie !== null &&
      (typeof value.cookie !== 'number' ||
        !Number.isSafeInteger(value.cookie) ||
        value.cookie < 0))
  ) {
    throw new ZeroHttpRequestError(400, 'invalid pull body')
  }
  return value as PullBody
}

function toZeroValue(type: string, raw: unknown): unknown {
  if (raw === null || raw === undefined) return null
  if (type === 'boolean') {
    if (typeof raw === 'boolean') return raw
    return raw === 1 || raw === '1' || raw === 'true' || raw === 't'
  }
  if (type === 'number' && typeof raw === 'string') {
    const numeric = Number(raw)
    if (Number.isFinite(numeric)) return numeric
  }
  if (type === 'json' && typeof raw === 'string') return JSON.parse(raw)
  return raw
}

export function createZeroHttpSyncServer<S extends Schema>(options: {
  readonly applicationDatabase: ApplicationDatabase
  readonly schema: S
  readonly tables: ZeroHttpTables
  readonly mutators: MutatorRegistry<S>
  readonly visible?: ZeroHttpVisibility
  readonly retainChanges?: number
}) {
  const { applicationDatabase, mutators, schema, tables } = options
  const retainChanges = options.retainChanges ?? 4096
  const schemaConfig = schema as unknown as ZeroSchemaConfig
  const tableConfigs = Object.entries(tables).map(([logical, spec]): TableConfig => {
    const schemaTable = schemaConfig.tables[logical]
    if (!schemaTable) throw new TypeError(`unknown table: ${logical}`)
    return {
      logical,
      physical: schemaTable.serverName ?? schemaTable.name ?? logical,
      columns: Object.fromEntries(
        Object.entries(spec.columns).map(([column, type]) => {
          const schemaColumn = schemaTable.columns[column]
          if (!schemaColumn) throw new TypeError(`unknown column: ${logical}.${column}`)
          return [column, { physical: schemaColumn.serverName ?? column, type }]
        })
      ),
      primaryKey: spec.primaryKey,
    }
  })
  const tableByPhysical = new Map(tableConfigs.map((table) => [table.physical, table]))
  const executor = createSyncExecutor({
    database: applicationDatabase,
    effects: {
      runBackground(promise) {
        return promise
      },
      report(error) {
        throw error
      },
    },
    mutators,
    schema,
  })

  // every statement below runs through applicationDatabase, never a raw handle:
  // that is what lets this mount sit on a remote/async application database, and
  // it keeps pull, invalidate and prune on the SAME queue as mutator execution
  // so a push cannot interleave with a pull mid-transaction.
  const ready = executor
    .push(
      { clientGroupID: '__zero_http_mount__', mutations: [], pushVersion: 1 },
      { userID: '__zero_http_mount__' }
    )
    .then(() =>
      applicationDatabase.transaction(async (tx) => {
        await tx.exec(`CREATE TABLE IF NOT EXISTS _zsync_meta (
        lock INTEGER PRIMARY KEY CHECK (lock = 1),
        floor INTEGER NOT NULL
      )`)
        await tx.exec(
          `INSERT INTO _zsync_meta (lock, floor) VALUES (1, 0)
         ON CONFLICT (lock) DO NOTHING`
        )
        for (const table of tableConfigs) {
          const pkObject = (ref: 'NEW' | 'OLD') =>
            `json_object(${table.primaryKey
              .map(
                (column) =>
                  `${quoteLiteral(column)}, ${ref}.${quoteIdentifier(table.columns[column]!.physical)}`
              )
              .join(', ')})`
          const trigger = `_zsync_tr_${table.physical}`
          const physical = quoteIdentifier(table.physical)
          const tableName = quoteLiteral(table.physical)
          // additive: CREATE TRIGGER IF NOT EXISTS never rewrites an existing
          // table, so installing these against a populated database changes no
          // rows and drops nothing.
          await tx.exec(`CREATE TRIGGER IF NOT EXISTS ${quoteIdentifier(`${trigger}_i`)}
          AFTER INSERT ON ${physical} BEGIN
          INSERT INTO _zsync_changes ("tableName", "op", "pk")
          VALUES (${tableName}, 'row', ${pkObject('NEW')});
        END`)
          await tx.exec(`CREATE TRIGGER IF NOT EXISTS ${quoteIdentifier(`${trigger}_u`)}
          AFTER UPDATE ON ${physical} BEGIN
          INSERT INTO _zsync_changes ("tableName", "op", "pk")
          VALUES (${tableName}, 'row', ${pkObject('OLD')});
          INSERT INTO _zsync_changes ("tableName", "op", "pk")
          VALUES (${tableName}, 'row', ${pkObject('NEW')});
        END`)
          await tx.exec(`CREATE TRIGGER IF NOT EXISTS ${quoteIdentifier(`${trigger}_d`)}
          AFTER DELETE ON ${physical} BEGIN
          INSERT INTO _zsync_changes ("tableName", "op", "pk")
          VALUES (${tableName}, 'row', ${pkObject('OLD')});
        END`)
        }
      })
    )

  async function watermarkIn(tx: ApplicationTransaction): Promise<number> {
    const rows = await tx.query(
      'SELECT COALESCE(MAX("watermark"), 0) AS value FROM _zsync_changes'
    )
    return Number(rows[0]!.value)
  }

  async function floorIn(tx: ApplicationTransaction): Promise<number> {
    const rows = await tx.query('SELECT floor FROM _zsync_meta')
    return Number(rows[0]!.floor)
  }

  async function claimClient(
    tx: ApplicationTransaction,
    clientGroupID: string,
    clientID: string,
    userID: string
  ): Promise<void> {
    await tx.exec(
      `INSERT INTO _zsync_clients ("clientGroupID", "clientID", "lastMutationID", "userID")
       SELECT ?, ?, 0, ?
       WHERE NOT EXISTS (
         SELECT 1 FROM _zsync_clients
         WHERE "clientGroupID" = ? AND "userID" IS NOT NULL AND "userID" <> ?
       )
       ON CONFLICT ("clientGroupID", "clientID")
       DO UPDATE SET "userID" = excluded."userID" WHERE "userID" IS NULL`,
      [clientGroupID, clientID, userID, clientGroupID, userID]
    )
    const owners = await tx.query(
      `SELECT DISTINCT "userID" FROM _zsync_clients
       WHERE "clientGroupID" = ? AND "userID" IS NOT NULL`,
      [clientGroupID]
    )
    if (owners.some((owner) => owner.userID !== userID)) {
      throw new ZeroHttpRequestError(403, 'client group belongs to a different user')
    }
  }

  async function prune(tx: ApplicationTransaction): Promise<void> {
    const cutoff = (await watermarkIn(tx)) - retainChanges
    if (cutoff <= (await floorIn(tx))) return
    await tx.exec('DELETE FROM _zsync_changes WHERE "watermark" <= ?', [cutoff])
    await tx.exec('UPDATE _zsync_meta SET floor = ?', [cutoff])
  }

  async function visibleRows(
    tx: ApplicationTransaction,
    table: TableConfig,
    userID: string
  ): Promise<readonly Record<string, unknown>[]> {
    const filter = options.visible?.(table.logical, userID)
    return filter
      ? tx.query(filter.sql, filter.params)
      : tx.query(`SELECT * FROM ${quoteIdentifier(table.physical)}`)
  }

  async function snapshot(
    tx: ApplicationTransaction,
    userID: string
  ): Promise<unknown[]> {
    const patch: unknown[] = [{ op: 'clear' }]
    for (const table of tableConfigs) {
      for (const row of await visibleRows(tx, table, userID)) {
        patch.push({
          op: 'put',
          tableName: table.physical,
          value: Object.fromEntries(
            Object.values(table.columns).map((column) => [
              column.physical,
              toZeroValue(column.type, row[column.physical]),
            ])
          ),
        })
      }
    }
    return patch
  }

  async function diff(tx: ApplicationTransaction, cookie: number): Promise<unknown[]> {
    const touched = new Map<string, { table: string; pk: Record<string, unknown> }>()
    for (const change of await tx.query(
      `SELECT DISTINCT "tableName", "pk" FROM _zsync_changes
       WHERE "watermark" > ? AND "op" = 'row'`,
      [cookie]
    )) {
      const table = String(change.tableName)
      const pk = JSON.parse(String(change.pk)) as Record<string, unknown>
      touched.set(`${table} ${change.pk}`, { table, pk })
    }
    const patch: unknown[] = []
    for (const { table, pk } of touched.values()) {
      const config = tableByPhysical.get(table)
      if (!config) throw new Error(`unknown change-log table: ${table}`)
      const where = config.primaryKey
        .map((column) => `${quoteIdentifier(config.columns[column]!.physical)} = ?`)
        .join(' AND ')
      const params = config.primaryKey.map((column) => pk[column])
      const row = (
        await tx.query(
          `SELECT * FROM ${quoteIdentifier(config.physical)} WHERE ${where}`,
          params
        )
      )[0]
      if (!row) {
        patch.push({
          op: 'del',
          tableName: config.physical,
          id: Object.fromEntries(
            config.primaryKey.map((column) => [
              config.columns[column]!.physical,
              toZeroValue(config.columns[column]!.type, pk[column]),
            ])
          ),
        })
      } else {
        patch.push({
          op: 'put',
          tableName: config.physical,
          value: Object.fromEntries(
            Object.values(config.columns).map((column) => [
              column.physical,
              toZeroValue(column.type, row[column.physical]),
            ])
          ),
        })
      }
    }
    return patch
  }

  return {
    executor,
    ready: () => ready,
    async watermark(): Promise<number> {
      await ready
      return applicationDatabase.transaction((tx) => watermarkIn(tx))
    },
    async invalidate(): Promise<void> {
      await ready
      await applicationDatabase.transaction(async (tx) => {
        await tx.exec(
          `INSERT INTO _zsync_changes ("tableName", "op", "pk")
           VALUES ('_zsync_meta', 'marker', NULL)`
        )
        await tx.exec(
          `UPDATE _zsync_meta SET floor =
           (SELECT COALESCE(MAX("watermark"), 0) FROM _zsync_changes)`
        )
      })
    },
    async handlePull(value: unknown, claims: NormalizedClaims): Promise<unknown> {
      await ready
      const body = validatePull(value)
      return applicationDatabase.transaction(async (tx) => {
        await claimClient(tx, body.clientGroupID, body.clientID, claims.userID)
        await prune(tx)
        const current = await watermarkIn(tx)
        if (body.cookie !== null && body.cookie > current) {
          throw new ZeroHttpRequestError(
            409,
            `future cookie ${body.cookie} is ahead of watermark ${current}`
          )
        }
        if (body.cookie === current) return { cookie: current, unchanged: true }
        const lmids = Object.fromEntries(
          (
            await tx.query(
              `SELECT "clientID", "lastMutationID" FROM _zsync_clients
               WHERE "clientGroupID" = ?`,
              [body.clientGroupID]
            )
          ).map((row) => [String(row.clientID), Number(row.lastMutationID)])
        )
        const canDiff =
          body.cookie !== null &&
          body.cookie >= (await floorIn(tx)) &&
          options.visible === undefined
        return {
          cookie: current,
          lastMutationIDChanges: lmids,
          rowsPatch: canDiff
            ? await diff(tx, body.cookie!)
            : await snapshot(tx, claims.userID),
        }
      })
    },
    async handlePush(value: unknown, claims: NormalizedClaims): Promise<unknown> {
      await ready
      const result = await executor.push(value, claims)
      if (
        'mutations' in result.pushResponse &&
        result.pushResponse.mutations.length > 0
      ) {
        await applicationDatabase.transaction((tx) => prune(tx))
      }
      return result
    },
  }
}
export type ZeroHttpSyncServer = ReturnType<typeof createZeroHttpSyncServer>

export type ZeroHttpOperation = 'pull' | 'push'
export type ZeroHttpRoute = { databaseID: string; operation: ZeroHttpOperation }

const DATABASE_ROUTE = /^([A-Za-z0-9_-]{1,64})\/(pull|push)$/

export function createZeroHttpMount(options: {
  readonly pathPrefix: string
  server(databaseID: string): ZeroHttpSyncServer
}) {
  if (
    !options.pathPrefix.startsWith('/') ||
    (options.pathPrefix !== '/' && options.pathPrefix.endsWith('/'))
  ) {
    throw new TypeError('pathPrefix must start with / and end before the database ID')
  }
  return {
    match(pathname: string): ZeroHttpRoute | null {
      if (!pathname.startsWith(options.pathPrefix)) return null
      const match = DATABASE_ROUTE.exec(pathname.slice(options.pathPrefix.length))
      if (!match) return null
      return {
        databaseID: match[1]!,
        operation: match[2]! as ZeroHttpOperation,
      }
    },
    handle(route: ZeroHttpRoute, body: unknown, claims: NormalizedClaims) {
      const server = options.server(route.databaseID)
      return route.operation === 'pull'
        ? server.handlePull(body, claims)
        : server.handlePush(body, claims)
    },
  }
}
