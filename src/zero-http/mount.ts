import { createSyncExecutor } from 'orez-sync-executor'

import type { Schema } from '@rocicorp/zero'
import type {
  ApplicationDatabase,
  ApplicationTransaction,
  EffectScheduler,
  MutatorRegistry,
  NormalizedClaims,
  ZeroSchemaConfig,
} from 'orez-sync-executor'

export type ZeroHttpSyncDb = {
  exec(sql: string, params?: readonly unknown[]): void
  all(sql: string, params?: readonly unknown[]): Record<string, unknown>[]
  transaction<Value>(work: () => Value): Value
}

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
) => { where: string; params: readonly unknown[] }

export type ZeroHttpVisibilityChange = {
  readonly table: string
  readonly before: Readonly<Record<string, unknown>> | null
  readonly after: Readonly<Record<string, unknown>> | null
}

export type ZeroHttpVisibilityInvalidation = {
  readonly capture: Readonly<Record<string, readonly string[]>>
  shouldReset(options: {
    readonly transaction: ApplicationTransaction
    readonly userID: string
    readonly changes: readonly ZeroHttpVisibilityChange[]
  }): boolean | Promise<boolean>
}

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
  readonly tables: readonly string[]
  readonly mutators: MutatorRegistry<S>
  readonly effects: EffectScheduler
  readonly visible?: ZeroHttpVisibility
  readonly visibilityInvalidation?: ZeroHttpVisibilityInvalidation
  readonly initialCookie?: (
    transaction: ApplicationTransaction
  ) => number | Promise<number>
  readonly retainChanges?: number
}) {
  const { applicationDatabase, effects, mutators, schema, tables } = options
  const retainChanges = options.retainChanges ?? 4096
  const schemaConfig = schema as unknown as ZeroSchemaConfig
  const tableConfigs = tables.map((logical): TableConfig => {
    const schemaTable = schemaConfig.tables[logical]
    if (!schemaTable) throw new TypeError(`unknown table: ${logical}`)
    return {
      logical,
      physical: schemaTable.serverName ?? schemaTable.name ?? logical,
      columns: Object.fromEntries(
        Object.entries(schemaTable.columns).map(([column, spec]) => {
          const schemaColumn = schemaTable.columns[column]
          if (!schemaColumn) throw new TypeError(`unknown column: ${logical}.${column}`)
          return [
            column,
            { physical: schemaColumn.serverName ?? column, type: spec.type },
          ]
        })
      ),
      primaryKey: schemaTable.primaryKey,
    }
  })
  const tableByLogical = new Map(tableConfigs.map((table) => [table.logical, table]))
  const tableByPhysical = new Map(tableConfigs.map((table) => [table.physical, table]))
  for (const [table, columns] of Object.entries(
    options.visibilityInvalidation?.capture ?? {}
  )) {
    const config = tableByLogical.get(table)
    if (!config)
      throw new TypeError(`visibility invalidation names unknown table: ${table}`)
    for (const column of columns) {
      if (!config.columns[column]) {
        throw new TypeError(
          `visibility invalidation names unknown column: ${table}.${column}`
        )
      }
    }
  }
  const executor = createSyncExecutor({
    database: applicationDatabase,
    effects,
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
        floor INTEGER NOT NULL,
        initialized INTEGER NOT NULL DEFAULT 0
      )`)
        const metaColumns = await tx.query<{ name: string }>(
          `SELECT name FROM pragma_table_info('_zsync_meta')`
        )
        if (!metaColumns.some((column) => column.name === 'initialized')) {
          await tx.exec(
            'ALTER TABLE _zsync_meta ADD COLUMN initialized INTEGER NOT NULL DEFAULT 0'
          )
        }
        await tx.exec(
          `INSERT INTO _zsync_meta (lock, floor, initialized) VALUES (1, 0, 0)
         ON CONFLICT (lock) DO NOTHING`
        )
        const [meta] = await tx.query<{ initialized: number }>(
          'SELECT initialized FROM _zsync_meta WHERE lock = 1'
        )
        if (!meta?.initialized) {
          const requestedCookie = options.initialCookie
            ? await options.initialCookie(tx)
            : 0
          if (
            !Number.isSafeInteger(requestedCookie) ||
            requestedCookie < 0 ||
            requestedCookie >= Number.MAX_SAFE_INTEGER
          ) {
            throw new TypeError(`invalid initial cookie: ${requestedCookie}`)
          }
          const baseline = Math.max(await watermarkIn(tx), requestedCookie)
          if (baseline === 0) {
            await tx.exec('UPDATE _zsync_meta SET initialized = 1 WHERE lock = 1')
          } else {
            const epoch = baseline + 1
            await tx.exec(
              `INSERT INTO _zsync_changes ("watermark", "tableName", "op", "pk")
               VALUES (?, '_zsync_meta', 'marker', NULL)`,
              [epoch]
            )
            await tx.exec(
              'UPDATE _zsync_meta SET floor = ?, initialized = 1 WHERE lock = 1',
              [epoch]
            )
          }
        }
        for (const table of tableConfigs) {
          const capture = [
            ...new Set([
              ...table.primaryKey,
              ...(options.visibilityInvalidation?.capture[table.logical] ?? []),
            ]),
          ]
          const rowObject = (ref: 'NEW' | 'OLD') =>
            `json_object(${capture
              .map(
                (column) =>
                  `${quoteLiteral(column)}, ${ref}.${quoteIdentifier(table.columns[column]!.physical)}`
              )
              .join(', ')})`
          const trigger = `_zsync_tr_${table.physical}`
          const physical = quoteIdentifier(table.physical)
          const tableName = quoteLiteral(table.physical)
          for (const suffix of ['i', 'u', 'd']) {
            await tx.exec(
              `DROP TRIGGER IF EXISTS ${quoteIdentifier(`${trigger}_${suffix}`)}`
            )
          }
          await tx.exec(`CREATE TRIGGER ${quoteIdentifier(`${trigger}_i`)}
          AFTER INSERT ON ${physical} BEGIN
          INSERT INTO _zsync_changes ("tableName", "op", "pk")
          VALUES (${tableName}, 'row', json_object('before', NULL, 'after', ${rowObject('NEW')}));
        END`)
          await tx.exec(`CREATE TRIGGER ${quoteIdentifier(`${trigger}_u`)}
          AFTER UPDATE ON ${physical} BEGIN
          INSERT INTO _zsync_changes ("tableName", "op", "pk")
          VALUES (${tableName}, 'row', json_object('before', ${rowObject('OLD')}, 'after', ${rowObject('NEW')}));
          INSERT INTO _zsync_changes ("tableName", "op", "pk")
          VALUES ('_zsync_meta', 'marker', NULL);
        END`)
          await tx.exec(`CREATE TRIGGER ${quoteIdentifier(`${trigger}_d`)}
          AFTER DELETE ON ${physical} BEGIN
          INSERT INTO _zsync_changes ("tableName", "op", "pk")
          VALUES (${tableName}, 'row', json_object('before', ${rowObject('OLD')}, 'after', NULL));
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
      ? tx.query(
          `SELECT * FROM ${quoteIdentifier(table.physical)} WHERE ${filter.where}`,
          filter.params
        )
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

  function loggedChanges(
    rows: readonly Record<string, unknown>[]
  ): ZeroHttpVisibilityChange[] {
    return rows.map((row) => {
      const table = String(row.tableName)
      const config = tableByPhysical.get(table)
      if (!config) throw new Error(`unknown change-log table: ${table}`)
      const value = JSON.parse(String(row.pk)) as unknown
      if (
        !isRecord(value) ||
        !('before' in value) ||
        !('after' in value) ||
        (value.before !== null && !isRecord(value.before)) ||
        (value.after !== null && !isRecord(value.after))
      ) {
        throw new Error(`invalid change-log row for table: ${table}`)
      }
      return {
        table: config.logical,
        before: value.before,
        after: value.after,
      }
    })
  }

  async function changesSince(
    tx: ApplicationTransaction,
    cookie: number
  ): Promise<ZeroHttpVisibilityChange[]> {
    return loggedChanges(
      await tx.query(
        `SELECT "tableName", "pk" FROM _zsync_changes
         WHERE "watermark" > ? AND "op" = 'row'
         ORDER BY "watermark"`,
        [cookie]
      )
    )
  }

  async function diff(
    tx: ApplicationTransaction,
    changes: readonly ZeroHttpVisibilityChange[],
    userID: string
  ): Promise<unknown[]> {
    const touched = new Map<string, { table: string; pk: Record<string, unknown> }>()
    for (const change of changes) {
      const config = tableByLogical.get(change.table)
      if (!config) throw new Error(`unknown change-log table: ${change.table}`)
      for (const row of [change.before, change.after]) {
        if (!row) continue
        const pk = Object.fromEntries(
          config.primaryKey.map((column) => [column, row[column]])
        )
        touched.set(`${config.physical} ${JSON.stringify(pk)}`, {
          table: config.physical,
          pk,
        })
      }
    }
    const patch: unknown[] = []
    for (const { table, pk } of touched.values()) {
      const config = tableByPhysical.get(table)
      if (!config) throw new Error(`unknown change-log table: ${table}`)
      const where = config.primaryKey
        .map((column) => `${quoteIdentifier(config.columns[column]!.physical)} = ?`)
        .join(' AND ')
      const params = config.primaryKey.map((column) => pk[column])
      const filter = options.visible?.(config.logical, userID)
      const row = (
        await tx.query(
          `SELECT * FROM ${quoteIdentifier(config.physical)} WHERE ${where}${
            filter ? ` AND (${filter.where})` : ''
          }`,
          filter ? [...params, ...filter.params] : params
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
        const canDiff = body.cookie !== null && body.cookie >= (await floorIn(tx))
        const changes = canDiff ? await changesSince(tx, body.cookie!) : []
        const mustReset =
          canDiff &&
          options.visibilityInvalidation !== undefined &&
          (await options.visibilityInvalidation.shouldReset({
            transaction: tx,
            userID: claims.userID,
            changes,
          }))
        return {
          cookie: current,
          lastMutationIDChanges: lmids,
          rowsPatch:
            canDiff && !mustReset
              ? await diff(tx, changes, claims.userID)
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
export type ZeroHttpSyncServer<S extends Schema = Schema> = ReturnType<
  typeof createZeroHttpSyncServer<S>
>

export type ZeroHttpOperation = 'pull' | 'push'
export type ZeroHttpRoute = { databaseID: string; operation: ZeroHttpOperation }

const DATABASE_ROUTE = /^([A-Za-z0-9_-]{1,64})\/(pull|push)$/

export function createZeroHttpMount(options: {
  readonly pathPrefix: string
  server(databaseID: string): ZeroHttpSyncServer<any>
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
