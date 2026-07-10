// sync-server: the sqlite-native zero sync server core (rewrite phase 2 seed,
// see plans/zero-server-rewrite.md + plans/zero-conformance-harness.md M2).
//
// serves the on-zero `transport: 'http-pull'` dialect to STOCK @rocicorp/zero
// clients: cursor-diff pulls over a trigger-fed change log (with full
// snapshot as the single recovery path) and v51 custom-mutator pushes with
// LMID bookkeeping, over any sqlite handle. no zero-cache, no CVR, no
// per-client resident state — durable per-client state is the clients table
// (lastMutationID + group→user binding) only.
//
// wire contract (pinned by ~/orez/plans/zero-http.md VERDICT +
// ~/orez/src/zero-http/server.test.ts, prod-proven by soot's
// src/zero/httpPull.server.ts):
//   POST /pull {clientID, clientGroupID, cookie:number|null}
//     -> {cookie, lastMutationIDChanges, rowsPatch}
//        rowsPatch is put/del diffs when the cookie is within the retained
//        change window, else [{op:'clear'},...puts] (fresh client, cookie
//        below the retention floor, or per-user visibility filtering)
//     -> {cookie, unchanged:true} when cookie === watermark
//     -> 409 when cookie > watermark (client rebuilds via
//        InvalidConnectionRequestBaseCookie)
//   POST /push <v51 push body> -> {pushResponse}
//     replayed ids ack idempotently; app errors advance the LMID and make no
//     row change. every LMID advance appends a change-log marker so pulls
//     never report `unchanged` past it (mutation RECOVERY settles via
//     lastMutationIDChanges in a non-unchanged pull, so an LMID-only push —
//     e.g. an app error — must still advance the cookie).
//
// the cookie is the change log's high watermark (rewrite phase 2, see
// plans/zero-server-rewrite.md). sqlite triggers installed per table feed
// _zsync_changes for EVERY write path — mutators and upstream/admin sql
// alike — a watermark-autoincrement log like orez's production
// `_zero_changes`, but storing touched pks only (diffs re-read live rows).
// retention is size-bounded: pruned changes raise the floor and clients
// below it get a snapshot.
//
// hosting: the caller provides the sqlite handle and the http layer. bun/node
// pass bun:sqlite / better-sqlite3 adapters; a DO passes ctx.storage.sql
// (which supports CREATE TRIGGER; probed 2026-07-09).

// minimal sqlite surface the core needs — deliberately tiny so every host
// (bun:sqlite, better-sqlite3, DO ctx.storage.sql) adapts in a few lines
export type SyncDb = {
  exec(sql: string, params?: unknown[]): void
  all(sql: string, params?: unknown[]): Record<string, unknown>[]
  // must be synchronous-transactional: fn's writes commit atomically
  transaction<T>(fn: () => T): T
}

export type ZeroColumnType = 'string' | 'number' | 'boolean' | 'json' | 'null'

export type SyncTables = Record<
  string,
  { columns: Record<string, ZeroColumnType>; primaryKey: string[] }
>

export class SyncHttpError extends Error {
  constructor(
    readonly status: number,
    message: string
  ) {
    super(message)
  }
}

// thrown by mutators for application-level failures: the mutation's row
// changes roll back, the LMID still advances, the client rolls back its
// optimistic state
export class MutationAppError extends Error {
  constructor(
    readonly details: string,
    message = details
  ) {
    super(message)
  }
}

export type SyncServerConfig = {
  db: SyncDb
  tables: SyncTables
  // per-user row visibility: return {sql, params} selecting the user's
  // visible rows of `table`. default: the whole table.
  visible?: (table: string, userID: string) => { sql: string; params: unknown[] }
  // executes one custom mutation inside the push transaction. throw
  // MutationAppError for app-level rejection (LMID still advances).
  mutate: (tx: SyncDb, name: string, args: unknown, ctx: { userID: string }) => void
  // change-log rows kept below the high watermark; clients whose cookie
  // falls below the pruned floor get a full snapshot. default 4096.
  retainChanges?: number
}

type PullBody = { clientID: string; clientGroupID: string; cookie: number | null }

type PushMutation = {
  type: 'custom'
  id: number
  clientID: string
  name: string
  args: readonly unknown[]
  timestamp?: number
}

type PushBody = {
  clientGroupID: string
  mutations: PushMutation[]
  pushVersion: number
  requestID?: string
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function isNonNegativeInteger(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0
}

function validatePullBody(body: unknown): asserts body is PullBody {
  if (
    !isRecord(body) ||
    typeof body.clientID !== 'string' ||
    typeof body.clientGroupID !== 'string' ||
    (body.cookie !== null && !isNonNegativeInteger(body.cookie))
  ) {
    throw new SyncHttpError(400, 'invalid pull body')
  }
}

function validatePushBody(body: unknown): asserts body is PushBody {
  if (
    !isRecord(body) ||
    typeof body.clientGroupID !== 'string' ||
    !Array.isArray(body.mutations) ||
    typeof body.pushVersion !== 'number' ||
    !Number.isFinite(body.pushVersion)
  ) {
    throw new SyncHttpError(400, 'invalid push body')
  }

  for (const [index, mutation] of body.mutations.entries()) {
    if (
      !isRecord(mutation) ||
      mutation.type !== 'custom' ||
      !isNonNegativeInteger(mutation.id) ||
      mutation.id === 0 ||
      typeof mutation.clientID !== 'string' ||
      typeof mutation.name !== 'string' ||
      !Array.isArray(mutation.args)
    ) {
      throw new SyncHttpError(400, `invalid mutation at index ${index}`)
    }
  }
}

// derive the tables spec from a zero createSchema() result
export function tablesFromZeroSchema(schema: {
  tables: Record<
    string,
    { columns: Record<string, { type: string }>; primaryKey: readonly string[] }
  >
}): SyncTables {
  const tables: SyncTables = {}
  for (const [name, table] of Object.entries(schema.tables)) {
    const columns: Record<string, ZeroColumnType> = {}
    for (const [col, spec] of Object.entries(table.columns)) {
      columns[col] = spec.type as ZeroColumnType
    }
    tables[name] = { columns, primaryKey: [...table.primaryKey] }
  }
  return tables
}

// sqlite stores booleans as 0/1 and json as text; rowsPatch values must
// match the zero schema's column types (same conversion zero-cache's
// replication does, mirrored from soot's toZeroValue)
function toZeroValue(type: ZeroColumnType, raw: unknown): unknown {
  if (raw === null || raw === undefined) return null
  if (type === 'boolean') {
    if (typeof raw === 'boolean') return raw
    return raw === 1 || raw === '1' || raw === 'true' || raw === 't'
  }
  if (type === 'number' && typeof raw === 'string') {
    const numeric = Number(raw)
    if (Number.isFinite(numeric)) return numeric
  }
  if (type === 'json' && typeof raw === 'string') {
    return JSON.parse(raw)
  }
  return raw
}

export function createSyncServer(config: SyncServerConfig) {
  const { db, tables } = config
  const retainChanges = config.retainChanges ?? 4096

  db.exec(`CREATE TABLE IF NOT EXISTS _zsync_clients (
    clientGroupID TEXT NOT NULL,
    clientID TEXT NOT NULL,
    lastMutationID INTEGER NOT NULL,
    userID TEXT,
    PRIMARY KEY (clientGroupID, clientID)
  )`)
  db.exec(`CREATE TABLE IF NOT EXISTS _zsync_meta (
    lock INTEGER PRIMARY KEY CHECK (lock = 1),
    floor INTEGER NOT NULL
  )`)
  db.exec(`INSERT INTO _zsync_meta (lock, floor) VALUES (1, 0)
           ON CONFLICT (lock) DO NOTHING`)
  // the change log records WHICH pks were touched, never row values: sqlite's
  // json functions format REAL at 15 significant digits (probed: 0.1+0.2 →
  // 0.3), so row images through json_object would corrupt float columns. the
  // diff pull re-reads live rows in its own transaction instead — exists=put,
  // gone=del — which is also trivially consistent with pull-time state.
  // op 'marker' rows carry no pk; they only advance the watermark (LMID-only
  // pushes, epoch invalidation). (pk values themselves do pass through
  // json_object: TEXT/INTEGER are exact, so don't use REAL primary keys.)
  db.exec(`CREATE TABLE IF NOT EXISTS _zsync_changes (
    watermark INTEGER PRIMARY KEY AUTOINCREMENT,
    tableName TEXT NOT NULL,
    op TEXT NOT NULL CHECK (op IN ('row', 'marker')),
    pk TEXT
  )`)

  // triggers capture EVERY write path into the change log — mutators inside
  // handlePush and upstream/admin sql alike. installed AFTER any seed so the
  // initial dataset stays out of the log (fresh clients snapshot anyway).
  // updates log OLD and NEW pks so a pk-changing UPDATE dels the old row.
  for (const [table, spec] of Object.entries(tables)) {
    const pkObject = (ref: 'NEW' | 'OLD') =>
      `json_object(${spec.primaryKey.map((col) => `'${col}', ${ref}."${col}"`).join(', ')})`
    db.exec(`CREATE TRIGGER IF NOT EXISTS "_zsync_tr_${table}_i" AFTER INSERT ON "${table}" BEGIN
      INSERT INTO _zsync_changes (tableName, op, pk)
      VALUES ('${table}', 'row', ${pkObject('NEW')});
    END`)
    db.exec(`CREATE TRIGGER IF NOT EXISTS "_zsync_tr_${table}_u" AFTER UPDATE ON "${table}" BEGIN
      INSERT INTO _zsync_changes (tableName, op, pk)
      VALUES ('${table}', 'row', ${pkObject('OLD')});
      INSERT INTO _zsync_changes (tableName, op, pk)
      VALUES ('${table}', 'row', ${pkObject('NEW')});
    END`)
    db.exec(`CREATE TRIGGER IF NOT EXISTS "_zsync_tr_${table}_d" AFTER DELETE ON "${table}" BEGIN
      INSERT INTO _zsync_changes (tableName, op, pk)
      VALUES ('${table}', 'row', ${pkObject('OLD')});
    END`)
  }

  // the cookie: high watermark of the change log (0 = pristine/seed-only)
  function watermark(): number {
    return Number(
      db.all(`SELECT COALESCE(MAX(watermark), 0) AS w FROM _zsync_changes`)[0]!.w
    )
  }

  function floorValue(): number {
    return Number(db.all(`SELECT floor FROM _zsync_meta`)[0]!.floor)
  }

  function pruneChanges(): void {
    const cutoff = watermark() - retainChanges
    if (cutoff > floorValue()) {
      db.exec(`DELETE FROM _zsync_changes WHERE watermark <= ?`, [cutoff])
      db.exec(`UPDATE _zsync_meta SET floor = ?`, [cutoff])
    }
  }

  // epoch bump: forces every client's next pull to be a full snapshot — for
  // changes no row diff can express (visibility/membership revocation,
  // table-set change). the marker advances the watermark so no cookie can
  // answer `unchanged`; the floor then rises past every prior cookie. one
  // recovery path: same snapshot the fresh-client and below-retention cases
  // use. measured prod project snapshots are sub-MB (see
  // plans/zero-server-rewrite.md), so a global epoch is deliberately the
  // whole mechanism — no per-user bookkeeping.
  function invalidate(): void {
    db.transaction(() => {
      db.exec(`INSERT INTO _zsync_changes (tableName, op, pk)
               VALUES ('_zsync_meta', 'marker', NULL)`)
      db.exec(`UPDATE _zsync_meta SET floor =
               (SELECT COALESCE(MAX(watermark), 0) FROM _zsync_changes)`)
    })
  }

  // guarded claim (soot's claimStatement, sqlite dialect): bind the group to
  // this user unless another user already owns it; adopt userID-less rows
  function claimClient(clientGroupID: string, clientID: string, userID: string) {
    // plain positional ? only (repeated params passed twice): DO SqlStorage
    // does not support ?N numbered bindings
    db.exec(
      `INSERT INTO _zsync_clients (clientGroupID, clientID, lastMutationID, userID)
       SELECT ?, ?, 0, ?
       WHERE NOT EXISTS (
         SELECT 1 FROM _zsync_clients
         WHERE clientGroupID = ? AND userID IS NOT NULL AND userID <> ?
       )
       ON CONFLICT (clientGroupID, clientID)
       DO UPDATE SET userID = excluded.userID WHERE userID IS NULL`,
      [clientGroupID, clientID, userID, clientGroupID, userID]
    )
    const owners = db.all(
      `SELECT DISTINCT userID FROM _zsync_clients
       WHERE clientGroupID = ? AND userID IS NOT NULL`,
      [clientGroupID]
    )
    for (const row of owners) {
      if (row.userID !== userID) {
        throw new SyncHttpError(403, 'client group belongs to a different user')
      }
    }
  }

  function visibleRows(table: string, userID: string) {
    const filter = config.visible?.(table, userID)
    if (filter) return db.all(filter.sql, filter.params)
    return db.all(`SELECT * FROM "${table}"`)
  }

  function handlePull(body: unknown, userID: string) {
    validatePullBody(body)
    const { clientID, clientGroupID, cookie } = body

    // one synchronous sqlite transaction = one consistent view (soot needed
    // repeatable-read gymnastics on pg; sqlite gives it for free)
    return db.transaction(() => {
      claimClient(clientGroupID, clientID, userID)
      // Upstream/admin writes also feed the log, so a read-only workload must
      // not depend on a later client push to enforce retention.
      pruneChanges()
      const current = watermark()
      if (cookie !== null && cookie > current) {
        throw new SyncHttpError(
          409,
          `future cookie ${cookie} is ahead of watermark ${current}`
        )
      }
      if (cookie === current) {
        return { cookie: current, unchanged: true as const }
      }

      const lastMutationIDChanges: Record<string, number> = {}
      for (const row of db.all(
        `SELECT clientID, lastMutationID FROM _zsync_clients WHERE clientGroupID = ?`,
        [clientGroupID]
      )) {
        lastMutationIDChanges[row.clientID as string] = Number(row.lastMutationID)
      }

      // diff pulls need uniform visibility (the project-plane assumption):
      // a per-user visible() filter can revoke rows without any row change,
      // which no diff can express — those configs always snapshot
      const canDiff =
        cookie !== null && cookie >= floorValue() && config.visible === undefined
      const rowsPatch: unknown[] = canDiff ? diffPatch(cookie) : snapshotPatch(userID)

      return { cookie: current, lastMutationIDChanges, rowsPatch }
    })
  }

  function snapshotPatch(userID: string): unknown[] {
    const rowsPatch: unknown[] = [{ op: 'clear' }]
    for (const [table, spec] of Object.entries(tables)) {
      for (const row of visibleRows(table, userID)) {
        const value: Record<string, unknown> = {}
        for (const [col, type] of Object.entries(spec.columns)) {
          value[col] = toZeroValue(type, row[col])
        }
        rowsPatch.push({ op: 'put', tableName: table, value })
      }
    }
    return rowsPatch
  }

  // pks touched since the cookie, deduped, then resolved against LIVE table
  // state inside the pull transaction: row exists -> put (current values),
  // row gone -> del. no row images in the log, no op coalescing to get wrong.
  function diffPatch(cookie: number): unknown[] {
    const touched = new Map<string, { table: string; pk: Record<string, unknown> }>()
    for (const change of db.all(
      `SELECT DISTINCT tableName, pk FROM _zsync_changes
       WHERE watermark > ? AND op = 'row'`,
      [cookie]
    )) {
      const table = change.tableName as string
      const pkText = change.pk as string
      touched.set(`${table} ${pkText}`, { table, pk: JSON.parse(pkText) })
    }

    const rowsPatch: unknown[] = []
    for (const { table, pk } of touched.values()) {
      const spec = tables[table]!
      const where = spec.primaryKey.map((col) => `"${col}" = ?`).join(' AND ')
      const params = spec.primaryKey.map((col) => pk[col])
      const row = db.all(`SELECT * FROM "${table}" WHERE ${where}`, params)[0]
      if (!row) {
        const id: Record<string, unknown> = {}
        for (const col of spec.primaryKey)
          id[col] = toZeroValue(spec.columns[col]!, pk[col])
        rowsPatch.push({ op: 'del', tableName: table, id })
      } else {
        const value: Record<string, unknown> = {}
        for (const [col, type] of Object.entries(spec.columns)) {
          value[col] = toZeroValue(type, row[col])
        }
        rowsPatch.push({ op: 'put', tableName: table, value })
      }
    }
    return rowsPatch
  }

  function handlePush(body: unknown, userID: string) {
    validatePushBody(body)
    const { clientGroupID, mutations } = body

    // The pinned client still accepts this legacy pushResponse error form.
    // It is the direct-transport equivalent of zero-cache's PushFailed /
    // unsupportedPushVersion response and prevents any mutation processing.
    if (body.pushVersion !== 1) {
      return {
        pushResponse: {
          error: 'unsupportedPushVersion' as const,
          mutationIDs: mutations.map(({ clientID, id }) => ({ clientID, id })),
        },
      }
    }

    const results: Array<{
      id: { clientID: string; id: number }
      result: Record<string, unknown>
    }> = []

    for (const mutation of mutations) {
      // each mutation commits atomically (rows + LMID advance in one tx). an
      // app error aborts that whole tx, then a SECOND tx advances the LMID
      // and records the error — same net semantics as a savepoint rollback,
      // but with NO savepoint: DO sqlite forbids raw SAVEPOINT/BEGIN (only
      // storage.transactionSync). crash between the two txs is safe: nothing
      // committed, replay re-executes and hits the same app error.
      const applyMutation = (
        executeMutator: boolean
      ): { status: 'applied' } | { status: 'replay'; expectedID: number } =>
        db.transaction(() => {
          claimClient(clientGroupID, mutation.clientID, userID)
          const lmid = Number(
            db.all(
              `SELECT lastMutationID FROM _zsync_clients
               WHERE clientGroupID = ? AND clientID = ?`,
              [clientGroupID, mutation.clientID]
            )[0]!.lastMutationID
          )
          if (mutation.id <= lmid) return { status: 'replay', expectedID: lmid + 1 }
          if (mutation.id > lmid + 1) {
            throw new SyncHttpError(
              400,
              `mutation id ${mutation.id} skips lmid ${lmid} (out of order)`
            )
          }
          if (executeMutator) {
            config.mutate(db, mutation.name, mutation.args[0], { userID })
          }
          db.exec(
            `UPDATE _zsync_clients SET lastMutationID = ?
             WHERE clientGroupID = ? AND clientID = ?`,
            [mutation.id, clientGroupID, mutation.clientID]
          )
          // watermark marker: an LMID-only mutation (app error) must still
          // advance the cookie or group peers' pulls stay `unchanged` and
          // mutation recovery never settles
          db.exec(
            `INSERT INTO _zsync_changes (tableName, op, pk)
             VALUES ('_zsync_clients', 'marker', NULL)`
          )
          return { status: 'applied' }
        })

      const id = { clientID: mutation.clientID, id: mutation.id }
      try {
        const applied = applyMutation(true)
        results.push({
          id,
          result:
            applied.status === 'replay'
              ? {
                  error: 'alreadyProcessed',
                  details: `Ignoring mutation from ${mutation.clientID} with ID ${mutation.id} as it was already processed. Expected: ${applied.expectedID}`,
                }
              : {},
        })
      } catch (error) {
        if (error instanceof MutationAppError) {
          applyMutation(false)
          results.push({
            id,
            result: { error: 'app', message: error.message, details: error.details },
          })
        } else {
          throw error
        }
      }
    }

    // size-bounded retention: pruned changes raise the floor; clients whose
    // cookie fell below it get one snapshot on their next pull
    if (mutations.length > 0) {
      db.transaction(pruneChanges)
    }

    return { pushResponse: { mutations: results } }
  }

  return { handlePull, handlePush, watermark, invalidate }
}

export type SyncServer = ReturnType<typeof createSyncServer>

export type SyncServerOperation = 'pull' | 'push'

export type SyncServerRoute = {
  databaseID: string
  operation: SyncServerOperation
}

export type SyncServerMountConfig = {
  // routes are `${pathPrefix}<databaseID>/pull|push`; `/p-` produces soot's
  // `/p-<projectID>/pull|push`, while `/` produces `/<namespace>/pull|push`.
  pathPrefix: string
  // resolved only when mount.handle() runs, after the caller has had a chance
  // to authorize route.databaseID. the caller owns server and db lifetime.
  server(databaseID: string): SyncServer
}

const SYNC_DATABASE_ROUTE = /^([A-Za-z0-9_-]{1,64})\/(pull|push)$/

// mount the byte-identical pull/push handlers behind one database-id path
// segment. match() performs routing only; handle() delegates directly without
// translating bodies, responses, or errors.
export function createSyncServerMount(config: SyncServerMountConfig) {
  if (
    !config.pathPrefix.startsWith('/') ||
    (config.pathPrefix !== '/' && config.pathPrefix.endsWith('/'))
  ) {
    throw new TypeError(`pathPrefix must start with '/' and end before the database ID`)
  }

  return {
    match(pathname: string): SyncServerRoute | null {
      if (!pathname.startsWith(config.pathPrefix)) return null
      const match = SYNC_DATABASE_ROUTE.exec(pathname.slice(config.pathPrefix.length))
      if (!match) return null

      const databaseID = match[1]!
      const operation = match[2]! as SyncServerOperation
      return { databaseID, operation }
    },
    handle(route: SyncServerRoute, body: unknown, userID: string) {
      const server = config.server(route.databaseID)
      return route.operation === 'pull'
        ? server.handlePull(body, userID)
        : server.handlePush(body, userID)
    },
  }
}
