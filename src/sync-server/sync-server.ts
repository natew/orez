// sync-server: the sqlite-native zero sync server core (rewrite phase 2 seed,
// see plans/zero-server-rewrite.md + plans/zero-conformance-harness.md M2).
//
// serves the on-zero `transport: 'http-pull'` dialect to STOCK @rocicorp/zero
// clients: full per-user snapshot pulls (clear + puts) and v51 custom-mutator
// pushes with LMID bookkeeping, over any sqlite handle. no zero-cache, no
// CVR, no per-client resident state — the only durable per-client state is
// the clients table (lastMutationID + group→user binding).
//
// wire contract (pinned by ~/orez/plans/zero-http.md VERDICT +
// ~/orez/src/zero-http/server.test.ts, prod-proven by soot's
// src/zero/httpPull.server.ts):
//   POST /pull {clientID, clientGroupID, cookie:number|null}
//     -> {cookie, lastMutationIDChanges, rowsPatch:[{op:'clear'},...puts]}
//     -> {cookie, unchanged:true} when cookie === version
//     -> 409 when cookie > version (client rebuilds via
//        InvalidConnectionRequestBaseCookie)
//   POST /push <v51 push body> -> {pushResponse}
//     replayed ids ack idempotently; app errors advance the LMID and make no
//     row change; every processed push bumps the version cookie.
//
// hosting: the caller provides the sqlite handle and the http layer. bun/node
// pass bun:sqlite / better-sqlite3 adapters; a DO passes ctx.storage.sql.

// minimal sqlite surface the core needs — deliberately tiny so every host
// (bun:sqlite, better-sqlite3, DO ctx.storage.sql) adapts in a few lines
export type SyncDb = {
  exec(sql: string, params?: unknown[]): void
  all(sql: string, params?: unknown[]): Record<string, unknown>[]
  // must be synchronous-transactional: fn's writes commit atomically
  transaction<T>(fn: () => T): T
}

export type ZeroColumnType = 'string' | 'number' | 'boolean' | 'json' | 'null'

export type SyncTables = Record<string, { columns: Record<string, ZeroColumnType> }>

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
  mutate: (
    tx: SyncDb,
    name: string,
    args: unknown,
    ctx: { userID: string }
  ) => void
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

// derive the tables spec from a zero createSchema() result
export function tablesFromZeroSchema(schema: {
  tables: Record<string, { columns: Record<string, { type: string }> }>
}): SyncTables {
  const tables: SyncTables = {}
  for (const [name, table] of Object.entries(schema.tables)) {
    const columns: Record<string, ZeroColumnType> = {}
    for (const [col, spec] of Object.entries(table.columns)) {
      columns[col] = spec.type as ZeroColumnType
    }
    tables[name] = { columns }
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

  db.exec(`CREATE TABLE IF NOT EXISTS _zsync_clients (
    clientGroupID TEXT NOT NULL,
    clientID TEXT NOT NULL,
    lastMutationID INTEGER NOT NULL,
    userID TEXT,
    PRIMARY KEY (clientGroupID, clientID)
  )`)
  db.exec(`CREATE TABLE IF NOT EXISTS _zsync_meta (
    lock INTEGER PRIMARY KEY CHECK (lock = 1),
    version INTEGER NOT NULL
  )`)
  db.exec(`INSERT INTO _zsync_meta (lock, version) VALUES (1, 1)
           ON CONFLICT (lock) DO NOTHING`)

  function version(): number {
    return Number(db.all(`SELECT version FROM _zsync_meta`)[0]!.version)
  }

  // any upstream write outside handlePush must bump the version so pulls see
  // it (the DO host replaces this with its change-tracking watermark)
  function bumpVersion(): number {
    db.exec(`UPDATE _zsync_meta SET version = version + 1`)
    return version()
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

  function handlePull(body: PullBody, userID: string) {
    const { clientID, clientGroupID, cookie } = body
    if (typeof clientID !== 'string' || typeof clientGroupID !== 'string') {
      throw new SyncHttpError(400, 'invalid pull body')
    }

    // one synchronous sqlite transaction = one consistent view (soot needed
    // repeatable-read gymnastics on pg; sqlite gives it for free)
    return db.transaction(() => {
      claimClient(clientGroupID, clientID, userID)
      const current = version()
      if (cookie !== null && cookie > current) {
        throw new SyncHttpError(409, `future cookie ${cookie} is ahead of server ${current}`)
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

      return { cookie: current, lastMutationIDChanges, rowsPatch }
    })
  }

  function handlePush(body: PushBody, userID: string) {
    const { clientGroupID, mutations } = body
    if (typeof clientGroupID !== 'string' || !Array.isArray(mutations)) {
      throw new SyncHttpError(400, 'invalid push body')
    }

    const results: Array<{
      id: { clientID: string; id: number }
      result: Record<string, unknown>
    }> = []

    for (const mutation of mutations) {
      if (mutation.type !== 'custom') {
        throw new SyncHttpError(400, `unsupported mutation type: ${mutation.type}`)
      }
      // each mutation is its own transaction: row changes + LMID advance
      // commit atomically; app errors keep the LMID advance, drop the rows
      db.transaction(() => {
        claimClient(clientGroupID, mutation.clientID, userID)
        const lmid = Number(
          db.all(
            `SELECT lastMutationID FROM _zsync_clients
             WHERE clientGroupID = ? AND clientID = ?`,
            [clientGroupID, mutation.clientID]
          )[0]!.lastMutationID
        )
        if (mutation.id <= lmid) {
          // replay: ack idempotently without re-executing
          results.push({ id: { clientID: mutation.clientID, id: mutation.id }, result: {} })
          return
        }
        if (mutation.id > lmid + 1) {
          throw new SyncHttpError(
            400,
            `mutation id ${mutation.id} skips lmid ${lmid} (out of order)`
          )
        }

        const advance = () =>
          db.exec(
            `UPDATE _zsync_clients SET lastMutationID = ?
             WHERE clientGroupID = ? AND clientID = ?`,
            [mutation.id, clientGroupID, mutation.clientID]
          )

        try {
          // nested savepoint so an app error rolls back the mutator's rows
          // while the outer tx still commits the LMID advance
          db.exec(`SAVEPOINT zsync_mutation`)
          try {
            config.mutate(db, mutation.name, mutation.args[0], { userID })
            db.exec(`RELEASE zsync_mutation`)
          } catch (error) {
            db.exec(`ROLLBACK TO zsync_mutation`)
            db.exec(`RELEASE zsync_mutation`)
            throw error
          }
          advance()
          results.push({ id: { clientID: mutation.clientID, id: mutation.id }, result: {} })
        } catch (error) {
          if (error instanceof MutationAppError) {
            advance()
            results.push({
              id: { clientID: mutation.clientID, id: mutation.id },
              result: { error: 'app', details: error.details },
            })
          } else {
            throw error
          }
        }
      })
    }

    if (mutations.length > 0) bumpVersion()
    return { pushResponse: { mutations: results } }
  }

  return { handlePull, handlePush, version, bumpVersion }
}

export type SyncServer = ReturnType<typeof createSyncServer>
