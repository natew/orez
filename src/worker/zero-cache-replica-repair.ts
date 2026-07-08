/**
 * self-heal for a zero-cache embed's DO-SQLite replica.
 *
 * zero-cache snapshots its publication into a replica (here, the Durable
 * Object's `ctx.storage.sql`, `ZERO_REPLICA_FILE=':do-sqlite:'`). on a real
 * Postgres that replica lives in a normal file with normal transaction
 * semantics; inside a DO the sqlite shim makes BEGIN/COMMIT/ROLLBACK no-ops
 * (the object auto-commits per I/O turn) and any boot can be killed across an
 * await (the 120s ready-timeout, an eviction, an OOM). that combination leaves
 * three distinct corrupt-replica states that each wedge `/sync` permanently
 * until healed. these functions detect and repair each one; every one of them
 * fixed a specific production incident (see the per-function comments).
 *
 * the replica is *derived* data — the upstream rows live in the SQL DO and are
 * untouched by any wipe here, so dropping the replica only forces zero-cache to
 * re-run initial sync. pure logic over minimal `exec`/`get`/`put` shapes (no
 * `@cloudflare/...` types); the decision logic is unit-tested in
 * zero-cache-replica-repair.test.ts against simulated replica states. consumers
 * call these from their ZeroCacheDO boot sequence with their own storage key +
 * log prefix.
 */

export interface ReplicaSqlResult {
  toArray(): Array<Record<string, unknown>>
}

export interface ReplicaSqlStorage {
  exec(sql: string, ...params: unknown[]): ReplicaSqlResult
}

export interface ReplicaKvStorage {
  get(key: string): Promise<unknown>
  put(key: string, value: unknown): Promise<unknown>
}

/**
 * run a SQL statement against the SQL-DO backend (pg-over-DO `/exec`), returning
 * its `{ rows, error }` body. consumers wrap their backend fetch in this shape;
 * an `{ error }` is surfaced (never treated as "no rows", which would silently
 * disable the guards).
 */
export type BackendExec = (
  sql: string,
  params?: unknown[]
) => Promise<{ rows?: Array<Record<string, unknown>>; error?: string }>

/**
 * drop every non-internal table from the replica SQLite. SQLite internals
 * (`sqlite_*`) and Cloudflare's DO tables (`_cf_*`) are skipped: the DO storage
 * authorizer rejects DROP on `_cf_*` with SQLITE_AUTH, which would abort the
 * whole reset. returns the number of tables dropped.
 */
export function dropReplicaTables(sql: ReplicaSqlStorage): number {
  const rows = sql.exec("SELECT name FROM sqlite_master WHERE type='table'").toArray()
  let dropped = 0
  for (const row of rows) {
    const name = String(row.name)
    if (name.startsWith('sqlite_') || name.startsWith('_cf_')) continue
    sql.exec('DROP TABLE IF EXISTS "' + name.replaceAll('"', '""') + '"')
    dropped++
  }
  return dropped
}

// zero-cache snapshots the publication's tables into its replica ONCE during
// initial sync and never picks up a table OR COLUMN added afterward — ALTER only
// feeds the change stream, not the existing snapshot. so a redeploy that evolves
// the schema leaves the persisted replica stuck on the old shape and every
// client fails SchemaVersionNotSupported (2026-06-10: file.title/description
// columns — table set unchanged, so a tables-only tag never reset). key the tag
// on schemaVersion (a hash of the full deploy-time DDL batch — any
// table/column/type change) plus the table set, and wipe the replica on change
// so zero-cache re-runs initial sync over the full publication.
export async function resetReplicaIfTableSetChanged(
  sql: ReplicaSqlStorage,
  storage: ReplicaKvStorage,
  opts: {
    schemaVersion: string
    tables?: Iterable<string>
    /** durable storage key the last-applied tag is persisted under. */
    tagKey: string
  }
): Promise<void> {
  const tag = JSON.stringify([opts.schemaVersion, [...(opts.tables || [])].sort()])
  const lastTag = await storage.get(opts.tagKey)
  // reset whenever the tag differs — including the no-baseline case (lastTag
  // undefined), which covers a DO whose replica was initialized by a deploy that
  // predates this tracking. on a brand-new DO the replica is empty so the drop
  // loop is a no-op; on a stale one it forces a full re-sync.
  if (lastTag !== tag) {
    dropReplicaTables(sql)
  }
  await storage.put(opts.tagKey, tag)
}

// repair a PARTIALLY-INITIALIZED replica left by an interrupted embed boot.
// zero-cache's runSchemaMigrations wraps initial-sync (createReplicationStateTables
// + the versionHistory row write) in one BEGIN EXCLUSIVE/COMMIT, expecting it to
// be atomic. but on a CF DO the sqlite shim makes BEGIN/COMMIT/ROLLBACK NO-OPS
// (the DO auto-commits per I/O turn), and the setup migration is async (it awaits
// initialSync, which yields across turns). so if the boot is killed mid-migration
// — the 120s ready-timeout, a DO eviction, an OOM — the _zero.* tables auto-commit
// but the closing versionHistory INSERT never runs. next boot: getVersionHistory
// reads an empty table => dataVersion 0 => it re-runs the setup migration =>
// CREATE TABLE "_zero.replicationConfig" => "already exists" SQLITE_ERROR, and
// /sync never reaches ready (editor stuck on "loading files"). detect that exact
// inconsistency (replica data tables present but no versionHistory row) and wipe
// the _zero.* replica so the embed re-runs initial sync cleanly.
export function repairPartialReplicaInit(
  sql: ReplicaSqlStorage,
  opts: { logPrefix?: string } = {}
): void {
  const logPrefix = opts.logPrefix ?? '[orez]'
  const hasConfig = sql
    .exec(
      "SELECT 1 FROM sqlite_master WHERE type='table' AND name='_zero.replicationConfig'"
    )
    .toArray().length
  if (!hasConfig) return
  let versionRows = 0
  try {
    versionRows = sql
      .exec('SELECT 1 FROM "_zero.versionHistory" LIMIT 1')
      .toArray().length
  } catch {
    // versionHistory table missing entirely is also an inconsistent state
    versionRows = 0
  }
  if (versionRows > 0) return
  // inconsistent: the replica's _zero.* control tables exist but version tracking
  // is empty. the ENTIRE replica is half-initialized — initial-sync creates the
  // _zero.* control tables AND the published app tables in the same interrupted
  // run, so re-running setup also fails on a duplicate app table. drop every
  // replica table so the next boot initial-syncs the whole set from scratch.
  const dropped = dropReplicaTables(sql)
  console.log(
    logPrefix +
      ' repaired partial replica init: dropped ' +
      dropped +
      ' replica tables (no versionHistory row) so initial sync re-runs'
  )
}

// a changeLog transaction group without a commit entry is an interrupted storer
// write (zero stores each replicated tx inside one pg transaction; real pg rolls
// a crashed tx back, but the DO sqlite shim auto-commits per turn, so a kill
// persists the partial group). catchup replays it as begin->data->begin and the
// replicator dies on "Already in a transaction" on every boot. wiping the replica
// here makes the uninitialized-replica guard clear cdc state, forcing a clean
// initial sync.
export async function resetReplicaIfChangeLogPoisoned(
  sql: ReplicaSqlStorage,
  backendExec: BackendExec,
  opts: { appId: string; logPrefix?: string }
): Promise<void> {
  const logPrefix = opts.logPrefix ?? '[orez]'
  const initialized = sql
    .exec(
      "SELECT 1 FROM sqlite_master WHERE type='table' AND name='_zero.replicationConfig'"
    )
    .toArray().length
  if (!initialized) return
  // /exec parses pg SQL: placeholders are $1-style, never '?'. an {error}
  // response must be surfaced — treating it as "no rows" silently disables the
  // guard (the exact failure mode this guard exists to prevent).
  const listBody = await backendExec(
    "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE $1",
    [opts.appId + '_%/cdc_changeLog']
  )
  if (listBody.error) {
    console.error(logPrefix + ' changeLog poison check failed (list): ' + listBody.error)
    return
  }
  const names = (listBody.rows || []).map((row) => String(row.name))
  for (const name of names) {
    const checkBody = await backendExec(
      'SELECT watermark FROM "' +
        name.replaceAll('"', '""') +
        "\" GROUP BY watermark HAVING SUM(CASE WHEN json_extract(change, '$.tag') = 'commit' THEN 1 ELSE 0 END) = 0 LIMIT 1"
    )
    if (checkBody.error) {
      console.error(
        logPrefix + ' changeLog poison check failed (scan): ' + checkBody.error
      )
      return
    }
    const rows = checkBody.rows || []
    console.log(
      logPrefix +
        ' changeLog poison scan: ' +
        name +
        ' -> ' +
        (rows.length ? 'POISONED at ' + rows[0].watermark : 'clean')
    )
    if (!rows.length) continue
    const dropped = dropReplicaTables(sql)
    console.log(
      logPrefix +
        ' cdc changeLog has a partial transaction at watermark ' +
        rows[0].watermark +
        ' (interrupted storer write): dropped ' +
        dropped +
        ' replica tables so cdc state clears and initial sync re-runs'
    )
    return
  }
}

// a replica without its init marker must not reuse the cdc subscription state,
// or initial sync never re-runs. the change-streamer's subscription state lives
// in the SQL DO and SURVIVES a replica wipe (the resets above, or an OOM
// eviction); a wiped replica + surviving subscription state makes zero-cache
// skip initial sync ("already synced") and serve an EMPTY replica that only ever
// receives catchup changes. when the replica has no init marker, clear the cdc
// state so the embed re-runs initial sync from scratch.
export async function clearChangeStreamerStateIfReplicaUninitialized(
  sql: ReplicaSqlStorage,
  backendExec: BackendExec,
  opts: { appId: string; logPrefix?: string }
): Promise<void> {
  const logPrefix = opts.logPrefix ?? '[orez]'
  const initialized = sql
    .exec(
      "SELECT 1 FROM sqlite_master WHERE type='table' AND name='_zero.replicationConfig'"
    )
    .toArray().length
  if (initialized) return
  // /exec parses pg SQL: $1-style placeholders, never '?' (a '?' is a parse
  // error whose {error} body silently disabled this guard).
  const body = await backendExec(
    "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE $1",
    [opts.appId + '_%/cdc_%']
  )
  if (body.error) {
    console.error(logPrefix + ' cdc state clear failed (list): ' + body.error)
    return
  }
  const names = (body.rows || []).map((row) => String(row.name))
  for (const name of names) {
    await backendExec('DROP TABLE IF EXISTS "' + name.replaceAll('"', '""') + '"')
  }
  if (names.length) {
    console.log(
      logPrefix +
        ' replica uninitialized: cleared ' +
        names.length +
        ' cdc state tables so initial sync re-runs'
    )
  }
}

// zero 1.6's replicaSchema (valita) requires replicas.rank to be a bigint;
// getReplicaAtVersion parses every boot. rank is BIGSERIAL on real pg, so a
// NULL can never exist there — but on the DO backend a replicas row written
// before the serial-column emulation kept NULL, and the parse TypeError kills
// the change-streamer worker in a restart loop. every restart re-streams the
// retained change set (the 2026-07 rows-written burn). backfill rank with
// distinct Date.now()-based values — the same scheme zero's own createReplica
// uses — preserving ORDER BY rank DESC picking the newest replica.
export async function healNullReplicaRank(
  backendExec: BackendExec,
  opts: { appId: string; nowMs?: number; logPrefix?: string }
): Promise<void> {
  const logPrefix = opts.logPrefix ?? '[orez]'
  const list = await backendExec(
    "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE $1",
    [opts.appId + '_%replicas']
  )
  if (list.error) {
    console.error(logPrefix + ' replicas rank heal failed (list): ' + list.error)
    return
  }
  for (const row of list.rows || []) {
    const name = String(row.name)
    const table = name.replaceAll('"', '""')
    const bad = await backendExec('SELECT id FROM "' + table + '" WHERE rank IS NULL')
    if (bad.error) {
      console.error(logPrefix + ' replicas rank heal failed (scan): ' + bad.error)
      continue
    }
    const ids = (bad.rows || []).map((r) => String(r.id))
    let rank = opts.nowMs ?? Date.now()
    for (const id of ids) {
      const updated = await backendExec(
        'UPDATE "' + table + '" SET rank = $1 WHERE id = $2 AND rank IS NULL',
        [rank++, id]
      )
      if (updated.error) {
        console.error(logPrefix + ' replicas rank heal failed (update): ' + updated.error)
        return
      }
    }
    if (ids.length) {
      console.log(logPrefix + ' healed ' + ids.length + ' NULL rank row(s) in ' + name)
    }
  }
}
