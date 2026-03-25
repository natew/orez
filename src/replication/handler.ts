/**
 * replication protocol handler.
 *
 * intercepts replication-mode queries (IDENTIFY_SYSTEM, CREATE_REPLICATION_SLOT,
 * START_REPLICATION) and returns fake responses that make zero-cache believe
 * it's talking to a real postgres with logical replication.
 */

import { log } from '../log.js'

const textEncoder = new TextEncoder()
import {
  getChangesSince,
  getCurrentWatermark,
  purgeConsumedChanges,
  installTriggersOnShardTables,
  type ChangeRecord,
} from './change-tracker.js'
import {
  encodeBegin,
  encodeCommit,
  encodeRelation,
  encodeInsert,
  encodeUpdate,
  encodeDelete,
  encodeKeepalive,
  encodeWrappedChange,
  getTableOid,
  inferColumns,
} from './pgoutput-encoder.js'

import type { Mutex } from '../mutex.js'
import type { PGlite } from '@electric-sql/pglite'

export interface ReplicationWriter {
  write(data: Uint8Array): void
  readonly closed?: boolean
}

/**
 * in-process replication writer. routes pgoutput data via callback
 * instead of a TCP socket. used in CF Workers / embedded mode where
 * there's no network between orez and zero-cache.
 */
export class InProcessWriter implements ReplicationWriter {
  #onData: (data: Uint8Array) => void
  #closed = false

  constructor(onData: (data: Uint8Array) => void) {
    this.#onData = onData
  }

  write(data: Uint8Array): void {
    if (!this.#closed) {
      this.#onData(data)
    }
  }

  get closed(): boolean {
    return this.#closed
  }

  close(): void {
    this.#closed = true
  }
}

// current lsn counter
let currentLsn = 0x1000000n
// persistent watermark across handler restarts so new handlers
// don't replay already-streamed changes
let lastStreamedWatermark = 0

// direct wakeup from proxy — bypasses pg_notify for instant replication
let _replicationWakeup: (() => void) | null = null

/** signal the replication handler that changes may be available.
 *  called by the proxy after executing writes on the postgres instance. */
export function signalReplicationChange() {
  _replicationWakeup?.()
}

// cached setup results so reconnects skip the expensive mutex-holding setup phase.
// zero-cache reconnects the replication stream after initial sync, and if setup
// takes too long (holding the mutex, blocking proxy queries), zero-cache's
// queries timeout and it kills the connection.
let cachedTableKeyColumns: Map<string, Set<string>> | null = null
let cachedExcludedColumns: Map<string, Set<string>> | null = null
let cachedColumnTypeOids: Map<string, Map<string, number>> | null = null

/** reset module state (for tests) */
export function resetReplicationState(): void {
  currentLsn = 0x1000000n
  lastStreamedWatermark = 0
  cachedTableKeyColumns = null
  cachedExcludedColumns = null
  cachedColumnTypeOids = null
}
function nextLsn(): bigint {
  currentLsn += 0x100n
  return currentLsn
}

function lsnToString(lsn: bigint): string {
  const high = Number(lsn >> 32n)
  const low = Number(lsn & 0xffffffffn)
  return `${high.toString(16).toUpperCase()}/${low.toString(16).toUpperCase()}`
}

function nowMicros(): bigint {
  return BigInt(Date.now()) * 1000n
}

// build a wire protocol row description + data row response
function buildSimpleResponse(columns: string[], values: string[]): Uint8Array {
  const parts: Uint8Array[] = []
  const encoder = textEncoder

  // RowDescription (0x54)
  let rdSize = 6 // int32 len + int16 numFields
  const colBytes: Uint8Array[] = []
  for (const col of columns) {
    const b = encoder.encode(col)
    colBytes.push(b)
    rdSize += b.length + 1 + 4 + 2 + 4 + 2 + 4 + 2 // name+null + tableOid + colAttr + typeOid + typeLen + typeMod + formatCode
  }
  const rd = new Uint8Array(1 + rdSize)
  const rdv = new DataView(rd.buffer)
  rd[0] = 0x54
  rdv.setInt32(1, rdSize)
  rdv.setInt16(5, columns.length)
  let pos = 7
  for (let i = 0; i < columns.length; i++) {
    rd.set(colBytes[i], pos)
    pos += colBytes[i].length
    rd[pos++] = 0
    rdv.setInt32(pos, 0) // tableOid
    pos += 4
    rdv.setInt16(pos, 0) // colAttr
    pos += 2
    rdv.setInt32(pos, 25) // typeOid (text)
    pos += 4
    rdv.setInt16(pos, -1) // typeLen
    pos += 2
    rdv.setInt32(pos, -1) // typeMod
    pos += 4
    rdv.setInt16(pos, 0) // formatCode (text)
    pos += 2
  }
  parts.push(rd)

  // DataRow (0x44)
  let drSize = 6 // int32 len + int16 numCols
  const valBytes: Uint8Array[] = []
  for (const val of values) {
    const b = encoder.encode(val)
    valBytes.push(b)
    drSize += 4 + b.length
  }
  const dr = new Uint8Array(1 + drSize)
  const drv = new DataView(dr.buffer)
  dr[0] = 0x44
  drv.setInt32(1, drSize)
  drv.setInt16(5, values.length)
  pos = 7
  for (const vb of valBytes) {
    drv.setInt32(pos, vb.length)
    pos += 4
    dr.set(vb, pos)
    pos += vb.length
  }
  parts.push(dr)

  // CommandComplete (0x43)
  const tag = encoder.encode('SELECT 1\0')
  const cc = new Uint8Array(1 + 4 + tag.length)
  cc[0] = 0x43
  new DataView(cc.buffer).setInt32(1, 4 + tag.length)
  cc.set(tag, 5)
  parts.push(cc)

  // ReadyForQuery (0x5a)
  const rfq = new Uint8Array(6)
  rfq[0] = 0x5a
  new DataView(rfq.buffer).setInt32(1, 5)
  rfq[5] = 0x49 // 'I' idle
  parts.push(rfq)

  // concatenate
  const totalLen = parts.reduce((sum, p) => sum + p.length, 0)
  const result = new Uint8Array(totalLen)
  let offset = 0
  for (const p of parts) {
    result.set(p, offset)
    offset += p.length
  }
  return result
}

function buildCommandComplete(tag: string): Uint8Array {
  const encoder = textEncoder
  const tagBytes = encoder.encode(tag + '\0')
  const cc = new Uint8Array(1 + 4 + tagBytes.length)
  cc[0] = 0x43
  new DataView(cc.buffer).setInt32(1, 4 + tagBytes.length)
  cc.set(tagBytes, 5)

  const rfq = new Uint8Array(6)
  rfq[0] = 0x5a
  new DataView(rfq.buffer).setInt32(1, 5)
  rfq[5] = 0x49

  const result = new Uint8Array(cc.length + rfq.length)
  result.set(cc, 0)
  result.set(rfq, cc.length)
  return result
}

function buildErrorResponse(message: string): Uint8Array {
  const encoder = textEncoder
  const msgBytes = encoder.encode(message)
  // S(severity) + M(message) + null terminator
  const fields = new Uint8Array(2 + 6 + 2 + msgBytes.length + 1 + 1) // S + ERROR\0 + M + msg\0 + terminator
  let pos = 0
  fields[pos++] = 0x53 // 'S'
  const sev = encoder.encode('ERROR\0')
  fields.set(sev, pos)
  pos += sev.length
  fields[pos++] = 0x4d // 'M'
  fields.set(msgBytes, pos)
  pos += msgBytes.length
  fields[pos++] = 0 // null terminate message
  fields[pos++] = 0 // final terminator

  const buf = new Uint8Array(1 + 4 + pos)
  buf[0] = 0x45 // 'E'
  new DataView(buf.buffer).setInt32(1, 4 + pos)
  buf.set(fields.subarray(0, pos), 5)
  return buf
}

/**
 * handle a replication query. returns response bytes or null if not handled.
 * async because slot operations need to write to pglite.
 */
export async function handleReplicationQuery(
  query: string,
  db: PGlite
): Promise<Uint8Array | null> {
  const trimmed = query.trim().replace(/;$/, '').trim()
  const upper = trimmed.toUpperCase()

  if (upper === 'IDENTIFY_SYSTEM') {
    const lsn = lsnToString(currentLsn)
    return buildSimpleResponse(
      ['systemid', 'timeline', 'xlogpos', 'dbname'],
      ['1234567890', '1', lsn, 'postgres']
    )
  }

  if (upper.startsWith('CREATE_REPLICATION_SLOT')) {
    const match = trimmed.match(
      /CREATE_REPLICATION_SLOT\s+(?:"([^"]+)"|'([^']+)'|(\S+))/i
    )
    const slotName = match?.[1] || match?.[2] || match?.[3] || 'zero_slot'
    const lsn = lsnToString(nextLsn())
    const snapshotName = `00000003-00000001-1`

    // set watermark to current DB state so replication only delivers changes
    // that happen AFTER this point. this mirrors real postgres behavior where
    // CREATE_REPLICATION_SLOT creates a consistent snapshot — the initial copy
    // captures everything up to this point, and replication picks up from here.
    // on reconnect this is effectively a no-op since the watermark is already
    // at or past the current DB state.
    const currentWm = await getCurrentWatermark(db)
    if (currentWm > lastStreamedWatermark) {
      lastStreamedWatermark = currentWm
    }

    // persist slot so pg_replication_slots queries find it
    await db.query(
      `INSERT INTO _orez._zero_replication_slots (slot_name, restart_lsn, confirmed_flush_lsn)
       VALUES ($1, $2, $2)
       ON CONFLICT (slot_name) DO UPDATE SET restart_lsn = $2, confirmed_flush_lsn = $2`,
      [slotName, lsn]
    )

    return buildSimpleResponse(
      ['slot_name', 'consistent_point', 'snapshot_name', 'output_plugin'],
      [slotName, lsn, snapshotName, 'pgoutput']
    )
  }

  if (upper.startsWith('DROP_REPLICATION_SLOT')) {
    const match = trimmed.match(/DROP_REPLICATION_SLOT\s+(?:"([^"]+)"|'([^']+)'|(\S+))/i)
    const slotName = match?.[1] || match?.[2] || match?.[3]
    if (slotName) {
      await db.query(`DELETE FROM _orez._zero_replication_slots WHERE slot_name = $1`, [
        slotName,
      ])
    }
    return buildCommandComplete('DROP_REPLICATION_SLOT')
  }

  // wal_level check via simple query
  if (upper.includes('WAL_LEVEL') && upper.includes('CURRENT_SETTING')) {
    return buildSimpleResponse(['walLevel', 'version'], ['logical', '170004'])
  }

  // ALTER ROLE for replication permission
  if (upper.startsWith('ALTER ROLE') && upper.includes('REPLICATION')) {
    return buildCommandComplete('ALTER ROLE')
  }

  // SET TRANSACTION - pglite rejects this if any query ran first (e.g. SET search_path).
  // return synthetic response since pglite is single-connection and doesn't need isolation levels.
  if (upper.startsWith('SET TRANSACTION') || upper.startsWith('SET SESSION')) {
    return buildCommandComplete('SET')
  }

  return null
}

/**
 * start streaming replication changes to the client.
 * this runs indefinitely until the connection is closed.
 */
export async function handleStartReplication(
  query: string,
  writer: ReplicationWriter,
  db: PGlite,
  mutex: Mutex
): Promise<void> {
  log.debug.repl('entering streaming mode')

  // send CopyBothResponse to enter streaming mode
  const copyBoth = new Uint8Array(1 + 4 + 1 + 2)
  copyBoth[0] = 0x57 // 'W' CopyBothResponse
  new DataView(copyBoth.buffer).setInt32(1, 4 + 1 + 2)
  copyBoth[5] = 0 // overall format (0 = text)
  new DataView(copyBoth.buffer).setInt16(6, 0) // 0 columns
  writer.write(copyBoth)

  // resume from where the previous handler left off to avoid
  // replaying already-streamed changes after reconnect
  let lastWatermark = lastStreamedWatermark

  // use cached setup results on reconnect to avoid holding the mutex
  // for seconds doing trigger installation + schema queries. zero-cache
  // disconnects if its proxy queries are blocked too long by the mutex.
  let tableKeyColumns: Map<string, Set<string>>
  let excludedColumns: Map<string, Set<string>>
  let columnTypeOids: Map<string, Map<string, number>>

  if (cachedTableKeyColumns && cachedExcludedColumns && cachedColumnTypeOids) {
    log.debug.repl('reconnect: using cached setup (skipping mutex)')
    tableKeyColumns = cachedTableKeyColumns
    excludedColumns = cachedExcludedColumns
    columnTypeOids = cachedColumnTypeOids
  } else {
    tableKeyColumns = new Map()
    excludedColumns = new Map()
    columnTypeOids = new Map()

    // acquire mutex for all setup queries to avoid conflicting with proxy connections.
    // the change-streamer's initial copy also queries PGlite via the proxy, and
    // direct db.query()/db.exec() calls here bypass the proxy's mutex, causing
    // "already in transaction" errors when they interleave.
    // phase 1: DDL operations (trigger installation) under mutex
    // split into two phases so proxy queries can run between them
    await mutex.acquire()
    let relevantSchemas: string[]
    try {
      // install change tracking triggers on shard schema tables (e.g. chat_0.clients)
      await installTriggersOnShardTables(db)

      // set up LISTEN + install notify triggers in one batch
      const pubName = process.env.ZERO_APP_PUBLICATIONS?.trim()
      let tables: { tablename: string }[]
      if (pubName) {
        const result = await db.query<{ tablename: string }>(
          `SELECT tablename FROM pg_publication_tables
           WHERE pubname = $1 AND schemaname = 'public' AND tablename NOT LIKE '_zero_%'`,
          [pubName]
        )
        tables = result.rows
        if (tables.length === 0) {
          log.proxy(
            `publication "${pubName}" is empty; installing no public notify triggers`
          )
        }
      } else {
        const all = await db.query<{ tablename: string }>(
          `SELECT tablename FROM pg_tables
           WHERE schemaname = 'public'
             AND tablename NOT IN ('migrations', 'changes')
             AND tablename NOT LIKE '_zero_%'`
        )
        tables = all.rows
      }

      // combine notify function creation + trigger installations into single exec
      const ddlParts: string[] = [
        `CREATE OR REPLACE FUNCTION public._zero_notify_change() RETURNS TRIGGER AS $$
        BEGIN
          PERFORM pg_notify('changes', TG_TABLE_NAME);
          RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;`,
      ]
      for (const { tablename } of tables) {
        const quoted = '"' + tablename.replace(/"/g, '""') + '"'
        ddlParts.push(
          `DROP TRIGGER IF EXISTS _zero_notify_trigger ON public.${quoted};
          CREATE TRIGGER _zero_notify_trigger
            AFTER INSERT OR UPDATE OR DELETE ON public.${quoted}
            FOR EACH STATEMENT EXECUTE FUNCTION public._zero_notify_change();`
        )
      }

      // discover shard schemas and install their triggers in same batch
      const shardSchemas = await db.query<{ nspname: string }>(
        `SELECT nspname FROM pg_namespace
       WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'public')
         AND nspname NOT LIKE 'pg_%'
         AND nspname NOT LIKE 'zero_%'
         AND nspname NOT LIKE '_zero_%'
         AND nspname NOT LIKE '%/%'`
      )
      relevantSchemas = ['public', ...shardSchemas.rows.map((r) => r.nspname)]

      const shardClientSchemas = shardSchemas.rows
        .map((r) => r.nspname)
        .filter((s) => s !== 'public')
      if (shardClientSchemas.length > 0) {
        const shardTables = await db.query<{ schemaname: string; tablename: string }>(
          `SELECT schemaname, tablename FROM pg_tables
           WHERE schemaname = ANY($1) AND tablename = 'clients'`,
          [shardClientSchemas]
        )
        for (const { schemaname, tablename } of shardTables.rows) {
          const qs = '"' + schemaname.replace(/"/g, '""') + '"'
          const qt = '"' + tablename.replace(/"/g, '""') + '"'
          ddlParts.push(
            `DROP TRIGGER IF EXISTS _zero_notify_trigger ON ${qs}.${qt};
            CREATE TRIGGER _zero_notify_trigger
              AFTER INSERT OR UPDATE OR DELETE ON ${qs}.${qt}
              FOR EACH STATEMENT EXECUTE FUNCTION public._zero_notify_change();`
          )
        }
        if (shardTables.rows.length > 0) {
          log.debug.proxy(
            `installed notify triggers on ${shardTables.rows.length} shard tables`
          )
        }
      }

      await db.exec(ddlParts.join('\n'))
      if (tables.length > 0) {
        log.proxy(`installed notify triggers on ${tables.length} public table(s)`)
      }
    } finally {
      mutex.release()
    }

    // phase 2: schema introspection (read-only, separate mutex acquisition)
    // releasing between phases lets proxy queries run during the gap
    await mutex.acquire()
    try {
      // combined PK + column introspection in a single query using UNION ALL
      const schemaResult = await db.query<{
        kind: string
        table_schema: string
        table_name: string
        column_name: string
        data_type: string | null
      }>(
        `SELECT 'pk' AS kind, tc.table_schema, tc.table_name, kcu.column_name, NULL AS data_type
         FROM information_schema.table_constraints tc
         JOIN information_schema.key_column_usage kcu
           ON tc.constraint_name = kcu.constraint_name
           AND tc.table_schema = kcu.table_schema
         WHERE tc.constraint_type = 'PRIMARY KEY'
           AND tc.table_schema = ANY($1)
         UNION ALL
         SELECT 'col' AS kind, table_schema, table_name, column_name, data_type
         FROM information_schema.columns
         WHERE table_schema = ANY($1)`,
        [relevantSchemas]
      )

      for (const row of schemaResult.rows) {
        const key = `${row.table_schema}.${row.table_name}`
        if (row.kind === 'pk') {
          let keys = tableKeyColumns.get(key)
          if (!keys) {
            keys = new Set()
            tableKeyColumns.set(key, keys)
          }
          keys.add(row.column_name)
        } else {
          const UNSUPPORTED_TYPES = new Set(['tsvector', 'tsquery', 'USER-DEFINED'])
          const PG_DATA_TYPE_OIDS: Record<string, number> = {
            boolean: 16,
            bytea: 17,
            bigint: 20,
            smallint: 21,
            integer: 23,
            text: 25,
            json: 114,
            real: 700,
            'double precision': 701,
            character: 1042,
            'character varying': 1043,
            date: 1082,
            'time without time zone': 1083,
            'timestamp without time zone': 1114,
            'timestamp with time zone': 1184,
            'time with time zone': 1266,
            numeric: 1700,
            uuid: 2950,
            jsonb: 3802,
          }
          if (row.data_type && UNSUPPORTED_TYPES.has(row.data_type)) {
            let cols = excludedColumns.get(key)
            if (!cols) {
              cols = new Set()
              excludedColumns.set(key, cols)
            }
            cols.add(row.column_name)
          }
          if (row.data_type) {
            const oid = PG_DATA_TYPE_OIDS[row.data_type]
            if (oid !== undefined) {
              let cols = columnTypeOids.get(key)
              if (!cols) {
                cols = new Map()
                columnTypeOids.set(key, cols)
              }
              cols.set(row.column_name, oid)
            }
          }
        }
      }
      log.debug.proxy(`loaded primary keys for ${tableKeyColumns.size} tables`)
      if (excludedColumns.size > 0) {
        log.debug.proxy(
          `excluding unsupported columns: ${[...excludedColumns.entries()].map(([t, c]) => `${t}(${[...c].join(',')})`).join(', ')}`
        )
      }

      // cache for subsequent reconnects
      cachedTableKeyColumns = tableKeyColumns
      cachedExcludedColumns = excludedColumns
      cachedColumnTypeOids = columnTypeOids
    } finally {
      mutex.release()
    }
  }

  // track which tables we've sent RELATION messages for
  const sentRelations = new Set<string>()
  let txCounter = 1

  // event-driven replication: proxy signals changes directly via signalReplicationChange(),
  // pg_notify as secondary signal, polling as final fallback.
  const pollIntervalIdle = 5000
  const batchSize = 50000
  const purgeEveryN = 1
  const shardRescanIntervalMs = 10_000
  let running = true
  let pollsSincePurge = 0
  let tryAcquireFailures = 0
  let lastShardRescan = -shardRescanIntervalMs
  let hasStreamedOnce = false

  // promise-based wakeup mechanism.
  // signalPending captures signals that arrive while the handler is
  // processing (not in waitForWakeup), preventing signal loss.
  let wakeupResolve: (() => void) | null = null
  let signalPending = false
  let lastWakeupTime = 0
  const wakeup = () => {
    signalPending = true
    if (wakeupResolve) {
      lastWakeupTime = performance.now()
      log.debug.repl('signal received, waking up')
      wakeupResolve()
      wakeupResolve = null
    }
  }
  const waitForWakeup = (timeoutMs: number): Promise<boolean> => {
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        wakeupResolve = null
        resolve(false)
      }, timeoutMs)
      wakeupResolve = () => {
        clearTimeout(timer)
        resolve(true)
      }
    })
  }

  // register direct wakeup so the proxy can signal us immediately
  _replicationWakeup = wakeup

  // expose on globalThis so external code (e.g. pglite-pool) can signal
  // without importing from this module (works across separate bundles)
  ;(globalThis as any).__orez_signal_replication = wakeup

  // also set up LISTEN as secondary signal
  let unsubscribe: (() => Promise<void>) | null = null
  try {
    unsubscribe = await db.listen('changes', wakeup)
    log.debug.proxy('replication: listening for changes notifications')
  } catch {
    log.debug.proxy('replication: LISTEN not available')
  }

  const poll = async () => {
    let queryPending = true // query immediately on first iteration
    let idleTimeoutCount = 0

    while (running) {
      // check if the connection or database was closed
      if (writer.closed || db.closed) {
        log.debug.proxy('replication: writer/db closed, exiting poll loop')
        running = false
        break
      }

      try {
        // when no query is pending, wait for a signal or timeout.
        // signals fire instantly when the proxy processes a write,
        // so we only hit the timeout when truly idle.
        if (!queryPending) {
          // check if a signal arrived while we were processing
          if (!signalPending) {
            log.debug.repl(
              `waiting for signal (lastWm=${lastWatermark}, streamed=${hasStreamedOnce})`
            )
            const wasSignaled = await waitForWakeup(pollIntervalIdle)
            if (writer.closed || db.closed) {
              running = false
              break
            }
            if (!wasSignaled) {
              idleTimeoutCount++
              // send keepalive on every timeout
              writer.write(encodeKeepalive(currentLsn, nowMicros(), false))
              log.debug.repl(`idle keepalive (lastWatermark=${lastWatermark})`)
              // re-scan for new shard schemas during idle
              if (performance.now() - lastShardRescan > shardRescanIntervalMs) {
                if (mutex.tryAcquire()) {
                  lastShardRescan = performance.now()
                  try {
                    await installTriggersOnShardTables(db)
                  } finally {
                    mutex.release()
                  }
                }
              }
              // safety poll every ~30s to catch edge cases (6 * 5000ms)
              if (idleTimeoutCount < 6) continue
              idleTimeoutCount = 0
              log.debug.repl('safety poll')
              // fall through to query
            } else {
              idleTimeoutCount = 0
            }
          } else {
            idleTimeoutCount = 0
          }
          signalPending = false
        }
        queryPending = false

        // periodically re-scan for new shard schemas (e.g. chat_0 created by zero-cache)
        if (performance.now() - lastShardRescan > shardRescanIntervalMs) {
          if (mutex.tryAcquire()) {
            lastShardRescan = performance.now()
            try {
              await installTriggersOnShardTables(db)
            } finally {
              mutex.release()
            }
          } else {
            log.debug.repl('shard rescan skipped: mutex busy')
          }
        }

        // try to acquire mutex without blocking proxy connections.
        // post-sync: short backoff since writes signal us directly.
        // pre-sync: yield more generously so zero-cache initial copy can finish.
        log.debug.repl(
          `pre-query: tryAcquire mutex (streamed=${hasStreamedOnce}, fails=${tryAcquireFailures})`
        )
        if (!mutex.tryAcquire()) {
          if (hasStreamedOnce) {
            // post-sync: block immediately. change query is fast (~0.5ms),
            // so holding the mutex briefly doesn't starve proxy connections.
            // avoids 25ms+ backoff delays that cause test flakiness.
            await mutex.acquire()
          } else {
            tryAcquireFailures++
            if (tryAcquireFailures < 10) {
              // pre-sync: yield so zero-cache initial copy can finish
              await waitForWakeup(Math.min(10 * tryAcquireFailures, 100))
              queryPending = true
              continue
            }
            await mutex.acquire()
            tryAcquireFailures = 0
          }
        } else {
          tryAcquireFailures = 0
        }
        let changes: Awaited<ReturnType<typeof getChangesSince>>
        const queryStart = performance.now()
        try {
          try {
            changes = await getChangesSince(db, lastWatermark, batchSize)
          } catch (queryErr: unknown) {
            // pglite is single-connection — if we acquire the mutex between
            // extended protocol messages and the previous query left an aborted
            // transaction, we'll get 25P02. rollback and retry once.
            const code =
              queryErr && typeof queryErr === 'object' && 'code' in queryErr
                ? (queryErr as { code: string }).code
                : ''
            if (code === '25P02') {
              try {
                await db.exec('ROLLBACK')
              } catch {}
              changes = await getChangesSince(db, lastWatermark, batchSize)
            } else {
              throw queryErr
            }
          }
        } finally {
          mutex.release()
        }

        if (changes.length > 0) {
          const queryMs = performance.now() - queryStart
          const signalToQueryMs =
            lastWakeupTime > 0 ? (performance.now() - lastWakeupTime).toFixed(1) : '?'
          // summarize which tables changed
          const tableSummary = [...new Set(changes.map((c) => c.table_name))].join(',')
          log.debug.repl(
            `found ${changes.length} changes [${tableSummary}] (wm ${lastWatermark}→${changes[changes.length - 1].watermark}) query=${queryMs.toFixed(1)}ms signal→query=${signalToQueryMs}ms`
          )
          // filter out shard tables that zero-cache doesn't expect.
          // only `clients` is needed (for .server promise resolution).
          // other shard tables (replicas, mutations) crash zero-cache
          // with "Unknown table" in change-processor.
          const batchEnd = changes[changes.length - 1].watermark
          const preFilterCount = changes.length
          changes = changes.filter((c) => {
            const dot = c.table_name.indexOf('.')
            if (dot === -1) return true
            const schema = c.table_name.substring(0, dot)
            if (schema === 'public') return true
            const table = c.table_name.substring(dot + 1)
            return table === 'clients'
          })
          log.debug.repl(`filter: ${preFilterCount} → ${changes.length} changes`)

          if (changes.length === 0) {
            lastWatermark = batchEnd
            lastStreamedWatermark = batchEnd
            // all changes were filtered out (e.g. shard internal tables).
            // brief wait to avoid tight loop, then recheck.
            await waitForWakeup(200)
            queryPending = true
            continue
          }

          log.debug.repl(`streaming ${changes.length} changes to writer`)
          await streamChanges(
            changes,
            writer,
            sentRelations,
            txCounter++,
            tableKeyColumns,
            excludedColumns,
            columnTypeOids
          )
          lastWatermark = batchEnd
          lastStreamedWatermark = batchEnd
          log.debug.repl(`streamed ok, watermark=${batchEnd}`)
          hasStreamedOnce = true

          // purge consumed changes periodically to free wasm memory
          pollsSincePurge++
          if (pollsSincePurge >= purgeEveryN && mutex.tryAcquire()) {
            pollsSincePurge = 0
            try {
              const purged = await purgeConsumedChanges(db, lastWatermark)
              if (purged > 0) {
                log.debug.proxy(`purged ${purged} consumed changes`)
              }
            } finally {
              mutex.release()
            }
          }

          // got changes - continue immediately to check for more
          queryPending = true
          continue
        }

        // no changes: send keepalive
        const ts = nowMicros()
        writer.write(encodeKeepalive(currentLsn, ts, false))
        log.debug.repl(`idle (lastWatermark=${lastWatermark})`)
        // next iteration will wait for signal at the top
      } catch (err: unknown) {
        const msg = err instanceof Error ? err.message : String(err)
        log.repl(`replication poll error: ${msg}`)
        if (
          msg.includes('closed') ||
          msg.includes('destroyed') ||
          msg.includes('ECONNRESET') ||
          msg.includes('EPIPE')
        ) {
          running = false
          break
        }
        await new Promise((resolve) => setTimeout(resolve, 1000))
      }
    }
  }

  log.debug.repl(`starting poll (lastWatermark=${lastWatermark})`)
  try {
    await poll()
  } finally {
    // only clear if still pointing to our wakeup (a new handler may have replaced it)
    if (_replicationWakeup === wakeup) {
      _replicationWakeup = null
    }
    if (unsubscribe) {
      await unsubscribe().catch(() => {})
    }
  }
  log.repl('poll loop exited')
}

// cache column info per table to avoid per-change allocation
const cachedColumns = new Map<string, ReturnType<typeof inferColumns>>()

async function streamChanges(
  changes: ChangeRecord[],
  writer: ReplicationWriter,
  sentRelations: Set<string>,
  txId: number,
  tableKeyColumns: Map<string, Set<string>>,
  excludedColumns: Map<string, Set<string>>,
  columnTypeOids: Map<string, Map<string, number>>
): Promise<void> {
  const ts = nowMicros()
  const lsn = nextLsn()

  // collect all encoded messages into a list, then batch-write
  // to minimize syscalls (each writer.write → socket.write is a syscall)
  const messages: Uint8Array[] = []

  // BEGIN
  messages.push(encodeWrappedChange(lsn, lsn, ts, encodeBegin(lsn, ts, txId)))

  for (const change of changes) {
    // parse schema-qualified name (schema.table or bare table)
    const dot = change.table_name.indexOf('.')
    const schema = dot !== -1 ? change.table_name.substring(0, dot) : 'public'
    const tableName =
      dot !== -1 ? change.table_name.substring(dot + 1) : change.table_name
    const qualifiedKey = `${schema}.${tableName}`

    const tableOid = getTableOid(qualifiedKey)
    const excluded = excludedColumns.get(qualifiedKey)

    // filter out unsupported columns from row data
    let rowData = change.row_data
    let oldData = change.old_data
    if (excluded && excluded.size > 0) {
      if (rowData) {
        rowData = Object.fromEntries(
          Object.entries(rowData).filter(([k]) => !excluded.has(k))
        )
      }
      if (oldData) {
        oldData = Object.fromEntries(
          Object.entries(oldData).filter(([k]) => !excluded.has(k))
        )
      }
    }

    // zero-cache expects specific camel-cased keys in shard clients rows
    if (schema !== 'public' && tableName === 'clients') {
      rowData = normalizeShardClientsRow(rowData)
      oldData = normalizeShardClientsRow(oldData)
    }

    const row = rowData || oldData
    if (!row) continue

    // use cached columns or build and cache them
    let columns = cachedColumns.get(qualifiedKey)
    if (!columns) {
      const keySet = tableKeyColumns.get(qualifiedKey)
      const typeOids = columnTypeOids.get(qualifiedKey)
      columns = inferColumns(row).map((col) => ({
        ...col,
        typeOid: typeOids?.get(col.name) ?? col.typeOid,
        isKey: keySet?.has(col.name) ?? false,
      }))
      cachedColumns.set(qualifiedKey, columns)
    }

    // send RELATION if not yet sent
    if (!sentRelations.has(qualifiedKey)) {
      const relMsg = encodeRelation(tableOid, schema, tableName, 0x64, columns)
      messages.push(encodeWrappedChange(lsn, lsn, ts, relMsg))
      sentRelations.add(qualifiedKey)
    }

    // encode the change
    let changeMsg: Uint8Array | null = null
    switch (change.op) {
      case 'INSERT':
        if (!rowData) continue
        changeMsg = encodeInsert(tableOid, rowData, columns)
        break
      case 'UPDATE':
        if (!rowData) continue
        changeMsg = encodeUpdate(tableOid, rowData, oldData, columns)
        break
      case 'DELETE':
        if (!oldData) continue
        changeMsg = encodeDelete(tableOid, oldData, columns)
        break
      default:
        continue
    }

    messages.push(encodeWrappedChange(lsn, lsn, ts, changeMsg))
  }

  // COMMIT
  const endLsn = nextLsn()
  messages.push(encodeWrappedChange(endLsn, endLsn, ts, encodeCommit(0, lsn, endLsn, ts)))

  // write messages individually — works for both TCP sockets and in-process
  // pipes (browser pipe handler parses one message per write() call)
  let totalSize = 0
  for (const msg of messages) totalSize += msg.length
  log.debug.repl(
    `streaming ${messages.length} wal messages (${totalSize} bytes, txId=${txId})`
  )
  for (const msg of messages) {
    writer.write(msg)
  }
}

function normalizeShardClientsRow(
  row: Record<string, unknown> | null
): Record<string, unknown> | null {
  if (!row) return row
  const out: Record<string, unknown> = { ...row }
  if (out.clientGroupID === undefined && out.clientgroupid !== undefined) {
    out.clientGroupID = out.clientgroupid
  }
  if (out.clientID === undefined && out.clientid !== undefined) {
    out.clientID = out.clientid
  }
  if (out.lastMutationID === undefined && out.lastmutationid !== undefined) {
    out.lastMutationID = out.lastmutationid
  }
  return out
}

export { buildErrorResponse }
