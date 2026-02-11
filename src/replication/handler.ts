/**
 * replication protocol handler.
 *
 * intercepts replication-mode queries (IDENTIFY_SYSTEM, CREATE_REPLICATION_SLOT,
 * START_REPLICATION) and returns fake responses that make zero-cache believe
 * it's talking to a real postgres with logical replication.
 */

import { log } from '../log.js'
import {
  getChangesSince,
  getCurrentWatermark,
  purgeConsumedChanges,
  installTriggersOnShardTables,
  ensureChangeTrackingOnAllTables,
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
  wrapXLogData,
  wrapCopyData,
  getTableOid,
  inferColumns,
  type ColumnInfo,
} from './pgoutput-encoder.js'

import type { Mutex } from '../mutex.js'
import type { PGlite } from '@electric-sql/pglite'

export interface ReplicationWriter {
  write(data: Uint8Array): void
}

// current lsn counter
let currentLsn = 0x1000000n
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
  const encoder = new TextEncoder()

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
  const encoder = new TextEncoder()
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
  const encoder = new TextEncoder()
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

    // persist slot so pg_replication_slots queries find it
    await db.query(
      `INSERT INTO public._zero_replication_slots (slot_name, restart_lsn, confirmed_flush_lsn)
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
      await db.query(`DELETE FROM public._zero_replication_slots WHERE slot_name = $1`, [
        slotName,
      ])
    }
    return buildCommandComplete('DROP_REPLICATION_SLOT')
  }

  // wal_level check via simple query
  if (upper.includes('WAL_LEVEL') && upper.includes('CURRENT_SETTING')) {
    return buildSimpleResponse(['walLevel', 'version'], ['logical', '160004'])
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
  log.debug.proxy('replication: entering streaming mode')

  // send CopyBothResponse to enter streaming mode
  const copyBoth = new Uint8Array(1 + 4 + 1 + 2)
  copyBoth[0] = 0x57 // 'W' CopyBothResponse
  new DataView(copyBoth.buffer).setInt32(1, 4 + 1 + 2)
  copyBoth[5] = 0 // overall format (0 = text)
  new DataView(copyBoth.buffer).setInt16(6, 0) // 0 columns
  writer.write(copyBoth)

  let lastWatermark = 0

  // declared outside mutex block so they're accessible in the poll loop
  const tableKeyColumns = new Map<string, Set<string>>()
  const excludedColumns = new Map<string, Set<string>>()
  const columnTypeOids = new Map<string, Map<string, number>>()

  // acquire mutex for all setup queries to avoid conflicting with proxy connections.
  // the change-streamer's initial copy also queries PGlite via the proxy, and
  // direct db.query()/db.exec() calls here bypass the proxy's mutex, causing
  // "already in transaction" errors when they interleave.
  await mutex.acquire()
  try {
    // install change tracking triggers on any tables created after startup
    await ensureChangeTrackingOnAllTables(db)

    // install change tracking triggers on shard schema tables (e.g. chat_0.clients)
    // these track zero-cache's lastMutationID for .server promise resolution
    await installTriggersOnShardTables(db)

    // set up LISTEN for real-time change notifications
    await db.exec(`
    CREATE OR REPLACE FUNCTION public._zero_notify_change() RETURNS TRIGGER AS $$
    BEGIN
      PERFORM pg_notify('_zero_changes', TG_TABLE_NAME);
      RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;
  `)

    // install notify trigger on tracked tables (use configured publication if available)
    const pubName = process.env.ZERO_APP_PUBLICATIONS
    let tables: { tablename: string }[]
    if (pubName) {
      const result = await db.query<{ tablename: string }>(
        `SELECT tablename FROM pg_publication_tables
       WHERE pubname = $1 AND schemaname = 'public' AND tablename NOT LIKE '_zero_%'`,
        [pubName]
      )
      tables = result.rows
    } else {
      const result = await db.query<{ tablename: string }>(
        `SELECT tablename FROM pg_tables
       WHERE schemaname = 'public'
         AND tablename NOT IN ('migrations', '_zero_changes')
         AND tablename NOT LIKE '_zero_%'`
      )
      tables = result.rows
    }

    for (const { tablename } of tables) {
      const quoted = '"' + tablename.replace(/"/g, '""') + '"'
      await db.exec(`
      DROP TRIGGER IF EXISTS _zero_notify_trigger ON public.${quoted};
      CREATE TRIGGER _zero_notify_trigger
        AFTER INSERT OR UPDATE OR DELETE ON public.${quoted}
        FOR EACH STATEMENT EXECUTE FUNCTION public._zero_notify_change();
    `)
    }

    // discover shard schemas (e.g. chat_0) and install NOTIFY triggers
    const shardSchemas = await db.query<{ nspname: string }>(
      `SELECT nspname FROM pg_namespace
     WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'public')
       AND nspname NOT LIKE 'pg_%'
       AND nspname NOT LIKE 'zero_%'
       AND nspname NOT LIKE '_zero_%'
       AND nspname NOT LIKE '%/%'`
    )
    const relevantSchemas = ['public', ...shardSchemas.rows.map((r) => r.nspname)]

    for (const schema of relevantSchemas) {
      if (schema === 'public') continue
      const shardTables = await db.query<{ tablename: string }>(
        `SELECT tablename FROM pg_tables WHERE schemaname = $1`,
        [schema]
      )
      for (const { tablename } of shardTables.rows) {
        const quotedSchema = '"' + schema.replace(/"/g, '""') + '"'
        const quotedTable = '"' + tablename.replace(/"/g, '""') + '"'
        await db.exec(`
        DROP TRIGGER IF EXISTS _zero_notify_trigger ON ${quotedSchema}.${quotedTable};
        CREATE TRIGGER _zero_notify_trigger
          AFTER INSERT OR UPDATE OR DELETE ON ${quotedSchema}.${quotedTable}
          FOR EACH STATEMENT EXECUTE FUNCTION public._zero_notify_change();
      `)
      }
      if (shardTables.rows.length > 0) {
        log.debug.proxy(
          `installed notify triggers on ${shardTables.rows.length} tables in schema "${schema}"`
        )
      }
    }

    // build primary key lookup for all relevant schemas
    for (const schema of relevantSchemas) {
      const pkResult = await db.query<{ table_name: string; column_name: string }>(
        `SELECT tc.table_name, kcu.column_name
       FROM information_schema.table_constraints tc
       JOIN information_schema.key_column_usage kcu
         ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
       WHERE tc.constraint_type = 'PRIMARY KEY'
         AND tc.table_schema = $1`,
        [schema]
      )
      for (const { table_name, column_name } of pkResult.rows) {
        const key = `${schema}.${table_name}`
        let keys = tableKeyColumns.get(key)
        if (!keys) {
          keys = new Set()
          tableKeyColumns.set(key, keys)
        }
        keys.add(column_name)
      }
    }
    log.debug.proxy(`loaded primary keys for ${tableKeyColumns.size} tables`)

    // build excluded columns lookup (types zero-cache can't handle)
    // also build column type OID map so RELATION messages carry correct postgres type OIDs.
    // zero-cache uses these to select value parsers (e.g. timestamp → number via timestampToFpMillis).
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
    for (const schema of relevantSchemas) {
      const colResult = await db.query<{
        table_name: string
        column_name: string
        data_type: string
      }>(
        `SELECT table_name, column_name, data_type
       FROM information_schema.columns
       WHERE table_schema = $1`,
        [schema]
      )
      for (const { table_name, column_name, data_type } of colResult.rows) {
        const key = `${schema}.${table_name}`
        if (UNSUPPORTED_TYPES.has(data_type)) {
          let cols = excludedColumns.get(key)
          if (!cols) {
            cols = new Set()
            excludedColumns.set(key, cols)
          }
          cols.add(column_name)
        }
        const oid = PG_DATA_TYPE_OIDS[data_type]
        if (oid !== undefined) {
          let cols = columnTypeOids.get(key)
          if (!cols) {
            cols = new Map()
            columnTypeOids.set(key, cols)
          }
          cols.set(column_name, oid)
        }
      }
    }
    if (excludedColumns.size > 0) {
      log.debug.proxy(
        `excluding unsupported columns: ${[...excludedColumns.entries()].map(([t, c]) => `${t}(${[...c].join(',')})`).join(', ')}`
      )
    }
  } finally {
    mutex.release()
  }

  // track which tables we've sent RELATION messages for
  const sentRelations = new Set<string>()
  let txCounter = 1

  // polling + notification loop
  // adaptive: poll fast when catching up, slow when idle
  const pollIntervalIdle = 500
  const pollIntervalCatchUp = 20
  const batchSize = 2000
  const purgeEveryN = 10
  let running = true
  let pollsSincePurge = 0

  const poll = async () => {
    while (running) {
      try {
        // acquire mutex to avoid conflicting with proxy connections
        await mutex.acquire()
        let changes: Awaited<ReturnType<typeof getChangesSince>>
        try {
          changes = await getChangesSince(db, lastWatermark, batchSize)
        } finally {
          mutex.release()
        }

        if (changes.length > 0) {
          await streamChanges(
            changes,
            writer,
            sentRelations,
            txCounter++,
            tableKeyColumns,
            excludedColumns,
            columnTypeOids
          )
          lastWatermark = changes[changes.length - 1].watermark

          // purge consumed changes periodically to free wasm memory
          pollsSincePurge++
          if (pollsSincePurge >= purgeEveryN) {
            pollsSincePurge = 0
            await mutex.acquire()
            try {
              const purged = await purgeConsumedChanges(db, lastWatermark)
              if (purged > 0) {
                log.debug.proxy(`purged ${purged} consumed changes`)
              }
            } finally {
              mutex.release()
            }
          }
        }

        // send keepalive
        const ts = nowMicros()
        writer.write(encodeKeepalive(currentLsn, ts, false))

        // if we got a full batch, there's likely more - poll fast
        const delay = changes.length >= batchSize ? pollIntervalCatchUp : pollIntervalIdle
        await new Promise((resolve) => setTimeout(resolve, delay))
      } catch (err: unknown) {
        const msg = err instanceof Error ? err.message : String(err)
        log.debug.proxy(`replication poll error: ${msg}`)
        if (msg.includes('closed') || msg.includes('destroyed')) {
          running = false
          break
        }
        await new Promise((resolve) => setTimeout(resolve, 1000))
      }
    }
  }

  log.debug.proxy('replication: starting poll loop')
  await poll()
  log.debug.proxy('replication: poll loop exited')
}

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

  // BEGIN
  const beginMsg = wrapXLogData(lsn, lsn, ts, encodeBegin(lsn, ts, txId))
  writer.write(wrapCopyData(beginMsg))

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

    const row = rowData || oldData
    if (!row) continue

    const keySet = tableKeyColumns.get(qualifiedKey)
    const typeOids = columnTypeOids.get(qualifiedKey)
    const columns = inferColumns(row).map((col) => ({
      ...col,
      typeOid: typeOids?.get(col.name) ?? col.typeOid,
      isKey: keySet?.has(col.name) ?? false,
    }))

    // send RELATION if not yet sent
    if (!sentRelations.has(qualifiedKey)) {
      const relMsg = encodeRelation(tableOid, schema, tableName, 0x64, columns)
      writer.write(wrapCopyData(wrapXLogData(lsn, lsn, ts, relMsg)))
      sentRelations.add(qualifiedKey)
    }

    // send the change
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

    writer.write(wrapCopyData(wrapXLogData(lsn, lsn, ts, changeMsg)))
  }

  // COMMIT
  const endLsn = nextLsn()
  const commitMsg = wrapXLogData(endLsn, endLsn, ts, encodeCommit(0, lsn, endLsn, ts))
  writer.write(wrapCopyData(commitMsg))
}

export { buildErrorResponse }
