import { log } from '../log.js'

import type { PGlite } from '@electric-sql/pglite'

export interface ChangeRecord {
  id: number
  watermark: number
  table_name: string
  op: 'INSERT' | 'UPDATE' | 'DELETE'
  row_data: Record<string, unknown> | null
  old_data: Record<string, unknown> | null
  changed_at: string
}

export async function installChangeTracking(db: PGlite): Promise<void> {
  // create changes table and watermark sequence
  await db.exec(`
    CREATE SEQUENCE IF NOT EXISTS public._zero_watermark;

    CREATE TABLE IF NOT EXISTS public._zero_changes (
      id BIGSERIAL PRIMARY KEY,
      watermark BIGINT NOT NULL DEFAULT nextval('public._zero_watermark'),
      table_name TEXT NOT NULL,
      op TEXT NOT NULL,
      row_data JSONB,
      old_data JSONB,
      changed_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS _zero_changes_watermark_idx ON public._zero_changes (watermark);

    CREATE TABLE IF NOT EXISTS public._zero_replication_slots (
      slot_name TEXT PRIMARY KEY,
      restart_lsn TEXT NOT NULL DEFAULT '0/1000000',
      confirmed_flush_lsn TEXT NOT NULL DEFAULT '0/1000000',
      wal_status TEXT NOT NULL DEFAULT 'reserved',
      plugin TEXT NOT NULL DEFAULT 'pgoutput',
      slot_type TEXT NOT NULL DEFAULT 'logical',
      active BOOLEAN NOT NULL DEFAULT false,
      active_pid INTEGER DEFAULT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `)

  // create trigger function
  await db.exec(`
    CREATE OR REPLACE FUNCTION public._zero_track_change() RETURNS TRIGGER AS $$
    DECLARE
      qualified_name TEXT;
    BEGIN
      qualified_name := TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME;
      IF TG_OP = 'DELETE' THEN
        INSERT INTO public._zero_changes (table_name, op, old_data)
        VALUES (qualified_name, 'DELETE', row_to_json(OLD)::jsonb);
        RETURN OLD;
      ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO public._zero_changes (table_name, op, row_data, old_data)
        VALUES (qualified_name, 'UPDATE', row_to_json(NEW)::jsonb, row_to_json(OLD)::jsonb);
        RETURN NEW;
      ELSIF TG_OP = 'INSERT' THEN
        INSERT INTO public._zero_changes (table_name, op, row_data)
        VALUES (qualified_name, 'INSERT', row_to_json(NEW)::jsonb);
        RETURN NEW;
      END IF;
      RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;
  `)

  // install triggers on all public tables
  await installTriggersOnAllTables(db)
}

function quoteIdent(name: string): string {
  return '"' + name.replace(/"/g, '""') + '"'
}

async function installTriggersOnAllTables(db: PGlite): Promise<void> {
  // use the configured app publication to determine which tables to track.
  // this avoids streaming changes for private tables (user, account, session, etc.)
  // that zero-cache doesn't know about.
  const pubName = process.env.ZERO_APP_PUBLICATIONS
  let tables: { tablename: string }[]

  if (pubName) {
    const result = await db.query<{ tablename: string }>(
      `SELECT tablename FROM pg_publication_tables
       WHERE pubname = $1
         AND schemaname = 'public'
         AND tablename NOT LIKE '_zero_%'`,
      [pubName]
    )
    tables = result.rows
    log.debug.pglite(`using publication "${pubName}" (${tables.length} tables)`)
    if (tables.length === 0) {
      log.debug.pglite(
        `publication "${pubName}" has no tables yet (will be populated by migrations)`
      )
    }

    // drop stale triggers from tables NOT in the publication
    // (these may exist from a prior install before the publication was created)
    const publishedSet = new Set(tables.map((t) => t.tablename))
    const allTriggered = await db.query<{ event_object_table: string }>(
      `SELECT DISTINCT event_object_table FROM information_schema.triggers
       WHERE trigger_name = '_zero_change_trigger'
         AND event_object_schema = 'public'`
    )
    for (const { event_object_table } of allTriggered.rows) {
      if (!publishedSet.has(event_object_table)) {
        const quoted = quoteIdent(event_object_table)
        await db.exec(`DROP TRIGGER IF EXISTS _zero_change_trigger ON public.${quoted}`)
        log.debug.pglite(
          `removed stale trigger from non-published table: ${event_object_table}`
        )
      }
    }
  } else {
    const result = await db.query<{ tablename: string }>(
      `SELECT tablename FROM pg_tables
       WHERE schemaname = 'public'
         AND tablename NOT IN ('migrations', '_zero_changes')
         AND tablename NOT LIKE '_zero_%'`
    )
    tables = result.rows
  }

  let count = 0
  for (const { tablename } of tables) {
    const quoted = quoteIdent(tablename)
    await db.exec(`
      DROP TRIGGER IF EXISTS _zero_change_trigger ON public.${quoted};
      CREATE TRIGGER _zero_change_trigger
        AFTER INSERT OR UPDATE OR DELETE ON public.${quoted}
        FOR EACH ROW EXECUTE FUNCTION public._zero_track_change();
    `)
    count++
  }

  log.debug.pglite(`installed change tracking triggers on ${count} tables`)
}

/**
 * install change tracking triggers on tables in shard schemas.
 * zero-cache creates shard schemas (e.g. chat_0) with clients/mutations
 * tables that track mutation confirmations. these must be replicated
 * for .server promises to resolve.
 */
export async function installTriggersOnShardTables(db: PGlite): Promise<void> {
  const result = await db.query<{ nspname: string }>(
    `SELECT nspname FROM pg_namespace
     WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'public')
       AND nspname NOT LIKE 'pg_%'
       AND nspname NOT LIKE 'zero_%'
       AND nspname NOT LIKE '_zero_%'
       AND nspname NOT LIKE '%/%'`
  )

  if (result.rows.length === 0) return

  // only track `clients` — that's the table zero-cache expects in the
  // replication stream (needed for .server promise resolution). other shard
  // tables like `replicas` are zero-cache internal state and streaming them
  // back causes "Unknown table" crashes in zero-cache's change-processor.
  let count = 0
  for (const { nspname } of result.rows) {
    // remove stale triggers from non-clients tables (from previous versions)
    const stale = await db.query<{ event_object_table: string }>(
      `SELECT DISTINCT event_object_table FROM information_schema.triggers
       WHERE trigger_name = '_zero_change_trigger'
         AND event_object_schema = $1
         AND event_object_table != 'clients'`,
      [nspname]
    )
    for (const { event_object_table } of stale.rows) {
      const qs = quoteIdent(nspname)
      const qt = quoteIdent(event_object_table)
      await db.exec(`DROP TRIGGER IF EXISTS _zero_change_trigger ON ${qs}.${qt}`)
      log.debug.pglite(
        `removed stale shard trigger from ${nspname}.${event_object_table}`
      )
    }

    const tables = await db.query<{ tablename: string }>(
      `SELECT tablename FROM pg_tables WHERE schemaname = $1 AND tablename = 'clients'`,
      [nspname]
    )

    for (const { tablename } of tables.rows) {
      const quotedSchema = quoteIdent(nspname)
      const quotedTable = quoteIdent(tablename)
      await db.exec(`
        DROP TRIGGER IF EXISTS _zero_change_trigger ON ${quotedSchema}.${quotedTable};
        CREATE TRIGGER _zero_change_trigger
          AFTER INSERT OR UPDATE OR DELETE ON ${quotedSchema}.${quotedTable}
          FOR EACH ROW EXECUTE FUNCTION public._zero_track_change();
      `)
      count++
    }
  }

  if (count > 0) {
    log.debug.pglite(`installed change tracking on ${count} shard tables`)
  }
}

export async function getChangesSince(
  db: PGlite,
  watermark: number,
  limit = 1000
): Promise<ChangeRecord[]> {
  const result = await db.query<ChangeRecord>(
    'SELECT * FROM public._zero_changes WHERE watermark > $1 ORDER BY watermark LIMIT $2',
    [watermark, limit]
  )
  return result.rows
}

export async function purgeConsumedChanges(
  db: PGlite,
  watermark: number
): Promise<number> {
  const result = await db.query<{ count: string }>(
    'WITH deleted AS (DELETE FROM public._zero_changes WHERE watermark <= $1 RETURNING 1) SELECT count(*)::text AS count FROM deleted',
    [watermark]
  )
  return Number(result.rows[0]?.count || 0)
}

export async function getCurrentWatermark(db: PGlite): Promise<number> {
  const result = await db.query<{ last_value: string; is_called: boolean }>(
    'SELECT last_value, is_called FROM public._zero_watermark'
  )
  const { last_value, is_called } = result.rows[0]
  if (!is_called) return 0
  return Number(last_value)
}
