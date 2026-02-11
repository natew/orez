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

  // auto-install change tracking on tables created after startup (e.g. via restore
  // or wire protocol). uses a DDL event trigger that fires on CREATE TABLE.
  await db.exec(`
    CREATE OR REPLACE FUNCTION public._zero_auto_track() RETURNS event_trigger AS $$
    DECLARE
      obj record;
    BEGIN
      FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands()
                 WHERE command_tag = 'CREATE TABLE'
      LOOP
        IF obj.schema_name = 'public'
           AND obj.object_identity NOT LIKE '%._zero_%'
           AND obj.object_identity NOT LIKE '%.migrations'
        THEN
          EXECUTE format(
            'CREATE TRIGGER _zero_change_trigger AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION public._zero_track_change()',
            obj.object_identity
          );
        END IF;
      END LOOP;
    END;
    $$ LANGUAGE plpgsql;

    DROP EVENT TRIGGER IF EXISTS _zero_auto_track_trigger;
    CREATE EVENT TRIGGER _zero_auto_track_trigger ON ddl_command_end
      WHEN TAG IN ('CREATE TABLE')
      EXECUTE FUNCTION public._zero_auto_track();
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
 * re-install change tracking triggers on any public tables that don't have them.
 * catches tables created between startup and replication start.
 */
export async function ensureChangeTrackingOnAllTables(db: PGlite): Promise<void> {
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
  } else {
    const result = await db.query<{ tablename: string }>(
      `SELECT tablename FROM pg_tables
       WHERE schemaname = 'public'
         AND tablename NOT IN ('migrations', '_zero_changes')
         AND tablename NOT LIKE '_zero_%'`
    )
    tables = result.rows
  }

  // find tables missing the change trigger
  const triggered = await db.query<{ event_object_table: string }>(
    `SELECT DISTINCT event_object_table FROM information_schema.triggers
     WHERE trigger_name = '_zero_change_trigger'
       AND event_object_schema = 'public'`
  )
  const hasTracker = new Set(triggered.rows.map((r) => r.event_object_table))

  let count = 0
  for (const { tablename } of tables) {
    if (hasTracker.has(tablename)) continue
    const quoted = quoteIdent(tablename)
    await db.exec(`
      CREATE TRIGGER _zero_change_trigger
        AFTER INSERT OR UPDATE OR DELETE ON public.${quoted}
        FOR EACH ROW EXECUTE FUNCTION public._zero_track_change();
    `)
    count++
  }

  if (count > 0) {
    log.debug.pglite(`installed change tracking on ${count} new tables`)
  }
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

  let count = 0
  for (const { nspname } of result.rows) {
    const tables = await db.query<{ tablename: string }>(
      `SELECT tablename FROM pg_tables WHERE schemaname = $1`,
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

export async function getCurrentWatermark(db: PGlite): Promise<number> {
  const result = await db.query<{ last_value: string; is_called: boolean }>(
    'SELECT last_value, is_called FROM public._zero_watermark'
  )
  const { last_value, is_called } = result.rows[0]
  if (!is_called) return 0
  return Number(last_value)
}
