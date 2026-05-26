/**
 * Catalog seed — populates `_orez_catalog__*` tables in a DO SQLite database.
 *
 * The catalog pass rewrites `pg_catalog.pg_class`, `information_schema.columns`,
 * etc. references to point at flat `_orez_catalog__*` table names. Here we
 * create those tables and fill them with rows synthesized from SQLite's
 * introspection (`sqlite_master`, `PRAGMA table_info(...)`).
 *
 * Tables seeded (every entry in the catalog pass's recognized-name set —
 * keeping them in sync ensures "no such table" errors don't surface a
 * confusing `_orez_catalog__pg_index`-style internal name):
 *
 *   pg_namespace                 public + pg_catalog + information_schema
 *   pg_type                      ~20 built-in OIDs
 *   pg_class                     one row per user table
 *   pg_attribute                 one row per column
 *   pg_attrdef                   per-column defaults
 *   pg_constraint                empty stub (queryable, no rows)
 *   pg_index                     empty stub (sqlite_master indexes could be
 *                                synthesized here in a follow-up)
 *   pg_proc                      empty stub
 *   pg_trigger                   empty stub
 *   pg_inherits                  empty stub
 *   pg_depend, pg_description,
 *   pg_enum, pg_roles, pg_user,
 *   pg_settings                  empty stubs (queryable)
 *   pg_publication               one row per supplied publication name
 *   pg_publication_tables        cross-product when publications set
 *   pg_publication_rel           empty stub
 *   pg_replication_slots         empty (orez has its own change tracker)
 *   pg_stat_replication          empty stub
 *   pg_subscription              empty stub
 *   pg_extension                 empty stub
 *   pg_sequence                  empty stub
 *   pg_views                     empty stub
 *   pg_tables                    one row per user table
 *   pg_collation, pg_am,
 *   pg_operator, pg_cast,
 *   pg_language, pg_statistic,
 *   pg_locks                     empty stubs
 *   information_schema_columns   one row per column
 *   information_schema_tables    one row per user table
 *
 * Idempotent and transactional — wrapped in BEGIN/COMMIT so readers never
 * observe a half-built catalog mid-rebuild. Safe to call on every DO init.
 *
 * Used in tests via `buildCatalogTables(db, opts?)`. In the DO runtime, the
 * same function runs against `ctx.storage.sql` (better-sqlite3-compatible).
 */

export interface SqliteLike {
  exec(sql: string): unknown
  prepare(sql: string): {
    all(...args: unknown[]): unknown[]
    run(...args: unknown[]): unknown
  }
}

export interface SeedOptions {
  /** publication names to expose in pg_publication. */
  publications?: readonly string[]
}

const NAMESPACE_OID_PUBLIC = 2200
const NAMESPACE_OID_PG_CATALOG = 11
const NAMESPACE_OID_INFO_SCHEMA = 99
const TABLE_OID_BASE = 50_000

function hashName(name: string, base: number): number {
  let h = 0
  for (let i = 0; i < name.length; i++) {
    h = ((h * 33) ^ name.charCodeAt(i)) >>> 0
  }
  return base + (h % 1_000_000)
}

/** PG type OID lookup for the small set of types we expose. */
const PG_TYPE_OIDS: Record<string, number> = {
  bool: 16,
  bytea: 17,
  int8: 20,
  int2: 21,
  int4: 23,
  text: 25,
  oid: 26,
  name: 19,
  float4: 700,
  float8: 701,
  varchar: 1043,
  date: 1082,
  time: 1083,
  timestamp: 1114,
  timestamptz: 1184,
  interval: 1186,
  numeric: 1700,
  json: 114,
  jsonb: 3802,
  uuid: 2950,
  regclass: 2205,
  regtype: 2206,
  regproc: 24,
}

/** Map SQLite affinity → PG type name we'll report (best-effort). */
function sqliteToPgType(decltype: string | null): string {
  if (!decltype) return 'text'
  const lower = decltype.toLowerCase()
  if (lower.includes('int')) return 'int4'
  if (lower.includes('char') || lower.includes('clob') || lower === 'text') return 'text'
  if (lower.includes('real') || lower.includes('floa') || lower.includes('doub'))
    return 'float8'
  if (lower.includes('blob')) return 'bytea'
  if (lower === 'numeric' || lower.includes('dec')) return 'numeric'
  return 'text'
}

interface PragmaCol {
  cid: number
  name: string
  type: string | null
  notnull: number
  dflt_value: string | null
  pk: number
}

/**
 * Seed catalog tables. Wrapped in a transaction so readers never observe a
 * partial rebuild. Tables created as `_orez_catalog__pg_class` etc. The
 * catalog pass rewrites `pg_catalog.pg_class` → `_orez_catalog__pg_class`.
 */
export function buildCatalogTables(db: SqliteLike, opts: SeedOptions = {}): void {
  // De-dupe publication names so we don't violate the UNIQUE constraint
  const publications = [...new Set(opts.publications ?? [])]

  db.exec('BEGIN IMMEDIATE;')
  try {
    seedPgNamespace(db)
    seedPgType(db)
    const tables = readUserTables(db)
    seedPgClass(db, tables)
    seedPgAttributeAndDefaults(db, tables)
    seedPgPublication(db, publications, tables)
    seedSimpleStubs(db)
    seedInformationSchema(db, tables)
    db.exec('COMMIT;')
  } catch (err) {
    try {
      db.exec('ROLLBACK;')
    } catch {}
    throw err
  }
}

function readUserTables(db: SqliteLike): { name: string; type: string }[] {
  return db
    .prepare(
      "SELECT name, type FROM sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_orez_catalog__%'"
    )
    .all() as { name: string; type: string }[]
}

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`
}

function readColumns(db: SqliteLike, table: string): PragmaCol[] {
  return db.prepare(`PRAGMA table_info(${quoteIdent(table)})`).all() as PragmaCol[]
}

function seedPgNamespace(db: SqliteLike): void {
  db.exec(`
    DROP TABLE IF EXISTS _orez_catalog__pg_namespace;
    CREATE TABLE _orez_catalog__pg_namespace (
      oid INTEGER PRIMARY KEY,
      nspname TEXT NOT NULL,
      nspowner INTEGER NOT NULL DEFAULT 10,
      nspacl TEXT
    );
  `)
  const insert = db.prepare(
    'INSERT INTO _orez_catalog__pg_namespace (oid, nspname) VALUES (?, ?)'
  )
  insert.run(NAMESPACE_OID_PUBLIC, 'public')
  insert.run(NAMESPACE_OID_PG_CATALOG, 'pg_catalog')
  insert.run(NAMESPACE_OID_INFO_SCHEMA, 'information_schema')
}

function seedPgType(db: SqliteLike): void {
  db.exec(`
    DROP TABLE IF EXISTS _orez_catalog__pg_type;
    CREATE TABLE _orez_catalog__pg_type (
      oid INTEGER PRIMARY KEY,
      typname TEXT NOT NULL,
      typnamespace INTEGER NOT NULL DEFAULT ${NAMESPACE_OID_PG_CATALOG},
      typlen INTEGER NOT NULL DEFAULT -1,
      typtype TEXT NOT NULL DEFAULT 'b',
      typbasetype INTEGER NOT NULL DEFAULT 0,
      typelem INTEGER NOT NULL DEFAULT 0,
      typcategory TEXT,
      typnotnull INTEGER NOT NULL DEFAULT 0
    );
  `)
  const insert = db.prepare(
    'INSERT INTO _orez_catalog__pg_type (oid, typname) VALUES (?, ?)'
  )
  for (const [name, oid] of Object.entries(PG_TYPE_OIDS)) insert.run(oid, name)
}

function seedPgClass(db: SqliteLike, tables: { name: string; type: string }[]): void {
  db.exec(`
    DROP TABLE IF EXISTS _orez_catalog__pg_class;
    CREATE TABLE _orez_catalog__pg_class (
      oid INTEGER PRIMARY KEY,
      relname TEXT NOT NULL,
      relnamespace INTEGER NOT NULL DEFAULT ${NAMESPACE_OID_PUBLIC},
      relkind TEXT NOT NULL DEFAULT 'r',
      relowner INTEGER NOT NULL DEFAULT 10,
      relhasindex INTEGER NOT NULL DEFAULT 0,
      relhasrules INTEGER NOT NULL DEFAULT 0,
      relhastriggers INTEGER NOT NULL DEFAULT 0,
      relhassubclass INTEGER NOT NULL DEFAULT 0,
      relpersistence TEXT NOT NULL DEFAULT 'p',
      reltuples INTEGER NOT NULL DEFAULT 0
    );
  `)
  if (tables.length === 0) return
  const insert = db.prepare(
    'INSERT INTO _orez_catalog__pg_class (oid, relname, relnamespace, relkind) VALUES (?, ?, ?, ?)'
  )
  for (const t of tables) {
    const oid = hashName(t.name, TABLE_OID_BASE)
    const kind = t.type === 'view' ? 'v' : 'r'
    insert.run(oid, t.name, NAMESPACE_OID_PUBLIC, kind)
  }
}

function seedPgAttributeAndDefaults(
  db: SqliteLike,
  tables: { name: string; type: string }[]
): void {
  db.exec(`
    DROP TABLE IF EXISTS _orez_catalog__pg_attribute;
    CREATE TABLE _orez_catalog__pg_attribute (
      attrelid INTEGER NOT NULL,
      attname TEXT NOT NULL,
      attnum INTEGER NOT NULL,
      atttypid INTEGER NOT NULL DEFAULT 25,
      attlen INTEGER NOT NULL DEFAULT -1,
      attnotnull INTEGER NOT NULL DEFAULT 0,
      atthasdef INTEGER NOT NULL DEFAULT 0,
      attisdropped INTEGER NOT NULL DEFAULT 0,
      atttypmod INTEGER NOT NULL DEFAULT -1,
      PRIMARY KEY (attrelid, attnum)
    );

    DROP TABLE IF EXISTS _orez_catalog__pg_attrdef;
    CREATE TABLE _orez_catalog__pg_attrdef (
      oid INTEGER PRIMARY KEY,
      adrelid INTEGER NOT NULL,
      adnum INTEGER NOT NULL,
      adbin TEXT,
      adsrc TEXT
    );
  `)
  const attrInsert = db.prepare(
    'INSERT INTO _orez_catalog__pg_attribute (attrelid, attname, attnum, atttypid, attnotnull, atthasdef) VALUES (?, ?, ?, ?, ?, ?)'
  )
  const defInsert = db.prepare(
    'INSERT INTO _orez_catalog__pg_attrdef (oid, adrelid, adnum, adbin, adsrc) VALUES (?, ?, ?, ?, ?)'
  )
  let defOid = 100_000
  for (const t of tables) {
    const cols = readColumns(db, t.name)
    if (cols.length === 0) continue
    const oid = hashName(t.name, TABLE_OID_BASE)
    for (const c of cols) {
      const typname = sqliteToPgType(c.type)
      const typoid = PG_TYPE_OIDS[typname] ?? PG_TYPE_OIDS.text
      const hasdef = c.dflt_value !== null ? 1 : 0
      attrInsert.run(oid, c.name, c.cid + 1, typoid, c.notnull, hasdef)
      if (c.dflt_value !== null) {
        defInsert.run(defOid++, oid, c.cid + 1, c.dflt_value, c.dflt_value)
      }
    }
  }
}

function seedPgPublication(
  db: SqliteLike,
  publications: readonly string[],
  tables: { name: string; type: string }[]
): void {
  db.exec(`
    DROP TABLE IF EXISTS _orez_catalog__pg_publication;
    CREATE TABLE _orez_catalog__pg_publication (
      oid INTEGER PRIMARY KEY,
      pubname TEXT NOT NULL UNIQUE,
      pubowner INTEGER NOT NULL DEFAULT 10,
      puballtables INTEGER NOT NULL DEFAULT 0,
      pubinsert INTEGER NOT NULL DEFAULT 1,
      pubupdate INTEGER NOT NULL DEFAULT 1,
      pubdelete INTEGER NOT NULL DEFAULT 1,
      pubtruncate INTEGER NOT NULL DEFAULT 1
    );

    DROP TABLE IF EXISTS _orez_catalog__pg_publication_tables;
    CREATE TABLE _orez_catalog__pg_publication_tables (
      pubname TEXT NOT NULL,
      schemaname TEXT NOT NULL,
      tablename TEXT NOT NULL,
      PRIMARY KEY (pubname, schemaname, tablename)
    );

    DROP TABLE IF EXISTS _orez_catalog__pg_publication_rel;
    CREATE TABLE _orez_catalog__pg_publication_rel (
      oid INTEGER PRIMARY KEY,
      prpubid INTEGER NOT NULL,
      prrelid INTEGER NOT NULL
    );
  `)
  if (publications.length === 0) return

  const pubInsert = db.prepare(
    'INSERT INTO _orez_catalog__pg_publication (oid, pubname) VALUES (?, ?)'
  )
  const ptInsert = db.prepare(
    'INSERT INTO _orez_catalog__pg_publication_tables (pubname, schemaname, tablename) VALUES (?, ?, ?)'
  )
  const relInsert = db.prepare(
    'INSERT INTO _orez_catalog__pg_publication_rel (oid, prpubid, prrelid) VALUES (?, ?, ?)'
  )
  let relOid = 200_000
  for (const p of publications) {
    const pubOid = hashName(p, 1_000_000)
    pubInsert.run(pubOid, p)
    for (const t of tables) {
      ptInsert.run(p, 'public', t.name)
      relInsert.run(relOid++, pubOid, hashName(t.name, TABLE_OID_BASE))
    }
  }
}

/**
 * Empty-but-queryable stubs for catalog tables that the pass recognizes but
 * we don't actively populate. Created with their PG-compatible columns so
 * SELECT-from-them returns 0 rows cleanly instead of throwing.
 */
function seedSimpleStubs(db: SqliteLike): void {
  const stubs: { name: string; columns: string }[] = [
    {
      name: 'pg_constraint',
      columns:
        'oid INTEGER PRIMARY KEY, conname TEXT NOT NULL, connamespace INTEGER, contype TEXT, conrelid INTEGER, conindid INTEGER, confrelid INTEGER, conkey TEXT, confkey TEXT',
    },
    {
      name: 'pg_index',
      columns:
        'indexrelid INTEGER PRIMARY KEY, indrelid INTEGER NOT NULL, indnatts INTEGER, indisunique INTEGER DEFAULT 0, indisprimary INTEGER DEFAULT 0, indkey TEXT',
    },
    {
      name: 'pg_proc',
      columns:
        'oid INTEGER PRIMARY KEY, proname TEXT NOT NULL, pronamespace INTEGER, prorettype INTEGER, proargtypes TEXT',
    },
    {
      name: 'pg_trigger',
      columns:
        'oid INTEGER PRIMARY KEY, tgrelid INTEGER NOT NULL, tgname TEXT NOT NULL, tgfoid INTEGER, tgenabled TEXT',
    },
    {
      name: 'pg_inherits',
      columns: 'inhrelid INTEGER NOT NULL, inhparent INTEGER NOT NULL, inhseqno INTEGER',
    },
    {
      name: 'pg_depend',
      columns:
        'classid INTEGER, objid INTEGER, objsubid INTEGER, refclassid INTEGER, refobjid INTEGER, refobjsubid INTEGER, deptype TEXT',
    },
    {
      name: 'pg_description',
      columns: 'objoid INTEGER, classoid INTEGER, objsubid INTEGER, description TEXT',
    },
    {
      name: 'pg_enum',
      columns:
        'oid INTEGER PRIMARY KEY, enumtypid INTEGER NOT NULL, enumsortorder REAL, enumlabel TEXT NOT NULL',
    },
    {
      name: 'pg_extension',
      columns:
        'oid INTEGER PRIMARY KEY, extname TEXT NOT NULL, extowner INTEGER, extnamespace INTEGER, extversion TEXT',
    },
    {
      name: 'pg_sequence',
      columns:
        'seqrelid INTEGER PRIMARY KEY, seqtypid INTEGER, seqstart INTEGER, seqincrement INTEGER, seqmax INTEGER, seqmin INTEGER, seqcache INTEGER, seqcycle INTEGER',
    },
    {
      name: 'pg_views',
      columns: 'schemaname TEXT, viewname TEXT, viewowner TEXT, definition TEXT',
    },
    { name: 'pg_collation', columns: 'oid INTEGER PRIMARY KEY, collname TEXT NOT NULL' },
    {
      name: 'pg_am',
      columns: 'oid INTEGER PRIMARY KEY, amname TEXT NOT NULL, amtype TEXT',
    },
    { name: 'pg_operator', columns: 'oid INTEGER PRIMARY KEY, oprname TEXT NOT NULL' },
    {
      name: 'pg_cast',
      columns:
        'oid INTEGER PRIMARY KEY, castsource INTEGER, casttarget INTEGER, castfunc INTEGER, castcontext TEXT, castmethod TEXT',
    },
    { name: 'pg_language', columns: 'oid INTEGER PRIMARY KEY, lanname TEXT NOT NULL' },
    {
      name: 'pg_statistic',
      columns:
        'starelid INTEGER, staattnum INTEGER, stainherit INTEGER, stanullfrac REAL',
    },
    {
      name: 'pg_locks',
      columns:
        'locktype TEXT, database INTEGER, relation INTEGER, page INTEGER, tuple INTEGER, transactionid INTEGER, mode TEXT, granted INTEGER',
    },
    {
      name: 'pg_replication_slots',
      columns:
        "slot_name TEXT PRIMARY KEY, plugin TEXT, slot_type TEXT NOT NULL DEFAULT 'logical', database TEXT, active INTEGER NOT NULL DEFAULT 0, restart_lsn TEXT, confirmed_flush_lsn TEXT",
    },
    {
      name: 'pg_stat_replication',
      columns:
        'pid INTEGER, usesysid INTEGER, usename TEXT, application_name TEXT, client_addr TEXT, state TEXT',
    },
    {
      name: 'pg_subscription',
      columns:
        'oid INTEGER PRIMARY KEY, subname TEXT NOT NULL, subenabled INTEGER, subconninfo TEXT, subslotname TEXT',
    },
    {
      name: 'pg_database',
      columns: 'oid INTEGER PRIMARY KEY, datname TEXT NOT NULL',
    },
    { name: 'pg_roles', columns: 'oid INTEGER PRIMARY KEY, rolname TEXT NOT NULL' },
    { name: 'pg_user', columns: 'usename TEXT PRIMARY KEY, usesysid INTEGER' },
    {
      name: 'pg_settings',
      columns:
        'name TEXT PRIMARY KEY, setting TEXT, category TEXT, short_desc TEXT, context TEXT, vartype TEXT',
    },
  ]
  for (const { name, columns } of stubs) {
    db.exec(
      `DROP TABLE IF EXISTS _orez_catalog__${name}; CREATE TABLE _orez_catalog__${name} (${columns});`
    )
  }
  // pg_database — single row "main"
  const datInsert = db.prepare(
    'INSERT INTO _orez_catalog__pg_database (oid, datname) VALUES (?, ?)'
  )
  datInsert.run(1, 'main')
}

function seedInformationSchema(
  db: SqliteLike,
  tables: { name: string; type: string }[]
): void {
  db.exec(`
    DROP TABLE IF EXISTS _orez_catalog__information_schema_columns;
    CREATE TABLE _orez_catalog__information_schema_columns (
      table_catalog TEXT NOT NULL DEFAULT 'main',
      table_schema TEXT NOT NULL DEFAULT 'public',
      table_name TEXT NOT NULL,
      column_name TEXT NOT NULL,
      ordinal_position INTEGER NOT NULL,
      column_default TEXT,
      is_nullable TEXT NOT NULL DEFAULT 'YES',
      data_type TEXT NOT NULL,
      character_maximum_length INTEGER,
      numeric_precision INTEGER,
      numeric_scale INTEGER,
      udt_name TEXT
    );

    DROP TABLE IF EXISTS _orez_catalog__information_schema_tables;
    CREATE TABLE _orez_catalog__information_schema_tables (
      table_catalog TEXT NOT NULL DEFAULT 'main',
      table_schema TEXT NOT NULL DEFAULT 'public',
      table_name TEXT NOT NULL,
      table_type TEXT NOT NULL DEFAULT 'BASE TABLE',
      PRIMARY KEY (table_schema, table_name)
    );
  `)
  const colInsert = db.prepare(
    'INSERT INTO _orez_catalog__information_schema_columns ' +
      '(table_catalog, table_schema, table_name, column_name, ordinal_position, column_default, is_nullable, data_type, udt_name) ' +
      'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
  )
  const tblInsert = db.prepare(
    'INSERT INTO _orez_catalog__information_schema_tables (table_catalog, table_schema, table_name, table_type) VALUES (?, ?, ?, ?)'
  )
  for (const t of tables) {
    tblInsert.run('main', 'public', t.name, t.type === 'view' ? 'VIEW' : 'BASE TABLE')
    const cols = readColumns(db, t.name)
    for (const c of cols) {
      const typname = sqliteToPgType(c.type)
      const isNullable = c.notnull ? 'NO' : 'YES'
      // dflt_value from PRAGMA is already a SQL expression as-written (e.g.
      // `'foo'` for a string default, `CURRENT_TIMESTAMP` bare for a keyword
      // default). Pass through unchanged — PG's column_default reports the
      // original expression text.
      colInsert.run(
        'main',
        'public',
        t.name,
        c.name,
        c.cid + 1,
        c.dflt_value,
        isNullable,
        typname,
        typname
      )
    }
  }
}
