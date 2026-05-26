/**
 * Catalog seed — populates `_orez_catalog.*` tables in a DO SQLite database.
 *
 * The catalog pass rewrites `pg_class`, `pg_attribute`, etc. references to
 * point at this schema. Here we create those tables and fill them with rows
 * synthesized from SQLite's introspection (`sqlite_master`, `PRAGMA
 * table_info(...)`).
 *
 * Tables seeded (covers what zero-cache and most PG clients probe):
 *
 *   pg_class                     one row per user table
 *   pg_attribute                 one row per column
 *   pg_namespace                 one row: 'public' (oid 2200)
 *   pg_type                      one row per PG type we know about (synthesized)
 *   pg_publication               typically empty for orez; one row if
 *                                ZERO_APP_PUBLICATIONS env is set
 *   pg_publication_tables        ditto
 *   pg_replication_slots         empty
 *   information_schema_columns   one row per column (flat-named because
 *                                SQLite can't dot through schemas)
 *   information_schema_tables    one row per user table
 *
 * Idempotent — safe to call on every DO init. Reflects the live schema each
 * time. For very large schemas the introspection is O(tables×columns); for
 * orez's chat-app-scale workloads (~30 tables, ~200 columns) this is sub-ms.
 *
 * Used in tests via `seedCatalog(db)` against an in-memory sqlite. In the DO
 * runtime, the same function runs against `ctx.storage.sql` (the API is
 * better-sqlite3-compatible via @rocicorp/zero-sqlite3's shim).
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

/**
 * Seed catalog tables. Schemas aren't a SQLite concept (no real CREATE
 * SCHEMA), so we use the convention `_orez_catalog__<table>` table names —
 * the catalog pass rewrites references to match. We expose a virtual schema
 * shape by attaching prefixed names.
 *
 * Note: SQLite DOES support attached databases (`ATTACH DATABASE`), which we
 * could use to make `_orez_catalog.pg_class` work literally. For DO SQLite
 * the simpler path is rename-on-rewrite. Keeping the API symmetric with PG
 * is a docs concern, not a SQL concern.
 *
 * Schema chosen: tables created as `_orez_catalog__pg_class` etc. The
 * catalog pass therefore rewrites `pg_catalog.pg_class` →
 * `_orez_catalog__pg_class` (without the dot).
 */
export function buildCatalogTables(db: SqliteLike, opts: SeedOptions = {}): void {
  const publications = opts.publications ?? []

  // pg_namespace
  db.exec(`
    DROP TABLE IF EXISTS _orez_catalog__pg_namespace;
    CREATE TABLE _orez_catalog__pg_namespace (
      oid INTEGER PRIMARY KEY,
      nspname TEXT NOT NULL,
      nspowner INTEGER NOT NULL DEFAULT 10,
      nspacl TEXT
    );
    INSERT INTO _orez_catalog__pg_namespace (oid, nspname) VALUES
      (${NAMESPACE_OID_PUBLIC}, 'public'),
      (11, 'pg_catalog'),
      (99, 'information_schema');
  `)

  // pg_type — small static set, covers what most clients probe by oid
  db.exec(`
    DROP TABLE IF EXISTS _orez_catalog__pg_type;
    CREATE TABLE _orez_catalog__pg_type (
      oid INTEGER PRIMARY KEY,
      typname TEXT NOT NULL,
      typnamespace INTEGER NOT NULL DEFAULT 11,
      typlen INTEGER NOT NULL DEFAULT -1,
      typtype TEXT NOT NULL DEFAULT 'b',
      typbasetype INTEGER NOT NULL DEFAULT 0,
      typelem INTEGER NOT NULL DEFAULT 0,
      typcategory TEXT,
      typnotnull INTEGER NOT NULL DEFAULT 0
    );
  `)
  const typeRows = Object.entries(PG_TYPE_OIDS)
    .map(([name, oid]) => `(${oid}, '${name}')`)
    .join(',\n      ')
  db.exec(`INSERT INTO _orez_catalog__pg_type (oid, typname) VALUES\n      ${typeRows};`)

  // pg_class — one row per user table
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
  const tables = db
    .prepare(
      "SELECT name, type FROM sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_orez_catalog__%'"
    )
    .all() as { name: string; type: string }[]
  const tableRows = tables
    .map((t) => {
      const oid = hashName(t.name, TABLE_OID_BASE)
      const kind = t.type === 'view' ? 'v' : 'r'
      return `(${oid}, '${t.name.replace(/'/g, "''")}', ${NAMESPACE_OID_PUBLIC}, '${kind}')`
    })
    .join(',\n        ')
  if (tableRows.length > 0) {
    db.exec(
      `INSERT INTO _orez_catalog__pg_class (oid, relname, relnamespace, relkind) VALUES
        ${tableRows};`
    )
  }

  // pg_attribute — one row per column of each user table
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
  `)
  for (const t of tables) {
    const cols = db
      .prepare(`PRAGMA table_info("${t.name.replace(/"/g, '""')}")`)
      .all() as {
      cid: number
      name: string
      type: string | null
      notnull: number
      dflt_value: string | null
      pk: number
    }[]
    if (cols.length === 0) continue
    const oid = hashName(t.name, TABLE_OID_BASE)
    const rows = cols
      .map((c) => {
        const typname = sqliteToPgType(c.type)
        const typoid = PG_TYPE_OIDS[typname] ?? 25
        const hasdef = c.dflt_value !== null ? 1 : 0
        return `(${oid}, '${c.name.replace(/'/g, "''")}', ${c.cid + 1}, ${typoid}, ${c.notnull}, ${hasdef})`
      })
      .join(',\n        ')
    db.exec(
      `INSERT INTO _orez_catalog__pg_attribute (attrelid, attname, attnum, atttypid, attnotnull, atthasdef) VALUES
        ${rows};`
    )
  }

  // pg_publication
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
  `)
  if (publications.length > 0) {
    const pubRows = publications
      .map(
        (p, i) =>
          `(${hashName(p, 1_000_000)}, '${p.replace(/'/g, "''")}', 10, 0, 1, 1, 1, 1)`
      )
      .join(',\n        ')
    db.exec(
      `INSERT INTO _orez_catalog__pg_publication (oid, pubname, pubowner, puballtables, pubinsert, pubupdate, pubdelete, pubtruncate) VALUES
        ${pubRows};`
    )
  }

  // pg_publication_tables — many-to-many; empty by default
  db.exec(`
    DROP TABLE IF EXISTS _orez_catalog__pg_publication_tables;
    CREATE TABLE _orez_catalog__pg_publication_tables (
      pubname TEXT NOT NULL,
      schemaname TEXT NOT NULL,
      tablename TEXT NOT NULL,
      PRIMARY KEY (pubname, schemaname, tablename)
    );
  `)
  if (publications.length > 0) {
    for (const p of publications) {
      const rows = tables
        .map(
          (t) => `('${p.replace(/'/g, "''")}', 'public', '${t.name.replace(/'/g, "''")}')`
        )
        .join(',\n          ')
      if (rows.length > 0) {
        db.exec(
          `INSERT INTO _orez_catalog__pg_publication_tables (pubname, schemaname, tablename) VALUES
          ${rows};`
        )
      }
    }
  }

  // pg_replication_slots — typically empty for orez (we run our own
  // change-tracker, not real PG logical replication)
  db.exec(`
    DROP TABLE IF EXISTS _orez_catalog__pg_replication_slots;
    CREATE TABLE _orez_catalog__pg_replication_slots (
      slot_name TEXT PRIMARY KEY,
      plugin TEXT,
      slot_type TEXT NOT NULL DEFAULT 'logical',
      database TEXT,
      active INTEGER NOT NULL DEFAULT 0,
      restart_lsn TEXT,
      confirmed_flush_lsn TEXT
    );
  `)

  // information_schema_columns
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
  `)
  for (const t of tables) {
    const cols = db
      .prepare(`PRAGMA table_info("${t.name.replace(/"/g, '""')}")`)
      .all() as {
      cid: number
      name: string
      type: string | null
      notnull: number
      dflt_value: string | null
    }[]
    if (cols.length === 0) continue
    const rows = cols
      .map((c) => {
        const typname = sqliteToPgType(c.type)
        const isNullable = c.notnull ? 'NO' : 'YES'
        const def =
          c.dflt_value === null ? 'NULL' : `'${c.dflt_value.replace(/'/g, "''")}'`
        return `('main', 'public', '${t.name.replace(/'/g, "''")}', '${c.name.replace(/'/g, "''")}', ${c.cid + 1}, ${def}, '${isNullable}', '${typname}', NULL, NULL, NULL, '${typname}')`
      })
      .join(',\n        ')
    db.exec(
      `INSERT INTO _orez_catalog__information_schema_columns (table_catalog, table_schema, table_name, column_name, ordinal_position, column_default, is_nullable, data_type, character_maximum_length, numeric_precision, numeric_scale, udt_name) VALUES
        ${rows};`
    )
  }

  // information_schema_tables
  db.exec(`
    DROP TABLE IF EXISTS _orez_catalog__information_schema_tables;
    CREATE TABLE _orez_catalog__information_schema_tables (
      table_catalog TEXT NOT NULL DEFAULT 'main',
      table_schema TEXT NOT NULL DEFAULT 'public',
      table_name TEXT NOT NULL,
      table_type TEXT NOT NULL DEFAULT 'BASE TABLE',
      PRIMARY KEY (table_schema, table_name)
    );
  `)
  if (tables.length > 0) {
    const trows = tables
      .map(
        (t) =>
          `('main', 'public', '${t.name.replace(/'/g, "''")}', '${t.type === 'view' ? 'VIEW' : 'BASE TABLE'}')`
      )
      .join(',\n        ')
    db.exec(
      `INSERT INTO _orez_catalog__information_schema_tables (table_catalog, table_schema, table_name, table_type) VALUES
        ${trows};`
    )
  }
}
