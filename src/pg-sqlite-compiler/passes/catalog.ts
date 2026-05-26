import { walkAst } from './ast-utils.js'

/**
 * Catalog pass.
 *
 * Rewrites `FROM pg_catalog.*`, `FROM pg_<table>`, and `FROM information_schema.*`
 * to a single-name table in the `_orez_catalog__*` namespace, where the
 * runtime (DurableObject init via `catalog/seed.ts`) seeds emulated catalog
 * tables.
 *
 * Why this matters:
 *   zero-cache (and every PG client library) probes the PG system catalog on
 *   startup — pg_class, pg_attribute, pg_namespace, pg_type, pg_publication,
 *   information_schema.columns, etc. SQLite has no such schemas. Without
 *   this rewrite, queries fail with "no such table: pg_class".
 *
 * Naming convention (flat, since SQLite doesn't support cross-schema dots
 * without ATTACH DATABASE):
 *   pg_catalog.pg_class       → _orez_catalog__pg_class
 *   pg_class (no schema)      → _orez_catalog__pg_class
 *   information_schema.columns → _orez_catalog__information_schema_columns
 *   information_schema.tables  → _orez_catalog__information_schema_tables
 *
 * Companion module: `catalog/seed.ts` creates the target tables on DO init.
 */
import type { Pass } from '../types.js'

const CATALOG_PREFIX = '_orez_catalog__'

/** PG catalog tables we recognize (bare, no schema). */
const PG_CATALOG_TABLES = new Set([
  'pg_class',
  'pg_attribute',
  'pg_namespace',
  'pg_type',
  'pg_proc',
  'pg_constraint',
  'pg_index',
  'pg_database',
  'pg_roles',
  'pg_user',
  'pg_settings',
  'pg_publication',
  'pg_publication_tables',
  'pg_publication_rel',
  'pg_replication_slots',
  'pg_stat_replication',
  'pg_subscription',
  'pg_description',
  'pg_depend',
  'pg_enum',
  'pg_extension',
  'pg_trigger',
  'pg_sequence',
  'pg_views',
  'pg_tables',
  'pg_collation',
  'pg_am',
  'pg_operator',
  'pg_cast',
  'pg_language',
  'pg_statistic',
  'pg_locks',
])

const FLATTENED_SCHEMAS = new Set(['information_schema'])

function rewriteRangeVar(node: any): void {
  // pg_catalog.* — strip schema, prefix relname
  if (node.schemaname === 'pg_catalog') {
    node.relname = `${CATALOG_PREFIX}${node.relname}`
    delete node.schemaname
    return
  }

  // information_schema.X — flatten to _orez_catalog__information_schema_X
  if (FLATTENED_SCHEMAS.has(node.schemaname)) {
    node.relname = `${CATALOG_PREFIX}${node.schemaname}_${node.relname}`
    delete node.schemaname
    return
  }

  // Bare table reference that matches a known PG catalog table — route to
  // catalog as well (PG accepts `pg_class` unqualified when search_path
  // includes pg_catalog, which it does by default).
  if (!node.schemaname && PG_CATALOG_TABLES.has(node.relname)) {
    node.relname = `${CATALOG_PREFIX}${node.relname}`
    return
  }
}

export const catalogPass: Pass = {
  name: 'catalog',
  run(rawStmt, _ctx) {
    walkAst(rawStmt, {
      RangeVar: (node: any) => {
        if (!node || typeof node !== 'object') return
        rewriteRangeVar(node)
      },
    })
  },
}
