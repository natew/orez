import { walkAst } from './ast-utils.js'

/**
 * Catalog pass.
 *
 * Rewrites schema-qualified PG catalog references to the
 * `_orez_catalog__*` namespace (seeded by `catalog/seed.ts` at DO init):
 *
 *   pg_catalog.pg_class       → _orez_catalog__pg_class
 *   information_schema.columns → _orez_catalog__information_schema_columns
 *
 * Why this matters:
 *   zero-cache (and most PG client libraries) probes the PG system catalog
 *   on startup. They always qualify these references with `pg_catalog.` or
 *   `information_schema.` — that's how `search_path` resolution works in PG
 *   and how every generated catalog query (libpq, psql, postgres.js) emits
 *   them.
 *
 * Why we DON'T rewrite bare `pg_class` (no schema):
 *   - it's user-table-namespace ambiguous (an app could legitimately call
 *     a table `pg_user`, `pg_views`, etc.)
 *   - bare catalog refs only resolve in PG via search_path; clients
 *     emitting catalog queries qualify them explicitly. Bare references in
 *     real apps are essentially always user tables.
 *   - silently hijacking them caused WRITE-path bugs (DML against a user
 *     table got routed to a synthetic catalog table) in earlier iterations
 *
 * If a future workload sends bare catalog refs we'll add a per-statement
 * opt-in (e.g. `WHERE rewrite_unqualified_catalog`) rather than a
 * footgun-prone global toggle.
 *
 * Companion module: `catalog/seed.ts` creates the target tables on DO init.
 */
import type { Pass } from '../types.js'

const CATALOG_PREFIX = '_orez_catalog__'
const FLATTENED_SCHEMAS = new Set(['information_schema'])

function rewriteRangeVar(node: any): void {
  // pg_catalog.X — strip schema, prefix relname
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
