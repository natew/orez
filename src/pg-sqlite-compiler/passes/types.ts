import { walkAst } from './ast-utils.js'

/**
 * Types pass.
 *
 * Normalizes PG type names in CREATE TABLE / ALTER TABLE / CAST / column
 * defs to SQLite-compatible equivalents:
 *
 *   bigserial / serial    → INTEGER     (and PRIMARY KEY on INTEGER becomes
 *                                        SQLite's rowid alias, equivalent to
 *                                        AUTOINCREMENT for our purposes)
 *   smallserial           → INTEGER
 *   jsonb / json          → TEXT        (stored as JSON text; SQLite json
 *                                        functions operate on TEXT directly)
 *   uuid                  → TEXT
 *   bytea                 → BLOB
 *   varchar / character varying → TEXT  (drop length typmods)
 *   char(N) / character(N)→ TEXT        (drop length typmods)
 *   text                  → TEXT
 *   timestamp / timestamptz / timestamp with time zone → TEXT
 *                                       (stored as ISO; SQLite datetime fns
 *                                        accept ISO text)
 *   date / time / timetz  → TEXT
 *   interval              → TEXT
 *   numeric / decimal     → NUMERIC     (SQLite native affinity; precision/
 *                                        scale typmods dropped)
 *   real / float4         → REAL
 *   double precision / float8 → REAL
 *   integer / int / int4  → INTEGER
 *   bigint / int8         → INTEGER
 *   smallint / int2       → INTEGER
 *   boolean               → INTEGER     (0/1; SQLite has no BOOLEAN type but
 *                                        the BOOLEAN keyword is accepted as
 *                                        affinity)
 *
 * Array types (any T[] with arrayBounds) become TEXT (stored as JSON arrays).
 *
 * pg_catalog.* prefix is stripped (libpg-query adds it canonically; SQLite
 * doesn't know about pg_catalog).
 */
import type { Pass } from '../types.js'

const TYPE_MAP: Record<string, string> = {
  // serial → integer (PRIMARY KEY on INTEGER is rowid alias in SQLite)
  bigserial: 'INTEGER',
  serial: 'INTEGER',
  serial4: 'INTEGER',
  serial8: 'INTEGER',
  smallserial: 'INTEGER',
  serial2: 'INTEGER',

  // integer
  int: 'INTEGER',
  integer: 'INTEGER',
  int2: 'INTEGER',
  int4: 'INTEGER',
  int8: 'INTEGER',
  bigint: 'INTEGER',
  smallint: 'INTEGER',

  // floats
  real: 'REAL',
  float4: 'REAL',
  float8: 'REAL',
  'double precision': 'REAL',
  double: 'REAL',

  // numerics — keep affinity name; SQLite parses but ignores precision
  numeric: 'NUMERIC',
  decimal: 'NUMERIC',

  // text-ish
  text: 'TEXT',
  varchar: 'TEXT',
  'character varying': 'TEXT',
  bpchar: 'TEXT',
  character: 'TEXT',
  char: 'TEXT',
  name: 'TEXT',
  citext: 'TEXT',

  // json / structured
  json: 'TEXT',
  jsonb: 'TEXT',
  uuid: 'TEXT',
  xml: 'TEXT',

  // bin
  bytea: 'BLOB',

  // bools
  bool: 'INTEGER',
  boolean: 'INTEGER',

  // time
  timestamp: 'TEXT',
  timestamptz: 'TEXT',
  'timestamp with time zone': 'TEXT',
  'timestamp without time zone': 'TEXT',
  date: 'TEXT',
  time: 'TEXT',
  timetz: 'TEXT',
  'time with time zone': 'TEXT',
  'time without time zone': 'TEXT',
  interval: 'TEXT',

  // network / ranges → TEXT for storage compatibility
  inet: 'TEXT',
  cidr: 'TEXT',
  macaddr: 'TEXT',
  macaddr8: 'TEXT',
  money: 'NUMERIC',
}

function extractName(names: any[] | undefined): string | undefined {
  if (!Array.isArray(names) || names.length === 0) return undefined
  // strip pg_catalog. prefix if present
  const last = names[names.length - 1]
  const sval = last?.String?.sval ?? last?.String?.str
  return typeof sval === 'string' ? sval.toLowerCase() : undefined
}

function setTypeName(typeName: any, sqliteName: string): void {
  typeName.names = [{ String: { sval: sqliteName } }]
  // drop length/precision typmods
  delete typeName.typmods
  // drop array bounds (SQLite has no array type; we store as JSON text)
  delete typeName.arrayBounds
}

export const typesPass: Pass = {
  name: 'types',
  run(rawStmt, _ctx) {
    walkAst(rawStmt, {
      TypeName: (node: any) => {
        const pgName = extractName(node.names)
        if (!pgName) return
        const hasArrayBounds =
          Array.isArray(node.arrayBounds) && node.arrayBounds.length > 0

        // Array of anything → TEXT
        if (hasArrayBounds) {
          setTypeName(node, 'TEXT')
          return
        }

        const sqliteName = TYPE_MAP[pgName]
        if (sqliteName) {
          setTypeName(node, sqliteName)
        }
        // Unknown types we leave alone — let SQLite reject loudly so we know
        // to add a mapping.
      },
    })
  },
}
