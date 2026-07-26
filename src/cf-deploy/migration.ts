import type { CloudflareConfig } from './config.js'

const SENTINEL_LOWER = 'nspfx'
const SENTINEL_PASCAL = 'Nspfx'

function applyPrefix(template: string, cfg: CloudflareConfig): string {
  const pascalName = cfg.name.charAt(0).toUpperCase() + cfg.name.slice(1)
  return template
    .split(SENTINEL_PASCAL)
    .join(pascalName)
    .split(SENTINEL_LOWER)
    .join(cfg.name)
}

export type CloudflareNativeTableShape = {
  columns: Array<{
    name: string
    notNull: boolean
    primaryKeyOrder: number
    sqlType: string
  }>
  name: string
}

export type CloudflareMigrationModuleSourceParts =
  | {
      mode: 'noop'
      schemaVersion: string
    }
  | {
      /** SQLite-native schema path for the Rust-sync application worker. */
      mode: 'native'
      schemaVersion: string
      schemaImportSpecifier: string
      nativeSqlStatements: unknown
      publicTables?: Array<{ table: string; publicTable: string }>
      expectedTables?: CloudflareNativeTableShape[]
    }

export function buildMigrationModuleSource(
  cfg: CloudflareConfig,
  parts: CloudflareMigrationModuleSourceParts
): string {
  const runCloudflareMigrations = applyPrefix('runNspfxCloudflareMigrations', cfg)
  const migrationTableName = applyPrefix('__nspfx_cf_migrations', cfg)

  if (parts.mode === 'noop') {
    return `export const SCHEMA_VERSION = ${JSON.stringify(parts.schemaVersion)}
export async function ${runCloudflareMigrations}() {}
export { ${runCloudflareMigrations} as runCloudflareMigrations }
`
  }

  if (parts.mode === 'native') {
    const applicationSqlGlobal = applyPrefix('__nspfx_cf_application_sql_client', cfg)
    return `import { schema } from ${JSON.stringify(parts.schemaImportSpecifier)}

export const SCHEMA_VERSION = ${JSON.stringify(parts.schemaVersion)}

const nativeSqlStatements = ${JSON.stringify(parts.nativeSqlStatements)}
const configuredPublicTables = ${JSON.stringify(parts.publicTables ?? [])}
const expectedTables = ${JSON.stringify(parts.expectedTables ?? [])}
const migrationTable = ${JSON.stringify(migrationTableName)}

function quoteIdentifier(value) {
  return '"' + String(value).replaceAll('"', '""') + '"'
}

function schemaMetadataStatements() {
  return Object.values(schema.tables || {})
    .filter((table) => table && typeof table.name === 'string')
    .map((table) => ({
      sql: 'INSERT OR REPLACE INTO _zero_schema_tables (name, schema_json) VALUES (?, ?)',
      params: [
        table.name,
        JSON.stringify({ columns: table.columns, primaryKey: table.primaryKey }),
      ],
    }))
}

function publicTables() {
  if (configuredPublicTables.length) return configuredPublicTables
  return Object.values(schema.tables || {})
    .filter((table) => table && typeof table.name === 'string')
    .map((table) => {
      const publicTable = table.name.startsWith('public.') ? table.name : 'public.' + table.name
      return { table: publicTable.replace(/^public\\./, ''), publicTable }
    })
}

async function shouldSkipStatement(tx, statement) {
  if (statement.skipIfTableMissing) {
    const tempRows = await tx.query(
      'PRAGMA table_info(' + quoteIdentifier(statement.skipIfTableMissing) + ')',
    )
    if (tempRows.length === 0) return true
  }
  const condition = statement.skipIfColumnExists || statement.skipIfColumnMissing
  if (!condition) return false
  const rows = await tx.query('PRAGMA table_info(' + quoteIdentifier(condition.table) + ')')
  const hasColumn = rows.some((row) => row && row.name === condition.column)
  return statement.skipIfColumnExists ? hasColumn : !hasColumn
}

function normalizeSqlType(value) {
  return String(value).trim().toLowerCase().replaceAll(/\\s+/g, ' ')
}

// every table's columns in ONE round trip. this used to be a PRAGMA
// table_info per expected table in the shape assert plus another per ledgered
// ADD COLUMN in the reconcile — order 125 sequential DO calls, all of them
// holding the namespace's exclusive application-SQL session while concurrent
// readers spin in the 25ms acquire retry. healing paths force this run on
// EVERY request for an unseeded project, so its cost lands on exactly the
// population it exists to repair (the 2026-07-21 outage shape).
//
// one behaviour difference from the PRAGMAs this replaced: lookups here are
// case-EXACT, because the keys come from sqlite_master.name, while
// PRAGMA table_info resolves a table name case-insensitively. No statement in
// the current set mismatches, but a hand-written repair migration that spelled
// a table name in a different case would read as a missing column, resurrect,
// and then die on "duplicate column name". Match sqlite_master's spelling.
async function readLiveColumns(tx) {
  // exclude protected/internal tables from the scan. once a Durable Object has
  // used its KV API, _cf_KV is listed in sqlite_master but the platform
  // authorizer refuses pragma_table_info on it, and one denied row fails the
  // WHOLE statement with SQLITE_AUTH (verified 2026-07-24 against a real DO:
  // scan passes without _cf_KV, fails the moment storage.put creates it, and
  // passes again with this filter). every prod namespace DO has _cf_KV, so the
  // unfiltered scan broke every migration session. caret escape on purpose:
  // this whole function body is emitted through a template literal, where a
  // literal backslash would need quadruple escaping.
  const rows = await tx.query(
    'SELECT m.name AS tableName, p.name AS columnName, p.type AS columnType,' +
      ' p."notnull" AS columnNotNull, p.pk AS columnPk' +
      " FROM sqlite_master m JOIN pragma_table_info(m.name) p WHERE m.type = 'table'" +
      " AND m.name NOT LIKE '^_cf^_%' ESCAPE '^'" +
      " AND m.name NOT LIKE 'sqlite^_%' ESCAPE '^'",
  )
  const tables = new Map()
  for (const row of rows) {
    let columns = tables.get(row.tableName)
    if (!columns) {
      columns = []
      tables.set(row.tableName, columns)
    }
    columns.push({
      name: row.columnName,
      notnull: row.columnNotNull,
      pk: row.columnPk,
      type: row.columnType,
    })
  }
  return tables
}

async function assertExpectedSchema(tx) {
  const liveColumns = await readLiveColumns(tx)
  // two passes on purpose. reporting only the FIRST missing table makes an
  // operator fix them one deploy at a time, and a per-column mismatch later in
  // the list would throw before the missing-table set was even known. collect
  // every absent table first and report them together.
  const present = []
  const missingTables = []
  for (const expectedTable of expectedTables) {
    const actualColumns = liveColumns.get(expectedTable.name) || []
    if (actualColumns.length === 0) missingTables.push(expectedTable.name)
    else present.push({ actualColumns, expectedTable })
  }
  if (missingTables.length > 0) {
    throw new Error(
      'application SQLite schema mismatch: missing table' +
        (missingTables.length > 1 ? 's' : '') +
        ' ' +
        missingTables.join(', '),
    )
  }

  for (const { actualColumns, expectedTable } of present) {
    const actualByName = new Map(actualColumns.map((column) => [column.name, column]))
    const missing = expectedTable.columns
      .filter((column) => !actualByName.has(column.name))
      .map((column) => column.name)
    if (missing.length > 0) {
      throw new Error(
        'application SQLite schema mismatch for ' + expectedTable.name +
          ': missing [' + missing.join(', ') + ']',
      )
    }

    const expectedPrimaryKey = expectedTable.columns
      .filter((column) => column.primaryKeyOrder > 0)
      .sort((left, right) => left.primaryKeyOrder - right.primaryKeyOrder)
      .map((column) => column.name)
    const actualPrimaryKey = actualColumns
      .filter((column) => Number(column.pk) > 0)
      .sort((left, right) => Number(left.pk) - Number(right.pk))
      .map((column) => column.name)
    let equivalentPrimaryKeyIndex = false
    if (
      expectedPrimaryKey.length > 0 &&
      (actualPrimaryKey.length !== expectedPrimaryKey.length ||
        actualPrimaryKey.some((column, index) => column !== expectedPrimaryKey[index]))
    ) {
      const indexes = await tx.query(
        'PRAGMA index_list(' + quoteIdentifier(expectedTable.name) + ')',
      )
      for (const index of indexes) {
        if (Number(index.unique) === 0 || Number(index.partial) !== 0) continue
        const columns = (await tx.query(
          'PRAGMA index_info(' + quoteIdentifier(index.name) + ')',
        ))
          .sort((left, right) => Number(left.seqno) - Number(right.seqno))
          .map((column) => column.name)
        if (
          columns.length === expectedPrimaryKey.length &&
          columns.every((column, index) => column === expectedPrimaryKey[index])
        ) {
          equivalentPrimaryKeyIndex = true
          break
        }
      }
    }

    for (const expected of expectedTable.columns) {
      const actual = actualByName.get(expected.name)
      if (!actual) continue
      const actualType = normalizeSqlType(actual.type)
      const compatibleLegacyTimestamp =
        expected.sqlType === 'timestamp' &&
        (actualType === 'text' || actualType === 'real')
      const compatibleLegacyNumber =
        expected.sqlType === 'integer' &&
        (actualType === 'real' || actualType === 'text')
      if (
        actualType !== expected.sqlType &&
        !compatibleLegacyTimestamp &&
        !compatibleLegacyNumber
      ) {
        throw new Error(
          'application SQLite schema mismatch for ' + expectedTable.name + '.' + expected.name +
            ': expected ' + expected.sqlType + ', found ' + (actualType || '(untyped)'),
        )
      }

      const actualNotNull = Number(actual.notnull) !== 0 || Number(actual.pk) !== 0
      if (actualNotNull !== expected.notNull) {
        if (expected.notNull && !actualNotNull) {
          const nullRows = await tx.query(
            'SELECT 1 FROM ' + quoteIdentifier(expectedTable.name) +
              ' WHERE ' + quoteIdentifier(expected.name) + ' IS NULL LIMIT 1',
          )
          if (nullRows.length === 0) continue
        }
        throw new Error(
          'application SQLite schema mismatch for ' + expectedTable.name + '.' + expected.name +
            ': expected ' + (expected.notNull ? 'NOT NULL' : 'nullable') +
            ', found ' + (actualNotNull ? 'NOT NULL' : 'nullable'),
        )
      }
      if (Number(actual.pk) !== expected.primaryKeyOrder && !equivalentPrimaryKeyIndex) {
        throw new Error(
          'application SQLite schema mismatch for ' + expectedTable.name + '.' + expected.name +
            ': expected primary-key position ' + expected.primaryKeyOrder +
            ', found ' + actual.pk,
        )
      }
    }
  }
}

function hashMigrationSql(sql) {
  let h = 2166136261
  for (let i = 0; i < sql.length; i++) {
    h ^= sql.charCodeAt(i)
    h = Math.imul(h, 16777619)
  }
  return (h >>> 0).toString(36)
}

// a run killed mid-transaction (client abort, isolate eviction) rolls its DDL
// back but keeps its ledger rows: the ledger is not a CDC-registered table, so
// the DO's row undo never captures it. those phantom rows made every later run
// skip statements whose effects no longer exist, permanently wedging the
// namespace (2026-07-23 provisioning incident). before trusting the ledger,
// delete entries whose effect is verifiably absent so their statements re-run
// in order. a standalone DROP TABLE never re-runs: its effect IS absence.
//
// what this does NOT detect, stated plainly rather than as a reassurance:
//   - DML. a backfill re-runs only when its target table is gone entirely (the
//     same pass recreates it first). undone on a surviving table it is
//     invisible in schema and stays skipped, unlogged. the current set holds 50
//     UPDATE, 10 INSERT INTO and 6 DELETE FROM statements. this cannot wedge a
//     namespace, it leaves stale data behind a correct-looking schema.
//   - an EXTRA column. assertExpectedSchema catches a missing table, a missing
//     column, a type change and a NOT NULL change, but not a surplus one, so a
//     ledgered ALTER TABLE ... DROP COLUMN whose effect rolled back is neither
//     resurrected nor flagged.
//   - a column a later statement in the SAME file renames away. the
//     x__rebuild scratch columns in 20260723140000_declare_epoch_ms_integer
//     are added, copied, then renamed over the original, so the ADD COLUMN rule
//     always reads them as missing and re-adds them on a perfectly healthy
//     namespace. surplus columns, so assertExpectedSchema stays quiet. this
//     predates the block work and is not fixed here.
//   - a rebuild block that changes only nullability, defaults or foreign keys
//     without changing the column SET. its rollback leaves a table this pass
//     reads as landed. it does not silently pass: assertExpectedSchema fails
//     the run on the nullability difference, so the namespace reports loudly
//     instead of self-repairing.
async function reconcilePhantomLedger(tx, applied) {
  if (applied.size === 0) return
  const schemaRows = await tx.query(
    "SELECT name, type FROM sqlite_master WHERE type IN ('table', 'index')",
  )
  const tables = new Set()
  const indexes = new Set()
  for (const row of schemaRows) {
    if (row.type === 'table') tables.add(row.name)
    else indexes.add(row.name)
  }
  const liveColumns = await readLiveColumns(tx)
  // a drizzle rebuild block (CREATE __new_<t> / INSERT..SELECT / DROP <t> /
  // RENAME) is ONE unit. its statements are individually meaningless, so they
  // resurrect together or not at all — including the DROP, which is the only
  // way the RENAME can land on a re-run. the honest evidence that the block
  // landed is <t> carrying the rebuilt COLUMN SET: a rolled-back block leaves
  // the ORIGINAL <t> in place, so judging it by table name reads every wedged
  // namespace as healthy and silently repairs nothing.
  const blockKey = (item) => item.id.split(':')[0] + '::' + item.rebuildTarget
  const blockLanded = new Map()
  for (const statement of nativeSqlStatements) {
    if (!statement || typeof statement !== 'object') continue
    if (!statement.rebuildTarget || !statement.rebuildColumns) continue
    const key = blockKey(statement)
    if (blockLanded.has(key)) continue
    const actualNames = new Set(
      (liveColumns.get(statement.rebuildTarget) || []).map((column) => column.name),
    )
    // a superset test, not equality: a later migration legitimately adds
    // columns to <t>, and treating that as "not landed" would drop a live
    // table and rebuild it from a shape that no longer has them.
    blockLanded.set(
      key,
      actualNames.size > 0 &&
        statement.rebuildColumns.every((column) => actualNames.has(column)),
    )
  }
  // the tables a resurrecting block will DROP and recreate this pass.
  const rebuiltTables = new Set()
  for (const statement of nativeSqlStatements) {
    if (!statement || typeof statement !== 'object' || !statement.rebuildTarget) continue
    if (blockLanded.get(blockKey(statement)) === false) {
      rebuiltTables.add(statement.rebuildTarget)
    }
  }
  const resurrect = new Set()
  for (const [index, statement] of nativeSqlStatements.entries()) {
    const item = typeof statement === 'string'
      ? { id: 'statement-' + index, sql: statement }
      : statement
    if (!item || typeof item.sql !== 'string' || !item.sql.trim()) continue
    const baseId = typeof item.id === 'string' && item.id ? item.id : 'statement-' + index
    let ledgered = false
    for (const id of applied) {
      if (id === baseId || id.startsWith(baseId + ':')) {
        ledgered = true
        break
      }
    }
    if (!ledgered) continue
    // block members bypass the per-statement rules entirely: their effect is
    // the block's effect, judged once above.
    if (item.rebuildTarget) {
      if (blockLanded.get(blockKey(item)) === false) resurrect.add(baseId)
      continue
    }
    const sql = item.sql.trim()
    let match
    let missing = false
    if ((match = /^CREATE TABLE\\s+(?:IF NOT EXISTS\\s+)?[\`"]?(\\w+)/i.exec(sql))) {
      missing = !tables.has(match[1])
    } else if ((match = /^CREATE (?:UNIQUE )?INDEX\\s+(?:IF NOT EXISTS\\s+)?[\`"]?(\\w+)[\`"]?\\s+ON\\s+[\`"]?(\\w+)/i.exec(sql))) {
      // the index set was sampled BEFORE this pass drops and rebuilds <t>, so
      // "the index exists" is stale evidence for any table a resurrecting
      // block is about to DROP: the drop takes its indexes with it and the
      // trailing CREATE INDEXes stay ledgered, leaving the rebuilt table
      // bare. that silently removed the UNIQUE index invite redemption's
      // ON CONFLICT target needs.
      missing = !indexes.has(match[1]) || rebuiltTables.has(match[2])
    } else if ((match = /^ALTER TABLE\\s+[\`"]?(\\w+)[\`"]?\\s+RENAME TO\\s+[\`"]?(\\w+)/i.exec(sql))) {
      missing = !tables.has(match[2])
    } else if ((match = /^ALTER TABLE\\s+[\`"]?(\\w+)[\`"]?\\s+ADD\\s+(?:COLUMN\\s+)?[\`"]?(\\w+)/i.exec(sql))) {
      const columns = liveColumns.get(match[1])
      missing = !columns || !columns.some((column) => column.name === match[2])
    } else if ((match = /^(?:INSERT INTO|UPDATE|DELETE FROM|ALTER TABLE)\\s+[\`"]?(\\w+)/i.exec(sql))) {
      missing = !tables.has(match[1])
    }
    if (missing) resurrect.add(baseId)
  }
  if (resurrect.size === 0) return
  // a resurrected rebuild block must re-run with its sibling PRAGMA
  // foreign_keys toggles, or the block re-executes under FK enforcement.
  const resurrectedFiles = new Set(
    [...resurrect].map((baseId) => baseId.split(':')[0]),
  )
  for (const [index, statement] of nativeSqlStatements.entries()) {
    const item = typeof statement === 'string'
      ? { id: 'statement-' + index, sql: statement }
      : statement
    if (!item || typeof item.sql !== 'string') continue
    if (!/^\\s*PRAGMA\\b/i.test(item.sql.trim())) continue
    const baseId = typeof item.id === 'string' && item.id ? item.id : 'statement-' + index
    if (resurrectedFiles.has(baseId.split(':')[0])) resurrect.add(baseId)
  }
  console.warn(
    '[orez-migrations] resurrecting ' + resurrect.size +
      ' phantom ledger entries: ' + [...resurrect].join(', '),
  )
  // clear a leftover __new_<t> before the block replays, but ONLY when <t> is
  // also present. both present means the previous run died before its DROP, so
  // __new_<t> is a stale partial copy and the resurrected INSERT..SELECT would
  // fail on rows already in it ("UNIQUE constraint failed: __new_<t>.id").
  // <t> ABSENT is the opposite case and must not be touched: there the DROP did
  // land, __new_<t> holds the only copy of the rows, and the replay recreates an
  // empty <t>, copies nothing out of it, drops it and renames __new_<t> over it.
  for (const table of rebuiltTables) {
    if (!tables.has(table) || !tables.has('__new_' + table)) continue
    await tx.exec('DROP TABLE IF EXISTS ' + quoteIdentifier('__new_' + table))
  }
  for (const baseId of resurrect) {
    for (const id of [...applied]) {
      if (id !== baseId && !id.startsWith(baseId + ':')) continue
      await tx.exec(
        'DELETE FROM ' + quoteIdentifier(migrationTable) + ' WHERE id = ?',
        [id],
      )
      applied.delete(id)
    }
  }
}

async function applyNativeSchema(tx, instance) {
  await tx.exec(
    'CREATE TABLE IF NOT EXISTS ' + quoteIdentifier(migrationTable) +
      ' (id TEXT PRIMARY KEY, applied_at INTEGER NOT NULL)',
  )
  const appliedRows = await tx.query('SELECT id FROM ' + quoteIdentifier(migrationTable))
  const applied = new Set(appliedRows.map((row) => row.id))
  await reconcilePhantomLedger(tx, applied)
  // register BEFORE the statements, not only after them. a cdc trigger whose
  // _orez_cdc_tables row is missing is invisible to beginSchemaChange, which
  // skips unregistered tables, so it never suspends capture and the next
  // DROP/RENAME COLUMN on that table dies on the surviving trigger. ensureTable
  // reads that orphan as changed (its registration lookup returns nothing),
  // drops the stale triggers and rewrites the row, so registering first is what
  // lets the DDL below suspend capture at all. the pass after the statements
  // still runs and still owns publication; this one only heals. on a fresh
  // namespace every table is absent, ensureTable returns false for each, and
  // this is a no-op beyond creating the cdc bookkeeping tables early.
  await tx.registerTables(publicTables())
  const appliedIds = [...applied]
  const appliedStatementIds = new Set()
  const supersededStatementIds = new Set()
  for (const [index, statement] of nativeSqlStatements.entries()) {
    const item = typeof statement === 'string'
      ? { id: 'statement-' + index, sql: statement }
      : statement
    if (!item || typeof item.sql !== 'string' || !item.sql.trim()) continue
    const baseId = typeof item.id === 'string' && item.id ? item.id : 'statement-' + index
    if (
      applied.has(baseId) ||
      appliedIds.some((appliedId) => appliedId.startsWith(baseId + ':'))
    ) {
      appliedStatementIds.add(baseId)
    }
    for (const id of Array.isArray(item.supersedes) ? item.supersedes : []) {
      supersededStatementIds.add(id)
    }
  }
  // CREATE TABLE IF NOT EXISTS is a SILENT NO-OP against a table that already
  // exists in an older shape, so every column that only ever appears inside a
  // CREATE definition is unreachable on a namespace whose table predates it,
  // and the first index over such a column fails the whole run forever (prod
  // 2026-07-26: 217 namespaces, ~2k billed rows per retry, for two days).
  //
  // this belongs here and not in the phantom-ledger pass, which is where it was
  // tried first and did nothing: that pass only considers LEDGERED statements,
  // and the ledger entry for a failing run's CREATE is deleted again by the
  // failure compensation below, so at the start of every retry the CREATE is
  // unledgered and the pass skipped it. converging here is independent of
  // ledger state, which is the only thing that made it reachable at all.
  for (const [index, statement] of nativeSqlStatements.entries()) {
    const item = typeof statement === 'string' ? null : statement
    if (!item || !Array.isArray(item.declaredColumns)) continue
    if (typeof item.sql !== 'string') continue
    const created = /^CREATE TABLE\\s+(?:IF NOT EXISTS\\s+)?[\`"]?(\\w+)/i.exec(item.sql.trim())
    if (!created) continue
    const baseId = typeof item.id === 'string' && item.id ? item.id : 'statement-' + index
    if (supersededStatementIds.has(baseId)) continue
    // PRAGMA, not the bulk sqlite_master scan: it resolves the table the same
    // way the statements do, and no rows means the table is genuinely absent,
    // so the CREATE below builds it correctly and there is nothing to converge.
    const liveRows = await tx.query(
      'PRAGMA table_info(' + quoteIdentifier(created[1]) + ')',
    )
    if (liveRows.length === 0) continue
    const live = new Set(liveRows.map((column) => String((column && column.name) ?? '')))
    for (const column of item.declaredColumns) {
      if (!column || typeof column.name !== 'string') continue
      if (live.has(column.name)) continue
      let definition = String(column.definition || '').trim()
      if (!definition) continue
      // sqlite refuses ADD COLUMN for these shapes; leave them to
      // assertExpectedSchema, which reports the namespace loudly.
      if (/\\bPRIMARY KEY\\b|\\bUNIQUE\\b/i.test(definition)) continue
      if (/\\bNOT NULL\\b/i.test(definition) && /\\bREFERENCES\\b/i.test(definition)) continue
      // a NOT NULL column with no default is skipped rather than given a
      // synthesised one. sqlite would accept ADD COLUMN ... NOT NULL DEFAULT '',
      // and every existing row would silently take that placeholder — inventing
      // data the migration author never wrote, on a table that already holds
      // real rows. the honest outcome is the loud one: assertExpectedSchema
      // names the column and the namespace reports instead of self-repairing.
      // no column that wedged production is of this shape.
      if (/\\bNOT NULL\\b/i.test(definition) && !/\\bDEFAULT\\b/i.test(definition)) continue
      console.warn(
        '[orez-migrations] converging ' + created[1] + '.' + column.name +
          ' declared by ' + baseId,
      )
      await tx.exec(
        'ALTER TABLE ' + quoteIdentifier(created[1]) + ' ADD COLUMN ' + definition,
      )
    }
  }
  // the ledger table is treated as replication bookkeeping by the DO backend,
  // so its writes are NOT covered by the transaction rollback: an aborted run
  // used to leave every already-recorded statement marked applied while its
  // data and DDL effects were undone, and later runs then skipped work that
  // never happened. compensate explicitly: on failure, delete the rows this
  // run inserted before rethrowing.
  const insertedThisRun = []
  try {
    for (const [index, statement] of nativeSqlStatements.entries()) {
      const item = typeof statement === 'string'
        ? { id: 'statement-' + index, sql: statement }
        : statement
      if (!item || typeof item.sql !== 'string' || !item.sql.trim()) continue
      // a statement that is only sql comments (a supersession-anchor
      // migration) has nothing to execute.
      if (!item.sql.split('\\n').some((line) => {
        const trimmed = line.trim()
        return trimmed.length > 0 && !trimmed.startsWith('--')
      })) continue
      const baseId = typeof item.id === 'string' && item.id ? item.id : 'statement-' + index
      if (supersededStatementIds.has(baseId)) continue
      const id = baseId + ':' + hashMigrationSql(item.sql)
      // migration statement ids are immutable execution identities. the SQL hash
      // records which source version ran, but editing an old migration must never
      // make destructive SQL run again under the same id.
      if (appliedStatementIds.has(baseId)) continue
      if (!(await shouldSkipStatement(tx, item))) {
        try {
          await tx.exec(item.sql, Array.isArray(item.params) ? item.params : [])
        } catch (error) {
          // a bare sqlite message ("no such table: x") names neither the failing
          // statement nor which of its siblings the ledger already recorded, and
          // that ledger state is the only thing that explains a half-applied
          // table-recreate block. the live column list of the target table is
          // the other half of that story for DDL chains.
          const file = baseId.split(':')[0]
          const siblings = [...applied].filter((appliedId) => appliedId.startsWith(file))
          let tableInfo = ''
          // CREATE INDEX belongs here as much as the DML shapes: "no such
          // column" on an index names a column the target table is missing,
          // and the table's real column list is the whole diagnosis. it also
          // has to come BEFORE the ledger list, which runs to hundreds of ids
          // and truncated every trailing byte of this off the stored error.
          const target =
            /(?:UPDATE|ALTER TABLE|DELETE FROM|INSERT INTO)\\s+[\`"]?(\\w+)/i.exec(item.sql) ||
            /^CREATE\\s+(?:UNIQUE\\s+)?INDEX\\s+(?:IF NOT EXISTS\\s+)?[\`"]?\\w+[\`"]?\\s+ON\\s+[\`"]?(\\w+)/i.exec(item.sql)
          if (target) {
            try {
              const columns = await tx.query('PRAGMA table_info(' + quoteIdentifier(target[1]) + ')')
              tableInfo = ' | table_info(' + target[1] + '): ' +
                (columns.map((column) => column.name + ':' + column.type).join(', ') ||
                  '(no columns)')
            } catch (infoError) {
              tableInfo = ' | table_info(' + target[1] + ') failed: ' +
                ((infoError && infoError.message) || String(infoError))
            }
          }
          throw new Error(
            'migration statement ' + baseId + ' failed on instance ' + instance + ': ' +
              (error && error.message ? error.message : String(error)) +
              ' | sql: ' + item.sql.replace(/\\s+/g, ' ').slice(0, 160) +
              tableInfo +
              ' | ledger for ' + file + ': ' + (siblings.join(', ') || '(none)'),
            { cause: error },
          )
        }
      }
      if (!applied.has(id)) {
        await tx.exec(
          'INSERT INTO ' + quoteIdentifier(migrationTable) + ' (id, applied_at) VALUES (?, ?)',
          [id, Date.now()],
        )
        applied.add(id)
        appliedStatementIds.add(baseId)
        insertedThisRun.push(id)
      }
    }
  } catch (error) {
    for (const insertedId of insertedThisRun) {
      try {
        await tx.exec(
          'DELETE FROM ' + quoteIdentifier(migrationTable) + ' WHERE id = ?',
          [insertedId],
        )
      } catch {}
    }
    throw error
  }
  await assertExpectedSchema(tx)
  await tx.exec(
    'CREATE TABLE IF NOT EXISTS _zero_schema_tables (name TEXT PRIMARY KEY, schema_json TEXT NOT NULL)',
  )
  try {
    for (const statement of schemaMetadataStatements()) {
      await tx.exec(statement.sql, statement.params)
    }
    await tx.registerTables(publicTables())
  } catch (error) {
    // publication runs against every registered table, so a table this schema
    // no longer has still gets queried here. without the label the error is
    // indistinguishable from a migration statement failing.
    throw new Error(
      'schema publication failed on instance ' + instance + ': ' +
        (error && error.message ? error.message : String(error)),
      { cause: error },
    )
  }
}

// what the namespace actually looks like, for a failure message. the single
// most expensive missing fact when a namespace is wedged is whether the table
// the error names is genuinely absent or merely unreachable — a whole day went
// into guessing that on 2026-07-23 because nothing ever said.
//
// runs on the error path only, in its own session, and must never throw: a
// namespace too broken to answer is exactly the case it exists to describe.
async function liveSchemaSummary(client) {
  try {
    let summary = ''
    await client.transaction(() => {
      throw new Error('native schema migration does not use queryAst')
    }, async (tx) => {
      const rows = await tx.query(
        "SELECT name FROM sqlite_master WHERE type = 'table'" +
          " AND name NOT LIKE '^_cf^_%' ESCAPE '^'" +
          " AND name NOT LIKE 'sqlite^_%' ESCAPE '^' ORDER BY name",
      )
      const present = new Set(rows.map((row) => row.name))
      const missing = expectedTables
        .map((table) => table.name)
        .filter((name) => !present.has(name))
      summary =
        ' | live tables: ' + present.size +
        (missing.length ? ', MISSING expected: ' + missing.join(', ') : ', none missing')
    })
    return summary
  } catch (error) {
    return ' | live schema unreadable: ' + (error && error.message ? error.message : String(error))
  }
}

// \`client\` lets a caller that is ALREADY inside the durable object hand in an
// in-process client (ZeroDO#applicationSqlLocalClient) instead of one that
// round-trips per statement. it is a parameter rather than another global
// because durable object instances share an isolate, so a global bound to one
// instance would be read by another.
export async function ${runCloudflareMigrations}({
  schemaOnly = false,
  registrationOnly = false,
  instance = 'singleton',
  client: providedClient,
} = {}) {
  if (schemaOnly && registrationOnly) {
    throw new Error('schemaOnly and registrationOnly are mutually exclusive')
  }
  let client = providedClient
  if (!client) {
    const createClient = globalThis.${applicationSqlGlobal}
    if (typeof createClient !== 'function') {
      throw new Error('Cloudflare application SQLite client is not initialized')
    }
    client = createClient(instance)
  }
  if (registrationOnly) {
    await client.registerTables(publicTables())
    return { tables: publicTables().map((table) => table.publicTable) }
  }
  // the statement loop labels its own failures; acquiring the session and
  // committing sit OUTSIDE every try/catch in applyNativeSchema, so a Durable
  // Object SQL error raised there arrives bare. the schema barrier hands that
  // string straight back as a 503 body, which is the entire diagnosis budget a
  // wedged namespace gets — prod has answered \`no such table: main.user:
  // SQLITE_ERROR\` since 2026-07-23 and it names neither the phase that raised
  // it nor whether that table is really gone. the phantom-ledger reconcile
  // heals a dropped table correctly (verified against a real durable object),
  // so what is left is only reachable through a message that says more.
  let phase = 'session-acquire'
  try {
    await client.transaction(() => {
      throw new Error('native schema migration does not use queryAst')
    }, async (tx) => {
      phase = 'reconcile'
      await applyNativeSchema(tx, instance)
      phase = 'commit'
    })
  } catch (error) {
    throw new Error(
      'schema migration failed on instance ' + instance + ' during ' + phase + ': ' +
        (error && error.message ? error.message : String(error)) +
        (await liveSchemaSummary(client)),
      { cause: error },
    )
  }
  return {
    tables: publicTables().map((table) => table.publicTable),
    ...(schemaOnly ? { schemaOnly: true } : null),
  }
}

export { ${runCloudflareMigrations} as runCloudflareMigrations }
`
  }

  const exhaustive: never = parts
  throw new TypeError(`unsupported Cloudflare migration mode: ${String(exhaustive)}`)
}
