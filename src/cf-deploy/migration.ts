import type { CfDeployConfig } from './config.js'

const SENTINEL_LOWER = 'nspfx'
const SENTINEL_PASCAL = 'Nspfx'

function applyPrefix(template: string, cfg: CfDeployConfig): string {
  return template
    .split(SENTINEL_PASCAL)
    .join(cfg.prefixPascal)
    .split(SENTINEL_LOWER)
    .join(cfg.prefix)
}

export interface CloudflareMigrationModuleFile {
  id: string
  importSpecifier: string
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
  | {
      mode: 'full'
      schemaVersion: string
      schemaImportSpecifier: string
      migrationFiles: CloudflareMigrationModuleFile[]
      initSql: string
      initSqlBatchStatements: unknown
      zeroHttpShardSql: string
      zeroHttpShardBatchStatements: unknown
    }

export function buildMigrationModuleSource(
  cfg: CfDeployConfig,
  parts: CloudflareMigrationModuleSourceParts
): string {
  const runCloudflareMigrations = applyPrefix('runNspfxCloudflareMigrations', cfg)
  const migrationTableName = applyPrefix('__nspfx_cf_migrations', cfg)
  const sqlFetchGlobal = applyPrefix('__nspfx_cf_do_sql_fetch_by_instance', cfg)

  if (parts.mode === 'noop') {
    return `export const SCHEMA_VERSION = ${JSON.stringify(parts.schemaVersion)}\nexport async function ${runCloudflareMigrations}() {}\n`
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
          const target = /(?:UPDATE|ALTER TABLE|DELETE FROM|INSERT INTO)\\s+[\`"]?(\\w+)/i.exec(item.sql)
          if (target) {
            try {
              const columns = await tx.query('PRAGMA table_info(' + quoteIdentifier(target[1]) + ')')
              tableInfo = ' | table_info(' + target[1] + '): ' +
                columns.map((column) => column.name + ':' + column.type).join(', ')
            } catch {}
          }
          throw new Error(
            'migration statement ' + baseId + ' failed on instance ' + instance + ': ' +
              (error && error.message ? error.message : String(error)) +
              ' | sql: ' + item.sql.replace(/\\s+/g, ' ').slice(0, 160) +
              ' | ledger for ' + file + ': ' + (siblings.join(', ') || '(none)') +
              tableInfo,
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
  publicationOnly = false,
  instance = 'singleton',
  client: providedClient,
} = {}) {
  if (schemaOnly && publicationOnly) {
    throw new Error('schemaOnly and publicationOnly are mutually exclusive')
  }
  let client = providedClient
  if (!client) {
    const createClient = globalThis.${applicationSqlGlobal}
    if (typeof createClient !== 'function') {
      throw new Error('Cloudflare application SQLite client is not initialized')
    }
    client = createClient(instance)
  }
  if (publicationOnly) {
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
`
  }

  const imports = [
    `import { Pool } from 'pg'`,
    `import { schema } from ${JSON.stringify(parts.schemaImportSpecifier)}`,
    ...parts.migrationFiles.map(
      (file, index) =>
        `import * as migration${index} from ${JSON.stringify(file.importSpecifier)}`
    ),
  ]
  const migrations = parts.migrationFiles
    .map((file, index) => {
      return `{ id: ${JSON.stringify(file.id)}, up: migration${index}.up }`
    })
    .join(',\n  ')

  return `${imports.join('\n')}

export const SCHEMA_VERSION = ${JSON.stringify(parts.schemaVersion)}

const migrations = [
  ${migrations}
].filter((migration) => typeof migration.up === 'function')

const migrationTable = '${migrationTableName}'

// regenerated-from-schema DDL (CREATE TABLE IF NOT EXISTS ...). always current
// vs the drizzle migrations dir, which the in-browser codegen never rewrites.
const initSql = ${JSON.stringify(parts.initSql)}
// the same DDL pre-translated at deploy time by orez's statement rewriter
// (deployTimeSchemaBatchStatements): SQLite-native DDL + the _orez_pg_metadata
// upserts that give the data tier its pg column types. /batch-applied with NO
// runtime libpg parse.
const initSqlBatchStatements = ${JSON.stringify(parts.initSqlBatchStatements)}
// zero-http uses the zero shard for clientGroupID ownership and mutation
// results. this repairs old persistent CF namespaces and creates the same
// shape for fresh namespaces before the first push.
const zeroHttpShardSql = ${JSON.stringify(parts.zeroHttpShardSql)}
const zeroHttpShardBatchStatements = ${JSON.stringify(parts.zeroHttpShardBatchStatements)}

function quoteIdentifier(value) {
  return '"' + String(value).replaceAll('"', '""') + '"'
}

// apply the full schema as ONE /batch of SQLite-native DDL straight to the SQL
// DO (ctx.storage.sql.exec, no parse, single transaction). this replaces ~118
// per-statement /exec calls, each of which paid the DoBackend client's
// libpg-query WASM parse (~1.4s) — the cold-migration loop that hit 36s + OOM.
// the DDL is IF-NOT-EXISTS so re-runs are no-ops. falls back to the node pg path
// off-CF (no per-instance DO fetch registry).
async function applyInitSqlDDL(client, instance) {
  const schemaMetadataStatements = Object.values(schema.tables || {})
    .filter((table) => table && typeof table.name === 'string')
    .map((table) => ({
      sql: 'INSERT OR REPLACE INTO _zero_schema_tables (name, schema_json) VALUES (?, ?)',
      params: [
        table.name,
        JSON.stringify({ columns: table.columns, primaryKey: table.primaryKey }),
      ],
    }))
  const batchStatements = [
    ...initSqlBatchStatements,
    ...zeroHttpShardBatchStatements,
    {
      sql: 'CREATE TABLE IF NOT EXISTS _zero_schema_tables (name TEXT PRIMARY KEY, schema_json TEXT NOT NULL)',
    },
    ...schemaMetadataStatements,
  ]
  const fetch = (globalThis.${sqlFetchGlobal} || {})[
    instance || 'singleton'
  ]
  if (typeof fetch === 'function') {
    if (batchStatements.length === 0) return
    const res = await fetch('https://orez-do-backend.local/batch', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ statements: batchStatements }),
    })
    if (!res.ok) {
      throw new Error('schema /batch failed: ' + res.status + ' ' + (await res.text()))
    }
    return
  }
  for (const sql of [initSql, zeroHttpShardSql]) {
    for (const statement of sql
      .split('--> statement-breakpoint')
      .map((statement) => statement.trim())
      .filter((statement) => statement.length > 0)) {
      await client.query(statement)
    }
  }
}

function tableNamesFromSchema() {
  return Object.values(schema.tables || {})
    .map((table) => table && table.name)
    .filter((name) => typeof name === 'string' && name.length > 0)
}

async function ensurePublication(client, publications) {
  const publicationName = publications[0]
  const schemaTables = tableNamesFromSchema()
  if (!publicationName || schemaTables.length === 0) return []

  // only publish tables that actually exist. a zero-schema table with no DDL
  // must not abort the whole publication (CREATE/ALTER PUBLICATION throws on a
  // missing relation), which would take every other table offline too. with
  // applyInitSqlDDL run first this is normally a no-op, but it keeps a single
  // drifted table from breaking all of replication.
  const existingTablesRes = await client.query(
    "SELECT tablename FROM pg_tables WHERE schemaname = 'public'",
  )
  const existingTables = new Set(existingTablesRes.rows.map((row) => row.tablename))
  const tableNames = schemaTables.filter((table) => existingTables.has(table))
  if (tableNames.length === 0) return []

  const existing = await client.query(
    'SELECT 1 FROM pg_publication WHERE pubname = $1',
    [publicationName],
  )
  if (!existing.rows.length) {
    await client.query(
      'CREATE PUBLICATION ' +
        quoteIdentifier(publicationName) +
        ' FOR TABLE ' +
        tableNames.map(quoteIdentifier).join(', '),
    )
    return tableNames
  }

  const current = await client.query(
    "SELECT tablename FROM pg_publication_tables WHERE pubname = $1 AND schemaname = 'public'",
    [publicationName],
  )
  const currentTables = new Set(current.rows.map((row) => row.tablename))
  const missing = tableNames.filter((table) => !currentTables.has(table))
  if (missing.length) {
    await client.query(
      'ALTER PUBLICATION ' +
        quoteIdentifier(publicationName) +
        ' ADD TABLE ' +
        missing.map(quoteIdentifier).join(', '),
    )
  }
  return tableNames
}

export async function ${runCloudflareMigrations}({
  publications = [],
  schemaOnly = false,
  publicationOnly = false,
  instance = 'singleton',
} = {}) {
  if (schemaOnly && publicationOnly) {
    throw new Error('schemaOnly and publicationOnly are mutually exclusive')
  }
  // the instance rides the connection string so the pg shim's backendFor
  // resolves THIS namespace's DO fetch (see cloudflarePgVirtualModule).
  const pool = new Pool({
    connectionString: 'orez-do://postgres?instance=' + encodeURIComponent(instance),
  })
  const client = await pool.connect()
  const onCloudflareDO =
    typeof (globalThis.${sqlFetchGlobal} || {})[instance] ===
    'function'
  try {
    // on Cloudflare every client.query() pays a libpg-query WASM parse
    // (~1.4s/statement) to translate PG->SQLite. so on CF we SKIP the drizzle
    // migration-tracking table + the no-op migration loop entirely (the
    // persisted SCHEMA_VERSION guard in ZeroCacheDO.migrateOnly already makes the
    // whole migration run once per schema) and rely on applyInitSqlDDL's single
    // pre-translated /batch. off-CF (node), keep the full drizzle tracking path.
    if (!onCloudflareDO) {
      await client.query(
        'CREATE TABLE IF NOT EXISTS ' +
          quoteIdentifier(migrationTable) +
          ' (id TEXT PRIMARY KEY, appliedAt BIGINT NOT NULL)',
      )
      for (const migration of migrations) {
        const seen = await client.query(
          'SELECT 1 FROM ' + quoteIdentifier(migrationTable) + ' WHERE id = $1',
          [migration.id],
        )
        if (seen.rows.length) continue
        await client.query('BEGIN')
        try {
          await migration.up(client)
          await client.query(
            'INSERT INTO ' +
              quoteIdentifier(migrationTable) +
              ' (id, appliedAt) VALUES ($1, $2)',
            [migration.id, Date.now()],
          )
          await client.query('COMMIT')
        } catch (err) {
          try {
            await client.query('ROLLBACK')
          } catch {}
          throw err
        }
      }
    }

    // ZeroCacheDO persists SCHEMA_VERSION and calls the schema path only when
    // that content hash changes. Embed boots still need to verify publication
    // membership, but must not replay the pre-translated schema batch: its
    // _orez_pg_metadata INSERT OR REPLACE statements write every schema row
    // even when the DDL itself is idempotent. Replaying that batch on every
    // reconnect produced the 2026-07-10 rows-written spike.
    if (!publicationOnly) await applyInitSqlDDL(client, instance)
    // schemaOnly: apply the table DDL but DEFER ensurePublication. on CF the
    // publication setup makes zero-cache/the pg-proxy register every published
    // table's schema (~6 queries x ~42 tables = ~260 libpg parses) — that parse
    // burst is what tips the app/cache-DO isolate over 128 MiB during the gate
    // (bootstrap/get-session). those writes only need the TABLES to exist, not the
    // publication; the publication is for /sync replication, so we run it lazily
    // in ensureReady (the /sync path) where the full embed already runs. off-CF,
    // node has no parse cost so callers don't pass schemaOnly.
    if (schemaOnly) return { tables: tableNamesFromSchema(), schemaOnly: true }
    const tables = await ensurePublication(client, publications)
    return { tables: tables || [] }
  } finally {
    client.release()
  }
}
`
}
