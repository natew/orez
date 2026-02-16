#!/usr/bin/env node
import { spawn } from 'node:child_process'
import { existsSync, readFileSync, unlinkSync, writeFileSync } from 'node:fs'
import { resolve } from 'node:path'

import { defineCommand, runMain } from 'citty'
import { deparseSync, loadModule, parseSync } from 'pgsql-parser'

import { startZeroLite } from './index.js'
import { log, url } from './log.js'

// detect admin port from running orez instance
async function detectAdminPort(dataDir: string): Promise<number | null> {
  const pidFile = resolve(dataDir, 'orez.pid')
  const adminFile = resolve(dataDir, 'orez.admin')

  if (!existsSync(pidFile)) return null

  // check if admin port file exists
  if (existsSync(adminFile)) {
    try {
      const port = parseInt(readFileSync(adminFile, 'utf-8').trim(), 10)
      if (port > 0) return port
    } catch {}
  }

  // fallback: try common admin ports
  for (const port of [6477, 6478, 6479]) {
    try {
      const res = await fetch(`http://127.0.0.1:${port}/health`, {
        signal: AbortSignal.timeout(500),
      })
      if (res.ok) return port
    } catch {}
  }

  return null
}

const s3Command = defineCommand({
  meta: {
    name: 's3',
    description: 'start a local s3-compatible server',
  },
  args: {
    port: {
      type: 'string',
      description: 'port to listen on',
      default: '9200',
    },
    'data-dir': {
      type: 'string',
      description: 'data directory for stored files',
      default: '.orez',
    },
  },
  async run({ args }) {
    const { startS3Local } = await import('./s3-local.js')
    const server = await startS3Local({
      port: Number(args.port),
      dataDir: args['data-dir'],
    })

    process.on('SIGINT', () => {
      server.close()
      process.exit(0)
    })
    process.on('SIGTERM', () => {
      server.close()
      process.exit(0)
    })
  },
})

const pgDumpCommand = defineCommand({
  meta: {
    name: 'pg_dump',
    description: 'dump the pglite postgres database to a SQL file',
  },
  args: {
    'data-dir': {
      type: 'string',
      description: 'data directory',
      default: '.orez',
    },
    output: {
      type: 'string',
      description: 'output file path (default: stdout)',
      alias: 'o',
    },
  },
  async run({ args }) {
    const { PGlite } = await import('@electric-sql/pglite')
    const { vector } = await import('@electric-sql/pglite/vector')
    const { pg_trgm } = await import('@electric-sql/pglite/contrib/pg_trgm')
    const { pgcrypto } = await import('@electric-sql/pglite/contrib/pgcrypto')
    const { pgDump } = await import('@electric-sql/pglite-tools/pg_dump')

    const dataPath = resolve(args['data-dir'], 'pgdata-postgres')
    if (!existsSync(dataPath)) {
      console.error(`error: no database found at ${dataPath}`)
      process.exit(1)
    }

    let db: InstanceType<typeof PGlite> | undefined
    try {
      db = new PGlite({
        dataDir: dataPath,
        extensions: { vector, pg_trgm, pgcrypto },
      })
      await db.waitReady

      const file = await pgDump({ pg: db })
      const sql = await file.text()

      if (args.output) {
        writeFileSync(args.output, sql)
        log.orez(`dump written to ${args.output}`)
      } else {
        process.stdout.write(sql)
      }
    } catch (err: any) {
      if (err?.message?.includes('lock')) {
        console.error(
          'error: database is locked — stop orez first before running pg_dump'
        )
      } else {
        console.error(`error: ${err?.message ?? err}`)
      }
      process.exit(1)
    } finally {
      await db?.close()
    }
  },
})

// extensions that don't exist in pglite — skip during restore
const UNSUPPORTED_EXTENSIONS = new Set([
  'pg_stat_statements',
  'pg_buffercache',
  'pg_freespacemap',
  'pg_prewarm',
  'pg_stat_kcache',
  'pg_wait_sampling',
  'auto_explain',
  'pg_cron',
])

// check if a statement should be skipped during restore
function shouldSkipStatement(stmt: string): boolean {
  const trimmed = stmt.trimStart()
  // skip psql meta-commands like \restrict (can't be parsed)
  if (trimmed.startsWith('\\')) return true

  let parsed
  try {
    parsed = parseSync(trimmed)
  } catch {
    return false // if parser can't handle it, let pglite try
  }

  for (const entry of parsed.stmts) {
    const nodeType = Object.keys(entry.stmt)[0]
    const node = entry.stmt[nodeType]

    // skip SET transaction_timeout (pg 18+ artifact)
    if (nodeType === 'VariableSetStmt' && node.name === 'transaction_timeout') return true

    // skip CREATE EXTENSION for unsupported extensions
    if (nodeType === 'CreateExtensionStmt' && UNSUPPORTED_EXTENSIONS.has(node.extname))
      return true

    // skip DROP EXTENSION for unsupported extensions
    if (nodeType === 'DropStmt' && node.removeType === 'OBJECT_EXTENSION') {
      const extName = node.objects?.[0]?.String?.sval
      if (extName && UNSUPPORTED_EXTENSIONS.has(extName)) return true
    }

    // skip COMMENT ON EXTENSION for unsupported extensions
    if (nodeType === 'CommentStmt' && node.objtype === 'OBJECT_EXTENSION') {
      const extName = node.object?.String?.sval
      if (extName && UNSUPPORTED_EXTENSIONS.has(extName)) return true
    }

    // skip CREATE/ALTER/DROP PUBLICATION — pglite doesn't support wal_level=logical
    // internally, so CREATE PUBLICATION errors and can roll back the transaction.
    // orez handles replication via its own change tracker, not publications.
    if (nodeType === 'CreatePublicationStmt' || nodeType === 'AlterPublicationStmt')
      return true
    if (nodeType === 'DropStmt' && node.removeType === 'OBJECT_PUBLICATION') return true
  }

  return false
}

// how many data statements to batch into a single transaction
const BATCH_SIZE = 200
// run CHECKPOINT every N batches to flush WAL and reclaim wasm memory
const CHECKPOINT_INTERVAL = 3

// true for statements that are data manipulation (INSERT/UPDATE/DELETE)
// these get batched into transactions. DDL runs outside batches.
// note: COPY FROM stdin is handled separately by the copy-data converter
function isDataStatement(stmt: string): boolean {
  try {
    const parsed = parseSync(stmt)
    if (parsed.stmts.length === 0) return false
    const nodeType = Object.keys(parsed.stmts[0].stmt)[0]
    return (
      nodeType === 'InsertStmt' || nodeType === 'UpdateStmt' || nodeType === 'DeleteStmt'
    )
  } catch {
    return false
  }
}

// detect COPY ... FROM stdin and extract table + columns from AST
function parseCopyFromStdin(stmt: string): { table: string; columns: string[] } | null {
  try {
    const parsed = parseSync(stmt)
    if (parsed.stmts.length === 0) return null
    const node = parsed.stmts[0].stmt.CopyStmt
    if (!node || !node.is_from) return null
    const schema = node.relation.schemaname
    const table = schema
      ? `"${schema}"."${node.relation.relname}"`
      : `"${node.relation.relname}"`
    const columns = node.attlist ? node.attlist.map((a: any) => `"${a.String.sval}"`) : []
    return { table, columns }
  } catch {
    return null
  }
}

// convert a COPY text-format value to a SQL literal
// handles: \N → NULL, \\ → \, \t \n \r escapes, and single-quote escaping
function copyValueToLiteral(val: string): string {
  if (val === '\\N') return 'NULL'
  let result = ''
  for (let i = 0; i < val.length; i++) {
    if (val[i] === '\\' && i + 1 < val.length) {
      const next = val[i + 1]
      if (next === '\\') {
        result += '\\'
        i++
      } else if (next === 'n') {
        result += '\n'
        i++
      } else if (next === 'r') {
        result += '\r'
        i++
      } else if (next === 't') {
        result += '\t'
        i++
      } else {
        result += val[i]
      }
    } else {
      result += val[i]
    }
  }
  return "'" + result.replace(/'/g, "''") + "'"
}

// stream a sql dump file statement-by-statement with transaction batching
export async function execDumpFile(
  db: { exec: (sql: string) => Promise<unknown> },
  filePath: string
): Promise<{ executed: number; skipped: number }> {
  const { createReadStream } = await import('node:fs')
  const { createInterface } = await import('node:readline')

  const rl = createInterface({
    input: createReadStream(filePath, 'utf-8'),
    crlfDelay: Infinity,
  })

  let buf = ''
  let executed = 0
  let skipped = 0
  let batchCount = 0
  let batchesSinceCheckpoint = 0
  let inBatch = false
  let dollarTag: string | null = null // tracks $tag$ quoting

  // copy-data mode: when we hit COPY ... FROM stdin, we read data lines until \.
  let copyTarget: { table: string; columns: string[] } | null = null
  // accumulate COPY rows for multi-row INSERT (reduces statement count ~50x)
  const COPY_ROWS_PER_INSERT = 50
  // flush early if accumulated SQL exceeds this (prevents WASM OOM on huge rows)
  const COPY_BATCH_MAX_BYTES = 1_000_000
  // skip individual rows larger than this — PGlite WASM crashes around 24MB
  const MAX_ROW_BYTES = 16_000_000
  let copyRows: string[] = []
  let copyRowsBytes = 0

  async function flushCopyRows() {
    if (copyRows.length === 0 || !copyTarget) return
    const colList =
      copyTarget.columns.length > 0 ? ` (${copyTarget.columns.join(', ')})` : ''
    const insert = `INSERT INTO ${copyTarget.table}${colList} VALUES ${copyRows.join(', ')}`
    try {
      await db.exec(insert)
      executed += copyRows.length
      batchCount += copyRows.length
    } catch (err: any) {
      log.orez(`warning: ${err?.message?.split('\n')[0] ?? err}`)
      skipped += copyRows.length
      // transaction is aborted, rollback and start fresh
      if (inBatch) {
        try {
          await db.exec('ROLLBACK')
        } catch {}
        inBatch = false
        batchCount = 0
      }
    }
    copyRows = []
    copyRowsBytes = 0
  }

  async function flushBatch() {
    if (inBatch) {
      await db.exec('COMMIT')
      inBatch = false
      batchesSinceCheckpoint++
      if (batchesSinceCheckpoint >= CHECKPOINT_INTERVAL) {
        await db.exec('CHECKPOINT')
        batchesSinceCheckpoint = 0
      }
    }
    batchCount = 0
  }

  for await (const line of rl) {
    // in copy-data mode: read tab-delimited rows until \.
    if (copyTarget) {
      if (line === '\\.') {
        if (copyRows.length > 0) {
          if (!inBatch) {
            await db.exec('BEGIN')
            inBatch = true
          }
          await flushCopyRows()
        }
        copyTarget = null
        continue
      }
      const values = line.split('\t').map(copyValueToLiteral)
      const row = `(${values.join(', ')})`

      // skip rows that exceed WASM memory limits (~24MB crashes PGlite)
      if (row.length > MAX_ROW_BYTES) {
        log.orez(
          `skipping oversized row (${(row.length / 1_000_000).toFixed(1)}MB) in ${copyTarget.table}`
        )
        skipped++
        continue
      }

      // flush accumulated rows before adding if this would exceed size limit
      if (copyRows.length > 0 && copyRowsBytes + row.length > COPY_BATCH_MAX_BYTES) {
        if (!inBatch) {
          await db.exec('BEGIN')
          inBatch = true
        }
        await flushCopyRows()
        if (batchCount >= BATCH_SIZE) {
          await flushBatch()
        }
      }

      copyRows.push(row)
      copyRowsBytes += row.length
      if (
        copyRows.length >= COPY_ROWS_PER_INSERT ||
        copyRowsBytes >= COPY_BATCH_MAX_BYTES
      ) {
        if (!inBatch) {
          await db.exec('BEGIN')
          inBatch = true
        }
        await flushCopyRows()
        if (batchCount >= BATCH_SIZE) {
          await flushBatch()
        }
      }
      continue
    }

    // skip empty lines and sql comments (only outside dollar-quoted blocks)
    if (!dollarTag && (line === '' || line.startsWith('--'))) continue

    buf += (buf ? '\n' : '') + line

    // track dollar-quoting: $$ or $tag$
    const dollarMatches = line.matchAll(/(\$[a-zA-Z_]*\$)/g)
    for (const m of dollarMatches) {
      if (dollarTag === null) {
        dollarTag = m[1]
      } else if (m[1] === dollarTag) {
        dollarTag = null
      }
    }

    // can't end a statement while inside a dollar-quoted block
    if (dollarTag) continue

    // statements end with ; at end of line (pg_dump always formats this way)
    if (!line.trimEnd().endsWith(';')) continue

    const stmt = buf
    buf = ''

    if (shouldSkipStatement(stmt)) {
      skipped++
      continue
    }

    // check for COPY ... FROM stdin → convert to INSERTs
    const copyInfo = parseCopyFromStdin(stmt)
    if (copyInfo) {
      copyTarget = copyInfo
      continue
    }

    // rewrite statements to be idempotent so restores don't crash on "already exists"
    let rewritten = stmt
    try {
      const parsed = parseSync(stmt)
      if (parsed.stmts.length > 0) {
        const nodeType = Object.keys(parsed.stmts[0].stmt)[0]
        const node = parsed.stmts[0].stmt[nodeType]
        let modified = false

        // CREATE SCHEMA → CREATE SCHEMA IF NOT EXISTS
        if (nodeType === 'CreateSchemaStmt' && !node.if_not_exists) {
          node.if_not_exists = true
          modified = true
        }
        // CREATE FUNCTION/PROCEDURE → CREATE OR REPLACE
        if (nodeType === 'CreateFunctionStmt' && !node.replace) {
          node.replace = true
          modified = true
        }
        // CREATE VIEW → CREATE OR REPLACE VIEW
        if (nodeType === 'ViewStmt' && !node.replace) {
          node.replace = true
          modified = true
        }

        if (modified) rewritten = deparseSync(parsed)
      }
    } catch {
      // if parse/deparse fails, use original
    }

    if (isDataStatement(rewritten)) {
      // batch data statements into transactions
      if (!inBatch) {
        await db.exec('BEGIN')
        inBatch = true
      }
      try {
        await db.exec(rewritten)
        executed++
        batchCount++
        if (batchCount >= BATCH_SIZE) {
          await flushBatch()
        }
      } catch (err: any) {
        // non-fatal data errors (duplicate keys from internal tables, etc.)
        log.orez(`warning: ${err?.message?.split('\n')[0] ?? err}`)
        skipped++
        // transaction is aborted, rollback and start fresh
        try {
          await db.exec('ROLLBACK')
        } catch {}
        inBatch = false
        batchCount = 0
      }
    } else {
      // DDL runs outside batches
      await flushBatch()
      try {
        await db.exec(rewritten)
        executed++
      } catch (err: any) {
        // non-fatal DDL errors (missing tables from filtered dumps, etc.)
        log.orez(`warning: ${err?.message?.split('\n')[0] ?? err}`)
        skipped++
      }
    }
  }

  // flush remaining batch + buffer
  await flushBatch()
  if (buf.trim()) {
    if (!shouldSkipStatement(buf)) {
      await db.exec(buf)
      executed++
    } else {
      skipped++
    }
  }

  return { executed, skipped }
}

// after restore, drop triggers whose backing functions no longer exist.
// this happens when a filtered dump includes triggers on public-schema tables
// that reference functions from excluded schemas.
async function cleanupBrokenTriggers(db: { exec: (q: string) => Promise<unknown> }) {
  try {
    const result = (await db.exec(`
      SELECT tgname, relname, nspname, proname, pronamespace
      FROM pg_trigger t
      JOIN pg_class c ON c.oid = t.tgrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      LEFT JOIN pg_proc p ON p.oid = t.tgfoid
      WHERE NOT t.tgisinternal
        AND n.nspname = 'public'
        AND (p.oid IS NULL OR p.pronamespace != n.oid)
    `)) as any

    const rows = result?.rows || result?.[0]?.rows || []
    for (const row of rows) {
      const trigger = row.tgname
      const table = row.relname
      try {
        await db.exec(`DROP TRIGGER IF EXISTS "${trigger}" ON "public"."${table}"`)
        log.orez(`dropped broken trigger "${trigger}" on "${table}"`)
      } catch {}
    }
  } catch {
    // best-effort cleanup
  }
}

// try restoring via wire protocol (postgres running on given port)
// returns true if connected and restored, false if connection unavailable
async function tryWireRestore(opts: {
  port: number
  user: string
  password: string
  clean: boolean
  sqlFile: string
  dataDir: string
}): Promise<boolean> {
  const postgres = (await import('postgres')).default
  const sql = postgres({
    host: '127.0.0.1',
    port: opts.port,
    user: opts.user,
    password: opts.password,
    database: 'postgres',
    connect_timeout: 3,
    max: 1, // single connection so BEGIN/COMMIT work correctly
    onnotice: () => {}, // suppress pglite transaction warnings
  })

  try {
    await sql`SELECT 1`
  } catch {
    await sql.end({ timeout: 0 }).catch(() => {})
    return false
  }

  // connected — restore errors should propagate, not fall back
  log.orez(`connected via wire protocol on port ${opts.port}`)

  // automatically stop zero-cache before restore to prevent conflicts
  const adminPort = await detectAdminPort(opts.dataDir)
  if (adminPort) {
    log.orez('stopping zero-cache for restore...')
    try {
      await fetch(`http://127.0.0.1:${adminPort}/api/actions/stop-zero`, {
        method: 'POST',
        signal: AbortSignal.timeout(10_000),
      })
      // give zero-cache time to stop
      await new Promise((r) => setTimeout(r, 1000))
    } catch {
      log.orez('warning: could not stop zero-cache (may not be running)')
    }
  }

  try {
    const pubName = process.env.ZERO_APP_PUBLICATIONS?.trim()
    let pubTablesBeforeRestore: string[] = []
    if (pubName) {
      try {
        const existing = await sql<{ tablename: string }[]>`
          SELECT tablename
          FROM pg_publication_tables
          WHERE pubname = ${pubName}
            AND schemaname = 'public'
        `
        pubTablesBeforeRestore = existing.map((r) => r.tablename)
      } catch {
        // publication might not exist yet
      }
    }

    if (opts.clean) {
      log.orez('dropping and recreating public schema')
      await sql.unsafe('DROP SCHEMA public CASCADE')
      await sql.unsafe('CREATE SCHEMA public')
    }

    const db = { exec: (query: string) => sql.unsafe(query) as Promise<unknown> }
    const { executed, skipped } = await execDumpFile(db, opts.sqlFile)
    await cleanupBrokenTriggers(db)
    await db.exec('SET search_path TO public')
    log.orez(
      `restored ${opts.sqlFile} via wire protocol (${executed} statements, ${skipped} skipped)`
    )

    // clear zero replication state (in _orez schema)
    await sql.unsafe('TRUNCATE _orez._zero_changes').catch(() => {})
    await sql.unsafe('TRUNCATE _orez._zero_replication_slots').catch(() => {})
    log.orez('cleared zero replication state')

    // drop zero cdb cdc schemas so zero-cache can recreate them fresh
    const cdbSql = postgres({
      host: '127.0.0.1',
      port: opts.port,
      user: opts.user,
      password: opts.password,
      database: 'zero_cdb',
      connect_timeout: 3,
      max: 1,
      onnotice: () => {},
    })
    try {
      const cdcSchemas = await cdbSql<{ nspname: string }[]>`
        SELECT DISTINCT nspname FROM pg_namespace WHERE nspname LIKE '%/cdc'
      `
      for (const { nspname } of cdcSchemas) {
        await cdbSql.unsafe(`DROP SCHEMA IF EXISTS "${nspname}" CASCADE`).catch(() => {})
      }
      if (cdcSchemas.length > 0) {
        log.orez(`dropped ${cdcSchemas.length} cdc schema(s) from zero_cdb`)
      }
    } catch {
      // zero_cdb might not exist yet
    } finally {
      await cdbSql.end({ timeout: 1 }).catch(() => {})
    }

    if (pubName) {
      const quoted = '"' + pubName.replace(/"/g, '""') + '"'
      await sql.unsafe(`CREATE PUBLICATION ${quoted}`).catch(() => {})

      // Rebuild publication membership after restore so replication resumes
      // without requiring an app restart or migration rerun.
      const existingPublicTables = await sql<{ tablename: string }[]>`
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
          AND tablename NOT LIKE '_zero_%'
      `
      const existingSet = new Set(existingPublicTables.map((r) => r.tablename))

      // Prefer pre-restore publication membership; if unavailable, fall back to
      // ALL public tables (prod dumps don't have _0_version columns yet).
      const desired = new Set<string>(
        pubTablesBeforeRestore.filter((t) => existingSet.has(t))
      )
      if (desired.size === 0) {
        // Add all public tables except internal ones
        for (const { tablename } of existingPublicTables) {
          if (!tablename.startsWith('_')) {
            desired.add(tablename)
          }
        }
      }

      if (desired.size > 0) {
        const inPub = await sql<{ tablename: string }[]>`
          SELECT tablename
          FROM pg_publication_tables
          WHERE pubname = ${pubName}
            AND schemaname = 'public'
        `
        const inPubSet = new Set(inPub.map((r) => r.tablename))
        const toAdd = [...desired].filter((t) => !inPubSet.has(t))
        if (toAdd.length > 0) {
          const tableList = toAdd
            .map((t) => `"public"."${t.replace(/"/g, '""')}"`)
            .join(', ')
          await sql.unsafe(`ALTER PUBLICATION ${quoted} ADD TABLE ${tableList}`)
          log.orez(`added ${toAdd.length} table(s) to publication "${pubName}"`)
        }
      }

      const countRows = await sql<{ count: string }[]>`
        SELECT count(*)::text AS count
        FROM pg_publication_tables
        WHERE pubname = ${pubName}
          AND schemaname = 'public'
      `
      const count = Number(countRows[0]?.count || '0')
      log.orez(`publication "${pubName}" has ${count} table(s) after restore`)
    }

    // drop zero shard schemas to prevent conflicts when zero restarts
    const shardSchemas = await sql<{ nspname: string }[]>`
      SELECT nspname FROM pg_namespace
      WHERE nspname LIKE 'chat_%'
         OR nspname LIKE 'zero_%'
         OR nspname LIKE 'startchat_%'
    `
    for (const { nspname } of shardSchemas) {
      await sql.unsafe(`DROP SCHEMA IF EXISTS "${nspname}" CASCADE`).catch(() => {})
    }
    if (shardSchemas.length > 0) {
      log.orez(`dropped ${shardSchemas.length} shard schema(s)`)
    }

    log.orez('restore complete')
  } finally {
    await sql.end({ timeout: 1 })
  }

  // restart zero-cache so it recreates shard schemas fresh
  if (adminPort) {
    log.orez('restarting zero-cache...')
    try {
      await fetch(`http://127.0.0.1:${adminPort}/api/actions/restart-zero`, {
        method: 'POST',
        signal: AbortSignal.timeout(10_000),
      })
      log.orez('zero-cache restarting')
    } catch {
      log.orez('warning: could not restart zero-cache')
    }
  }

  return true
}

// restore by opening PGlite directly (requires no other process holding the lock)
async function directRestore(opts: {
  dataDir: string
  clean: boolean
  sqlFile: string
}): Promise<void> {
  const { PGlite } = await import('@electric-sql/pglite')
  const { vector } = await import('@electric-sql/pglite/vector')
  const { pg_trgm } = await import('@electric-sql/pglite/contrib/pg_trgm')
  const { pgcrypto } = await import('@electric-sql/pglite/contrib/pgcrypto')

  const dataPath = resolve(opts.dataDir, 'pgdata-postgres')

  let db: InstanceType<typeof PGlite> | undefined
  try {
    db = new PGlite({
      dataDir: dataPath,
      extensions: { vector, pg_trgm, pgcrypto },
      relaxedDurability: true,
    })
    await db.waitReady

    if (opts.clean) {
      log.orez('dropping and recreating public schema')
      await db.exec('DROP SCHEMA public CASCADE')
      await db.exec('CREATE SCHEMA public')
    }

    const { executed, skipped } = await execDumpFile(db, opts.sqlFile)
    await cleanupBrokenTriggers(db)
    await db.exec('SET search_path TO public')
    log.orez(
      `restored ${opts.sqlFile} into ${dataPath} (${executed} statements, ${skipped} skipped)`
    )
  } catch (err: any) {
    if (err?.message?.includes('lock')) {
      console.error(
        'error: database is locked — stop orez first before running pg_restore'
      )
    } else {
      console.error(`error: ${err?.message ?? err}`)
    }
    process.exit(1)
  } finally {
    await db?.close()
  }
}

const pgRestoreCommand = defineCommand({
  meta: {
    name: 'pg_restore',
    description: 'restore a SQL dump into the pglite postgres database',
  },
  args: {
    file: {
      type: 'positional',
      description: 'SQL file to restore',
      required: true,
    },
    'data-dir': {
      type: 'string',
      description: 'data directory',
      default: '.orez',
    },
    clean: {
      type: 'boolean',
      description: 'drop and recreate public schema before restoring',
      default: false,
    },
    'pg-port': {
      type: 'string',
      description: 'postgresql port for wire protocol connection',
      default: '6434',
    },
    'pg-user': {
      type: 'string',
      description: 'postgresql user',
      default: 'user',
    },
    'pg-password': {
      type: 'string',
      description: 'postgresql password',
      default: 'password',
    },
    direct: {
      type: 'boolean',
      description: 'force direct PGlite access, skip wire protocol auto-detection',
      default: false,
    },
  },
  async run({ args }) {
    await loadModule() // initialize pgsql-parser WASM

    const sqlFile = args.file
    if (!existsSync(sqlFile)) {
      console.error(`error: file not found: ${sqlFile}`)
      process.exit(1)
    }

    // try wire protocol first (unless --direct)
    if (!args.direct) {
      try {
        const restored = await tryWireRestore({
          port: Number(args['pg-port']),
          user: args['pg-user'],
          password: args['pg-password'],
          clean: args.clean,
          sqlFile,
          dataDir: args['data-dir'],
        })
        if (restored) {
          // ensure clean exit - don't let any lingering handles keep process alive
          process.exit(0)
        }
        log.orez('wire protocol unavailable, falling back to direct PGlite')
      } catch (err: any) {
        // connected but restore failed — report error, don't fall back
        console.error(`error: ${err?.message ?? err}`)
        process.exit(1)
      }
    }

    await directRestore({
      dataDir: args['data-dir'],
      clean: args.clean,
      sqlFile,
    })
  },
})

const main = defineCommand({
  meta: {
    name: 'orez',
    description: 'pglite-powered zero-sync development backend',
  },
  args: {
    'pg-port': {
      type: 'string',
      description: 'postgresql proxy port',
      default: '6434',
    },
    'zero-port': {
      type: 'string',
      description: 'zero-cache port',
      default: '5849',
    },
    'data-dir': {
      type: 'string',
      description: 'data directory',
      default: '.orez',
    },
    migrations: {
      type: 'string',
      description: 'migrations directory',
      default: '',
    },
    seed: {
      type: 'string',
      description: 'seed file path',
      default: '',
    },
    'pg-user': {
      type: 'string',
      description: 'postgresql user',
      default: 'user',
    },
    'pg-password': {
      type: 'string',
      description: 'postgresql password',
      default: 'password',
    },
    'skip-zero-cache': {
      type: 'boolean',
      description: 'run pglite + proxy only, skip zero-cache',
      default: false,
    },
    'log-level': {
      type: 'string',
      description: 'log level: error, warn, info, debug (default: warn)',
    },
    s3: {
      type: 'boolean',
      description: 'also start a local s3-compatible server',
      default: false,
    },
    's3-port': {
      type: 'string',
      description: 's3 server port',
      default: '9200',
    },
    'disable-wasm-sqlite': {
      type: 'boolean',
      description: 'force native @rocicorp/zero-sqlite3 (fails if not available)',
      default: false,
    },
    'force-wasm-sqlite': {
      type: 'boolean',
      description: 'force wasm bedrock-sqlite even if native is available',
      default: false,
    },
    'on-db-ready': {
      type: 'string',
      description: 'command to run after db+proxy are ready, before zero-cache starts',
      default: '',
    },
    'on-healthy': {
      type: 'string',
      description: 'command to run once all services are healthy',
      default: '',
    },
    'disable-admin': {
      type: 'boolean',
      description: 'disable admin dashboard',
      default: false,
    },
    'admin-port': {
      type: 'string',
      description: 'admin dashboard port',
      default: '6477',
    },
  },
  subCommands: {
    s3: s3Command,
    pg_dump: pgDumpCommand,
    pg_restore: pgRestoreCommand,
  },
  async run({ args }) {
    const adminPort = args['disable-admin'] ? 0 : Number(args['admin-port'])
    const {
      config,
      stop,
      zeroEnv,
      logStore,
      httpLog,
      restartZero,
      stopZero,
      resetZero,
      resetZeroFull,
    } = await startZeroLite({
      pgPort: Number(args['pg-port']),
      zeroPort: Number(args['zero-port']),
      adminPort,
      dataDir: args['data-dir'],
      migrationsDir: args.migrations,
      seedFile: args.seed,
      pgUser: args['pg-user'],
      pgPassword: args['pg-password'],
      skipZeroCache: args['skip-zero-cache'],
      disableWasmSqlite: args['disable-wasm-sqlite'],
      forceWasmSqlite: args['force-wasm-sqlite'],
      logLevel: (args['log-level'] as 'error' | 'warn' | 'info' | 'debug') || undefined,
      onDbReady: args['on-db-ready'] || undefined,
      onHealthy: args['on-healthy'] || undefined,
    })

    let s3Server: import('node:http').Server | null = null
    if (args.s3) {
      const { startS3Local } = await import('./s3-local.js')
      s3Server = await startS3Local({
        port: Number(args['s3-port']),
        dataDir: args['data-dir'],
      })
    }

    let adminServer: import('node:http').Server | null = null
    if (!args['disable-admin'] && logStore && zeroEnv) {
      const { startAdminServer } = await import('./admin/server.js')
      adminServer = await startAdminServer({
        port: config.adminPort,
        logStore,
        httpLog,
        config,
        zeroEnv,
        actions: { restartZero, stopZero, resetZero, resetZeroFull },
        startTime: Date.now(),
      })
      log.orez(`admin: ${url(`http://localhost:${config.adminPort}`)}`)
    }

    log.pg(
      `ready ${url(`postgresql://${config.pgUser}:${config.pgPassword}@127.0.0.1:${config.pgPort}/postgres`)}`
    )

    let stopping = false
    const shutdown = async (reason: string, exitCode = 0) => {
      if (stopping) return
      stopping = true
      log.debug.orez(`shutdown requested: ${reason}`)
      adminServer?.close()
      s3Server?.close()
      await stop()
      process.exit(exitCode)
    }

    process.on('SIGINT', () => shutdown('SIGINT'))
    process.on('SIGTERM', () => shutdown('SIGTERM'))

    // handle crashes - try to clean up so next startup isn't corrupted
    process.on('uncaughtException', async (err) => {
      log.orez(`uncaught exception: ${err.message}`)
      await shutdown('uncaughtException', 1)
    })
    process.on('unhandledRejection', async (reason) => {
      log.orez(`unhandled rejection: ${reason}`)
      await shutdown('unhandledRejection', 1)
    })
  },
})

// only run CLI when executed directly (not when imported by tests)
// import.meta.main is Bun-specific; for Node, check if this file is the entry point
const isMain =
  typeof (import.meta as any).main === 'boolean'
    ? (import.meta as any).main
    : process.argv[1]?.endsWith('cli.js') || process.argv[1]?.endsWith('cli.ts')
if (isMain) {
  runMain(main)
}
