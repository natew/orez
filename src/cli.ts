#!/usr/bin/env node
import { spawn } from 'node:child_process'
import { existsSync, writeFileSync } from 'node:fs'
import { resolve } from 'node:path'

import { defineCommand, runMain } from 'citty'
import { deparseSync, loadModule, parseSync } from 'pgsql-parser'

import { startZeroLite } from './index.js'
import { log } from './log.js'

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
        extensions: { vector, pg_trgm },
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
    await db.exec(insert)
    executed += copyRows.length
    batchCount += copyRows.length
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

// try restoring via wire protocol (postgres running on given port)
// returns true if connected and restored, false if connection unavailable
async function tryWireRestore(opts: {
  port: number
  user: string
  password: string
  clean: boolean
  sqlFile: string
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
  })

  try {
    await sql`SELECT 1`
  } catch {
    await sql.end({ timeout: 0 }).catch(() => {})
    return false
  }

  // connected — restore errors should propagate, not fall back
  log.orez(`connected via wire protocol on port ${opts.port}`)
  try {
    if (opts.clean) {
      log.orez('dropping and recreating public schema')
      await sql.unsafe('DROP SCHEMA public CASCADE')
      await sql.unsafe('CREATE SCHEMA public')
    }

    const db = { exec: (query: string) => sql.unsafe(query) as Promise<unknown> }
    const { executed, skipped } = await execDumpFile(db, opts.sqlFile)
    log.orez(
      `restored ${opts.sqlFile} via wire protocol (${executed} statements, ${skipped} skipped)`
    )
    return true
  } finally {
    await sql.end()
  }
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

  const dataPath = resolve(opts.dataDir, 'pgdata-postgres')

  let db: InstanceType<typeof PGlite> | undefined
  try {
    db = new PGlite({
      dataDir: dataPath,
      extensions: { vector, pg_trgm },
      relaxedDurability: true,
    })
    await db.waitReady

    if (opts.clean) {
      log.orez('dropping and recreating public schema')
      await db.exec('DROP SCHEMA public CASCADE')
      await db.exec('CREATE SCHEMA public')
    }

    const { executed, skipped } = await execDumpFile(db, opts.sqlFile)
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
        })
        if (restored) return
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
      description: 'use native @rocicorp/zero-sqlite3 instead of wasm bedrock-sqlite',
      default: true,
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
  },
  subCommands: {
    s3: s3Command,
    pg_dump: pgDumpCommand,
    pg_restore: pgRestoreCommand,
  },
  async run({ args }) {
    const { config, stop } = await startZeroLite({
      pgPort: Number(args['pg-port']),
      zeroPort: Number(args['zero-port']),
      dataDir: args['data-dir'],
      migrationsDir: args.migrations,
      seedFile: args.seed,
      pgUser: args['pg-user'],
      pgPassword: args['pg-password'],
      skipZeroCache: args['skip-zero-cache'],
      disableWasmSqlite: args['disable-wasm-sqlite'],
      logLevel: (args['log-level'] as 'error' | 'warn' | 'info' | 'debug') || undefined,
      onDbReady: args['on-db-ready'],
    })

    let s3Server: import('node:http').Server | null = null
    if (args.s3) {
      const { startS3Local } = await import('./s3-local.js')
      s3Server = await startS3Local({
        port: Number(args['s3-port']),
        dataDir: args['data-dir'],
      })
    }

    log.orez('ready')
    log.orez(
      `pg: postgresql://${config.pgUser}:${config.pgPassword}@127.0.0.1:${config.pgPort}/postgres`
    )
    if (!config.skipZeroCache) {
      log.zero(`http://localhost:${config.zeroPort}`)
    }

    if (args['on-healthy']) {
      log.orez(`running on-healthy: ${args['on-healthy']}`)
      const child = spawn(args['on-healthy'], {
        shell: true,
        stdio: 'inherit',
        env: {
          ...process.env,
          OREZ_PG_PORT: String(config.pgPort),
          OREZ_ZERO_PORT: String(config.zeroPort),
        },
      })
      child.on('exit', (code) => {
        if (code !== 0 && code !== null) {
          log.orez(`on-healthy command exited with code ${code}`)
        }
      })
    }

    process.on('SIGINT', async () => {
      s3Server?.close()
      await stop()
      process.exit(0)
    })
    process.on('SIGTERM', async () => {
      s3Server?.close()
      await stop()
      process.exit(0)
    })
  },
})

runMain(main)
