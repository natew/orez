import { sha256 } from '@noble/hashes/sha2.js'

export interface NamespaceBackupStatement {
  sql: string
  params?: readonly unknown[]
}

export interface NamespaceBackupObject {
  body?: ReadableStream<Uint8Array>
  /** Immutable object identity supplied by R2. */
  etag?: string
  json(): Promise<unknown>
}

export interface NamespaceBackupBucket {
  createMultipartUpload(key: string): Promise<{
    uploadPart(partNumber: number, value: Uint8Array): Promise<unknown>
    complete(parts: readonly unknown[]): Promise<unknown>
    abort(): Promise<unknown>
  }>
  get(key: string): Promise<NamespaceBackupObject | null>
  put(key: string, value: string): Promise<unknown>
  list(options: { prefix: string }): Promise<{
    objects?: readonly { key: string }[]
  }>
  delete(keys: readonly string[]): Promise<unknown>
}

export interface NamespaceBackupSummary {
  ns: string
  key: string
  marker: number
  exportedAt: string
  tables: number
  rows: number
  tableRows: Record<string, number>
  bytes: number
  parts: number
}

export interface NamespaceRestoreSummary {
  ok: true
  ns: string
  key: string
  sourceNs: string
  tables: number
  rows: number
  counts: Record<string, number>
}

export interface NamespaceBackupOptions<Env> {
  format: string
  /** Older on-disk formats accepted for restore but never emitted. */
  acceptedFormats?: readonly string[]
  markerTable: string
  files(env: Env): NamespaceBackupBucket
  query(
    env: Env,
    namespace: string,
    sql: string,
    params: readonly unknown[]
  ): Promise<Record<string, any>[]>
  /**
   * Run `work` against a single read session that excludes application writers
   * for its whole life.
   *
   * The export scan spans one statement per table page. Run through `query`,
   * each of those is its own session, so a commit lands between two of them and
   * the dump holds a state no transaction ever produced: a parent row read
   * before the write and its child rows read after it. The scan therefore owns
   * one session for its whole length.
   */
  readSession<Value>(
    env: Env,
    namespace: string,
    work: (
      query: (sql: string, params?: readonly unknown[]) => Promise<Record<string, any>[]>
    ) => Promise<Value>
  ): Promise<Value>
  batch(
    env: Env,
    namespace: string,
    statements: readonly NamespaceBackupStatement[]
  ): Promise<void>
  listNamespaces(env: Env): Promise<readonly string[]>
  /** Runs only after validation and the fresh-namespace guard pass. */
  beforeImport?(env: Env, namespace: string): Promise<void>
  afterImport?(env: Env, namespace: string): Promise<void>
  excludedTables?: readonly string[]
  prefix?(namespace: string): string
  logPrefix?: string
  keep?: number
  keepControlPlane?: number
  controlPlaneNamespace?: string
  runBudgetMs?: number
  partBytes?: number
  chunkTargetBytes?: number
}

export interface NamespaceBackupManager<Env> {
  backupPrefix(namespace: string): string
  readMarker(env: Env, namespace: string): Promise<number>
  exportNamespace(env: Env, namespace: string): Promise<NamespaceBackupSummary>
  importNamespace(
    env: Env,
    namespace: string,
    key: string,
    options?: { allowNonEmpty?: boolean }
  ): Promise<NamespaceRestoreSummary>
  pruneBackups(env: Env, namespace: string): Promise<void>
  runScheduledBackups(env: Env): Promise<{
    exported: number
    skipped: number
    failed: number
  }>
}

const REPLICATION_BOOKKEEPING_TABLES = new Set([
  '_zero_changes',
  '_zero_pending_changes',
  '_zero_change_state',
  '_orez___zero_watermark',
  '_orez___zero_streamed_batches',
  '_orez__zero_replication_slots',
])

function quoteIdentifier(value: string) {
  return value.replaceAll('"', '""')
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error)
}

function referencedTables(createSql: unknown): string[] {
  const sql = String(createSql ?? '')
  const references: string[] = []
  let awaitingTable = false
  let index = 0
  while (index < sql.length) {
    const character = sql[index]!
    if (/\s/.test(character) || ',();'.includes(character)) {
      index++
      continue
    }
    if (character === '-' && sql[index + 1] === '-') {
      index += 2
      while (index < sql.length && sql[index] !== '\n') index++
      continue
    }
    if (character === '/' && sql[index + 1] === '*') {
      const end = sql.indexOf('*/', index + 2)
      index = end === -1 ? sql.length : end + 2
      continue
    }
    if (character === "'") {
      index++
      while (index < sql.length) {
        if (sql[index] !== "'") {
          index++
          continue
        }
        if (sql[index + 1] === "'") {
          index += 2
          continue
        }
        index++
        break
      }
      continue
    }
    if (character === '"' || character === '`' || character === '[') {
      const close = character === '[' ? ']' : character
      let value = ''
      index++
      while (index < sql.length) {
        if (sql[index] !== close) {
          value += sql[index]
          index++
          continue
        }
        if (sql[index + 1] === close) {
          value += close
          index += 2
          continue
        }
        index++
        break
      }
      if (awaitingTable && value) {
        references.push(value)
        awaitingTable = false
      }
      continue
    }
    let end = index + 1
    while (end < sql.length && !/[\s,();]/.test(sql[end]!)) end++
    const token = sql.slice(index, end)
    index = end
    if (awaitingTable) {
      references.push(token)
      awaitingTable = false
    } else if (token.toUpperCase() === 'REFERENCES') {
      awaitingTable = true
    }
  }
  return references
}

function tableDependencies(
  createSql: unknown,
  tableName: string,
  namesBySqlIdentity: ReadonlyMap<string, string>
): string[] {
  const self = tableName.toLowerCase()
  return [
    ...new Set(
      referencedTables(createSql)
        .map((reference) => namesBySqlIdentity.get(reference.toLowerCase()))
        .filter(
          (dependency): dependency is string =>
            dependency !== undefined && dependency.toLowerCase() !== self
        )
    ),
  ]
}

function tableIdentities(names: readonly string[]): Map<string, string> {
  return new Map(names.map((name) => [name.toLowerCase(), name]))
}

function dependencyOrder(
  names: readonly string[],
  dependencies: ReadonlyMap<string, readonly string[]>
): string[] {
  const ordered: string[] = []
  const done = new Set<string>()
  const visiting = new Set<string>()
  const visit = (name: string) => {
    if (done.has(name) || visiting.has(name)) return
    visiting.add(name)
    for (const dependency of dependencies.get(name) ?? []) visit(dependency)
    visiting.delete(name)
    done.add(name)
    ordered.push(name)
  }
  for (const name of names) visit(name)
  return ordered
}

async function* ndjsonLines(stream: ReadableStream<Uint8Array>) {
  const decoder = new TextDecoder()
  const reader = stream.getReader()
  let carry = ''
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    carry += decoder.decode(value, { stream: true })
    let index = carry.indexOf('\n')
    while (index !== -1) {
      const line = carry.slice(0, index)
      carry = carry.slice(index + 1)
      if (line) yield line
      index = carry.indexOf('\n')
    }
  }
  carry += decoder.decode()
  if (carry.trim()) yield carry
}

function hex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('')
}

/**
 * Streaming, bounded-memory SQLite namespace backups for Orez Lite.
 *
 * Storage transport and namespace policy are injected. Orez owns the backup
 * format mechanics; an application owns how it reaches its Durable Object,
 * which namespaces exist, and what must happen after a restore.
 */
export function createNamespaceBackupManager<Env>(
  options: NamespaceBackupOptions<Env>
): NamespaceBackupManager<Env> {
  const partBytes = options.partBytes ?? 8 * 1024 * 1024
  const chunkTargetBytes = options.chunkTargetBytes ?? 2 * 1024 * 1024
  const keep = options.keep ?? 10
  const keepControlPlane = options.keepControlPlane ?? 30
  const controlPlaneNamespace = options.controlPlaneNamespace ?? 'singleton'
  const runBudgetMs = options.runBudgetMs ?? 10 * 60 * 1000
  const excludedTables = new Set(options.excludedTables ?? [])
  const acceptedFormats = new Set([options.format, ...(options.acceptedFormats ?? [])])
  const backupPrefix =
    options.prefix ?? ((namespace: string) => `backups/${namespace.replace(':', '/')}/`)

  const isExcluded = (name: unknown) => {
    const table = String(name)
    return (
      table.startsWith('sqlite_') ||
      table.startsWith('_cf_') ||
      table.startsWith('_orez_tx_') ||
      /^[A-Za-z0-9_]+_0\.(?:clients|mutations)$/.test(table) ||
      excludedTables.has(table) ||
      REPLICATION_BOOKKEEPING_TABLES.has(table)
    )
  }

  const log = (fields: Record<string, unknown>) => {
    console.log(
      JSON.stringify({ event: 'orez_backup', format: options.format, ...fields })
    )
  }

  type SessionQuery = (
    sql: string,
    params?: readonly unknown[]
  ) => Promise<Record<string, any>[]>

  const readMarkerWith = async (query: SessionQuery) => {
    try {
      const rows = await query(
        `SELECT write_seq FROM "${quoteIdentifier(options.markerTable)}" WHERE id = 1`,
        []
      )
      return Number(rows[0]?.write_seq) || 0
    } catch (error) {
      if (/no such table/i.test(errorMessage(error))) return 0
      throw error
    }
  }

  const readMarker = (env: Env, namespace: string) =>
    readMarkerWith((sql, params = []) => options.query(env, namespace, sql, params))

  const exportNamespace = async (
    env: Env,
    namespace: string
  ): Promise<NamespaceBackupSummary> => {
    const startedAt = Date.now()
    const files = options.files(env)
    const exportedAt = new Date().toISOString()
    const key = `${backupPrefix(namespace)}${Date.now()}.ndjson`
    // One read session for the whole scan. Every page below reads the same
    // committed state, so the dump is a database that actually existed.
    const scan = await options.readSession(env, namespace, async (read) => {
      // Read before scanning. A concurrent write then leaves the live marker
      // ahead of latest.json and guarantees another backup.
      const marker = await readMarkerWith(read)
      const master = await read(
        "SELECT name, sql, type, tbl_name FROM sqlite_master WHERE type IN ('table', 'index') AND sql IS NOT NULL ORDER BY name",
        []
      )
      const unorderedTables = master.filter(
        (row) => row.type === 'table' && !isExcluded(row.name)
      )
      const tableNames = unorderedTables.map((row) => String(row.name))
      const tableNamesBySqlIdentity = tableIdentities(tableNames)
      // sqlite_master already carries every CREATE statement in this bounded
      // schema read. Derive FK edges from those statements instead of asking
      // pragma_foreign_key_list to re-walk the complete schema once per table.
      const dependencies = new Map(
        unorderedTables.map((table) => [
          String(table.name),
          tableDependencies(table.sql, String(table.name), tableNamesBySqlIdentity),
        ])
      )
      const orderedNames = dependencyOrder(
        unorderedTables.map((table) => String(table.name)),
        dependencies
      )
      const tableByName = new Map(
        unorderedTables.map((table) => [String(table.name), table])
      )
      const tables = orderedNames.map((name) => tableByName.get(name)!)
      const indexes = master.filter(
        (row) =>
          row.type === 'index' && !isExcluded(row.name) && !isExcluded(row.tbl_name)
      )
      const upload = await files.createMultipartUpload(key)
      const uploadedParts: unknown[] = []
      const encoder = new TextEncoder()
      let chunks: Uint8Array[] = []
      let bufferedBytes = 0
      let totalBytes = 0
      const digest = sha256.create()

      const flushParts = async (final: boolean) => {
        if (!final && bufferedBytes < partBytes) return
        let merged = new Uint8Array(bufferedBytes)
        let offset = 0
        for (const chunk of chunks) {
          merged.set(chunk, offset)
          offset += chunk.byteLength
        }
        while (merged.byteLength >= partBytes) {
          uploadedParts.push(
            await upload.uploadPart(uploadedParts.length + 1, merged.slice(0, partBytes))
          )
          merged = merged.slice(partBytes)
        }
        if (final && (merged.byteLength > 0 || uploadedParts.length === 0)) {
          uploadedParts.push(await upload.uploadPart(uploadedParts.length + 1, merged))
          merged = new Uint8Array(0)
        }
        chunks = merged.byteLength ? [merged] : []
        bufferedBytes = merged.byteLength
      }

      const writeLine = async (value: unknown, includeInDigest = true) => {
        const bytes = encoder.encode(`${JSON.stringify(value)}\n`)
        if (includeInDigest) digest.update(bytes)
        chunks.push(bytes)
        bufferedBytes += bytes.byteLength
        totalBytes += bytes.byteLength
        await flushParts(false)
        return bytes.byteLength
      }

      let rowTotal = 0
      const tableRows: Record<string, number> = {}
      try {
        await writeLine({
          kind: 'header',
          format: options.format,
          integrity: 'sha256',
          ns: namespace,
          exportedAt,
          marker,
          orderedTables: true,
        })
        for (const table of tables) {
          const name = String(table.name)
          const withoutRowid = /\bWITHOUT\s+ROWID\b/i.test(String(table.sql))
          const primaryKeyColumns = withoutRowid
            ? (await read(`PRAGMA table_info("${quoteIdentifier(name)}")`, []))
                .filter((column) => Number(column.pk) > 0)
                .sort((left, right) => Number(left.pk) - Number(right.pk))
                .map((column) => String(column.name))
            : []
          if (withoutRowid && primaryKeyColumns.length === 0) {
            throw new Error(`WITHOUT ROWID table ${name} has no primary key`)
          }
          const quotedPrimaryKey = primaryKeyColumns
            .map((column) => `"${quoteIdentifier(column)}"`)
            .join(', ')
          let tableRowTotal = 0
          await writeLine({
            kind: 'table',
            name,
            sql: table.sql,
            indexes: indexes
              .filter((index) => index.tbl_name === name)
              .map((index) => index.sql),
          })
          let rowidCursor: unknown = 0
          let primaryKeyCursor: unknown[] | null = null
          let limit = 200
          while (true) {
            const usedLimit = limit
            const rows: Record<string, unknown>[] = withoutRowid
              ? await read(
                  primaryKeyCursor
                    ? `SELECT * FROM "${quoteIdentifier(name)}" WHERE (${quotedPrimaryKey}) > (${primaryKeyColumns.map(() => '?').join(', ')}) ORDER BY ${quotedPrimaryKey} LIMIT ?`
                    : `SELECT * FROM "${quoteIdentifier(name)}" ORDER BY ${quotedPrimaryKey} LIMIT ?`,
                  primaryKeyCursor ? [...primaryKeyCursor, usedLimit] : [usedLimit]
                )
              : await read(
                  `SELECT rowid AS __orez_backup_rowid, * FROM "${quoteIdentifier(name)}" WHERE rowid > ? ORDER BY rowid LIMIT ?`,
                  [rowidCursor, usedLimit]
                )
            if (rows.length === 0) break
            if (withoutRowid) {
              const last = rows.at(-1)!
              primaryKeyCursor = primaryKeyColumns.map((column) => last[column])
            } else {
              rowidCursor = rows.at(-1)?.__orez_backup_rowid
              for (const row of rows) delete row.__orez_backup_rowid
            }
            const lineBytes = await writeLine({ kind: 'rows', table: name, rows })
            rowTotal += rows.length
            tableRowTotal += rows.length
            const perRow = Math.max(1, Math.ceil(lineBytes / rows.length))
            limit = Math.max(20, Math.min(1000, Math.floor(chunkTargetBytes / perRow)))
            if (rows.length < usedLimit) break
          }
          tableRows[name] = tableRowTotal
        }
        await writeLine(
          {
            kind: 'footer',
            tables: tables.length,
            rows: rowTotal,
            sha256: hex(digest.digest()),
          },
          false
        )
        await flushParts(true)
        await upload.complete(uploadedParts)
      } catch (error) {
        try {
          await upload.abort()
        } catch {
          // Preserve the original export failure.
        }
        log({
          phase: 'export_upload',
          outcome: 'error',
          namespace,
          durationMs: Date.now() - startedAt,
          error: errorMessage(error),
        })
        throw error
      }

      return {
        marker,
        tables: tables.length,
        rows: rowTotal,
        tableRows,
        bytes: totalBytes,
        parts: uploadedParts.length,
      }
    })

    const summary = {
      ns: namespace,
      key,
      exportedAt,
      ...scan,
    }
    let keepPreviousLatest = false
    if (scan.rows === 0) {
      try {
        const previous = await files.get(`${backupPrefix(namespace)}latest.json`)
        if (previous) {
          const previousSummary = (await previous.json()) as { rows?: unknown }
          keepPreviousLatest = Number(previousSummary.rows) > 0
        }
      } catch {
        // A missing/corrupt pointer must not prevent a new valid backup.
      }
    }
    if (!keepPreviousLatest) {
      await files.put(`${backupPrefix(namespace)}latest.json`, JSON.stringify(summary))
    }
    log({
      phase: 'export',
      outcome: 'success',
      namespace,
      durationMs: Date.now() - startedAt,
      rows: summary.rows,
      bytes: summary.bytes,
      parts: summary.parts,
    })
    return summary
  }

  const importNamespace = async (
    env: Env,
    namespace: string,
    key: string,
    importOptions: { allowNonEmpty?: boolean } = {}
  ): Promise<NamespaceRestoreSummary> => {
    const startedAt = Date.now()
    const files = options.files(env)
    const validationObject = await files.get(key)
    if (!validationObject?.body) throw new Error(`backup object not found: ${key}`)

    type TableEntry = {
      name: string
      sql: string
      indexes: string[]
    }
    let validatedHeader:
      | {
          ns?: unknown
          format?: unknown
          integrity?: unknown
          orderedTables?: unknown
        }
      | undefined
    let validatedFooter: { rows?: unknown; sha256?: unknown } | undefined
    let validatedRows = 0
    const validationDigest = sha256.create()
    const encoder = new TextEncoder()
    const tableEntries: TableEntry[] = []
    const seenTables = new Set<string>()
    for await (const line of ndjsonLines(validationObject.body)) {
      const entry = JSON.parse(line) as Record<string, any>
      if (entry.kind === 'header') {
        if (validatedHeader) throw new Error('backup contains multiple headers')
        if (!acceptedFormats.has(String(entry.format))) {
          throw new Error(`unsupported backup format: ${entry.format}`)
        }
        validatedHeader = entry
      } else if (entry.kind === 'table') {
        const name = String(entry.name ?? '')
        if (!name || seenTables.has(name)) {
          throw new Error(`invalid or duplicate backup table: ${name}`)
        }
        seenTables.add(name)
        tableEntries.push({
          name,
          sql: String(entry.sql ?? ''),
          indexes: Array.isArray(entry.indexes)
            ? entry.indexes.map((sql: unknown) => String(sql))
            : [],
        })
      } else if (entry.kind === 'rows') {
        if (!Array.isArray(entry.rows) || !seenTables.has(String(entry.table))) {
          throw new Error('invalid backup rows entry')
        }
        validatedRows += entry.rows.length
      } else if (entry.kind === 'footer') {
        if (validatedFooter) throw new Error('backup contains multiple footers')
        validatedFooter = entry
      } else {
        throw new Error(`unsupported backup entry kind: ${String(entry.kind)}`)
      }
      if (entry.kind !== 'footer') validationDigest.update(encoder.encode(`${line}\n`))
    }
    if (!validatedHeader || !validatedFooter) {
      throw new Error(`backup is truncated or not a supported dump`)
    }
    if (Number(validatedFooter.rows) !== validatedRows) {
      throw new Error(
        `backup row count mismatch: footer says ${validatedFooter.rows}, read ${validatedRows}`
      )
    }

    if (
      validatedHeader.integrity !== undefined &&
      validatedHeader.integrity !== 'sha256'
    ) {
      throw new Error(
        `unsupported backup integrity: ${String(validatedHeader.integrity)}`
      )
    }

    const statedDigest =
      typeof validatedFooter.sha256 === 'string' ? validatedFooter.sha256 : null
    const actualDigest = hex(validationDigest.digest())
    const requiresDigest =
      validatedHeader.integrity === 'sha256' ||
      String(validatedHeader.format) === 'orez-backup-v2'
    if (requiresDigest && statedDigest === null) {
      throw new Error('backup footer is missing its sha256 digest')
    }
    if (statedDigest !== null && statedDigest !== actualDigest) {
      throw new Error('backup sha256 digest mismatch')
    }
    log({
      phase: 'restore_validation',
      outcome: 'success',
      namespace,
      durationMs: Date.now() - startedAt,
      rows: validatedRows,
      digest: statedDigest === null ? 'legacy_absent' : 'verified',
    })

    // This is the same schema query restore already needed for dependency-safe
    // drops, moved before the first mutation. Default restores are fresh-only;
    // destructive replacement requires an explicit operator override.
    const liveTableRows = await options.query(
      env,
      namespace,
      "SELECT name, sql FROM sqlite_master WHERE type = 'table' AND sql IS NOT NULL ORDER BY name",
      []
    )
    const liveTableNames = liveTableRows
      .map((row) => String(row.name ?? ''))
      .filter((name) => name && !isExcluded(name))
    if (liveTableNames.length > 0 && importOptions.allowNonEmpty !== true) {
      throw new Error(
        `restore target is not empty (${liveTableNames.length} application tables); pass the explicit replacement override`
      )
    }

    const object = await files.get(key)
    if (!object?.body) throw new Error(`backup object disappeared during restore: ${key}`)
    if (validationObject.etag && object.etag && validationObject.etag !== object.etag) {
      throw new Error('backup object changed between validation and restore')
    }

    await options.beforeImport?.(env, namespace)

    for (const name of REPLICATION_BOOKKEEPING_TABLES) {
      try {
        await options.query(env, namespace, `DELETE FROM "${quoteIdentifier(name)}"`, [])
      } catch {
        // Fresh namespaces do not have every bookkeeping table.
      }
    }

    const header = validatedHeader
    const footer = validatedFooter
    let rowTotal = 0
    let skippedRows = 0
    const tableNames = tableEntries
      .map((entry) => entry.name)
      .filter((name) => !isExcluded(name))
    const bufferedRows = new Map<string, Record<string, unknown>[]>()
    const insertSql = new Map<string, string>()

    const statementsForRows = (
      name: string,
      rows: readonly Record<string, unknown>[]
    ): NamespaceBackupStatement[] =>
      rows.map((row) => {
        const columns = Object.keys(row)
        const signature = `${name}\0${columns.join('\0')}`
        let sql = insertSql.get(signature)
        if (!sql) {
          sql =
            `INSERT INTO "${quoteIdentifier(name)}" (` +
            columns.map((column) => `"${quoteIdentifier(column)}"`).join(', ') +
            `) VALUES (${columns.map(() => '?').join(', ')})`
          insertSql.set(signature, sql)
        }
        return {
          sql,
          params: columns.map((column) => row[column]),
        }
      })

    const insertRows = async (name: string, rows: readonly Record<string, unknown>[]) => {
      for (let offset = 0; offset < rows.length; offset += 400) {
        await options.batch(
          env,
          namespace,
          statementsForRows(name, rows.slice(offset, offset + 400))
        )
      }
      rowTotal += rows.length
    }

    const dropNames = [...new Set([...tableNames, ...liveTableNames])]
    const dropNamesBySqlIdentity = tableIdentities(dropNames)
    const dropDependencies = new Map(
      liveTableRows
        .map((row) => ({ name: String(row.name ?? ''), sql: row.sql }))
        .filter((row) => row.name && !isExcluded(row.name))
        .map((row) => [
          row.name,
          tableDependencies(row.sql, row.name, dropNamesBySqlIdentity),
        ])
    )
    const dropStatements = dependencyOrder(dropNames, dropDependencies)
      .reverse()
      .map((name) => ({
        sql: `DROP TABLE IF EXISTS "${quoteIdentifier(name)}"`,
      }))
    // workerd's DROP TABLE schema work grows with the number of live tables.
    // Keep each destructive storage transaction small even though ordinary
    // row inserts can safely use the larger 400-statement import batches.
    const destructiveBatchSize = 40
    for (let offset = 0; offset < dropStatements.length; offset += destructiveBatchSize) {
      await options.batch(
        env,
        namespace,
        dropStatements.slice(offset, offset + destructiveBatchSize)
      )
    }
    const includedEntries = tableEntries.filter((entry) => !isExcluded(entry.name))
    for (let offset = 0; offset < includedEntries.length; offset += 400) {
      await options.batch(
        env,
        namespace,
        includedEntries.slice(offset, offset + 400).map((entry) => ({
          sql: entry.sql,
        }))
      )
    }

    for await (const line of ndjsonLines(object.body)) {
      const entry = JSON.parse(line) as Record<string, any>
      if (entry.kind === 'header') {
      } else if (entry.kind === 'table') {
      } else if (entry.kind === 'rows') {
        if (isExcluded(entry.table)) {
          skippedRows += entry.rows.length
          continue
        }
        const name = String(entry.table)
        if (validatedHeader.orderedTables === true) {
          await insertRows(name, entry.rows)
        } else {
          const rows = bufferedRows.get(name) ?? []
          for (const row of entry.rows) rows.push(row)
          bufferedRows.set(name, rows)
        }
      } else if (entry.kind === 'footer') {
      }
    }

    if (validatedHeader.orderedTables !== true) {
      const tableNamesBySqlIdentity = tableIdentities(tableNames)
      const dependencies = new Map(
        includedEntries.map((entry) => [
          entry.name,
          tableDependencies(entry.sql, entry.name, tableNamesBySqlIdentity),
        ])
      )
      const ordered = dependencyOrder(tableNames, dependencies)
      for (const name of ordered) await insertRows(name, bufferedRows.get(name) ?? [])
    }

    const indexStatements = includedEntries.flatMap((entry) =>
      entry.indexes.map((sql) => ({ sql }))
    )
    for (let offset = 0; offset < indexStatements.length; offset += 400) {
      await options.batch(env, namespace, indexStatements.slice(offset, offset + 400))
    }

    if (Number(footer.rows) !== rowTotal + skippedRows) {
      throw new Error(
        `row count mismatch: footer says ${footer.rows}, imported ${rowTotal} + skipped bookkeeping ${skippedRows}`
      )
    }
    const counts: Record<string, number> = {}
    for (const name of tableNames) {
      const rows = await options.query(
        env,
        namespace,
        `SELECT COUNT(*) AS n FROM "${quoteIdentifier(name)}"`,
        []
      )
      counts[name] = Number(rows[0]?.n) || 0
    }
    await options.afterImport?.(env, namespace)
    const summary = {
      ok: true,
      ns: namespace,
      key,
      sourceNs: String(header.ns ?? ''),
      tables: tableNames.length,
      rows: rowTotal,
      counts,
    } as const
    log({
      phase: 'restore',
      outcome: 'success',
      namespace,
      durationMs: Date.now() - startedAt,
      rows: rowTotal,
      tables: tableNames.length,
      replacement: importOptions.allowNonEmpty === true,
    })
    return summary
  }

  const pruneBackups = async (env: Env, namespace: string) => {
    const files = options.files(env)
    const prefix = backupPrefix(namespace)
    const listed = await files.list({ prefix })
    const dumps = (listed.objects ?? [])
      .filter((object) => /\/\d+\.(ndjson|json)$/.test(object.key))
      .sort((left, right) => (left.key < right.key ? -1 : 1))
    const retained = namespace === controlPlaneNamespace ? keepControlPlane : keep
    const excess = dumps.slice(0, Math.max(0, dumps.length - retained))
    if (excess.length > 0) {
      await files.delete(excess.map((object) => object.key))
    }
  }

  const runScheduledBackups = async (env: Env) => {
    const started = Date.now()
    const namespaces = [...(await options.listNamespaces(env))]
    for (let index = namespaces.length - 1; index > 0; index--) {
      const other = Math.floor(Math.random() * (index + 1))
      ;[namespaces[index], namespaces[other]] = [namespaces[other], namespaces[index]]
    }
    let exported = 0
    let skipped = 0
    let failed = 0
    for (const namespace of namespaces) {
      if (Date.now() - started > runBudgetMs) {
        log({
          phase: 'scheduled',
          outcome: 'budget_exhausted',
          durationMs: Date.now() - started,
        })
        break
      }
      try {
        const marker = await readMarker(env, namespace)
        const latest = await options
          .files(env)
          .get(`${backupPrefix(namespace)}latest.json`)
        if (latest) {
          const previous = (await latest.json()) as { marker?: unknown }
          if (Number(previous.marker) === marker) {
            skipped++
            continue
          }
        }
        const summary = await exportNamespace(env, namespace)
        await pruneBackups(env, namespace)
        exported++
        log({
          phase: 'scheduled_namespace',
          outcome: 'success',
          namespace,
          rows: summary.rows,
          bytes: summary.bytes,
        })
      } catch (error) {
        failed++
        log({
          phase: 'scheduled_namespace',
          outcome: 'error',
          namespace,
          error: errorMessage(error),
        })
      }
    }
    log({
      phase: 'scheduled',
      outcome: failed > 0 ? 'partial' : 'success',
      exported,
      skipped,
      failed,
      durationMs: Date.now() - started,
    })
    return { exported, skipped, failed }
  }

  return {
    backupPrefix,
    readMarker,
    exportNamespace,
    importNamespace,
    pruneBackups,
    runScheduledBackups,
  }
}
