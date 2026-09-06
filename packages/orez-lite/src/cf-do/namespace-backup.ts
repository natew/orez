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

export type NamespaceBackupExportResult = {
  outcome: 'exported'
  summary: NamespaceBackupSummary
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

export interface NamespaceBackupSnapshotOptions {
  markerTable: string
  excludedTables: readonly string[]
}

export interface NamespaceBackupSchemaRow {
  name: string
  sql: string
  type: string
  tbl_name: string
}

export interface NamespaceBackupSnapshot {
  id: string
  lease: { [Symbol.dispose](): void }
  marker: number
  tables: string[]
  columns: Record<string, string[]>
  schema: NamespaceBackupSchemaRow[]
}

export function isNamespaceBackupTableExcluded(
  name: string,
  excludedTables: ReadonlySet<string>
): boolean {
  return (
    name.startsWith('sqlite_') ||
    name.startsWith('_cf_') ||
    name.startsWith('_orez_tx_') ||
    name.startsWith('_orez_bk_') ||
    /^[A-Za-z0-9_]+_0\.(?:clients|mutations)$/.test(name) ||
    excludedTables.has(name) ||
    REPLICATION_BOOKKEEPING_TABLES.has(name)
  )
}

export interface NamespaceBackupOptions<Env> {
  format: string
  /** Older on-disk formats accepted for restore but never emitted. */
  acceptedFormats?: readonly string[]
  markerTable: string
  files(env: Env): NamespaceBackupBucket
  /**
   * one statement in its own admitted turn. the export pages its immutable
   * snapshot through this, so no read session is ever held across a network
   * hop and an arriving writer waits for one synchronous statement at most.
   */
  query(
    env: Env,
    namespace: string,
    sql: string,
    params: readonly unknown[]
  ): Promise<Record<string, any>[]>
  snapshot(
    env: Env,
    namespace: string,
    options: NamespaceBackupSnapshotOptions
  ): Promise<NamespaceBackupSnapshot>
  dropSnapshot(env: Env, namespace: string, id: string): Promise<void>
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
  /**
   * multipart uploads allowed to remain outstanding; bounded by worker memory.
   */
  maxInflightParts?: number
}

export interface NamespaceBackupManager<Env> {
  backupPrefix(namespace: string): string
  readMarker(env: Env, namespace: string): Promise<number>
  exportNamespace(env: Env, namespace: string): Promise<NamespaceBackupExportResult>
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
  const maxInflightParts = Math.max(1, options.maxInflightParts ?? 4)
  const excludedTables = new Set(options.excludedTables ?? [])
  const acceptedFormats = new Set([options.format, ...(options.acceptedFormats ?? [])])
  const backupPrefix =
    options.prefix ?? ((namespace: string) => `backups/${namespace.replace(':', '/')}/`)

  const isExcluded = (name: unknown) =>
    isNamespaceBackupTableExcluded(String(name), excludedTables)

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

  type ExportTable = {
    name: string
    sql: string
    indexes: string[]
    columns: string[]
    snapshotName: string
  }

  type ScanLine = { bytes: Uint8Array; digested: boolean }

  const encoder = new TextEncoder()

  const encodeLine = (value: unknown, digested = true): ScanLine => ({
    bytes: encoder.encode(`${JSON.stringify(value)}\n`),
    digested,
  })

  const readScanSchema = ({
    id,
    marker,
    schema: master,
    columns,
    tables: snapshotTables,
  }: NamespaceBackupSnapshot) => {
    const included = new Set(snapshotTables)
    const unorderedTables = master.filter(
      (row) => row.type === 'table' && included.has(row.name)
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
    const orderedNames = dependencyOrder(tableNames, dependencies)
    const tableByName = new Map(
      unorderedTables.map((table) => [String(table.name), table])
    )
    const indexes = master.filter(
      (row) => row.type === 'index' && !isExcluded(row.name) && !isExcluded(row.tbl_name)
    )
    const tables: ExportTable[] = []
    for (const name of orderedNames) {
      const row = tableByName.get(name)!
      const sql = String(row.sql)
      tables.push({
        name,
        sql,
        columns: columns[name]!,
        snapshotName: `_orez_bk_${id}_${name}`,
        indexes: indexes
          .filter((index) => index.tbl_name === name)
          .map((index) => String(index.sql)),
      })
    }
    return { marker, tables }
  }

  /**
   * copy once under writer admission, then page the copy without fencing live
   * writes. every page is one single-statement read in its own admitted turn,
   * so there is no session for a writer to evict and nothing to retry: the
   * snapshot cannot change, and the export finishes at the pace of its pages.
   */
  const runScan = async (
    env: Env,
    namespace: string,
    key: string,
    exportedAt: string
  ): Promise<{
    marker: number
    tables: number
    rows: number
    tableRows: Record<string, number>
    bytes: number
    parts: number
  }> => {
    const files = options.files(env)
    const snapshotStarted = performance.now()
    const snapshot = await options.snapshot(env, namespace, {
      markerTable: options.markerTable,
      excludedTables: [...excludedTables],
    })
    log({
      phase: 'snapshot',
      outcome: 'success',
      namespace,
      rpcDurationMs: performance.now() - snapshotStarted,
    })
    try {
      const { marker, tables } = readScanSchema(snapshot)

      const upload = await files.createMultipartUpload(key)
      const partUploads: Promise<unknown>[] = []
      let chunks: Uint8Array[] = []
      let bufferedBytes = 0
      let totalBytes = 0
      let rowTotal = 0
      const tableRows: Record<string, number> = {}
      const digest = sha256.create()

      // an abort that races an in-flight uploadPart can leave the part behind, and
      // this bucket already carries thousands of orphans. settle first, always.
      const abortUpload = async () => {
        await Promise.allSettled(partUploads)
        await upload.abort().catch(() => {})
      }

      const sendPart = async (value: Uint8Array) => {
        const pending = upload.uploadPart(partUploads.length + 1, value)
        // Nothing awaits this until the scan is done, so keep workerd from
        // reporting a rejection that the final Promise.all will surface anyway.
        void pending.catch(() => {})
        partUploads.push(pending)
        const bound = partUploads.length - maxInflightParts
        if (bound >= 0) await partUploads[bound]
      }

      const flushParts = async (final: boolean) => {
        if (!final && bufferedBytes < partBytes) return
        let merged = new Uint8Array(bufferedBytes)
        let offset = 0
        for (const chunk of chunks) {
          merged.set(chunk, offset)
          offset += chunk.byteLength
        }
        while (merged.byteLength >= partBytes) {
          await sendPart(merged.slice(0, partBytes))
          merged = merged.slice(partBytes)
        }
        if (final && (merged.byteLength > 0 || partUploads.length === 0)) {
          await sendPart(merged)
          merged = new Uint8Array(0)
        }
        chunks = merged.byteLength ? [merged] : []
        bufferedBytes = merged.byteLength
      }

      const appendLine = (line: ScanLine) => {
        if (line.digested) digest.update(line.bytes)
        chunks.push(line.bytes)
        bufferedBytes += line.bytes.byteLength
        totalBytes += line.bytes.byteLength
      }

      try {
        appendLine(
          encodeLine({
            kind: 'header',
            format: options.format,
            integrity: 'sha256',
            ns: namespace,
            exportedAt,
            marker,
            orderedTables: true,
          })
        )
        for (const table of tables) {
          appendLine(
            encodeLine({
              kind: 'table',
              name: table.name,
              sql: table.sql,
              indexes: table.indexes,
            })
          )
          tableRows[table.name] = 0
          let rowidCursor: unknown = 0
          let limit = 200
          while (true) {
            const rows = await options.query(
              env,
              namespace,
              `SELECT rowid AS __orez_backup_rowid, * FROM "${quoteIdentifier(table.snapshotName)}" WHERE rowid > ? ORDER BY rowid LIMIT ?`,
              [rowidCursor, limit]
            )
            if (rows.length === 0) break
            rowidCursor = rows.at(-1)?.__orez_backup_rowid
            const line = encodeLine({
              kind: 'rows',
              table: table.name,
              rows: rows.map((row) =>
                Object.fromEntries(
                  table.columns.map((column, index) => [column, row[`c${index}`]])
                )
              ),
            })
            appendLine(line)
            tableRows[table.name] += rows.length
            rowTotal += rows.length
            await flushParts(false)
            if (rows.length < limit) break
            const perRow = Math.max(1, Math.ceil(line.bytes.byteLength / rows.length))
            limit = Math.max(20, Math.min(1000, Math.floor(chunkTargetBytes / perRow)))
          }
        }
        appendLine(
          encodeLine(
            {
              kind: 'footer',
              tables: tables.length,
              rows: rowTotal,
              sha256: hex(digest.digest()),
            },
            false
          )
        )
        await flushParts(true)
        await upload.complete(await Promise.all(partUploads))
      } catch (error) {
        await abortUpload()
        throw error
      }

      return {
        marker,
        tables: tables.length,
        rows: rowTotal,
        tableRows,
        bytes: totalBytes,
        parts: partUploads.length,
      }
    } finally {
      try {
        await options.dropSnapshot(env, namespace, snapshot.id)
      } catch (error) {
        log({
          phase: 'snapshot_drop',
          outcome: 'error',
          namespace,
          error: errorMessage(error),
        })
      } finally {
        snapshot.lease[Symbol.dispose]()
      }
    }
  }

  const exportNamespace = async (
    env: Env,
    namespace: string
  ): Promise<NamespaceBackupExportResult> => {
    const startedAt = Date.now()
    const files = options.files(env)
    const exportedAt = new Date().toISOString()
    const key = `${backupPrefix(namespace)}${Date.now()}.ndjson`
    let scan: Awaited<ReturnType<typeof runScan>>
    try {
      scan = await runScan(env, namespace, key, exportedAt)
    } catch (error) {
      log({
        phase: 'export_upload',
        outcome: 'error',
        namespace,
        durationMs: Date.now() - startedAt,
        error: errorMessage(error),
      })
      throw error
    }

    const summary = {
      ns: namespace,
      key,
      exportedAt,
      marker: scan.marker,
      tables: scan.tables,
      rows: scan.rows,
      tableRows: scan.tableRows,
      bytes: scan.bytes,
      parts: scan.parts,
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
    return { outcome: 'exported', summary }
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
        const result = await exportNamespace(env, namespace)
        await pruneBackups(env, namespace)
        exported++
        log({
          phase: 'scheduled_namespace',
          outcome: 'success',
          namespace,
          rows: result.summary.rows,
          bytes: result.summary.bytes,
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
