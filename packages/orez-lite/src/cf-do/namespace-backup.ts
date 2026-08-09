export interface NamespaceBackupStatement {
  sql: string
  params?: readonly unknown[]
}

export interface NamespaceBackupObject {
  body?: ReadableStream<Uint8Array>
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
  batch(
    env: Env,
    namespace: string,
    statements: readonly NamespaceBackupStatement[]
  ): Promise<void>
  listNamespaces(env: Env): Promise<readonly string[]>
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
    key: string
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
  const logPrefix = options.logPrefix ?? '[orez]'
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

  const readMarker = async (env: Env, namespace: string) => {
    try {
      const rows = await options.query(
        env,
        namespace,
        `SELECT write_seq FROM "${quoteIdentifier(options.markerTable)}" WHERE id = 1`,
        []
      )
      return Number(rows[0]?.write_seq) || 0
    } catch (error) {
      if (/no such table/i.test(errorMessage(error))) return 0
      throw error
    }
  }

  const exportNamespace = async (
    env: Env,
    namespace: string
  ): Promise<NamespaceBackupSummary> => {
    const files = options.files(env)
    const exportedAt = new Date().toISOString()
    // Read before scanning. A concurrent write then leaves the live marker
    // ahead of latest.json and guarantees another backup.
    const marker = await readMarker(env, namespace)
    const master = await options.query(
      env,
      namespace,
      "SELECT name, sql, type, tbl_name FROM sqlite_master WHERE type IN ('table', 'index') AND sql IS NOT NULL ORDER BY name",
      []
    )
    const unorderedTables = master.filter(
      (row) => row.type === 'table' && !isExcluded(row.name)
    )
    const tableNames = new Set(unorderedTables.map((row) => String(row.name)))
    const dependencies = new Map<string, string[]>()
    for (const table of unorderedTables) {
      const name = String(table.name)
      const foreignKeys = await options.query(
        env,
        namespace,
        `PRAGMA foreign_key_list("${quoteIdentifier(name)}")`,
        []
      )
      dependencies.set(
        name,
        foreignKeys
          .map((foreignKey) => String(foreignKey.table))
          .filter((dependency) => dependency !== name && tableNames.has(dependency))
      )
    }
    const orderedNames: string[] = []
    const ordered = new Set<string>()
    const visiting = new Set<string>()
    const visit = (name: string) => {
      if (ordered.has(name) || visiting.has(name)) return
      visiting.add(name)
      for (const dependency of dependencies.get(name) ?? []) visit(dependency)
      visiting.delete(name)
      ordered.add(name)
      orderedNames.push(name)
    }
    for (const table of unorderedTables) visit(String(table.name))
    const tableByName = new Map(
      unorderedTables.map((table) => [String(table.name), table])
    )
    const tables = orderedNames.map((name) => tableByName.get(name)!)
    const indexes = master.filter(
      (row) => row.type === 'index' && !isExcluded(row.name) && !isExcluded(row.tbl_name)
    )
    const key = `${backupPrefix(namespace)}${Date.now()}.ndjson`
    const upload = await files.createMultipartUpload(key)
    const uploadedParts: unknown[] = []
    const encoder = new TextEncoder()
    let chunks: Uint8Array[] = []
    let bufferedBytes = 0
    let totalBytes = 0

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

    const writeLine = async (value: unknown) => {
      const bytes = encoder.encode(`${JSON.stringify(value)}\n`)
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
        ns: namespace,
        exportedAt,
        marker,
        orderedTables: true,
      })
      for (const table of tables) {
        const name = String(table.name)
        const withoutRowid = /\bWITHOUT\s+ROWID\b/i.test(String(table.sql))
        const primaryKeyColumns = withoutRowid
          ? (
              await options.query(
                env,
                namespace,
                `PRAGMA table_info("${quoteIdentifier(name)}")`,
                []
              )
            )
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
            ? await options.query(
                env,
                namespace,
                primaryKeyCursor
                  ? `SELECT * FROM "${quoteIdentifier(name)}" WHERE (${quotedPrimaryKey}) > (${primaryKeyColumns.map(() => '?').join(', ')}) ORDER BY ${quotedPrimaryKey} LIMIT ?`
                  : `SELECT * FROM "${quoteIdentifier(name)}" ORDER BY ${quotedPrimaryKey} LIMIT ?`,
                primaryKeyCursor ? [...primaryKeyCursor, usedLimit] : [usedLimit]
              )
            : await options.query(
                env,
                namespace,
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
      await writeLine({ kind: 'footer', tables: tables.length, rows: rowTotal })
      await flushParts(true)
      await upload.complete(uploadedParts)
    } catch (error) {
      try {
        await upload.abort()
      } catch {
        // Preserve the original export failure.
      }
      throw error
    }

    const summary = {
      ns: namespace,
      key,
      marker,
      exportedAt,
      tables: tables.length,
      rows: rowTotal,
      tableRows,
      bytes: totalBytes,
      parts: uploadedParts.length,
    }
    let keepPreviousLatest = false
    if (rowTotal === 0) {
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
    return summary
  }

  const importNamespace = async (
    env: Env,
    namespace: string,
    key: string
  ): Promise<NamespaceRestoreSummary> => {
    const files = options.files(env)
    const validationObject = await files.get(key)
    if (!validationObject?.body) throw new Error(`backup object not found: ${key}`)

    type TableEntry = {
      name: string
      sql: string
      indexes: string[]
    }
    let validatedHeader:
      | { ns?: unknown; format?: unknown; orderedTables?: unknown }
      | undefined
    let validatedFooter: { rows?: unknown } | undefined
    let validatedRows = 0
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
        validatedFooter = entry
      } else {
        throw new Error(`unsupported backup entry kind: ${String(entry.kind)}`)
      }
    }
    if (!validatedHeader || !validatedFooter) {
      throw new Error(`backup is truncated or not a supported dump`)
    }
    if (Number(validatedFooter.rows) !== validatedRows) {
      throw new Error(
        `backup row count mismatch: footer says ${validatedFooter.rows}, read ${validatedRows}`
      )
    }

    const object = await files.get(key)
    if (!object?.body) throw new Error(`backup object disappeared during restore: ${key}`)

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

    for (let offset = 0; offset < tableNames.length; offset += 400) {
      await options.batch(
        env,
        namespace,
        tableNames
          .slice()
          .reverse()
          .slice(offset, offset + 400)
          .map((name) => ({
            sql: `DROP TABLE IF EXISTS "${quoteIdentifier(name)}"`,
          }))
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
      const dependencies = new Map<string, string[]>()
      for (const name of tableNames) {
        const foreignKeys = await options.query(
          env,
          namespace,
          `PRAGMA foreign_key_list("${quoteIdentifier(name)}")`,
          []
        )
        dependencies.set(
          name,
          foreignKeys
            .map((foreignKey) => String(foreignKey.table))
            .filter((table) => table !== name && tableNames.includes(table))
        )
      }
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
      for (const name of tableNames) visit(name)
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
    return {
      ok: true,
      ns: namespace,
      key,
      sourceNs: String(header.ns ?? ''),
      tables: tableNames.length,
      rows: rowTotal,
      counts,
    }
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
        console.log(`${logPrefix} backup run: wall budget reached`)
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
        console.log(
          `${logPrefix} backup: ${namespace} -> ${summary.key} (${summary.rows} rows, ${summary.bytes} bytes)`
        )
      } catch (error) {
        failed++
        console.log(`${logPrefix} backup failed for ${namespace}: ${errorMessage(error)}`)
      }
    }
    console.log(
      `${logPrefix} backup run: exported ${exported} skipped ${skipped} failed ${failed} in ${Date.now() - started}ms`
    )
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
