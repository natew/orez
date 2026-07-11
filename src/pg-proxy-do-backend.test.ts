import { createServer, type Server } from 'node:http'

import Database from '@rocicorp/zero-sqlite3'
import { afterEach, describe, expect, test } from 'vitest'

import { DoBackend, deployTimeSchemaBatchStatements } from './pg-proxy-do-backend.js'

const encoder = new TextEncoder()
let servers: Server[] = []

function cstr(s: string): Uint8Array {
  const encoded = encoder.encode(s)
  const out = new Uint8Array(encoded.length + 1)
  out.set(encoded)
  return out
}

function i16(v: number): Uint8Array {
  const out = new Uint8Array(2)
  new DataView(out.buffer).setInt16(0, v)
  return out
}

function i32(v: number): Uint8Array {
  const out = new Uint8Array(4)
  new DataView(out.buffer).setInt32(0, v)
  return out
}

function concat(...parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((sum, part) => sum + part.length, 0))
  let offset = 0
  for (const part of parts) {
    out.set(part, offset)
    offset += part.length
  }
  return out
}

function msg(type: number, payload: Uint8Array): Uint8Array {
  return concat(new Uint8Array([type]), i32(payload.length + 4), payload)
}

function parseMessage(sql: string, name = '', paramOIDs: number[] = []): Uint8Array {
  return msg(
    0x50,
    concat(
      cstr(name),
      cstr(sql),
      i16(paramOIDs.length),
      ...paramOIDs.map((oid) => {
        const out = new Uint8Array(4)
        new DataView(out.buffer).setUint32(0, oid)
        return out
      })
    )
  )
}

function describeStatement(name = ''): Uint8Array {
  return msg(0x44, concat(encoder.encode('S'), cstr(name)))
}

function describePortal(name = ''): Uint8Array {
  return msg(0x44, concat(encoder.encode('P'), cstr(name)))
}

function bindStatement(statement = '', portal = ''): Uint8Array {
  return msg(0x42, concat(cstr(portal), cstr(statement), i16(0), i16(0), i16(0)))
}

function bindStatementParams(params: unknown[], statement = '', portal = ''): Uint8Array {
  return msg(
    0x42,
    concat(
      cstr(portal),
      cstr(statement),
      i16(0),
      i16(params.length),
      ...params.map((param) => {
        if (param === null || param === undefined) return i32(-1)
        const encoded = encoder.encode(String(param))
        return concat(i32(encoded.length), encoded)
      }),
      i16(0)
    )
  )
}

function executePortal(portal = ''): Uint8Array {
  return msg(0x45, concat(cstr(portal), i32(0)))
}

function closePortal(name = ''): Uint8Array {
  return msg(0x43, concat(encoder.encode('P'), cstr(name)))
}

function closeStatement(name = ''): Uint8Array {
  return msg(0x43, concat(encoder.encode('S'), cstr(name)))
}

function messageTypes(data: Uint8Array): string[] {
  const types: string[] = []
  for (let offset = 0; offset < data.length; ) {
    types.push(String.fromCharCode(data[offset]))
    const len = new DataView(data.buffer, data.byteOffset + offset + 1, 4).getInt32(0)
    offset += 1 + len
  }
  return types
}

// asserts a binary COPY row of (id text 'f1', readOnly bool false, size int4 847)
// is encoded field-by-field at the pg wire sizes a typed consumer decodes with.
function expectBinaryCopyRow(row: Uint8Array): void {
  const view = new DataView(row.buffer, row.byteOffset, row.byteLength)
  let offset = 0
  expect(view.getInt16(offset)).toBe(3)
  offset += 2
  // id text
  const idLen = view.getInt32(offset)
  offset += 4
  expect(new TextDecoder().decode(row.subarray(offset, offset + idLen))).toBe('f1')
  offset += idLen
  // readOnly bool: exactly 1 byte
  expect(view.getInt32(offset)).toBe(1)
  offset += 4
  expect(row[offset]).toBe(0)
  offset += 1
  // size int4: exactly 4 bytes big-endian
  expect(view.getInt32(offset)).toBe(4)
  offset += 4
  expect(view.getInt32(offset)).toBe(847)
}

function copyDataPayloads(data: Uint8Array): Uint8Array[] {
  const payloads: Uint8Array[] = []
  for (let offset = 0; offset < data.length; ) {
    const type = data[offset]
    const len = new DataView(data.buffer, data.byteOffset + offset + 1, 4).getInt32(0)
    if (type === 0x64) payloads.push(data.subarray(offset + 5, offset + 1 + len))
    offset += 1 + len
  }
  return payloads
}

function readyForQueryStatuses(data: Uint8Array): string[] {
  const statuses: string[] = []
  for (let offset = 0; offset < data.length; ) {
    const type = data[offset]
    const len = new DataView(data.buffer, data.byteOffset + offset + 1, 4).getInt32(0)
    if (type === 0x5a) statuses.push(String.fromCharCode(data[offset + 5]))
    offset += 1 + len
  }
  return statuses
}

function rowDescriptionOids(data: Uint8Array): Record<string, number> {
  const oids: Record<string, number> = {}
  for (let offset = 0; offset < data.length; ) {
    const type = data[offset]
    const len = new DataView(data.buffer, data.byteOffset + offset + 1, 4).getInt32(0)
    if (type === 0x54) {
      const view = new DataView(data.buffer, data.byteOffset + offset, 1 + len)
      const count = view.getInt16(5)
      let pos = 7
      for (let i = 0; i < count; i++) {
        const nameStart = pos
        while (pos < 1 + len && data[offset + pos] !== 0) pos++
        const name = new TextDecoder().decode(
          data.subarray(offset + nameStart, offset + pos)
        )
        pos++
        pos += 6
        oids[name] = view.getUint32(pos)
        pos += 12
      }
    }
    offset += 1 + len
  }
  return oids
}

function rowDescriptionNames(data: Uint8Array): string[] {
  for (let offset = 0; offset < data.length; ) {
    const type = data[offset]
    const len = new DataView(data.buffer, data.byteOffset + offset + 1, 4).getInt32(0)
    if (type === 0x54) {
      const view = new DataView(data.buffer, data.byteOffset + offset, 1 + len)
      const count = view.getInt16(5)
      let pos = 7
      const names: string[] = []
      for (let i = 0; i < count; i++) {
        const nameStart = pos
        while (pos < 1 + len && data[offset + pos] !== 0) pos++
        names.push(
          new TextDecoder().decode(data.subarray(offset + nameStart, offset + pos))
        )
        pos += 19
      }
      return names
    }
    offset += 1 + len
  }
  return []
}

function parameterDescriptionOids(data: Uint8Array): number[] {
  for (let offset = 0; offset < data.length; ) {
    const type = data[offset]
    const len = new DataView(data.buffer, data.byteOffset + offset + 1, 4).getInt32(0)
    if (type === 0x74) {
      const view = new DataView(data.buffer, data.byteOffset + offset, 1 + len)
      const count = view.getInt16(5)
      return Array.from({ length: count }, (_, index) => view.getUint32(7 + index * 4))
    }
    offset += 1 + len
  }
  return []
}

function dataRowValues(data: Uint8Array): (string | null)[][] {
  const rows: (string | null)[][] = []
  for (let offset = 0; offset < data.length; ) {
    const type = data[offset]
    const len = new DataView(data.buffer, data.byteOffset + offset + 1, 4).getInt32(0)
    if (type === 0x44) {
      const view = new DataView(data.buffer, data.byteOffset + offset, 1 + len)
      const count = view.getInt16(5)
      let pos = 7
      const row: (string | null)[] = []
      for (let i = 0; i < count; i++) {
        const valueLength = view.getInt32(pos)
        pos += 4
        if (valueLength === -1) {
          row.push(null)
          continue
        }
        row.push(
          new TextDecoder().decode(
            data.subarray(offset + pos, offset + pos + valueLength)
          )
        )
        pos += valueLength
      }
      rows.push(row)
    }
    offset += 1 + len
  }
  return rows
}

function compactSQL(sql: string): string {
  return sql.replace(/\s+/g, ' ').trim()
}

function appendMetadataParamRows(
  rows: Record<string, unknown>[],
  params: unknown[]
): void {
  if (params.length === 3) {
    const [kind, key, value] = params
    rows.push({ kind, key, subkey: '', value })
    return
  }
  for (let i = 0; i + 3 < params.length; i += 4) {
    const [kind, key, subkey, value] = params.slice(i, i + 4)
    rows.push({ kind, key, subkey, value })
  }
}

function sqlContaining(sqls: string[], needle: string): string {
  const found = [...sqls].reverse().find((sql) => compactSQL(sql).includes(needle))
  expect(found).toBeDefined()
  return found ?? ''
}

function startDoHttp(
  handler: (sql: string, url: URL) => { rows?: unknown[]; columns?: string[] } | Response
): Promise<{
  url: string
  requests: URL[]
  sqls: string[]
  params: unknown[][]
  bodies: any[]
}> {
  const requests: URL[] = []
  const sqls: string[] = []
  const params: unknown[][] = []
  const bodies: any[] = []
  const server = createServer(async (req, res) => {
    const url = new URL(req.url || '/', 'http://127.0.0.1')
    requests.push(url)
    let body = ''
    req.on('data', (chunk) => {
      body += chunk
    })
    req.on('end', () => {
      const parsed = body ? JSON.parse(body) : {}
      bodies.push(parsed)
      if (url.pathname === '/batch') {
        // mirror the real ZeroDO /batch: execute each statement through the
        // same handler and return its rows, not empty stubs.
        const statements = Array.isArray(parsed.statements) ? parsed.statements : []
        const results = statements.map((statement: any) => {
          const sql = typeof statement === 'string' ? statement : statement.sql
          sqls.push(sql)
          params.push(Array.isArray(statement?.params) ? statement.params : [])
          const result = handler(sql, url)
          if (result instanceof Response) return { rows: [], columns: [] }
          return { rows: result.rows ?? [], columns: result.columns ?? [] }
        })
        res.setHeader('content-type', 'application/json')
        res.end(JSON.stringify({ results }))
        return
      }
      const sql = parsed.sql || ''
      sqls.push(sql)
      params.push(Array.isArray(parsed.params) ? parsed.params : [])
      const result = handler(sql, url)
      if (result instanceof Response) {
        res.statusCode = result.status
        result.text().then((text) => res.end(text))
        return
      }
      res.setHeader('content-type', 'application/json')
      res.end(JSON.stringify({ rows: result.rows ?? [], columns: result.columns ?? [] }))
    })
  })
  servers.push(server)

  return new Promise((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', () => {
      const addr = server.address()
      if (!addr || typeof addr === 'string') {
        reject(new Error('server did not bind to a tcp port'))
        return
      }
      resolve({
        url: `http://127.0.0.1:${addr.port}`,
        requests,
        sqls,
        params,
        bodies,
      })
    })
  })
}

afterEach(async () => {
  await Promise.all(
    servers.map(
      (server) =>
        new Promise<void>((resolve) => {
          server.close(() => resolve())
        })
    )
  )
  servers = []
})

describe('DoBackend', () => {
  test('defers durable object IO until readiness is awaited', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const warm = new DoBackend(http.url, 'postgres', 'lazy-startup-warm')
    await warm.waitReady
    http.requests.length = 0
    http.sqls.length = 0

    const backend = new DoBackend(http.url, 'postgres', 'lazy-startup')
    await new Promise((resolve) => setTimeout(resolve, 0))

    expect(http.requests).toEqual([])
    expect(http.sqls).toEqual([])

    await backend.waitReady
    expect(http.requests.length).toBeGreaterThan(0)
    expect(http.sqls.length).toBeGreaterThan(0)
  })

  test('bootstraps zero-cache change tracking tables for postgres backends', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'change-tracking-bootstrap')
    await backend.waitReady

    const sent = compactSQL(http.sqls.join('; '))
    expect(sent).toContain('CREATE TABLE IF NOT EXISTS "_zero_changes"')
    expect(sent).toContain('CREATE TABLE IF NOT EXISTS "_zero_change_state"')
    expect(sent).toContain('CREATE TABLE IF NOT EXISTS "_orez___zero_watermark"')
    expect(sent).toContain('CREATE TABLE IF NOT EXISTS "_orez__zero_replication_slots"')
  })

  test('sends the configured Durable Object namespace on every SQL request', async () => {
    const http = await startDoHttp(() => ({ rows: [{ ok: 1 }], columns: ['ok'] }))
    const backend = new DoBackend(http.url, 'postgres', 'chat-test-namespace')
    await backend.waitReady

    await backend.query('SELECT 1 AS ok')

    expect(http.requests.length).toBeGreaterThanOrEqual(2)
    expect(http.requests.every((url) => url.searchParams.get('db') === 'postgres')).toBe(
      true
    )
    expect(
      http.requests.every((url) => url.searchParams.get('ns') === 'chat-test-namespace')
    ).toBe(true)
  })

  test('quotes sqlite keyword table identifiers in DO-bound SQL', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'sqlite-keyword-table-test')
    await backend.waitReady
    http.sqls.length = 0

    await backend.query(`
      CREATE TABLE IF NOT EXISTS "transaction" (
        id text PRIMARY KEY,
        amount integer NOT NULL
      )
    `)
    await backend.query('CREATE INDEX "transaction_amount_idx" ON "transaction" (amount)')
    await backend.query('INSERT INTO "transaction" (id, amount) VALUES ($1, $2)', [
      'tx1',
      42,
    ])
    await backend.query('UPDATE "transaction" SET amount = $1 WHERE id = $2', [64, 'tx1'])
    await backend.query('SELECT "transaction".id FROM "transaction" WHERE id = $1', [
      'tx1',
    ])
    await backend.query('DELETE FROM "transaction" WHERE id = $1', ['tx1'])

    const sent = compactSQL(http.sqls.join('; '))
    expect(sent).toContain('CREATE TABLE IF NOT EXISTS "transaction"')
    expect(sent).toContain('ON "transaction"')
    expect(sent).toContain('INSERT INTO "transaction"')
    expect(sent).toContain('UPDATE "transaction"')
    expect(sent).toContain('FROM "transaction"')
    expect(sent).toContain('DELETE FROM "transaction"')
  })

  test('describes prepared statements with parameter and row metadata', async () => {
    const http = await startDoHttp((sql) => {
      if (sql.includes('_orez_describe')) return { rows: [], columns: ['value'] }
      return { rows: [{ value: 'ok' }], columns: ['value'] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'describe-test')
    await backend.waitReady

    expect(
      messageTypes(
        await backend.execProtocolRaw(parseMessage('SELECT $1 AS value', '', [25]))
      )
    ).toEqual(['1'])
    expect(messageTypes(await backend.execProtocolRaw(describeStatement()))).toEqual([
      't',
      'T',
    ])
  })

  test('returns row metadata for zero-row selects', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: ['id'] }))
    const backend = new DoBackend(http.url, 'postgres', 'zero-row-test')
    await backend.waitReady

    const result = await backend.execProtocolRaw(
      msg(0x51, cstr('SELECT id FROM message WHERE 1 = 0'))
    )

    expect(messageTypes(result)).toEqual(['T', 'C', 'Z'])
  })

  test('tags json `->`/`#>` access columns with a json oid so the driver parses them', async () => {
    // zero-cache's changeLog catchup reads `change->'tag' as tag`. sqlite's
    // `->` returns the json TEXT representation (`"begin"` with quotes), so the
    // column must carry a json oid for the driver to JSON.parse it back into the
    // bare string `begin` — otherwise the `case "begin"` switch never matches
    // and every begin/commit is mis-emitted as a `data` change.
    const http = await startDoHttp(() => ({
      rows: [{ tag: '"begin"', deep: '"x"', t2: 'begin' }],
      columns: ['tag', 'deep', 't2'],
    }))
    const backend = new DoBackend(http.url, 'postgres', 'json-access-oid-test')
    await backend.waitReady

    const described = await backend.execProtocolRaw(
      msg(
        0x51,
        cstr(
          `SELECT change->'tag' AS tag, change#>'{a,b}' AS deep, change->>'tag' AS t2 FROM "soot/cdc"."changeLog"`
        )
      )
    )
    const oids = rowDescriptionOids(described)
    expect(oids.tag).toBe(114) // PG_TYPE_JSON
    expect(oids.deep).toBe(114)
    // `->>` returns text, must NOT be tagged json
    expect(oids.t2).not.toBe(114)
  })

  test('rewrites pg_column_size totals as ordinary SQL instead of catalog probes', async () => {
    const http = await startDoHttp((sql) => {
      if (compactSQL(sql).includes('length')) {
        return { rows: [{ totalBytes: 42 }], columns: ['totalBytes'] }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'pg-column-size-test')
    await backend.waitReady

    await backend.execProtocolRaw(
      parseMessage(`
        SELECT (
          SUM(COALESCE(pg_column_size("id"), 0)) +
          SUM(COALESCE(pg_column_size("payload"), 0))
        ) AS "totalBytes"
        FROM public.message
      `)
    )
    await backend.execProtocolRaw(bindStatement())
    const result = await backend.execProtocolRaw(executePortal())

    const sent = compactSQL(http.sqls.at(-1) || '')
    expect(messageTypes(result)).toEqual(['T', 'D', 'C'])
    expect(dataRowValues(result)).toEqual([['42']])
    expect(sent).toContain('length')
    expect(sent).toContain('FROM message')
    expect(sent).not.toContain('pg_column_size')
    expect(sent).not.toContain('public.message')
  })

  test('rewrites pg_column_size table-row estimates without treating table names as columns', async () => {
    const http = await startDoHttp(() => ({
      rows: [{ totalBytes: 5 }],
      columns: ['totalBytes'],
    }))
    const backend = new DoBackend(http.url, 'postgres', 'pg-column-size-row-test')
    await backend.waitReady

    await backend.query(`
      SELECT
        SUM(COALESCE(pg_column_size(chat.permissions), 0)) +
        SUM(COALESCE(pg_column_size(hash), 0)) AS "totalBytes"
      FROM chat.permissions
    `)

    const sent = compactSQL(http.sqls.at(-1) || '')
    expect(sent).toContain('COALESCE(0, 0)')
    expect(sent).toContain('length(hash)')
    expect(sent).toContain('FROM chat_permissions')
    expect(sent).not.toMatch(/length\(chat_permissions\)/)
    expect(sent).not.toContain('pg_column_size')
  })

  test('streams COPY TO STDOUT from a parsed select query', async () => {
    const http = await startDoHttp(() => ({
      rows: [{ id: 'm1', deleted: 0 }],
      columns: ['id', 'deleted'],
    }))
    const backend = new DoBackend(http.url, 'postgres', 'copy-test')
    await backend.waitReady
    await backend.exec(`
      CREATE TABLE public.message (
        id text PRIMARY KEY,
        deleted boolean NOT NULL DEFAULT false
      )
    `)

    const result = await backend.execProtocolRaw(
      msg(0x51, cstr('COPY (SELECT id, deleted FROM public.message) TO STDOUT'))
    )

    expect(messageTypes(result)).toEqual(['H', 'd', 'c', 'C', 'Z'])
    expect(new TextDecoder().decode(result)).toContain('m1\tf\n')
    expect(http.sqls.some((sql) => compactSQL(sql).includes('FROM message'))).toBe(true)
  })

  test('streams binary COPY TO STDOUT with a PostgreSQL binary header', async () => {
    const http = await startDoHttp(() => ({
      rows: [{ id: 'm1', deleted: 0 }],
      columns: ['id', 'deleted'],
    }))
    const backend = new DoBackend(http.url, 'postgres', 'binary-copy-test')
    await backend.waitReady
    await backend.exec(`
      CREATE TABLE public.message (
        id text PRIMARY KEY,
        deleted boolean NOT NULL DEFAULT false
      )
    `)

    const result = await backend.execProtocolRaw(
      msg(
        0x51,
        cstr(
          'COPY (SELECT id, deleted FROM public.message) TO STDOUT WITH (FORMAT BINARY)'
        )
      )
    )

    expect(messageTypes(result)).toEqual(['H', 'd', 'd', 'd', 'c', 'C', 'Z'])
    const payloads = copyDataPayloads(result)
    expect([...payloads[0].slice(0, 11)]).toEqual([
      80, 71, 67, 79, 80, 89, 10, 255, 13, 10, 0,
    ])
    expect(new TextDecoder().decode(payloads[1])).toContain('m1')
    expect(payloads.at(-1)).toEqual(new Uint8Array([255, 255]))
  })

  test('binary COPY encodes int4/bool fields by column type for quoted qualified tables', async () => {
    // mirrors zero-cache initial sync: COPY (SELECT "col",... FROM "public"."tbl")
    // TO STDOUT WITH (FORMAT binary). the consumer decodes each field by the
    // table schema's declared type, so an int4 field MUST be 4 bytes big-endian —
    // a text fallback ("847" = 3 bytes) crashes readInt32BE on the other side.
    const http = await startDoHttp(() => ({
      rows: [{ id: 'f1', readOnly: 0, size: 847 }],
      columns: ['id', 'readOnly', 'size'],
    }))
    const backend = new DoBackend(http.url, 'postgres', 'binary-copy-int4-test')
    await backend.waitReady
    await backend.exec(`
      CREATE TABLE public.file (
        id text PRIMARY KEY,
        "readOnly" boolean NOT NULL DEFAULT false,
        size integer NOT NULL
      )
    `)

    const result = await backend.execProtocolRaw(
      msg(
        0x51,
        cstr(
          'COPY (SELECT "id","readOnly","size" FROM "public"."file") TO STDOUT WITH (FORMAT binary)'
        )
      )
    )

    const payloads = copyDataPayloads(result)
    expectBinaryCopyRow(payloads[1])
  })

  test('deploy-time schema batch hydrates pg types for binary COPY (out-of-band DDL)', async () => {
    // mirrors the Cloudflare deploy: DDL is applied straight to the SQL DO at
    // deploy time (no statement ever flows through a DoBackend), so the only
    // way a fresh backend learns the pg column types is the _orez_pg_metadata
    // rows emitted by deployTimeSchemaBatchStatements. without them, binary
    // COPY falls back to text encoding ("847" = 3 bytes) and the consumer's
    // int4 readInt32BE crashes — the zero-cache initial-sync failure.
    const initSql = `
      CREATE TABLE IF NOT EXISTS "file" (
        "id" text PRIMARY KEY,
        "readOnly" boolean DEFAULT false NOT NULL,
        "size" integer NOT NULL,
        "createdAt" timestamp DEFAULT now() NOT NULL
      );
      --> statement-breakpoint
      CREATE INDEX IF NOT EXISTS "file_id_idx" ON "file" ("id");
    `
    const batch = await deployTimeSchemaBatchStatements(initSql)
    const ddl = batch.filter((statement) => !statement.params)
    expect(ddl.some((statement) => /CREATE TABLE/i.test(statement.sql))).toBe(true)
    // translated to SQLite types, defaults rewritten
    const createTable = ddl.find((statement) => /CREATE TABLE/i.test(statement.sql))!
    expect(createTable.sql).toContain('integer')
    expect(createTable.sql).not.toMatch(/\bboolean\b/i)
    expect(createTable.sql).not.toContain('now()')

    const metadataInserts = batch.filter(
      (statement) => statement.params && statement.sql.includes('_orez_pg_metadata')
    )
    expect(metadataInserts.length).toBeGreaterThan(0)
    const metadataRows: { kind: string; key: string; subkey: string; value: string }[] =
      []
    for (const statement of metadataInserts) {
      for (let i = 0; i < statement.params!.length; i += 4) {
        const [kind, key, subkey, value] = statement.params!.slice(i, i + 4)
        metadataRows.push({ kind, key, subkey, value })
      }
    }
    expect(metadataRows.some((row) => row.key === 'file' && row.subkey === 'size')).toBe(
      true
    )

    // fresh backend: no DDL through it, metadata served from the durable table
    const http = await startDoHttp((sql) => {
      if (sql.includes('_orez_pg_metadata') && compactSQL(sql).startsWith('SELECT')) {
        return {
          rows: metadataRows,
          columns: ['kind', 'key', 'subkey', 'value'],
        }
      }
      if (compactSQL(sql).startsWith('SELECT')) {
        return {
          rows: [{ id: 'f1', readOnly: 0, size: 847 }],
          columns: ['id', 'readOnly', 'size'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'deploy-batch-metadata-test')
    await backend.waitReady

    const result = await backend.execProtocolRaw(
      msg(
        0x51,
        cstr(
          'COPY (SELECT "id","readOnly","size" FROM "public"."file") TO STDOUT WITH (FORMAT binary)'
        )
      )
    )
    expectBinaryCopyRow(copyDataPayloads(result)[1])
  })

  test('deploy-time schema batch carries ADD COLUMN IF NOT EXISTS skip conditions', async () => {
    // forward-migration of an EXISTING table: CREATE IF NOT EXISTS is a no-op,
    // so schema-evolved columns ship as ALTER ... ADD COLUMN IF NOT EXISTS.
    // deploy time can't know a target namespace's shape, so the batch item
    // must carry the skip condition for the DO /batch executor to evaluate
    // (dropping it makes the re-applied ALTER abort the whole batch).
    const ddl = `
      CREATE TABLE IF NOT EXISTS "file" ("id" text PRIMARY KEY, "path" text NOT NULL);
      --> statement-breakpoint
      ALTER TABLE "file" ADD COLUMN IF NOT EXISTS "title" text;
      --> statement-breakpoint
      ALTER TABLE "file" ADD COLUMN IF NOT EXISTS "size" integer NOT NULL DEFAULT 0;
    `
    const batch = await deployTimeSchemaBatchStatements(ddl)
    const adds = batch.filter((statement) => /ADD COLUMN/i.test(statement.sql))
    expect(adds.map((statement) => statement.skipIfColumnExists)).toEqual([
      { table: 'file', column: 'title' },
      { table: 'file', column: 'size' },
    ])
    // the pg type metadata for the added columns must flow too — without it
    // binary COPY downgrades to text for those columns after a forward
    // migration.
    const metadataParams = batch
      .filter((statement) => statement.params)
      .flatMap((statement) => statement.params!)
    expect(metadataParams).toContain('title')
    expect(metadataParams).toContain('size')
  })

  test('formats timestamp typed rows as postgres text for DataRow and COPY', async () => {
    const http = await startDoHttp((sql) => {
      if (compactSQL(sql).startsWith('SELECT')) {
        return {
          rows: [
            { id: 'm1', createdAt: '2026-05-25T20:17:28.377Z' },
            { id: 'm2', createdAt: '1779746873949.0' },
          ],
          columns: ['id', 'createdAt'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'timestamp-format-test')
    await backend.waitReady

    await backend.exec(`
      CREATE TABLE public.message (
        id text PRIMARY KEY,
        "createdAt" timestamptz NOT NULL
      )
    `)

    const select = await backend.execProtocolRaw(
      msg(0x51, cstr('SELECT id, "createdAt" FROM public.message'))
    )
    const copy = await backend.execProtocolRaw(
      msg(0x51, cstr('COPY (SELECT id, "createdAt" FROM public.message) TO STDOUT'))
    )

    expect(rowDescriptionOids(select)).toMatchObject({ createdAt: 1184 })
    expect(dataRowValues(select)).toEqual([
      ['m1', '2026-05-25 20:17:28.377+00'],
      ['m2', '2026-05-25 22:07:53.949+00'],
    ])
    expect(new TextDecoder().decode(copy)).toContain('m1\t2026-05-25 20:17:28.377+00\n')
    expect(new TextDecoder().decode(copy)).toContain('m2\t2026-05-25 22:07:53.949+00\n')
  })

  test('normalizes timestamp typed parameters before sending them to SQLite', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'timestamp-param-test')
    await backend.waitReady

    await backend.exec(`
      CREATE TABLE public.event (
        id text PRIMARY KEY,
        "createdAt" timestamptz NOT NULL
      )
    `)
    await backend.exec(`
      CREATE TABLE public.notification (
        id text PRIMARY KEY,
        "seenAt" timestamptz
      )
    `)

    await backend.execProtocolRaw(
      parseMessage(
        'INSERT INTO public.event (id, "createdAt") VALUES ($1, $2)',
        'insert_timestamp'
      )
    )
    const insertDescribe = await backend.execProtocolRaw(
      describeStatement('insert_timestamp')
    )
    expect(parameterDescriptionOids(insertDescribe)).toEqual([25, 1184])
    await backend.execProtocolRaw(
      bindStatementParams(['e1', 1779746873949], 'insert_timestamp')
    )
    await backend.execProtocolRaw(executePortal())

    await backend.execProtocolRaw(
      parseMessage(
        'UPDATE public.event SET "createdAt" = $1 WHERE id = $2',
        'update_timestamp'
      )
    )
    const updateDescribe = await backend.execProtocolRaw(
      describeStatement('update_timestamp')
    )
    expect(parameterDescriptionOids(updateDescribe)).toEqual([1184, 25])
    await backend.execProtocolRaw(
      bindStatementParams([1779746874950, 'e1'], 'update_timestamp')
    )
    await backend.execProtocolRaw(executePortal())

    await backend.execProtocolRaw(
      parseMessage(
        'INSERT INTO public.notification (id, "seenAt") VALUES ($1, $2)',
        'insert_null_timestamp'
      )
    )
    const nullDescribe = await backend.execProtocolRaw(
      describeStatement('insert_null_timestamp')
    )
    expect(parameterDescriptionOids(nullDescribe)).toEqual([25, 1184])
    await backend.execProtocolRaw(
      bindStatementParams(['n1', null], 'insert_null_timestamp')
    )
    await backend.execProtocolRaw(executePortal())

    expect(http.params.at(-3)).toEqual(['e1', '2026-05-25 22:07:53.949+00'])
    expect(http.params.at(-2)).toEqual(['2026-05-25 22:07:54.950+00', 'e1'])
    expect(http.params.at(-1)).toEqual(['n1', null])
  })

  test('normalizes boolean parameters before sending them to SQLite', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'boolean-param-test')
    await backend.waitReady

    await backend.exec(`
      CREATE TABLE public.flag_probe (
        id text PRIMARY KEY,
        enabled boolean NOT NULL
      )
    `)
    await backend.execProtocolRaw(
      parseMessage('INSERT INTO public.flag_probe (id, enabled) VALUES ($1, $2)')
    )
    await backend.execProtocolRaw(bindStatementParams(['f1', false]))
    await backend.execProtocolRaw(executePortal())
    await backend.execProtocolRaw(bindStatementParams(['t1', true]))
    await backend.execProtocolRaw(executePortal())

    expect(http.params.at(-2)).toEqual(['f1', 0])
    expect(http.params.at(-1)).toEqual(['t1', 1])
  })

  test('normalizes boolean parameters through text cast chains', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'boolean-text-cast-param-test')
    await backend.waitReady

    await backend.query(
      'SELECT id FROM public.flag_probe WHERE enabled = $1::text::boolean OR enabled = $1::text::boolean',
      ['false']
    )

    expect(http.params.at(-1)).toEqual([0, 0])
  })

  test('infers JSON parameters in inserts and ON CONFLICT updates', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'json-param-test')
    await backend.waitReady

    await backend.exec(`
      CREATE TABLE public.client_state (
        id text PRIMARY KEY,
        "clientSchema" jsonb
      )
    `)

    await backend.execProtocolRaw(
      parseMessage(
        `INSERT INTO public.client_state (id, "clientSchema")
         VALUES ($1, $2)
         ON CONFLICT (id) DO UPDATE SET "clientSchema" = $3`,
        'json_upsert'
      )
    )
    const describe = await backend.execProtocolRaw(describeStatement('json_upsert'))
    expect(parameterDescriptionOids(describe)).toEqual([25, 3802, 3802])

    await backend.query(
      `INSERT INTO public.client_state (id, "clientSchema")
       VALUES ($1, $2)
       ON CONFLICT (id) DO UPDATE SET "clientSchema" = $3`,
      ['cg', { tables: { insert: true } }, { tables: { update: true } }]
    )

    expect(http.params.at(-1)).toEqual([
      'cg',
      '{"tables":{"insert":true}}',
      '{"tables":{"update":true}}',
    ])
  })

  test('returns aliased current_setting catalog values needed by zero-cache', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'current-setting-test')
    await backend.waitReady

    const result = await (backend as any).handleCatalogQuery(`
      SELECT current_setting('wal_level') as "walLevel",
             current_setting('server_version_num') as "version";
    `)

    expect(result.fields.map((field: any) => field.name)).toEqual(['walLevel', 'version'])
    expect(result.rows).toEqual([{ walLevel: 'logical', version: '160000' }])

    const rewrittenResult = await (backend as any).handleCatalogQuery(`
      SELECT 'logical'::text as "walLevel",
             current_setting('server_version_num') as "version";
    `)

    expect(rewrittenResult.fields.map((field: any) => field.name)).toEqual([
      'walLevel',
      'version',
    ])
    expect(rewrittenResult.rows).toEqual([{ walLevel: 'logical', version: '160000' }])
  })

  test('projects pg_settings expressions from synthesized settings rows', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'pg-settings-test')
    await backend.waitReady

    const result = await (backend as any).handleCatalogQuery(`
      SELECT EXTRACT(EPOCH FROM (setting || unit)::interval) * 1000
        AS "walSenderTimeoutMs"
      FROM pg_settings
      WHERE name = 'wal_sender_timeout'
    `)

    expect(result.fields).toEqual([{ name: 'walSenderTimeoutMs', oid: 701 }])
    expect(result.rows).toEqual([{ walSenderTimeoutMs: 60000 }])
  })

  test('returns requested pg_publication rows for zero-cache validation', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'publication-test')
    await backend.waitReady
    await backend.exec('CREATE PUBLICATION zero_chat FOR TABLE message')

    const result = await (backend as any).handleCatalogQuery(`
      SELECT pubname FROM pg_publication WHERE pubname IN ('zero_chat')
    `)

    expect(result.fields.map((field: any) => field.name)).toEqual(['pubname'])
    expect(result.rows).toEqual([{ pubname: 'zero_chat' }])
  })

  test('executes the portal named by extended-protocol Execute', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: ['value'] }))
    const backend = new DoBackend(http.url, 'postgres', 'portal-execute-test')
    await backend.waitReady
    await backend.exec('CREATE PUBLICATION zero_chat FOR TABLE message')

    await backend.execProtocolRaw(
      parseMessage('SELECT value FROM ordinary WHERE id = $1', 'ordinary')
    )
    await backend.execProtocolRaw(
      bindStatementParams(['ignore'], 'ordinary', 'ordinary_portal')
    )
    await backend.execProtocolRaw(
      parseMessage(
        'SELECT pubname FROM pg_publication WHERE pubname IN ($1)',
        'publication'
      )
    )
    await backend.execProtocolRaw(
      bindStatementParams(['zero_chat'], 'publication', 'publication_portal')
    )

    const result = await backend.execProtocolRaw(executePortal('publication_portal'))

    expect(messageTypes(result)).toEqual(['T', 'D', 'C'])
    expect(new TextDecoder().decode(result)).toContain('zero_chat')
    expect(http.sqls.some((sql) => compactSQL(sql).includes('FROM ordinary'))).toBe(false)
  })

  test('describes and closes bound portals by portal name', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: ['value'] }))
    const backend = new DoBackend(http.url, 'postgres', 'portal-describe-test')
    await backend.waitReady

    await backend.execProtocolRaw(parseMessage('SELECT $1 AS value', 'statement', [25]))
    await backend.execProtocolRaw(bindStatementParams(['ok'], 'statement', 'portal'))

    expect(messageTypes(await backend.execProtocolRaw(describePortal('portal')))).toEqual(
      ['T']
    )
    expect(messageTypes(await backend.execProtocolRaw(closePortal('portal')))).toEqual([
      '3',
    ])
    expect(messageTypes(await backend.execProtocolRaw(describePortal('portal')))).toEqual(
      ['n']
    )

    await backend.execProtocolRaw(bindStatementParams(['ok'], 'statement', 'portal'))
    expect(
      messageTypes(await backend.execProtocolRaw(closeStatement('statement')))
    ).toEqual(['3'])
    expect(messageTypes(await backend.execProtocolRaw(describePortal('portal')))).toEqual(
      ['n']
    )
  })

  test('returns publication flags with boolean oids for zero-cache schema checks', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'publication-flags-test')
    await backend.waitReady
    await backend.exec(`
      CREATE PUBLICATION zero_chat FOR TABLE message;
      CREATE PUBLICATION _zero_metadata FOR TABLE "_zero"."clients";
    `)

    const result = await (backend as any).handleCatalogQuery(`
      SELECT pubname,pubinsert,pubupdate,pubdelete,pubtruncate FROM pg_publication pb
      WHERE pb.pubname IN ('zero_chat','_zero_metadata')
      ORDER BY pubname
    `)

    expect(result.fields).toEqual([
      { name: 'pubname', oid: undefined },
      { name: 'pubinsert', oid: 16 },
      { name: 'pubupdate', oid: 16 },
      { name: 'pubdelete', oid: 16 },
      { name: 'pubtruncate', oid: 16 },
    ])
    expect(result.rows).toEqual([
      {
        pubname: '_zero_metadata',
        pubinsert: 't',
        pubupdate: 't',
        pubdelete: 't',
        pubtruncate: 't',
      },
      {
        pubname: 'zero_chat',
        pubinsert: 't',
        pubupdate: 't',
        pubdelete: 't',
        pubtruncate: 't',
      },
    ])
  })

  test('preserves selected field metadata for empty catalog probes', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'pg-class-probe-test')
    await backend.waitReady

    const result = await (backend as any).handleCatalogQuery(`
      SELECT nspname, relname FROM pg_class
      JOIN pg_namespace ON relnamespace = pg_namespace.oid
      WHERE nspname = 'chat_0' AND relname = 'versionHistory'
    `)

    expect(result.rows).toEqual([])
    expect(result.fields.map((field: any) => field.name)).toEqual(['nspname', 'relname'])
  })

  test('synthesizes advisory lock catalog calls with one null row', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'advisory-lock-test')
    await backend.waitReady

    const result = await (backend as any).handleCatalogQuery(`
      SELECT pg_advisory_xact_lock(hashtext('migrate-schema:chat_0'))
    `)

    expect(result.fields.map((field: any) => field.name)).toEqual([
      'pg_advisory_xact_lock',
    ])
    expect(result.rows).toEqual([{ pg_advisory_xact_lock: null }])
  })

  test('synthesizes logical message lag-report probes', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'logical-message-test')
    await backend.waitReady

    const result = await (backend as any).handleCatalogQuery(`
      WITH CTE AS (SELECT extract(epoch from now()) * 1000 AS "commitTimeMs")
      SELECT "commitTimeMs", pg_logical_emit_message(
        false,
        'zero/0',
        json_build_object(
          'id', 'lag-1'::text,
          'sendTimeMs', 1::int8,
          'commitTimeMs', "commitTimeMs"
        )::text
      ) as lsn FROM CTE;
    `)

    expect(result.fields.map((field: any) => field.name)).toEqual(['commitTimeMs', 'lsn'])
    expect(result.rows[0]).toEqual({
      commitTimeMs: expect.any(Number),
      lsn: '0/1',
    })

    const wire = await backend.execProtocolRaw(
      msg(
        0x51,
        cstr(`
          WITH CTE AS (SELECT extract(epoch from now()) * 1000 AS "commitTimeMs")
          SELECT "commitTimeMs", pg_logical_emit_message(
            false,
            'zero/0',
            json_build_object(
              'id', 'lag-1'::text,
              'sendTimeMs', 1::int8,
              'commitTimeMs', "commitTimeMs"
            )::text
          ) as lsn FROM CTE;
        `)
      )
    )
    expect(rowDescriptionOids(wire)).toMatchObject({
      commitTimeMs: 701,
      lsn: 25,
    })
  })

  test('synthesizes pg_tables from sqlite schema for publication setup', async () => {
    const http = await startDoHttp((sql) => {
      if (sql.includes('sqlite_master')) {
        return {
          rows: [
            { name: 'message', sql: 'CREATE TABLE message (id varchar PRIMARY KEY)' },
            { name: 'user', sql: 'CREATE TABLE user (id varchar PRIMARY KEY)' },
            {
              name: 'public_migrations',
              sql: 'CREATE TABLE public_migrations (id integer PRIMARY KEY)',
            },
          ],
          columns: ['name', 'sql'],
        }
      }
      if (sql.includes('PRAGMA table_info("message")')) {
        return {
          rows: [
            { cid: 0, name: 'id', type: 'varchar', notnull: 1, dflt_value: null, pk: 1 },
          ],
          columns: ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'pg-tables-test')
    await backend.waitReady

    const result = await (backend as any).handleCatalogQuery(`
      SELECT tablename FROM pg_tables
      WHERE schemaname = 'public'
        AND tablename != ALL('{"user","migrations"}')
    `)

    expect(result.rows).toEqual([{ tablename: 'message' }])
  })

  test('synthesizes information_schema.columns from parsed DDL metadata', async () => {
    const http = await startDoHttp((sql) => {
      if (sql.includes('sqlite_master')) {
        return {
          rows: [
            {
              name: 'message',
              sql: 'CREATE TABLE message (id varchar, payload text, enabled integer, tags text)',
            },
          ],
          columns: ['name', 'sql'],
        }
      }
      if (sql.includes('PRAGMA table_info("message")')) {
        return {
          rows: [
            { cid: 0, name: 'id', type: 'varchar', notnull: 1, dflt_value: null, pk: 1 },
            {
              cid: 1,
              name: 'payload',
              type: 'text',
              notnull: 0,
              dflt_value: null,
              pk: 0,
            },
            {
              cid: 2,
              name: 'enabled',
              type: 'integer',
              notnull: 1,
              dflt_value: '0',
              pk: 0,
            },
            { cid: 3, name: 'tags', type: 'text', notnull: 0, dflt_value: null, pk: 0 },
          ],
          columns: ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'information-schema-test')
    await backend.waitReady
    await backend.exec(`
      CREATE TABLE public.message (
        id varchar(64) PRIMARY KEY,
        payload jsonb,
        enabled boolean NOT NULL DEFAULT false,
        tags text[]
      )
    `)

    const result = await (backend as any).handleCatalogQuery(`
      SELECT
          c.table_schema::text AS schema,
          c.table_name::text AS table,
          c.column_name::text AS column,
          c.data_type::text AS "dataType",
          c.character_maximum_length AS length,
          c.numeric_precision AS precision,
          c.numeric_scale AS scale,
          t.typtype::text AS typtype,
          t.typname::text AS typename,
          CASE WHEN t.typelem <> 0 THEN et.typtype::text ELSE NULL END AS "elemTyptype",
          CASE WHEN t.typelem <> 0 THEN et.typname::text ELSE NULL END AS "elemTypname"
      FROM information_schema.columns c
      JOIN pg_catalog.pg_type t ON c.udt_name = t.typname
      LEFT JOIN pg_catalog.pg_type et ON t.typelem = et.oid
      JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
      WHERE (c.table_schema, c.table_name) IN (('public'::text, 'message'::text))
    `)

    expect(result.rows).toEqual([
      expect.objectContaining({
        column: 'id',
        dataType: 'character varying',
        length: 64,
        typtype: 'b',
        typename: 'varchar',
        elemTyptype: null,
      }),
      expect.objectContaining({
        column: 'payload',
        dataType: 'jsonb',
        typename: 'jsonb',
      }),
      expect.objectContaining({
        column: 'enabled',
        dataType: 'boolean',
        typename: 'bool',
      }),
      expect.objectContaining({
        column: 'tags',
        dataType: 'ARRAY',
        typename: '_text',
        elemTyptype: 'b',
        elemTypname: 'text',
      }),
    ])
  })

  test('reloads schema metadata persisted after backend initialization', async () => {
    const batch = await deployTimeSchemaBatchStatements(`
      CREATE TABLE "probe" (
        "id" text PRIMARY KEY,
        "enabled" boolean NOT NULL,
        "payload" jsonb,
        "createdAt" timestamp NOT NULL
      );
    `)
    const metadataRows: Record<string, unknown>[] = []
    let metadataSelects = 0
    const http = await startDoHttp((sql) => {
      const compact = compactSQL(sql)
      if (compact.startsWith('SELECT kind, key, subkey, value FROM')) {
        metadataSelects++
        return {
          rows: metadataRows,
          columns: ['kind', 'key', 'subkey', 'value'],
        }
      }
      if (compact.includes("sqlite_master WHERE type = 'table'")) {
        return {
          rows: [
            {
              name: 'probe',
              sql: 'CREATE TABLE probe (id text PRIMARY KEY, enabled integer NOT NULL, payload text, createdAt text NOT NULL)',
            },
          ],
          columns: ['name', 'sql'],
        }
      }
      if (compact.includes('PRAGMA table_info("probe")')) {
        return {
          rows: [
            { cid: 0, name: 'id', type: 'text', notnull: 1, dflt_value: null, pk: 1 },
            {
              cid: 1,
              name: 'enabled',
              type: 'integer',
              notnull: 1,
              dflt_value: null,
              pk: 0,
            },
            {
              cid: 2,
              name: 'payload',
              type: 'text',
              notnull: 0,
              dflt_value: null,
              pk: 0,
            },
            {
              cid: 3,
              name: 'createdAt',
              type: 'text',
              notnull: 1,
              dflt_value: null,
              pk: 0,
            },
          ],
          columns: ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'late-schema-metadata-test')
    await backend.waitReady
    await backend.exec('BEGIN')

    for (const statement of batch) {
      if (statement.params) appendMetadataParamRows(metadataRows, statement.params)
    }

    const catalogSQL = `
      SELECT c.column_name::text AS column,
             c.data_type::text AS "dataType",
             t.typname::text AS typename
      FROM information_schema.columns c
      JOIN pg_catalog.pg_type t ON c.udt_name = t.typname
      LEFT JOIN pg_catalog.pg_type et ON t.typelem = et.oid
      JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
      WHERE (c.table_schema, c.table_name) IN (('public'::text, 'probe'::text))
    `
    const result = await backend.query(catalogSQL)

    expect(result.rows).toEqual([
      { column: 'id', dataType: 'text', typename: 'text' },
      { column: 'enabled', dataType: 'boolean', typename: 'bool' },
      { column: 'payload', dataType: 'jsonb', typename: 'jsonb' },
      {
        column: 'createdAt',
        dataType: 'timestamp without time zone',
        typename: 'timestamp',
      },
    ])

    await backend.exec('ROLLBACK')
    const afterRollback = await backend.query(catalogSQL)
    expect(afterRollback.rows).toEqual(result.rows)
    expect(metadataSelects).toBe(3)
  })

  test('reloads a stale internal-only catalog after out-of-band schema provisioning', async () => {
    let provisioned = false
    let metadataSelects = 0
    const http = await startDoHttp((sql) => {
      if (sql.includes('FROM "_orez_pg_metadata"')) {
        metadataSelects++
        const userMetadata = {
          kind: 'schema-column',
          key: 'user',
          subkey: 'id',
          value: JSON.stringify({
            table: 'user',
            schema: 'public',
            tableName: 'user',
            column: 'id',
            oid: 25,
            typeOid: 25,
            dataType: 'text',
            typtype: 'b',
            typname: 'text',
            elemTyptype: null,
            elemTypname: null,
          }),
        }
        return {
          rows: [
            userMetadata,
            {
              kind: 'schema-column',
              key: 'message',
              subkey: 'id',
              value: JSON.stringify({
                table: 'message',
                schema: 'public',
                tableName: 'message',
                column: 'id',
                oid: 25,
                typeOid: 25,
                dataType: 'text',
                typtype: 'b',
                typname: 'text',
                elemTyptype: null,
                elemTypname: null,
              }),
            },
          ],
          columns: ['kind', 'key', 'subkey', 'value'],
        }
      }
      if (sql.includes('sqlite_master')) {
        return {
          rows: provisioned
            ? [
                { name: 'soot_0_clients', sql: 'CREATE TABLE soot_0_clients (id text)' },
                { name: 'user', sql: 'CREATE TABLE user (id text)' },
                { name: 'message', sql: 'CREATE TABLE message (id text)' },
              ]
            : [
                { name: 'soot_0_clients', sql: 'CREATE TABLE soot_0_clients (id text)' },
                { name: 'user', sql: 'CREATE TABLE user (id text)' },
              ],
          columns: ['name', 'sql'],
        }
      }
      if (sql.includes('PRAGMA table_info("soot_0_clients")')) {
        return {
          rows: [
            { cid: 0, name: 'id', type: 'text', notnull: 1, dflt_value: null, pk: 1 },
          ],
          columns: ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'],
        }
      }
      if (sql.includes('PRAGMA table_info("message")')) {
        return {
          // Simulate a large out-of-band catalog scan whose PRAGMA batch did
          // not include the newly provisioned public table.
          rows: [],
          columns: ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'],
        }
      }
      if (sql.includes('PRAGMA table_info("user")')) {
        return {
          rows: [
            { cid: 0, name: 'id', type: 'text', notnull: 1, dflt_value: null, pk: 1 },
          ],
          columns: ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'late-table-catalog-test')
    await backend.waitReady
    const catalogSQL = `
      SELECT c.table_schema::text AS schema,
             c.table_name::text AS table,
             c.column_name::text AS column,
             c.data_type::text AS "dataType",
             c.character_maximum_length AS length,
             c.numeric_precision AS precision,
             c.numeric_scale AS scale,
             t.typtype::text AS typtype,
             t.typname::text AS typename,
             CASE WHEN t.typelem <> 0 THEN et.typtype::text ELSE NULL END AS "elemTyptype",
             CASE WHEN t.typelem <> 0 THEN et.typname::text ELSE NULL END AS "elemTypname"
      FROM information_schema.columns c
      JOIN pg_catalog.pg_type t ON c.udt_name = t.typname
      LEFT JOIN pg_catalog.pg_type et ON t.typelem = et.oid
      JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
      WHERE (c.table_schema, c.table_name) IN (
        ('public'::text, 'user'::text),
        ('public'::text, 'message'::text)
      )
    `

    expect((await backend.query(catalogSQL)).rows).toEqual([
      {
        schema: 'public',
        table: 'user',
        column: 'id',
        dataType: 'text',
        length: null,
        precision: null,
        scale: null,
        typtype: 'b',
        typename: 'text',
        elemTyptype: null,
        elemTypname: null,
      },
    ])
    expect(metadataSelects).toBe(1)
    provisioned = true
    expect((await backend.query(catalogSQL)).rows).toEqual([
      {
        schema: 'public',
        table: 'user',
        column: 'id',
        dataType: 'text',
        length: null,
        precision: null,
        scale: null,
        typtype: 'b',
        typename: 'text',
        elemTyptype: null,
        elemTypname: null,
      },
      {
        schema: 'public',
        table: 'message',
        column: 'id',
        dataType: 'text',
        length: null,
        precision: null,
        scale: null,
        typtype: 'b',
        typename: 'text',
        elemTyptype: null,
        elemTypname: null,
      },
    ])
    expect(metadataSelects).toBe(1)
  })

  test('tracks parser-backed publication membership without private table lists', async () => {
    const http = await startDoHttp((sql) => {
      if (sql.includes('sqlite_master')) {
        return {
          rows: [
            { name: 'message', sql: 'CREATE TABLE message (id text)' },
            { name: 'account', sql: 'CREATE TABLE account (id text)' },
          ],
          columns: ['name', 'sql'],
        }
      }
      if (sql.includes('PRAGMA table_info("message")')) {
        return {
          rows: [
            { cid: 0, name: 'id', type: 'text', notnull: 1, dflt_value: null, pk: 1 },
          ],
          columns: ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'],
        }
      }
      if (sql.includes('PRAGMA table_info("account")')) {
        return {
          rows: [
            { cid: 0, name: 'id', type: 'text', notnull: 1, dflt_value: null, pk: 1 },
          ],
          columns: ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'publication-membership-test')
    await backend.waitReady
    await backend.exec('CREATE PUBLICATION zero_chat FOR TABLE message')

    const result = await (backend as any).handleCatalogQuery(`
      SELECT schemaname, tablename, pubname
      FROM pg_publication_tables
      WHERE pubname IN ('zero_chat')
    `)

    expect(result.rows).toEqual([
      { schemaname: 'public', tablename: 'message', pubname: 'zero_chat' },
    ])
  })

  test('cascade DELETE sends change-tracked child statements (RETURNING) to the DO', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'fk-cascade-tracking-test')
    await backend.waitReady
    await backend.exec(`
      CREATE TABLE thread (id text PRIMARY KEY);
      CREATE TABLE message (id text PRIMARY KEY, "threadId" text REFERENCES thread(id) ON DELETE CASCADE);
      CREATE TABLE reaction (id text PRIMARY KEY, "messageId" text REFERENCES message(id) ON DELETE CASCADE);
      CREATE TABLE bookmark (id text PRIMARY KEY, "threadId" text REFERENCES thread(id) ON DELETE SET NULL);
      CREATE PUBLICATION zero_all FOR ALL TABLES;
    `)
    http.sqls.length = 0

    await backend.query('DELETE FROM thread WHERE id = $1', ['t1'])

    const sent = http.sqls.map((sql) => compactSQL(sql))
    const tracked = (re: RegExp) =>
      sent.some((sql) => re.test(sql) && /RETURNING/i.test(sql))
    // each cascade child reaches the DO as its own RETURNING-tracked write —
    // identical to a normal delete, so the deletion replicates. leaves-first:
    // reaction (grandchild) and message (child) deleted, bookmark link nulled.
    expect(tracked(/DELETE FROM "?reaction"?/i)).toBe(true)
    expect(tracked(/DELETE FROM "?message"?/i)).toBe(true)
    expect(tracked(/UPDATE "?bookmark"? SET/i)).toBe(true)
    expect(tracked(/DELETE FROM "?thread"?/i)).toBe(true)
    // and the child deletes are ordered before the parent thread delete
    const idx = (re: RegExp) => sent.findIndex((sql) => re.test(sql))
    expect(idx(/DELETE FROM "?reaction"?/i)).toBeLessThan(idx(/DELETE FROM "?message"?/i))
    expect(idx(/DELETE FROM "?message"?/i)).toBeLessThan(idx(/DELETE FROM "?thread"?/i))
  })

  test('synthesizes zero-cache publication metadata result sets', async () => {
    const http = await startDoHttp((sql) => {
      if (sql.includes('sqlite_master')) {
        return {
          rows: [
            { name: 'message', sql: 'CREATE TABLE message (id varchar PRIMARY KEY)' },
          ],
          columns: ['name', 'sql'],
        }
      }
      if (sql.includes('PRAGMA table_info("message")')) {
        return {
          rows: [
            { cid: 0, name: 'id', type: 'varchar', notnull: 1, dflt_value: null, pk: 1 },
            {
              cid: 1,
              name: 'deleted',
              type: 'INTEGER',
              notnull: 1,
              dflt_value: '0',
              pk: 0,
            },
          ],
          columns: ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'publication-metadata-test')
    await backend.waitReady
    await backend.exec(`
      CREATE TABLE message (
        id varchar PRIMARY KEY,
        deleted boolean NOT NULL DEFAULT false
      );
      CREATE PUBLICATION zero_chat FOR TABLE message;
    `)

    const results = await (backend as any).handleCatalogQueries(`
      SELECT schemaname AS "schema", tablename AS "table",
        json_object_agg(pubname, attnames) AS "publications"
      FROM pg_publication_tables pb
      WHERE pb.pubname IN ('zero_chat')
      GROUP BY schemaname, tablename;

      WITH published_columns AS (
        SELECT attname FROM pg_attribute
        JOIN pg_publication_tables pb ON attname = ANY(pb.attnames)
        WHERE pb.pubname IN ('zero_chat')
      )
      SELECT COALESCE(json_agg("table"), '[]'::json) as "tables" FROM published_columns;

      WITH indexed_columns AS (
        SELECT pg_indexes.indexname FROM pg_indexes
        JOIN pg_index ON true
      )
      SELECT COALESCE(json_agg("index"), '[]'::json) as "indexes" FROM indexed_columns;
    `)

    expect(results[0].rows).toEqual([
      {
        schema: 'public',
        table: 'message',
        publications: { zero_chat: ['id', 'deleted'] },
      },
    ])
    expect(results[1].rows[0].tables).toEqual([
      expect.objectContaining({
        name: 'message',
        primaryKey: ['id'],
        columns: expect.objectContaining({
          id: expect.objectContaining({ dataType: 'character varying', typeOID: 1043 }),
          deleted: expect.objectContaining({ dataType: 'boolean', typeOID: 16 }),
        }),
      }),
    ])
    expect(results[2]).toEqual({
      rows: [
        {
          indexes: [
            expect.objectContaining({
              schema: 'public',
              tableName: 'message',
              name: 'message_id_pkey',
              unique: true,
              isPrimaryKey: true,
              isImmediate: true,
              columns: { id: 'ASC' },
            }),
          ],
        },
      ],
      fields: [{ name: 'indexes', oid: 114 }],
    })

    const zero15 = await (backend as any).handleCatalogQueries(`
      WITH published_columns AS (
        SELECT attname FROM pg_attribute
        JOIN pg_publication_tables pb ON attname = ANY(pb.attnames)
        WHERE pb.pubname IN ('zero_chat')
      ),
      indexed_columns AS (
        SELECT pg_indexes.indexname FROM pg_indexes
        JOIN pg_publication_tables pb ON true
        JOIN pg_index ON true
        WHERE pb.pubname IN ('zero_chat')
      )
      SELECT json_build_object(
        'tables', COALESCE((SELECT json_agg("table") FROM published_columns), '[]'::json),
        'indexes', COALESCE((SELECT json_agg("index") FROM indexed_columns), '[]'::json)
      ) as "publishedSchema";
    `)

    expect(zero15[0]).toEqual({
      rows: [
        {
          publishedSchema: {
            tables: [
              expect.objectContaining({
                name: 'message',
                primaryKey: ['id'],
              }),
            ],
            indexes: [
              expect.objectContaining({
                tableName: 'message',
                name: 'message_id_pkey',
              }),
            ],
          },
        },
      ],
      fields: [{ name: 'publishedSchema', oid: 114 }],
    })
  })

  test('materializes zero-cache schema_specs function calls in writes', async () => {
    const http = await startDoHttp((sql) => {
      if (sql.includes('sqlite_master')) {
        return {
          rows: [
            { name: 'message', sql: 'CREATE TABLE message (id varchar PRIMARY KEY)' },
          ],
          columns: ['name', 'sql'],
        }
      }
      if (sql.includes('PRAGMA table_info("message")')) {
        return {
          rows: [
            { cid: 0, name: 'id', type: 'varchar', notnull: 1, dflt_value: null, pk: 1 },
            {
              cid: 1,
              name: 'deleted',
              type: 'INTEGER',
              notnull: 1,
              dflt_value: '0',
              pk: 0,
            },
          ],
          columns: ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'schema-specs-write-test')
    await backend.waitReady
    await backend.exec(`
      CREATE TABLE message (
        id varchar PRIMARY KEY,
        deleted boolean NOT NULL DEFAULT false
      );
      CREATE PUBLICATION zero_chat FOR TABLE message;
    `)
    await backend.exec(`
      DROP FUNCTION IF EXISTS chat_0.schema_specs();
      CREATE FUNCTION chat_0.schema_specs()
      RETURNS JSON
      STABLE
      AS $$
        SELECT json_build_object(
          'tables', '[]'::json,
          'indexes', '[]'::json
        ) AS "publishedSchema"
      $$ LANGUAGE sql;
    `)

    await backend.query(`
      INSERT INTO chat_0."publishedSchema" (current) VALUES (chat_0.schema_specs())
        ON CONFLICT (exists) DO UPDATE SET current = excluded.current
    `)

    const insert = sqlContaining(
      http.sqls,
      'INSERT INTO "chat_0_publishedSchema" ( "current" ) VALUES'
    )
    expect(insert).not.toContain('schema_specs')
    expect(compactSQL(insert)).toContain('VALUES ( ? )')
    const schemaParam = http.params.at(-1)?.[0]
    expect(typeof schemaParam).toBe('string')
    expect(schemaParam).toContain('"tables":[{')
    expect(schemaParam).toContain('"name":"message"')
    expect(schemaParam).toContain('"indexes":[{')
    expect(compactSQL(insert)).toContain(
      'ON CONFLICT ("exists") DO UPDATE SET "current" = excluded."current"'
    )
  })

  test('tracks published table writes with parser-derived returning SQL', async () => {
    const http = await startDoHttp((sql) => {
      if (compactSQL(sql).includes('RETURNING *')) {
        return { rows: [{ id: 't1', body: 'hello' }], columns: ['id', 'body'] }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'write-tracking-test')
    await backend.waitReady
    await backend.exec('CREATE PUBLICATION zero_chat FOR TABLE task_item')

    const result = await backend.execProtocolRaw(
      msg(0x51, cstr("INSERT INTO task_item (id, body) VALUES ('t1', 'hello')"))
    )

    expect(messageTypes(result)).toEqual(['C', 'Z'])
    const tracked = http.bodies.find((body) => body.track)
    expect(tracked.track).toEqual({
      tableName: 'public.task_item',
      operation: 'INSERT',
      returnRows: false,
    })
    expect(compactSQL(tracked.sql)).toContain('RETURNING *')
  })

  test('signals replication immediately after tracked writes', async () => {
    const http = await startDoHttp((sql) => {
      if (compactSQL(sql).includes('RETURNING *')) {
        return { rows: [{ id: 't1', body: 'hello' }], columns: ['id', 'body'] }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'write-signal-test')
    await backend.waitReady
    await backend.exec('CREATE PUBLICATION zero_chat FOR TABLE task_item')

    const globalObject = globalThis as any
    const previousWakeup = globalObject.__orez_signal_replication
    let wakeups = 0
    globalObject.__orez_signal_replication = () => {
      wakeups++
    }
    try {
      await backend.query("INSERT INTO task_item (id, body) VALUES ('t1', 'hello')")
    } finally {
      if (previousWakeup === undefined) {
        delete globalObject.__orez_signal_replication
      } else {
        globalObject.__orez_signal_replication = previousWakeup
      }
    }

    expect(wakeups).toBe(1)
    const trackedBodies = http.bodies.filter((body) => body.track)
    expect(trackedBodies.every((body) => body.track.transactionID === undefined)).toBe(
      true
    )
  })

  test('defers tracked replication signals until transaction commit', async () => {
    const http = await startDoHttp((sql) => {
      if (compactSQL(sql).includes('RETURNING *')) {
        return { rows: [{ id: 't1', body: 'hello' }], columns: ['id', 'body'] }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'tx-write-signal-test')
    await backend.waitReady
    await backend.exec('CREATE PUBLICATION zero_chat FOR TABLE task_item')

    const globalObject = globalThis as any
    const previousWakeup = globalObject.__orez_signal_replication
    let wakeups = 0
    globalObject.__orez_signal_replication = () => {
      wakeups++
    }
    try {
      await backend.execProtocolRaw(msg(0x51, cstr('BEGIN')))
      await backend.query("INSERT INTO task_item (id, body) VALUES ('t1', 'hello')")
      await backend.query("UPDATE task_item SET body = 'world' WHERE id = 't1'")

      expect(wakeups).toBe(0)

      await backend.execProtocolRaw(msg(0x51, cstr('COMMIT')))
    } finally {
      if (previousWakeup === undefined) {
        delete globalObject.__orez_signal_replication
      } else {
        globalObject.__orez_signal_replication = previousWakeup
      }
    }

    expect(wakeups).toBe(1)
    const trackedBodies = http.bodies.filter((body) => body.track)
    expect(trackedBodies).toHaveLength(2)
    const transactionIDs = new Set(
      trackedBodies.map((body) => body.track.transactionID).filter(Boolean)
    )
    expect(transactionIDs.size).toBe(1)
    expect(http.requests.some((url) => url.pathname === '/commit-tx')).toBe(true)
    expect(http.requests.some((url) => url.pathname === '/rollback-tx')).toBe(false)
  })

  test('drops deferred tracked replication signals on transaction rollback', async () => {
    const http = await startDoHttp((sql) => {
      if (compactSQL(sql).includes('RETURNING *')) {
        return { rows: [{ id: 't1', body: 'hello' }], columns: ['id', 'body'] }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'tx-rollback-signal-test')
    await backend.waitReady
    await backend.exec('CREATE PUBLICATION zero_chat FOR TABLE task_item')

    const globalObject = globalThis as any
    const previousWakeup = globalObject.__orez_signal_replication
    let wakeups = 0
    globalObject.__orez_signal_replication = () => {
      wakeups++
    }
    try {
      await backend.execProtocolRaw(msg(0x51, cstr('BEGIN')))
      await backend.query("INSERT INTO task_item (id, body) VALUES ('t1', 'hello')")
      await backend.execProtocolRaw(msg(0x51, cstr('ROLLBACK')))
    } finally {
      if (previousWakeup === undefined) {
        delete globalObject.__orez_signal_replication
      } else {
        globalObject.__orez_signal_replication = previousWakeup
      }
    }

    expect(wakeups).toBe(0)
    const trackedBodies = http.bodies.filter((body) => body.track)
    expect(trackedBodies).toHaveLength(1)
    expect(typeof trackedBodies[0].track.transactionID).toBe('string')
    expect(http.requests.some((url) => url.pathname === '/commit-tx')).toBe(false)
    expect(http.requests.some((url) => url.pathname === '/rollback-tx')).toBe(true)
  })

  test('tracks full published rows while preserving client RETURNING projection', async () => {
    const http = await startDoHttp((sql) => {
      const compact = compactSQL(sql)
      if (compact.includes('RETURNING *')) {
        return {
          rows: [{ id: 't1', body: 'hello', __orez_returning_1: 'HELLO' }],
          columns: ['id', 'body', '__orez_returning_1'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'write-returning-test')
    await backend.waitReady
    await backend.exec('CREATE TABLE task_item (id TEXT PRIMARY KEY, body TEXT)')
    await backend.exec('CREATE PUBLICATION zero_chat FOR TABLE task_item')

    const result = await backend.execProtocolRaw(
      msg(
        0x51,
        cstr(
          `INSERT INTO task_item (id, body)
           VALUES ('t1', 'hello')
           RETURNING "id", upper("body") AS "bodyUpper"`
        )
      )
    )

    expect(messageTypes(result)).toEqual(['T', 'D', 'C', 'Z'])
    expect(rowDescriptionNames(result)).toEqual(['id', 'bodyUpper'])
    expect(dataRowValues(result)).toEqual([['t1', 'HELLO']])

    const tracked = http.bodies.find((body) => body.track)
    expect(tracked.track).toEqual({
      tableName: 'public.task_item',
      operation: 'INSERT',
      returnRows: true,
      rowColumns: ['id', 'body'],
    })
    expect(compactSQL(tracked.sql)).toContain('RETURNING *')
    expect(compactSQL(tracked.sql)).toContain('__orez_returning_1')
  })

  test("promotes a keyless table's unique index to its primary key in catalog answers", async () => {
    // the accountMember shape: no PK on the table, key carried by a separate
    // <table>_pkey unique index (soot generateDDL for composite drizzle
    // primaryKey()). without promotion, zero builds a keyless replica spec and
    // its change processor throws on the first UPDATE (2026-07-10 soot prod).
    const http = await startDoHttp((sql) => {
      if (sql.includes('sqlite_master')) {
        return {
          rows: [
            {
              name: 'member',
              sql: 'CREATE TABLE member ("accountId" TEXT NOT NULL, "userId" TEXT NOT NULL, role TEXT NOT NULL)',
            },
          ],
          columns: ['name', 'sql'],
        }
      }
      if (sql.includes('PRAGMA table_info("member")')) {
        return {
          rows: [
            {
              cid: 0,
              name: 'accountId',
              type: 'TEXT',
              notnull: 1,
              dflt_value: null,
              pk: 0,
            },
            { cid: 1, name: 'userId', type: 'TEXT', notnull: 1, dflt_value: null, pk: 0 },
            { cid: 2, name: 'role', type: 'TEXT', notnull: 1, dflt_value: null, pk: 0 },
          ],
          columns: ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'],
        }
      }
      if (sql.includes('PRAGMA index_list("member")')) {
        return {
          rows: [{ seq: 0, name: 'member_pkey', unique: 1, origin: 'c', partial: 0 }],
          columns: ['seq', 'name', 'unique', 'origin', 'partial'],
        }
      }
      if (sql.includes('PRAGMA index_xinfo("member_pkey")')) {
        return {
          rows: [
            { seqno: 0, cid: 0, name: 'accountId', desc: 0, key: 1 },
            { seqno: 1, cid: 1, name: 'userId', desc: 0, key: 1 },
            { seqno: 2, cid: -1, name: null, desc: 0, key: 0 },
          ],
          columns: ['seqno', 'cid', 'name', 'desc', 'key'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'promoted-key-test')
    await backend.waitReady

    const result = await backend.query<{
      kind: string
      table_name: string
      column_name: string
      ordinal_position: number
    }>(
      `SELECT 'pk' AS kind, tc.table_schema, tc.table_name, kcu.column_name, NULL AS data_type, kcu.ordinal_position
       FROM information_schema.table_constraints tc
       JOIN information_schema.key_column_usage kcu
         ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
       WHERE tc.constraint_type = 'PRIMARY KEY'
         AND tc.table_schema = ANY($1)
       UNION ALL
       SELECT 'col' AS kind, table_schema, table_name, column_name, data_type, ordinal_position
       FROM information_schema.columns
       WHERE table_schema = ANY($1)
       ORDER BY table_schema, table_name, kind, ordinal_position`,
      [['public']]
    )

    const memberPk = result.rows.filter(
      (row) => row.kind === 'pk' && row.table_name === 'member'
    )
    expect(memberPk.map((row) => row.column_name)).toEqual(['accountId', 'userId'])
    expect(memberPk.map((row) => row.ordinal_position)).toEqual([1, 2])
  })

  test('promotion trusts <table>_pkey on nullable legacy columns; other indexes need NOT NULL and full coverage', async () => {
    // three keyless tables:
    // - member: LEGACY shape — nullable physical columns (no metadata either),
    //   <table>_pkey unique index. must promote (the pkey name IS the
    //   generator's primary-key convention; prod accountMember predates
    //   NOT NULL in its DDL).
    // - audit: only a PARTIAL unique index and a unique index over a nullable
    //   column. must NOT promote (no row identity).
    // - pref: a 2-column <table>_pkey AND a narrower single-column unique
    //   index. the pkey convention must win over narrowest.
    const tableInfo: Record<string, unknown[]> = {
      member: [
        { cid: 0, name: 'accountId', type: 'TEXT', notnull: 0, dflt_value: null, pk: 0 },
        { cid: 1, name: 'userId', type: 'TEXT', notnull: 0, dflt_value: null, pk: 0 },
        { cid: 2, name: 'role', type: 'TEXT', notnull: 0, dflt_value: null, pk: 0 },
      ],
      audit: [
        { cid: 0, name: 'actor', type: 'TEXT', notnull: 0, dflt_value: null, pk: 0 },
        { cid: 1, name: 'event', type: 'TEXT', notnull: 1, dflt_value: null, pk: 0 },
      ],
      pref: [
        { cid: 0, name: 'orgId', type: 'TEXT', notnull: 1, dflt_value: null, pk: 0 },
        { cid: 1, name: 'slug', type: 'TEXT', notnull: 1, dflt_value: null, pk: 0 },
        { cid: 2, name: 'alias', type: 'TEXT', notnull: 1, dflt_value: null, pk: 0 },
      ],
    }
    const indexList: Record<string, unknown[]> = {
      member: [{ seq: 0, name: 'member_pkey', unique: 1, origin: 'c', partial: 0 }],
      audit: [
        { seq: 0, name: 'audit_partial_key', unique: 1, origin: 'c', partial: 1 },
        { seq: 1, name: 'audit_actor_key', unique: 1, origin: 'c', partial: 0 },
      ],
      pref: [
        { seq: 0, name: 'pref_alias_key', unique: 1, origin: 'c', partial: 0 },
        { seq: 1, name: 'pref_pkey', unique: 1, origin: 'c', partial: 0 },
      ],
    }
    const indexColumns: Record<string, unknown[]> = {
      member_pkey: [
        { seqno: 0, cid: 0, name: 'accountId', desc: 0, key: 1 },
        { seqno: 1, cid: 1, name: 'userId', desc: 0, key: 1 },
      ],
      audit_partial_key: [{ seqno: 0, cid: 1, name: 'event', desc: 0, key: 1 }],
      audit_actor_key: [{ seqno: 0, cid: 0, name: 'actor', desc: 0, key: 1 }],
      pref_alias_key: [{ seqno: 0, cid: 2, name: 'alias', desc: 0, key: 1 }],
      pref_pkey: [
        { seqno: 0, cid: 0, name: 'orgId', desc: 0, key: 1 },
        { seqno: 1, cid: 1, name: 'slug', desc: 0, key: 1 },
      ],
    }
    const http = await startDoHttp((sql) => {
      if (sql.includes('sqlite_master')) {
        return {
          rows: Object.keys(tableInfo).map((name) => ({
            name,
            sql: `CREATE TABLE ${name} (x TEXT)`,
          })),
          columns: ['name', 'sql'],
        }
      }
      const tableMatch = sql.match(/PRAGMA table_info\("(\w+)"\)/)
      if (tableMatch) {
        return {
          rows: tableInfo[tableMatch[1]] ?? [],
          columns: ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'],
        }
      }
      const listMatch = sql.match(/PRAGMA index_list\("(\w+)"\)/)
      if (listMatch) {
        return {
          rows: indexList[listMatch[1]] ?? [],
          columns: ['seq', 'name', 'unique', 'origin', 'partial'],
        }
      }
      const xinfoMatch = sql.match(/PRAGMA index_xinfo\("(\w+)"\)/)
      if (xinfoMatch) {
        return {
          rows: indexColumns[xinfoMatch[1]] ?? [],
          columns: ['seqno', 'cid', 'name', 'desc', 'key'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'promotion-qualification-test')
    await backend.waitReady

    const result = await backend.query<{
      kind: string
      table_name: string
      column_name: string
      ordinal_position: number
    }>(
      `SELECT 'pk' AS kind, tc.table_schema, tc.table_name, kcu.column_name, NULL AS data_type, kcu.ordinal_position
       FROM information_schema.table_constraints tc
       JOIN information_schema.key_column_usage kcu
         ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
       WHERE tc.constraint_type = 'PRIMARY KEY'
         AND tc.table_schema = ANY($1)
       UNION ALL
       SELECT 'col' AS kind, table_schema, table_name, column_name, data_type, ordinal_position
       FROM information_schema.columns
       WHERE table_schema = ANY($1)
       ORDER BY table_schema, table_name, kind, ordinal_position`,
      [['public']]
    )

    const pkByTable = new Map<string, string[]>()
    for (const row of result.rows) {
      if (row.kind !== 'pk') continue
      const list = pkByTable.get(row.table_name) ?? []
      list.push(row.column_name)
      pkByTable.set(row.table_name, list)
    }
    // legacy nullable pkey: promoted
    expect(pkByTable.get('member')).toEqual(['accountId', 'userId'])
    // partial + nullable-column indexes: no identity, no promotion
    expect(pkByTable.get('audit')).toBeUndefined()
    // pkey convention beats the narrower arbitrary unique index
    expect(pkByTable.get('pref')).toEqual(['orgId', 'slug'])
  })

  test('synthesizes primary-key rows for zero-cache relation metadata queries', async () => {
    const http = await startDoHttp((sql) => {
      if (sql.includes('sqlite_master')) {
        return {
          rows: [{ name: 'server', sql: 'CREATE TABLE server (id varchar PRIMARY KEY)' }],
          columns: ['name', 'sql'],
        }
      }
      if (sql.includes('PRAGMA table_info("server")')) {
        return {
          rows: [
            { cid: 0, name: 'id', type: 'varchar', notnull: 1, dflt_value: null, pk: 1 },
            {
              cid: 1,
              name: 'name',
              type: 'varchar',
              notnull: 1,
              dflt_value: null,
              pk: 0,
            },
          ],
          columns: ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'relation-metadata-test')
    await backend.waitReady

    const result = await backend.query<{
      kind: string
      table_schema: string
      table_name: string
      column_name: string
      data_type: string | null
      ordinal_position: number
    }>(
      `SELECT 'pk' AS kind, tc.table_schema, tc.table_name, kcu.column_name, NULL AS data_type, kcu.ordinal_position
       FROM information_schema.table_constraints tc
       JOIN information_schema.key_column_usage kcu
         ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
       WHERE tc.constraint_type = 'PRIMARY KEY'
         AND tc.table_schema = ANY($1)
       UNION ALL
       SELECT 'col' AS kind, table_schema, table_name, column_name, data_type, ordinal_position
       FROM information_schema.columns
       WHERE table_schema = ANY($1)
       ORDER BY table_schema, table_name, kind, ordinal_position`,
      [['public']]
    )

    expect(result.rows).toContainEqual({
      kind: 'pk',
      table_schema: 'public',
      table_name: 'server',
      column_name: 'id',
      data_type: null,
      ordinal_position: 1,
    })
    expect(result.rows).toContainEqual({
      kind: 'col',
      table_schema: 'public',
      table_name: 'server',
      column_name: 'name',
      data_type: 'character varying',
      ordinal_position: 2,
    })
  })

  test('does not track unpublished public table writes', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'unpublished-write-test')
    await backend.waitReady

    await backend.execProtocolRaw(
      msg(0x51, cstr("INSERT INTO private_note (id) VALUES ('n1')"))
    )

    expect(http.bodies.some((body) => body.track)).toBe(false)
  })

  test('synthesizes primary and unique index metadata for published tables', async () => {
    const http = await startDoHttp((sql) => {
      if (sql.includes('sqlite_master')) {
        return {
          rows: [
            {
              name: 'account',
              sql: 'CREATE TABLE account (id text PRIMARY KEY, email text NOT NULL UNIQUE)',
            },
          ],
          columns: ['name', 'sql'],
        }
      }
      if (sql.includes('PRAGMA table_info("account")')) {
        return {
          rows: [
            { cid: 0, name: 'id', type: 'text', notnull: 1, dflt_value: null, pk: 1 },
            {
              cid: 1,
              name: 'email',
              type: 'text',
              notnull: 1,
              dflt_value: null,
              pk: 0,
            },
          ],
          columns: ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'],
        }
      }
      if (sql.includes('PRAGMA index_list("account")')) {
        return {
          rows: [
            {
              seq: 0,
              name: 'sqlite_autoindex_account_2',
              unique: 1,
              origin: 'u',
              partial: 0,
            },
            {
              seq: 1,
              name: 'sqlite_autoindex_account_1',
              unique: 1,
              origin: 'pk',
              partial: 0,
            },
          ],
          columns: ['seq', 'name', 'unique', 'origin', 'partial'],
        }
      }
      if (sql.includes('PRAGMA index_xinfo("sqlite_autoindex_account_2")')) {
        return {
          rows: [
            { seqno: 0, cid: 1, name: 'email', desc: 0, key: 1 },
            { seqno: 1, cid: -1, name: null, desc: 0, key: 0 },
          ],
          columns: ['seqno', 'cid', 'name', 'desc', 'key'],
        }
      }
      if (sql.includes('PRAGMA index_xinfo("sqlite_autoindex_account_1")')) {
        return {
          rows: [
            { seqno: 0, cid: 0, name: 'id', desc: 0, key: 1 },
            { seqno: 1, cid: -1, name: null, desc: 0, key: 0 },
          ],
          columns: ['seqno', 'cid', 'name', 'desc', 'key'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'published-index-test')
    await backend.waitReady
    await backend.exec('CREATE PUBLICATION zero_chat FOR TABLE account')

    const result = await (backend as any).handleCatalogQuery(`
      WITH indexed_columns AS (
        SELECT pg_indexes.indexname FROM pg_indexes
        JOIN pg_index ON true
        JOIN pg_publication_tables pb ON true
        WHERE pb.pubname IN ('zero_chat')
      )
      SELECT COALESCE(json_agg("index"), '[]'::json) as "indexes"
      FROM indexed_columns;
    `)

    expect(result).toEqual({
      rows: [
        {
          indexes: [
            expect.objectContaining({
              schema: 'public',
              tableName: 'account',
              name: 'account_id_pkey',
              unique: true,
              isPrimaryKey: true,
              isImmediate: true,
              columns: { id: 'ASC' },
            }),
            expect.objectContaining({
              schema: 'public',
              tableName: 'account',
              name: 'account_email_key',
              unique: true,
              isPrimaryKey: false,
              isImmediate: true,
              columns: { email: 'ASC' },
            }),
          ],
        },
      ],
      fields: [{ name: 'indexes', oid: 114 }],
    })
  })

  test('uses parsed ADD COLUMN primary-key metadata when SQLite cannot alter the physical key', async () => {
    const http = await startDoHttp((sql) => {
      if (sql.includes('sqlite_master')) {
        return {
          rows: [
            {
              name: 'appInstall',
              sql: 'CREATE TABLE "appInstall" ("serverId" text, "creatorId" text, "appId" text, "id" text, PRIMARY KEY ("serverId", "creatorId", "appId"))',
            },
          ],
          columns: ['name', 'sql'],
        }
      }
      if (sql.includes('PRAGMA table_info("appInstall")')) {
        return {
          rows: [
            {
              cid: 0,
              name: 'serverId',
              type: 'text',
              notnull: 0,
              dflt_value: null,
              pk: 1,
            },
            {
              cid: 1,
              name: 'creatorId',
              type: 'text',
              notnull: 0,
              dflt_value: null,
              pk: 2,
            },
            {
              cid: 2,
              name: 'appId',
              type: 'text',
              notnull: 0,
              dflt_value: null,
              pk: 3,
            },
            { cid: 3, name: 'id', type: 'text', notnull: 0, dflt_value: null, pk: 0 },
          ],
          columns: ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'],
        }
      }
      if (sql.includes('PRAGMA index_list("appInstall")')) {
        return {
          rows: [
            {
              seq: 0,
              name: 'sqlite_autoindex_appInstall_1',
              unique: 1,
              origin: 'pk',
              partial: 0,
            },
          ],
          columns: ['seq', 'name', 'unique', 'origin', 'partial'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'alter-primary-key-test')
    await backend.waitReady
    await backend.exec(`
      ALTER TABLE "appInstall"
        ADD COLUMN "id" varchar PRIMARY KEY NOT NULL;
      CREATE PUBLICATION zero_chat FOR TABLE "appInstall";
    `)

    const results = await (backend as any).handleCatalogQueries(`
      WITH published_columns AS (
        SELECT attname FROM pg_attribute
        JOIN pg_publication_tables pb ON attname = ANY(pb.attnames)
        WHERE pb.pubname IN ('zero_chat')
      )
      SELECT COALESCE(json_agg("table"), '[]'::json) as "tables" FROM published_columns;

      WITH indexed_columns AS (
        SELECT pg_indexes.indexname FROM pg_indexes
        JOIN pg_index ON true
      )
      SELECT COALESCE(json_agg("index"), '[]'::json) as "indexes" FROM indexed_columns;
    `)

    expect(results[0].rows[0].tables).toEqual([
      expect.objectContaining({
        name: 'appInstall',
        primaryKey: ['id'],
        columns: expect.objectContaining({
          id: expect.objectContaining({ notNull: true }),
        }),
      }),
    ])
    expect(results[1].rows[0].indexes).toEqual([
      expect.objectContaining({
        tableName: 'appInstall',
        unique: true,
        isPrimaryKey: true,
        columns: { id: 'ASC' },
      }),
    ])
  })

  test('does not convert backend SQL errors into empty result sets', async () => {
    const http = await startDoHttp((sql) =>
      sql.includes('SELECT broken')
        ? new Response('boom', { status: 500 })
        : { rows: [], columns: [] }
    )
    const backend = new DoBackend(http.url, 'postgres', 'error-test')
    await backend.waitReady

    await expect(backend.query('SELECT broken')).rejects.toThrow('HTTP 500: boom')
  })

  test('flattens public schema table references before sending SQL to DO', async () => {
    const http = await startDoHttp(() => ({ rows: [{ count: 0 }], columns: ['count'] }))
    const backend = new DoBackend(http.url, 'postgres', 'schema-flatten-test')
    await backend.waitReady

    await backend.query('SELECT count(*) FROM public.migrations')

    expect(
      http.sqls.some((sql) => compactSQL(sql).includes('FROM public_migrations'))
    ).toBe(true)
    expect(
      http.sqls.some((sql) => compactSQL(sql).includes('FROM public.migrations'))
    ).toBe(false)
  })

  test('rewrites implicit qualifiers for schema-qualified select joins', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'schema-join-flatten-test')
    await backend.waitReady

    await backend.query(
      'SELECT replicas.slot, "shardConfig".publications FROM chat_0.replicas JOIN chat_0."shardConfig" ON 1 WHERE version = $1',
      ['v1']
    )

    const sent = compactSQL(http.sqls.at(-1) || '')
    expect(sent).toContain('chat_0_replicas.slot')
    expect(sent).toContain('"chat_0_shardConfig".publications')
    expect(sent).toContain('FROM chat_0_replicas')
    expect(sent).toContain('JOIN "chat_0_shardConfig"')
    expect(sent).not.toMatch(/(^|[^_])replicas\.slot/)
    expect(sent).not.toMatch(/(^|[^_])"shardConfig"\.publications/)
  })

  test('rewrites implicit qualifiers in schema-qualified upsert conflict clauses', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'schema-upsert-flatten-test')
    await backend.waitReady

    await backend.query(
      `
      INSERT INTO chat_0.replicas ("slot", "version")
      VALUES ($1, $2)
      ON CONFLICT ("slot") DO UPDATE SET "version" = excluded."version"
      WHERE replicas."version" IS DISTINCT FROM excluded."version"
    `,
      ['slot_1', 'v2']
    )

    const sent = compactSQL(http.sqls.at(-1) || '')
    expect(sent).toContain('INSERT INTO chat_0_replicas')
    expect(sent).toContain('WHERE chat_0_replicas.version IS DISTINCT FROM')
    expect(sent).toContain('excluded.version')
    expect(sent).not.toMatch(/(^|[^_])replicas\.version/)
  })

  test('neutralizes the _orez._drop_zero_slot cleanup call so the slot-drop SELECT parses on sqlite', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'drop-zero-slot-test')
    await backend.waitReady

    // the form zero-cache's dropUnclaimedSlots emits after orez's browser-proxy
    // rewrite of pg_drop_replication_slot -> _orez._drop_zero_slot. left intact
    // on the DO sqlite backend the schema-qualified function call makes sqlite
    // throw `near "(": syntax error`; because zero-cache AWAITS this orphan-slot
    // cleanup on the initial-sync path it wedged the embed before it could
    // signal ready (120s timeout, dead /sync). see soot incident 2026-06-14.
    await backend.query(
      'SELECT slot_name AS slot, _orez._drop_zero_slot(slot_name) FROM _orez._zero_replication_slots LEFT JOIN soot_0.replicas AS replica ON slot_name = slot WHERE slot_name LIKE $1 AND NOT active AND replica.id IS NULL',
      ['soot_%']
    )

    const sent = compactSQL(http.sqls.at(-1) || '')
    // the schema-qualified call (sqlite can't parse) must be gone, neutralized
    // to its slot-name arg so the projection still returns the matched slots.
    expect(sent).not.toContain('_drop_zero_slot')
    expect(sent).not.toContain('_orez.')
    expect(sent).toContain('slot_name AS slot')
  })

  test('does not rewrite unqualified columns that match schema-qualified table names', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'schema-column-flatten-test')
    await backend.waitReady

    await backend.query('SELECT permissions, hash, lock FROM chat.permissions')

    const sent = compactSQL(http.sqls.at(-1) || '')
    expect(sent).toContain('SELECT permissions, hash, lock FROM chat_permissions')
    expect(sent).not.toContain('SELECT chat_permissions, hash, lock')
  })

  test('flushes transactional writes before reads so migrations can see DDL', async () => {
    const http = await startDoHttp((sql) => {
      if (/select\s+name\s+from\s+public_migrations/i.test(sql)) {
        return { rows: [], columns: ['name'] }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'transaction-read-test')
    await backend.waitReady

    await backend.execProtocolRaw(msg(0x51, cstr('BEGIN')))
    await backend.execProtocolRaw(
      msg(
        0x51,
        cstr(`
          CREATE TABLE IF NOT EXISTS public.migrations (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL
          )
        `)
      )
    )
    await backend.execProtocolRaw(msg(0x51, cstr('SELECT name FROM public.migrations')))

    const createIndex = http.sqls.findIndex((sql) =>
      /create\s+table\s+if\s+not\s+exists\s+public_migrations/i.test(sql)
    )
    const selectIndex = http.sqls.findIndex((sql) =>
      /select\s+name\s+from\s+public_migrations/i.test(sql)
    )
    expect(createIndex).toBeGreaterThanOrEqual(0)
    expect(selectIndex).toBeGreaterThan(createIndex)
    await backend.execProtocolRaw(msg(0x51, cstr('ROLLBACK')))
  })

  test('emulates a non-PK BIGSERIAL column with an AFTER INSERT trigger', async () => {
    // zero 1.6's replicas table declares `rank BIGSERIAL` (not the PK). SQLite
    // only auto-increments an INTEGER PRIMARY KEY, so the column would stay NULL
    // and zero throws "Expected bigint at rank. Got null". the backend must emit
    // a sequence-emulating trigger.
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'bigserial-trigger-test')
    await backend.waitReady

    http.sqls.length = 0
    await backend.execProtocolRaw(
      msg(
        0x51,
        cstr(`
          CREATE TABLE IF NOT EXISTS chat_0.replicas (
            "id" TEXT PRIMARY KEY,
            "rank" BIGSERIAL,
            "slot" TEXT NOT NULL
          )
        `)
      )
    )

    const trigger = http.sqls.find((sql) => /CREATE TRIGGER/i.test(sql))
    expect(trigger).toBeDefined()
    const compact = compactSQL(trigger || '')
    expect(compact).toContain('AFTER INSERT ON "chat_0_replicas"')
    expect(compact).toContain('WHEN NEW."rank" IS NULL')
    expect(compact).toContain(
      'SET "rank" = (SELECT coalesce(max("rank"), 0) + 1 FROM "chat_0_replicas")'
    )
    // the BIGSERIAL type itself must be rewritten to a plain integer column
    const create = http.sqls.find((sql) => /CREATE TABLE/i.test(sql)) || ''
    expect(create).not.toMatch(/BIGSERIAL/i)
  })

  test('intercepts parser-recognized transaction variants before DO execution', async () => {
    // The Durable Object refuses raw BEGIN/SAVEPOINT (it requires the JS-side
    // transaction API). All PG transaction control statements must be handled
    // locally and never reach the DO as SQL.
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'transaction-variant-test')
    await backend.waitReady

    const begin = await backend.execProtocolRaw(
      msg(0x51, cstr('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ'))
    )
    await backend.execProtocolRaw(msg(0x51, cstr('SAVEPOINT zero_migrate')))
    await backend.execProtocolRaw(msg(0x51, cstr('RELEASE SAVEPOINT zero_migrate')))
    await backend.execProtocolRaw(msg(0x51, cstr('COMMIT')))

    expect(messageTypes(begin)).toEqual(['C', 'Z'])
    const sent = http.sqls.map((sql) => sql.trim().toUpperCase())
    expect(sent.some((sql) => sql.startsWith('BEGIN'))).toBe(false)
    expect(sent.some((sql) => sql.startsWith('COMMIT'))).toBe(false)
    expect(sent.some((sql) => sql.startsWith('SAVEPOINT'))).toBe(false)
    expect(sent.some((sql) => sql.startsWith('RELEASE'))).toBe(false)
  })

  test('reports ReadyForQuery transaction status while a transaction is open', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: ['ok'] }))
    const backend = new DoBackend(http.url, 'postgres', 'transaction-status-test')
    await backend.waitReady

    const begin = await backend.execProtocolRaw(msg(0x51, cstr('BEGIN')))
    const select = await backend.execProtocolRaw(msg(0x51, cstr('SELECT 1 AS ok')))
    const commit = await backend.execProtocolRaw(msg(0x51, cstr('COMMIT')))

    expect(readyForQueryStatuses(begin)).toEqual(['T'])
    expect(readyForQueryStatuses(select)).toEqual(['T'])
    expect(readyForQueryStatuses(commit)).toEqual(['I'])
  })

  test('keeps extended-protocol Sync in transaction state after BEGIN', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(
      http.url,
      'postgres',
      'extended-transaction-status-test'
    )
    await backend.waitReady

    await backend.execProtocolRaw(parseMessage('BEGIN'))
    await backend.execProtocolRaw(bindStatement())
    await backend.execProtocolRaw(executePortal())
    const sync = await backend.execProtocolRaw(msg(0x53, new Uint8Array(0)))

    expect(readyForQueryStatuses(sync)).toEqual(['T'])
    await backend.execProtocolRaw(parseMessage('ROLLBACK'))
    await backend.execProtocolRaw(bindStatement())
    await backend.execProtocolRaw(executePortal())
  })

  test('returns command completion for extended transaction starts', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'extended-begin-test')
    await backend.waitReady

    await backend.execProtocolRaw(parseMessage('BEGIN'))
    await backend.execProtocolRaw(bindStatement())
    const result = await backend.execProtocolRaw(executePortal())

    expect(messageTypes(result)).toEqual(['C'])
    expect(http.sqls.some((sql) => compactSQL(sql).startsWith('BEGIN'))).toBe(false)
    await backend.execProtocolRaw(parseMessage('ROLLBACK'))
    await backend.execProtocolRaw(bindStatement())
    await backend.execProtocolRaw(executePortal())
  })

  test('clears the rewrite cache on transaction rollback', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'rewrite-cache-rollback-test')
    await backend.waitReady

    await backend.exec('SELECT 1')
    const beforeCount = http.sqls.filter((sql) => compactSQL(sql) === 'SELECT 1').length

    await backend.execProtocolRaw(msg(0x51, cstr('BEGIN')))
    await backend.execProtocolRaw(msg(0x51, cstr('ROLLBACK')))

    await backend.exec('SELECT 1')
    // BEGIN / ROLLBACK don't reach the DO; the cache invalidation should
    // re-issue the SELECT.
    expect(http.sqls.filter((sql) => compactSQL(sql) === 'SELECT 1').length).toBe(
      beforeCount + 1
    )
  })

  test('snapshots extended transaction writes for rollback', async () => {
    const http = await startDoHttp((sql) => {
      if (sql.includes('sqlite_master')) {
        return { rows: [{ ok: 1 }], columns: ['ok'] }
      }
      if (compactSQL(sql).includes('RETURNING *')) {
        return {
          rows: [{ clientGroupID: 'cg', clientID: 'client-a', lastMutationID: 1 }],
          columns: ['clientGroupID', 'clientID', 'lastMutationID'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'extended-tx-session-test')
    await backend.waitReady

    await backend.execProtocolRaw(parseMessage('BEGIN'))
    await backend.execProtocolRaw(bindStatement())
    await backend.execProtocolRaw(executePortal())

    await backend.execProtocolRaw(
      parseMessage(`
        INSERT INTO "chat_0"."clients" AS current
          ("clientGroupID", "clientID", "lastMutationID")
        VALUES ('cg', 'client-a', 1)
        ON CONFLICT ("clientGroupID", "clientID")
        DO UPDATE SET "lastMutationID" = current."lastMutationID" + 1
        RETURNING "lastMutationID"
      `)
    )
    await backend.execProtocolRaw(bindStatement())
    const result = await backend.execProtocolRaw(executePortal())
    expect(dataRowValues(result)).toEqual([['1']])

    await backend.execProtocolRaw(parseMessage('ROLLBACK'))
    await backend.execProtocolRaw(bindStatement())
    await backend.execProtocolRaw(executePortal())

    // the snapshot and its journal manifest row land in one atomic /batch
    expect(
      http.sqls.some((sql) =>
        /CREATE TABLE "_orez_tx_.*_chat_0_clients" AS SELECT \* FROM "chat_0_clients"/.test(
          sql
        )
      )
    ).toBe(true)
    const manifestBatch = http.bodies.find(
      (body) =>
        Array.isArray(body.statements) &&
        body.statements.some(
          (statement: any) =>
            typeof statement === 'object' &&
            statement.sql?.includes('INSERT INTO "_orez_tx_manifest"')
        )
    )
    expect(manifestBatch).toBeTruthy()
    const manifestInsert = manifestBatch.statements.find(
      (statement: any) =>
        typeof statement === 'object' &&
        statement.sql?.includes('INSERT INTO "_orez_tx_manifest"')
    )
    expect(manifestInsert.params?.[2]).toBe('chat_0_clients')
    // rollback is ONE atomic server-side call carrying the journaled tx id
    expect(http.requests.some((url) => url.pathname === '/rollback-tx')).toBe(true)
    const rollbackBody = http.bodies.find((body) => body.transactionID)
    expect(rollbackBody?.transactionID).toBe(manifestInsert.params?.[0])
  })

  test('serializes concurrent public operations on one backend', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'operation-queue-test')
    await backend.waitReady

    const events: string[] = []
    let releaseFirst!: () => void
    let firstStarted!: () => void
    const firstStartedPromise = new Promise<void>((resolve) => {
      firstStarted = resolve
    })
    const releaseFirstPromise = new Promise<void>((resolve) => {
      releaseFirst = resolve
    })
    ;(backend as any).handleTransactionControl = async (sql: string) => {
      events.push(`start:${sql}`)
      if (sql === 'SELECT first') {
        firstStarted()
        await releaseFirstPromise
      }
      events.push(`end:${sql}`)
      return true
    }

    const first = backend.query('SELECT first')
    await firstStartedPromise
    const second = backend.query('SELECT second')
    await Promise.resolve()

    expect(events).toEqual(['start:SELECT first'])
    releaseFirst()
    await Promise.all([first, second])
    expect(events).toEqual([
      'start:SELECT first',
      'end:SELECT first',
      'start:SELECT second',
      'end:SELECT second',
    ])
  })

  test('rejects transaction snapshots before writing a null manifest tx id', async () => {
    const http = await startDoHttp((sql) => {
      if (sql.includes('sqlite_master')) {
        return { rows: [{ ok: 1 }], columns: ['ok'] }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'tx-id-invariant-test')
    await backend.waitReady
    ;(backend as any).inTransaction = true
    ;(backend as any).txID = null

    await expect(
      backend.query(`INSERT INTO task_item (id, body) VALUES ('t1', 'hello')`)
    ).rejects.toThrow('internal transaction state is missing a transaction id')
    expect(
      http.bodies.some((body) =>
        body.statements?.some((statement: any) =>
          statement.sql?.includes('INSERT INTO "_orez_tx_manifest"')
        )
      )
    ).toBe(false)
  })

  test('returns command completion for parser-skipped extended statements', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'extended-noop-test')
    await backend.waitReady

    await backend.execProtocolRaw(
      parseMessage(`
        CREATE OR REPLACE FUNCTION chat.set_permissions_hash()
        RETURNS TRIGGER AS $$
        BEGIN
          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
      `)
    )

    expect(messageTypes(await backend.execProtocolRaw(describeStatement()))).toEqual([
      't',
      'n',
    ])
    await backend.execProtocolRaw(bindStatement())
    const result = await backend.execProtocolRaw(executePortal())

    expect(messageTypes(result)).toEqual(['C'])
    expect(http.sqls.some((sql) => sql.includes('CREATE OR REPLACE FUNCTION'))).toBe(
      false
    )
  })

  test('rewrites DEFAULT values in inserts by omitting those columns', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'insert-default-test')
    await backend.waitReady

    await backend.exec(`
      INSERT INTO reaction(id, value, keyword, "createdAt", "updatedAt")
        VALUES ('1', 'wave', 'wave', DEFAULT, DEFAULT)
        ON CONFLICT DO NOTHING;
    `)

    const sent = compactSQL(http.sqls.at(-1) || '')
    expect(sent).toContain('INSERT INTO reaction')
    expect(sent).toContain('id')
    expect(sent).toContain('value')
    expect(sent).toContain('keyword')
    expect(sent).toContain('ON CONFLICT DO NOTHING')
    expect(sent).not.toContain('DEFAULT')
    expect(sent).not.toContain('createdAt')
    expect(sent).not.toContain('updatedAt')
  })

  test('rewrites json_to_recordset range functions to SQLite json_each', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'json-recordset-test')
    await backend.waitReady

    await backend.query(
      `
        INSERT INTO "chat_0/cvr_queries" (
          "clientGroupID",
          "queryHash",
          "clientAST",
          "queryName",
          "queryArgs",
          "patchVersion",
          "transformationHash",
          "transformationVersion",
          "internal",
          "deleted"
        )
        SELECT
          "clientGroupID",
          "queryHash",
          "clientAST",
          "queryName",
          CASE
            WHEN "queryArgs" IS NULL THEN NULL
            ELSE "queryArgs"::json
          END,
          "patchVersion",
          "transformationHash",
          "transformationVersion",
          "internal",
          "deleted"
        FROM json_to_recordset($1) AS x(
          "clientGroupID" TEXT,
          "queryHash" TEXT,
          "clientAST" JSONB,
          "queryName" TEXT,
          "queryArgs" TEXT,
          "patchVersion" TEXT,
          "transformationHash" TEXT,
          "transformationVersion" TEXT,
          "internal" BOOLEAN,
          "deleted" BOOLEAN
        )
        ON CONFLICT ("clientGroupID", "queryHash") DO UPDATE SET
          "clientAST" = excluded."clientAST",
          "queryName" = excluded."queryName"
      `,
      [
        JSON.stringify([
          {
            clientGroupID: 'cg1',
            queryHash: 'hash1',
            clientAST: { table: 'message' },
            queryName: 'messages',
            queryArgs: null,
            patchVersion: '01',
            transformationHash: 'th',
            transformationVersion: 'tv',
            internal: false,
            deleted: false,
          },
        ]),
      ]
    )

    const sent = compactSQL(http.sqls.at(-1) || '')
    expect(sent).toContain('FROM json_each(?)')
    expect(sent).toContain(`json_extract(value, '$.clientGroupID') AS "clientGroupID"`)
    expect(sent).toContain('WHERE 1 ON CONFLICT')
    expect(sent).not.toContain('json_to_recordset')
  })

  test('infers JSON parameter oid for json_to_recordset inputs', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'json-recordset-oid-test')
    await backend.waitReady

    await backend.execProtocolRaw(
      parseMessage(
        `
          SELECT *
          FROM json_to_recordset($1) AS x(
            "clientAST" JSONB,
            "deleted" BOOLEAN
          )
        `,
        '',
        [0]
      )
    )

    const describe = await backend.execProtocolRaw(describeStatement())

    expect(parameterDescriptionOids(describe)).toEqual([114])
  })

  test('normalizes PostgreSQL array literal params used as JSON documents', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'json-param-normalize-test')
    await backend.waitReady

    await backend.query(
      `
        SELECT "clientGroupID"
        FROM json_to_recordset($1) AS x("clientGroupID" TEXT)
      `,
      ['{"{\\"clientGroupID\\":\\"cg1\\"}"}']
    )

    expect(compactSQL(http.sqls.at(-1) || '')).toContain('FROM json_each(?)')
    expect(http.params.at(-1)).toEqual(['[{"clientGroupID":"cg1"}]'])
  })

  test('rewrites Zero timestamp and row JSON helpers for SQLite', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: ['zql_result'] }))
    const backend = new DoBackend(http.url, 'postgres', 'zql-helper-rewrite-test')
    await backend.waitReady

    await backend.query(`
      SELECT row_to_json(zql_root) AS zql_result
      FROM (
        SELECT
          "userPublic_0".id AS id,
          EXTRACT(EPOCH FROM "userPublic_0"."joinedAt") * 1000 AS "joinedAt"
        FROM "userPublic" AS "userPublic_0"
      ) zql_root
    `)

    const select = compactSQL(http.sqls.at(-1) || '')
    expect(select).toContain(`"json_object"('id'`)
    expect(select).toContain(`zql_root.id`)
    expect(select).toContain(`'joinedAt'`)
    expect(select).toContain(`zql_root."joinedAt"`)
    expect(select).toContain(`strftime('%s', "userPublic_0"."joinedAt") * 1000`)
    expect(select).not.toContain('row_to_json')
    expect(select).not.toContain('EXTRACT')

    await backend.query(
      `
        INSERT INTO "userPublic" ("joinedAt")
        VALUES (to_timestamp($1::text::numeric / 1000.0) AT TIME ZONE 'UTC')
      `,
      [123000]
    )

    const insert = compactSQL(http.sqls.at(-1) || '')
    expect(insert).toContain(`datetime(? / 1000.0, 'unixepoch')`)
    expect(http.params.at(-1)).toEqual([123000])
    expect(insert).not.toContain('to_timestamp')
    expect(insert).not.toContain('AT TIME ZONE')

    await backend.query(
      `
        INSERT INTO "userPublic" ("joinedAt")
        VALUES (to_timestamp($1::text::numeric / 1000.0) AT TIME ZONE 'UTC')
      `,
      ['2026-05-25T22:07:53.949Z']
    )

    expect(compactSQL(http.sqls.at(-1) || '')).toContain(
      `datetime(? / 1000.0, 'unixepoch')`
    )
    expect(http.params.at(-1)).toEqual([1779746873949])
  })

  test('respects explicit text casts on JSON result expressions', async () => {
    const http = await startDoHttp(() => ({
      rows: [{ zql_result: [{ id: 'u1' }] }],
      columns: ['zql_result'],
    }))
    const backend = new DoBackend(http.url, 'postgres', 'zql-text-result-test')
    await backend.waitReady

    await backend.execProtocolRaw(
      parseMessage(`
        SELECT COALESCE(json_agg(row_to_json(zql_root)), '[]'::json)::text AS zql_result
        FROM (
          SELECT id
          FROM "userPublic"
        ) zql_root
      `)
    )
    await backend.execProtocolRaw(bindStatement())
    const result = await backend.execProtocolRaw(executePortal())

    expect(rowDescriptionOids(result)).toMatchObject({ zql_result: 25 })
    expect(dataRowValues(result)).toEqual([[`[{"id":"u1"}]`]])
  })

  test('high-level query() returns a string for explicit ::text casts over JSON expressions', async () => {
    // rewriteNode() strips every TypeCast from the rewritten SQL it ships to
    // SQLite. when normalizedHighLevelResult() previously read its column
    // metadata from that REWRITTEN sql, the outer `::text` cast was gone and
    // `expressionOid` saw only `json_agg(row_to_json(...))` → PG_TYPE_JSON →
    // postgresQueryJson auto-JSON.parse()d the SQLite-returned JSON text into
    // a JS object. zero's apex-side `parse$1(...)` then String()s the object
    // to `[object Object]` and every server-side `tx.run(zql.<table>.where().one())`
    // inside a custom mutator threw "Unexpected 'o', expecting JSON value".
    // metadata must be derived from the ORIGINAL pg sql so the cast is honored
    // and the column comes back as the raw text the apex caller expects.
    const http = await startDoHttp(() => ({
      rows: [{ zql_result: '[{"id":"u1"}]' }],
      columns: ['zql_result'],
    }))
    const backend = new DoBackend(http.url, 'postgres', 'zql-text-result-query-test')
    await backend.waitReady

    const result = await backend.query<{ zql_result: unknown }>(
      `SELECT COALESCE(json_agg(row_to_json(zql_root)), '[]'::json)::text AS zql_result
       FROM (SELECT id FROM "userPublic") zql_root`
    )

    expect(typeof result.rows[0]?.zql_result).toBe('string')
    expect(result.rows[0]?.zql_result).toBe('[{"id":"u1"}]')
  })

  test('flushes simple-protocol transaction writes before extended statements', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'mixed-protocol-test')
    await backend.waitReady

    await backend.execProtocolRaw(msg(0x51, cstr('BEGIN')))
    await backend.execProtocolRaw(
      msg(0x51, cstr('CREATE TABLE reaction (id TEXT PRIMARY KEY)'))
    )
    await backend.execProtocolRaw(parseMessage("INSERT INTO reaction(id) VALUES ('1')"))
    await backend.execProtocolRaw(bindStatement())
    await backend.execProtocolRaw(executePortal())

    const createIndex = http.sqls.findIndex((sql) =>
      /create\s+table\s+(if\s+not\s+exists\s+)?reaction/i.test(sql)
    )
    const insertIndex = http.sqls.findIndex((sql) =>
      /insert\s+into\s+reaction/i.test(sql)
    )
    expect(createIndex).toBeGreaterThanOrEqual(0)
    expect(insertIndex).toBeGreaterThan(createIndex)
    await backend.execProtocolRaw(msg(0x51, cstr('ROLLBACK')))
  })

  test('sends extended-protocol params as bound DO parameters', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'param-inline-test')
    await backend.waitReady

    await backend.execProtocolRaw(
      parseMessage('INSERT INTO docs(id, content) VALUES ($1, $2)')
    )
    await backend.execProtocolRaw(
      bindStatementParams(['doc_start-doc/intro', "body keeps $1 and 'quote'"])
    )
    await backend.execProtocolRaw(executePortal())

    const sent = compactSQL(http.sqls.at(-1) || '')
    expect(sent).toContain('VALUES ( ?, ? )')
    expect(http.params.at(-1)).toEqual([
      'doc_start-doc/intro',
      "body keeps $1 and 'quote'",
    ])
    expect(sent).not.toContain("body keeps 'doc_start-doc/intro'")
  })

  test('rewrites PG ALL array comparisons to SQLite json_each subqueries', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'all-array-test')
    await backend.waitReady

    await backend.execProtocolRaw(
      parseMessage(
        `DELETE FROM search_documents
         WHERE id LIKE 'doc_start-doc/%'
           AND type = 'doc'
           AND id != ALL($1)`
      )
    )
    await backend.execProtocolRaw(
      bindStatementParams(['{"doc_start-doc/intro","doc_start-doc/api"}'])
    )
    await backend.execProtocolRaw(executePortal())

    const sent = compactSQL(http.sqls.at(-1) || '')
    expect(sent).toContain('NOT (id IN (SELECT value FROM json_each')
    expect(sent).toContain('json_each(?)')
    expect(http.params.at(-1)).toEqual(['["doc_start-doc/intro","doc_start-doc/api"]'])
    expect(sent).not.toContain('ALL')
  })

  test('rewrites JSONB array element filters to SQLite json_each', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: ['value'] }))
    const backend = new DoBackend(http.url, 'postgres', 'jsonb-array-elements-test')
    await backend.waitReady

    await backend.execProtocolRaw(
      parseMessage(
        `SELECT value
         FROM jsonb_array_elements_text($1::text::jsonb)`
      )
    )
    await backend.execProtocolRaw(bindStatementParams(['{"data","chat"}']))
    await backend.execProtocolRaw(executePortal())

    const sent = compactSQL(http.sqls.at(-1) || '')
    expect(sent).toContain('FROM json_each(?)')
    expect(http.params.at(-1)).toEqual(['["data","chat"]'])
    expect(sent).not.toContain('jsonb_array_elements_text')
  })

  test('collapses json_each over ARRAY subqueries from plural filters', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: ['id'] }))
    const backend = new DoBackend(http.url, 'postgres', 'jsonb-array-filter-test')
    await backend.waitReady

    await backend.execProtocolRaw(
      parseMessage(
        `SELECT id
         FROM app
         WHERE id IN (
           SELECT value
           FROM jsonb_array_elements_text(
             ARRAY(
               SELECT value::text
               FROM jsonb_array_elements_text($1::text::jsonb)
             )
           )
         )`
      )
    )
    await backend.execProtocolRaw(bindStatementParams(['{"data","chat"}']))
    await backend.execProtocolRaw(executePortal())

    const sent = compactSQL(http.sqls.at(-1) || '')
    expect(sent).toContain('IN (SELECT value FROM json_each(?))')
    expect(sent).not.toContain('ARRAY')
    expect(http.params.at(-1)).toEqual(['["data","chat"]'])
  })

  test('rewrites PG array column declarations to SQLite text columns', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'array-column-test')
    await backend.waitReady

    await backend.exec(`
      CREATE TABLE "chat_0"."shardConfig" (
        "publications" TEXT[] NOT NULL,
        "ddlDetection" BOOL NOT NULL
      );
    `)

    const sent = compactSQL(
      sqlContaining(http.sqls, 'CREATE TABLE IF NOT EXISTS "chat_0_shardConfig"')
    )
    expect(sent).toContain('CREATE TABLE IF NOT EXISTS "chat_0_shardConfig"')
    expect(sent).toContain('publications text NOT NULL')
    expect(sent).not.toContain('text[]')
  })

  test('rewrites PG array constructors to JSON text literals', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'array-constructor-test')
    await backend.waitReady

    await backend.exec(`
      INSERT INTO "chat_0"."shardConfig" ("publications", "ddlDetection")
      VALUES (ARRAY['zero_chat', '_zero_metadata'], false);
    `)

    const sent = compactSQL(http.sqls.at(-1) || '')
    expect(sent).toContain(`'["zero_chat","_zero_metadata"]'`)
    expect(sent).not.toContain('ARRAY')
  })

  test('rewrites PG sequences to readable SQLite sequence tables', async () => {
    const http = await startDoHttp((sql) => {
      if (compactSQL(sql).startsWith('SELECT')) {
        return {
          rows: [{ last_value: 1, is_called: 0 }],
          columns: ['last_value', 'is_called'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'sequence-table-test')
    await backend.waitReady

    await backend.exec('CREATE SEQUENCE IF NOT EXISTS _orez._zero_watermark')
    const result = await backend.execProtocolRaw(
      msg(0x51, cstr('SELECT last_value, is_called FROM _orez._zero_watermark'))
    )

    const sent = compactSQL(http.sqls.join('; '))
    expect(sent).toContain('CREATE TABLE IF NOT EXISTS "_orez___zero_watermark"')
    expect(sent).toContain('INSERT OR IGNORE INTO "_orez___zero_watermark"')
    expect(rowDescriptionOids(result)).toMatchObject({
      last_value: 20,
      is_called: 16,
    })
    expect(dataRowValues(result)).toEqual([['1', 'f']])
  })

  test('sends high-level query params as bound DO parameters', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'query-param-test')
    await backend.waitReady

    await backend.query(
      `INSERT INTO _orez._zero_replication_slots (
        slot_name,
        restart_lsn,
        confirmed_flush_lsn
      ) VALUES ($1, $2, $3)`,
      ['slot_1', '0/16B6C50', '0/16B6C50']
    )

    const sent = compactSQL(http.sqls.at(-1) || '')
    expect(sent).toContain('VALUES ( ?, ?, ? )')
    expect(sent).not.toContain('$1')
    expect(http.params.at(-1)).toEqual(['slot_1', '0/16B6C50', '0/16B6C50'])
  })

  test('returns JSON and boolean type metadata for rewritten SQLite rows', async () => {
    const http = await startDoHttp((sql) => {
      if (compactSQL(sql).startsWith('SELECT')) {
        return {
          rows: [
            {
              publications: '["_chat_metadata_0","zero_chat"]',
              ddlDetection: 0,
            },
          ],
          columns: ['publications', 'ddlDetection'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'json-field-oid-test')
    await backend.waitReady

    await backend.exec(`
      CREATE TABLE "_zero"."shardConfig" (
        "publications" TEXT[] NOT NULL,
        "ddlDetection" BOOL NOT NULL
      );
    `)

    const result = await backend.execProtocolRaw(
      msg(0x51, cstr('SELECT "publications", "ddlDetection" FROM "_zero_shardConfig"'))
    )

    expect(messageTypes(result)).toEqual(['T', 'D', 'C', 'Z'])
    expect(rowDescriptionOids(result)).toMatchObject({
      publications: 114,
      ddlDetection: 16,
    })
    expect(dataRowValues(result)).toEqual([['["_chat_metadata_0","zero_chat"]', 'f']])
  })

  test('normalizes high-level query rows using PG metadata', async () => {
    const http = await startDoHttp((sql) => {
      if (compactSQL(sql).startsWith('SELECT')) {
        return {
          rows: [
            {
              hasRows: 1,
              config: '{"enabled":true,"limit":3}',
              createdAt: 1781568000000,
            },
          ],
          columns: ['hasRows', 'config', 'createdAt'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'query-row-normalize-test')
    await backend.waitReady

    await backend.exec(`
      CREATE TABLE "job" (
        "id" TEXT PRIMARY KEY,
        "config" jsonb NOT NULL,
        "createdAt" timestamptz NOT NULL
      );
    `)

    const result = await backend.query<{
      hasRows: boolean
      config: { enabled: boolean; limit: number }
      createdAt: Date
    }>(
      'SELECT EXISTS(SELECT 1 FROM "job") AS "hasRows", "config", "createdAt" FROM "job"'
    )

    expect(result.rows).toEqual([
      {
        hasRows: true,
        config: { enabled: true, limit: 3 },
        createdAt: new Date('2026-06-16T00:00:00.000Z'),
      },
    ])
  })

  test('falls back to zero-cache metadata column types when durable metadata is absent', async () => {
    const http = await startDoHttp((sql) => {
      const compact = compactSQL(sql)
      if (
        compact.includes('_orez_pg_metadata') ||
        compact.includes('sqlite_master') ||
        compact.startsWith('PRAGMA')
      ) {
        return { rows: [], columns: [] }
      }
      if (compact.startsWith('SELECT')) {
        return {
          rows: [
            {
              slot: 'slot_1',
              version: '01',
              publications: '["_chat_metadata_0","zero_chat"]',
              ddlDetection: 1,
            },
          ],
          columns: ['slot', 'version', 'publications', 'ddlDetection'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'missing-metadata-oid-test')
    await backend.waitReady

    const result = await backend.execProtocolRaw(
      msg(
        0x51,
        cstr(`
          SELECT * FROM "chat_0".replicas
          JOIN "chat_0"."shardConfig" ON true
          WHERE version = '01'
        `)
      )
    )

    expect(rowDescriptionOids(result)).toMatchObject({
      publications: 114,
      ddlDetection: 16,
    })
    expect(dataRowValues(result)).toEqual([
      ['slot_1', '01', '["_chat_metadata_0","zero_chat"]', 't'],
    ])
  })

  test('resolves JSON metadata for columns from joined tables', async () => {
    const http = await startDoHttp((sql) => {
      if (compactSQL(sql).startsWith('SELECT')) {
        return {
          rows: [
            {
              slot: 'slot_1',
              version: '01',
              publications: '["_chat_metadata_0","zero_chat"]',
              ddlDetection: 1,
            },
          ],
          columns: ['slot', 'version', 'publications', 'ddlDetection'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'join-field-oid-test')
    await backend.waitReady

    await backend.exec(`
      CREATE TABLE "chat_0".replicas (
        "slot" text PRIMARY KEY,
        "version" text NOT NULL
      );
      CREATE TABLE "chat_0"."shardConfig" (
        "publications" TEXT[] NOT NULL,
        "ddlDetection" BOOL NOT NULL
      );
      CREATE TABLE "chat_0/cdc"."replicationConfig" (
        "publications" text NOT NULL
      );
    `)

    const result = await backend.execProtocolRaw(
      msg(
        0x51,
        cstr(`
          SELECT * FROM "chat_0".replicas
          JOIN "chat_0"."shardConfig" ON true
          WHERE version = '01'
        `)
      )
    )

    expect(rowDescriptionOids(result)).toMatchObject({
      publications: 114,
      ddlDetection: 16,
    })
    expect(dataRowValues(result)).toEqual([
      ['slot_1', '01', '["_chat_metadata_0","zero_chat"]', 't'],
    ])

    await backend.execProtocolRaw(
      parseMessage(
        `
          SELECT * FROM "chat_0".replicas
          JOIN "chat_0"."shardConfig" ON true
          WHERE version = $1
        `,
        'replica-at-version',
        [25]
      )
    )
    const described = await backend.execProtocolRaw(
      describeStatement('replica-at-version')
    )
    expect(rowDescriptionOids(described)).toMatchObject({
      publications: 114,
      ddlDetection: 16,
    })
  })

  test('hydrates persisted PG column metadata for existing DO tables', async () => {
    const metadataRows: Record<string, unknown>[] = []
    const http = await startDoHttp((sql) => {
      const compact = compactSQL(sql)
      if (compact.startsWith('CREATE TABLE IF NOT EXISTS "_orez_pg_metadata"')) {
        return { rows: [], columns: [] }
      }
      if (compact.startsWith('SELECT kind, key, subkey, value FROM')) {
        return {
          rows: metadataRows,
          columns: ['kind', 'key', 'subkey', 'value'],
        }
      }
      if (compact.startsWith('DELETE FROM "_orez_pg_metadata"')) {
        metadataRows.length = 0
        return { rows: [], columns: [] }
      }
      if (compact.startsWith('INSERT OR REPLACE INTO "_orez_pg_metadata"')) {
        return { rows: [], columns: [] }
      }
      return { rows: [], columns: [] }
    })

    const first = new DoBackend(http.url, 'postgres', 'durable-metadata-test')
    await first.waitReady
    await first.exec(`
      CREATE TABLE "chat_0".replicas (
        "slot" text PRIMARY KEY,
        "version" text NOT NULL
      );
      CREATE TABLE "chat_0"."shardConfig" (
        "publications" TEXT[] NOT NULL,
        "ddlDetection" BOOL NOT NULL
      );
    `)

    for (const body of http.bodies) {
      if (
        typeof body.sql === 'string' &&
        compactSQL(body.sql).startsWith('INSERT OR REPLACE INTO "_orez_pg_metadata"')
      ) {
        appendMetadataParamRows(metadataRows, body.params)
      }
    }

    const second = new DoBackend(http.url, 'postgres', 'durable-metadata-test')
    await second.waitReady
    await second.execProtocolRaw(
      parseMessage(
        `
          SELECT * FROM "chat_0".replicas
          JOIN "chat_0"."shardConfig" ON true
          WHERE version = $1
        `,
        'replica-at-version',
        [25]
      )
    )
    const described = await second.execProtocolRaw(
      describeStatement('replica-at-version')
    )

    expect(rowDescriptionOids(described)).toMatchObject({
      publications: 114,
      ddlDetection: 16,
    })
  })

  test('persists durable metadata below DO SQLite variable limits', async () => {
    const http = await startDoHttp((sql) => {
      const compact = compactSQL(sql)
      if (compact.startsWith('CREATE TABLE IF NOT EXISTS "_orez_pg_metadata"')) {
        return { rows: [], columns: [] }
      }
      if (compact.startsWith('SELECT kind, key, subkey, value FROM')) {
        return { rows: [], columns: ['kind', 'key', 'subkey', 'value'] }
      }
      return { rows: [], columns: [] }
    })

    const backend = new DoBackend(http.url, 'postgres', 'metadata-chunk-test')
    await backend.waitReady
    await backend.exec(`
      CREATE TABLE public.big_metadata_table (
        id text PRIMARY KEY,
        c01 text,
        c02 text,
        c03 text,
        c04 text,
        c05 text,
        c06 text,
        c07 text,
        c08 text,
        c09 text,
        c10 text,
        c11 text,
        c12 text,
        c13 text,
        c14 text,
        c15 text,
        c16 text,
        c17 text,
        c18 text,
        c19 text,
        c20 text,
        c21 text,
        c22 text,
        c23 text,
        c24 text,
        c25 text
      );
    `)

    const inserts = http.bodies.filter(
      (body) =>
        typeof body.sql === 'string' &&
        compactSQL(body.sql).startsWith('INSERT OR REPLACE INTO "_orez_pg_metadata"')
    )

    expect(inserts.length).toBeGreaterThan(1)
    expect(
      Math.max(...inserts.map((body) => body.params?.length ?? 0))
    ).toBeLessThanOrEqual(80)
  })

  test('does not rewrite unchanged durable metadata on commit/rollback churn', async () => {
    // every persisted row is a real DO rows-written cost even when identical
    // (INSERT OR REPLACE always rewrites), and persistDurableMetadata fires on
    // every dirty commit AND every rollback. a crash-looping zero-cache embed
    // boot used to rewrite the full set (~700 rows on a real schema) several
    // times per cycle until the SQL DO write circuit tripped and blocked auth
    // (2026-07-09 prod incident). unchanged metadata must persist zero rows.
    const http = await startDoHttp((sql) => {
      const compact = compactSQL(sql)
      if (compact.startsWith('SELECT kind, key, subkey, value FROM')) {
        return { rows: [], columns: ['kind', 'key', 'subkey', 'value'] }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'metadata-diff-test')
    await backend.waitReady
    await backend.exec(`
      CREATE TABLE public.diff_table (
        id text PRIMARY KEY,
        body text
      );
    `)

    const metadataInserts = () =>
      http.bodies.filter(
        (body) =>
          typeof body.sql === 'string' &&
          compactSQL(body.sql).startsWith('INSERT OR REPLACE INTO "_orez_pg_metadata"')
      )
    const rowCount = (bodies: Array<{ params?: unknown[] }>) =>
      bodies.reduce((total, body) => total + (body.params?.length ?? 0) / 4, 0)
    const afterCreate = metadataInserts().length
    expect(afterCreate).toBeGreaterThan(0)

    // a read-only transaction rolled back: the rollback path re-persists
    // unconditionally; with nothing changed it must write zero rows.
    await backend.execProtocolRaw(msg(0x51, cstr('BEGIN')))
    await backend.execProtocolRaw(msg(0x51, cstr('SELECT 1')))
    await backend.execProtocolRaw(msg(0x51, cstr('ROLLBACK')))
    expect(metadataInserts().length).toBe(afterCreate)

    // DDL inside a rolled-back tx: the snapshot restore returns metadata to
    // its pre-tx state, so the rollback persist must also write zero rows.
    await backend.execProtocolRaw(msg(0x51, cstr('BEGIN')))
    await backend.execProtocolRaw(
      msg(0x51, cstr('CREATE TABLE public.rolled_back (id text PRIMARY KEY)'))
    )
    await backend.execProtocolRaw(msg(0x51, cstr('ROLLBACK')))
    expect(metadataInserts().length).toBe(afterCreate)

    // committed DDL persists only the new table's rows, never the full set.
    const before = metadataInserts()
    const beforeRows = rowCount(before)
    await backend.execProtocolRaw(msg(0x51, cstr('BEGIN')))
    await backend.execProtocolRaw(
      msg(0x51, cstr('CREATE TABLE public.diff_extra (id text PRIMARY KEY)'))
    )
    await backend.execProtocolRaw(msg(0x51, cstr('COMMIT')))
    const committedRows = rowCount(metadataInserts()) - beforeRows
    expect(committedRows).toBeGreaterThan(0)
    expect(committedRows).toBeLessThanOrEqual(3)
  })

  test('repairs internal metadata publication from shardConfig rows', async () => {
    const http = await startDoHttp((sql) => {
      const compact = compactSQL(sql)
      if (compact.startsWith('CREATE TABLE IF NOT EXISTS "_orez_pg_metadata"')) {
        return { rows: [], columns: [] }
      }
      if (compact.startsWith('SELECT kind, key, subkey, value FROM')) {
        return { rows: [], columns: ['kind', 'key', 'subkey', 'value'] }
      }
      if (compact.startsWith('DELETE FROM "_orez_pg_metadata"')) {
        return { rows: [], columns: [] }
      }
      if (compact.startsWith('INSERT OR REPLACE INTO "_orez_pg_metadata"')) {
        return { rows: [], columns: [] }
      }
      if (compact.includes('sqlite_master')) {
        return {
          rows: [
            {
              name: 'todo_permissions',
              sql: 'CREATE TABLE todo_permissions (permissions text, hash text)',
            },
            {
              name: 'todo_0_clients',
              sql: 'CREATE TABLE todo_0_clients (clientGroupID text, clientID text)',
            },
            {
              name: 'todo_0_mutations',
              sql: 'CREATE TABLE todo_0_mutations (clientGroupID text, mutation text)',
            },
            {
              name: 'todo_0_shardConfig',
              sql: 'CREATE TABLE todo_0_shardConfig (publications text)',
            },
          ],
          columns: ['name', 'sql'],
        }
      }
      if (compact === 'SELECT publications FROM "todo_0_shardConfig" LIMIT 1') {
        return {
          rows: [{ publications: '["_todo_metadata_0","zero_todo"]' }],
          columns: ['publications'],
        }
      }
      if (compact.includes('PRAGMA table_info("todo_permissions")')) {
        return {
          rows: [
            { cid: 0, name: 'permissions', type: 'text', notnull: 0, pk: 0 },
            { cid: 1, name: 'hash', type: 'text', notnull: 0, pk: 0 },
          ],
          columns: ['cid', 'name', 'type', 'notnull', 'pk'],
        }
      }
      if (compact.includes('PRAGMA table_info("todo_0_clients")')) {
        return {
          rows: [
            { cid: 0, name: 'clientGroupID', type: 'text', notnull: 1, pk: 1 },
            { cid: 1, name: 'clientID', type: 'text', notnull: 1, pk: 2 },
          ],
          columns: ['cid', 'name', 'type', 'notnull', 'pk'],
        }
      }
      if (compact.includes('PRAGMA table_info("todo_0_mutations")')) {
        return {
          rows: [
            { cid: 0, name: 'clientGroupID', type: 'text', notnull: 1, pk: 1 },
            { cid: 1, name: 'mutation', type: 'text', notnull: 1, pk: 0 },
          ],
          columns: ['cid', 'name', 'type', 'notnull', 'pk'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'metadata-publication-test')
    await backend.waitReady

    expect(
      http.bodies.some(
        (body) =>
          body.params?.[0] === 'publication' && body.params?.[1] === '_todo_metadata_0'
      )
    ).toBe(true)

    const publications = await (backend as any).handleCatalogQuery(`
      SELECT pubname FROM pg_publication
      WHERE pubname IN ('_todo_metadata_0', 'zero_todo')
    `)
    expect(publications.rows).toEqual([{ pubname: '_todo_metadata_0' }])

    const publicationTables = await (backend as any).handleCatalogQuery(`
      SELECT pubname, schemaname, tablename
      FROM pg_publication_tables
      WHERE pubname IN ('_todo_metadata_0')
    `)
    expect(publicationTables.rows).toEqual(
      expect.arrayContaining([
        {
          pubname: '_todo_metadata_0',
          schemaname: 'todo',
          tablename: 'permissions',
        },
        {
          pubname: '_todo_metadata_0',
          schemaname: 'todo_0',
          tablename: 'clients',
        },
        {
          pubname: '_todo_metadata_0',
          schemaname: 'todo_0',
          tablename: 'mutations',
        },
      ])
    )
    expect(publicationTables.rows).toHaveLength(3)
  })

  test('repairing internal metadata publication preserves app publications', async () => {
    const metadataRows: Record<string, unknown>[] = [
      {
        kind: 'publication',
        key: 'zero_todo',
        subkey: '',
        value: JSON.stringify({
          name: 'zero_todo',
          allTables: false,
          schemas: [],
          tables: [['todo', { table: 'todo', schema: 'public', tableName: 'todo' }]],
        }),
      },
    ]
    let http: Awaited<ReturnType<typeof startDoHttp>>
    http = await startDoHttp((sql) => {
      const compact = compactSQL(sql)
      if (compact.startsWith('CREATE TABLE IF NOT EXISTS "_orez_pg_metadata"')) {
        return { rows: [], columns: [] }
      }
      if (compact.startsWith('SELECT kind, key, subkey, value FROM')) {
        return {
          rows: metadataRows,
          columns: ['kind', 'key', 'subkey', 'value'],
        }
      }
      if (compact.startsWith('INSERT OR REPLACE INTO "_orez_pg_metadata"')) {
        const rows: Record<string, unknown>[] = []
        appendMetadataParamRows(rows, http.bodies.at(-1)?.params ?? [])
        for (const row of rows) {
          const existing = metadataRows.findIndex(
            (existingRow) =>
              existingRow.kind === row.kind &&
              existingRow.key === row.key &&
              existingRow.subkey === row.subkey
          )
          if (existing >= 0) metadataRows[existing] = row
          else metadataRows.push(row)
        }
        return { rows: [], columns: [] }
      }
      if (compact.includes('sqlite_master')) {
        return {
          rows: [
            {
              name: 'todo_0_shardConfig',
              sql: 'CREATE TABLE todo_0_shardConfig (publications text)',
            },
          ],
          columns: ['name', 'sql'],
        }
      }
      if (compact === 'SELECT publications FROM "todo_0_shardConfig" LIMIT 1') {
        return {
          rows: [{ publications: '["_todo_metadata_0","zero_todo"]' }],
          columns: ['publications'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'metadata-merge-test')
    await backend.waitReady

    const publications = await (backend as any).handleCatalogQuery(`
      SELECT pubname FROM pg_publication
      WHERE pubname IN ('_todo_metadata_0', 'zero_todo')
      ORDER BY pubname
    `)
    expect(publications.rows).toEqual([
      { pubname: '_todo_metadata_0' },
      { pubname: 'zero_todo' },
    ])
    expect(metadataRows.map((row) => row.key).sort()).toEqual([
      '_todo_metadata_0',
      'zero_todo',
    ])
  })

  test('infers JSON parameter oids from parsed insert target columns', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'json-param-oid-test')
    await backend.waitReady

    await backend.exec(`
      CREATE TABLE "chat_0".replicas (
        "slot" text PRIMARY KEY,
        "version" text NOT NULL,
        "initialSchema" JSON NOT NULL,
        "initialSyncContext" JSON
      )
    `)

    await backend.execProtocolRaw(
      parseMessage(
        `INSERT INTO "chat_0".replicas
          ("slot", "version", "initialSchema", "initialSyncContext")
         VALUES ($1, $2, $3, $4)`,
        '',
        [0, 0, 0, 0]
      )
    )
    const describe = await backend.execProtocolRaw(describeStatement())

    expect(parameterDescriptionOids(describe)).toEqual([25, 25, 114, 114])

    await backend.execProtocolRaw(
      bindStatementParams(
        [
          'slot_1',
          '0/16B6C50',
          '{"tables":[{"name":"message"}],"indexes":[]}',
          '{"requestID":"req_1"}',
        ],
        '',
        'replica_insert'
      )
    )
    await backend.execProtocolRaw(executePortal('replica_insert'))

    expect(compactSQL(http.sqls.at(-1) || '')).toContain('VALUES ( ?, ?, ?, ? )')
    expect(http.params.at(-1)).toEqual([
      'slot_1',
      '0/16B6C50',
      '{"tables":[{"name":"message"}],"indexes":[]}',
      '{"requestID":"req_1"}',
    ])
  })

  test('infers zero-cache JSON parameter oids without durable metadata', async () => {
    const http = await startDoHttp((sql) => {
      const compact = compactSQL(sql)
      if (
        compact.includes('_orez_pg_metadata') ||
        compact.includes('sqlite_master') ||
        compact.startsWith('PRAGMA')
      ) {
        return { rows: [], columns: [] }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'json-param-fallback-test')
    await backend.waitReady

    await backend.execProtocolRaw(
      parseMessage(
        `INSERT INTO "chat_0".replicas
          ("slot", "version", "initialSchema", "initialSyncContext")
         VALUES ($1, $2, $3, $4)`,
        'replica-insert-no-metadata',
        [0, 0, 0, 0]
      )
    )
    let describe = await backend.execProtocolRaw(
      describeStatement('replica-insert-no-metadata')
    )
    expect(parameterDescriptionOids(describe)).toEqual([0, 0, 114, 114])

    await backend.execProtocolRaw(
      parseMessage(
        `UPDATE "chat_0".replicas
          SET "subscriberContext" = $1
          WHERE slot = $2`,
        'replica-update-no-metadata',
        [0, 0]
      )
    )
    describe = await backend.execProtocolRaw(
      describeStatement('replica-update-no-metadata')
    )
    expect(parameterDescriptionOids(describe)).toEqual([114, 0])
  })

  test('infers fallback insert params for ON CONFLICT without durable metadata', async () => {
    const http = await startDoHttp((sql) => {
      const compact = compactSQL(sql)
      if (
        compact.includes('_orez_pg_metadata') ||
        compact.includes('sqlite_master') ||
        compact.startsWith('PRAGMA')
      ) {
        return { rows: [], columns: [] }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'json-upsert-no-metadata')
    await backend.waitReady

    await backend.execProtocolRaw(
      parseMessage(
        `INSERT INTO "chat_0".replicas
          ("slot", "version", "initialSchema", "initialSyncContext")
         VALUES ($1, $2, $3, $4)
         ON CONFLICT ("slot") DO UPDATE SET "initialSchema" = $5`,
        'replica-upsert-no-metadata',
        [0, 0, 0, 0, 0]
      )
    )
    const describe = await backend.execProtocolRaw(
      describeStatement('replica-upsert-no-metadata')
    )

    expect(parameterDescriptionOids(describe)).toEqual([0, 0, 114, 114, 114])
  })

  test('splits Drizzle statement-breakpoint batches and drops PG constraint alters', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'statement-batch-test')
    await backend.waitReady

    await backend.exec(`
      CREATE TABLE "parent" ("id" text PRIMARY KEY);
      --> statement-breakpoint
      ALTER TABLE "child" ADD CONSTRAINT "child_parent_fk" FOREIGN KEY ("parentId") REFERENCES "public"."parent"("id");
      --> statement-breakpoint
      ALTER TABLE "child" ADD PRIMARY KEY("id");
      --> statement-breakpoint
      CREATE TABLE "child" ("id" text PRIMARY KEY, "parentId" text);
    `)

    const sent = http.sqls.join('; ')
    expect(sent).toContain('CREATE TABLE IF NOT EXISTS parent')
    expect(sent).toContain('CREATE TABLE IF NOT EXISTS child')
    expect(sent).not.toContain('ADD CONSTRAINT')
    expect(sent).not.toContain('statement-breakpoint')
  })

  test('splits semicolon batches inside Drizzle breakpoint chunks', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'semicolon-batch-test')
    await backend.waitReady

    await backend.exec(`
      ALTER TABLE "serverApp" DROP COLUMN "id";
      ALTER TABLE "serverApp" ADD CONSTRAINT "serverApp_pk" PRIMARY KEY("serverId","creatorId");
      --> statement-breakpoint
      INSERT INTO "log" ("message") VALUES ('keeps ; inside strings');
    `)

    const sent = http.sqls.at(-1) || ''
    const compact = compactSQL(sent)
    expect(compact).toContain('ALTER TABLE "serverApp" DROP COLUMN id')
    expect(compact).toContain(
      `INSERT INTO log ( message ) VALUES ( 'keeps ; inside strings' )`
    )
    expect(sent).not.toContain('ADD CONSTRAINT')
    expect(sent).not.toContain('PRIMARY KEY("serverId"')
  })

  test('rewrites btree indexes and drops unsupported PG index methods', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'index-rewrite-test')
    await backend.waitReady

    await backend.exec(`
      CREATE INDEX "idx_message_channel" ON "message" USING btree ("channelId","order");
      --> statement-breakpoint
      CREATE INDEX "idx_message_search" ON "message" USING gin ("content" gin_trgm_ops);
    `)

    const sent = http.sqls.at(-1) || ''
    expect(compactSQL(sent)).toContain(
      'CREATE INDEX IF NOT EXISTS idx_message_channel ON message ("channelId", "order")'
    )
    expect(sent).not.toContain('USING')
    expect(sent).not.toContain('idx_message_search')
  })

  test('drops PG null ordering from index elements', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'index-nulls-order-test')
    await backend.waitReady

    await backend.exec(`
      CREATE INDEX queries_patch_version
        ON "chat_0/cvr".queries ("patchVersion" NULLS FIRST);
    `)

    const sent = compactSQL(http.sqls.at(-1) || '')
    expect(sent).toContain(
      'CREATE INDEX IF NOT EXISTS queries_patch_version ON "chat_0/cvr_queries" ("patchVersion")'
    )
    expect(sent).not.toContain('NULLS FIRST')
    expect(sent).not.toContain('"chat_0/cvr".')
  })

  test('materializes ALTER TABLE ADD UNIQUE CONSTRAINT as a SQLite index', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'alter-unique-constraint-test')
    await backend.waitReady

    await backend.exec(
      'ALTER TABLE "userState" ADD CONSTRAINT "userState_userId_unique" UNIQUE("userId");'
    )

    const sent = compactSQL(
      sqlContaining(
        http.sqls,
        'CREATE UNIQUE INDEX IF NOT EXISTS "userState_userId_unique"'
      )
    )
    expect(sent).toContain(
      'CREATE UNIQUE INDEX IF NOT EXISTS "userState_userId_unique" ON "userState" ("userId")'
    )
    expect(sent).not.toContain('ALTER TABLE')
  })

  test('materializes ALTER TABLE DROP CONSTRAINT as a SQLite index drop', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'drop-constraint-test')
    await backend.waitReady

    await backend.exec('ALTER TABLE "serverApp" DROP CONSTRAINT "serverApp_appId_pk";')

    const sent = compactSQL(
      sqlContaining(http.sqls, 'DROP INDEX IF EXISTS "serverApp_appId_pk"')
    )
    expect(sent).toBe('DROP INDEX IF EXISTS "serverApp_appId_pk"')
    expect(sent).not.toContain('ALTER TABLE')
  })

  test('normalizes unsupported ALTER TABLE ADD COLUMN constraints', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'alter-add-column-test')
    await backend.waitReady

    await backend.exec(
      'ALTER TABLE "appInstall" ADD COLUMN IF NOT EXISTS "id" varchar PRIMARY KEY NOT NULL DEFAULT md5(random()::text);'
    )

    const sent = compactSQL(
      sqlContaining(http.sqls, 'ALTER TABLE "appInstall" ADD COLUMN id varchar')
    )
    expect(sent).toContain('ALTER TABLE "appInstall" ADD COLUMN id varchar')
    expect(sent).not.toContain('IF NOT EXISTS')
    expect(sent).not.toContain('PRIMARY KEY')
    expect(sent).not.toContain('NOT NULL')
    expect(sent).not.toContain('md5')
  })

  test('tracks parser metadata through table and column renames', async () => {
    const http = await startDoHttp((sql) => {
      const compact = compactSQL(sql)
      if (compact.includes("sqlite_master WHERE type = 'table'")) {
        return {
          rows: [{ name: 'app', sql: 'CREATE TABLE "app" ("madeAt" text, meta text)' }],
          columns: ['name', 'sql'],
        }
      }
      if (compact.includes('PRAGMA table_info("app")')) {
        return {
          rows: [
            {
              cid: 0,
              name: 'madeAt',
              type: 'text',
              notnull: 0,
              dflt_value: null,
              pk: 0,
            },
            {
              cid: 1,
              name: 'meta',
              type: 'text',
              notnull: 0,
              dflt_value: null,
              pk: 0,
            },
          ],
          columns: ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'rename-metadata-test')
    await backend.waitReady

    await backend.exec(`
      CREATE TABLE "plugin" (
        "createdAt" timestamp,
        "meta" jsonb
      );
    `)
    await backend.exec('ALTER TABLE "plugin" RENAME TO "app";')
    await backend.exec('ALTER TABLE "app" RENAME COLUMN "createdAt" TO "madeAt";')

    const result = await (backend as any).handleCatalogQuery(`
      SELECT c.column_name::text AS column,
             c.data_type::text AS "dataType",
             t.typname::text AS typename
      FROM information_schema.columns c
      JOIN pg_catalog.pg_type t ON c.udt_name = t.typname
      LEFT JOIN pg_catalog.pg_type et ON t.typelem = et.oid
      JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
      WHERE (c.table_schema, c.table_name) IN (('public'::text, 'app'::text))
    `)

    expect(result.rows).toEqual([
      {
        column: 'madeAt',
        dataType: 'timestamp without time zone',
        typename: 'timestamp',
      },
      { column: 'meta', dataType: 'jsonb', typename: 'jsonb' },
    ])
  })

  test('tracks ALTER COLUMN TYPE as catalog metadata without SQLite DDL', async () => {
    const http = await startDoHttp((sql) => {
      const compact = compactSQL(sql)
      if (compact.includes("sqlite_master WHERE type = 'table'")) {
        return {
          rows: [{ name: 'data', sql: 'CREATE TABLE data (value text)' }],
          columns: ['name', 'sql'],
        }
      }
      if (compact.includes('PRAGMA table_info("data")')) {
        return {
          rows: [
            {
              cid: 0,
              name: 'value',
              type: 'text',
              notnull: 0,
              dflt_value: null,
              pk: 0,
            },
          ],
          columns: ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'alter-type-metadata-test')
    await backend.waitReady

    await backend.exec('CREATE TABLE data (value text);')
    await backend.exec(
      'ALTER TABLE data ALTER COLUMN value SET DATA TYPE jsonb USING value::jsonb;'
    )

    const result = await (backend as any).handleCatalogQuery(`
      SELECT c.column_name::text AS column,
             c.data_type::text AS "dataType",
             t.typname::text AS typename
      FROM information_schema.columns c
      JOIN pg_catalog.pg_type t ON c.udt_name = t.typname
      LEFT JOIN pg_catalog.pg_type et ON t.typelem = et.oid
      JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
      WHERE (c.table_schema, c.table_name) IN (('public'::text, 'data'::text))
    `)

    expect(http.sqls.some((sql) => compactSQL(sql).includes('ALTER COLUMN'))).toBe(false)
    expect(result.rows).toEqual([
      { column: 'value', dataType: 'jsonb', typename: 'jsonb' },
    ])
  })

  test('normalizes pgvector and generated tsvector columns in create table', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'search-table-test')
    await backend.waitReady

    await backend.exec(`
      CREATE TABLE IF NOT EXISTS search_documents (
        id text PRIMARY KEY,
        title text,
        content text,
        search_vector tsvector GENERATED ALWAYS AS (
          setweight(to_tsvector('english', coalesce(title, '')), 'A')
        ) STORED,
        embedding vector(384)
      );
    `)

    const sent = compactSQL(
      sqlContaining(http.sqls, 'CREATE TABLE IF NOT EXISTS search_documents')
    )
    expect(sent).toContain('search_vector text')
    expect(sent).toContain('embedding text')
    expect(sent).not.toContain('GENERATED')
    expect(sent).not.toContain('to_tsvector')
    expect(sent).not.toContain('vector(384)')
  })

  test('makes create-table statements idempotent for repeated migrations', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'create-table-idempotent-test')
    await backend.waitReady

    await backend.exec(`
      CREATE TABLE "privateChatsStats" (
        "id" text PRIMARY KEY,
        "createdAt" timestamptz DEFAULT now()
      );
    `)

    const sent = compactSQL(
      sqlContaining(http.sqls, 'CREATE TABLE IF NOT EXISTS "privateChatsStats"')
    )
    expect(sent).toContain('CREATE TABLE IF NOT EXISTS "privateChatsStats"')
    expect(sent).toContain('"createdAt" text DEFAULT CURRENT_TIMESTAMP')
  })

  test('drops foreign-key constraints while flattening schema-qualified create table DDL', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'cvr-foreign-key-test')
    await backend.waitReady

    await backend.exec(`
      CREATE TABLE "chat_0/cvr".rows (
        "clientGroupID" TEXT,
        "rowKey" JSONB,
        PRIMARY KEY ("clientGroupID", "rowKey"),
        CONSTRAINT fk_rows_client_group
          FOREIGN KEY("clientGroupID")
          REFERENCES "chat_0/cvr"."rowsVersion" ("clientGroupID")
          ON DELETE CASCADE
      );
    `)

    const sent = compactSQL(
      sqlContaining(http.sqls, 'CREATE TABLE IF NOT EXISTS "chat_0/cvr_rows"')
    )
    expect(sent).toContain('CREATE TABLE IF NOT EXISTS "chat_0/cvr_rows"')
    expect(sent).toContain('"rowKey" text')
    expect(sent).not.toContain('FOREIGN KEY')
    expect(sent).not.toContain('REFERENCES')
    expect(sent).not.toContain('"chat_0/cvr".')
  })

  test('rewrites temporary create-table-as statements to persistent SQLite tables', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'temp-table-as-test')
    await backend.waitReady

    await backend.exec(`
      CREATE TEMP TABLE app_id_mapping AS
      SELECT id AS old_id, uid AS new_id
      FROM public.app;
    `)

    const sent = compactSQL(http.sqls.at(-1) || '')
    expect(sent).toContain('CREATE TABLE app_id_mapping AS SELECT')
    expect(sent).toContain('FROM app')
    expect(sent).not.toContain('TEMP')
  })

  test('normalizes multiline ALTER TABLE ADD COLUMN modifiers', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'multiline-add-column-test')
    await backend.waitReady

    await backend.exec(`
      ALTER TABLE "userPublic"
        ADD COLUMN IF NOT EXISTS "hasOnboarded" BOOLEAN NOT NULL DEFAULT false;
    `)

    const sent = compactSQL(
      sqlContaining(
        http.sqls,
        'ALTER TABLE "userPublic" ADD COLUMN "hasOnboarded" integer DEFAULT 0'
      )
    )
    expect(sent).toContain(
      'ALTER TABLE "userPublic" ADD COLUMN "hasOnboarded" integer DEFAULT 0'
    )
    expect(sent).not.toContain('IF NOT EXISTS')
    expect(sent).not.toContain('NOT NULL')
  })

  test('splits multi-command ALTER TABLE statements for SQLite', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'multi-add-column-test')
    await backend.waitReady

    await backend.exec(`
      ALTER TABLE search_documents
        ADD COLUMN server_id text,
        ADD COLUMN channel_id text;
    `)

    const sent = http.sqls.join('; ')
    expect(compactSQL(sent)).toContain(
      'ALTER TABLE search_documents ADD COLUMN server_id text'
    )
    expect(compactSQL(sent)).toContain(
      'ALTER TABLE search_documents ADD COLUMN channel_id text'
    )
    expect(compactSQL(sent)).not.toContain('server_id text, ADD COLUMN')
  })

  test('skips ADD COLUMN IF NOT EXISTS when parser metadata finds the column', async () => {
    const http = await startDoHttp((sql) => {
      if (sql.toLowerCase().includes('pragma table_info')) {
        return { rows: [{ name: 'hasOnboarded' }], columns: ['name'] }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'conditional-add-column-test')
    await backend.waitReady

    await backend.execProtocolRaw(msg(0x51, cstr('BEGIN')))
    await backend.execProtocolRaw(
      msg(
        0x51,
        cstr(`
          ALTER TABLE "userPublic"
            ADD COLUMN IF NOT EXISTS "hasOnboarded" BOOLEAN NOT NULL DEFAULT false;
        `)
      )
    )
    await backend.execProtocolRaw(msg(0x51, cstr('COMMIT')))

    expect(http.sqls.some((sql) => compactSQL(sql).includes('PRAGMA table_info'))).toBe(
      true
    )
    expect(http.sqls.some((sql) => compactSQL(sql).includes('ADD COLUMN'))).toBe(false)
  })

  test('skips plain ADD COLUMN when parser metadata finds the column', async () => {
    const http = await startDoHttp((sql) => {
      if (sql.toLowerCase().includes('pragma table_info')) {
        return { rows: [{ name: 'latestMessageOrder' }], columns: ['name'] }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'plain-add-column-test')
    await backend.waitReady

    await backend.execProtocolRaw(msg(0x51, cstr('BEGIN')))
    await backend.execProtocolRaw(
      msg(
        0x51,
        cstr(`
          ALTER TABLE channel
            ADD COLUMN "latestMessageOrder" varchar;
        `)
      )
    )
    await backend.execProtocolRaw(msg(0x51, cstr('COMMIT')))

    expect(http.sqls.some((sql) => compactSQL(sql).includes('PRAGMA table_info'))).toBe(
      true
    )
    expect(http.sqls.some((sql) => compactSQL(sql).includes('ADD COLUMN'))).toBe(false)
  })

  test('skips DROP COLUMN IF EXISTS when parser metadata cannot find the column', async () => {
    const http = await startDoHttp((sql) => {
      if (sql.toLowerCase().includes('pragma table_info')) {
        return { rows: [{ name: 'id' }], columns: ['name'] }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'conditional-drop-column-test')
    await backend.waitReady

    await backend.execProtocolRaw(msg(0x51, cstr('BEGIN')))
    await backend.execProtocolRaw(
      msg(
        0x51,
        cstr(`
          ALTER TABLE search_documents
            DROP COLUMN IF EXISTS message_order;
        `)
      )
    )
    await backend.execProtocolRaw(msg(0x51, cstr('COMMIT')))

    expect(http.sqls.some((sql) => compactSQL(sql).includes('PRAGMA table_info'))).toBe(
      true
    )
    expect(http.sqls.some((sql) => compactSQL(sql).includes('DROP COLUMN'))).toBe(false)
  })

  test('keeps table-qualified column refs while flattening schemas', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'update-from-test')
    await backend.waitReady

    await backend.exec(`
      UPDATE thread
      SET "serverId" = channel."serverId"
      FROM channel
      WHERE thread."channelId" = channel.id
        AND public.thread."deleted" = false;
    `)

    const sent = compactSQL(http.sqls.at(-1) || '')
    expect(sent).toContain('channel."serverId"')
    expect(sent).toContain('thread."channelId" = channel.id')
    expect(sent).toContain('thread.deleted = 0')
    expect(sent).not.toContain('channel_id')
    expect(sent).not.toContain('thread_channelId')
  })

  test('rewrites PG least and greatest scalar functions to SQLite min and max', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'least-greatest-test')
    await backend.waitReady

    await backend.exec(`
      UPDATE "thread"
      SET "replyCount" = LEAST((SELECT COUNT(*)::INTEGER FROM "message"), 11),
          "order" = GREATEST("order", 0);
    `)

    const sent = compactSQL(http.sqls.at(-1) || '')
    expect(sent).toContain('"replyCount" = min')
    expect(sent).toContain('"order" = max')
    expect(sent).not.toContain('LEAST')
    expect(sent).not.toContain('GREATEST')
  })

  test('rewrites PG starts_with scalar function to SQLite instr predicate', async () => {
    const http = await startDoHttp((sql) => {
      if (sql.includes('instr(')) return { rows: [{ users: 3 }], columns: ['users'] }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'starts-with-test')
    await backend.waitReady

    const result = await backend.query(
      `SELECT count(*) FILTER (WHERE starts_with("profileID", 'p')) AS users
       FROM "chat_0/cvr_instances"`
    )

    expect(result.rows).toEqual([{ users: 3 }])
    const sent = http.sqls.at(-1) ?? ''
    expect(sent).not.toContain('starts_with')
    expect(sent).toContain('instr("profileID", \'p\') = 1')
  })

  test('rewrites DISTINCT ON selects with a window function for SQLite', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'distinct-on-test')
    await backend.waitReady

    await backend.exec(`
      UPDATE "agentConfig" ac
      SET "systemPrompt" = sub."chatPrompt"
      FROM (
        SELECT DISTINCT ON (c."serverId")
          c."serverId",
          c."chatPrompt"
        FROM channel c
        WHERE c."chatPrompt" IS NOT NULL
          AND c."chatPrompt" != ''
        ORDER BY c."serverId", c."updatedAt" DESC
      ) sub
      WHERE ac."serverId" = sub."serverId"
        AND ac.type = 'builtin';
    `)

    const sent = compactSQL(http.sqls.at(-1) || '')
    expect(sent).toContain('row_number() OVER')
    expect(sent).toContain('PARTITION BY c."serverId"')
    expect(sent).toContain('ORDER BY c."serverId", c."updatedAt" DESC')
    expect(sent).toContain('_orez_rn = 1')
    expect(sent).not.toContain('DISTINCT ON')
  })

  test('executes a DISTINCT ON whose ORDER BY references a CASE select-list alias against real sqlite', async () => {
    // runtime proof: the rewritten window ORDER BY must NOT reference the
    // select-list alias `match_rank` — sqlite cannot resolve a select-list
    // alias inside that same select's window function and throws
    // `no such column: match_rank`. mirrors app/api/site/landing/recent+api.ts.
    const db = new Database(':memory:')
    db.exec(`
      CREATE TABLE "previewShare" (
        id TEXT, kind TEXT, "createdAt" TEXT, "repoId" TEXT, visibility TEXT
      );
      CREATE TABLE "projectGithubLink" ( owner TEXT, repo TEXT );
      INSERT INTO "previewShare" VALUES
        ('p1','flow','2026-01-01','acme/widget','unlisted'),
        ('p1','flow','2026-02-01','acme/other','unlisted'),
        ('p2','flow','2026-02-01','x/y','unlisted'),
        ('p2','flow','2026-03-01','x/z','unlisted');
      INSERT INTO "projectGithubLink" VALUES ('acme','widget');
    `)

    const http = await startDoHttp((sql, _url) => {
      // execute only the DISTINCT-ON-derived select against real sqlite; the
      // bootstrap/metadata chatter the backend emits stays a no-op stub.
      if (!sql.includes('row_number') && !sql.includes('_orez_rn')) {
        return { rows: [], columns: [] }
      }
      const rows = db.prepare(sql.replace(/\$\d+/g, '?')).all('acme') as Record<
        string,
        unknown
      >[]
      return { rows, columns: rows.length ? Object.keys(rows[0]) : [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'distinct-on-alias-runtime-test')
    await backend.waitReady

    const result = await backend.query(
      `SELECT DISTINCT ON (p.id)
              p.id, p."createdAt",
              CASE
                WHEN LOWER(p."repoId") = LOWER(l.owner || '/' || l.repo) THEN 0
                ELSE 1
              END AS match_rank
       FROM "previewShare" p
       JOIN "projectGithubLink" l ON LOWER(l.owner) = LOWER($1)
       WHERE p.visibility = 'unlisted' AND p.kind = 'flow'
       ORDER BY p.id, match_rank, p."createdAt" DESC`,
      ['acme']
    )

    // one row per id: p1 picks the repo-match (rank 0), p2 has no match so both
    // rows are rank 1 and the createdAt DESC tiebreak selects the newest.
    expect(result.rows).toEqual([
      { id: 'p1', createdAt: '2026-01-01', match_rank: 0 },
      { id: 'p2', createdAt: '2026-03-01', match_rank: 1 },
    ])

    const sent = compactSQL(http.sqls.find((s) => s.includes('row_number')) ?? '')
    // the window ORDER BY carries the inlined CASE, not the unresolvable alias.
    expect(sent).toContain('row_number() OVER')
    expect(sent).toMatch(/ORDER BY p\.id, CASE/)
    expect(sent).not.toMatch(/ORDER BY p\.id, match_rank/)
    db.close()
  })

  test('strips PostgreSQL row-locking clauses from SELECTs for SQLite', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: ['clientGroupID'] }))
    const backend = new DoBackend(http.url, 'postgres', 'select-locking-clause-test')
    await backend.waitReady

    await backend.exec(`
      SELECT "clientGroupID"
      FROM "chat_0/cvr_instances"
      WHERE NOT "deleted"
      ORDER BY "lastActive" ASC
      LIMIT 10
      FOR UPDATE SKIP LOCKED
    `)

    const sent = compactSQL(http.sqls.at(-1) || '')
    expect(sent).toContain('ORDER BY "lastActive" ASC')
    expect(sent).toContain('LIMIT 10')
    expect(sent).not.toContain('FOR UPDATE')
    expect(sent).not.toContain('SKIP LOCKED')
  })

  test('rewrites LIKE with PostgreSQL escape semantics to SQLite like()', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: ['slot'] }))
    const backend = new DoBackend(http.url, 'postgres', 'like-escape-test')
    await backend.waitReady

    await backend.query(
      `
      SELECT slot_name AS slot
      FROM _orez._zero_replication_slots
      WHERE slot_name LIKE $1
    `,
      ['chat\\_0\\_%']
    )

    const sent = compactSQL(http.sqls.at(-1) || '')
    expect(sent).toContain('"like"(?, slot_name, "char"(92))')
    expect(sent).not.toContain('slot_name LIKE')
    expect(http.params.at(-1)).toEqual(['chat\\_0\\_%'])
  })

  test('rewrites PG JSONB helper functions to SQLite JSON1 equivalents', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'json-function-test')
    await backend.waitReady

    await backend.exec(`
      UPDATE task
      SET "numSteps" = CASE
        WHEN steps IS NULL THEN 0
        ELSE jsonb_array_length(steps)
      END;
    `)

    const sent = compactSQL(http.sqls.at(-1) || '')
    expect(sent).toContain('json_array_length(steps)')
    expect(sent).not.toContain('jsonb_array_length')
  })

  test('rewrites PG JSONB any-key operator to SQLite JSON1 joins', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'jsonb-any-key-test')
    await backend.waitReady

    await backend.query(
      `
      SELECT *
      FROM "chat_0/cvr_rows"
      WHERE
        "clientGroupID" = $1
        AND "patchVersion" > $2
        AND "patchVersion" <= $3
        AND ("refCounts" IS NULL OR NOT ("refCounts" ?| $4))
      `,
      ['cg1', '00:01', '00:02', ['q1', 'q2']]
    )

    const sent = compactSQL(http.sqls.at(-1) || '')
    expect(sent).toContain('json_each("refCounts")')
    expect(sent).toContain('json_each(?)')
    expect(sent).toContain('obj."key" = keys.value')
    expect(sent).not.toContain('?|')
    expect(http.params.at(-1)).toEqual(['cg1', '00:01', '00:02', '["q1","q2"]'])
  })

  test('rewrites JSON object builders and marks their result columns as JSON', async () => {
    const http = await startDoHttp((sql) => {
      if (compactSQL(sql).startsWith('SELECT')) {
        return {
          rows: [
            {
              table: '{"schema":"public","name":"message","metadata":{"rowKey":["id"]}}',
              columns: '{"payload":{"source":"backfill"}}',
            },
          ],
          columns: ['table', 'columns'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'json-object-builder-test')
    await backend.waitReady

    const result = await backend.execProtocolRaw(
      msg(
        0x51,
        cstr(`
          SELECT
            json_build_object(
              'schema', b."schema",
              'name', b."table",
              'metadata', t."metadata"
            ) AS "table",
            json_object_agg(b."column", b."backfill") AS "columns"
          FROM "chat_0/change-streamer_0"."backfilling" AS b
          LEFT JOIN "chat_0/change-streamer_0"."tableMetadata" AS t
            ON (b."schema" = t."schema" AND b."table" = t."table")
          GROUP BY b."schema", b."table", t."metadata"
        `)
      )
    )

    const sent = compactSQL(http.sqls.at(-1) || '')
    expect(sent).toContain('json_group_object')
    expect(sent).toContain('json_valid')
    expect(sent).not.toContain('json_build_object')
    expect(sent).not.toContain('json_object_agg')
    expect(rowDescriptionOids(result)).toMatchObject({
      table: 114,
      columns: 114,
    })
    expect(dataRowValues(result)).toEqual([
      [
        '{"schema":"public","name":"message","metadata":{"rowKey":["id"]}}',
        '{"payload":{"source":"backfill"}}',
      ],
    ])
  })

  test('skips unsupported regexp_replace updates when the target table is empty', async () => {
    const http = await startDoHttp((sql) => {
      if (compactSQL(sql).includes('SELECT 1 AS ok FROM "message" LIMIT 1')) {
        return { rows: [], columns: ['ok'] }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'regexp-empty-update-test')
    await backend.waitReady

    await backend.execProtocolRaw(msg(0x51, cstr('BEGIN')))
    await backend.execProtocolRaw(
      msg(
        0x51,
        cstr(`
          UPDATE message
          SET content = regexp_replace(content, '<@{([^:}]+):([^:}]+):([^}]+)}>', E'<@{\\\\1%\\\\2%\\\\3}>', 'g')
          WHERE content LIKE '%<@{%:%:%}%';
        `)
      )
    )
    await backend.execProtocolRaw(msg(0x51, cstr('COMMIT')))

    expect(
      http.sqls.some((sql) =>
        compactSQL(sql).includes('SELECT 1 AS ok FROM "message" LIMIT 1')
      )
    ).toBe(true)
    expect(http.sqls.some((sql) => sql.includes('regexp_replace'))).toBe(false)
  })

  test('skips CTE backfill inserts when the source table is empty', async () => {
    const http = await startDoHttp((sql) => {
      if (compactSQL(sql).includes('SELECT 1 AS ok FROM "messageReaction" LIMIT 1')) {
        return { rows: [], columns: ['ok'] }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'empty-cte-insert-test')
    await backend.waitReady

    await backend.execProtocolRaw(msg(0x51, cstr('BEGIN')))
    await backend.execProtocolRaw(
      msg(
        0x51,
        cstr(`
          INSERT INTO "messageReactionStats"
          WITH ranked_reactions AS (
            SELECT mr."messageId", mr."reactionId"
            FROM "messageReaction" mr
          )
          SELECT "messageId", "reactionId" FROM ranked_reactions;
        `)
      )
    )
    await backend.execProtocolRaw(msg(0x51, cstr('COMMIT')))

    expect(
      http.sqls.some((sql) =>
        compactSQL(sql).includes('SELECT 1 AS ok FROM "messageReaction" LIMIT 1')
      )
    ).toBe(true)
    expect(http.sqls.some((sql) => compactSQL(sql).startsWith('INSERT INTO'))).toBe(false)
  })

  test('executes zero-cache DELETE RETURNING count CTEs as SQLite deletes', async () => {
    const http = await startDoHttp((sql) => {
      if (compactSQL(sql).startsWith('DELETE FROM "todo_0/cdc_changeLog"')) {
        return {
          rows: [{ __orez_count__deleted: 1 }, { __orez_count__deleted: 1 }],
          columns: ['__orez_count__deleted'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'zero_cdb', 'delete-count-cte-test')
    await backend.waitReady

    await backend.execProtocolRaw(
      parseMessage(`
        WITH purged AS (
          DELETE FROM "todo_0/cdc"."changeLog"
          WHERE watermark < $1
          RETURNING watermark, pos
        )
        SELECT COUNT(*) AS deleted
        FROM purged;
      `)
    )
    await backend.execProtocolRaw(bindStatementParams(['a1zs3dw2usxs']))
    const result = await backend.execProtocolRaw(executePortal())

    expect(dataRowValues(result)).toEqual([['2']])
    expect(compactSQL(http.sqls.at(-1) || '')).toBe(
      'DELETE FROM "todo_0/cdc_changeLog" WHERE watermark < ? RETURNING 1 AS __orez_count__deleted'
    )
    expect(http.params.at(-1)).toEqual(['a1zs3dw2usxs'])
  })

  test('skips DELETE USING cleanup statements when the target table is empty', async () => {
    const http = await startDoHttp((sql) => {
      if (
        compactSQL(sql).includes(
          'SELECT 1 AS ok FROM "channelNotificationSetting" LIMIT 1'
        )
      ) {
        return { rows: [], columns: ['ok'] }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'empty-delete-using-test')
    await backend.waitReady

    await backend.execProtocolRaw(msg(0x51, cstr('BEGIN')))
    await backend.execProtocolRaw(
      msg(
        0x51,
        cstr(`
          WITH ranked AS (
            SELECT ctid, row_number() OVER (
              PARTITION BY "channelId", "userId"
              ORDER BY "updatedAt" DESC NULLS LAST, "id" DESC
            ) AS rn
            FROM "channelNotificationSetting"
          )
          DELETE FROM "channelNotificationSetting" t
          USING ranked
          WHERE t.ctid = ranked.ctid
            AND ranked.rn > 1;
        `)
      )
    )
    await backend.execProtocolRaw(msg(0x51, cstr('COMMIT')))

    expect(
      http.sqls.some((sql) =>
        compactSQL(sql).includes(
          'SELECT 1 AS ok FROM "channelNotificationSetting" LIMIT 1'
        )
      )
    ).toBe(true)
    expect(http.sqls.some((sql) => compactSQL(sql).startsWith('WITH ranked'))).toBe(false)
  })

  test('rewrites TRUNCATE statements to SQLite deletes', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'truncate-rewrite-test')
    await backend.waitReady

    await backend.exec('TRUNCATE public.data, "apiKey";')

    const sent = compactSQL(http.sqls.at(-1) || '')
    expect(sent).toContain('DELETE FROM data')
    expect(sent).toContain('DELETE FROM "apiKey"')
    expect(sent).not.toContain('TRUNCATE')
  })

  test('translates simple plpgsql row triggers to SQLite triggers', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'sqlite-trigger-test')
    await backend.waitReady

    await backend.exec(`
      CREATE OR REPLACE FUNCTION update_channel_latest_message_order()
      RETURNS TRIGGER AS $$
      BEGIN
        IF NEW.type IS DISTINCT FROM 'draft'
           AND NEW.deleted = false
           AND NEW."isThreadReply" = false
           AND NEW."order" IS NOT NULL THEN
          UPDATE "channel"
          SET "latestMessageOrder" = NEW."order"
          WHERE id = NEW."channelId"
            AND ("latestMessageOrder" IS NULL OR NEW."order" > "latestMessageOrder");
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;

      DROP TRIGGER IF EXISTS trg_update_channel_latest_message_order ON "message";
      CREATE TRIGGER trg_update_channel_latest_message_order
        AFTER INSERT OR UPDATE ON "message"
        FOR EACH ROW
        EXECUTE FUNCTION update_channel_latest_message_order();
    `)

    const sent = http.sqls.map(compactSQL).join('\n')
    expect(sent).toContain(
      'DROP TRIGGER IF EXISTS "trg_update_channel_latest_message_order_insert"'
    )
    expect(sent).toContain(
      'CREATE TRIGGER IF NOT EXISTS "trg_update_channel_latest_message_order_insert" AFTER INSERT ON "message"'
    )
    expect(sent).toContain(
      'CREATE TRIGGER IF NOT EXISTS "trg_update_channel_latest_message_order_update" AFTER UPDATE ON "message"'
    )
    expect(sent).toContain('UPDATE channel SET "latestMessageOrder" = new."order"')
    expect(sent).toContain('new.deleted = 0')
    expect(sent).not.toContain('CREATE OR REPLACE FUNCTION')
    expect(sent).not.toContain('EXECUTE FUNCTION')
  })

  test('translates SELECT INTO NEW in plpgsql row triggers', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'sqlite-select-into-trigger-test')
    await backend.waitReady

    await backend.exec(`
      CREATE OR REPLACE FUNCTION update_seen_last_order()
      RETURNS TRIGGER AS $$
      BEGIN
        IF NEW."messageId" IS NOT NULL THEN
          SELECT "order" INTO NEW."lastSeenOrder"
          FROM "message"
          WHERE id = NEW."messageId";
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;

      CREATE TRIGGER trg_update_seen_last_order
        BEFORE INSERT OR UPDATE ON "seen"
        FOR EACH ROW
        EXECUTE FUNCTION update_seen_last_order();
    `)

    const sent = http.sqls.map(compactSQL).join('\n')
    expect(sent).toContain(
      'CREATE TRIGGER IF NOT EXISTS "trg_update_seen_last_order_insert" AFTER INSERT ON "seen"'
    )
    expect(sent).toContain(
      'UPDATE "seen" SET "lastSeenOrder" = (SELECT "order" FROM message WHERE id = new."messageId") WHERE rowid = NEW.rowid'
    )
  })

  test('translates NEW column assignments in plpgsql row triggers', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(
      http.url,
      'postgres',
      'sqlite-new-assignment-trigger-test'
    )
    await backend.waitReady

    await backend.exec(`
      CREATE OR REPLACE FUNCTION update_task_numsteps()
      RETURNS TRIGGER AS $$
      BEGIN
        IF NEW."steps" IS NULL THEN
          NEW."numSteps" = 0;
        ELSE
          NEW."numSteps" = jsonb_array_length(NEW."steps");
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;

      CREATE TRIGGER task_numsteps_trigger
        BEFORE INSERT OR UPDATE OF "steps" ON "task"
        FOR EACH ROW
        EXECUTE FUNCTION update_task_numsteps();
    `)

    const sent = http.sqls.map(compactSQL).join('\n')
    expect(sent).toContain(
      'CREATE TRIGGER IF NOT EXISTS "task_numsteps_trigger_insert" AFTER INSERT ON "task"'
    )
    expect(sent).toContain(
      'CREATE TRIGGER IF NOT EXISTS "task_numsteps_trigger_update" AFTER UPDATE ON "task"'
    )
    expect(sent).toContain(
      'UPDATE "task" SET "numSteps" = 0 WHERE rowid = NEW.rowid AND (new.steps IS NULL)'
    )
    expect(sent).toContain(
      'UPDATE "task" SET "numSteps" = json_array_length(new.steps) WHERE rowid = NEW.rowid AND (NOT (new.steps IS NULL))'
    )
  })

  test('removes update target aliases from compiled SQLite triggers', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(
      http.url,
      'postgres',
      'sqlite-update-alias-trigger-test'
    )
    await backend.waitReady

    await backend.exec(`
      CREATE OR REPLACE FUNCTION sync_user_role_permissions_on_insert()
      RETURNS TRIGGER AS $$
      BEGIN
        UPDATE "userRole" ur SET
          "canAdmin" = r."canAdmin"
        FROM "role" r
        WHERE ur."roleId" = r."id"
          AND ur."serverId" = NEW."serverId";
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;

      CREATE TRIGGER trg_user_role_permission_copy
        AFTER INSERT ON "userRole"
        FOR EACH ROW
        EXECUTE FUNCTION sync_user_role_permissions_on_insert();
    `)

    const sent = http.sqls.map(compactSQL).join('\n')
    expect(sent).toContain(
      'UPDATE "userRole" SET "canAdmin" = r."canAdmin" FROM role AS r WHERE "userRole"."roleId" = r.id'
    )
    expect(sent).toContain('"userRole"."serverId" = new."serverId"')
    expect(sent).not.toContain('UPDATE "userRole" AS ur')
  })

  test('rewrites md5 trigger expressions to deterministic SQLite-compatible values', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'sqlite-md5-trigger-test')
    await backend.waitReady

    await backend.exec(`
      CREATE OR REPLACE FUNCTION set_permissions_hash()
      RETURNS TRIGGER AS $$
      BEGIN
        NEW.hash = md5(NEW.permissions::text);
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;

      CREATE TRIGGER on_set_permissions
        BEFORE INSERT OR UPDATE ON "chat_permissions"
        FOR EACH ROW
        EXECUTE FUNCTION set_permissions_hash();
    `)

    const sent = http.sqls.map(compactSQL).join('\n')
    expect(sent).toContain(
      'UPDATE "chat_permissions" SET "hash" = new.permissions WHERE rowid = NEW.rowid'
    )
    expect(sent).not.toContain('md5(')
  })

  test('translates chat thread reply count TG_OP triggers', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(
      http.url,
      'postgres',
      'sqlite-thread-reply-trigger-test'
    )
    await backend.waitReady

    await backend.exec(`
      CREATE OR REPLACE FUNCTION "updateThreadReplyCount"()
      RETURNS trigger AS $$
      BEGIN
        IF TG_OP = 'INSERT' THEN
          RETURN NEW;
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;

      CREATE TRIGGER "threadReplyCountInsertTrigger"
        AFTER INSERT ON "message"
        FOR EACH ROW
        WHEN (NEW."threadId" IS NOT NULL)
        EXECUTE FUNCTION "updateThreadReplyCount"();
    `)

    const sent = http.sqls.map(compactSQL).join('\n')
    expect(sent).toContain(
      'CREATE TRIGGER IF NOT EXISTS "threadReplyCountInsertTrigger" AFTER INSERT ON "message"'
    )
    expect(sent).toContain('UPDATE "thread" SET "replyCount" = min(11')
    expect(sent).toContain('WHERE "threadId" = NEW."threadId"')
    expect(sent).toContain('AND "type" IS DISTINCT FROM')
    expect(sent).not.toContain('TG_OP')
  })

  test('skips unsupported plpgsql trigger statements instead of throwing', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'sqlite-unsupported-trigger-test')
    await backend.waitReady

    await backend.exec(`
      CREATE OR REPLACE FUNCTION unsupported_trigger()
      RETURNS TRIGGER AS $$
      BEGIN
        invalid plpgsql syntax here;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;

      CREATE TRIGGER unsupported_trigger
        BEFORE INSERT ON "task"
        FOR EACH ROW
        EXECUTE FUNCTION unsupported_trigger();
    `)

    const sent = http.sqls.map(compactSQL).join('\n')
    expect(sent).not.toContain('CREATE TRIGGER IF NOT EXISTS "unsupported_trigger"')
  })

  test('skips plpgsql triggers that require PostgreSQL trigger variables', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'sqlite-trigger-variable-test')
    await backend.waitReady

    await backend.exec(`
      CREATE OR REPLACE FUNCTION refresh_stats()
      RETURNS TRIGGER AS $$
      BEGIN
        IF TG_OP = 'INSERT' THEN
          UPDATE stats SET count = count + 1;
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;

      CREATE TRIGGER refresh_stats_insert
        AFTER INSERT ON "messageReaction"
        FOR EACH ROW
        EXECUTE FUNCTION refresh_stats();
    `)

    const sent = http.sqls.map(compactSQL).join('\n')
    expect(sent).not.toContain('CREATE TRIGGER IF NOT EXISTS "refresh_stats_insert"')
    expect(sent).not.toContain('TG_OP')
  })

  test('rewrites trigger drops for SQLite trigger variants', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'drop-trigger-test')
    await backend.waitReady

    await backend.exec(
      'DROP TRIGGER IF EXISTS "messageReactionInsertTrigger" ON "messageReaction";'
    )

    const sent = http.sqls.map(compactSQL).join('\n')
    expect(sent).toContain('DROP TRIGGER IF EXISTS "messageReactionInsertTrigger"')
    expect(sent).toContain('DROP TRIGGER IF EXISTS "messageReactionInsertTrigger_insert"')
  })

  test('drops event trigger statements because event trigger creation is skipped', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'drop-event-trigger-test')
    await backend.waitReady

    await backend.exec(`
      DROP EVENT TRIGGER IF EXISTS chat_ddl_start_0;
      CREATE EVENT TRIGGER chat_ddl_start_0
        ON ddl_command_start EXECUTE FUNCTION public._zero_notify_change();
    `)

    expect(http.sqls.some((sql) => compactSQL(sql).includes('EVENT TRIGGER'))).toBe(false)
  })

  test('drops function statements because function creation is skipped', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'drop-function-test')
    await backend.waitReady

    await backend.exec('DROP FUNCTION IF EXISTS "notifyReactionChange"();')

    expect(http.sqls.some((sql) => compactSQL(sql).startsWith('DROP FUNCTION'))).toBe(
      false
    )
  })

  test('drops anonymous DO blocks because they only wrap skipped PG DDL', async () => {
    const http = await startDoHttp(() => ({ rows: [], columns: [] }))
    const backend = new DoBackend(http.url, 'postgres', 'do-block-test')
    await backend.waitReady

    await backend.exec(`
      DO $$ BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM pg_constraint WHERE conname = 'example_constraint'
        ) THEN
          ALTER TABLE "example" ADD CONSTRAINT "example_constraint" UNIQUE ("id");
        END IF;
      END $$;
    `)

    expect(http.sqls.some((sql) => compactSQL(sql).startsWith('DO'))).toBe(false)
  })

  test('no-ops direct calls to functions skipped by parser-backed DDL rewrite', async () => {
    const http = await startDoHttp((sql) => {
      if (compactSQL(sql).startsWith('SELECT NULL AS')) {
        return {
          rows: [{ refreshMessageReactionStats: null }],
          columns: ['refreshMessageReactionStats'],
        }
      }
      return { rows: [], columns: [] }
    })
    const backend = new DoBackend(http.url, 'postgres', 'skipped-function-call-test')
    await backend.waitReady

    await backend.exec(`
      CREATE OR REPLACE FUNCTION "refreshMessageReactionStats"()
      RETURNS void AS $$
      BEGIN
      END;
      $$ LANGUAGE plpgsql;
    `)
    await backend.exec('SELECT "refreshMessageReactionStats"();')

    expect(http.sqls.some((sql) => sql.includes('"refreshMessageReactionStats"()'))).toBe(
      false
    )
    expect(
      http.sqls.some((sql) =>
        compactSQL(sql).includes('SELECT NULL AS "refreshMessageReactionStats"')
      )
    ).toBe(true)
  })
})
