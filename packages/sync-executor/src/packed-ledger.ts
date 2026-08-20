import { MutationWriteSetError } from './errors.js'

import type {
  ApplicationDatabase,
  ApplicationTransaction,
  JsonPrimitive,
} from './types.js'

export type PackedLedgerKey = {
  readonly table: string
  readonly key: Readonly<Record<string, JsonPrimitive>>
}

export type PackedLedgerIdentity = {
  readonly clientGroupID: string
  readonly clientID: string
  readonly mutationID: number
}

type PackedLedgerPayload = {
  format: 2
  transactions: {
    version: string
    changes: [string, Readonly<Record<string, JsonPrimitive>>][]
    reset?: true
  }[]
}

type LegacyPackedLedgerPayload = {
  format: 1
  lmids: Record<string, Record<string, string>>
  transactions: {
    version: string
    changes: [string, Readonly<Record<string, JsonPrimitive>>][]
    lmid?: {
      clientGroupID: string
      clientID: string
      mutationID: string
    }
    reset?: true
  }[]
}

type ActiveSegment = {
  startVersion: number | bigint | string
  endVersion: number | bigint | string
  payload: string
  pending: string
  captureMode: number | bigint | string
}

const ROTATE_AT_BYTES = 768 * 1_024
const MAX_PAYLOAD_BYTES = 1_024 * 1_024

function counter(value: unknown, name: string): number {
  const parsed = Number(value)
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new MutationWriteSetError(`packed ledger ${name} is unsafe: ${String(value)}`)
  }
  return parsed
}

function parsePayload(value: unknown): PackedLedgerPayload {
  if (typeof value !== 'string') {
    throw new MutationWriteSetError('packed ledger payload is not text')
  }
  let parsed: unknown
  try {
    parsed = JSON.parse(value)
  } catch {
    throw new MutationWriteSetError('packed ledger payload is invalid JSON')
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new MutationWriteSetError('packed ledger payload is not an object')
  }
  const payload = parsed as Partial<PackedLedgerPayload>
  if (payload.format !== 2 || !Array.isArray(payload.transactions)) {
    throw new MutationWriteSetError('packed ledger payload has an unknown shape')
  }
  return payload as PackedLedgerPayload
}

function parseLegacyPayload(value: unknown): LegacyPackedLedgerPayload {
  if (typeof value !== 'string') {
    throw new MutationWriteSetError('packed ledger payload is not text')
  }
  let parsed: unknown
  try {
    parsed = JSON.parse(value)
  } catch {
    throw new MutationWriteSetError('packed ledger payload is invalid JSON')
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new MutationWriteSetError('packed ledger payload is not an object')
  }
  const payload = parsed as Partial<LegacyPackedLedgerPayload>
  if (
    payload.format !== 1 ||
    !payload.lmids ||
    typeof payload.lmids !== 'object' ||
    Array.isArray(payload.lmids) ||
    !Array.isArray(payload.transactions)
  ) {
    throw new MutationWriteSetError('packed ledger payload has an unknown shape')
  }
  return payload as LegacyPackedLedgerPayload
}

function activeSegment(rows: readonly ActiveSegment[]): ActiveSegment {
  if (rows.length !== 1) {
    throw new MutationWriteSetError('packed ledger has no active segment')
  }
  return rows[0]!
}

async function readActiveSegment(tx: ApplicationTransaction): Promise<ActiveSegment> {
  return activeSegment(
    await tx.query<ActiveSegment>(
      `SELECT "startVersion" AS "startVersion", "endVersion" AS "endVersion",
              "payload" AS "payload", "pending" AS "pending",
              "captureMode" AS "captureMode"
       FROM "_zsync_log_segments" ORDER BY "startVersion" DESC LIMIT 1`
    )
  )
}

function emptyPayload(): PackedLedgerPayload {
  return { format: 2, transactions: [] }
}

async function rotatePackedLedger(
  tx: ApplicationTransaction,
  active: ActiveSegment
): Promise<ActiveSegment> {
  const start = counter(active.startVersion, 'start version')
  const end = counter(active.endVersion, 'end version')
  const mode = counter(active.captureMode, 'capture mode')
  if (mode !== 0 && mode !== 1) {
    throw new MutationWriteSetError('packed ledger capture mode is invalid')
  }
  if (end + 1 < start) {
    throw new MutationWriteSetError('packed ledger segment bounds are invalid')
  }
  if (active.pending !== '[]') {
    throw new MutationWriteSetError(
      'packed ledger cannot rotate with pending trigger keys'
    )
  }
  if (end === Number.MAX_SAFE_INTEGER) {
    throw new MutationWriteSetError('packed ledger version space is exhausted')
  }
  if (mode !== 0) {
    const reset = await tx.exec(
      `UPDATE "_zsync_log_segments" SET "captureMode" = 0
       WHERE "startVersion" = ? AND "captureMode" = ?`,
      [start, mode]
    )
    if (reset.changes !== 1) {
      throw new MutationWriteSetError('packed ledger rotation missed its old segment')
    }
  }
  const inserted = await tx.exec(
    `INSERT INTO "_zsync_log_segments"
       ("startVersion", "endVersion", "payload", "pending", "captureMode")
     VALUES (?, ?, ?, '[]', ?)`,
    [end + 1, end, JSON.stringify(emptyPayload()), mode]
  )
  if (inserted.changes !== 1) {
    throw new MutationWriteSetError('packed ledger rotation did not create a segment')
  }
  return readActiveSegment(tx)
}

export async function initializePackedLedger(
  database: ApplicationDatabase
): Promise<void> {
  await database.transaction(async (tx) => {
    await tx.exec(`CREATE TABLE IF NOT EXISTS "_zsync_log_segments" (
      "startVersion" INTEGER PRIMARY KEY,
      "endVersion" INTEGER NOT NULL,
      "payload" TEXT NOT NULL,
      "pending" TEXT NOT NULL,
      "captureMode" INTEGER NOT NULL CHECK ("captureMode" IN (0, 1))
    ) WITHOUT ROWID`)
    // read only the active segment here: this runs on every executor start,
    // and scanning every retained payload belongs to the format-1 migration
    // alone.
    // captureMode rides along on the row we already read, so the residue check
    // below costs nothing on a settled namespace.
    const activeRows = await tx.query<{
      payload: string
      captureMode: number | bigint | string
    }>(
      `SELECT "payload" AS "payload", "captureMode" AS "captureMode"
       FROM "_zsync_log_segments" ORDER BY "startVersion" DESC LIMIT 1`
    )
    if (activeRows.length > 0) {
      const active = activeRows[0]!
      let activePayload: LegacyPackedLedgerPayload | null = null
      try {
        parsePayload(active.payload)
      } catch {
        activePayload = parseLegacyPayload(active.payload)
      }
      if (activePayload) {
        for (const [clientGroupID, clients] of Object.entries(activePayload.lmids)) {
          for (const [clientID, mutationID] of Object.entries(clients)) {
            await tx.exec(
              `INSERT INTO "_zsync_clients"
                 ("clientGroupID", "clientID", "lastMutationID", "userID")
               VALUES (?, ?, ?, NULL)
               ON CONFLICT ("clientGroupID", "clientID")
               DO UPDATE SET "lastMutationID" = excluded."lastMutationID"`,
              [clientGroupID, clientID, counter(mutationID, 'legacy last mutation id')]
            )
          }
        }
        const segments = await tx.query<{
          startVersion: number | bigint | string
          payload: string
        }>(
          `SELECT "startVersion" AS "startVersion", "payload" AS "payload"
           FROM "_zsync_log_segments" ORDER BY "startVersion"`
        )
        for (const segment of segments) {
          const legacy = parseLegacyPayload(segment.payload)
          await tx.exec(
            `UPDATE "_zsync_log_segments" SET "payload" = ? WHERE "startVersion" = ?`,
            [
              JSON.stringify({
                format: 2,
                transactions: legacy.transactions.map(({ changes, reset, version }) => ({
                  version,
                  changes,
                  ...(reset ? { reset: true as const } : {}),
                })),
              } satisfies PackedLedgerPayload),
              counter(segment.startVersion, 'start version'),
            ]
          )
        }
      }
      // a committed captureMode = 1 is orphaned capture state. this column is
      // toggled through plain sql mid-transaction, so a delegated push
      // abandoned before commit or rollback used to leave it set forever: every
      // later push then failed preparePackedLedger with "uncommitted capture
      // state", and the trigger bodies (gated on captureMode = 0) silently
      // dropped the next write's change envelope while still writing its row.
      // the tx journal restores the column on session disposal now, so anything
      // still set at start-up belongs to a namespace wedged before that landed
      // and no live writer owns it.
      //
      // one exception: while a transaction is journaled in flight, the toggle is
      // that transaction's, and its commit or rollback (not this pass) resolves
      // it. `_orez_tx_manifest` rows exist exactly until a transaction commits,
      // so their absence is the proof that no writer is mid-flight; a host that
      // keeps no journal has no such writer either.
      if (counter(active.captureMode, 'capture mode') !== 0) {
        const journaling = await tx.query(
          `SELECT 1 FROM sqlite_schema
           WHERE type = 'table' AND name = '_orez_tx_manifest'`
        )
        const inFlight =
          journaling.length > 0 &&
          (await tx.query(`SELECT 1 FROM "_orez_tx_manifest" LIMIT 1`)).length > 0
        if (!inFlight) {
          await tx.exec(
            `UPDATE "_zsync_log_segments" SET "captureMode" = 0
             WHERE "startVersion" = (SELECT MAX("startVersion") FROM "_zsync_log_segments")`
          )
        }
      }
      return
    }

    // seed above every watermark the retired `_zsync_changes` journal ever
    // assigned, so cookies issued against it cannot regress. the DO drops that
    // journal at boot after copying its high watermark into `_zsync_watermark`
    // below, so on most namespaces only the durable watermark read remains.
    const legacyTable = await tx.query<{ name: string }>(
      `SELECT name FROM sqlite_schema WHERE type = 'table' AND name = '_zsync_changes'`
    )
    let head = 0
    if (legacyTable.length > 0) {
      const legacy = await tx.query<{ watermark: number | bigint | string }>(
        'SELECT COALESCE(MAX("watermark"), 0) AS "watermark" FROM "_zsync_changes"'
      )
      head = counter(legacy[0]?.watermark ?? 0, 'legacy watermark')
    }
    const watermarkTable = await tx.query<{ name: string }>(
      `SELECT name FROM sqlite_schema WHERE type = 'table' AND name = '_zsync_watermark'`
    )
    if (watermarkTable.length > 0) {
      const durable = await tx.query<{ high: number | bigint | string }>(
        'SELECT "high" AS "high" FROM "_zsync_watermark" WHERE "lock" = 1'
      )
      head = Math.max(head, counter(durable[0]?.high ?? 0, 'durable watermark'))
    }
    await tx.exec(
      `INSERT INTO "_zsync_log_segments"
         ("startVersion", "endVersion", "payload", "pending", "captureMode")
       VALUES (?, ?, ?, '[]', 0)`,
      [head + 1, head, JSON.stringify(emptyPayload())]
    )
  })
}

export async function preparePackedLedger(tx: ApplicationTransaction): Promise<number> {
  let active = await readActiveSegment(tx)
  if (counter(active.captureMode, 'capture mode') !== 0 || active.pending !== '[]') {
    throw new MutationWriteSetError('packed ledger has uncommitted capture state')
  }
  parsePayload(active.payload)
  if (new TextEncoder().encode(active.payload).byteLength >= ROTATE_AT_BYTES) {
    active = await rotatePackedLedger(tx, active)
  }
  return counter(active.endVersion, 'end version')
}

export async function setPackedCaptureMode(
  tx: ApplicationTransaction,
  exact: boolean
): Promise<void> {
  const result = await tx.exec(
    `UPDATE "_zsync_log_segments" SET "captureMode" = ?
     WHERE "startVersion" = (SELECT MAX("startVersion") FROM "_zsync_log_segments")`,
    [exact ? 1 : 0]
  )
  if (result.changes !== 1) {
    throw new MutationWriteSetError('packed ledger capture mode update missed its row')
  }
}

function parsePending(value: unknown): PackedLedgerKey[] {
  if (typeof value !== 'string') {
    throw new MutationWriteSetError('packed ledger pending keys are not text')
  }
  let parsed: unknown
  try {
    parsed = JSON.parse(value)
  } catch {
    throw new MutationWriteSetError('packed ledger pending keys are invalid JSON')
  }
  if (!Array.isArray(parsed)) {
    throw new MutationWriteSetError('packed ledger pending keys are not an array')
  }
  return parsed.map((entry) => {
    if (
      !Array.isArray(entry) ||
      entry.length !== 2 ||
      typeof entry[0] !== 'string' ||
      !entry[1] ||
      typeof entry[1] !== 'object' ||
      Array.isArray(entry[1])
    ) {
      throw new MutationWriteSetError('packed ledger pending key has an invalid shape')
    }
    const key: Record<string, JsonPrimitive> = {}
    for (const [column, field] of Object.entries(entry[1])) {
      if (
        field !== null &&
        typeof field !== 'string' &&
        typeof field !== 'boolean' &&
        !(typeof field === 'number' && Number.isFinite(field))
      ) {
        throw new MutationWriteSetError('packed ledger pending key is not JSON-safe')
      }
      key[column] = field
    }
    return { table: entry[0], key }
  })
}

export async function commitPackedLedger(
  tx: ApplicationTransaction,
  exactKeys: readonly PackedLedgerKey[],
  identity?: PackedLedgerIdentity,
  rawCaptureStart?: number
): Promise<void> {
  let active = await readActiveSegment(tx)
  let payload = parsePayload(active.payload)
  const changes = new Map<string, PackedLedgerKey>()
  const rawKeys = new Set<string>()
  if (rawCaptureStart !== undefined && exactKeys.length > 0) {
    const segments = await tx.query<{ payload: string }>(
      `SELECT "payload" AS "payload" FROM "_zsync_log_segments"
       WHERE "endVersion" > ? ORDER BY "startVersion"`,
      [rawCaptureStart]
    )
    for (const segment of segments) {
      for (const transaction of parsePayload(segment.payload).transactions) {
        if (counter(transaction.version, 'transaction version') <= rawCaptureStart) {
          continue
        }
        for (const change of parsePending(JSON.stringify(transaction.changes))) {
          rawKeys.add(JSON.stringify([change.table, change.key]))
        }
      }
    }
  }
  for (const change of [
    ...parsePending(active.pending),
    ...exactKeys.filter(
      (change) => !rawKeys.has(JSON.stringify([change.table, change.key]))
    ),
  ]) {
    changes.set(JSON.stringify([change.table, change.key]), change)
  }
  if (!identity && changes.size === 0) {
    if (counter(active.captureMode, 'capture mode') !== 0) {
      await setPackedCaptureMode(tx, false)
    }
    return
  }
  if (new TextEncoder().encode(active.payload).byteLength >= ROTATE_AT_BYTES) {
    active = await rotatePackedLedger(tx, active)
    payload = parsePayload(active.payload)
  }
  const start = counter(active.startVersion, 'start version')
  const end = counter(active.endVersion, 'end version')
  if (end === Number.MAX_SAFE_INTEGER) {
    throw new MutationWriteSetError('packed ledger version space is exhausted')
  }
  const version = end + 1
  if (identity) {
    if (!Number.isSafeInteger(identity.mutationID) || identity.mutationID < 0) {
      throw new MutationWriteSetError('packed ledger mutation id is unsafe')
    }
    const advanced = await tx.exec(
      `UPDATE "_zsync_clients" SET "lastMutationID" = ?
       WHERE "clientGroupID" = ? AND "clientID" = ?`,
      [identity.mutationID, identity.clientGroupID, identity.clientID]
    )
    if (advanced.changes !== 1) {
      throw new MutationWriteSetError('packed ledger client identity is missing')
    }
  }
  payload.transactions.push({
    version: String(version),
    changes: [...changes.values()]
      .sort((left, right) =>
        JSON.stringify([left.table, left.key]).localeCompare(
          JSON.stringify([right.table, right.key])
        )
      )
      .map(({ table, key }) => [table, key]),
  })
  const encoded = JSON.stringify(payload)
  if (new TextEncoder().encode(encoded).byteLength > MAX_PAYLOAD_BYTES) {
    throw new MutationWriteSetError('packed ledger transaction exceeds the 1 MiB limit')
  }
  const result = await tx.exec(
    `UPDATE "_zsync_log_segments"
     SET "endVersion" = ?, "payload" = ?, "pending" = '[]', "captureMode" = 0
     WHERE "startVersion" = ?`,
    [version, encoded, start]
  )
  if (result.changes !== 1) {
    throw new MutationWriteSetError('packed ledger commit missed its active segment')
  }
}

export async function readPackedLMID(
  tx: ApplicationTransaction,
  clientGroupID: string,
  clientID: string
): Promise<number> {
  const rows = await tx.query<{ lastMutationID: number | bigint | string }>(
    `SELECT "lastMutationID" AS "lastMutationID" FROM "_zsync_clients"
     WHERE "clientGroupID" = ? AND "clientID" = ?`,
    [clientGroupID, clientID]
  )
  return counter(rows[0]?.lastMutationID ?? 0, 'last mutation id')
}
