/**
 * pgoutput binary protocol encoder.
 *
 * encodes change records into the binary format that postgres uses
 * for logical replication (pgoutput plugin).
 *
 * all functions return Uint8Array for cross-platform compatibility.
 */

// postgres epoch: 2000-01-01 in microseconds from unix epoch
const PG_EPOCH_MICROS = 946684800000000n

// shared encoder instance - avoids per-call allocation
const encoder = new TextEncoder()

// table oid tracking
const tableOids = new Map<string, number>()
let nextOid = 16384

function getTableOid(tableName: string): number {
  let oid = tableOids.get(tableName)
  if (!oid) {
    oid = nextOid++
    tableOids.set(tableName, oid)
  }
  return oid
}

export interface ColumnInfo {
  name: string
  typeOid: number
  typeMod: number
  isKey?: boolean
}

// infer columns from a jsonb row
export function inferColumns(row: Record<string, unknown>): ColumnInfo[] {
  return Object.keys(row).map((name) => ({
    name,
    typeOid: 25, // text oid - safe default, zero-cache re-maps types
    typeMod: -1,
  }))
}

// reusable scratch buffer for building messages (64KB, grows if needed)
let scratch = new Uint8Array(65536)
let scratchView = new DataView(scratch.buffer)

function ensureScratch(size: number): void {
  if (scratch.length < size) {
    const newSize = Math.max(size, scratch.length * 2)
    const newScratch = new Uint8Array(newSize)
    newScratch.set(scratch)
    scratch = newScratch
    scratchView = new DataView(scratch.buffer)
  }
}

function writeInt16At(offset: number, val: number): void {
  scratchView.setInt16(offset, val)
}

function writeInt32At(offset: number, val: number): void {
  scratchView.setInt32(offset, val)
}

// legacy helpers for standalone buffers
function writeInt16(buf: Uint8Array, offset: number, val: number): void {
  new DataView(buf.buffer, buf.byteOffset).setInt16(offset, val)
}

function writeInt32(buf: Uint8Array, offset: number, val: number): void {
  new DataView(buf.buffer, buf.byteOffset).setInt32(offset, val)
}

function writeInt64(buf: Uint8Array, offset: number, val: bigint): void {
  new DataView(buf.buffer, buf.byteOffset).setBigInt64(offset, val)
}

// encode a BEGIN message
export function encodeBegin(lsn: bigint, timestamp: bigint, xid: number): Uint8Array {
  const buf = new Uint8Array(1 + 8 + 8 + 4)
  buf[0] = 0x42 // 'B'
  writeInt64(buf, 1, lsn)
  writeInt64(buf, 9, timestamp - PG_EPOCH_MICROS)
  writeInt32(buf, 17, xid)
  return buf
}

// encode a COMMIT message
export function encodeCommit(
  flags: number,
  lsn: bigint,
  endLsn: bigint,
  timestamp: bigint
): Uint8Array {
  const buf = new Uint8Array(1 + 1 + 8 + 8 + 8)
  buf[0] = 0x43 // 'C'
  buf[1] = flags
  writeInt64(buf, 2, lsn)
  writeInt64(buf, 10, endLsn)
  writeInt64(buf, 18, timestamp - PG_EPOCH_MICROS)
  return buf
}

// encode a RELATION message
export function encodeRelation(
  tableOid: number,
  schema: string,
  tableName: string,
  replicaIdentity: number,
  columns: ColumnInfo[]
): Uint8Array {
  const schemaBytes = encoder.encode(schema)
  const nameBytes = encoder.encode(tableName)

  // calculate column sizes
  let columnsSize = 0
  const colNameBytes: Uint8Array[] = []
  for (const col of columns) {
    const nb = encoder.encode(col.name)
    colNameBytes.push(nb)
    columnsSize += 1 + nb.length + 1 + 4 + 4 // flags + name + null + typeOid + typeMod
  }

  const total =
    1 + 4 + schemaBytes.length + 1 + nameBytes.length + 1 + 1 + 2 + columnsSize
  const buf = new Uint8Array(total)
  let pos = 0

  buf[pos++] = 0x52 // 'R'
  writeInt32(buf, pos, tableOid)
  pos += 4
  buf.set(schemaBytes, pos)
  pos += schemaBytes.length
  buf[pos++] = 0
  buf.set(nameBytes, pos)
  pos += nameBytes.length
  buf[pos++] = 0
  buf[pos++] = replicaIdentity
  writeInt16(buf, pos, columns.length)
  pos += 2

  for (let i = 0; i < columns.length; i++) {
    buf[pos++] = columns[i].isKey ? 1 : 0 // flags: 1 = part of replica identity key
    buf.set(colNameBytes[i], pos)
    pos += colNameBytes[i].length
    buf[pos++] = 0
    writeInt32(buf, pos, columns[i].typeOid)
    pos += 4
    writeInt32(buf, pos, columns[i].typeMod)
    pos += 4
  }

  return buf
}

// encode tuple data directly into scratch buffer starting at given offset.
// returns the number of bytes written.
function encodeTupleDataInto(
  row: Record<string, unknown>,
  columns: ColumnInfo[],
  startOffset: number
): number {
  let pos = startOffset

  // reserve space for ncolumns
  ensureScratch(pos + 2 + columns.length * 32)
  writeInt16At(pos, columns.length)
  pos += 2

  for (const col of columns) {
    const val = row[col.name]
    if (val === null || val === undefined) {
      ensureScratch(pos + 1)
      scratch[pos++] = 0x6e // 'n' for null
    } else {
      // convert to postgresql text format
      let strVal: string
      if (typeof val === 'boolean') {
        strVal = val ? 't' : 'f'
      } else if (typeof val === 'object') {
        strVal = JSON.stringify(val)
      } else {
        strVal = String(val)
        // normalize ISO timestamps to postgres text format.
        // to_jsonb() produces "2026-03-19T07:20:11.643" but postgres
        // pgoutput sends "2026-03-19 07:20:11.643" (space, no T).
        // mismatch causes zero-cache to see different values during
        // mutation reconciliation, triggering unnecessary rebases.
        if (
          (col.typeOid === 1114 || col.typeOid === 1184) &&
          typeof val === 'string' &&
          val.length >= 19
        ) {
          strVal = strVal.replace('T', ' ')
        }
      }
      const bytes = encoder.encode(strVal)
      ensureScratch(pos + 1 + 4 + bytes.length)
      scratch[pos++] = 0x74 // 't' for text
      writeInt32At(pos, bytes.length)
      pos += 4
      scratch.set(bytes, pos)
      pos += bytes.length
    }
  }

  return pos - startOffset
}

/**
 * encode a complete change message wrapped in CopyData(XLogData(...)).
 * avoids intermediate buffer allocations by writing directly into one buffer.
 */
export function encodeWrappedChange(
  walStart: bigint,
  walEnd: bigint,
  timestamp: bigint,
  changeData: Uint8Array
): Uint8Array {
  // CopyData header: 'd' + int32 len
  // XLogData header: 'w' + int64 walStart + int64 walEnd + int64 timestamp
  // then changeData
  const xlogSize = 1 + 8 + 8 + 8 + changeData.length
  const totalSize = 1 + 4 + xlogSize
  const buf = new Uint8Array(totalSize)

  // CopyData
  buf[0] = 0x64 // 'd'
  writeInt32(buf, 1, 4 + xlogSize)

  // XLogData
  buf[5] = 0x77 // 'w'
  writeInt64(buf, 6, walStart)
  writeInt64(buf, 14, walEnd)
  writeInt64(buf, 22, timestamp - PG_EPOCH_MICROS)

  // change payload
  buf.set(changeData, 30)

  return buf
}

// encode an INSERT message
export function encodeInsert(
  tableOid: number,
  row: Record<string, unknown>,
  columns: ColumnInfo[]
): Uint8Array {
  // write header + tuple directly into scratch
  const headerSize = 1 + 4 + 1 // 'I' + oid + 'N'
  ensureScratch(headerSize + 2 + columns.length * 64)
  scratch[0] = 0x49 // 'I'
  writeInt32At(1, tableOid)
  scratch[5] = 0x4e // 'N' for new tuple
  const tupleLen = encodeTupleDataInto(row, columns, 6)
  return scratch.slice(0, 6 + tupleLen)
}

// encode an UPDATE message
export function encodeUpdate(
  tableOid: number,
  row: Record<string, unknown>,
  oldRow: Record<string, unknown> | null,
  columns: ColumnInfo[]
): Uint8Array {
  ensureScratch(1 + 4 + 1 + columns.length * 128)
  scratch[0] = 0x55 // 'U'
  writeInt32At(1, tableOid)

  if (oldRow) {
    scratch[5] = 0x4f // 'O' for old tuple
    const oldLen = encodeTupleDataInto(oldRow, columns, 6)
    scratch[6 + oldLen] = 0x4e // 'N' for new tuple
    const newLen = encodeTupleDataInto(row, columns, 7 + oldLen)
    return scratch.slice(0, 7 + oldLen + newLen)
  }

  scratch[5] = 0x4e // 'N'
  const newLen = encodeTupleDataInto(row, columns, 6)
  return scratch.slice(0, 6 + newLen)
}

// encode a DELETE message
export function encodeDelete(
  tableOid: number,
  oldRow: Record<string, unknown>,
  columns: ColumnInfo[]
): Uint8Array {
  ensureScratch(1 + 4 + 1 + columns.length * 64)
  scratch[0] = 0x44 // 'D'
  writeInt32At(1, tableOid)
  scratch[5] = 0x4b // 'K' for key tuple
  const tupleLen = encodeTupleDataInto(oldRow, columns, 6)
  return scratch.slice(0, 6 + tupleLen)
}

// wrap a pgoutput message in XLogData format
export function wrapXLogData(
  walStart: bigint,
  walEnd: bigint,
  timestamp: bigint,
  data: Uint8Array
): Uint8Array {
  const buf = new Uint8Array(1 + 8 + 8 + 8 + data.length)
  buf[0] = 0x77 // 'w' XLogData
  writeInt64(buf, 1, walStart)
  writeInt64(buf, 9, walEnd)
  writeInt64(buf, 17, timestamp - PG_EPOCH_MICROS)
  buf.set(data, 25)
  return buf
}

// wrap in CopyData format
export function wrapCopyData(data: Uint8Array): Uint8Array {
  const buf = new Uint8Array(1 + 4 + data.length)
  buf[0] = 0x64 // 'd' CopyData
  writeInt32(buf, 1, 4 + data.length)
  buf.set(data, 5)
  return buf
}

// encode a primary keepalive message
export function encodeKeepalive(
  walEnd: bigint,
  timestamp: bigint,
  replyRequested: boolean
): Uint8Array {
  const inner = new Uint8Array(1 + 8 + 8 + 1)
  inner[0] = 0x6b // 'k' keepalive
  writeInt64(inner, 1, walEnd)
  writeInt64(inner, 9, timestamp - PG_EPOCH_MICROS)
  inner[17] = replyRequested ? 1 : 0
  return wrapCopyData(inner)
}

export { getTableOid }
