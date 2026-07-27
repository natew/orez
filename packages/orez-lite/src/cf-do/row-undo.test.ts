/**
 * Row-undo runs against real SQLite because every defect it guards is a SQLite
 * behavior: json_object cannot hold a BLOB, JSON.parse rounds an int64, CAST to
 * TEXT loses a double's low bits, and SQLite refuses to write a generated
 * column. A mock SQL layer would assert the bug back into existence.
 */

// @ts-expect-error - CJS module
import BedrockSqlite from 'bedrock-sqlite'
import { describe, expect, it } from 'vitest'

import { suspendTriggers, TransactionalCdc } from './cdc.js'
import {
  appendPendingChange,
  ensurePendingChangesTable,
  rollbackPendingChanges,
} from './row-undo.js'

import type { DurableSqlStorage } from './watermark.js'

const BetterSqlite3 = BedrockSqlite.Database
const TX = 'tx-1'

function createSqliteStorage() {
  const nativeDb = new BetterSqlite3(':memory:')
  const exec = (sql: string, ...params: unknown[]) => {
    const stmt = nativeDb.prepare(sql)
    const rows: Array<Record<string, unknown>> = stmt.reader
      ? stmt.all(...params)
      : (stmt.run(...params), [])
    return { toArray: () => rows, one: () => rows[0] }
  }
  return { nativeDb, sql: { exec } as DurableSqlStorage }
}

/** Apply a tracked write exactly as ZeroDO does, journaling what it captured. */
function trackedWrite(
  sql: DurableSqlStorage,
  cdc: TransactionalCdc,
  table: string,
  statement: string,
  params: unknown[] = []
) {
  expect(
    cdc.ensureTable({ physicalTableName: table, tableName: `public.${table}` })
  ).toBe(true)
  sql.exec(statement, ...params)
  const captured = cdc.drain()
  ensurePendingChangesTable(sql)
  for (const change of captured) {
    appendPendingChange(sql, {
      transactionID: TX,
      physicalTableName: change.physicalTableName,
      tableName: change.tableName,
      publish: true,
      op: change.op,
      rowData: change.rowData,
      oldData: change.oldData,
      rowJournal: change.rowJournal,
      oldJournal: change.oldJournal,
      newRowid: change.newRowid,
      oldRowid: change.oldRowid,
      undoable: true,
    })
  }
  return captured
}

/** Every column plus its SQLite storage class, so a type change cannot hide. */
function typedRows(sql: DurableSqlStorage, table: string, order = 'rowid') {
  const columns = sql
    .exec(`PRAGMA table_xinfo("${table}")`)
    .toArray()
    .map((row) => String(row.name))
  const projection = columns
    .map((column) => `"${column}", typeof("${column}") AS "${column}#type"`)
    .join(', ')
  return sql.exec(`SELECT ${projection} FROM "${table}" ORDER BY ${order}`).toArray()
}

describe('row undo: lossless storage journal', () => {
  it('restores every SQLite storage class with its exact value and type', () => {
    const { sql } = createSqliteStorage()
    sql.exec(
      'CREATE TABLE v (id INTEGER PRIMARY KEY, i INTEGER, r REAL, t TEXT, b BLOB, n TEXT)'
    )
    sql.exec(
      `INSERT INTO v VALUES (1, 42, 1.5, 'héllo 🎉', x'00ff10', NULL),
                            (2, -7, 0.30000000000000004, '{"a":[1,null]}', x'', 'set')`
    )
    const before = typedRows(sql, 'v')

    const cdc = new TransactionalCdc(sql)
    trackedWrite(
      sql,
      cdc,
      'v',
      `UPDATE v SET i = 0, r = 0.0, t = 'clobbered', b = x'ffff', n = 'clobbered'`
    )
    trackedWrite(sql, cdc, 'v', 'DELETE FROM v WHERE id = 1')
    trackedWrite(sql, cdc, 'v', `INSERT INTO v VALUES (3, 1, 1.0, 'x', x'01', 'y')`)

    rollbackPendingChanges(sql, TX)
    expect(typedRows(sql, 'v')).toEqual(before)
  })

  it('round-trips int64 boundaries the wire format cannot hold', () => {
    const { sql } = createSqliteStorage()
    sql.exec('CREATE TABLE big (id INTEGER PRIMARY KEY, n INTEGER)')
    sql.exec(
      `INSERT INTO big VALUES (1, 9223372036854775807), (2, -9223372036854775808), (3, 9007199254740993)`
    )
    const cdc = new TransactionalCdc(sql)
    const captured = trackedWrite(sql, cdc, 'big', 'UPDATE big SET n = 0')

    // the journal keeps the exact decimal; the wire keeps it too, as text,
    // because a JSON number past 2^53 is a different integer.
    expect(captured[0].oldJournal).toMatchObject({ n: 'i9223372036854775807' })
    expect(captured[0].oldData).toMatchObject({ n: '9223372036854775807' })
    expect(captured[2].oldData).toMatchObject({ n: '9007199254740993' })

    rollbackPendingChanges(sql, TX)
    const exact = sql
      .exec(
        `SELECT
           sum(n = 9223372036854775807) AS max64,
           sum(n = -9223372036854775808) AS min64,
           sum(n = 9007199254740993) AS beyond53
         FROM big`
      )
      .toArray()
    expect(exact).toEqual([{ max64: 1, min64: 1, beyond53: 1 }])
  })

  it('round-trips doubles that a text cast would silently degrade', () => {
    const { sql } = createSqliteStorage()
    sql.exec('CREATE TABLE d (id INTEGER PRIMARY KEY, v REAL)')
    // MAX_DOUBLE through SQLite's default 15-digit text cast comes back as Inf,
    // and 0.30000000000000004 collapses to 0.3.
    sql.exec(
      `INSERT INTO d VALUES (1, 1.7976931348623157e308), (2, 0.30000000000000004),
                            (3, 5e-324), (4, 9e999), (5, -9e999)`
    )
    const before = sql.exec('SELECT id, v FROM d ORDER BY id').toArray()

    const cdc = new TransactionalCdc(sql)
    const captured = trackedWrite(sql, cdc, 'd', 'UPDATE d SET v = 1.0')
    expect(captured[3].oldData).toMatchObject({ v: 'Infinity' })
    expect(captured[4].oldData).toMatchObject({ v: '-Infinity' })
    const pendingWire = sql
      .exec(
        `SELECT old_data FROM _zero_pending_changes WHERE transaction_id = ? ORDER BY id`,
        TX
      )
      .toArray()
      .map((row) => JSON.parse(String(row.old_data)).v)
    expect(pendingWire.slice(3)).toEqual(['Infinity', '-Infinity'])
    rollbackPendingChanges(sql, TX)

    expect(sql.exec('SELECT id, v FROM d ORDER BY id').toArray()).toEqual(before)
    expect(
      sql.exec('SELECT sum(v = 9e999) AS inf, sum(v = -9e999) AS neg FROM d').toArray()
    ).toEqual([{ inf: 1, neg: 1 }])
  })

  it('keeps a blob a blob, and text that merely looks like one text', () => {
    const { sql } = createSqliteStorage()
    sql.exec('CREATE TABLE b (id INTEGER PRIMARY KEY, payload BLOB, label TEXT)')
    // The Zero wire format renders a blob as postgres bytea text, so a TEXT
    // column holding that same literal is indistinguishable on the wire. Only a
    // typed journal can tell them apart on the way back.
    sql.exec(`INSERT INTO b VALUES (1, x'00ff', '\\x00ff')`)
    const before = typedRows(sql, 'b')

    const cdc = new TransactionalCdc(sql)
    const captured = trackedWrite(
      sql,
      cdc,
      'b',
      `UPDATE b SET payload = x'aa', label = 'gone'`
    )
    expect(captured[0].oldJournal).toMatchObject({ payload: 'b00ff', label: 's\\x00ff' })
    expect(captured[0].oldData).toMatchObject({ payload: '\\x00ff', label: '\\x00ff' })

    rollbackPendingChanges(sql, TX)
    expect(typedRows(sql, 'b')).toEqual(before)
    expect(
      sql.exec(`SELECT typeof(payload) AS p, typeof(label) AS l FROM b`).toArray()
    ).toEqual([{ p: 'blob', l: 'text' }])
  })
})

describe('row undo: stable row identity', () => {
  it('deletes exactly one of two identical rows in a keyless table', () => {
    const { sql } = createSqliteStorage()
    sql.exec('CREATE TABLE keyless (a TEXT, b TEXT)')
    sql.exec(`INSERT INTO keyless VALUES ('dup', 'dup'), ('dup', 'dup')`)

    const cdc = new TransactionalCdc(sql)
    trackedWrite(sql, cdc, 'keyless', `INSERT INTO keyless VALUES ('dup', 'dup')`)
    expect(sql.exec('SELECT count(*) AS c FROM keyless').toArray()).toEqual([{ c: 3 }])

    // Matching on column values would delete all three.
    rollbackPendingChanges(sql, TX)
    expect(sql.exec('SELECT count(*) AS c FROM keyless').toArray()).toEqual([{ c: 2 }])
  })

  it('restores a deleted keyless row under its original rowid', () => {
    const { sql } = createSqliteStorage()
    sql.exec('CREATE TABLE keyless (a TEXT)')
    sql.exec(`INSERT INTO keyless VALUES ('one'), ('two'), ('three')`)
    const before = sql.exec('SELECT rowid, a FROM keyless ORDER BY rowid').toArray()

    const cdc = new TransactionalCdc(sql)
    trackedWrite(sql, cdc, 'keyless', `DELETE FROM keyless WHERE a = 'two'`)
    rollbackPendingChanges(sql, TX)

    expect(sql.exec('SELECT rowid, a FROM keyless ORDER BY rowid').toArray()).toEqual(
      before
    )
  })

  it('undoes a primary-key update, whose identity moved', () => {
    const { sql } = createSqliteStorage()
    sql.exec('CREATE TABLE item (id INTEGER PRIMARY KEY, body TEXT)')
    sql.exec(`INSERT INTO item VALUES (1, 'one')`)

    const cdc = new TransactionalCdc(sql)
    trackedWrite(sql, cdc, 'item', `UPDATE item SET id = 9, body = 'moved' WHERE id = 1`)
    expect(sql.exec('SELECT id FROM item').toArray()).toEqual([{ id: 9 }])

    rollbackPendingChanges(sql, TX)
    expect(sql.exec('SELECT id, body FROM item').toArray()).toEqual([
      { id: 1, body: 'one' },
    ])
  })

  it('identifies rows of a WITHOUT ROWID table by its composite key', () => {
    const { sql } = createSqliteStorage()
    sql.exec(
      'CREATE TABLE ck (tenant TEXT, id TEXT, body TEXT, PRIMARY KEY (tenant, id)) WITHOUT ROWID'
    )
    sql.exec(`INSERT INTO ck VALUES ('t1', 'a', 'first'), ('t2', 'a', 'second')`)
    const before = sql.exec('SELECT * FROM ck ORDER BY tenant').toArray()

    const cdc = new TransactionalCdc(sql)
    trackedWrite(sql, cdc, 'ck', `UPDATE ck SET body = 'clobbered' WHERE tenant = 't1'`)
    trackedWrite(sql, cdc, 'ck', `DELETE FROM ck WHERE tenant = 't2'`)
    trackedWrite(sql, cdc, 'ck', `INSERT INTO ck VALUES ('t3', 'a', 'new')`)

    rollbackPendingChanges(sql, TX)
    expect(sql.exec('SELECT * FROM ck ORDER BY tenant').toArray()).toEqual(before)
  })
})

describe('row undo: generated columns', () => {
  it('restores rows without ever writing a generated column', () => {
    const { sql } = createSqliteStorage()
    sql.exec(
      `CREATE TABLE g (
         id INTEGER PRIMARY KEY,
         qty INTEGER,
         price INTEGER,
         total INTEGER GENERATED ALWAYS AS (qty * price) VIRTUAL,
         doubled INTEGER GENERATED ALWAYS AS (qty * 2) STORED
       )`
    )
    sql.exec('INSERT INTO g (id, qty, price) VALUES (1, 2, 10), (2, 3, 5)')
    const before = typedRows(sql, 'g')

    const cdc = new TransactionalCdc(sql)
    // Generated columns still belong in the captured image; SQLite just refuses
    // to let any INSERT or UPDATE name them.
    const captured = trackedWrite(sql, cdc, 'g', 'UPDATE g SET qty = 99 WHERE id = 1')
    expect(captured[0].oldData).toMatchObject({ qty: 2, total: 20, doubled: 4 })

    trackedWrite(sql, cdc, 'g', 'DELETE FROM g WHERE id = 2')
    trackedWrite(sql, cdc, 'g', 'INSERT INTO g (id, qty, price) VALUES (3, 7, 7)')

    rollbackPendingChanges(sql, TX)
    expect(typedRows(sql, 'g')).toEqual(before)
  })
})

describe('row undo: conflict detection', () => {
  it('fails the rollback when the row it must restore is not there', () => {
    const { sql } = createSqliteStorage()
    sql.exec('CREATE TABLE item (id INTEGER PRIMARY KEY, body TEXT)')
    sql.exec(`INSERT INTO item VALUES (1, 'one')`)

    const cdc = new TransactionalCdc(sql)
    trackedWrite(sql, cdc, 'item', `UPDATE item SET body = 'two' WHERE id = 1`)

    // something outside the transaction removed the row: restoring it would
    // silently write nothing, so the rollback has to fail instead.
    suspendTriggers(sql, ['item'])
    sql.exec('DELETE FROM item')

    expect(() => rollbackPendingChanges(sql, TX)).toThrow(/matched 0 rows/)
  })

  it('refuses to undo a change whose before-image predates the typed journal', () => {
    const { sql } = createSqliteStorage()
    sql.exec('CREATE TABLE item (id INTEGER PRIMARY KEY, body TEXT)')
    sql.exec(`INSERT INTO item VALUES (1, 'one')`)
    const cdc = new TransactionalCdc(sql)
    ensurePendingChangesTable(sql)

    // a pre-v2 row: the lossy wire image only, which cannot restore a blob or
    // an int64. Leaving the write applied would break rollback atomicity.
    appendPendingChange(sql, {
      transactionID: TX,
      physicalTableName: 'item',
      tableName: 'public.item',
      op: 'UPDATE',
      rowData: { id: 1, body: 'two' },
      oldData: { id: 1, body: 'one' },
      undoable: true,
    })

    expect(() => rollbackPendingChanges(sql, TX)).toThrow(/no usable before-image/)
  })
})

describe('row undo: business triggers', () => {
  it('restores rows without firing the table own triggers or staging phantom changes', () => {
    const { sql } = createSqliteStorage()
    sql.exec('CREATE TABLE item (id INTEGER PRIMARY KEY, body TEXT)')
    sql.exec('CREATE TABLE audit (id INTEGER PRIMARY KEY, note TEXT)')
    // Undoing an INSERT means running a DELETE, which would fire this trigger
    // and write an audit row the original transaction never made. The audit
    // table is captured too, so its CDC trigger would then stage a phantom
    // INSERT on top of the fake row.
    sql.exec(
      `CREATE TRIGGER item_audit AFTER DELETE ON item BEGIN
         INSERT INTO audit (note) VALUES ('deleted');
       END`
    )
    const cdc = new TransactionalCdc(sql)
    cdc.syncTables([
      { physicalTableName: 'item', tableName: 'public.item' },
      { physicalTableName: 'audit', tableName: 'public.audit' },
    ])

    trackedWrite(sql, cdc, 'item', `INSERT INTO item VALUES (1, 'one')`)
    rollbackPendingChanges(sql, TX)

    expect(sql.exec('SELECT * FROM item').toArray()).toEqual([])
    expect(sql.exec('SELECT * FROM audit').toArray()).toEqual([])
    expect(cdc.drain()).toEqual([])

    // and the business trigger is back for real writes
    sql.exec(`INSERT INTO item VALUES (2, 'two')`)
    sql.exec('DELETE FROM item WHERE id = 2')
    expect(sql.exec('SELECT count(*) AS c FROM audit').toArray()).toEqual([{ c: 1 }])
  })
})

describe('row undo: corrupt journal', () => {
  it('fails the rollback rather than restoring an unknown tag as NULL', () => {
    const { sql } = createSqliteStorage()
    sql.exec('CREATE TABLE item (id INTEGER PRIMARY KEY, body TEXT)')
    sql.exec(`INSERT INTO item VALUES (1, 'one')`)
    ensurePendingChangesTable(sql)

    // An unrecognized tag must not fall open to SQL NULL: that would "roll
    // back" the column by destroying the very value it exists to restore.
    appendPendingChange(sql, {
      transactionID: TX,
      physicalTableName: 'item',
      tableName: 'public.item',
      op: 'UPDATE',
      rowData: { id: 1, body: 'two' },
      oldData: { id: 1, body: 'one' },
      rowJournal: { id: 'i1', body: 'stwo' },
      oldJournal: { id: 'i1', body: 'xcorrupt' },
      newRowid: '1',
      oldRowid: '1',
      undoable: true,
    })

    expect(() => rollbackPendingChanges(sql, TX)).toThrow(/unknown value tag/)
  })

  it.each([
    ['i12junk', /corrupt integer payload/],
    ['rjunk', /corrupt real payload/],
    ['r9e999', /corrupt real payload/],
    ['i99999999999999999999', /out of int64 range/],
    ['ntrailing', /corrupt null payload/],
    ['babc', /corrupt blob payload/],
  ])('fails the rollback on a corrupt %s payload', (payload, message) => {
    const { sql } = createSqliteStorage()
    sql.exec('CREATE TABLE item (id INTEGER PRIMARY KEY, body TEXT)')
    sql.exec(`INSERT INTO item VALUES (1, 'one')`)
    ensurePendingChangesTable(sql)

    // SQLite's CAST is forgiving in exactly the wrong way: '12junk' reads as 12
    // and 'junk' as 0.0, so a corrupt payload would restore a plausible but
    // wrong value under a codec that only rejected unknown tags.
    appendPendingChange(sql, {
      transactionID: TX,
      physicalTableName: 'item',
      tableName: 'public.item',
      op: 'UPDATE',
      rowData: { id: 1, body: 'two' },
      oldData: { id: 1, body: 'one' },
      rowJournal: { id: 'i1', body: 'stwo' },
      oldJournal: { id: 'i1', body: payload },
      newRowid: '1',
      oldRowid: '1',
      undoable: true,
    })

    expect(() => rollbackPendingChanges(sql, TX)).toThrow(message)
    expect(sql.exec('SELECT body FROM item').toArray()).toEqual([{ body: 'one' }])
  })

  it('fails rather than skipping a missing non-key value in an update image', () => {
    const { sql } = createSqliteStorage()
    sql.exec('CREATE TABLE item (id INTEGER PRIMARY KEY, body TEXT)')
    sql.exec(`INSERT INTO item VALUES (1, 'two')`)
    ensurePendingChangesTable(sql)
    appendPendingChange(sql, {
      transactionID: TX,
      physicalTableName: 'item',
      tableName: 'public.item',
      op: 'UPDATE',
      rowData: { id: 1, body: 'two' },
      oldData: { id: 1, body: 'one' },
      rowJournal: { id: 'i1', body: 'stwo' },
      oldJournal: { id: 'i1' },
      newRowid: '1',
      oldRowid: '1',
      undoable: true,
    })

    expect(() => rollbackPendingChanges(sql, TX)).toThrow(/missing column\(s\): body/)
    expect(sql.exec('SELECT body FROM item').toArray()).toEqual([{ body: 'two' }])
  })
})

describe('row undo: INTEGER PRIMARY KEY DESC', () => {
  it('restores rows when the declared integer key is not the rowid', () => {
    const { sql } = createSqliteStorage()
    // SQLite's one exception: INTEGER PRIMARY KEY is a rowid alias, but adding
    // DESC makes it an ordinary indexed column with a separate rowid. Treating
    // it as the rowid would let a restored row land on a fresh rowid, and an
    // earlier change matched by its captured rowid would then find nothing.
    sql.exec('CREATE TABLE t (id INTEGER PRIMARY KEY DESC, body TEXT)')
    sql.exec(`INSERT INTO t VALUES (100, 'old'), (200, 'other')`)
    const before = sql.exec('SELECT rowid, id, body FROM t ORDER BY rowid').toArray()

    const cdc = new TransactionalCdc(sql)
    trackedWrite(sql, cdc, 't', `UPDATE t SET body = 'new' WHERE id = 100`)
    trackedWrite(sql, cdc, 't', `DELETE FROM t WHERE id = 100`)

    rollbackPendingChanges(sql, TX)
    expect(sql.exec('SELECT rowid, id, body FROM t ORDER BY rowid').toArray()).toEqual(
      before
    )
  })
})
