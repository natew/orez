import { DatabaseSync } from 'node:sqlite'

import { createSchema, string, table } from '@rocicorp/zero'
import { afterEach, describe, expect, test, vi } from 'vitest'

import { MutationRetryError } from './errors.js'
import { createSyncExecutor, handleSyncExecutorPushRequest } from './executor.js'

import type {
  ApplicationDatabase,
  ApplicationTransaction,
  EffectScheduler,
  MutatorRegistry,
} from './types.js'

const item = table('item').columns({ id: string(), value: string() }).primaryKey('id')
const schema = createSchema({ tables: [item] })

const databases: DatabaseSync[] = []

afterEach(() => {
  for (const database of databases.splice(0)) database.close()
})

function sqliteDatabase(): { database: ApplicationDatabase; sqlite: DatabaseSync } {
  const sqlite = new DatabaseSync(':memory:')
  databases.push(sqlite)
  sqlite.exec(`
    CREATE TABLE item (id TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE _zsync_log_segments (
      startVersion INTEGER PRIMARY KEY,
      endVersion INTEGER NOT NULL,
      payload TEXT NOT NULL,
      pending TEXT NOT NULL,
      captureMode INTEGER NOT NULL CHECK (captureMode IN (0, 1))
    ) WITHOUT ROWID;
    CREATE TRIGGER item_insert AFTER INSERT ON item BEGIN
      UPDATE _zsync_log_segments
      SET endVersion = endVersion + 1,
          payload = json_insert(
            payload,
            '$.transactions[#]',
            json_object(
              'version', CAST(endVersion + 1 AS TEXT),
              'changes', json_array(json_array('item', json_object('id', NEW.id)))
            )
          )
      WHERE startVersion = (SELECT MAX(startVersion) FROM _zsync_log_segments)
        AND captureMode = 0;
    END;
    CREATE TRIGGER item_update AFTER UPDATE ON item BEGIN
      UPDATE _zsync_log_segments
      SET endVersion = endVersion + 2,
          payload = json_insert(
            payload,
            '$.transactions[#]',
            json_object(
              'version', CAST(endVersion + 2 AS TEXT),
              'changes', json_array(
                json_array('item', json_object('id', OLD.id)),
                json_array('item', json_object('id', NEW.id))
              )
            )
          )
      WHERE startVersion = (SELECT MAX(startVersion) FROM _zsync_log_segments)
        AND captureMode = 0;
    END;
    CREATE TRIGGER item_delete AFTER DELETE ON item BEGIN
      UPDATE _zsync_log_segments
      SET endVersion = endVersion + 1,
          payload = json_insert(
            payload,
            '$.transactions[#]',
            json_object(
              'version', CAST(endVersion + 1 AS TEXT),
              'changes', json_array(json_array('item', json_object('id', OLD.id)))
            )
          )
      WHERE startVersion = (SELECT MAX(startVersion) FROM _zsync_log_segments)
        AND captureMode = 0;
    END;
  `)

  const applicationTransaction: ApplicationTransaction = {
    async exec(sql, params = []) {
      const result = sqlite.prepare(sql).run(...params)
      return { changes: Number(result.changes) }
    },
    async query<Row extends Record<string, unknown> = Record<string, unknown>>(
      sql: string,
      params: readonly unknown[] = []
    ): Promise<readonly Row[]> {
      return sqlite.prepare(sql).all(...params) as Row[]
    },
    async queryAst() {
      throw new Error('queryAst is not used by this fixture')
    },
  }

  const database: ApplicationDatabase = {
    async transaction<Value>(
      work: (tx: ApplicationTransaction) => Value | Promise<Value>
    ): Promise<Value> {
      sqlite.exec('BEGIN')
      try {
        const value = await work(applicationTransaction)
        sqlite.exec('COMMIT')
        return value
      } catch (error) {
        sqlite.exec('ROLLBACK')
        throw error
      }
    },
    async query<Row extends Record<string, unknown> = Record<string, unknown>>(
      sql: string,
      params: readonly unknown[] = []
    ): Promise<readonly Row[]> {
      return sqlite.prepare(sql).all(...params) as Row[]
    },
  }

  return { database, sqlite }
}

function packedPayload(sqlite: DatabaseSync) {
  const row = sqlite
    .prepare('SELECT payload FROM _zsync_log_segments ORDER BY startVersion DESC LIMIT 1')
    .get() as { payload: string }
  return JSON.parse(row.payload) as {
    format: 2
    transactions: {
      version: string
      changes: [string, Record<string, unknown>][]
    }[]
  }
}

function storedLMID(
  sqlite: DatabaseSync,
  clientGroupID = 'group-1',
  clientID = 'client-1'
) {
  return sqlite
    .prepare(
      `SELECT lastMutationID FROM _zsync_clients
       WHERE clientGroupID = ? AND clientID = ?`
    )
    .get(clientGroupID, clientID)?.lastMutationID
}

const effects: EffectScheduler = {
  async runBackground(promise) {
    await promise
  },
  report(error) {
    throw error
  },
}

function push(name: string, id = 1, clientID = 'client-1') {
  return {
    pushVersion: 1,
    schemaVersion: 1,
    clientGroupID: 'group-1',
    mutations: [
      {
        type: 'custom',
        clientID,
        id,
        name,
        args: [{}],
        timestamp: 1,
      },
    ],
  }
}

describe('sync executor', () => {
  test('packed helper capture keeps warm mutation writes bounded', async () => {
    const measureRowsWritten = async (
      lane: 'raw' | 'helper',
      logicalRows: 1 | 2 | 32
    ) => {
      const { database, sqlite } = sqliteDatabase()
      const mutators = {
        warm: async () => {},
        create: async ({ tx }) => {
          const write = async (id: string, value: string) => {
            if (lane === 'helper') {
              await tx.mutate.item.insert({ id, value })
            } else {
              await tx.dbTransaction.wrappedTransaction.exec(
                'INSERT INTO item (id, value) VALUES (?, ?)',
                [id, value]
              )
            }
          }
          for (let index = 0; index < logicalRows; index++) {
            await write(`item-${index}`, `value-${index}`)
          }
        },
      } satisfies MutatorRegistry<typeof schema>
      const executor = createSyncExecutor({ database, effects, mutators, schema })
      await executor.push(push('warm'), { userID: 'user-1' })
      const before = Number(
        sqlite.prepare('SELECT total_changes() AS total').get()!.total
      )
      await executor.push(push('create', 2), { userID: 'user-1' })
      const after = Number(sqlite.prepare('SELECT total_changes() AS total').get()!.total)
      return after - before
    }

    await expect(measureRowsWritten('raw', 1)).resolves.toBe(4)
    await expect(measureRowsWritten('helper', 1)).resolves.toBe(4)
    await expect(measureRowsWritten('helper', 2)).resolves.toBe(5)
    await expect(measureRowsWritten('raw', 32)).resolves.toBe(66)
    await expect(measureRowsWritten('helper', 32)).resolves.toBe(35)
  })

  test('helper rotation keeps the per-client LMID checkpoint', async () => {
    for (const [fillerBytes, expectedSegments] of [
      [760_000, 1],
      [790_000, 2],
    ] as const) {
      const { database, sqlite } = sqliteDatabase()
      const mutators = {
        create: async ({ tx, args }) => {
          await tx.mutate.item.insert(args)
        },
      } satisfies MutatorRegistry<typeof schema>
      const executor = createSyncExecutor({ database, effects, mutators, schema })
      await executor.push(push('create'), { userID: 'user-1' })
      const payload = packedPayload(sqlite)
      payload.transactions[0]!.changes = [['item', { id: 'x'.repeat(fillerBytes) }]]
      sqlite
        .prepare('UPDATE _zsync_log_segments SET payload = ? WHERE startVersion = 1')
        .run(JSON.stringify(payload))

      await executor.push(
        {
          clientGroupID: 'group-1',
          pushVersion: 1,
          mutations: [
            {
              type: 'custom',
              id: 2,
              clientID: 'client-1',
              name: 'create',
              args: [{ id: 'second', value: 'v2' }],
            },
          ],
        },
        { userID: 'user-1' }
      )
      expect(
        sqlite.prepare('SELECT COUNT(*) AS count FROM _zsync_log_segments').get()
      ).toEqual({ count: expectedSegments })
      expect(storedLMID(sqlite)).toBe(2)
      expect(sqlite.prepare("SELECT value FROM item WHERE id = 'second'").get()).toEqual({
        value: 'v2',
      })
    }
  })

  test('a large LMID checkpoint does not wedge the next mutation', async () => {
    const { database, sqlite } = sqliteDatabase()
    const mutators = {
      create: async ({ tx, ctx }) => {
        await tx.mutate.item.insert({
          id: `item-${ctx.mutationID}`,
          value: `value-${ctx.mutationID}`,
        })
      },
    } satisfies MutatorRegistry<typeof schema>
    const firstExecutor = createSyncExecutor({ database, effects, mutators, schema })
    await firstExecutor.push(push('create'), { userID: 'user-1' })

    const lmids: Record<string, Record<string, string>> = {
      'group-1': { 'client-1': '1' },
    }
    for (let index = 0; index < 18_000; index++) {
      lmids[`historical-group-${index.toString().padStart(5, '0')}`] = {
        [`historical-client-${index.toString().padStart(5, '0')}`]: '1',
      }
    }
    const payload = JSON.stringify({ format: 1, lmids, transactions: [] })
    expect(Buffer.byteLength(payload)).toBeGreaterThanOrEqual(768 * 1_024)
    sqlite
      .prepare(
        'UPDATE _zsync_log_segments SET endVersion = startVersion - 1, payload = ?'
      )
      .run(payload)

    const executor = createSyncExecutor({ database, effects, mutators, schema })

    await expect(executor.push(push('create', 2), { userID: 'user-1' })).resolves.toEqual(
      {
        pushResponse: {
          mutations: [{ id: { clientID: 'client-1', id: 2 }, result: {} }],
        },
      }
    )
    expect(storedLMID(sqlite)).toBe(2)
    expect(sqlite.prepare("SELECT value FROM item WHERE id = 'item-2'").get()).toEqual({
      value: 'value-2',
    })
  })

  test('format-1 migration rewrites every retained segment', async () => {
    const { database, sqlite } = sqliteDatabase()
    const mutators = {
      create: async ({ tx, ctx }) => {
        await tx.mutate.item.insert({
          id: `item-${ctx.mutationID}`,
          value: `value-${ctx.mutationID}`,
        })
      },
    } satisfies MutatorRegistry<typeof schema>
    const firstExecutor = createSyncExecutor({ database, effects, mutators, schema })
    await firstExecutor.push(push('create'), { userID: 'user-1' })

    const legacySegment = (
      versions: number[],
      lmids: Record<string, Record<string, string>>
    ) =>
      JSON.stringify({
        format: 1,
        lmids,
        transactions: versions.map((version) => ({
          version: String(version),
          changes: [['item', { id: `legacy-${version}` }]],
        })),
      })
    sqlite.prepare('DELETE FROM _zsync_log_segments').run()
    const insert = sqlite.prepare(
      `INSERT INTO _zsync_log_segments
         (startVersion, endVersion, payload, pending, captureMode)
       VALUES (?, ?, ?, '[]', 0)`
    )
    // non-active maps are rotation-time copies and must be ignored: only the
    // active segment's map is canonical at migration time.
    insert.run(1, 2, legacySegment([1, 2], { 'group-1': { 'client-1': '999' } }))
    insert.run(
      3,
      3,
      legacySegment([3], {
        'group-1': { 'client-1': '7' },
        'historical-group': { 'historical-client': '3' },
      })
    )

    const executor = createSyncExecutor({ database, effects, mutators, schema })
    await expect(executor.push(push('create', 8), { userID: 'user-1' })).resolves.toEqual(
      {
        pushResponse: {
          mutations: [{ id: { clientID: 'client-1', id: 8 }, result: {} }],
        },
      }
    )
    expect(storedLMID(sqlite)).toBe(8)
    expect(storedLMID(sqlite, 'historical-group', 'historical-client')).toBe(3)

    const segments = sqlite
      .prepare('SELECT payload FROM _zsync_log_segments ORDER BY startVersion')
      .all() as { payload: string }[]
    expect(segments).toHaveLength(2)
    const versions: string[] = []
    for (const segment of segments) {
      const payload = JSON.parse(segment.payload) as {
        format: number
        lmids?: unknown
        transactions: { version: string }[]
      }
      expect(payload.format).toBe(2)
      expect(payload.lmids).toBeUndefined()
      versions.push(...payload.transactions.map((transaction) => transaction.version))
    }
    expect(versions).toEqual(['1', '2', '3', '4'])
  })

  test('an orphaned captureMode is cleared when the executor starts', async () => {
    const { database, sqlite } = sqliteDatabase()
    const mutators = {
      create: async ({ tx, args }) => {
        await tx.mutate.item.insert(args)
      },
    } satisfies MutatorRegistry<typeof schema>
    const first = createSyncExecutor({ database, effects, mutators, schema })
    await first.push(push('create'), { userID: 'user-1' })

    // a delegated push abandoned between the capture toggle and its commit
    // leaves the column set with no writer behind it
    sqlite.exec('UPDATE _zsync_log_segments SET captureMode = 1')

    const captureMode = () =>
      sqlite
        .prepare(
          'SELECT captureMode FROM _zsync_log_segments ORDER BY startVersion DESC LIMIT 1'
        )
        .get()!.captureMode
    const totalChanges = () =>
      Number(sqlite.prepare('SELECT total_changes() AS total').get()!.total)

    const restarted = createSyncExecutor({ database, effects, mutators, schema })
    const transactionsBefore = packedPayload(sqlite).transactions.length
    await restarted.push(
      {
        clientGroupID: 'group-1',
        pushVersion: 1,
        mutations: [
          {
            type: 'custom',
            id: 2,
            clientID: 'client-1',
            name: 'create',
            args: [{ id: 'second', value: 'v2' }],
          },
        ],
      },
      { userID: 'user-1' }
    )
    expect(captureMode()).toBe(0)
    // the trigger bodies are gated on captureMode = 0, so an uncleared column
    // wrote the row while silently dropping its change envelope
    expect(sqlite.prepare("SELECT value FROM item WHERE id = 'second'").get()).toEqual({
      value: 'v2',
    })
    const appended = packedPayload(sqlite).transactions.slice(transactionsBefore)
    expect(appended.flatMap((transaction) => transaction.changes)).toEqual([
      ['item', { id: 'second' }],
    ])
    expect(storedLMID(sqlite)).toBe(2)

    // a settled start-up writes nothing
    const before = totalChanges()
    const settled = createSyncExecutor({ database, effects, mutators, schema })
    await settled.push(push('create', 2), { userID: 'user-1' })
    expect(totalChanges() - before).toBe(0)
  })

  test('a journaled transaction keeps its captureMode across a restart', async () => {
    const { database, sqlite } = sqliteDatabase()
    const mutators = {
      create: async ({ tx, args }) => {
        await tx.mutate.item.insert(args)
      },
    } satisfies MutatorRegistry<typeof schema>
    const first = createSyncExecutor({ database, effects, mutators, schema })
    await first.push(push('create'), { userID: 'user-1' })

    // the journal owns the toggle until its transaction commits or rolls back;
    // a start-up that clobbered it would strand a live writer
    sqlite.exec(`
      CREATE TABLE _orez_tx_manifest (
        seq INTEGER PRIMARY KEY AUTOINCREMENT, tx_id TEXT NOT NULL,
        owner TEXT NOT NULL DEFAULT 'default', original TEXT NOT NULL,
        snapshot TEXT);
      INSERT INTO _orez_tx_manifest (tx_id, original)
      VALUES ('live-tx', '_zsync_log_segments');
      UPDATE _zsync_log_segments SET captureMode = 1;
    `)
    const captureMode = () =>
      sqlite
        .prepare(
          'SELECT captureMode FROM _zsync_log_segments ORDER BY startVersion DESC LIMIT 1'
        )
        .get()!.captureMode

    const blocked = createSyncExecutor({ database, effects, mutators, schema })
    await expect(blocked.push(push('create', 2), { userID: 'user-1' })).rejects.toThrow(
      'packed ledger has uncommitted capture state'
    )
    expect(captureMode()).toBe(1)

    // once that transaction is gone the next start-up clears it
    sqlite.exec('DELETE FROM _orez_tx_manifest')
    const recovered = createSyncExecutor({ database, effects, mutators, schema })
    await recovered.push(
      {
        clientGroupID: 'group-1',
        pushVersion: 1,
        mutations: [
          {
            type: 'custom',
            id: 2,
            clientID: 'client-1',
            name: 'create',
            args: [{ id: 'second', value: 'v2' }],
          },
        ],
      },
      { userID: 'user-1' }
    )
    expect(captureMode()).toBe(0)
    expect(storedLMID(sqlite)).toBe(2)
  })

  test('insert conflict keeps the existing row and commits the later insert', async () => {
    const { database, sqlite } = sqliteDatabase()
    sqlite.prepare('INSERT INTO item (id, value) VALUES (?, ?)').run('a', 'original')

    const mutators = {
      converge: async ({ tx }) => {
        await tx.mutate.item.insert({ id: 'a', value: 'replacement' })
        await tx.mutate.item.insert({ id: 'b', value: 'later' })
      },
    } satisfies MutatorRegistry<typeof schema>
    const executor = createSyncExecutor({ database, effects, mutators, schema })

    await expect(executor.push(push('converge'), { userID: 'user-1' })).resolves.toEqual({
      pushResponse: {
        mutations: [{ id: { clientID: 'client-1', id: 1 }, result: {} }],
      },
    })
    expect(sqlite.prepare('SELECT * FROM item ORDER BY id').all()).toEqual([
      { id: 'a', value: 'original' },
      { id: 'b', value: 'later' },
    ])
    expect(storedLMID(sqlite)).toBe(1)
  })

  test('the push endpoint returns zero mutate response shape, not an internal wrapper', async () => {
    const { database, sqlite } = sqliteDatabase()
    let receivedClaims: unknown
    const mutators = {
      create: async ({ ctx, tx }) => {
        receivedClaims = ctx.claims
        await tx.mutate.item.insert({ id: 'a', value: 'v' })
      },
    } satisfies MutatorRegistry<typeof schema>
    const executor = createSyncExecutor({ database, effects, mutators, schema })

    const response = await handleSyncExecutorPushRequest({
      executor,
      request: new Request('https://example.test/push', {
        method: 'POST',
        body: JSON.stringify(push('create')),
      }),
      authData: { id: 'user-1', role: 'admin' },
    })

    // zero parses this with mutateResponseSchema, which accepts
    // {mutations:[...]} or {kind:'MutateResponse',...} and nothing else
    expect(await response.json()).toEqual({
      mutations: [{ id: { clientID: 'client-1', id: 1 }, result: {} }],
    })
    expect(receivedClaims).toEqual({
      userID: 'user-1',
      authData: { id: 'user-1', role: 'admin' },
    })
  })

  test('the push endpoint reports structured unsupported-version diagnostics', async () => {
    const { database, sqlite } = sqliteDatabase()
    const executor = createSyncExecutor({ database, effects, mutators: {}, schema })
    const callback = vi.fn()
    const body = { ...push('create'), pushVersion: 2 }

    const response = await handleSyncExecutorPushRequest({
      executor,
      request: new Request('https://example.test/push?appID=chat', {
        method: 'POST',
        body: JSON.stringify(body),
      }),
      authData: { id: 'user-1' },
      diagnostics: { argAllowlist: ['id'], callback },
    })

    expect(response.status).toBe(200)
    expect(callback).toHaveBeenCalledWith({
      request: expect.objectContaining({
        appID: 'chat',
        clientGroupID: 'group-1',
        mutationCount: 1,
      }),
      failure: {
        kind: 'unsupportedPushVersion',
        origin: 'response',
        reason: 'unsupportedPushVersion',
        message: null,
        status: null,
        mutationIDs: [{ id: 1, clientID: 'client-1' }],
      },
      mutationErrors: [],
    })
  })

  test('a mutation naming an inherited key is rejected, not resolved off the prototype', async () => {
    const { database, sqlite } = sqliteDatabase()
    const executor = createSyncExecutor({ database, effects, mutators: {}, schema })

    await expect(executor.push(push('toString'), { userID: 'user-1' })).resolves.toEqual({
      pushResponse: {
        mutations: [
          {
            id: { clientID: 'client-1', id: 1 },
            // an unnamed Error carries no details, and zero omits the field
            result: { error: 'app', message: 'unknown mutator: toString' },
          },
        ],
      },
    })
  })

  test('a crud payload naming an inherited key is rejected as an unknown column', async () => {
    const { database, sqlite } = sqliteDatabase()
    const mutators = {
      sneak: async ({ tx }) => {
        await tx.mutate.item.insert({ id: 'a', value: 'v', toString: 'x' } as never)
      },
    } satisfies MutatorRegistry<typeof schema>
    const executor = createSyncExecutor({ database, effects, mutators, schema })

    const result = await executor.push(push('sneak'), { userID: 'user-1' })
    expect(result.pushResponse).toMatchObject({
      mutations: [{ result: { error: 'app', message: 'unknown column: item.toString' } }],
    })
    expect(sqlite.prepare('SELECT * FROM item').all()).toEqual([])
  })

  test('raw and helper writes share one transaction while wrong helper keys fail', async () => {
    const { database, sqlite } = sqliteDatabase()
    const mutators = {
      mixed: async ({ tx }) => {
        const sql = tx.dbTransaction.wrappedTransaction
        await sql.exec('INSERT INTO item (id, value) VALUES (?, ?)', ['raw', 'v'])
        await tx.mutate.item.insert({ id: 'helper', value: 'v' })
        await sql.query('INSERT INTO item (id, value) VALUES (?, ?) RETURNING id', [
          'query-write',
          'v',
        ])
      },
      wrongAfterRaw: async ({ tx }) => {
        const sql = tx.dbTransaction.wrappedTransaction
        await sql.exec('INSERT INTO item (id, value) VALUES (?, ?)', [
          'rolled-back-raw',
          'v',
        ])
        try {
          await sql.exec('INSERT INTO item (id, value) VALUES (?, ?)', ['actual', 'v'], {
            table: 'item',
            publicTable: 'item',
            kind: 'insert',
            capture: 'exact',
            primaryKeys: [{ after: { id: 'claimed' } }],
          })
        } catch {}
        await sql.exec('INSERT INTO item (id, value) VALUES (?, ?)', [
          'after-caught-invariant',
          'v',
        ])
      },
    } satisfies MutatorRegistry<typeof schema>
    const executor = createSyncExecutor({ database, effects, mutators, schema })

    await expect(executor.push(push('mixed'), { userID: 'user-1' })).resolves.toEqual({
      pushResponse: {
        mutations: [{ id: { clientID: 'client-1', id: 1 }, result: {} }],
      },
    })
    expect(sqlite.prepare('SELECT * FROM item ORDER BY id').all()).toEqual([
      { id: 'helper', value: 'v' },
      { id: 'query-write', value: 'v' },
      { id: 'raw', value: 'v' },
    ])
    const committed = packedPayload(sqlite)
    expect(
      committed.transactions
        .flatMap((transaction) => transaction.changes)
        .map(([table, key]) => JSON.stringify([table, key]))
        .sort()
    ).toEqual(
      ['helper', 'query-write', 'raw']
        .map((id) => JSON.stringify(['item', { id }]))
        .sort()
    )
    expect(storedLMID(sqlite)).toBe(1)
    expect(
      new Set(committed.transactions.map((transaction) => transaction.version)).size
    ).toBe(committed.transactions.length)
    await expect(
      executor.push(push('wrongAfterRaw', 1, 'wrong-client'), { userID: 'user-1' })
    ).rejects.toThrow('does not match returned write set')
    expect(
      sqlite
        .prepare(
          "SELECT id FROM item WHERE id IN ('actual', 'rolled-back-raw', 'after-caught-invariant')"
        )
        .all()
    ).toEqual([])
    expect(
      sqlite.prepare("SELECT * FROM _zsync_clients WHERE clientID = 'wrong-client'").all()
    ).toEqual([])
  })

  test('metadata-less SQL remains on the trigger lane', async () => {
    const { database, sqlite } = sqliteDatabase()
    const mutators = {
      legacy: async ({ tx }) => {
        await tx.dbTransaction.wrappedTransaction.exec(
          'INSERT INTO item (id, value) VALUES (?, ?)',
          ['legacy', 'v']
        )
      },
    } satisfies MutatorRegistry<typeof schema>
    const executor = createSyncExecutor({ database, effects, mutators, schema })

    await expect(executor.push(push('legacy'), { userID: 'user-1' })).resolves.toEqual({
      pushResponse: {
        mutations: [{ id: { clientID: 'client-1', id: 1 }, result: {} }],
      },
    })
    expect(sqlite.prepare('SELECT * FROM item').all()).toEqual([
      { id: 'legacy', value: 'v' },
    ])
    expect(
      packedPayload(sqlite).transactions.flatMap((transaction) => transaction.changes)
    ).toEqual([['item', { id: 'legacy' }]])
  })

  test('triggers capture primary-key changes while helpers capture bulk and repeated keys', async () => {
    const { database, sqlite } = sqliteDatabase()
    sqlite
      .prepare('INSERT INTO item (id, value) VALUES (?, ?), (?, ?), (?, ?)')
      .run('old', 'v1', 'bulk-a', 'v2', 'bulk-b', 'v3')
    const mutators = {
      reshape: async ({ tx }) => {
        const sql = tx.dbTransaction.wrappedTransaction
        await sql.exec('UPDATE item SET id = ?, value = ? WHERE id = ?', [
          'new',
          'v4',
          'old',
        ])
        await sql.exec('UPDATE item SET value = ? WHERE id = ?', ['v5', 'new'], {
          table: 'item',
          publicTable: 'item',
          kind: 'update',
          capture: 'exact',
          primaryKeys: [{ before: { id: 'new' }, after: { id: 'new' } }],
        })
        await sql.exec('DELETE FROM item WHERE id IN (?, ?)', ['bulk-a', 'bulk-b'], {
          table: 'item',
          publicTable: 'item',
          kind: 'delete',
          capture: 'exact',
          primaryKeys: [{ before: { id: 'bulk-a' } }, { before: { id: 'bulk-b' } }],
        })
      },
    } satisfies MutatorRegistry<typeof schema>
    const executor = createSyncExecutor({ database, effects, mutators, schema })

    await expect(executor.push(push('reshape'), { userID: 'user-1' })).resolves.toEqual({
      pushResponse: {
        mutations: [{ id: { clientID: 'client-1', id: 1 }, result: {} }],
      },
    })
    expect(sqlite.prepare('SELECT * FROM item ORDER BY id').all()).toEqual([
      { id: 'new', value: 'v5' },
    ])
    expect(
      packedPayload(sqlite)
        .transactions.flatMap((transaction) => transaction.changes)
        .map(([table, key]) => JSON.stringify([table, key]))
        .sort()
    ).toEqual(
      ['bulk-a', 'bulk-b', 'new', 'old']
        .map((id) => JSON.stringify(['item', { id }]))
        .sort()
    )
  })

  test('primary-key-changing exact metadata fails before SQL reaches the database', async () => {
    const { database, sqlite } = sqliteDatabase()
    sqlite.prepare('INSERT INTO item (id, value) VALUES (?, ?)').run('old', 'v1')
    const mutators = {
      unsafe: async ({ tx }) => {
        await tx.dbTransaction.wrappedTransaction.exec(
          'UPDATE item SET id = ? WHERE id = ?',
          ['new', 'old'],
          {
            table: 'item',
            publicTable: 'item',
            kind: 'update',
            capture: 'exact',
            primaryKeys: [{ before: { id: 'old' }, after: { id: 'new' } }],
          }
        )
      },
    } satisfies MutatorRegistry<typeof schema>
    const executor = createSyncExecutor({ database, effects, mutators, schema })

    await expect(executor.push(push('unsafe'), { userID: 'user-1' })).rejects.toThrow(
      'primary-key-changing SQL must use transparent trigger capture'
    )
    expect(sqlite.prepare('SELECT * FROM item').all()).toEqual([
      { id: 'old', value: 'v1' },
    ])
  })

  test('wrapping an ordinary error keeps its structured details and name', async () => {
    const { database, sqlite } = sqliteDatabase()

    class ProfanityError extends Error {
      readonly details = { flaggedWords: ['badger'] }
      constructor() {
        super('profanity detected')
        this.name = 'ProfanityError'
      }
    }
    class EnsureError extends Error {
      constructor() {
        super('not authenticated')
        this.name = 'EnsureError'
      }
    }

    const mutators = {
      profane: async () => {
        throw new ProfanityError()
      },
      unauthed: async () => {
        throw new EnsureError()
      },
    } satisfies MutatorRegistry<typeof schema>
    const executor = createSyncExecutor({ database, effects, mutators, schema })

    const withDetails = await executor.push(push('profane'), { userID: 'user-1' })
    expect(withDetails.pushResponse).toMatchObject({
      mutations: [
        {
          result: {
            error: 'app',
            message: 'profanity detected',
            details: { flaggedWords: ['badger'] },
          },
        },
      ],
    })

    // no details payload, so the error name is the metadata zero would carry
    const named = await executor.push(push('unauthed', 2), { userID: 'user-1' })
    expect(named.pushResponse).toMatchObject({
      mutations: [
        {
          result: {
            error: 'app',
            message: 'not authenticated',
            details: { name: 'EnsureError' },
          },
        },
      ],
    })
  })

  test('an ordinary mutator error rolls back, advances the ledger, and unblocks the next id', async () => {
    const { database, sqlite } = sqliteDatabase()

    const mutators = {
      reject: async ({ tx }) => {
        await tx.mutate.item.insert({ id: 'a', value: 'rolled-back' })
        throw new Error('not authenticated')
      },
      create: async ({ tx }) => {
        await tx.mutate.item.insert({ id: 'b', value: 'later' })
      },
    } satisfies MutatorRegistry<typeof schema>
    const executor = createSyncExecutor({ database, effects, mutators, schema })

    await expect(executor.push(push('reject'), { userID: 'user-1' })).resolves.toEqual({
      pushResponse: {
        mutations: [
          {
            id: { clientID: 'client-1', id: 1 },
            result: { error: 'app', message: 'not authenticated' },
          },
        ],
      },
    })
    expect(sqlite.prepare('SELECT * FROM item').all()).toEqual([])
    expect(storedLMID(sqlite)).toBe(1)

    // the next mutation must land: a stalled ledger would reject id 2 as
    // out-of-order and the client would retry the failed mutation forever
    await expect(executor.push(push('create', 2), { userID: 'user-1' })).resolves.toEqual(
      {
        pushResponse: {
          mutations: [{ id: { clientID: 'client-1', id: 2 }, result: {} }],
        },
      }
    )
    expect(sqlite.prepare('SELECT * FROM item').all()).toEqual([
      { id: 'b', value: 'later' },
    ])
  })

  test('a retryable rejection writes nothing at all and keeps the mutation id', async () => {
    const { database, sqlite } = sqliteDatabase()

    let overBudget = true
    const mutators = {
      spend: async ({ tx }) => {
        if (overBudget) {
          throw new MutationRetryError(300_000, 'cloud spend budget exceeded', {
            error: 'cloudSpendBudgetExceeded',
          })
        }
        await tx.mutate.item.insert({ id: 'a', value: 'within budget' })
      },
    } satisfies MutatorRegistry<typeof schema>
    const executor = createSyncExecutor({ database, effects, mutators, schema })

    await expect(
      executor.push(push('spend'), { userID: 'user-1' })
    ).rejects.toMatchObject({
      name: 'MutationRetryError',
      status: 429,
      retryAfterMs: 300_000,
      details: { error: 'cloudSpendBudgetExceeded' },
    })

    // the whole point of refusing a write is that refusing is free. an
    // acknowledged rejection would move the ledger and append its lmid change
    // row, so the mechanism that exists to stop spending would itself spend.
    expect(packedPayload(sqlite)).toMatchObject({ format: 2, transactions: [] })
    expect(sqlite.prepare('SELECT COUNT(*) AS n FROM _zsync_clients').get()).toEqual({
      n: 0,
    })
    expect(sqlite.prepare('SELECT * FROM item').all()).toEqual([])

    // and the mutation is not consumed: the same id applies once the reason to
    // refuse it goes away, instead of being silently dropped forever.
    overBudget = false
    await expect(executor.push(push('spend'), { userID: 'user-1' })).resolves.toEqual({
      pushResponse: {
        mutations: [{ id: { clientID: 'client-1', id: 1 }, result: {} }],
      },
    })
    expect(sqlite.prepare('SELECT * FROM item').all()).toEqual([
      { id: 'a', value: 'within budget' },
    ])
  })

  test('a retryable rejection answers the push endpoint with 429 and Retry-After', async () => {
    const { database, sqlite } = sqliteDatabase()

    const mutators = {
      spend: async () => {
        throw new MutationRetryError(300_000, 'cloud spend budget exceeded', {
          error: 'cloudSpendBudgetExceeded',
        })
      },
    } satisfies MutatorRegistry<typeof schema>
    const executor = createSyncExecutor({ database, effects, mutators, schema })

    const response = await handleSyncExecutorPushRequest({
      executor,
      request: new Request('https://example.test/push', {
        method: 'POST',
        body: JSON.stringify(push('spend')),
      }),
      authData: { id: 'user-1' } as never,
    })

    expect(response.status).toBe(429)
    // seconds, because that is what an HTTP client reads
    expect(response.headers.get('retry-after')).toBe('300')
    await expect(response.json()).resolves.toEqual({
      error: 'cloud spend budget exceeded',
      details: { error: 'cloudSpendBudgetExceeded' },
      retryAfterMs: 300_000,
    })
    expect(packedPayload(sqlite)).toMatchObject({ format: 2, transactions: [] })
  })

  test('replay acknowledges without invoking the mutator or effects again', async () => {
    const { database, sqlite } = sqliteDatabase()
    const transaction = database.transaction.bind(database)
    let transactionOpen = false
    database.transaction = async (work) => {
      transactionOpen = true
      try {
        return await transaction(work)
      } finally {
        transactionOpen = false
      }
    }
    let mutationRuns = 0
    let effectRuns = 0
    const mutators = {
      create: async ({ tx, ctx }) => {
        mutationRuns++
        await tx.mutate.item.insert({ id: 'a', value: 'once' })
        ctx.defer(() => {
          expect(transactionOpen).toBe(false)
          effectRuns++
        })
      },
    } satisfies MutatorRegistry<typeof schema>
    const executor = createSyncExecutor({ database, effects, mutators, schema })

    await executor.push(push('create'), { userID: 'user-1' })
    const replay = await executor.push(push('create'), { userID: 'user-1' })

    expect(replay).toEqual({
      pushResponse: {
        mutations: [
          {
            id: { clientID: 'client-1', id: 1 },
            result: {
              error: 'alreadyProcessed',
              details:
                'Ignoring mutation from client-1 with ID 1 as it was already processed. Expected: 2',
            },
          },
        ],
      },
    })
    expect(mutationRuns).toBe(1)
    expect(effectRuns).toBe(1)
    expect(sqlite.prepare('SELECT * FROM item').all()).toEqual([
      { id: 'a', value: 'once' },
    ])
  })

  test('accepts cleanup mutation id zero without dispatch or acknowledgement', async () => {
    const { database, sqlite } = sqliteDatabase()
    const executor = createSyncExecutor({
      database,
      effects,
      mutators: {},
      schema,
    })

    await expect(
      executor.push(
        {
          pushVersion: 1,
          clientGroupID: 'group-1',
          mutations: [
            {
              type: 'custom',
              clientID: 'client-1',
              id: 0,
              name: '_zero_cleanupResults',
              args: [{}],
            },
          ],
        },
        { userID: 'user-1' }
      )
    ).resolves.toEqual({ pushResponse: { mutations: [] } })
  })

  test('recognizes application errors created by another package instance', async () => {
    const { database, sqlite } = sqliteDatabase()
    class ForeignMutationApplicationError extends Error {
      readonly details = { reason: 'denied' }

      constructor() {
        super('permission denied')
        this.name = 'MutationApplicationError'
      }
    }
    const executor = createSyncExecutor({
      database,
      effects,
      mutators: {
        denied: async ({ tx }) => {
          await tx.mutate.item.insert({ id: 'rolled-back', value: 'no' })
          throw new ForeignMutationApplicationError()
        },
      },
      schema,
    })

    await expect(executor.push(push('denied'), { userID: 'user-1' })).resolves.toEqual({
      pushResponse: {
        mutations: [
          {
            id: { clientID: 'client-1', id: 1 },
            result: {
              error: 'app',
              message: 'permission denied',
              details: { reason: 'denied' },
            },
          },
        ],
      },
    })
    expect(sqlite.prepare('SELECT * FROM item').all()).toEqual([])
    expect(storedLMID(sqlite)).toBe(1)
  })
})
