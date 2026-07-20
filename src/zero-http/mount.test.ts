import { DatabaseSync } from 'node:sqlite'

import { createSchema, string, table } from '@rocicorp/zero'
import { afterEach, describe, expect, test } from 'vitest'

import {
  createZeroHttpApplicationDatabase,
  createZeroHttpSyncServer,
  type ZeroHttpSyncDb,
} from './mount.js'

import type { ApplicationTransaction } from 'orez-sync-executor'

const item = table('item').columns({ id: string(), value: string() }).primaryKey('id')
const schema = createSchema({ tables: [item] })
const privateItem = table('privateItem')
  .columns({ id: string(), viewerId: string(), value: string() })
  .primaryKey('id')
const privateSchema = createSchema({ tables: [privateItem] })
const databases: DatabaseSync[] = []
const effects = {
  runBackground(promise: Promise<void>) {
    return promise
  },
  report(error: unknown) {
    throw error
  },
}

afterEach(() => {
  for (const database of databases.splice(0)) database.close()
})

function sqliteDb(sqlite: DatabaseSync): ZeroHttpSyncDb {
  return {
    exec(sql, params = []) {
      sqlite.prepare(sql).run(...params)
    },
    all(sql, params = []) {
      return sqlite.prepare(sql).all(...params)
    },
    transaction<Value>(work: () => Value): Value {
      sqlite.exec('BEGIN')
      try {
        const result = work()
        sqlite.exec('COMMIT')
        return result
      } catch (error) {
        sqlite.exec('ROLLBACK')
        throw error
      }
    },
  }
}

describe('zero-http executor mount', () => {
  test('pushes, replays once, and pulls converged insert rows', async () => {
    const sqlite = new DatabaseSync(':memory:')
    databases.push(sqlite)
    sqlite.exec('CREATE TABLE item (id TEXT PRIMARY KEY, value TEXT NOT NULL)')
    sqlite.prepare('INSERT INTO item (id, value) VALUES (?, ?)').run('a', 'original')
    const db = sqliteDb(sqlite)
    let runs = 0
    const server = createZeroHttpSyncServer({
      applicationDatabase: createZeroHttpApplicationDatabase(db),
      effects,
      schema,
      tables: ['item'],
      mutators: {
        converge: async ({ tx }) => {
          runs++
          await tx.mutate.item.insert({ id: 'a', value: 'replacement' })
          await tx.mutate.item.insert({ id: 'b', value: 'later' })
        },
        remove: async ({ tx }) => {
          await tx.mutate.item.delete({ id: 'b' })
        },
      },
    })
    const push = {
      pushVersion: 1,
      clientGroupID: 'group-1',
      mutations: [
        {
          type: 'custom',
          clientID: 'client-1',
          id: 1,
          name: 'converge',
          args: [{}],
        },
      ],
    }

    await expect(server.handlePush(push, { userID: 'user-1' })).resolves.toEqual({
      pushResponse: {
        mutations: [{ id: { clientID: 'client-1', id: 1 }, result: {} }],
      },
    })
    await expect(server.handlePush(push, { userID: 'user-1' })).resolves.toMatchObject({
      pushResponse: {
        mutations: [{ result: { error: 'alreadyProcessed' } }],
      },
    })
    expect(runs).toBe(1)
    expect(sqlite.prepare('SELECT id, value FROM item ORDER BY id').all()).toEqual([
      { id: 'a', value: 'original' },
      { id: 'b', value: 'later' },
    ])

    await expect(
      server.handlePull(
        { clientID: 'client-1', clientGroupID: 'group-1', cookie: null },
        { userID: 'user-1' }
      )
    ).resolves.toEqual({
      cookie: 2,
      lastMutationIDChanges: { 'client-1': 1 },
      rowsPatch: [
        { op: 'clear' },
        { op: 'put', tableName: 'item', value: { id: 'a', value: 'original' } },
        { op: 'put', tableName: 'item', value: { id: 'b', value: 'later' } },
      ],
    })

    await expect(
      server.handlePush(
        {
          ...push,
          mutations: [{ ...push.mutations[0]!, id: 2, name: 'remove' }],
        },
        { userID: 'user-1' }
      )
    ).resolves.toEqual({
      pushResponse: {
        mutations: [{ id: { clientID: 'client-1', id: 2 }, result: {} }],
      },
    })
    expect(sqlite.prepare('SELECT id, value FROM item ORDER BY id').all()).toEqual([
      { id: 'a', value: 'original' },
    ])
    await expect(
      server.handlePull(
        { clientID: 'client-1', clientGroupID: 'group-1', cookie: 2 },
        { userID: 'user-1' }
      )
    ).resolves.toEqual({
      cookie: 4,
      lastMutationIDChanges: { 'client-1': 2 },
      rowsPatch: [{ op: 'del', tableName: 'item', id: { id: 'b' } }],
    })
  })

  test('emits put and del when a row changes its own visibility fields', async () => {
    const sqlite = new DatabaseSync(':memory:')
    databases.push(sqlite)
    sqlite.exec(
      'CREATE TABLE privateItem (id TEXT PRIMARY KEY, viewerId TEXT NOT NULL, value TEXT NOT NULL)'
    )
    sqlite
      .prepare('INSERT INTO privateItem (id, viewerId, value) VALUES (?, ?, ?)')
      .run('private-1', 'user-1', 'secret')
    const db = sqliteDb(sqlite)
    const server = createZeroHttpSyncServer({
      applicationDatabase: createZeroHttpApplicationDatabase(db),
      effects,
      schema: privateSchema,
      tables: ['privateItem'],
      mutators: {},
      visible: (_table, userID) => ({
        where: 'privateItem.viewerId = ?',
        params: [userID],
      }),
    })

    const user1 = (await server.handlePull(
      { clientID: 'c1', clientGroupID: 'g1', cookie: null },
      { userID: 'user-1' }
    )) as { cookie: number; rowsPatch: unknown[] }
    const user2 = (await server.handlePull(
      { clientID: 'c2', clientGroupID: 'g2', cookie: null },
      { userID: 'user-2' }
    )) as { cookie: number; rowsPatch: unknown[] }
    expect(user1.rowsPatch).toEqual([
      { op: 'clear' },
      {
        op: 'put',
        tableName: 'privateItem',
        value: { id: 'private-1', viewerId: 'user-1', value: 'secret' },
      },
    ])
    expect(user2.rowsPatch).toEqual([{ op: 'clear' }])

    db.exec('UPDATE privateItem SET viewerId = ? WHERE id = ?', ['user-2', 'private-1'])

    await expect(
      server.handlePull(
        { clientID: 'c1', clientGroupID: 'g1', cookie: user1.cookie },
        { userID: 'user-1' }
      )
    ).resolves.toEqual({
      cookie: user1.cookie + 2,
      lastMutationIDChanges: { c1: 0 },
      rowsPatch: [{ op: 'del', tableName: 'privateItem', id: { id: 'private-1' } }],
    })
    await expect(
      server.handlePull(
        { clientID: 'c2', clientGroupID: 'g2', cookie: user2.cookie },
        { userID: 'user-2' }
      )
    ).resolves.toEqual({
      cookie: user2.cookie + 2,
      lastMutationIDChanges: { c2: 0 },
      rowsPatch: [
        {
          op: 'put',
          tableName: 'privateItem',
          value: { id: 'private-1', viewerId: 'user-2', value: 'secret' },
        },
      ],
    })
  })

  test('starts above a populated legacy cookie exactly once', async () => {
    const sqlite = new DatabaseSync(':memory:')
    databases.push(sqlite)
    sqlite.exec('CREATE TABLE item (id TEXT PRIMARY KEY, value TEXT NOT NULL)')
    sqlite.exec(
      'CREATE TABLE legacy_group_cookies (clientGroupID TEXT PRIMARY KEY, cookie INTEGER NOT NULL)'
    )
    sqlite
      .prepare('INSERT INTO legacy_group_cookies (clientGroupID, cookie) VALUES (?, ?)')
      .run('legacy-group', 4000)
    const db = sqliteDb(sqlite)
    const options = {
      applicationDatabase: createZeroHttpApplicationDatabase(db),
      effects,
      schema,
      tables: ['item'],
      mutators: {},
      initialCookie: async (transaction: ApplicationTransaction) => {
        const rows = await transaction.query(
          'SELECT COALESCE(MAX(cookie), 0) AS cookie FROM legacy_group_cookies'
        )
        return Number(rows[0]!.cookie)
      },
    }
    const firstServer = createZeroHttpSyncServer(options)

    await expect(
      firstServer.handlePull(
        { clientID: 'legacy-client', clientGroupID: 'legacy-group', cookie: 4000 },
        { userID: 'user-1' }
      )
    ).resolves.toEqual({
      cookie: 4001,
      lastMutationIDChanges: { 'legacy-client': 0 },
      rowsPatch: [{ op: 'clear' }],
    })

    const restarted = createZeroHttpSyncServer(options)
    await expect(
      restarted.handlePull(
        { clientID: 'legacy-client', clientGroupID: 'legacy-group', cookie: 4001 },
        { userID: 'user-1' }
      )
    ).resolves.toEqual({ cookie: 4001, unchanged: true })
  })

  test('keeps pull, push, invalidate, and prune on one transaction queue', async () => {
    const sqlite = new DatabaseSync(':memory:')
    databases.push(sqlite)
    sqlite.exec('CREATE TABLE item (id TEXT PRIMARY KEY, value TEXT NOT NULL)')
    const db = sqliteDb(sqlite)
    let tail = Promise.resolve()
    let active = 0
    let maxActive = 0
    const applicationDatabase = createZeroHttpApplicationDatabase(db, async (work) => {
      const previous = tail
      let release = () => {}
      tail = new Promise<void>((resolve) => {
        release = resolve
      })
      await previous
      active++
      maxActive = Math.max(maxActive, active)
      sqlite.exec('BEGIN')
      try {
        await Promise.resolve()
        const result = await work()
        sqlite.exec('COMMIT')
        return result
      } catch (error) {
        sqlite.exec('ROLLBACK')
        throw error
      } finally {
        active--
        release()
      }
    })
    const server = createZeroHttpSyncServer({
      applicationDatabase,
      effects,
      schema,
      tables: ['item'],
      retainChanges: 1,
      mutators: {
        create: async ({ tx }) => {
          await tx.mutate.item.insert({ id: 'queued', value: 'once' })
        },
      },
    })
    const initial = (await server.handlePull(
      { clientID: 'client-1', clientGroupID: 'group-1', cookie: null },
      { userID: 'user-1' }
    )) as { cookie: number }

    await Promise.all([
      server.handlePull(
        { clientID: 'client-1', clientGroupID: 'group-1', cookie: initial.cookie },
        { userID: 'user-1' }
      ),
      server.handlePush(
        {
          pushVersion: 1,
          clientGroupID: 'group-1',
          mutations: [
            {
              type: 'custom',
              clientID: 'client-1',
              id: 1,
              name: 'create',
              args: [{}],
            },
          ],
        },
        { userID: 'user-1' }
      ),
      server.invalidate(),
      server.watermark(),
    ])

    expect(maxActive).toBe(1)
    expect(sqlite.prepare('SELECT id, value FROM item').all()).toEqual([
      { id: 'queued', value: 'once' },
    ])
  })
})
