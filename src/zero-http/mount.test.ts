import { DatabaseSync } from 'node:sqlite'

import { createSchema, string, table } from '@rocicorp/zero'
import { afterEach, describe, expect, test } from 'vitest'

import {
  createZeroHttpApplicationDatabase,
  createZeroHttpSyncServer,
  type ZeroHttpSyncDb,
} from './mount.js'

const item = table('item').columns({ id: string(), value: string() }).primaryKey('id')
const schema = createSchema({ tables: [item] })
const databases: DatabaseSync[] = []

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
      db,
      schema,
      tables: {
        item: {
          columns: { id: 'string', value: 'string' },
          primaryKey: ['id'],
        },
      },
      mutators: {
        converge: async ({ tx }) => {
          runs++
          await tx.mutate.item.insert({ id: 'a', value: 'replacement' })
          await tx.mutate.item.insert({ id: 'b', value: 'later' })
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
  })
})
