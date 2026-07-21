import { DatabaseSync } from 'node:sqlite'

import { Zero } from '@rocicorp/zero'
import { afterEach, expect, test } from 'vitest'

import { zeroHttpFixtureMutators, zeroHttpFixtureSchema } from './fixture-schema.js'
import {
  createZeroHttpApplicationDatabase,
  createZeroHttpMount,
  createZeroHttpSyncServer,
  ZeroHttpRequestError,
  type ZeroHttpSyncDb,
} from './mount.js'
import { installHttpPullTransport } from './transport.js'

const ORIGIN = 'https://orez-client-conformance.local'
const databases: DatabaseSync[] = []
const zeros: Array<{ close(): Promise<unknown> }> = []
const transports: Array<{ uninstall(): void }> = []

afterEach(async () => {
  while (zeros.length) await zeros.pop()?.close()
  while (transports.length) transports.pop()?.uninstall()
  while (databases.length) databases.pop()?.close()
})

// the client always ships its desired queries and the mount's gotQueries ack
// is authoritative — a mount that never acked would leave every query stuck
// short of 'complete'.
test('stock Zero converges through the Orez client and mount halves', async () => {
  const sqlite = new DatabaseSync(':memory:')
  databases.push(sqlite)
  sqlite.exec(`
    CREATE TABLE user_record (
      user_id TEXT PRIMARY KEY,
      display_name TEXT NOT NULL
    );
    CREATE TABLE project_record (
      project_id TEXT PRIMARY KEY,
      owner_id TEXT NOT NULL,
      project_name TEXT NOT NULL
    );
    CREATE TABLE project_member (
      member_id TEXT PRIMARY KEY,
      project_id TEXT NOT NULL,
      user_id TEXT NOT NULL
    );
    INSERT INTO user_record VALUES ('u1', 'ada');
    INSERT INTO project_record VALUES ('p1', 'u1', 'first');
  `)
  const db: ZeroHttpSyncDb = {
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
  const server = createZeroHttpSyncServer({
    applicationDatabase: createZeroHttpApplicationDatabase(db),
    effects: {
      runBackground(promise) {
        return promise
      },
      report(error) {
        throw error
      },
    },
    schema: zeroHttpFixtureSchema,
    tables: ['user', 'project', 'member'],
    mutators: {
      'project|create': async ({ tx, args }) => {
        await tx.mutate.project.insert(
          args as { id: string; ownerId: string; name: string }
        )
      },
    },
  })
  const mount = createZeroHttpMount({
    pathPrefix: '/sync/',
    server: () => server,
    authenticate: () => ({ id: 'u1' }),
  })
  const fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
    const route = mount.match(new URL(String(input)).pathname)
    if (!route) return new Response('not found', { status: 404 })
    try {
      const response = await mount.handle(route, JSON.parse(String(init?.body)), {
        id: 'u1',
      })
      return Response.json(response)
    } catch (error) {
      if (error instanceof ZeroHttpRequestError) {
        return Response.json({ error: error.message }, { status: error.status })
      }
      throw error
    }
  }
  const transport = installHttpPullTransport({
    origin: ORIGIN,
    pullOrigin: `${ORIGIN}/sync/app`,
    pushOrigin: `${ORIGIN}/sync/app`,
    fetch: fetch as typeof globalThis.fetch,
  })
  transports.push(transport)
  const zero = new Zero({
    server: ORIGIN,
    userID: 'u1',
    auth: 'token-u1',
    schema: zeroHttpFixtureSchema,
    mutators: zeroHttpFixtureMutators,
    kvStore: 'mem',
    storageKey: 'orez-client-mount-conformance',
  })
  zeros.push(zero)

  const view = zero.query.project.materialize()
  let resultType = 'unknown'
  const stopListening = view.addListener((_data, type) => {
    resultType = String(type)
  })
  await eventually(() => {
    expect(view.data.map((project) => project.name)).toEqual(['first'])
    expect(resultType).toBe('complete')
  })
  stopListening()

  const mutation = zero.mutate.project.create({
    id: 'p2',
    ownerId: 'u1',
    name: 'second',
  })
  await mutation.client
  await mutation.server
  await eventually(() =>
    expect(view.data.map((project) => project.name).sort()).toEqual(['first', 'second'])
  )
  expect(
    sqlite.prepare('SELECT project_name FROM project_record ORDER BY project_id').all()
  ).toEqual([{ project_name: 'first' }, { project_name: 'second' }])
  view.destroy()
})

async function eventually(assertion: () => void | Promise<void>, timeout = 5_000) {
  const started = Date.now()
  let lastError: unknown
  while (Date.now() - started < timeout) {
    try {
      await assertion()
      return
    } catch (error) {
      lastError = error
      await new Promise((resolve) => setTimeout(resolve, 10))
    }
  }
  throw lastError
}
