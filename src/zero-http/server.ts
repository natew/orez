import { createServer, type IncomingMessage, type ServerResponse } from 'node:http'
import { DatabaseSync } from 'node:sqlite'

import { MutationApplicationError, type MutatorRegistry } from 'orez-sync-executor'

import { zeroHttpFixtureSchema } from './fixture-schema.js'
import {
  createZeroHttpApplicationDatabase,
  createZeroHttpSyncServer,
  type ZeroHttpSyncDb,
} from './mount.js'

import type { AddressInfo } from 'node:net'

export type Row = Record<string, string>

type TableName = 'user' | 'project' | 'member'
type Seed = { user?: Row[]; project?: Row[]; member?: Row[] }

const tables = {
  user: {
    physical: 'user_record',
    columns: { id: 'user_id', name: 'display_name' },
  },
  project: {
    physical: 'project_record',
    columns: { id: 'project_id', ownerId: 'owner_id', name: 'project_name' },
  },
  member: {
    physical: 'project_member',
    columns: { id: 'member_id', projectId: 'project_id', userId: 'user_id' },
  },
} as const

const effects = {
  runBackground(promise: Promise<void>) {
    return promise
  },
  report(error: unknown) {
    throw error
  },
}

function createDatabase(sqlite: DatabaseSync): ZeroHttpSyncDb {
  return {
    exec(sql, params = []) {
      sqlite.prepare(sql).run(...(params as never[]))
    },
    all(sql, params = []) {
      return sqlite.prepare(sql).all(...(params as never[])) as Record<string, unknown>[]
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

function createMutators(): MutatorRegistry<typeof zeroHttpFixtureSchema> {
  return {
    'project|create': async ({ tx, args, ctx }) => {
      const value = args as { id: string; ownerId: string; name: string }
      if (value.ownerId !== ctx.claims.userID) {
        throw new MutationApplicationError('forbidden')
      }
      await tx.mutate.project.insert(value)
    },
    'project|rename': async ({ tx, args, ctx }) => {
      const value = args as { id: string; name: string }
      const rows = Array.from(
        await tx.dbTransaction.query(
          'SELECT owner_id AS ownerID FROM project_record WHERE project_id = ?',
          [value.id]
        )
      )
      if (rows.length === 0) throw new MutationApplicationError('not-found')
      if (rows[0]!.ownerID !== ctx.claims.userID) {
        throw new MutationApplicationError('forbidden')
      }
      await tx.mutate.project.update(value)
    },
    'member|add': async ({ tx, args, ctx }) => {
      const value = args as { id: string; projectId: string; userId: string }
      const rows = Array.from(
        await tx.dbTransaction.query(
          'SELECT project_id FROM project_record WHERE project_id = ? AND owner_id = ?',
          [value.projectId, ctx.claims.userID]
        )
      )
      if (rows.length === 0) throw new MutationApplicationError('forbidden')
      await tx.mutate.member.insert(value)
    },
    'member|touch': async ({ tx, args, ctx }) => {
      const value = args as { id: string }
      const rows = Array.from(
        await tx.dbTransaction.query(
          `SELECT m.project_id AS projectId, m.user_id AS userId
           FROM project_member m
           JOIN project_record p ON p.project_id = m.project_id
           WHERE m.member_id = ? AND p.owner_id = ?`,
          [value.id, ctx.claims.userID]
        )
      )
      if (rows.length === 0) throw new MutationApplicationError('forbidden')
      await tx.mutate.member.update({
        id: value.id,
        projectId: String(rows[0]!.projectId),
        userId: String(rows[0]!.userId),
      })
    },
    'member|remove': async ({ tx, args, ctx }) => {
      const value = args as { id: string }
      const members = Array.from(
        await tx.dbTransaction.query(
          'SELECT project_id AS projectId FROM project_member WHERE member_id = ?',
          [value.id]
        )
      )
      if (members.length === 0) throw new MutationApplicationError('not-found')
      const projects = Array.from(
        await tx.dbTransaction.query(
          'SELECT project_id FROM project_record WHERE project_id = ? AND owner_id = ?',
          [members[0]!.projectId, ctx.claims.userID]
        )
      )
      if (projects.length === 0) throw new MutationApplicationError('forbidden')
      await tx.mutate.member.delete(value)
    },
  }
}

export async function startZeroHttpServer(opts?: { seed?: Seed }): Promise<{
  url: string
  version(): Promise<number>
  rows(table: string): Row[]
  close(): Promise<void>
}> {
  const sqlite = new DatabaseSync(':memory:')
  initializeApplicationTables(sqlite, opts?.seed)
  const db = createDatabase(sqlite)
  const sync = createZeroHttpSyncServer({
    applicationDatabase: createZeroHttpApplicationDatabase(db),
    effects,
    schema: zeroHttpFixtureSchema,
    tables: ['user', 'project', 'member'],
    mutators: createMutators(),
    visible(table, userID) {
      if (table === 'user') {
        return {
          where: 'user_record.user_id = ?',
          params: [userID],
        }
      }
      if (table === 'project') {
        return {
          where: `project_record.owner_id = ? OR EXISTS (
            SELECT 1 FROM project_member m
            WHERE m.project_id = project_record.project_id AND m.user_id = ?
          )`,
          params: [userID, userID],
        }
      }
      return {
        where: `EXISTS (
          SELECT 1 FROM project_record p
          WHERE p.project_id = project_member.project_id
            AND (p.owner_id = ? OR EXISTS (
              SELECT 1 FROM project_member viewer
              WHERE viewer.project_id = p.project_id AND viewer.user_id = ?
            ))
        )`,
        params: [userID, userID],
      }
    },
    visibilityInvalidation: {
      capture: { member: ['projectId', 'userId'] },
      shouldReset({ changes, userID }) {
        return changes.some((change) => {
          if (change.table !== 'member') return false
          const before = change.before
          const after = change.after
          return (
            (before?.userId === userID &&
              (after?.userId !== before.userId ||
                after?.projectId !== before.projectId)) ||
            (after?.userId === userID &&
              (before?.userId !== after.userId || before?.projectId !== after.projectId))
          )
        })
      },
    },
  })
  await sync.ready()

  const server = createServer(async (req, res) => {
    try {
      if (req.method !== 'POST') {
        sendJSON(res, 404, { error: 'not found' })
        return
      }
      const userID = authenticate(req, sqlite)
      if (!userID) {
        sendJSON(res, 401, { error: 'unauthorized' })
        return
      }
      const path = new URL(req.url || '/', 'http://127.0.0.1').pathname
      const body = await readJSON(req)
      if (path === '/push') {
        sendJSON(res, 200, await sync.handlePush(body, { userID }))
        return
      }
      if (path === '/pull') {
        sendJSON(res, 200, await sync.handlePull(body, { userID }))
        return
      }
      sendJSON(res, 404, { error: 'not found' })
    } catch (error) {
      const status =
        typeof error === 'object' && error !== null && 'status' in error
          ? Number(error.status)
          : error instanceof SyntaxError
            ? 400
            : 500
      sendJSON(res, status, {
        error: error instanceof Error ? error.message : String(error),
      })
    }
  })

  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', () => {
      server.off('error', reject)
      resolve()
    })
  })
  const address = server.address() as AddressInfo
  return {
    url: `http://127.0.0.1:${address.port}`,
    version: () => sync.watermark(),
    rows: (table) => rowsForTable(sqlite, table),
    close: () =>
      new Promise((resolve, reject) => {
        server.close((error) => {
          sqlite.close()
          error ? reject(error) : resolve()
        })
      }),
  }
}

function initializeApplicationTables(sqlite: DatabaseSync, seed?: Seed): void {
  sqlite.exec(`CREATE TABLE user_record (
    user_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL
  )`)
  sqlite.exec(`CREATE TABLE project_record (
    project_id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    project_name TEXT NOT NULL
  )`)
  sqlite.exec(`CREATE TABLE project_member (
    member_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    user_id TEXT NOT NULL
  )`)
  for (const table of Object.keys(tables) as TableName[]) {
    const config = tables[table]
    for (const row of seed?.[table] ?? []) {
      const columns = Object.keys(row) as Array<keyof typeof config.columns>
      const physicalColumns = columns.map((column) => config.columns[column])
      sqlite
        .prepare(
          `INSERT INTO "${config.physical}" (${physicalColumns
            .map((column) => `"${column}"`)
            .join(', ')}) VALUES (${columns.map(() => '?').join(', ')})`
        )
        .run(...columns.map((column) => row[column as string]))
    }
  }
}

function authenticate(req: IncomingMessage, sqlite: DatabaseSync): string | null {
  const header = req.headers.authorization
  if (!header?.startsWith('Bearer token-')) return null
  const userID = header.slice('Bearer token-'.length)
  const row = sqlite
    .prepare('SELECT user_id FROM user_record WHERE user_id = ?')
    .get(userID)
  return row ? userID : null
}

function rowsForTable(sqlite: DatabaseSync, table: string): Row[] {
  if (!(table in tables)) return []
  const config = tables[table as TableName]
  const logicalColumns = Object.entries(config.columns)
  return sqlite
    .prepare(`SELECT * FROM "${config.physical}"`)
    .all()
    .map((row) =>
      Object.fromEntries(
        logicalColumns.map(([logical, physical]) => [logical, String(row[physical])])
      )
    )
}

async function readJSON(req: IncomingMessage): Promise<unknown> {
  let body = ''
  for await (const chunk of req) body += chunk
  return body ? JSON.parse(body) : {}
}

function sendJSON(res: ServerResponse, status: number, body: unknown): void {
  res.writeHead(status, { 'content-type': 'application/json' })
  res.end(JSON.stringify(body))
}
