// differential oracle: runs the TypeScript reference core
// (src/sync-server/sync-server.ts) on a shared operation trace and emits the
// pull responses as JSON, so the Rust differential test can compare its own run
// of the SAME trace against this ground truth.
//
// run with: bun crates/sync-core/ts-oracle/run-oracle.ts <trace.json>
// the trace is a JSON array of ops; the runner maintains a per-client mutation
// id counter and per-client cookie EXACTLY as the Rust runner does, so both
// stay in lockstep. output on stdout: a JSON array of pull responses in order.
import { Database } from 'bun:sqlite'

import {
  createSyncServer,
  MutationAppError,
  type SyncDb,
  type SyncTables,
} from '../../../src/sync-server/sync-server.ts'

function bunSqliteDb(sqlite: Database): SyncDb {
  return {
    exec(sql, params = []) {
      sqlite.query(sql).run(...(params as never[]))
    },
    all(sql, params = []) {
      return sqlite.query(sql).all(...(params as never[])) as Record<string, unknown>[]
    },
    transaction<T>(fn: () => T): T {
      sqlite.run('BEGIN')
      try {
        const result = fn()
        sqlite.run('COMMIT')
        return result
      } catch (error) {
        sqlite.run('ROLLBACK')
        throw error
      }
    },
  }
}

const TABLES: SyncTables = {
  item: {
    columns: {
      id: 'string',
      label: 'string',
      rank: 'number',
      done: 'boolean',
      meta: 'json',
    },
    primaryKey: ['id'],
  },
}

function mutate(tx: SyncDb, name: string, args: unknown, _ctx: { userID: string }) {
  const a = args as Record<string, unknown>
  if (name === 'item.put') {
    tx.exec(
      `INSERT INTO item (id, label, rank, done, meta) VALUES (?, ?, ?, ?, ?)
       ON CONFLICT (id) DO UPDATE SET label = excluded.label, rank = excluded.rank,
       done = excluded.done, meta = excluded.meta`,
      [
        a.id,
        a.label,
        a.rank,
        a.done ? 1 : 0,
        a.meta === null ? null : JSON.stringify(a.meta),
      ]
    )
    return
  }
  if (name === 'item.del') {
    tx.exec(`DELETE FROM item WHERE id = ?`, [a.id])
    return
  }
  if (name === 'item.reject') {
    tx.exec(`INSERT INTO item (id, label, rank, done) VALUES ('rejected', 'x', 0, 0)`)
    throw new MutationAppError('nope')
  }
  throw new Error(`unknown mutator ${name}`)
}

type Op = Record<string, any>

const tracePath = process.argv[2]
if (!tracePath) throw new Error('usage: run-oracle.ts <trace.json>')
const trace = JSON.parse(await Bun.file(tracePath).text()) as Op[]

const sqlite = new Database(':memory:')
const db = bunSqliteDb(sqlite)
db.exec(
  `CREATE TABLE item (id TEXT PRIMARY KEY, label TEXT NOT NULL,
   rank REAL NOT NULL, done INTEGER NOT NULL, meta TEXT)`
)
const sync = createSyncServer({ db, tables: TABLES, mutate })

const nextId: Record<string, number> = {}
const cookies: Record<string, number | null> = {}
const pulls: unknown[] = []

for (const op of trace) {
  switch (op.op) {
    case 'put':
    case 'del':
    case 'reject': {
      const client = op.client as string
      const id = (nextId[client] = (nextId[client] ?? 0) + 1)
      const name =
        op.op === 'put' ? 'item.put' : op.op === 'del' ? 'item.del' : 'item.reject'
      const args =
        op.op === 'put'
          ? { id: op.item, label: op.label, rank: op.rank, done: op.done, meta: op.meta }
          : op.op === 'del'
            ? { id: op.item }
            : {}
      sync.handlePush(
        {
          clientGroupID: 'g1',
          mutations: [
            { type: 'custom', id, clientID: client, name, args: [args], timestamp: 0 },
          ],
          pushVersion: 1,
          requestID: 'r',
        },
        'u1'
      )
      break
    }
    case 'upstream':
      db.exec(op.sql as string)
      break
    case 'invalidate':
      sync.invalidate()
      break
    case 'pull': {
      const client = op.client as string
      const cookie = cookies[client] ?? null
      const resp = sync.handlePull(
        { clientID: client, clientGroupID: 'g1', cookie },
        'u1'
      ) as {
        cookie: number
      }
      cookies[client] = resp.cookie
      pulls.push(resp)
      break
    }
    default:
      throw new Error(`unknown op ${op.op}`)
  }
}

process.stdout.write(JSON.stringify(pulls))
