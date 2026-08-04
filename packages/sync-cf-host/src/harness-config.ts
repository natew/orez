import {
  MutationApplicationError,
  MutationRetryError,
  registerMutators,
  type JsonValue,
} from 'orez-sync-executor/core'
import { defineStreamingFields } from 'orez-sync-executor/realtime'

import { queries } from '../../../harness/src/query-resolver.mjs'
import { harnessSchema, harnessStreaming } from './harness-schema.js'
import { type SyncHostConfig, type SyncHostEnv, type SyncSql } from './index.js'

import type { AnyQueryRegistry, Schema } from '@rocicorp/zero'

const DDL = [
  'CREATE TABLE IF NOT EXISTS "user" (id TEXT PRIMARY KEY, name TEXT NOT NULL)',
  'CREATE TABLE IF NOT EXISTS project (id TEXT PRIMARY KEY, "ownerId" TEXT NOT NULL, name TEXT NOT NULL)',
  'CREATE TABLE IF NOT EXISTS member (id TEXT PRIMARY KEY, "projectId" TEXT NOT NULL, "userId" TEXT NOT NULL)',
  `CREATE TABLE IF NOT EXISTS task (
    id TEXT PRIMARY KEY,
    "projectId" TEXT NOT NULL,
    title TEXT NOT NULL,
    rank REAL NOT NULL,
    done INTEGER NOT NULL,
    meta TEXT,
    "dueAt" INTEGER
  )`,
  `CREATE TABLE IF NOT EXISTS message (
    id TEXT PRIMARY KEY,
    "serverId" TEXT NOT NULL,
    "channelId" TEXT NOT NULL,
    "creatorId" TEXT NOT NULL,
    content TEXT NOT NULL,
    type TEXT NOT NULL,
    "createdAt" INTEGER NOT NULL,
    "order" TEXT NOT NULL,
    meta TEXT
  )`,
]

function authenticateHarness(request: Request) {
  const userID = request.headers.get('authorization')?.match(/^Bearer token-(.+)$/)?.[1]
  return userID ? { userID } : null
}

function mulberry32(seed: number) {
  let value = seed
  return () => {
    value |= 0
    value = (value + 0x6d2b79f5) | 0
    let mixed = Math.imul(value ^ (value >>> 15), 1 | value)
    mixed = (mixed + Math.imul(mixed ^ (mixed >>> 7), 61 | mixed)) ^ mixed
    return ((mixed ^ (mixed >>> 14)) >>> 0) / 4_294_967_296
  }
}

function seedRows() {
  const random = mulberry32(1)
  const pick = <Value>(values: Value[]) => values[Math.floor(random() * values.length)]!
  const user = Array.from({ length: 8 }, (_, index) => ({
    id: `u${index}`,
    name:
      pick(['ann', 'bob 🌵', 'çelik', 'dee', 'evan fix', 'frida', 'gus', 'hana']) +
      ` ${index}`,
  }))
  const project = Array.from({ length: 12 }, (_, index) => ({
    id: `p${index}`,
    ownerId: `u${index % user.length}`,
    name: pick(['alpha', 'fixup', 'Zenith', 'delta x', 'ütopia', 'omega']) + ` ${index}`,
  }))
  const member: Array<{ id: string; projectId: string; userId: string }> = []
  let memberID = 0
  for (const row of project) {
    const count = 1 + Math.floor(random() * 3)
    for (let index = 0; index < count; index++) {
      member.push({
        id: `m${memberID++}`,
        projectId: row.id,
        userId: `u${Math.floor(random() * user.length)}`,
      })
    }
  }
  const metas = [
    null,
    { tags: ['a', 'b'], depth: { n: 1 } },
    { emoji: '✅', list: [1, 2.5, -3] },
    { s: 'plain' },
    [1, 'two', null],
    'scalar string',
    42.5,
    true,
  ]
  const task = Array.from({ length: 48 }, (_, index) => ({
    id: `t${index}`,
    projectId: `p${Math.floor(random() * 10)}`,
    title:
      pick([
        'fix login',
        'polish ux',
        'refactor sync',
        'fix flaky test',
        'ship it 🚀',
        'triage',
      ]) + ` ${index}`,
    rank: Math.round((random() * 20 - 4) * 100) / 100,
    done: random() > 0.6,
    meta: pick(metas),
    dueAt:
      random() > 0.3 ? 1_750_000_000_000 + Math.floor(random() * 10_000_000_000) : null,
  }))
  return { user, project, member, task }
}

function initializeHarness(sql: SyncSql): void {
  for (const statement of DDL) sql.exec(statement)
  const [{ count }] = sql.query<{ count: number }>(
    'SELECT COUNT(*) AS count FROM project'
  )
  if (Number(count) > 0) return

  const seed = seedRows()
  for (const row of seed.user) {
    sql.exec('INSERT INTO "user" (id, name) VALUES (?, ?)', [row.id, row.name])
  }
  for (const row of seed.project) {
    sql.exec('INSERT INTO project (id, "ownerId", name) VALUES (?, ?, ?)', [
      row.id,
      row.ownerId,
      row.name,
    ])
  }
  for (const row of seed.member) {
    sql.exec('INSERT INTO member (id, "projectId", "userId") VALUES (?, ?, ?)', [
      row.id,
      row.projectId,
      row.userId,
    ])
  }
  for (const row of seed.task) {
    sql.exec(
      'INSERT INTO task (id, "projectId", title, rank, done, meta, "dueAt") VALUES (?, ?, ?, ?, ?, ?, ?)',
      [
        row.id,
        row.projectId,
        row.title,
        row.rank,
        row.done ? 1 : 0,
        row.meta === null ? null : JSON.stringify(row.meta),
        row.dueAt,
      ]
    )
  }
}

const harnessMutators = registerMutators({
  async 'project.create'({ tx, args }) {
    const sql = tx.dbTransaction.wrappedTransaction
    const value = args as { id: string; ownerId: string; name: string }
    const exists = await sql.query('SELECT 1 FROM project WHERE id = ?', [value.id])
    if (exists.length > 0) throw new MutationApplicationError('exists')
    await tx.mutate.project.insert(value)
  },
  async 'project.rename'({ tx, args }) {
    const value = args as { id: string; name: string }
    await tx.mutate.project.update(value)
  },
  async 'project.delete'({ tx, args }) {
    const value = args as { id: string }
    await tx.mutate.project.delete(value)
  },
  async 'member.add'({ tx, args }) {
    const value = args as { id: string; projectId: string; userId: string }
    await tx.mutate.member.insert(value)
  },
  async 'member.remove'({ tx, args }) {
    const value = args as { id: string }
    await tx.mutate.member.delete(value)
  },
  async 'task.create'({ tx, args }) {
    const value = args as {
      id: string
      projectId: string
      title: string
      rank: number
      done: boolean
      meta?: JsonValue
      dueAt?: number
    }
    await tx.mutate.task.insert(value)
  },
  async 'task.toggle'({ tx, args }) {
    const sql = tx.dbTransaction.wrappedTransaction
    const value = args as { id: string }
    const rows = await sql.query<{ done: number }>('SELECT done FROM task WHERE id = ?', [
      value.id,
    ])
    if (rows.length === 0) throw new MutationApplicationError('not-found')
    await tx.mutate.task.update({ id: value.id, done: rows[0]!.done === 0 })
  },
  async 'task.setRank'({ tx, args }) {
    const value = args as { id: string; rank: number }
    await tx.mutate.task.update(value)
  },
  async 'message.send'({ tx, args }) {
    const value = args as {
      id: string
      serverId: string
      channelId: string
      creatorId: string
      content: string
      type: string
      createdAt: number
      order: string
      meta?: JsonValue
    }
    await tx.mutate.message.insert(value)
  },
  async 'test.exactWriteSet'({ tx, args }) {
    const value = args as { id: string }
    await tx.dbTransaction.wrappedTransaction.exec(
      'INSERT INTO project (id, "ownerId", name) VALUES (?, ?, ?)',
      [value.id, 'user-a', 'exact-write-set'],
      {
        table: 'project',
        publicTable: 'project',
        kind: 'insert',
        capture: 'exact',
        primaryKeys: [{ after: { id: value.id } }],
      }
    )
  },
  async 'test.rawWrite'({ tx, args }) {
    const value = args as { id: string }
    await tx.dbTransaction.wrappedTransaction.exec(
      'INSERT INTO project (id, "ownerId", name) VALUES (?, ?, ?)',
      [value.id, 'user-a', 'raw-trigger-write']
    )
  },
  async 'test.mixedCapture'({ tx, args }) {
    const value = args as { id: string }
    await tx.dbTransaction.wrappedTransaction.exec(
      'INSERT INTO project (id, "ownerId", name) VALUES (?, ?, ?)',
      [`${value.id}-raw`, 'u0', 'mixed raw trigger']
    )
    await tx.mutate.project.insert({
      id: `${value.id}-helper`,
      ownerId: 'u0',
      name: 'mixed helper',
    })
  },
  async 'test.batchHelper'({ tx, args }) {
    const value = args as { prefix: string; count: number }
    if (!Number.isSafeInteger(value.count) || value.count < 1 || value.count > 250) {
      throw new MutationApplicationError('invalid batch count')
    }
    for (let index = 0; index < value.count; index++) {
      await tx.mutate.project.insert({
        id: `${value.prefix}-${index}`,
        ownerId: 'u0',
        name: `helper batch ${index}`,
      })
    }
  },
  async 'test.batchRaw'({ tx, args }) {
    const value = args as { prefix: string; count: number }
    if (!Number.isSafeInteger(value.count) || value.count < 1 || value.count > 250) {
      throw new MutationApplicationError('invalid batch count')
    }
    for (let index = 0; index < value.count; index++) {
      await tx.dbTransaction.wrappedTransaction.exec(
        'INSERT INTO project (id, "ownerId", name) VALUES (?, ?, ?)',
        [`${value.prefix}-${index}`, 'u0', `raw batch ${index}`]
      )
    }
  },
  async 'test.wrongWriteSet'({ tx, args }) {
    const value = args as { id: string }
    const sql = tx.dbTransaction.wrappedTransaction
    await sql.exec('INSERT INTO project (id, "ownerId", name) VALUES (?, ?, ?)', [
      `${value.id}-raw`,
      'user-a',
      'must roll back with wrong helper keys',
    ])
    await sql.exec(
      'INSERT INTO project (id, "ownerId", name) VALUES (?, ?, ?)',
      [value.id, 'user-a', 'wrong-write-set'],
      {
        table: 'project',
        publicTable: 'project',
        kind: 'insert',
        capture: 'exact',
        primaryKeys: [{ after: { id: `${value.id}-wrong` } }],
      }
    )
  },
  async 'test.queryWrite'({ tx, args }) {
    const value = args as { id: string }
    await tx.dbTransaction.wrappedTransaction.query(
      `WITH input(id, ownerId, name) AS (VALUES (?, ?, ?))
       INSERT INTO project (id, "ownerId", name)
       SELECT id, ownerId, name FROM input RETURNING id`,
      [value.id, 'user-a', 'query-write']
    )
  },
  async 'test.effectSuccess'({ tx, args, ctx }) {
    const value = args as { id: string }
    await tx.mutate.project.insert({
      id: value.id,
      ownerId: ctx.claims.userID,
      name: 'deferred-effect-success',
    })
    ctx.defer(() => {}, { barrier: true })
  },
  async 'test.effectRollback'({ tx, args, ctx }) {
    const value = args as { id: string }
    await tx.mutate.project.insert({
      id: value.id,
      ownerId: ctx.claims.userID,
      name: 'must-roll-back',
    })
    ctx.defer(
      () => {
        throw new Error('rolled-back effect ran')
      },
      { barrier: true }
    )
    throw new MutationApplicationError('intentional-rollback')
  },
  async 'test.retryLater'({ tx, args, ctx }) {
    const value = args as { id: string }
    await tx.mutate.project.insert({
      id: value.id,
      ownerId: ctx.claims.userID,
      name: 'must-roll-back',
    })
    throw new MutationRetryError(300_000, 'intentional-retry', {
      error: 'harnessBudgetExceeded',
    })
  },
})

export function harnessConfig<Env extends SyncHostEnv>(): SyncHostConfig<Env> {
  return {
    hostVersion: '0.1.0',
    schema: harnessSchema,
    mutators: harnessMutators,
    queryTransformVersion: 1,
    // the fixture's real defineQueries registry: the host resolves every
    // desired named query through the same builders the client registers.
    queries: queries as AnyQueryRegistry,
    initialize: initializeHarness,
    namespace(request) {
      return new URL(request.url).pathname.split('/')[1] || null
    },
    authenticate: authenticateHarness,
    authorize() {
      return true
    },
    authorizeWake(request) {
      const claims = authenticateHarness(request)
      return claims ? { userID: claims.userID } : false
    },
    authorizeNotify(request, env) {
      return (
        Boolean(env.ADMIN_KEY) && request.headers.get('x-admin-key') === env.ADMIN_KEY
      )
    },
    streamingManifest: harnessStreaming.manifest,
    // A producer upgrade is a WebSocket, which cannot set headers, so the
    // harness puts its admin key in the query string. A real deployment uses a
    // service binding and never exposes this route publicly.
    authorizeProduce(request, env) {
      return (
        Boolean(env.ADMIN_KEY) &&
        new URL(request.url).searchParams.get('adminKey') === env.ADMIN_KEY
      )
    },
  }
}
