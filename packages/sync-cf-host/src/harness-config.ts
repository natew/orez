import { MutationApplicationError, registerMutators } from 'orez-sync-executor/core'
import { defineStreamingFields } from 'orez-sync-executor/realtime'

import { queryNameToAst } from '../../../harness/src/query-resolver.mjs'
import { harnessSchema, harnessStreaming } from './harness-schema.js'
import { verifyHarnessWakeToken } from './harness-wake-token.js'
import {
  visibility,
  type SyncHostConfig,
  type SyncHostEnv,
  type SyncSql,
} from './index.js'

import type { Schema } from '@rocicorp/zero'

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
  `CREATE TABLE IF NOT EXISTS _harness_effects (
    id TEXT PRIMARY KEY,
    observedCommitted INTEGER NOT NULL
  )`,
]

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
    await sql.exec('INSERT INTO project (id, "ownerId", name) VALUES (?, ?, ?)', [
      value.id,
      value.ownerId,
      value.name,
    ])
  },
  async 'project.rename'({ tx, args }) {
    const sql = tx.dbTransaction.wrappedTransaction
    const value = args as { id: string; name: string }
    await sql.exec('UPDATE project SET name = ? WHERE id = ?', [value.name, value.id])
  },
  async 'project.delete'({ tx, args }) {
    const sql = tx.dbTransaction.wrappedTransaction
    const value = args as { id: string }
    await sql.exec('DELETE FROM project WHERE id = ?', [value.id])
  },
  async 'member.add'({ tx, args }) {
    const sql = tx.dbTransaction.wrappedTransaction
    const value = args as { id: string; projectId: string; userId: string }
    await sql.exec('INSERT INTO member (id, "projectId", "userId") VALUES (?, ?, ?)', [
      value.id,
      value.projectId,
      value.userId,
    ])
  },
  async 'member.remove'({ tx, args }) {
    const sql = tx.dbTransaction.wrappedTransaction
    const value = args as { id: string }
    await sql.exec('DELETE FROM member WHERE id = ?', [value.id])
  },
  async 'task.create'({ tx, args }) {
    const sql = tx.dbTransaction.wrappedTransaction
    const value = args as {
      id: string
      projectId: string
      title: string
      rank: number
      done: boolean
      meta?: unknown
      dueAt?: number
    }
    await sql.exec(
      'INSERT INTO task (id, "projectId", title, rank, done, meta, "dueAt") VALUES (?, ?, ?, ?, ?, ?, ?)',
      [
        value.id,
        value.projectId,
        value.title,
        value.rank,
        value.done ? 1 : 0,
        value.meta == null ? null : JSON.stringify(value.meta),
        value.dueAt ?? null,
      ]
    )
  },
  async 'task.toggle'({ tx, args }) {
    const sql = tx.dbTransaction.wrappedTransaction
    const value = args as { id: string }
    const rows = await sql.query<{ done: number }>('SELECT done FROM task WHERE id = ?', [
      value.id,
    ])
    if (rows.length === 0) throw new MutationApplicationError('not-found')
    await sql.exec('UPDATE task SET done = ? WHERE id = ?', [
      rows[0]!.done ? 0 : 1,
      value.id,
    ])
  },
  async 'task.setRank'({ tx, args }) {
    const sql = tx.dbTransaction.wrappedTransaction
    const value = args as { id: string; rank: number }
    await sql.exec('UPDATE task SET rank = ? WHERE id = ?', [value.rank, value.id])
  },
  async 'message.send'({ tx, args }) {
    const sql = tx.dbTransaction.wrappedTransaction
    const value = args as {
      id: string
      serverId: string
      channelId: string
      creatorId: string
      content: string
      type: string
      createdAt: number
      order: string
      meta?: unknown
    }
    await sql.exec(
      'INSERT INTO message (id, "serverId", "channelId", "creatorId", content, type, "createdAt", "order", meta) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
      [
        value.id,
        value.serverId,
        value.channelId,
        value.creatorId,
        value.content,
        value.type,
        value.createdAt,
        value.order,
        value.meta == null ? null : JSON.stringify(value.meta),
      ]
    )
  },
  async 'test.effectSuccess'({ tx, args, ctx }) {
    const sql = tx.dbTransaction.wrappedTransaction
    const value = args as { id: string; clientID: string; mutationID: number }
    await sql.exec('INSERT INTO project (id, "ownerId", name) VALUES (?, ?, ?)', [
      value.id,
      ctx.claims.userID,
      'deferred-effect-success',
    ])
    ctx.defer(async () => {
      const rows = await sql.query<{ committed: number }>(
        `SELECT COUNT(*) AS committed FROM _zsync_clients
         WHERE clientID = ? AND lastMutationID >= ?`,
        [value.clientID, value.mutationID]
      )
      await sql.exec(
        'INSERT INTO _harness_effects (id, observedCommitted) VALUES (?, ?)',
        [value.id, Number(rows[0]?.committed ?? 0) > 0 ? 1 : 0]
      )
    })
  },
  async 'test.effectRollback'({ tx, args, ctx }) {
    const sql = tx.dbTransaction.wrappedTransaction
    const value = args as { id: string }
    await sql.exec('INSERT INTO project (id, "ownerId", name) VALUES (?, ?, ?)', [
      value.id,
      ctx.claims.userID,
      'must-roll-back',
    ])
    ctx.defer(async () => {
      await sql.exec(
        'INSERT INTO _harness_effects (id, observedCommitted) VALUES (?, 0)',
        [value.id]
      )
    })
    throw new MutationApplicationError('intentional-rollback')
  },
})

export function harnessConfig<Env extends SyncHostEnv>(): SyncHostConfig<Env> {
  return {
    hostVersion: '0.1.0',
    schema: harnessSchema,
    mutators: harnessMutators,
    queryAware: false,
    queryTransformVersion: 1,
    // one call for the whole patch, and one delay for it: a resolver that slept
    // per query would hide the very waterfall the batch contract removes, so
    // the lane's timing assertions would pass either way.
    async resolveQueries(requests) {
      const delayMs = Math.max(
        0,
        ...requests.map((request) =>
          Number((request.args[0] as { delayMs?: unknown } | undefined)?.delayMs ?? 0)
        )
      )
      if (delayMs > 0) await scheduler.wait(delayMs)
      return requests.map((request) => {
        try {
          return { ast: queryNameToAst(request.name, request.args) as never }
        } catch (error) {
          return { error: error instanceof Error ? error.message : String(error) }
        }
      })
    },
    initialize: initializeHarness,
    // The harness has no metering sink, and a handler that always fails is the
    // useful shape to run under workerd: every pull in the integration suite is
    // then evidence that a metering handler cannot turn a committed pull into
    // an error the client sees.
    onCommittedOperation() {
      throw new Error('harness metering sink is intentionally unavailable')
    },
    namespace(request) {
      return new URL(request.url).pathname.split('/')[1] || null
    },
    authenticate(request) {
      const userID = request.headers
        .get('authorization')
        ?.match(/^Bearer token-(.+)$/)?.[1]
      return userID ? { userID } : null
    },
    authorize() {
      return true
    },
    authorizeWake(request, env) {
      const url = new URL(request.url)
      const namespace = url.pathname.split('/')[1]
      if (!namespace || !env.ADMIN_KEY) return false
      return verifyHarnessWakeToken(
        url.searchParams.get('wakeToken') ?? '',
        namespace,
        env.ADMIN_KEY
      )
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
    visibility: {
      rowLocal: false,
      filter(table, claims) {
        const user = claims.userID
        const userValue = visibility.value(user)
        const eq = (
          left: ReturnType<typeof visibility.column>,
          right:
            | ReturnType<typeof visibility.column>
            | ReturnType<typeof visibility.value>
        ) => visibility.comparison(left, '=', right)
        if (table === 'user')
          return visibility.filter(eq(visibility.column('user', 'id'), userValue))
        if (table === 'project') {
          return visibility.filter(
            visibility.or(
              eq(visibility.column('project', 'ownerId'), userValue),
              visibility.exists(
                'member',
                visibility.and(
                  eq(
                    visibility.column('member', 'projectId'),
                    visibility.column('project', 'id')
                  ),
                  eq(visibility.column('member', 'userId'), userValue)
                )
              )
            )
          )
        }
        if (table === 'member') {
          return visibility.filter(
            visibility.exists(
              'project',
              visibility.and(
                eq(
                  visibility.column('project', 'id', 'p'),
                  visibility.column('member', 'projectId')
                ),
                visibility.or(
                  eq(visibility.column('project', 'ownerId', 'p'), userValue),
                  visibility.exists(
                    'member',
                    visibility.and(
                      eq(
                        visibility.column('member', 'projectId', 'access'),
                        visibility.column('project', 'id', 'p')
                      ),
                      eq(visibility.column('member', 'userId', 'access'), userValue)
                    ),
                    'access'
                  )
                )
              ),
              'p'
            )
          )
        }
        if (table === 'task')
          return visibility.filter(
            visibility.exists(
              'project',
              visibility.and(
                eq(
                  visibility.column('project', 'id'),
                  visibility.column('task', 'projectId')
                ),
                visibility.or(
                  eq(visibility.column('project', 'ownerId'), userValue),
                  visibility.exists(
                    'member',
                    visibility.and(
                      eq(
                        visibility.column('member', 'projectId'),
                        visibility.column('project', 'id')
                      ),
                      eq(visibility.column('member', 'userId'), userValue)
                    )
                  )
                )
              )
            )
          )
        if (table === 'message')
          return visibility.filter(
            visibility.exists(
              'project',
              visibility.and(
                eq(
                  visibility.column('project', 'id'),
                  visibility.column('message', 'serverId')
                ),
                visibility.or(
                  eq(visibility.column('project', 'ownerId'), userValue),
                  visibility.exists(
                    'member',
                    visibility.and(
                      eq(
                        visibility.column('member', 'projectId'),
                        visibility.column('project', 'id')
                      ),
                      eq(visibility.column('member', 'userId'), userValue)
                    )
                  )
                )
              )
            )
          )
        return undefined
      },
    },
  }
}
