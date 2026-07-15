import { queryNameToAst } from '../../../harness/src/query-resolver.mjs'
import {
  MutationApplicationError,
  registerMutators,
  type SyncHostConfig,
  type SyncHostEnv,
  type SyncSql,
  type ZeroSchemaConfig,
} from './index.js'

export const harnessSchema = {
  tables: {
    user: {
      columns: { id: { type: 'string' }, name: { type: 'string' } },
      primaryKey: ['id'],
    },
    project: {
      columns: {
        id: { type: 'string' },
        ownerId: { type: 'string' },
        name: { type: 'string' },
      },
      primaryKey: ['id'],
    },
    member: {
      columns: {
        id: { type: 'string' },
        projectId: { type: 'string' },
        userId: { type: 'string' },
      },
      primaryKey: ['id'],
    },
    task: {
      columns: {
        id: { type: 'string' },
        projectId: { type: 'string' },
        title: { type: 'string' },
        rank: { type: 'number' },
        done: { type: 'boolean' },
        meta: { type: 'json' },
        dueAt: { type: 'number' },
      },
      primaryKey: ['id'],
    },
    message: {
      columns: {
        id: { type: 'string' },
        serverId: { type: 'string' },
        channelId: { type: 'string' },
        creatorId: { type: 'string' },
        content: { type: 'string' },
        type: { type: 'string' },
        createdAt: { type: 'number' },
        order: { type: 'string' },
        meta: { type: 'json' },
      },
      primaryKey: ['id'],
    },
  },
} as const satisfies ZeroSchemaConfig

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
  async 'project.create'(tx, args) {
    const value = args as { id: string; ownerId: string; name: string }
    const exists = await tx.query('SELECT 1 FROM project WHERE id = ?', [value.id])
    if (exists.length > 0) throw new MutationApplicationError('exists')
    await tx.exec('INSERT INTO project (id, "ownerId", name) VALUES (?, ?, ?)', [
      value.id,
      value.ownerId,
      value.name,
    ])
  },
  async 'project.rename'(tx, args) {
    const value = args as { id: string; name: string }
    await tx.exec('UPDATE project SET name = ? WHERE id = ?', [value.name, value.id])
  },
  async 'project.delete'(tx, args) {
    const value = args as { id: string }
    await tx.exec('DELETE FROM project WHERE id = ?', [value.id])
  },
  async 'member.add'(tx, args) {
    const value = args as { id: string; projectId: string; userId: string }
    await tx.exec('INSERT INTO member (id, "projectId", "userId") VALUES (?, ?, ?)', [
      value.id,
      value.projectId,
      value.userId,
    ])
  },
  async 'member.remove'(tx, args) {
    const value = args as { id: string }
    await tx.exec('DELETE FROM member WHERE id = ?', [value.id])
  },
  async 'task.create'(tx, args) {
    const value = args as {
      id: string
      projectId: string
      title: string
      rank: number
      done: boolean
      meta?: unknown
      dueAt?: number
    }
    await tx.exec(
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
  async 'task.toggle'(tx, args) {
    const value = args as { id: string }
    const rows = await tx.query<{ done: number }>('SELECT done FROM task WHERE id = ?', [
      value.id,
    ])
    if (rows.length === 0) throw new MutationApplicationError('not-found')
    await tx.exec('UPDATE task SET done = ? WHERE id = ?', [
      rows[0]!.done ? 0 : 1,
      value.id,
    ])
  },
  async 'task.setRank'(tx, args) {
    const value = args as { id: string; rank: number }
    await tx.exec('UPDATE task SET rank = ? WHERE id = ?', [value.rank, value.id])
  },
  async 'message.send'(tx, args) {
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
    await tx.exec(
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
  async 'test.effectSuccess'(tx, args, context) {
    const value = args as { id: string; clientID: string; mutationID: number }
    await tx.exec('INSERT INTO project (id, "ownerId", name) VALUES (?, ?, ?)', [
      value.id,
      context.claims.userID,
      'deferred-effect-success',
    ])
    context.defer(async () => {
      const rows = await tx.query<{ committed: number }>(
        `SELECT COUNT(*) AS committed FROM _zsync_clients
         WHERE clientID = ? AND lastMutationID >= ?`,
        [value.clientID, value.mutationID]
      )
      await tx.exec(
        'INSERT INTO _harness_effects (id, observedCommitted) VALUES (?, ?)',
        [value.id, Number(rows[0]?.committed ?? 0) > 0 ? 1 : 0]
      )
    })
  },
  async 'test.effectRollback'(tx, args, context) {
    const value = args as { id: string }
    await tx.exec('INSERT INTO project (id, "ownerId", name) VALUES (?, ?, ?)', [
      value.id,
      context.claims.userID,
      'must-roll-back',
    ])
    context.defer(() =>
      tx.exec('INSERT INTO _harness_effects (id, observedCommitted) VALUES (?, 0)', [
        value.id,
      ])
    )
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
    async resolveQuery(name, args) {
      const delayMs = Number((args[0] as { delayMs?: unknown } | undefined)?.delayMs ?? 0)
      if (delayMs > 0) await scheduler.wait(delayMs)
      return queryNameToAst(name, args) as never
    },
    initialize: initializeHarness,
    namespace(request) {
      return new URL(request.url).pathname.split('/')[1] || null
    },
    authenticate(request) {
      const userID = request.headers
        .get('authorization')
        ?.match(/^Bearer token-(.+)$/)?.[1]
      return userID ? { userID } : null
    },
    authorizeWake(request) {
      return /^test-wake-(user-a|user-b)$/.test(
        new URL(request.url).searchParams.get('wakeToken') ?? ''
      )
    },
    authorizeNotify(request, env) {
      return Boolean(env.ADMIN_KEY) && request.headers.get('x-admin-key') === env.ADMIN_KEY
    },
    visibility: {
      rowLocal: false,
      filter(table, claims) {
        const user = claims.userID
        if (table === 'user') return { sql: '"id" = ?', params: [user] }
        if (table === 'project') {
          return {
            sql: '("ownerId" = ? OR EXISTS (SELECT 1 FROM member WHERE member."projectId" = project.id AND member."userId" = ?))',
            params: [user, user],
          }
        }
        if (table === 'member') {
          return {
            sql: 'EXISTS (SELECT 1 FROM project p WHERE p.id = member."projectId" AND (p."ownerId" = ? OR EXISTS (SELECT 1 FROM member access WHERE access."projectId" = p.id AND access."userId" = ?)))',
            params: [user, user],
          }
        }
        if (table === 'task')
          return {
            sql: 'EXISTS (SELECT 1 FROM project WHERE project.id = task."projectId" AND (project."ownerId" = ? OR EXISTS (SELECT 1 FROM member WHERE member."projectId" = project.id AND member."userId" = ?)))',
            params: [user, user],
          }
        if (table === 'message')
          return {
            sql: 'EXISTS (SELECT 1 FROM project WHERE project.id = message."serverId" AND (project."ownerId" = ? OR EXISTS (SELECT 1 FROM member WHERE member."projectId" = project.id AND member."userId" = ?)))',
            params: [user, user],
          }
        return undefined
      },
    },
  }
}
