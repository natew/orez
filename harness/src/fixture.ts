// shared fixture: one zero schema + named queries + custom mutators + DDL +
// deterministic seed, used by every target. modeled on the shapes found in
// ~/chat's query layer (the canonical large zero app): related() fan-out with
// nested one(), per-relation orderBy/limit windows, exists, and/or trees, IN,
// LIKE, nullable compares, junction hops. plus a types-exercising task table
// (number/boolean/json/nullable) because rowsPatch value conversion is a
// per-target responsibility the shapes lane must catch.
//
// modern zero API only: queries are named `defineQuery` definitions
// transformed server-side; writes are custom mutators. ad-hoc zql from
// `createBuilder` reads the local cache only and never syncs more data.
import {
  ANYONE_CAN_DO_ANYTHING,
  createBuilder,
  createSchema,
  defineMutator,
  defineMutators,
  definePermissions,
  defineQueries,
  defineQuery,
  boolean,
  json,
  number,
  relationships,
  string,
  table,
  type Transaction,
} from '@rocicorp/zero'

import { validateAtomicAppendArgs } from './consistency/atomic-visibility-workload.js'
import { validateIncrementProbeArgs } from './consistency/exactly-once-workload.js'

const user = table('user')
  .columns({
    id: string(),
    name: string(),
  })
  .primaryKey('id')

const project = table('project')
  .columns({
    id: string(),
    ownerId: string(),
    name: string(),
  })
  .primaryKey('id')

const member = table('member')
  .columns({
    id: string(),
    projectId: string(),
    userId: string(),
  })
  .primaryKey('id')

const task = table('task')
  .columns({
    id: string(),
    projectId: string(),
    title: string(),
    rank: number(),
    done: boolean(),
    meta: json().optional(),
    dueAt: number().optional(),
  })
  .primaryKey('id')

const projectRelationships = relationships(project, ({ many }) => ({
  members: many({
    sourceField: ['id'],
    destSchema: member,
    destField: ['projectId'],
  }),
  tasks: many({
    sourceField: ['id'],
    destSchema: task,
    destField: ['projectId'],
  }),
}))

const memberRelationships = relationships(member, ({ one }) => ({
  user: one({
    sourceField: ['userId'],
    destSchema: user,
    destField: ['id'],
  }),
  project: one({
    sourceField: ['projectId'],
    destSchema: project,
    destField: ['id'],
  }),
}))

const taskRelationships = relationships(task, ({ one }) => ({
  project: one({
    sourceField: ['projectId'],
    destSchema: project,
    destField: ['id'],
  }),
}))

export const schema = createSchema({
  tables: [user, project, member, task],
  relationships: [projectRelationships, memberRelationships, taskRelationships],
})

export type Schema = typeof schema

// ad-hoc zql builder: local-cache-only on clients, AST builder on the server
export const zql = createBuilder(schema)

// ---------------------------------------------------------------------------
// query corpus: ONE builder map shared by the client registry and the server
// transform endpoint, so both sides always produce the same AST. shapes
// modeled on the chat census (plans/zero-conformance-harness.md M3).
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// generated queries (sweep lane): ONE named query whose args ARE a shape
// spec, interpreted into zql by this builder on the client registry AND the
// server transform (both share queryBuilders), so randomized shapes need no
// per-shape registration. grammar intentionally mirrors what the corpus and
// the chat census use: cmp/and/or trees, exists, orderBy, limit, related
// (with sub-where/order/limit/one), one().
// ---------------------------------------------------------------------------

export type GenWhere =
  | { op: 'cmp'; col: string; cmp: string; value: unknown }
  | { op: 'and' | 'or'; children: GenWhere[] }

export type GenSubSpec = {
  where?: GenWhere
  orderBy?: [string, 'asc' | 'desc'][]
  limit?: number
  one?: boolean
  // recursive: related-of-related, e.g. project→members→user one()
  related?: { rel: string; sub?: GenSubSpec }[]
}

export type GenSpec = GenSubSpec & {
  table: 'user' | 'project' | 'member' | 'task'
  exists?: { rel: string; where?: GenWhere }[]
  // cursor pagination: seek past `row` in the spec's orderBy order
  start?: { row: Record<string, unknown>; inclusive?: boolean }
}

// biome-ignore lint/suspicious/noExplicitAny: dynamic zql chain by design
type AnyQuery = any

function applyWhere(q: AnyQuery, where: GenWhere): AnyQuery {
  if (where.op === 'cmp') return q.where(where.col, where.cmp, where.value)
  return q.where(({ and, or, cmp }: AnyQuery) => {
    const build = (node: GenWhere): AnyQuery =>
      node.op === 'cmp'
        ? cmp(node.col, node.cmp, node.value)
        : (node.op === 'and' ? and : or)(...node.children.map(build))
    return build(where)
  })
}

function applySub(q: AnyQuery, sub: GenSubSpec): AnyQuery {
  if (sub.where) q = applyWhere(q, sub.where)
  for (const [col, dir] of sub.orderBy ?? []) q = q.orderBy(col, dir)
  if (sub.limit !== undefined) q = q.limit(sub.limit)
  for (const r of sub.related ?? []) {
    q = r.sub
      ? q.related(r.rel, (sq: AnyQuery) => applySub(sq, r.sub!))
      : q.related(r.rel)
  }
  if (sub.one) q = q.one()
  return q
}

export function buildGenerated(spec: GenSpec): AnyQuery {
  let q: AnyQuery = zql[spec.table]
  if (spec.where) q = applyWhere(q, spec.where)
  for (const e of spec.exists ?? []) {
    q = e.where
      ? q.whereExists(e.rel, (sq: AnyQuery) => applyWhere(sq, e.where!))
      : q.whereExists(e.rel)
  }
  for (const [col, dir] of spec.orderBy ?? []) q = q.orderBy(col, dir)
  if (spec.start) {
    q = q.start(spec.start.row, spec.start.inclusive ? { inclusive: true } : undefined)
  }
  if (spec.limit !== undefined) q = q.limit(spec.limit)
  for (const r of spec.related ?? []) {
    q = r.sub
      ? q.related(r.rel, (sq: AnyQuery) => applySub(sq, r.sub!))
      : q.related(r.rel)
  }
  if (spec.one) q = q.one()
  return q
}

export const queryBuilders = {
  generated: (args: GenSpec) => buildGenerated(args),
  allProjects: () => zql.project.related('members'),
  projectById: (args: { id: string }) =>
    zql.project
      .where('id', args.id)
      .one()
      .related('members', (q) => q.related('user', (q) => q.one()))
      .related('tasks', (q) => q.orderBy('rank', 'desc')),
  projectsOwnedBy: (args: { ownerId: string }) =>
    zql.project.where('ownerId', args.ownerId).orderBy('name', 'asc'),
  projectsWithRecentTasks: () =>
    zql.project.related('tasks', (q) => q.orderBy('rank', 'desc').limit(3)),
  projectMemberUsers: () =>
    zql.project
      .orderBy('id', 'asc')
      .related('members', (q) => q.related('user', (q) => q.one())),
  tasksDone: () => zql.task.where('done', true).orderBy('id', 'asc'),
  tasksNotDoneByDue: () =>
    zql.task.where('done', '!=', true).orderBy('dueAt', 'asc').orderBy('id', 'asc'),
  tasksInProjects: (args: { projectIds: string[] }) =>
    zql.task.where('projectId', 'IN', args.projectIds).orderBy('id', 'asc'),
  tasksTitleLike: (args: { pattern: string }) =>
    zql.task.where('title', 'LIKE', args.pattern).orderBy('id', 'asc'),
  tasksRankRange: (args: { min: number; max: number }) =>
    zql.task
      .where('rank', '>', args.min)
      .where('rank', '<=', args.max)
      .orderBy('id', 'asc'),
  tasksAndOr: () =>
    zql.task.where(({ and, or, cmp }) =>
      and(cmp('done', '=', false), or(cmp('rank', '>', 5), cmp('title', 'LIKE', '%x%')))
    ),
  projectsWithAnyDoneTask: () =>
    zql.project.whereExists('tasks', (q) => q.where('done', true)).orderBy('id', 'asc'),
  // not(exists()) is unsupported on the zero client (bugs.rocicorp.dev/3438,
  // found by this lane 2026-07-09) — junction-scoped exists instead
  projectsWithUserMember: (args: { userId: string }) =>
    zql.project
      .whereExists('members', (q) => q.where('userId', args.userId))
      .orderBy('id', 'asc'),
  taskById: (args: { id: string }) => zql.task.where('id', args.id).one(),
  projectsOrderMulti: () =>
    zql.project.orderBy('ownerId', 'asc').orderBy('name', 'desc').limit(5),
  firstProjectAlphabetical: () => zql.project.orderBy('name', 'asc').one(),
  membersOfProject: (args: { projectId: string }) =>
    zql.member
      .where('projectId', args.projectId)
      .orderBy('id', 'asc')
      .related('user', (q) => q.one()),
  // window shapes: rows enter/leave these via rank/dueAt churn in the write
  // script — the incremental (diff-poke) maintenance path must match a fresh
  // server evaluation exactly
  tasksTopByRank: () => zql.task.orderBy('rank', 'desc').orderBy('id', 'asc').limit(5),
  tasksAfterCursor: (args: { cursor: { rank: number; id: string } }) =>
    zql.task.orderBy('rank', 'asc').orderBy('id', 'asc').start(args.cursor).limit(6),
  projectTasksPage: () =>
    zql.project
      .orderBy('name', 'asc')
      .limit(4)
      .related('tasks', (q) => q.orderBy('rank', 'desc').limit(2))
      .related('members', (q) => q.related('user', (q) => q.one())),
  tasksDueNull: () => zql.task.where('dueAt', 'IS', null).orderBy('id', 'asc'),
  tasksDueBefore: (args: { before: number }) =>
    zql.task
      .where('dueAt', '<', args.before)
      .orderBy('dueAt', 'asc')
      .orderBy('id', 'asc'),
} as const

export type QueryName = keyof typeof queryBuilders

// the query-aware transport's transform seam: resolve a named query's
// (name, args) to its Zero v51 AST via the SAME builder map the client
// registry + stock-zero app-server use. args arrives as the client's desired-
// query arg array ([argObject] or []), so the single arg object is args[0].
// this is the harness playing the trusted consumer-transform role (app-server
// .ts plays it for stock-zero); a production worker would apply auth here.
export function queryNameToAst(name: string, args: readonly unknown[]): unknown {
  const build = queryBuilders[name as QueryName]
  if (!build) throw new Error(`unknown query: ${name}`)
  return (build(args[0] as never) as { ast: unknown }).ast
}

export const queries = defineQueries({
  generated: defineQuery(({ args }: { args: GenSpec }) => buildGenerated(args)),
  allProjects: defineQuery(() => queryBuilders.allProjects()),
  projectById: defineQuery(({ args }: { args: { id: string } }) =>
    queryBuilders.projectById(args)
  ),
  projectsOwnedBy: defineQuery(({ args }: { args: { ownerId: string } }) =>
    queryBuilders.projectsOwnedBy(args)
  ),
  projectsWithRecentTasks: defineQuery(() => queryBuilders.projectsWithRecentTasks()),
  projectMemberUsers: defineQuery(() => queryBuilders.projectMemberUsers()),
  tasksDone: defineQuery(() => queryBuilders.tasksDone()),
  tasksNotDoneByDue: defineQuery(() => queryBuilders.tasksNotDoneByDue()),
  tasksInProjects: defineQuery(({ args }: { args: { projectIds: string[] } }) =>
    queryBuilders.tasksInProjects(args)
  ),
  tasksTitleLike: defineQuery(({ args }: { args: { pattern: string } }) =>
    queryBuilders.tasksTitleLike(args)
  ),
  tasksRankRange: defineQuery(({ args }: { args: { min: number; max: number } }) =>
    queryBuilders.tasksRankRange(args)
  ),
  tasksAndOr: defineQuery(() => queryBuilders.tasksAndOr()),
  projectsWithAnyDoneTask: defineQuery(() => queryBuilders.projectsWithAnyDoneTask()),
  projectsWithUserMember: defineQuery(({ args }: { args: { userId: string } }) =>
    queryBuilders.projectsWithUserMember(args)
  ),
  taskById: defineQuery(({ args }: { args: { id: string } }) =>
    queryBuilders.taskById(args)
  ),
  projectsOrderMulti: defineQuery(() => queryBuilders.projectsOrderMulti()),
  firstProjectAlphabetical: defineQuery(() => queryBuilders.firstProjectAlphabetical()),
  membersOfProject: defineQuery(({ args }: { args: { projectId: string } }) =>
    queryBuilders.membersOfProject(args)
  ),
  tasksTopByRank: defineQuery(() => queryBuilders.tasksTopByRank()),
  tasksAfterCursor: defineQuery(
    ({ args }: { args: { cursor: { rank: number; id: string } } }) =>
      queryBuilders.tasksAfterCursor(args)
  ),
  projectTasksPage: defineQuery(() => queryBuilders.projectTasksPage()),
  tasksDueNull: defineQuery(() => queryBuilders.tasksDueNull()),
  tasksDueBefore: defineQuery(({ args }: { args: { before: number } }) =>
    queryBuilders.tasksDueBefore(args)
  ),
})

// the shapes lane materializes each of these on every target and compares
export const queryCorpus: Array<{ name: QueryName; args?: unknown }> = [
  { name: 'allProjects' },
  { name: 'projectById', args: { id: 'p3' } },
  { name: 'projectsOwnedBy', args: { ownerId: 'u2' } },
  { name: 'projectsWithRecentTasks' },
  { name: 'projectMemberUsers' },
  { name: 'tasksDone' },
  { name: 'tasksNotDoneByDue' },
  { name: 'tasksInProjects', args: { projectIds: ['p1', 'p4', 'p9'] } },
  { name: 'tasksTitleLike', args: { pattern: '%fix%' } },
  { name: 'tasksRankRange', args: { min: 2, max: 8 } },
  { name: 'tasksAndOr' },
  { name: 'projectsWithAnyDoneTask' },
  { name: 'projectsWithUserMember', args: { userId: 'u3' } },
  { name: 'taskById', args: { id: 't17' } },
  { name: 'projectsOrderMulti' },
  { name: 'firstProjectAlphabetical' },
  { name: 'membersOfProject', args: { projectId: 'p2' } },
  { name: 'tasksTopByRank' },
  { name: 'tasksAfterCursor', args: { cursor: seedCursor() } },
  { name: 'projectTasksPage' },
  { name: 'tasksDueNull' },
  { name: 'tasksDueBefore', args: { before: 1755000000000 } },
]

// a real seed row as the pagination cursor (deterministic: same SEED on
// every target), mid-way through the (rank, id) order
function seedCursor() {
  const ordered = [...SEED.task].sort(
    (a, b) => a.rank - b.rank || a.id.localeCompare(b.id)
  )
  const row = ordered[Math.floor(ordered.length / 2)]!
  return { rank: row.rank, id: row.id }
}

// ---------------------------------------------------------------------------
// mutators
// ---------------------------------------------------------------------------

type Tx = Transaction<Schema>

export const mutators = defineMutators({
  exactlyOnce: {
    incrementProbe: defineMutator(async ({ tx, args }: { tx: Tx; args: unknown }) => {
      const { id } = validateIncrementProbeArgs(args)
      await tx.mutate.task.update({ id, rank: 1 })
    }),
  },
  atomicVisibility: {
    appendGroup: defineMutator(async ({ tx, args }: { tx: Tx; args: unknown }) => {
      const validated = validateAtomicAppendArgs(args)
      for (const effect of validated.effects) {
        await tx.mutate.task.insert({
          id: effect.id,
          projectId: effect.projectId,
          title: `atomic-visibility:${effect.id}`,
          rank: effect.rank,
          done: false,
        })
      }
    }),
  },
  project: {
    create: defineMutator(
      async ({
        tx,
        args,
      }: {
        tx: Tx
        args: { id: string; ownerId: string; name: string }
      }) => {
        await tx.mutate.project.insert(args)
      }
    ),
    rename: defineMutator(
      async ({ tx, args }: { tx: Tx; args: { id: string; name: string } }) => {
        await tx.mutate.project.update({ id: args.id, name: args.name })
      }
    ),
    delete: defineMutator(async ({ tx, args }: { tx: Tx; args: { id: string } }) => {
      await tx.mutate.project.delete({ id: args.id })
    }),
  },
  member: {
    add: defineMutator(
      async ({
        tx,
        args,
      }: {
        tx: Tx
        args: { id: string; projectId: string; userId: string }
      }) => {
        await tx.mutate.member.insert(args)
      }
    ),
    remove: defineMutator(async ({ tx, args }: { tx: Tx; args: { id: string } }) => {
      await tx.mutate.member.delete({ id: args.id })
    }),
  },
  task: {
    create: defineMutator(
      async ({
        tx,
        args,
      }: {
        tx: Tx
        args: {
          id: string
          projectId: string
          title: string
          rank: number
          done: boolean
          meta?: unknown
          dueAt?: number
        }
      }) => {
        await tx.mutate.task.insert(args as never)
      }
    ),
    toggle: defineMutator(
      async ({ tx, args }: { tx: Tx; args: { id: string; done: boolean } }) => {
        // A client transaction can only read rows already present in that
        // client's synced cache. Have the caller provide the optimistic target
        // state; executeMutator remains authoritative and flips the DB row.
        await tx.mutate.task.update(args)
      }
    ),
    setRank: defineMutator(
      async ({ tx, args }: { tx: Tx; args: { id: string; rank: number } }) => {
        await tx.mutate.task.update({ id: args.id, rank: args.rank })
      }
    ),
  },
})

// zero-cache requires a deployed permissions row; named queries carry their
// own server-side filtering so the row itself is permissive
export const permissions = definePermissions<unknown, Schema>(schema, () => ({
  user: ANYONE_CAN_DO_ANYTHING,
  project: ANYONE_CAN_DO_ANYTHING,
  member: ANYONE_CAN_DO_ANYTHING,
  task: ANYONE_CAN_DO_ANYTHING,
}))

// ---------------------------------------------------------------------------
// storage: DDL, seed, table spec, and the server-side mutator executor live
// in fixture-data.ts (zero-import-free so the cloudflare worker can bundle
// them without dragging in @rocicorp/zero)
// ---------------------------------------------------------------------------

import { tablesFromZeroSchema } from '../../src/sync-server/sync-server'
import { SEED, TABLES, jsonColumnsOf } from './fixture-data.js'

export { DDL, SEED, generateSeed, jsonColumnsOf as jsonColumns } from './fixture-data.js'

// fail loud at module eval if the hand-mirrored TABLES spec drifts from the
// zero schema (the worker bundle depends on the mirror being right)
{
  const sortKeys = (v: unknown): string =>
    JSON.stringify(v, (_k, val) =>
      val !== null && typeof val === 'object' && !Array.isArray(val)
        ? Object.fromEntries(
            Object.entries(val as Record<string, unknown>).sort(([a], [b]) =>
              a.localeCompare(b)
            )
          )
        : val
    )
  const want = sortKeys(tablesFromZeroSchema(schema))
  const got = sortKeys(TABLES)
  if (want !== got) {
    throw new Error(
      `fixture-data TABLES drifted from zero schema:\n got ${got}\nwant ${want}`
    )
  }
}
