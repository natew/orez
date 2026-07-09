// shared fixture: one zero schema + named queries + custom mutators + pg DDL
// + seed, used by every target. mirrors the zero-http spike fixture
// (user/project/member) so results stay comparable across targets.
//
// modern zero API only (no enableLegacyQueries/enableLegacyMutators): queries
// are named `defineQuery` definitions transformed server-side via
// ZERO_QUERY_URL; writes are custom mutators executed optimistically on the
// client and authoritatively via ZERO_MUTATE_URL. ad-hoc zql built from
// `createBuilder` still works client-side but only READS THE LOCAL CACHE, it
// never syncs more data — the smoke exercises that distinction explicitly.
import {
  ANYONE_CAN_DO_ANYTHING,
  createBuilder,
  createSchema,
  defineMutator,
  defineMutators,
  definePermissions,
  defineQueries,
  defineQuery,
  relationships,
  string,
  table,
  type Transaction,
} from '@rocicorp/zero'

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

const projectRelationships = relationships(project, ({ many }) => ({
  members: many({
    sourceField: ['id'],
    destSchema: member,
    destField: ['projectId'],
  }),
}))

export const schema = createSchema({
  tables: [user, project, member],
  relationships: [projectRelationships],
})

export type Schema = typeof schema

// ad-hoc zql builder: local-cache-only on clients, AST builder on the server
export const zql = createBuilder(schema)

export const queries = defineQueries({
  allProjects: defineQuery(() => zql.project.related('members')),
  projectById: defineQuery(({ args }: { args: { id: string } }) =>
    zql.project.where('id', args.id).one()
  ),
})

type Tx = Transaction<Schema>

export const mutators = defineMutators({
  project: {
    create: defineMutator(
      async ({ tx, args }: { tx: Tx; args: { id: string; ownerId: string; name: string } }) => {
        await tx.mutate.project.insert(args)
      }
    ),
  },
  member: {
    add: defineMutator(
      async ({ tx, args }: { tx: Tx; args: { id: string; projectId: string; userId: string } }) => {
        await tx.mutate.member.insert(args)
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
}))

// column names are unmapped, so store columns must match the zero schema
// exactly. this DDL is valid in BOTH postgres and sqlite — every target runs
// the same statements.
export const DDL = [
  `CREATE TABLE "user" (id text PRIMARY KEY, name text NOT NULL)`,
  `CREATE TABLE project (id text PRIMARY KEY, "ownerId" text NOT NULL, name text NOT NULL)`,
  `CREATE TABLE member (id text PRIMARY KEY, "projectId" text NOT NULL, "userId" text NOT NULL)`,
]

export const SEED = {
  user: [{ id: 'u-seed', name: 'seed user' }],
  project: [{ id: 'p-seed', ownerId: 'u-seed', name: 'seed project' }],
  member: [{ id: 'm-seed', projectId: 'p-seed', userId: 'u-seed' }],
}
