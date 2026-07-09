// shared fixture: one zero schema + pg DDL + seed used by every target.
// mirrors the proven zero-http spike fixture (user/project/member) so results
// are comparable across stock zero, orez-local sqlite, and orez-cf.
import {
  ANYONE_CAN_DO_ANYTHING,
  createSchema,
  definePermissions,
  relationships,
  string,
  table,
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
  // zero 1.6 gates zero.query.<table> and zero.mutate.<table> CRUD behind
  // these; the smoke uses both (custom mutators need a push server — M2)
  enableLegacyQueries: true,
  enableLegacyMutators: true,
})

export type Schema = typeof schema

// zero-deploy-permissions loads this export by name from this file
export const permissions = definePermissions<unknown, Schema>(schema, () => ({
  user: ANYONE_CAN_DO_ANYTHING,
  project: ANYONE_CAN_DO_ANYTHING,
  member: ANYONE_CAN_DO_ANYTHING,
}))

// column names are unmapped, so pg columns must match the zero schema exactly
export const PG_DDL = [
  `CREATE TABLE "user" (id text PRIMARY KEY, name text NOT NULL)`,
  `CREATE TABLE project (id text PRIMARY KEY, "ownerId" text NOT NULL, name text NOT NULL)`,
  `CREATE TABLE member (id text PRIMARY KEY, "projectId" text NOT NULL, "userId" text NOT NULL)`,
]

export const SEED = {
  user: [
    { id: 'u-seed', name: 'seed user' },
  ],
  project: [
    { id: 'p-seed', ownerId: 'u-seed', name: 'seed project' },
  ],
  member: [
    { id: 'm-seed', projectId: 'p-seed', userId: 'u-seed' },
  ],
}
