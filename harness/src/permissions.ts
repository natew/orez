// HIGH 6 permissions lane: stock Zero clients against the visible()-filtered
// orez-local target. Pins per-user project/task isolation plus membership-add
// reveal and membership-revoke removal from an already-populated client cache.
//
//   bun src/permissions.ts
//   bun src/permissions.ts --target rust-local
import { parseArgs } from 'node:util'

import { queries } from './fixture.js'
import { startOrezLocal } from './targets/orez-local.js'

import type { FixtureZero } from './target.js'

const { values: cli } = parseArgs({
  options: { target: { type: 'string', default: 'orez-local' } },
})

type ProjectRow = {
  id: string
  members: Array<{ id: string }>
}
type TaskRow = { id: string }

const ISOLATED_PROJECTS = ['perm-project', 'perm-foreign']

function fixtureVisibility(table: string, userID: string) {
  const projectAccess = `(p."ownerId" = ? OR EXISTS (
    SELECT 1 FROM member access
    WHERE access."projectId" = p.id AND access."userId" = ?
  ))`
  switch (table) {
    case 'user':
      return { sql: `SELECT * FROM "user" WHERE id = ?`, params: [userID] }
    case 'project':
      return {
        sql: `SELECT p.* FROM project p WHERE ${projectAccess}`,
        params: [userID, userID],
      }
    case 'member':
      return {
        sql: `SELECT m.* FROM member m
              WHERE EXISTS (
                SELECT 1 FROM project p
                WHERE p.id = m."projectId" AND ${projectAccess}
              )`,
        params: [userID, userID],
      }
    case 'task':
      return {
        sql: `SELECT t.* FROM task t
              WHERE EXISTS (
                SELECT 1 FROM project p
                WHERE p.id = t."projectId" AND ${projectAccess}
              )`,
        params: [userID, userID],
      }
    default:
      throw new Error(`no fixture visibility policy for table ${table}`)
  }
}

function watchAccess(zero: FixtureZero) {
  const projectView = zero.materialize(queries.allProjects())
  const taskView = zero.materialize(
    queries.tasksInProjects({ projectIds: ISOLATED_PROJECTS })
  )
  let projects: ProjectRow[] = []
  let tasks: TaskRow[] = []
  let projectsComplete = false
  let tasksComplete = false

  projectView.addListener((data, resultType) => {
    projects = JSON.parse(JSON.stringify(data)) as ProjectRow[]
    if (resultType === 'complete') projectsComplete = true
  })
  taskView.addListener((data, resultType) => {
    tasks = JSON.parse(JSON.stringify(data)) as TaskRow[]
    if (resultType === 'complete') tasksComplete = true
  })

  return {
    get complete() {
      return projectsComplete && tasksComplete
    },
    projectIDs() {
      return projects
        .map(({ id }) => id)
        .filter((id) => ISOLATED_PROJECTS.includes(id))
        .sort()
    },
    taskIDs() {
      return tasks.map(({ id }) => id).sort()
    },
    memberIDs(projectID: string) {
      return (
        projects
          .find(({ id }) => id === projectID)
          ?.members.map(({ id }) => id)
          .filter((id) => id.startsWith('perm-'))
          .sort() ?? []
      )
    },
    destroy() {
      projectView.destroy()
      taskView.destroy()
    },
  }
}

async function eventually(check: () => void, label: string) {
  const started = Date.now()
  let lastError: unknown
  while (Date.now() - started < 30_000) {
    try {
      check()
      return
    } catch (error) {
      lastError = error
      await new Promise((resolve) => setTimeout(resolve, 50))
    }
  }
  throw new Error(`timeout waiting for ${label}: ${String(lastError)}`)
}

function equal(actual: string[], expected: string[], label: string) {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(
      `${label}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`
    )
  }
}

// rust-local bakes the SAME fixture visibility policy into the native host
// behind --visible; orez-local takes it as a JS callback. same semantics.
const target =
  cli.target === 'rust-local'
    ? await (
        await import('./targets/rust-local.js')
      ).startRustLocal({
        pullIntervalMs: 100,
        visible: true,
      })
    : await startOrezLocal({ pullIntervalMs: 100, visible: fixtureVisibility })
const views: ReturnType<typeof watchAccess>[] = []

try {
  await target.sql(
    `INSERT INTO project (id, "ownerId", name) VALUES
      ('perm-project', 'u0', 'visible project'),
      ('perm-foreign', 'u2', 'foreign project')`
  )
  await target.sql(
    `INSERT INTO member (id, "projectId", "userId")
     VALUES ('perm-member', 'perm-project', 'u1')`
  )
  await target.sql(
    `INSERT INTO task (id, "projectId", title, rank, done, meta, "dueAt") VALUES
      ('perm-task', 'perm-project', 'visible task', 1, false, NULL, NULL),
      ('perm-foreign-task', 'perm-foreign', 'foreign task', 2, false, NULL, NULL)`
  )

  const owner = watchAccess(target.createClient('u0'))
  const member = watchAccess(target.createClient('u1'))
  const foreign = watchAccess(target.createClient('u2'))
  views.push(owner, member, foreign)

  await eventually(() => {
    if (!views.every((view) => view.complete)) throw new Error('views are not complete')
    equal(owner.projectIDs(), ['perm-project'], 'owner projects')
    equal(owner.taskIDs(), ['perm-task'], 'owner tasks')
    equal(member.projectIDs(), ['perm-project'], 'member projects')
    equal(member.taskIDs(), ['perm-task'], 'member tasks')
    equal(foreign.projectIDs(), ['perm-foreign'], 'foreign projects')
    equal(foreign.taskIDs(), ['perm-foreign-task'], 'foreign tasks')
    equal(owner.memberIDs('perm-project'), ['perm-member'], 'owner membership rows')
  }, 'initial per-user isolation')
  console.log('[permissions] initial owner/member/foreign isolation PASS')

  await target.sql(
    `INSERT INTO member (id, "projectId", "userId")
     VALUES ('perm-foreign-member', 'perm-project', 'u2')`
  )
  await eventually(() => {
    equal(foreign.projectIDs(), ['perm-foreign', 'perm-project'], 'new member projects')
    equal(foreign.taskIDs(), ['perm-foreign-task', 'perm-task'], 'new member tasks')
    equal(
      owner.memberIDs('perm-project'),
      ['perm-foreign-member', 'perm-member'],
      'owner membership rows after add'
    )
  }, 'membership-add reveal')
  console.log('[permissions] membership-add reveal PASS')

  await target.sql(`DELETE FROM member WHERE id = 'perm-member'`)
  await eventually(() => {
    equal(member.projectIDs(), [], 'revoked member projects')
    equal(member.taskIDs(), [], 'revoked member tasks')
    equal(owner.projectIDs(), ['perm-project'], 'owner projects after revoke')
    equal(owner.taskIDs(), ['perm-task'], 'owner tasks after revoke')
  }, 'membership-revoke cache clearing')
  console.log('[permissions] membership-revoke cache clearing PASS')

  const lateMember = watchAccess(target.createClient('u1'))
  views.push(lateMember)
  await eventually(() => {
    if (!lateMember.complete) throw new Error('late member view is not complete')
    equal(lateMember.projectIDs(), [], 'late revoked member projects')
    equal(lateMember.taskIDs(), [], 'late revoked member tasks')
  }, 'late revoked member hydration')

  console.log('[permissions] PASS: filtered hydrate + add + revoke + late client')
} finally {
  for (const view of views) view.destroy()
  await target.close()
}
