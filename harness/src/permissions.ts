// HIGH 6 permissions lane: stock Zero clients against the visible()-filtered
// orez-local target. Pins per-user project/task isolation plus membership-add
// reveal and membership-revoke removal from an already-populated client cache.
//
//   bun src/permissions.ts
//   bun src/permissions.ts --target rust-local
import { parseArgs } from 'node:util'

import { fixtureVisibility, fixtureVisibilityInvalidation } from './fixture-visibility.js'
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

// rust-local/rust-cf bake the SAME fixture visibility policy into the host
// behind --visible; orez-local takes it as a JS callback. same semantics.
const target =
  cli.target === 'rust-local'
    ? await (
        await import('./targets/rust-local.js')
      ).startRustLocal({ pullIntervalMs: 100, visible: true })
    : cli.target === 'rust-cf'
      ? await (
          await import('./targets/rust-cf.js')
        ).startRustCf({ pullIntervalMs: 100, visible: true })
      : await startOrezLocal({
          pullIntervalMs: 100,
          visible: fixtureVisibility,
          visibilityInvalidation: fixtureVisibilityInvalidation,
        })
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

  // Keep several client caches alive across repeated grant/revoke cycles while
  // unrelated writes and explicit pulls overlap each transition. Production's
  // failure appeared only under fresh-namespace timing, so a single serial
  // add/delete proves too little: every hydrated client must repeatedly reveal
  // the rows on grant and remove them after revoke without a restart or fresh
  // storage identity.
  const churnMembers = [member, lateMember]
  for (let index = 0; index < 4; index++) {
    const view = watchAccess(target.createClient('u1'))
    churnMembers.push(view)
    views.push(view)
  }
  await eventually(() => {
    if (!churnMembers.every((view) => view.complete)) {
      throw new Error('permission churn views are not complete')
    }
    for (const [index, view] of churnMembers.entries()) {
      equal(view.projectIDs(), [], `churn client ${index} initially revoked projects`)
      equal(view.taskIDs(), [], `churn client ${index} initially revoked tasks`)
    }
  }, 'permission churn clients hydrate revoked')

  for (let round = 0; round < 12; round++) {
    await Promise.all([
      target.sql(
        `INSERT INTO member (id, "projectId", "userId")
         VALUES ('perm-member', 'perm-project', 'u1')`
      ),
      target.sql(
        `UPDATE task SET title = 'foreign churn ${round}'
         WHERE id = 'perm-foreign-task'`
      ),
      target.pull(),
    ])
    await eventually(() => {
      for (const [index, view] of churnMembers.entries()) {
        equal(
          view.projectIDs(),
          ['perm-project'],
          `round ${round} client ${index} granted projects`
        )
        equal(
          view.taskIDs(),
          ['perm-task'],
          `round ${round} client ${index} granted tasks`
        )
      }
    }, `permission churn round ${round} grant`)

    await Promise.all([
      target.sql(`DELETE FROM member WHERE id = 'perm-member'`),
      target.sql(
        `UPDATE task SET title = 'visible churn ${round}'
         WHERE id = 'perm-task'`
      ),
      target.pull(),
    ])
    await eventually(() => {
      for (const [index, view] of churnMembers.entries()) {
        equal(view.projectIDs(), [], `round ${round} client ${index} revoked projects`)
        equal(view.taskIDs(), [], `round ${round} client ${index} revoked tasks`)
      }
      equal(owner.projectIDs(), ['perm-project'], `round ${round} owner projects`)
      equal(owner.taskIDs(), ['perm-task'], `round ${round} owner tasks`)
    }, `permission churn round ${round} revoke`)
  }
  console.log(
    `[permissions] concurrent churn PASS: ${churnMembers.length} populated caches x 12 grant/revoke rounds`
  )

  console.log(
    '[permissions] PASS: filtered hydrate + add + revoke + late client + concurrent churn'
  )
} finally {
  for (const view of views) view.destroy()
  await target.close()
}
