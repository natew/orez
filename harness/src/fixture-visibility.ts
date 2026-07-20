import type {
  ZeroHttpVisibility,
  ZeroHttpVisibilityInvalidation,
} from '../../src/zero-http/mount.js'

// The fixture's per-user row visibility policy: a project is visible to its
// owner or any member, and its member/task rows follow the project. Shared by
// every permission lane (permissions.ts and permission-transition-lane.ts) and
// baked identically into the rust host behind --visible, so there is exactly
// one definition of "who can see what".
export const fixtureVisibility: ZeroHttpVisibility = (table, userID) => {
  const projectAccess = `(p."ownerId" = ? OR EXISTS (
    SELECT 1 FROM member access
    WHERE access."projectId" = p.id AND access."userId" = ?
  ))`
  switch (table) {
    case 'user':
      return { where: `"user".id = ?`, params: [userID] }
    case 'project':
      return {
        where: `(project."ownerId" = ? OR EXISTS (
          SELECT 1 FROM member access
          WHERE access."projectId" = project.id AND access."userId" = ?
        ))`,
        params: [userID, userID],
      }
    case 'member':
      return {
        where: `EXISTS (
          SELECT 1 FROM project p
          WHERE p.id = member."projectId" AND ${projectAccess}
        )`,
        params: [userID, userID],
      }
    case 'task':
      return {
        where: `EXISTS (
          SELECT 1 FROM project p
          WHERE p.id = task."projectId" AND ${projectAccess}
        )`,
        params: [userID, userID],
      }
    default:
      throw new Error(`no fixture visibility policy for table ${table}`)
  }
}

export const fixtureVisibilityInvalidation: ZeroHttpVisibilityInvalidation = {
  capture: { member: ['projectId', 'userId'] },
  shouldReset({ userID, changes }) {
    return changes.some(
      (change) =>
        change.table === 'member' &&
        (change.before?.userId === userID || change.after?.userId === userID)
    )
  },
}
