// The fixture's per-user row visibility policy: a project is visible to its
// owner or any member, and its member/task rows follow the project. Shared by
// every permission lane (permissions.ts and permission-transition-lane.ts) and
// baked identically into the rust host behind --visible, so there is exactly
// one definition of "who can see what".
export function fixtureVisibility(
  table: string,
  userID: string
): { sql: string; params: unknown[] } {
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
