import { log } from './log.js'

import type { PGlite } from '@electric-sql/pglite'

export async function syncManagedPublications(
  db: PGlite,
  names: string[],
  managedByOrez: boolean
): Promise<void> {
  if (!managedByOrez || names.length === 0) return

  const tables = await db.query<{ tablename: string }>(
    `SELECT tablename
     FROM pg_tables
     WHERE schemaname = 'public'
       AND tablename NOT LIKE '_zero_%'`
  )
  const publicTables = tables.rows
    .map((r) => r.tablename)
    .filter((t) => !t.startsWith('_'))

  for (const pub of names) {
    const quotedPub = '"' + pub.replace(/"/g, '""') + '"'
    await db.exec(`CREATE PUBLICATION ${quotedPub}`).catch(() => {})

    if (publicTables.length === 0) continue
    const inPub = await db.query<{ tablename: string }>(
      `SELECT tablename
       FROM pg_publication_tables
       WHERE pubname = $1
         AND schemaname = 'public'`,
      [pub]
    )
    const inPubSet = new Set(inPub.rows.map((r) => r.tablename))
    const toAdd = publicTables.filter((t) => !inPubSet.has(t))
    if (toAdd.length === 0) continue
    let added = 0
    for (const table of toAdd) {
      const quotedTable = quotePublicTable(table)
      try {
        await db.exec(`ALTER PUBLICATION ${quotedPub} ADD TABLE ${quotedTable}`)
        added++
      } catch (err) {
        if (!isPublicationAlreadyMemberError(err)) throw err
      }
    }
    log.debug.orez(`added ${added} table(s) to publication "${pub}"`)
  }
}

/**
 * ensure publications have table membership after on-db-ready.
 * handles the case where orez pre-created an empty publication and the app's
 * migration skipped adding tables because the publication already existed.
 */
export async function ensurePublicationHasTables(
  db: PGlite,
  names: string[]
): Promise<void> {
  for (const pub of names) {
    const inPub = await db.query<{ count: string }>(
      `SELECT count(*)::text as count FROM pg_publication_tables
       WHERE pubname = $1 AND schemaname = 'public'`,
      [pub]
    )
    if (Number(inPub.rows[0]?.count) > 0) continue

    // publication exists but has no tables - add all public tables
    const pubExists = await db.query<{ count: string }>(
      `SELECT count(*)::text as count FROM pg_publication WHERE pubname = $1`,
      [pub]
    )
    if (Number(pubExists.rows[0]?.count) === 0) continue

    const tables = await db.query<{ tablename: string }>(
      `SELECT tablename FROM pg_tables
       WHERE schemaname = 'public'
         AND tablename NOT LIKE '_zero_%'
         AND tablename NOT LIKE '\\_%'`
    )
    if (tables.rows.length === 0) continue

    const quotedPub = '"' + pub.replace(/"/g, '""') + '"'
    let added = 0
    for (const table of tables.rows) {
      const quotedTable = quotePublicTable(table.tablename)
      try {
        await db.exec(`ALTER PUBLICATION ${quotedPub} ADD TABLE ${quotedTable}`)
        added++
      } catch (err) {
        if (!isPublicationAlreadyMemberError(err)) throw err
      }
    }
    log.orez(`publication "${pub}" was empty, added ${added} table(s)`)
  }
}

function quotePublicTable(table: string): string {
  return `"public"."${table.replace(/"/g, '""')}"`
}

function isPublicationAlreadyMemberError(err: unknown): boolean {
  return /already member of publication/i.test(
    err instanceof Error ? err.message : String(err)
  )
}
