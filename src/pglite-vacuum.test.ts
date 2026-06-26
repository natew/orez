import { PGlite } from '@electric-sql/pglite'
import { describe, it, expect, beforeEach, afterEach } from 'vitest'

import { startPeriodicVacuum } from './pglite-manager'
import { installChangeTracking } from './replication/change-tracker'

// proves the periodic vacuum actually reclaims PGlite dead-tuple bloat from the
// change-tracking churn tables. this is the bug the fix exists for: PGlite has no
// effective autovacuum, so insert-then-purge of _orez._zero_changes leaves dead
// tuples that grow without bound until the change-streamer scan times out and
// Zero stops sending live updates.

const tableSize = async (db: PGlite, table: string): Promise<number> => {
  const r = await db.query<{ size: string }>(
    `SELECT pg_total_relation_size($1) AS size`,
    [table]
  )
  return Number(r.rows[0].size)
}

describe('startPeriodicVacuum', () => {
  let db: PGlite

  beforeEach(async () => {
    db = new PGlite()
    await db.waitReady
    await installChangeTracking(db)
  })

  afterEach(async () => {
    await db.close()
  })

  it('reclaims dead-tuple bloat from _orez._zero_changes', async () => {
    // churn the change buffer the way live replication does: append many change
    // rows, then purge them (consumer durably committed). repeat to pile up dead
    // tuples without any vacuum in between.
    for (let cycle = 0; cycle < 3; cycle++) {
      await db.exec(`
        INSERT INTO _orez._zero_changes (table_name, op, row_data)
        SELECT 'public.items', 'INSERT', jsonb_build_object('n', g, 'pad', repeat('x', 200))
        FROM generate_series(1, 8000) g
      `)
      await db.exec(`DELETE FROM _orez._zero_changes`)
    }

    const bloated = await tableSize(db, '_orez._zero_changes')

    const stop = startPeriodicVacuum({ postgres: db } as any, 60 * 60 * 1000)
    try {
      // startPeriodicVacuum runs one vacuum immediately (fire-and-forget); poll
      // until the FULL rewrite reclaims the space. on PGlite a small-table
      // VACUUM FULL finishes well under 150ms, so this converges fast.
      let reclaimed = bloated
      for (let i = 0; i < 100 && reclaimed >= bloated; i++) {
        await new Promise((r) => setTimeout(r, 50))
        reclaimed = await tableSize(db, '_orez._zero_changes')
      }
      // the empty table should be a small fraction of the bloated size
      expect(reclaimed).toBeLessThan(bloated / 2)
    } finally {
      stop()
    }
  })
})
