// raw client-store inspection for forbidden-row assertions (invariants 13-15).
//
// ad-hoc zql from createBuilder reads the client's LOCAL synced cache only and
// never syncs more data (pinned by smoke.ts), so materializing an unfiltered
// table query surfaces exactly the rows physically present in the client's
// Replicache store — including any forbidden row a query-aware client should
// never hold. this is the direct read of the client store the query-aware
// lanes assert against: a revoked or undesired row must LEAVE the raw store,
// never linger even transiently.
import { zql } from './fixture.js'

import type { FixtureZero } from './target.js'

export type RawTable = 'user' | 'project' | 'member' | 'task'

export async function rawClientRows(
  zero: FixtureZero,
  table: RawTable,
  settleMs = 150
): Promise<Record<string, unknown>[]> {
  const view = zero.materialize(zql[table] as never)
  // let any just-applied poke settle into the local store before reading
  await new Promise((resolve) => setTimeout(resolve, settleMs))
  const rows = JSON.parse(
    JSON.stringify((view as unknown as { data: unknown }).data)
  ) as Record<string, unknown>[]
  view.destroy()
  return rows
}

export async function rawClientIds(
  zero: FixtureZero,
  table: RawTable,
  settleMs?: number
): Promise<string[]> {
  const rows = await rawClientRows(zero, table, settleMs)
  return rows.map((row) => String(row.id)).sort()
}
