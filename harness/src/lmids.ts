import type { SyncTarget } from './target.js'

export type LmidCheckpoint = Readonly<Record<string, Readonly<Record<string, string>>>>

export async function readLmidCheckpoint(target: SyncTarget): Promise<LmidCheckpoint> {
  const rows = await target.oracle(
    `SELECT clientGroupID, clientID, CAST(lastMutationID AS TEXT) AS lastMutationID
     FROM _zsync_clients`
  )
  const checkpoint: Record<string, Record<string, string>> = {}
  for (const row of rows) {
    if (
      typeof row.clientGroupID !== 'string' ||
      typeof row.clientID !== 'string' ||
      typeof row.lastMutationID !== 'string' ||
      !/^\d+$/.test(row.lastMutationID)
    ) {
      throw new Error('LMID checkpoint row is invalid')
    }
    const group = (checkpoint[row.clientGroupID] ??= {})
    group[row.clientID] = row.lastMutationID
  }
  return checkpoint
}
