import type { SyncTarget } from './target.js'

export type PackedLmidCheckpoint = Readonly<
  Record<string, Readonly<Record<string, string>>>
>

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

export async function readPackedLmidCheckpoint(
  target: SyncTarget
): Promise<PackedLmidCheckpoint> {
  const rows = await target.oracle(
    'SELECT payload FROM _zsync_log_segments ORDER BY startVersion DESC LIMIT 1'
  )
  if (rows.length !== 1 || typeof rows[0]?.payload !== 'string') {
    throw new Error('packed LMID checkpoint is missing')
  }
  let decoded: unknown
  try {
    decoded = JSON.parse(rows[0].payload)
  } catch {
    throw new Error('packed LMID checkpoint is invalid JSON')
  }
  if (!isRecord(decoded) || decoded.format !== 1 || !isRecord(decoded.lmids)) {
    throw new Error('packed LMID checkpoint has an unknown shape')
  }
  const checkpoint: Record<string, Record<string, string>> = {}
  for (const [groupID, clients] of Object.entries(decoded.lmids)) {
    if (!isRecord(clients)) throw new Error(`packed LMID group ${groupID} is invalid`)
    const group: Record<string, string> = {}
    for (const [clientID, mutationID] of Object.entries(clients)) {
      if (typeof mutationID !== 'string' || !/^\d+$/.test(mutationID)) {
        throw new Error(`packed LMID ${groupID}/${clientID} is invalid`)
      }
      group[clientID] = mutationID
    }
    checkpoint[groupID] = group
  }
  return checkpoint
}
