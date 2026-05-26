export const RETURNING_INTERNAL_PREFIX = '__orez_returning_'

export interface TrackedRowFilter {
  rowColumns?: string[]
}

export function trackedChangeRow(
  row: Record<string, unknown>,
  track: TrackedRowFilter
): Record<string, unknown> {
  const allowed = track.rowColumns ? new Set(track.rowColumns) : null
  const out: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(row)) {
    if (key.startsWith(RETURNING_INTERNAL_PREFIX)) continue
    if (allowed && !allowed.has(key)) continue
    out[key] = value
  }
  return out
}
