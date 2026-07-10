// canonical stringify for cross-target comparison: json column values pass
// through pg jsonb on one target (normalizes object key order) and sqlite
// text on the other (preserves it) — key order is not app-meaningful, so
// compare with sorted keys
export function canonical(value: unknown): string {
  return JSON.stringify(value, (_k, v) =>
    v !== null && typeof v === 'object' && !Array.isArray(v)
      ? Object.fromEntries(
          Object.entries(v as Record<string, unknown>).sort(([a], [b]) =>
            a.localeCompare(b)
          )
        )
      : v
  )
}
