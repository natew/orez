// cursor-pull: the phase-2 delta primitive over the PRODUCTION change log
// (plans/zero-server-rewrite.md "build plan for the production composition").
// maps `_zero_changes` rows since a client's cookie into put/del row patches
// for an http-pull diff response.
//
// semantics follow the reference core (src/sync-server/sync-server.ts + its
// delta suite) exactly: collect the pks TOUCHED since the cookie, dedup,
// then resolve each against LIVE table state inside the pull transaction —
// row exists → put (current values), row gone → del. old row images in the
// log are used only to find pks (DELETE + pk-changing UPDATE), never as
// patch values, so op-coalescing bugs and serializer fidelity questions
// can't exist. tables outside the spec (internal _orez_*/soot_0_* state)
// are skipped — the sync surface is the zero schema tables only.
//
// the host (soot's project pull endpoint) owns everything else: cookie
// compare against the watermark, floor/epoch snapshot fallback, LMID rows,
// and wire value conversion (its toZeroRow).

export type CursorPullTables = Record<string, { primaryKey: string[] }>

// one parsed `_zero_changes` row (the shape cf-do/worker.ts readChangesSince
// produces: row_data for INSERT/UPDATE, old_data for UPDATE/DELETE)
export type ChangeLogRow = {
  tableName: string
  op: 'INSERT' | 'UPDATE' | 'DELETE'
  rowData: Record<string, unknown> | null
  oldData: Record<string, unknown> | null
}

export type CursorDiffOp =
  | { op: 'put'; tableName: string; row: Record<string, unknown> }
  | { op: 'del'; tableName: string; pk: Record<string, unknown> }

// pks touched by a change: NEW row for INSERT/UPDATE, OLD row for
// UPDATE/DELETE (a pk-changing UPDATE touches both, deleting the old pk)
function pksOf(change: ChangeLogRow, primaryKey: string[]): Record<string, unknown>[] {
  const out: Record<string, unknown>[] = []
  for (const source of [change.rowData, change.oldData]) {
    if (!source) continue
    const pk: Record<string, unknown> = {}
    for (const col of primaryKey) pk[col] = source[col]
    out.push(pk)
  }
  return out
}

export function cursorDiffPatch(opts: {
  changes: ChangeLogRow[]
  tables: CursorPullTables
  // live point-read within the pull transaction; undefined = row gone
  readRow: (
    tableName: string,
    pk: Record<string, unknown>
  ) => Record<string, unknown> | undefined
}): CursorDiffOp[] {
  const { changes, tables, readRow } = opts

  const touched = new Map<string, { tableName: string; pk: Record<string, unknown> }>()
  for (const change of changes) {
    const spec = tables[change.tableName]
    if (!spec) continue // internal / non-synced table
    for (const pk of pksOf(change, spec.primaryKey)) {
      const key = `${change.tableName} ${JSON.stringify(spec.primaryKey.map((c) => pk[c]))}`
      touched.set(key, { tableName: change.tableName, pk })
    }
  }

  const patch: CursorDiffOp[] = []
  for (const { tableName, pk } of touched.values()) {
    const row = readRow(tableName, pk)
    if (row === undefined) patch.push({ op: 'del', tableName, pk })
    else patch.push({ op: 'put', tableName, row })
  }
  return patch
}
