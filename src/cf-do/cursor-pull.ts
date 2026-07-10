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
// can't exist.
//
// table identity is EXPLICIT (2026-07-09 design review, blocker 1):
// production `_zero_changes.table_name` is schema-qualified
// (`public.file`, `soot_0.clients` — pg-proxy trackingRequest emits
// `<schema>.<table>`), so `tables` is keyed by the EXACT log identity and
// carries the zero/client table name to emit. rows matching neither the
// synced spec nor the host's `skip` classifier THROW — a silently dropped
// synced change is permanent client divergence, the one failure mode this
// module must never have.
//
// completeness contract (review, high 4): the caller must pass EVERY change
// row with `watermark > cookie` up to the watermark it returns as the next
// cookie, read atomically with the live point-reads. if the host caps the
// change read, it must return the last INCLUDED watermark as the cookie —
// never the global current watermark — so the remainder ships on the next
// pull. pk mutability (review, high 5): the main pg-proxy tracking path
// records old_data=null for UPDATEs, so a pk-changing UPDATE through it
// only surfaces the NEW pk. published-table pks must be immutable (enforce
// host-side) unless the tracker learns to capture old pk images.
//
// the host (soot's project pull endpoint) owns everything else: cookie
// compare against the watermark, floor/epoch snapshot fallback, LMID rows,
// and wire value conversion (its toZeroRow).

export type CursorPullTables = Record<
  string, // EXACT _zero_changes.table_name identity, e.g. 'public.file'
  { clientName: string; primaryKey: string[] }
>

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
// UPDATE/DELETE (when the tracker provides old images, a pk-changing
// UPDATE touches both, deleting the old pk)
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
  // classifies KNOWN non-synced log tables (internal bookkeeping, private
  // app tables). anything not synced and not skipped throws — never
  // silently dropped.
  skip: (logTableName: string) => boolean
  // live point-read within the pull transaction, by CLIENT table name;
  // undefined = row gone
  readRow: (
    clientTableName: string,
    pk: Record<string, unknown>
  ) => Record<string, unknown> | undefined
}): CursorDiffOp[] {
  const { changes, tables, skip, readRow } = opts

  const touched = new Map<string, { clientName: string; pk: Record<string, unknown> }>()
  for (const change of changes) {
    const spec = tables[change.tableName]
    if (!spec) {
      if (skip(change.tableName)) continue
      throw new Error(
        `cursor-pull: change log row for unmapped table '${change.tableName}' — ` +
          `synced tables must be in the spec and internal tables must be classified by skip(); ` +
          `silently dropping it would permanently diverge clients`
      )
    }
    for (const pk of pksOf(change, spec.primaryKey)) {
      const key = `${spec.clientName} ${JSON.stringify(spec.primaryKey.map((c) => pk[c]))}`
      touched.set(key, { clientName: spec.clientName, pk })
    }
  }

  const patch: CursorDiffOp[] = []
  for (const { clientName, pk } of touched.values()) {
    const row = readRow(clientName, pk)
    if (row === undefined) patch.push({ op: 'del', tableName: clientName, pk })
    else patch.push({ op: 'put', tableName: clientName, row })
  }
  return patch
}
