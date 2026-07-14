# atomic resnapshot schema repair

## objective

Make `POST /<namespace>/admin/resnapshot` repair a derived sync engine when the
authoritative DATA database contains tables outside that consumer's sync
schema, without weakening the existing all-or-nothing abort behavior.

## required evidence

- Reproduce the production failure shape using the actual corrupted project
  namespace's schema and snapshot table set, without mutating production.
- Determine whether `user` is incorrectly included in a project-scoped DATA
  snapshot or is a legitimate table absent from the derived engine's stale
  schema.
- Add a failing-first test that reaches the same `unknown table user` conflict.
- If the engine schema is stale, apply its refresh and snapshot in one atomic
  engine step. If DATA is leaking out-of-scope tables, fix that scope at DATA
  and retain the engine's strict schema check.
- Prove a failed repair leaves the upstream watermark and derived rows unchanged.
- Prove the repaired flow accepts the real table set and fixes timestamp values
  that were persisted as null by the old snapshot decoder.
- Run the focused Rust and host tests, the full Orez test suite, and `bun check`.
- Commit the completed change from a clean worktree based on Orez `main`.

## non-goals

- Do not alter production namespaces or make another production repair call.
- Do not publish, release, deploy, or push.
- Do not add a best-effort or partial snapshot path.
- Do not touch or include the three dirty files in `/Users/n8/orez`.

## closeout

Report the root cause as either a DATA scope leak or legitimate schema drift,
the failing-first and fixed runtime evidence, complete validation commands, and
the commit SHA. The owner will handle release and deployment separately.

## root cause decision

This is a DATA snapshot scope leak. The affected project namespace's physical
database legitimately contains the private `user` access-mirror table, while
the consumer schema exposes `userPublic` and does not include `user`. The DATA
snapshot endpoint ignored the consumer table surface and returned every entry
ever recorded in `_zero_schema_tables`. Schema refresh would therefore ingest a
private table that the consumer must not query. The repair scopes the snapshot
at DATA using the consumer schema's explicit table list and keeps sync-core's
unknown-table conflict unchanged for any malformed scoped payload.

## runtime evidence

- A read-only production status request for
  `p-proj_mrbggrph_ya7ome` reported upstream watermark `1616`, matching the
  pre-failure value and confirming the rejected repair changed no cursor state.
- A read-only `sqlite_master` query against that same derived engine found both
  `user` and `userPublic`. The current Soot consumer schema contains
  `userPublic` and does not contain `user`, proving that `user` is physical
  private/control data rather than a missing public consumer table.
- The failing-first workerd test registered the same extra physical `user`
  table in a project DATA namespace. Before the fix, the real
  `/admin/resnapshot` route returned HTTP 409 with
  `schema refresh required: unknown table user`.
- With the scoped DATA request, the same route returns HTTP 200, does not create
  `user` in the derived engine, and a repaired PostgreSQL timestamp text value
  pulls as epoch milliseconds (`1783776886000`).
- A sync-core transaction test sends the unscoped `user` payload directly to
  the engine and proves the 409 leaves the prior row and upstream watermark
  unchanged. This pins the abort independently of the DATA filtering layer.
