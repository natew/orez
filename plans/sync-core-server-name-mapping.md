# sync-core `serverName` mapping audit

## Contract

Zero schema table and column keys are logical names. `serverName`, when present,
is the physical SQLite identifier; otherwise the logical name is also physical.
Every SQL reference to an application table or column uses the physical name.
Every Zero query AST, change-log key, primary-key JSON object, membership key,
snapshot progress key, upstream feed key, patch `tableName`, row object key, and
result field uses the logical name.

Internal `_zsync_*` identifiers, compiler aliases, and paged-snapshot stage names
are engine-owned physical identifiers and do not cross the wire.

## SQL identifier audit

### `schema.rs`: schema ingestion and trigger journaling

- `Tables::from_zero_schema` must parse, validate, and retain table and column
  `serverName` values. `primaryKey` entries continue to refer to logical column
  keys. Logical and physical table names, and logical and physical columns within
  a table, must each be unique under SQLite's case-insensitive identifier rules.
- `trigger_ddl` targets the physical table. Its `NEW` and `OLD` expressions read
  physical primary-key columns.
- Trigger names are engine-owned names derived from the PHYSICAL table name (schema.rs trigger_key), quoted against identifier injection.
- `_zsync_changes.tableName` and primary-key JSON object keys remain logical.
  This is required by ordinary diff lookup, query change scanning, membership
  storage, deduplication, and Zero patches.

### `pull.rs` and `value.rs`: snapshot and cursor diff reads

- Snapshot `FROM` clauses use physical table names. Their projections alias each
  physical column back to its logical key before `zero_row` constructs a patch.
- Diff change rows and dedup keys remain logical. `_zsync_changes.tableName` is
  used to look up a logical `TableSpec`.
- `resolve_row` uses the physical table and physical primary-key predicates, then
  aliases the projection back to logical names. Delete IDs and patch table names
  remain logical.
- Visibility callbacks and their SQL fragments use logical table and column names.
  Snapshot and point-read SQL project every modeled physical table into logical
  CTEs before applying the fragment, so qualified outer references and nested
  visibility subqueries keep working without a second schema compiler.
- `zero_row`, `zero_pk_id`, and row/PK dedup helpers consume logical aliases and
  emit logical object keys. They contain no application SQL identifiers.

### `query/compile.rs`: query-aware pull compiler

- Root and related `FROM` clauses use physical table names.
- AST predicate, ordering, start-bound, correlation, primary-key tie-breaker,
  partition, and predicate-probe column references map logical AST keys to
  physical SQLite columns.
- Every selected application column is explicitly projected from physical to its
  logical alias. Membership and patch construction continue to consume logical
  `Row` fields.
- Parent subqueries expose logical aliases; correlation SQL must distinguish the
  child live-table side (physical) from the projected parent side (logical).
- Window rank aliases must avoid collisions with both physical and logical column
  names.
- Compiled dependency tables, related child tables, primary-key lists, query
  hashes, durable query state, and emitted patches remain logical.
- `compile_predicate_probe` and `compile_related_of` follow the same mapping.

### `query/qpull.rs` and `query/membership.rs`: durable query state

- `_zsync_changes.tableName`, changed-row keys, `rootTable`, dependency lists,
  `rowTable`, serialized row PKs, refcount keys, and patch table names remain
  logical.
- These modules execute application-table SQL only through `query/compile.rs`;
  their row decoding depends on the compiler's physical-to-logical projections.

### `query/transaction.rs`: transaction-query compiler

- This compiler already implements the contract: it parses table and column
  `serverName`, uses physical identifiers in SQL, and aliases results to logical
  names. Its validation and projection behavior are the conformance reference for
  the shared `Tables` mapping. The new mapping must not introduce a second,
  incompatible interpretation.

### `upstream.rs`: live ingest and paged snapshots

- Upstream change and snapshot payloads identify logical tables and carry logical
  row keys. Validation and primary-key comparison remain logical.
- Live deletes/upserts target physical tables and physical columns; values are
  taken from logical row keys. Conflict targets and update assignments are
  physical.
- Stage tables are engine-owned physical tables cloned from the physical live
  table. Writes into a stage use its inherited physical columns while source row
  keys remain logical.
- `table_create_sql`, index discovery by `tbl_name`, foreign-key inspection, and
  live-trigger discovery look up the physical live table.
- Finalization drops the physical live table and renames the logical-derived stage
  table to the physical live name before reinstalling physical trigger SQL.
- Snapshot progress `tableName`, sorted-table iteration, and upstream cursors stay
  logical so resumability matches source feed keys.
- Stage names may stay derived from logical table keys because logical keys are
  unique and validated. `_zsync_snapshot_cleanup.stageName` stores the generated
  physical stage name. The generation-wide `_zsync_stage_<generation>_*` cleanup
  glob is independent of logical or physical application names and needs no
  mapping.
- Full upstream snapshot clearing targets every physical live table.

### Other core paths

- `push.rs` runs host-supplied mutators and does not generate application-table
  SQL. Mutators remain responsible for their own physical SQL identifiers.
- `store.rs` only addresses fixed `_zsync_*` tables. LMID journal rows are internal
  and unaffected.
- `wire.rs`, query hashing, change caps/cut watermarks, LMID collection, and
  retention operate on counters or logical journal values and need no mapping.

## Test plan

1. Add a failing boot regression using the real Soot generated-schema shape:
   logical `userState(userId, monthlyTokens)` mapped to physical
   `user_state(user_id, monthly_tokens)`. Create only the physical SQLite table;
   `init_schema` must install working triggers without referring to logical SQL
   identifiers.
2. Add schema validation coverage for fallback-to-logical mapping and ambiguous
   physical names.
3. Run existing sync-core behavior scenarios in both identity and mapped modes.
   The mapped mode uses snake_case physical DDL and a schema-derived `Tables`, but
   asserts the same logical snapshots, diffs, changes, query results, visibility,
   caps, upstream ingest, and paged-snapshot outcomes.
4. Add focused mapped regressions for trigger PK journaling, update/delete diff
   resolution, query predicate/order/related/window compilation, visibility,
   upstream upsert/delete/snapshot, and paged-snapshot clone/catch-up/final swap.
5. Run `cargo test -p sync-core -p sync-native`, workspace formatting, and clippy
   with warnings denied, then hand the verified SHA to the blocked Soot agent for
   its real browser-host boot/runtime acceptance.
