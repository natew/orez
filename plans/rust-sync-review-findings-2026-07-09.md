# adversarial review findings — rust sync server (2026-07-09)

Reviewer: fresh codex Sol (high), no prior branch context. Verdict:
NOT ship-quality until the criticals + highs are fixed. Throwaway repro
crate: /tmp/orez-review-cases. `cargo test --workspace` was green
throughout, so these are gaps the existing tests do not cover — each fix
must land WITH a regression test.

## CRITICAL

1. Remote permission-transform bypass — packages/sync-cf-host/src/host.ts
   ~385-397. A query-aware client sends
   `queries.patch=[{op:"put",hash:"pwn",ast:{table:"secret"}}]` with no
   `name`; the host resolves only NAMED puts and forwards every other op
   unchanged, so Rust accepts the client-authored AST with no consumer
   permission transform or baseline visibility -> forbidden rows returned.
   Owner: sol (sync-cf-host). Fix: reject any query put that is not a
   resolved named query (no client-supplied raw AST path, ever); the
   server-side resolveQuery is the only way an AST reaches the engine.

2. Query ASTs global by client-controlled hash, not scoped by
   group/user/transform version — crates/sync-core/src/query/membership.rs
   ~71-79,130-170,281-305. Group A registers restricted AST under hash h;
   group B registers a permissive AST under the same h; ON CONFLICT
   overwrites the single global row and clears all groups' query-state
   markers; A's next pull recomputes under B's AST -> forbidden rows.
   transformVersion is always 0, never supplied/checked. Violates
   invariant 15. Owner: opus-m1 (sync-core). Fix: key stored query
   definitions by (clientGroupID, hash) or by a server-trusted content
   hash of the transformed AST, not a client-chosen hash; incorporate the
   transformation version and verify it.

## HIGH

3. DDL injection via trigger identifiers — crates/sync-core/src/schema.rs
   ~199-220. Table targets/literals/PK columns are quoted, but trigger
   NAMES interpolate the raw table string; a table named
   `x" AFTER INSERT ON "victim" BEGIN DELETE FROM "victim"; END; --`
   installs an injected trigger. Requires consumer-controlled schema (not
   a wire request), but real. Owner: opus-m1. Fix: quote/escape the
   trigger identifier too, and have Tables::from_zero_schema reject table
   /column names that are not valid identifiers.

4. Cookie/LMID output can silently lose i64 precision —
   crates/sync-core/src/wire.rs ~19-24 + crates/sync-wasm/src/lib.rs
   ~381-385. counter_to_json emits i64 as a JSON Number even above
   2^53; the wasm JSON serializer materializes a JS number ->
   9007199254740993 rounds to ...992. Plan says cookies/watermarks must
   not silently round. NOTE: baseline watermarks/LMIDs are monotonic from
   0 and unreachable past 2^53 in practice, so this is currently
   theoretical for the HTTP wire — but the engine must FAIL LOUD rather
   than round. Owner: opus-m1 (wire.rs) + sol (wasm serializer). Fix:
   when a counter exceeds MAX_SAFE_INTEGER, error instead of emitting a
   rounding JSON number; keep byte-compat below that bound.

5. Nested query bounds accepted but ignored —
   crates/sync-core/src/query/compile.rs ~330-365,434-485.
   compile_related_of applies only child WHERE, dropping child
   orderBy/start/limit; compile_exists drops child start/limit. Desired
   one related row -> two durable memberships/puts; limit 0 under EXISTS
   can evaluate true. Owner: opus-m1. Fix: apply child orderBy/limit/start
   in related-of compilation; if a bound cannot be expressed in the
   correlated subquery, reject the query rather than silently widen.

## MEDIUM

6. gotQueries version can regress after del/clear —
   crates/sync-core/src/query/membership.rs ~175-245 + qpull.rs ~162-174.
   Only `put` stores clientVersion; del/clear store none, so ack can go
   2 then back to 1 -> non-monotonic query-state ack, replay/churn.
   Owner: opus-m1. Fix: track the acknowledged query-state version at the
   group level monotonically, independent of per-desire rows.

7. Query-aware EngineError status discarded at the WASM boundary —
   crates/sync-wasm/src/lib.rs ~526-543 uses map_err(js_err) instead of
   the engine_error path baseline pull/push use, so a 403 (group reuse) or
   400 (malformed query) becomes a host 500. Owner: sol/opus-m1
   (whoever owns the query wasm export). Fix: propagate status like the
   baseline exports.

8. maxChangeRows=0 permanently stalls a baseline diff —
   packages/sync-cf-host/src/host.ts ~195-197 + crates/sync-core/src/
   pull.rs ~215-227,325-327. Zero cap admits no row; cut_watermark stays
   at the input cookie; every response repeats the same cookie + empty
   patch. Owner: sol (host validation) + opus-m1 (engine guard). Fix:
   reject a cap < 1 (host validation) and/or guarantee one-row progress in
   the engine.

## Not a defect

Touched-PK narrowing: reviewer specifically tried and could NOT find an
unsoundness. Any non-root dependency touch forces recompute; recursively
collected dependency tables cover EXISTS + related-of-related; a root
touch recomputes if the row was durable or currently matches — sufficient
for departure/entry and window shifts.

## Re-verify round (2026-07-10, fresh sol reviewer)

Scope: the four gaps dispatched after the first round (a840443 GAP-2
nested bounds, 6235344 GAP-3 forward migration, 8de39d1 GAP-1 unknown
query 400, d0753a8 GAP-4 CI). Verdict: GAP-1 and GAP-4 RESOLVED; GAP-2
and GAP-3 PARTIAL; one NEW defect from the GAP-2 fix. M4b gate stays
open until these close.

- GAP-2a (blocking): ast.rs ~292-301 copies only explicit orderBy fields
  into Bound.row, discarding implicit PK tie-break components. Stock
  builder cursors include the PK (start({rank:2,id:'t2'})), so
  compile_start 400s with "start.row missing ordering key 'id'".
  Composite PKs drop every implicit component.
- GAP-2b (blocking): nullable cursor comparisons — compile.rs ~205-228
  emits `col > NULL` / `col = NULL`, false for all rows in SQLite, so a
  valid cursor with a null ordered value hides all later rows.
- GAP-2c (NEW from a840443): ROW_NUMBER window alias `_zrn` hard-coded
  but legal as an application column; a child table with its own `_zrn`
  makes the window filter compare against app data (repro: app _zrn=99,
  limit 1 -> zero rows).
- GAP-3a (blocking): the query-schema migration reset wipes
  defs/desires/acks/membership but never invalidates the baseline
  cookie/epoch; a migrated client's next pull fast-paths to
  {unchanged:true} forever (silent staleness, live-reproduced on
  persisted workerd storage).
- GAP-3b: QUERY_SCHEMA_VERSION is never read — init unconditionally
  overwrites the stored version, so shape mismatches (including FUTURE
  versions) are silently stamped back instead of triggering reset or
  fail-loud.

Separately root-caused by the coordinator during this round (fixed,
002def5): the intermittent rust-cf query-diff timeout was the CF host
keeping admin namespace knobs (query-aware/visibility/retention) in
instance fields; a DO restart reverted a query-aware namespace to
baseline and every pull answered {unchanged:true}. Knobs now persist in
_zsync_host_control with a workerd kill+restart regression lane
(test:restart).
