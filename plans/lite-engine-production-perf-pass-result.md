# Lite engine production performance pass — result (2026-10-12)

## Headline (RAN)

Workerd A/B on real billed `rowsRead` (/admin/sql-billing deltas, 20 clients
x 10 queries + 30 idle groups + ~10 segments, pre-pass baseline vs this pass):

- steady pull: 959 -> 550 rowsRead (-43%)
- caught-up pull: 481 -> 77 rowsRead (-84%)
- writes 0 on both; wall flat sub-ms at this scale

## Landed (all on orez main, Conventional Commits, pushed)

1. `69698620` watermark+floor read once per pull (was 10 reads): ten-slice
   steady 45 -> 38 stmts, caught-up 13 -> 7. + metering test.
2. `f710492e` ledger by PK not scan: watermark 3.84 -> 0.47ms, scan
   4.67 -> 1.68ms, steady prune 3.79 -> 0.52ms (400x14KiB, release/200).
   - plan-shape test (no SCAN of segments).
3. `f64eb83c` parse each query AST once per pull: 100q steady
   2.97 -> 2.83ms, caught-up 301 -> 292us.
4. `157101d5` batch touched-pk narrowing: 100q steady 218 -> 25 stmts
   (8.7x), rows identical 709, wall 5.26 -> 3.48ms.
5. `453826c6` batch membership/refcount writes: 500-departure flip
   1524 -> 83 stmts (18x), rows/writes/dels identical; + bulk test with
   tombstone-replay proof to a lagging member.
6. `cc0f9bd6` index group bookkeeping (seen watermark, desires group+hash;
   active load JOIN -> EXISTS): wall 859 -> 620us, billed scanned rows
   ~600 -> ~25 by plan; responses byte-identical. CF bills rows scanned
   (D1 docs: "rows read (scanned)"), so this is the money change.
7. `7ee718cd` native serving watermark by key: 4.28 -> 0.73ms, same value.

Fix-forwards: `5c7669f4` regenerated L1 mutant for prune_known (net still
catches it); oxfmt plan-doc fix. CI green on the full tree (`25dbec8d`,
incl. native integration 6m10s).

## Billing grounding (TESTED)

- CF rowsRead = rows scanned (D1 metrics docs verbatim), shared with DO
  billing. Every SCAN -> SEARCH in this pass cuts billed rows, verified by
  plan in committed tests and by workerd A/B above.
- Index write cost: +1 row per desires patch / seen touch (rare) vs
  per-pull scan savings scaling with groups x clients. Stated per commit.

## Open leads (not started)

- Upstream bulk ingest (upstream.rs staging/progress): separate domain,
  no group grep red flags (paged keyset backfill already optimal).
- Executor per-mutation preflight (claim+owners re-read per mutation in a
  multi-mutation push): ~2 tiny stmts/mutation, push-rare, unmeasured.
- Row::get linear scan: wide rows scale sublinearly (2.7x for 10x cols);
  poor ROI vs wire risk.
- wasm columnar serde: crossings already cut 8.7x; interface change.
- `enforce_limit` trigger still uses ORDER BY+LIMIT shape (O(1) in
  practice, benchmark-proven); left - trigger bodies are versioned.

## CI note

`0416e564` (docs-only) CI red in native-integration: zero-cache boot
`taskID` option-parse error, before serving anything. Same code green on
`25dbec8d` (native integration passed). Environmental flake; rerun of
failed jobs issued. Latest main also carries co-tenant site commits
(their watch, incl. one self-colliding release race noted in passing).
