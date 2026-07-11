# populated-cache permission transition v1 profile (checker v2)

Task t-mrgureff-e1s0. Own only the permission-transition slice. One path, no
fallbacks. Do not push/publish/modify other worktrees.

## What this proves

A stock Zero client whose cache is ALREADY populated must reveal protected rows
the instant an admin grant lands and drop them the instant a revoke lands, and a
brand-new client must agree with it. The classic trap is confusing "client shows
no rows" (real revoke) with "client hasn't caught up yet". A disjoint **sentinel**
scope that permanently grants every participant defeats that trap: a fresh
sentinel marker committed at each epoch, observed complete by every client, is
proof the client is live at that epoch, so its protected-row snapshot is
trustworthy.

## Runtime shape (live lane)

- ONE http host. Two child namespaces A/B via `createSyncServerMount`
  (`/ns-<id>/pull|push`), each its own in-process sqlite db + `createSyncServer`
  with the fixture `visible()` permission policy. NOT separate processes.
- Identical protected ids in both namespaces (`pt-project`/`pt-task`/`pt-member`),
  distinct per-namespace marker (`mk-A`/`mk-B`) baked into names/titles. Separate
  dbs → identical ids never collide.
- A subject starts unauthorized (no `pt-member` in A); B subject starts
  authorized (`pt-member` present in B). Original clients: A owner, A subject,
  B subject — kept alive across all epochs, unique storage keys.
- Named full-scope views (project/member/task, scoped to `pt-project`) drive
  server sync. Pre-armed raw `zql` builder views read local cache only and are
  never forwarded as a second permission query.
- Sentinel scope `sn-*` disjoint from `pt-*`, permanently grants every
  participant in both namespaces; its ACL is oracle-checked unchanged at epoch
  0/1/2 while only the sentinel marker (`sn-0/1/2`) advances.
- Epochs: 0 initial (owner=A rows, subject=none, B=B rows); grant A subject
  (oracle count 1) + commit sentinel 1, snapshot; revoke (oracle count 0) +
  commit sentinel 2, snapshot. Fresh clients spun up after grant and after
  revoke; accept an epoch only when every original+fresh client reports complete.
- stock clientID/clientGroupID taken from the public client at creation; the
  observed pull body must echo both. Empty fault schedule only.

## Typed history (checker v2, fail precedence)

Four event types, one host, versioned, strict keys (unknown key rejected):

- `change` admin grant/revoke mutation. Terminal-only (ok|info). `ok` requires
  `sqlReturned` + an exact `authorityRef` oracle corroboration; ambiguous errors
  are `info` (inconclusive, does not prove the transition).
- `authority` terminal oracle read: protected-membership count, or sentinel-acl
  row set (unchanged across epochs).
- `client` a client's observed protected rows (sorted unique) + markers, named
  or raw, complete flag, fresh flag, echoed pull identity.
- `barrier` sentinel epoch commit: fresh marker + complete observation covering
  every client present at that epoch.

Precedence: structural/schema → topology (two ns/one host, exact grant+revoke,
roles) → liveness+corroboration (barriers, authority) → per-client row/marker.

## Files

- `harness/src/consistency/permission-transition.ts` — pure checker + profile
- `harness/src/consistency/permission-transition.test.ts` — mutant selftests
- `harness/src/consistency/permission-transition-workload.ts` — pure helpers
- `harness/src/consistency/permission-transition-workload.test.ts` — helper tests
- `harness/src/permission-transition-lane.ts` — real same-host A/B live lane
- CI: add self-tests to the consistency list + add the live lane step.

## Validation gate

root `bun test` (checker+workload selftests), oxfmt, oxlint, the live lane
default target, and the scoped harness tests. No push/publish.
