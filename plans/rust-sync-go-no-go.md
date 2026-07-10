# rust-sync-server: go/no-go decision sheet

Everything in plans/rust-sync-server-final-plan.md that can be done
without your explicit word is done and green (tracker:
rust-sync-server-execution.md, CI green at 3caf010, run 29101207022).

Status update 2026-07-10: on-zero 0.6.8 shipped with the timer fix
(d942e301) — item 2 below is DONE. orez 0.4.49 shipped from main and
does NOT contain this branch (no `orez/sync-server` export), so item 1
still needs the merge first. The branch is rebased onto v0.4.49 and
fully green locally (lint + format + types + 899 tests); merging is now
a fast-forward push of `rust-sync-server` to main, then a normal
release. Chat staging deploy (item 3 phase 1) is in progress.

## 1. Merge + release orez `rust-sync-server` (npm) — STILL PENDING

What it ships: the sync-server core + `orez/sync-server` export
(createSyncServer, createSyncServerMount), the reset-retry and
recovery-classifier fixes, the four CI-flake fixes.

```sh
# branch is pre-rebased onto main; this is a clean fast-forward
cd ~/.worktrees/orez-rust-sync
git push origin rust-sync-server:main
cd ~/orez && git pull && bun release --patch --ci --skip-test
```

Unblocks: removing the dist-overlay caveat on soot's
`soot-mount-consume` branch (it imports `orez/sync-server`).

## 2. Publish takeout on-zero timer fix — DONE (0.6.8, 2026-07-10)

`d942e301` — createMutators.withTimeoutGuard clears its 60s timer in a
finally. Consumers still need `bun up` to pick it up.

## 3. Production cutover (soot, then chat)

The plan's final gate. All prep is real and rehearsed:

- rollback/one-writer drill PASS twice against lslcf (old-only -> none
  -> new-only -> none -> old-only, zero invariant failures)
- soot's OWN composition deployed + 20/20 conformance on lslcf
  (soot-deployed-conformance harness lane, committed)
- cutover trip list recorded (soot-migration-inventory.md §H):
  auth ordering, namespace mapping, cookie-domain reset, preserved
  semantics, one-writer lifetime
- COOKIE DOMAINS ARE THE RISK: soot's project plane spans three cookie
  domains (embed watermark / node WAL LSN / \_zsync_changes). Cutover to
  the rust engine changes the domain — clients need a forced reset or a
  new storage identity; numerically plausible old cookies must never be
  accepted. Plan for a reset-on-cutover client experience.
- chat: the rust chat-host ran the real Chat app end-to-end under load
  (346 msg/s, 12k pushes clean post-fix); chat is your declared
  experiment/testbed, so it is the natural first cutover.

Suggested order: chat first (experiment, low blast radius), observe,
then soot canary namespace -> observation window -> remaining
namespaces -> delete the old path (final plan step).

## Also awaiting your OK (external, non-blocking)

- File plans/upstream-zero-cache-taskid-bug.md on rocicorp/mono
  (paste-ready).
