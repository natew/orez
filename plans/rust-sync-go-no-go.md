# rust-sync-server: go/no-go decision sheet

Everything in plans/rust-sync-server-final-plan.md that can be done
without your explicit word is done and green (tracker:
rust-sync-server-execution.md, CI green at 3caf010, run 29101207022).
Three actions remain, each yours to trigger. They are ordered; each
unblocks the next.

## 1. Release orez `rust-sync-server` (npm)

What it ships: the sync-server core + `orez/sync-server` export
(createSyncServer, createSyncServerMount), the reset-retry and
recovery-classifier fixes, the four CI-flake fixes, the pg-proxy work.

```sh
cd ~/orez   # after merging rust-sync-server, or release from the branch per repo flow
bun release --patch --ci --skip-test   # tests just ran green in CI
```

Unblocks: removing the dist-overlay caveat on soot's
`soot-mount-consume` branch (it imports `orez/sync-server`).

## 2. Publish takeout on-zero timer fix

What it ships: `d942e301` — createMutators.withTimeoutGuard clears its
60s timer in a finally. Without it every on-zero consumer accumulates
one live timer per completed mutation; workerd dies at its 10,000
active-timer cap (~10k sustained pushes), Node just accumulates.
Validated: on-zero 147 tests green; two independent 12k real-Chat runs
clean after the fix.

```sh
cd ~/takeout && bun ./packages/scripts/src/release.ts --patch --ci --skip-tests --dirty
# then in consumers: bun up takeout
```

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
