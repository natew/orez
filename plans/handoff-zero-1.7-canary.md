# handoff: Zero 1.7.0-canary.3 upgrade across orez / chat / soot

date: 2026-06-21
branch (orez): `chore/upgrade-zero-1.7-canary` (3 commits ahead of `main`)
branch (chat): `chore/upgrade-zero-1.7-canary` (2 commits ahead of `main`)
branch (soot worktree): `chore/upgrade-zero-1.7-canary` in `~/.worktrees/soot-zero-17` (0 commits yet — all working-tree)
prod: not deployed — staging-only upgrade until user approves publish
coordinator session: `ab-mqok354p-64475` (this Claude session)

## Goal in one paragraph

Upgrade `@rocicorp/zero` from `1.6.1` → `1.7.0-canary.3` across orez + chat +
soot (orez-web + orez-cf), driven by Codex sub-agents under agentbus.
Orez stays on the canary; downstream consumers already on React 19, so the
1.7 peer bump is a non-issue. Runbook is `plans/upgrade-zero.md` — both the
existing §6 worked example for 1.5→1.6 and the brand-new §6 entry for
1.6→1.7-canary.3 (committed in `670de46`) explain the coupling map and the
1.7-specific patches.

## What's done

### Stage 1-2: orez (commits on `chore/upgrade-zero-1.7-canary` in `~/orez`)

Driver: Codex session `ab-mqokjnye-60287` (now idle, can be re-used or left).

- `3da0cc4 chore(deps): upgrade @rocicorp/zero to 1.7.0-canary.3` — package.json + bun.lock + appended §6 worked example to `plans/upgrade-zero.md`.
- `fc6f96f fix: align zero 1.7 transport timing` — `src/zero-http/transport.ts` (+ a beforeEach hook timeout fix in `src/replication/zero-compat.test.ts`).
- `670de46 docs: record zero 1.7 canary validation` — finishes the §6 worked example.

Validation:

- `bun test src/worker/cf-patches.test.ts` — 4/4 (every patch anchor held; no edits to cf-patches.ts).
- `bun run test` — 49 files / 734 tests.
- `bun run native:bootstrap && bun run test:integration` — 10 files / 30 tests.
- `bun run test:wasm` — 22/22.
- `bun run test:compiler` — 52/52.
- `bun run check` clean.

Key decisions (don't re-litigate):

- `PROTOCOL_VERSION` stayed at **51** in 1.7-canary.3 (same as 1.6) — none of the 8 §3.A sync-protocol string sites needed updating. Confirmed by reading `node_modules/@rocicorp/zero/out/zero-protocol/src/protocol-version.d.ts` directly.
- All five `patchWorker*` anchors in `src/worker/cf-patches.ts` matched without edits. `WORKER_AUTOSTART_PREFIX` (stable-prefix rewrite from 1.6) still aligns with all 5 entrypoints.
- `patchWriteWorkerClient` inline body still mirrors upstream `write-worker.js`. `createLogContext(..., "write-worker")` and `serializeError` export both still match.
- Transport race fix uses a 25ms timer (`CONNECTED_QUERY_FLUSH_MS`) — schedule the initial pull on a timer so the asynchronously-arriving `initConnection` / `changeDesiredQueries` message (Zero 1.7 added a ~10ms client throttle) can cancel the timer and drive the pull itself, folding `gotQueriesPatch` into the first snapshot. Without it, `transport.test.ts: connect + complete hydrates a stock Zero materialized query` flakes 1/5.
- Vitest beforeEach hook in zero-compat is unrelated to 1.7 but surfaced as a load-flake under full-suite — `{ timeout: 30000 }` on the describe doesn't apply to hooks, fixed inline on the hook.

### Stage 3: chat (commits on `chore/upgrade-zero-1.7-canary` in `~/chat`)

Driver: Codex session `ab-mqolsesm-32105` (idle, done).

- `9cc6eaff3 chore(deps): upgrade @rocicorp/zero to 1.7.0-canary.3` — bumps package.json, bun.lock, `.github/workflows/ci.yml:143` (`ZERO_VERSION` default `'1.6.1'` → `'1.7.0-canary.3'`), `docker-compose.yml` `rocicorp/zero:${ZERO_VERSION:-...}` and matching `ZERO_VERSION` env default, and `src/uncloud/docker-compose.yml`'s generated default.
- `fc1934caa fix(hud): restore message focus after closing menu` — `src/features/message-input/MessageInputEditor.tsx` adds an Escape-key path that closes the HUD menu and re-focuses the editor. Surfaced by the `channels.test.ts` focus-flow case on the canary backend; verified to be a real regression, not test churn.

Validation:

- `bun check` clean.
- `bun run test e2e --unit` — 25 files / 304 tests.
- chat Docker Zero e2e on `rocicorp/zero:1.7.0-canary.3` — 49 passed / 2 skipped.
- orez DO-backed `bun run test:chat:e2e` (DO_BACKEND_URL=http://127.0.0.1:8798, wrangler dev port 8798, RETRY=1) — 48 passed / 1 flaky-passed-on-retry / 2 skipped, full integration in 7m21s.

`packages/flowb/*` was a co-tenant's WIP — left untouched. Confirmed final chat working tree only carries those pre-existing flowb mods.

### Stage 4: soot orez-web (worktree, no commits yet)

Driver: Codex session `ab-mqon44j7-43562` (**still running** as of writing — currently inside stage 5).

Worktree at `~/.worktrees/soot-zero-17` on branch `chore/upgrade-zero-1.7-canary` (based off committed soot `main` at `a02653e69`). `.env` + `.env.development` copied from `~/soot` (gitignored, do not commit).

Manifests bumped to 1.7.0-canary.3 (uncommitted in working tree):

- `package.json` root
- `packages/orez-web/package.json`
- `templates/app/package.json`, `templates/flights/package.json`, `templates/game/package.json`, `templates/sootbean-mobile/package.json`, `templates/todo/package.json`
- `bun.lock` regenerated

Validation (all green per Codex mail #13, PORT_OFFSET=700):

- `bun run build:prereqs:validate` — pass.
- `bun run build:orez` — pass.
- `grep -o 'protocolVersion: [0-9]*' public/orez-web-zc.worker.js` → `51`. No `!singleProcessMode()) exitAfter` leak (the 1.6 build-zero-cache.ts auto-start regex regression did NOT recur on 1.7).
- `test:orez:smoke` 3/3, `test:orez` 11/11, `test:orez:robust` 5/5.

orez-side patch landed by Codex (UNCOMMITTED in `~/orez`):

- `src/worker/shims/node-stub.ts` (+60 lines) — extends the worker node-stub for Zero 1.7's new node-API surface: `fs.createReadStream`, `net.isIP`, `http.Agent` / `https.Agent`, `module.isBuiltin` / `module.builtinModules`, `process.version` to `v22.x`. Was then shipped to the worktree via `bun release --into ~/.worktrees/soot-zero-17` and `bun run build:orez` re-ran to pick it up.

### Stage 5: soot orez-cf (in flight)

Driver: same Codex `ab-mqon44j7-43562`. Title bar at last check: "CF DoBackend transaction serialization bug fix" — meaning it's mid-way through fixing a compiler-gap surfaced by one of the live deploys.

orez-side patches in flight (UNCOMMITTED in `~/orez`):

- `src/pg-proxy-do-backend.ts` (+50/-13) — the compiler-gap fix Codex titled "DoBackend transaction serialization".
- `src/pg-proxy-do-backend.test.ts` — accompanying test.
- (`src/worker/shims/node-stub.ts` is the stage 4 patch above.)

Codex was instructed to commit these on the orez branch and re-`bun release --into` between fixes, per `plans/upgrade-zero.md` §7.4 step 4.

## What's remaining

### 1. Wait for soot codex `ab-mqon44j7-43562` to finish stage 5

Monitor `bztkoj1ti` is armed on the soot worktree branch + codex state.
Mail will arrive at coordinator `ab-mqok354p-64475` at milestone(s).

Stage 5 plan it's executing (from `plans/upgrade-zero.md` §7.4):

- `bun scripts/dev/test-cf-do-bundle.ts` — bundle check (expect ok ~4.5MB, 9 patches).
- `bun test test/cloudflare-do-deploy.test.ts` — unit guard (expect 23/23).
- `bun scripts/dev/test-cf-do-deploy.ts {todo,app,flights} zero17-cfdo --runtime` — each stages, builds, wrangler-deploys to soot's CF account, runs `validate-cf-do-runtime.ts` in two browser contexts, tears down.
- Expect a compiler-gap tail (different schemas than chat). Fix in orez, re-`bun release --into`, redeploy.

### 2. Cleanup pass on the soot worktree before any commits

The working tree currently has CONTAMINATION from the boot-time fix Codex did (so the dev server would start against committed-main):

Files that MUST NOT be in any commit (verify with `git diff --stat` before `git add`):

- `public/orez-web-pglite.worker.js` — built artifact, not source.
- `src/database/migrations/20260621011424_redundant_squadron_sinister.ts` — pre-existing modification, unrelated.
- `src/features/f2c/f2cStore.ts` and untracked `src/features/f2c/F2CProvider.tsx` — co-tenant's f2c WIP. Codex copied these from soot main to get the dev server to boot. These belong to whatever agent owns f2c in `~/soot`, NOT to this upgrade.
- `scripts/dev/validate-cf-do-runtime.ts` — verify before commit; this MAY have legitimate stage 5 changes Codex made for the cf-do runtime probe. Read the diff before deciding.
- `templates/app/**/*.tsx`, `templates/app/data/generated/syncedMutations.ts`, `syncedQueries.ts`, `templates/app/features/user/useUser.ts`, `templates/app/interface/feed/*`, `templates/flights/helpers/useDemoAutoSeed.tsx` — these look unrelated to the zero version bump. Read the diffs; if they're generated artifacts from Zero schema regeneration, the regeneration is fine but it should be its own conscious commit. If they're hand edits the f2c agent left behind, they should NOT be in this commit.

Files that SHOULD be in commits:

- `package.json` (root)
- `bun.lock`
- `packages/orez-web/package.json`
- `templates/{app,flights,game,sootbean-mobile,todo}/package.json`
- `src/worker/shims/node-stub.ts` ONLY IF soot has its own copy (unlikely — this is an orez file; the soot worktree dirty status showing it is suspicious and needs a `git log` check to confirm whether this file is actually tracked in soot).

Suggested split per `plans/upgrade-zero.md` §7.5 pattern:

- Commit A: `chore(deps): upgrade @rocicorp/zero to 1.7.0-canary.3` covering all 8 package.json + bun.lock files.
- Commit B (optional): `feat(scripts): ...` if `validate-cf-do-runtime.ts` was legitimately improved during stage 5.

### 3. Commit the orez stage-4/5 fixes on the orez branch (CRITICAL)

`~/orez` working tree has THREE uncommitted files that MUST land on
`chore/upgrade-zero-1.7-canary` before the upgrade can be considered done.
Without these commits, `bun release --into` shipped patched code, but the
orez branch itself still claims it doesn't need the fix — anyone re-shipping
later regresses:

```bash
cd ~/orez
git diff --stat
#  src/pg-proxy-do-backend.test.ts
#  src/pg-proxy-do-backend.ts
#  src/worker/shims/node-stub.ts

# Suggested commits:
git add src/worker/shims/node-stub.ts \
  && git commit -m "fix(node-stub): add fs/net/http/module/process surface for zero 1.7" \
  -- src/worker/shims/node-stub.ts

git add src/pg-proxy-do-backend.ts src/pg-proxy-do-backend.test.ts \
  && git commit -m "fix(do-backend): <whatever the transaction-serialization fix is>" \
  -- src/pg-proxy-do-backend.ts src/pg-proxy-do-backend.test.ts
```

Read the actual diff for the do-backend change before writing the message —
it's substantive (+50/-13 in `pg-proxy-do-backend.ts`). Likely something
specific to how 1.7 routes / batches `_zero_pending_changes` rows.

### 4. Update `plans/upgrade-zero.md` §6 worked example with the stage 4/5 findings

The current §6 entry for 1.6→1.7-canary.3 (commit `670de46`) only covers
stages 1-2 (it was written before stage 4/5 ran). Append:

- The node-stub extension list (which 1.7 dependencies pulled in which node APIs).
- The do-backend transaction-serialization fix (root cause + shape of fix).
- Stage 4 results (test:orez full/smoke/robust counts; `protocolVersion: 51` confirmation; `build-zero-cache.ts` guard didn't regress).
- Stage 5 results (bundle MB, unit guard count, 3 template deploy outcomes, any further compiler-gap fixes).
- Validation status (mirroring §6's existing 1.5→1.6 worked example structure).

### 5. Update CI default in orez

`~/orez/.github/workflows/` — search for any `ZERO_VERSION` default (chat's
chat repo CI uses one at line 143 in `ci.yml`). orez itself may not pin a
docker zero version, since orez is the implementation; double-check.
`grep -rn ZERO_VERSION ~/orez/.github/ ~/orez/scripts/` returned no hits
during recon, so probably nothing to do — verify.

### 6. Hand back to user for the release decision

These three branches and their commits are the deliverable for now:

- `~/orez` `chore/upgrade-zero-1.7-canary` (3 commits + the 2-3 fix commits from §3 above)
- `~/chat` `chore/upgrade-zero-1.7-canary` (2 commits)
- `~/.worktrees/soot-zero-17` `chore/upgrade-zero-1.7-canary` (the upgrade commit(s) per §2 above)

Do NOT publish. Do NOT push. User will approve. See `~/orez/.claude/CLAUDE.md`
and `~/.claude/CLAUDE.md` "Releasing" — the publish command is
`bun release --patch --ci` (add `--skip-tests` only if you've just run
them), and ONLY after explicit user permission. soot needs no publish — the
worktree branch is just merged/pushed.

## Key context

### How the three repos couple

- `~/orez` is the implementation: PG-protocol shim + Zero patches + cf-do worker.
- `~/chat` is a consumer that runs docker-zero + orez and is the canonical e2e gate (`bun run test:chat:e2e` from `~/orez`).
- `~/soot` is a consumer that bundles orez two ways: orez-web (browser worker) via `packages/orez-web`, and orez-cf (Cloudflare DO) via `src/deploy/cloudflareDoDeploy.ts` importing from `orez/cf-do`.
- soot resolves orez from `node_modules/orez/dist`, shipped by `bun release --into ~/soot` (or worktree) from `~/orez`. No publish needed for testing.

### Patches and anchors (the fragile bits)

The `plans/upgrade-zero.md` §3 coupling map is authoritative — every Zero
release MUST re-validate every anchor. On 1.7 every anchor held without
edits (the §3.C-2 stable-prefix anchor from 1.6 paid off). If a future
release moves them, the patches fail loud.

`src/worker/cf-patches.ts` is the most fragile. `patchWriteWorkerClient`
overwrites zero-cache's compiled `write-worker-client.js` with an inline
implementation — its exports + signatures must mirror upstream
`write-worker.js`. Diff before editing.

### Codex via agentbus — the harness pattern

Each Codex worker is spawned with:

```bash
agentbus spawn --role worker --cwd <repo-or-worktree> --name <alias> --quiet \
  -- codex --yolo "$(cat prompt.txt)"
```

The session can be addressed by id (`ab-mq...`) or by alias. Mail back to the
coordinator with `agentbus mail send ab-mqok354p-64475 "<body>"`. The
coordinator reads with `agentbus mail read` and inspects turns with
`agentbus show turns <id>` or `agentbus tail <id>`.

Coordinator (me) does plan + review only. Codex does the editing + commits.
This rule is from the user mid-session — see the goal directive (also
saved as the active Stop-hook condition on this session).

### Critical operational rules (from CLAUDE.md)

- No `git stash`, no `git add -A`, no `git checkout .` — shared checkouts.
- All commits use explicit pathspecs: `git add path/a path/b && git commit -m "..." -- path/a path/b`.
- No `gh run watch` (drains the 60/hr GitHub API budget in 3 minutes).
- For soot main: don't touch — many concurrent agents. Use the worktree.

## Verification checklist

For the next agent, in order:

- [ ] **Soot codex finished cleanly.** Verify: `agentbus list --all | grep ab-mqon44j7-43562` shows `idle` or `exited`. If `running`, read the latest mail at `agentbus mail read` and decide whether to let it continue or escalate.
- [ ] **Stage 5 mail landed.** Verify: `agentbus mail read` shows a final mail from `ab-mqon44j7-43562` summarizing 3 template deploys + commit SHAs. If not, `agentbus show turns ab-mqon44j7-43562 | tail -50` to see where it stopped.
- [ ] **Orez fixes committed.** Verify: `cd ~/orez && git diff --stat` is empty AND `git log --oneline origin/main..HEAD` shows 5-6 commits including the node-stub fix and the do-backend fix. The bare 3-commit state (3da0cc4 / fc6f96f / 670de46) means the fixes were NOT committed and the branch is incomplete.
- [ ] **Soot worktree commits are clean.** Verify: `cd ~/.worktrees/soot-zero-17 && git log --oneline origin/main..HEAD` shows commits that DO NOT touch `src/features/f2c/*`, `src/database/migrations/*`, `public/orez-web-pglite.worker.js`, or stray template files. If they do, those commits were contamination from the dev-boot fix and should be amended or split.
- [ ] **soot worktree env files are not staged.** Verify: `cd ~/.worktrees/soot-zero-17 && git status --short | grep -E '\.env(\.|$)'` returns nothing. They should be silently gitignored.
- [ ] **Stage 5 deploys actually torn down.** Verify: ask the user or check the CF dashboard / `wrangler` if there are orphan workers under the `zero17-cfdo-*` prefix on soot's CF account (`aa20b480cc813f2131bc005e2b7fd140`). The `--runtime` flow without `--keep` should clean up automatically.
- [ ] **`plans/upgrade-zero.md` §6 reflects all stages.** Verify: `grep -A2 "Stage 4 \|Stage 5 " ~/orez/plans/upgrade-zero.md` shows entries for 1.6→1.7 stages 4 and 5. If only stages 1-2 are described, append the missing sections per §4 above.
- [ ] **No `bunfig.toml` left anywhere.** Verify: `ls ~/orez/bunfig.toml ~/chat/bunfig.toml ~/.worktrees/soot-zero-17/bunfig.toml 2>/dev/null` returns empty.

When all boxes are checked, the three branches are ready to hand to the user
for publish + push + downstream consumer bumps.

## Active sub-agents

| session             | alias             | repo                      | state   | role                          |
| ------------------- | ----------------- | ------------------------- | ------- | ----------------------------- |
| `ab-mqokjnye-60287` | zero17-orez-codex | ~/orez                    | idle    | stage 1-2, done               |
| `ab-mqolsesm-32105` | zero17-chat-codex | ~/chat                    | idle    | stage 3, done                 |
| `ab-mqon44j7-43562` | zero17-soot-codex | ~/.worktrees/soot-zero-17 | running | stages 4-5, stage 5 in flight |

The first two can be left idle (no harm) or stood down with a SendMessage
shutdown_request (see how I did it for the original Claude driver). The
third should NOT be stopped until it finishes stage 5 or hits a blocker.

## Coordinator monitor

Monitor task id `bztkoj1ti` is armed on the soot worktree branch + codex
state. Any new commit on the soot branch or any state transition from
`running`/`thinking` to `idle`/`exited` fires a notification.
