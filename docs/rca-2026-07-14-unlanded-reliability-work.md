# RCA: reviewed reliability work sat unlanded for days

**Date:** 2026-07-14
**Scope:** ~12.6k lines of reviewed Orez reliability work, a production write-amplification fix, and pairwise sweep coverage that were complete and reviewed but never reached `origin/main`, spread across ~18 worktrees and branches.

## What landed (the recovery)

All of it is now on `origin/main` @ `c6437d1`:

- `c74392d` — full Jepsen-lite reliability suite (`test/jepsen-lite-integration`).
- four permission-transition commits, with the `ci.yml` consistency-checker test list unioned.
- pairwise sweep coverage ported from `zero-sync-server` (classifier verified 225/225 @ seed 42 against the current generator).
- `5075a29` — the `snapshotSideEffectWriteTables` trigger/FK-closure fix (startup source writes 10,888 → 890).
- `c6437d1` — a pre-existing `cargo fmt -p sync-native` fix that was blocking the whole `rust` CI job.

Superseded/stale branches and their worktrees were removed; active WIP (`fix/cf-instance-routing`), its acceptance harness (`test/two-zero-cache-do-proof`), the canonical profiling branch (`codex/orez-chat-wrapper-profile`), and the resnapshot RCA branch were kept.

## Timeline

- **2026-07-11** — reliability work committed on its branches (earliest `2b60396` 09:27, latest `bd2ef30` 12:15). Reviewed: multiple review rounds, mutant checks, replay evidence.
- **2026-07-11 → 07-14** — priorities pivoted to the Rust Zero migration. `origin/main` advanced 67 commits and shipped v0.5.9 and v0.5.11.
- **2026-07-14** — the reviewed branches were still unmerged. Agentbus parent task `t-mrgqyuci-23qp0` ("Build Jepsen-lite reliability testing for Orez") was still `open` / `urgent`.

The work stopped because priorities changed, not because reviewers rejected it.

## Root cause

The task lifecycle had no landing step. "Reviewed" was treated as terminal; nothing forced the diff onto main or forced an explicit discard. Four things turned that gap into three days of divergence:

1. **Worktree isolation put the work out of sight.** Each task got its own `~/.worktrees/<name>` directory and branch. Work finished there is invisible from the primary checkout: `git status` never shows it, `git branch` is the only reminder, and a switch away leaves no ambient trace. Physically-separate directories make "park it and move on" the path of least resistance.

2. **The priority pivot had no drain.** When the team moved to the Rust Zero migration, in-flight reviewed work was not landed first. The branches simply froze in place.

3. **A red main removed the signal that landing matters.** `origin/main`'s `test` and `rust` CI jobs were already failing (and had been for several releases). With no green main to protect, there is no feedback that a branch has drifted out of mergeability and no pressure to keep it landable. Branches diverged 67 commits with nobody noticing.

4. **The divergence tax compounded.** Every day main moved, the merge got scarier and landing felt more optional. The `rescue/worktree-hygiene-*` branches are evidence the team already felt this: a prior hygiene pass _preserved_ the work into more branches instead of _landing_ it, which added clutter without closing the loop.

The agentbus task state is the clearest single signal that was present but unwired: an `urgent` task stayed `open` while its deliverable sat done-but-parked. Nobody connected "open task + finished branch not on main" to "land it now."

## Prevention

### 1. Land-or-kill discipline

A task is done only when its diff is on `main` or explicitly discarded with a reason. "Reviewed and parked in a worktree" is not a terminal state. When an agentbus session ends, its branch is either landed, handed off with an owner, or closed as discarded.

### 2. Keep main green and mergeable

A green main is the cheapest prevention lever. It makes landing safe and makes divergence visible: a branch that stops merging cleanly is a bug to fix that day, not a someday-merge. Two pre-existing reds are now tracked: the `test`-job DDL/oracle failures (`t-mrlmp239-8ya0`) and the `rust` fmt blocker (fixed in this landing).

### 3. Prefer same-checkout branches over worktrees

Worktrees are for genuinely concurrent, heavily-overlapping work. For sequential work they trade a small convenience for a large "out of sight, out of mind" cost. Fewer worktrees means fewer parked-and-forgotten branches. This matches the standing guidance to not use a worktree by default.

### 4. Short branch lifetimes

Land within a day or two. If a branch cannot land that soon, that is the signal to finish or drop it, not to let it accrue divergence tax.

### 5. Agentbus auto-scan for landing debt

The coordinator should surface parked work automatically instead of relying on a human to remember it. Two pieces:

**a. `scripts/landing-debt.sh`** (added in this change) — read-only, repo-agnostic. For every local and `origin` branch it reports commits-not-on-main, age since last commit, whether it is checked out in a worktree, and whether it still merges cleanly. A row with `WORKTREE=no`, a high age, and `MERGE=clean` is almost always forgotten, mergeable work. Sample after this cleanup:

```
BRANCH                          AHEAD  AGEd  WORKTREE  MERGE
cf                                  1    49   no        CONFLICT
fix/cf-instance-routing             2     0   yes       clean
codex/orez-chat-wrapper-profile     5     0   yes       clean
```

Before cleanup the same scan would have listed ~18 rows, most with `WORKTREE=no`.

**b. Wire it into agentbus.** A periodic coordinator job (cron, per repo) runs `landing-debt.sh --tsv` and cross-references agentbus task state:

- branch with commits-not-on-main **and** a linked task that is `done`/`reviewed` → **landing debt**, open a "land or discard" follow-up owned by the manager.
- branch that is a live session's active worktree → active, skip.
- branch idle > N days with no live session → flag for triage regardless of task state.

Cross-referencing task state closes the specific hole here: an `open`/`done` task whose branch is not on main is landing debt by definition. When a session ends, the coordinator checks whether its branch landed and, if not, files the follow-up automatically so the next pivot cannot silently strand the work.

## Follow-ups (tracked, so they are not left behind)

- `t-mrlmp0qe-8a80` — fix the exactly-once live lane's concurrent false-negative before treating the suite as parallel-safe.
- `t-mrlmp239-8ya0` — fix the pre-existing red `test` CI job (DDL/oracle DoBackend failures).
- `t-mrlmp3gf-9nk0` — find an owner and finish `fix/cf-instance-routing`.
