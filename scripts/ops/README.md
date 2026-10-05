# release waits

When a release fails before receiving a hosted runner, check the job's
annotations and GitHub's Actions status before retrying. Wait for recovery
with `tm wait --exec 'node scripts/ops/watch-actions-recovery.mjs' --timeout 45m`.
The watcher checks only Actions, exits successfully when it is operational,
and stops after 40 minutes. `--once` checks the same condition without waiting.

After recovery, retry the current main push run and watch its exact run ID
with `bun scripts/ops/watch-ci.ts --run <id>`. A successful workflow that
skipped a superseded source is not evidence that npm contains the fix.
Verify the published tarball and its `releaseSourceCommit` before upgrading.
