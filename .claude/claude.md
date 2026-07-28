DO NOT PUBLUSH without permission!

COMMIT before you publish!

ALWAYS merge finished work to main and push (pull first). Standing
permission from nate (2026-07-16): main pushes do not need re-asking.
Publishing/releasing still ALWAYS needs explicit permission.

USE CONVENTIONAL COMMITS

Lint and format are SEPARATE CI gates. `bun run lint` (oxlint) passing does not
mean `bun run format:check` (oxfmt) passes, and CI runs both in the `test` job.
Run `bun run format:check` before pushing, or CI goes red on whitespace after a
green suite. This has already cost one red main push.

the ONLY way to publish is:

`bun release --patch --ci`

IF YOU JUST TESTED USE `--skip-test`

PACKAGE EXPORTS (owner rule, 2026-07-27): implementation may live in
sub-packages (orez-sync-executor, orez-sync-cf-host), but the CANONICAL import
path for consumers is always the parent package (`orez-lite/realtime`, not
`orez-sync-executor/realtime`). Published consumers (on-zero, apps) and docs
use the parent path only. Direct sub-package imports are allowed solely inside
this repo's own sub-packages and the harness where importing through the
parent would create a package cycle or an unbuilt-dist resolution problem.
