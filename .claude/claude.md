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
