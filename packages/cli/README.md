# @o/cli

Typed command definitions, subprocess execution, and process cleanup for project
tooling.

```ts
import { cmd, handleProcessExit, run } from '@o/cli'
```

Run package scripts in parallel with the `o` binary:

```sh
o run-all --flags=last api dev
```

Add `--pty` for the interactive terminal supervisor and `--watch` to restart
failed children.
