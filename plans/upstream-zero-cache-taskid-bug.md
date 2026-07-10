# Upstream bug report draft: zero-cache taskID nanoid option-parse crash

Target: rocicorp/mono (zero-cache 1.7.0). Not yet filed — filing is an
externally visible action, user-gated. Paste-ready body below.

---

Title: zero-cache crashes at boot (~0.03% of starts) when the generated
taskID nanoid starts with `-` followed by letters

**What happens**

When `ZERO_TASK_ID` is unset, zero-cache generates `taskID = nanoid()`
(`zero-cache/src/config/normalize.ts`) and re-exports it to child workers
via `env["ZERO_TASK_ID"]`. Each worker re-parses config through
`shared/src/options.ts`, which converts env vars into argv tokens:

```js
envArgv.push(`--${flag}`, processEnv[env]) // --task-id <nanoid>
```

`command-line-args` then tokenizes the value. A nanoid that begins with
`-` followed by letters (e.g. `-abcdefg`) parses as a short-option
cluster instead of a value, leaving `--task-id` valueless. The parser's
`normalizeFlagValue` maps the null value to `true`, and the valita
string schema rejects it:

```
TypeError: Expected string at taskID. Got true
```

The worker exits at boot with the full usage dump. nanoid's alphabet
includes `-` (and `_`), so roughly 1/64 of generated ids start with `-`;
of those, the crash needs the following characters to keep the token
flag-shaped, which lands around 0.02-0.03% of boots. In CI that is an
intermittent, unreproducible-looking boot failure.

**Repro (deterministic)**

```sh
ZERO_TASK_ID='-abcdefg' NODE_ENV=development \
ZERO_UPSTREAM_DB=postgres://x ZERO_REPLICA_FILE=/tmp/r.db \
node node_modules/@rocicorp/zero/out/zero/src/cli.js
# => TypeError: Expected string at taskID. Got true

ZERO_TASK_ID='-p123456' ... # parses fine (digits break the cluster)
ZERO_TASK_ID='--anything' ... # also crashes (double-dash option token)
ZERO_TASK_ID='_-abc' ... # fine (leading char not a dash)
```

**Suggested fixes (either suffices)**

1. Generate taskID from a dash-free alphabet (e.g. `nanoid` custom
   alphabet without `-`), or prefix it (`task-${nanoid()}`).
2. Make the env→argv conversion inject values as `--flag=value` (the
   `=` form is not re-tokenized as an option), which fixes the general
   class: any string-typed env value beginning with `-` currently
   round-trips into a mis-parse.

**Workaround for consumers**

Pin `ZERO_TASK_ID` explicitly when spawning zero-cache (we set
`ZERO_TASK_ID=zharness-stock-<port>` in our test harness).
