# on-zero is not covered by CI, and does not currently pass

**Date:** 2026-09-16
**Found while:** fixing `generate-cache.json` being trusted as a description of disk (`fix/on-zero-generate-cache-disk`).

## The gap

`.github/workflows/ci.yml` runs `bun run test`. The root `test` script does not
run every workspace; it names them:

```
drizzle-zero-sqlite, database, helpers, env, cli
```

`on-zero` is not in that list, and no workflow mentions it. Nothing in CI has
ever executed `packages/on-zero`'s tests. A change to the generator can red its
own suite and still show a green tree.

This is not hypothetical: the bug that prompted this note lived in
`writeFileIfChanged`, which had no test covering a generated file that exists
but holds different bytes, and the generator is the thing every downstream
repo's build consumes.

## Adding it is not a one-line change

Two things have to be fixed first, or wiring `on-zero` into `bun run test` reds
CI on failures that predate the change:

**1. Two tests fail on `origin/main` today.** Confirmed by running them against
`origin/main`'s `generate.ts`, not just against the branch:

- `src/mutations.test.ts` > generated CRUD authorization > authorizes both sides
  of composite-key writes through the server executor
- `src/useQuery.aggregate.zero.test.tsx` > a mounted useQuery view updates after
  a custom mutation projects an aggregate

Full suite: 204 pass, 2 fail.

**2. A fresh worktree cannot run vitest at all** without an install inside the
worktree. `packages/on-zero` pins TypeScript 7.0.2 while the repo root resolves
5.9.3, so `typescript/unstable/ast` is missing and the suite dies before the
first test. Two further stops follow: `orez-lite/dist` is not built, and
`drizzle-zero-sqlite` is not built. A CI job for on-zero needs those build steps
ordered ahead of it, and the TS version skew resolved rather than worked around
by installing twice.

## Why this matters more than the usual uncovered package

on-zero is a code generator. Its output is committed in every downstream repo
and consumed by their builds. An uncovered defect here does not surface as a
failing on-zero test; it surfaces weeks later as a downstream bundle nobody can
reproduce locally, which is exactly how the cache bug was found.

Downstream repos can defend themselves in the meantime. Soot regenerates in an
isolated copy and byte-compares (`scripts/check/zero-generated.ts`); that check
is the reason the drift was visible at all.
