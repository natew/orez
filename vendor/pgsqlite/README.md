# vendor/pgsqlite/

Build output and pinned binary for [`erans/pgsqlite`](https://github.com/erans/pgsqlite),
the Rust PG-wire-protocol-over-SQLite server we use as a **test oracle** for
`src/pg-sqlite-compiler/`.

This is **not** shipped at runtime. orez itself does not depend on pgsqlite —
the compiler is pure TS and runs in Cloudflare workerd. pgsqlite is purely a
dev/CI tool that lets us validate compiler output by comparing result sets
against a reference implementation.

## Pinned version

`v0.0.22` (pinned in `scripts/pgsqlite/ensure.ts`). Bump there + re-run
`bun run compiler:harvest-fixtures` to refresh the test corpus.

## How it's resolved

`scripts/pgsqlite/ensure.ts` looks in order:

1. `$PGSQLITE_BIN` env var
2. `pgsqlite` on PATH
3. `vendor/pgsqlite/<platform>/pgsqlite` (prebuilt, future)
4. `~/github/pgsqlite/target/release/pgsqlite` (local dev checkout)
5. `vendor/pgsqlite/build/target/release/pgsqlite` (cargo build from pin)

Writes the resolved path (or empty) to `.resolved-path`. Oracle-dependent
tests skip if the file is empty.

## In CI

The `compiler` job in `.github/workflows/ci.yml` installs rust, runs
`compiler:ensure-pgsqlite` (which clones + builds), caches `~/.cargo` and
`vendor/pgsqlite/build/target`, then runs `test:compiler`.

Build is ~5–8 min cold, near-instant warm via cache.

## Not committed

`build/`, `<platform>/`, and `.resolved-path` are gitignored. Only this README

- vendored test fixtures (under `src/pg-sqlite-compiler/fixtures/pgsqlite/`)
  are tracked in git.
