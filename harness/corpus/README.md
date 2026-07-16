# Upstream scenario corpus

`upstream-scenarios.json` is the provenance ledger for behavioral contracts
adapted from Zero, Turso, and Electric. Each entry pins a repository commit,
license, source test ID, Orez adaptation, and the hosts on which the contract is
meaningful.

The corpus deliberately records contracts and fixture ideas, not copied test
implementation. `bun src/corpus-check.ts` validates the ledger. The portable
four-host scenarios run through `bun src/upstream-corpus.ts`; trigger-CDC
contracts run as `cargo test -p sync-core --test upstream_corpus`; the generated
fault/state-machine contract runs through `bun src/state-machine.ts`.

To refresh a source, review the upstream diff and license, update the full
40-character commit, and keep the original scenario name stable in the entry.
Do not silently move a pin to a branch head.

## Chat transaction queries

`chat-transaction-query-v1.json` freezes 252 permission-resolved caller/query
cases across 123 unique query names from Chat commit
`cc2d26fa24a88161231f3337c0e0cae9d43ae2d1` and Zero 1.7.0. The generator
imports the pinned query builders and captures both `ast` and `format` through
Zero's `asQueryInternals`. `exploreTable` is excluded because its table name is
chosen at runtime.

Regenerate and compare it from an exact detached Chat checkout:

```sh
git -C ../chat worktree add /tmp/chat-zql-corpus cc2d26fa24a88161231f3337c0e0cae9d43ae2d1
(cd /tmp/chat-zql-corpus && bun install --frozen-lockfile)
bun scripts/harvest-chat-transaction-query-corpus.ts \
  --chat /tmp/chat-zql-corpus \
  --out /tmp/chat-transaction-query-v1.json
cmp harness/corpus/chat-transaction-query-v1.json \
  /tmp/chat-transaction-query-v1.json
bun run test:transaction-query:chat
```

The `expect` values are the original sync-membership oracle. The differential
runner compares transaction result shapes to official z2s/Postgres execution.
