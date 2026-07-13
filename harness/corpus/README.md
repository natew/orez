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
