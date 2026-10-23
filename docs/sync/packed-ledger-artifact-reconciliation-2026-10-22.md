# Packed ledger artifact reconciliation, 2026-10-22

The retained Contrast launch used an executor artifact from immediately before
the existing packed ledger rotation repair. No additional Orez runtime change
is justified by the reproduced failure. Upgrade the consumer's package family
and verify the installed bytes before evaluating downstream acceptance.

## Source and artifact reconciliation

The frozen executor is `0.16.20-canary.1792263974161`, with
`releaseSourceCommit` `dc42fb19c55ae5e301b5b9d254b7198d2607cb72`.
Its actual `dist/packed-ledger.js` calls rotation without draining pending keys.
The next source commit, `e2ef03b6df2803505612fc01d2a05ed0adfdf30e`, repairs
that lifecycle: merge captured keys, drain the old segment inside the application
transaction, rotate, then commit the envelope. A failed envelope rolls back the
drain and application writes together.

Exact published version `0.16.20-canary.1792576209733` records source
`aeac52c5719674688bc5864ce6096fd4bd1599bc`, a descendant of the repair.
The executor tarball's SHA512 matches registry integrity. Its ledger bytes
match a local build from Orez `845e45b8aa305c5627d34859fb55b43fe2cf211d`.

| Ledger artifact             | SHA256                                                             |
| --------------------------- | ------------------------------------------------------------------ |
| Frozen installed executor   | `259e9024b8e8a10de7dcb0e8bc57297121c9d1929260667ab2aa64799cae2566` |
| Verified published executor | `2e83c8668fd3003fd0a629457c9418138e95e46b6f600abeaade232489013560` |

Exact-version registry metadata confirms the same source and version for
`@o/cli`, `@o/database`, `@o/env`, `@o/helpers`, `@o/hooks`,
`drizzle-zero-sqlite`, `on-zero`, `orez-lite`, `orez-sync-executor`,
`orez-sync-cf-host`, and `bedrock-sqlite`. The public consumer path remains
`orez-lite`; the executor artifact is inspected directly only as internal
diagnostic evidence. No package was installed downstream or published here.

## Offline behavioral evidence

The retained probe copies the frozen `sootAgent`, `durableOperation`, and ledger
schema, rows, and actual triggers into memory. It positions a retained payload
below 16 KiB, performs an exact agent renewal followed by a raw reply renewal,
then commits through the unmodified executor capture implementation.

| Control                                        | Payload before / after raw renewal | Result                                                                                                        |
| ---------------------------------------------- | ---------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| Frozen executor below threshold                | 15,752 / 16,019 bytes              | Commits                                                                                                       |
| Frozen executor crossing threshold             | 16,200 / 16,467 bytes              | Throws `packed ledger cannot rotate with pending trigger keys`; renewal rolls back                            |
| Published repaired executor crossing threshold | 16,200 / 16,467 bytes              | Commits; both agent and reply keys survive; all segments settle with empty pending keys and capture mode zero |

The artifact probe reuses the original retained probe with only import targets
and the result label changed. The frozen database SHA256 remains
`e74f5e799b1032f00b7f04d5035a0d0c9697e5348494cd85462b19179f1a6b3d`.
It makes no provider calls and writes only to memory.

The existing executor suite also passes all 179 tests at `845e45b8`.
Its rotation regression checks opaque trigger side effects, exactly 11 physical
writes, LMID preservation, and restoration of ledger segments and application
rows after an oversized final envelope fails.

The probe reduces renewal predicates to row identity and controls the payload
boundary. It does not reconstruct the historical rolled-back pending state or
exercise complete HTTP authorization and ownership fences. Frozen events prove
three renewal failures preceded local expiry; they do not establish why the
Manager had no provider request. Keep those acceptance questions separate.

## Evidence and delivery ownership

Orez task: `t-mvk7i1d4-1hm70`. Source reconciliation: `r118512`.
Downstream acceptance and scoped cause review: `r118211`.
Broader delivery belongs to `ci-manager`; no new runtime delivery is queued.

Retained local evidence directories:

- `~/.team-machine/evidence/factory-sonnet-operator-2`: frozen inputs.
- `~/.team-machine/evidence/factory-startup-ownership-r118504`: original
  probe, source comparison, and primary chronology.
- `~/.team-machine/evidence/packed-ledger-reconciliation-r118512`: exact npm
  tarball, registry metadata, integrity receipt, artifact diff, and reused probe
  with its result.

The downstream owner must pin the package family in an isolated integration
tree, inspect the resolved executor bytes, and run the retained boundary against
that installation before broader factory acceptance. Preserve the frozen tree
as the failing control. No reset, data mutation, TTL relaxation, error
suppression, stable release, or paid rerun is part of this reconciliation.
